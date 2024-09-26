package storage

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"io"
	"strconv"
	"strings"
)

type AWSCloudStorageProxy struct {
	s3ServicesClient *s3.Client
}

func (handler ProxyAuthHandlerAWSDefaultIdentity) createProxy() (CloudStorageProxy, error) {
	awsConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, wrapError("unable to create AWS service client", err)
	} else {
		client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
			if handler.AccountURL != "" {
				o.UsePathStyle = true
				o.BaseEndpoint = aws.String(handler.AccountURL)
			}
		})
		return &AWSCloudStorageProxy{s3ServicesClient: client}, nil
	}
}

func (handler ProxyAuthHandlerAWSConfiguredIdentity) createProxy() (CloudStorageProxy, error) {
	awsConfig, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(handler.AccessID, handler.AccessKey, "")),
	)
	if err != nil {
		return nil, wrapError("unable to create AWS service client", err)
	} else {
		client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
			if handler.AccountURL != "" {
				o.UsePathStyle = true
				o.BaseEndpoint = aws.String(handler.AccountURL)
			}
		})
		return &AWSCloudStorageProxy{s3ServicesClient: client}, nil
	}
}

func (aw *AWSCloudStorageProxy) listFilesOrFolders(ctx context.Context, containerName string, maxNumber int,
	prefix string, listType blobListType) ([]string, error) {
	if maxNumber <= 0 {
		maxNumber = max_RESULT
	}

	itemList := make([]string, 0)
	isTruncated := false
	continuationToken := ""
	for {
		var token *string
		if continuationToken != "" {
			token = aws.String(continuationToken)
		}
		result, err := aw.s3ServicesClient.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(containerName),
			MaxKeys:           aws.Int32(int32(maxNumber)),
			Prefix:            aws.String(prefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: token,
		})
		if err != nil {
			return itemList, wrapError("unable to list contents of bucket "+containerName, err)
		} else {
			isTruncated = *result.IsTruncated
			if isTruncated {
				continuationToken = *result.NextContinuationToken
			}
			if listType == listTypeFile {
				for _, obj := range result.Contents {
					if len(itemList) < maxNumber {
						itemList = append(itemList, *obj.Key)
					} else {
						break
					}
				}
			} else {
				for _, obj := range result.CommonPrefixes {
					if len(itemList) < maxNumber {
						itemList = append(itemList, *obj.Prefix)
					} else {
						break
					}
				}
			}
		}
		if !isTruncated || len(itemList) >= maxNumber {
			break
		}
	}
	return itemList, nil
}

func (aw *AWSCloudStorageProxy) ListFiles(ctx context.Context, containerName string, maxNumber int,
	prefix string) ([]string, error) {
	return aw.listFilesOrFolders(ctx, containerName, maxNumber, prefix, listTypeFile)
}

func (aw *AWSCloudStorageProxy) ListFolders(ctx context.Context, containerName string, maxNumber int,
	prefix string) ([]string, error) {
	return aw.listFilesOrFolders(ctx, containerName, maxNumber, prefix, listTypeFolder)
}
func (aw *AWSCloudStorageProxy) getFileContentAndMetadata(ctx context.Context, containerName string, fileName string,
	includeMetadata bool) (string, map[string]string, error) {
	var metadata map[string]string
	resp, err := aw.s3ServicesClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(containerName),
		Key:    aws.String(fileName),
	})
	if err == nil {
		if includeMetadata {
			metadata = resp.Metadata
			metadata["last_modified"] = resp.LastModified.Format(time_FORMAT)
			metadata["content_length"] = strconv.Itoa(int(*resp.ContentLength))
		}

		defer resp.Body.Close()
		content, er := io.ReadAll(resp.Body)
		if er != nil {
			return "", metadata, wrapError("unable to read message body of file "+fileName, er)
		}
		return string(content), metadata, nil
	}
	return "", metadata, wrapError("unable to get file "+fileName, err)
}

func (aw *AWSCloudStorageProxy) GetFile(ctx context.Context, containerName string, fileName string) (CloudFile, error) {
	content, metadata, err := aw.getFileContentAndMetadata(ctx, containerName, fileName, true)
	cloudFile := CloudFile{
		Container: containerName,
		FileName:  fileName,
		Metadata:  metadata,
		Content:   content,
	}
	return cloudFile, err
}

func (aw *AWSCloudStorageProxy) GetFileContent(ctx context.Context, containerName string, fileName string) (string, error) {
	content, _, err := aw.getFileContentAndMetadata(ctx, containerName, fileName, false)
	return content, err
}

func (aw *AWSCloudStorageProxy) GetFileContentAsInputStream(ctx context.Context, containerName string, fileName string) (io.ReadCloser, error) {
	resp, err := aw.s3ServicesClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(containerName),
		Key:    aws.String(fileName),
	})
	if err == nil {
		return resp.Body, nil
	}
	return nil, wrapError("unable to get stream reader for file "+fileName, err)
}

func (aw *AWSCloudStorageProxy) GetMetadata(ctx context.Context, containerName string,
	fileName string) (map[string]string, error) {
	resp, err := aw.s3ServicesClient.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(containerName),
		Key:    aws.String(fileName),
	})
	if err == nil {
		metadata := resp.Metadata
		metadata["last_modified"] = resp.LastModified.Format(time_FORMAT)
		metadata["content_length"] = strconv.Itoa(int(*resp.ContentLength))
		return metadata, nil
	} else {
		return nil, wrapError("unable to get metadata for object "+fileName, err)
	}
}

func (aw *AWSCloudStorageProxy) SaveFileFromText(ctx context.Context, containerName string, fileName string,
	metadata map[string]string, content string) error {
	contentReader := strings.NewReader(content)
	_, err := aw.s3ServicesClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(containerName),
		Key:      aws.String(fileName),
		Body:     contentReader,
		Metadata: metadata,
	})
	if err != nil {
		return wrapError("Could not upload file "+fileName, err)
	}
	return nil
}

func (aw *AWSCloudStorageProxy) SaveFileFromInputStream(ctx context.Context, containerName string, fileName string, metadata map[string]string,
	inputStream io.Reader, fileSizeBytes int64, concurrency int) error {
	var uploader *manager.Uploader
	var partSize int64
	partSize = size_5MiB
	if concurrency <= 0 {
		concurrency = 5
	}
	if fileSizeBytes > size_5MiB*max_PARTS {
		// we need to increase the Part size
		partSize = fileSizeBytes / max_PARTS
	}
	uploader = manager.NewUploader(aw.s3ServicesClient, func(u *manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
		u.BufferProvider = manager.NewBufferedReadSeekerWriteToPool(int(partSize))
	})

	_, err := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(containerName),
		Key:      aws.String(fileName),
		Body:     inputStream,
		Metadata: metadata,
	})
	if err != nil {
		return wrapError("unable to upload file "+fileName, err)
	}
	return nil
}

func (aw *AWSCloudStorageProxy) DeleteFile(ctx context.Context, containerName string, fileName string) error {
	_, err := aw.s3ServicesClient.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(containerName),
		Key:    aws.String(fileName),
	})
	if err != nil {
		return wrapError("unable to delete file "+fileName, err)
	}
	return nil
}
