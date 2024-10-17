package storage

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AWSCloudStorageProxy struct {
	s3ServicesClient *s3.Client
}

func (handler ProxyAuthHandlerAWSDefaultIdentity) createProxy() (CloudStorageProxy, error) {
	awsConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, wrapError("unable to create S3 service client", err)
	}
	return createProxyFromConfig(handler.AccountURL, "", &awsConfig)

}

func (handler ProxyAuthHandlerAWSConfiguredIdentity) createProxy() (CloudStorageProxy, error) {
	awsConfig, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(handler.AccessID, handler.AccessKey, "")),
	)
	if err != nil {
		return nil, wrapError("unable to create S3 service client", err)
	}
	return createProxyFromConfig(handler.AccountURL, handler.Region, &awsConfig)
}

func createProxyFromConfig(accountURL string, accountRegion string, awsConfig *aws.Config) (CloudStorageProxy, error) {
	client := s3.NewFromConfig(*awsConfig, func(o *s3.Options) {
		if accountURL != "" {
			o.UsePathStyle = true
			o.BaseEndpoint = aws.String(accountURL)
		}
		if accountRegion != "" {
			o.Region = accountRegion
		}
	})
	return &AWSCloudStorageProxy{s3ServicesClient: client}, nil
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
		}
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

func (aw *AWSCloudStorageProxy) GetFileContentAsString(ctx context.Context, containerName string, fileName string) (string, error) {
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

func (aw *AWSCloudStorageProxy) GetLargeFileContentAsByteArray(ctx context.Context, containerName string, fileName string,
	fileSize int64, concurrency int) ([]byte, error) {
	if concurrency <= 0 {
		concurrency = 5
	}
	var partSize int64 = size_5MiB
	if fileSize > size_5MiB*max_PARTS {
		// we need to increase the Part size
		partSize = fileSize / max_PARTS
	}
	downloader := manager.NewDownloader(aw.s3ServicesClient, func(d *manager.Downloader) {
		d.PartSize = partSize
		d.Concurrency = concurrency
	})
	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(ctx, buffer, &s3.GetObjectInput{
		Bucket: aws.String(containerName),
		Key:    aws.String(fileName),
	})
	if err != nil {
		return buffer.Bytes(), wrapError("unable to download large file", err)
	}
	return buffer.Bytes(), nil
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
	}
	return nil, wrapError("unable to get metadata for object "+fileName, err)
}

func (aw *AWSCloudStorageProxy) UploadFileFromString(ctx context.Context, containerName string, fileName string,
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

func (aw *AWSCloudStorageProxy) UploadFileFromInputStream(ctx context.Context, containerName string, fileName string, metadata map[string]string,
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

func (aw *AWSCloudStorageProxy) GetSourceBlobSignedURL(ctx context.Context, containerName string, fileName string) (string, error) {
	presignClient := s3.NewPresignClient(aw.s3ServicesClient)
	request, err := presignClient.PresignGetObject(ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(containerName),
			Key:    aws.String(fileName),
		},
		func(options *s3.PresignOptions) {
			options.Expires = time.Hour
		},
	)
	if err != nil {
		return "", wrapError("could not obtain presigned url", err)
	}
	return request.URL, nil
}

func (aw *AWSCloudStorageProxy) CopyFileFromRemoteStorage(ctx context.Context, sourceContainer string, sourceFile string,
	destContainer string, destFile string, sourceProxy *CloudStorageProxy, concurrency int) error {
	// azure to s3 or different s3 account to s3
	if concurrency <= 0 {
		concurrency = 15
	}
	s := *sourceProxy
	metadata, err := s.GetMetadata(ctx, sourceContainer, sourceFile)
	if err != nil {
		return wrapError("unable to read source file metadata", err)
	}
	fileSize := getStringAsInt64(metadata["content_length"])
	if fileSize == 0 {
		fileSize = 1
	}
	var inputStream io.Reader
	if fileSize < size_LARGEOBJECT {
		inputStream, err = s.GetFileContentAsInputStream(ctx, sourceContainer, sourceFile)
		if err != nil {
			return wrapError("unable to read source file as stream", err)
		}
		if er := aw.UploadFileFromInputStream(ctx, destContainer, destFile, metadata, inputStream,
			fileSize, concurrency); er != nil {
			return er
		}
	} else {
		content, err := s.GetLargeFileContentAsByteArray(ctx, sourceContainer, sourceFile, fileSize, concurrency)
		if err != nil {
			return wrapError("unable to get large file as byte array", err)
		}
		if e := aw.doMultipartUpload(ctx, destContainer, destFile, metadata, content, concurrency); e != nil {
			return e
		}
		//inputStream = bytes.NewReader(content)
		//contentString := string(content)
		//if e := aw.UploadFileFromString(ctx, destContainer, destFile, metadata, contentString); e != nil {
		//	return e
		//}
	}

	return nil
}

func (aw *AWSCloudStorageProxy) doMultipartUpload(ctx context.Context, destContainer string, destFile string,
	metadata map[string]string, content []byte, concurrency int) error {
	lengthInt := len(content)
	length64 := int64(lengthInt)
	upload, err := aw.s3ServicesClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:   aws.String(destContainer),
		Key:      aws.String(destFile),
		Metadata: metadata,
	})
	if err != nil {
		return wrapError("unable to create multipart upload", err)
	}
	uploadId := *upload.UploadId
	var partSize int = size_5MiB
	if lengthInt > partSize*max_PARTS {
		// we need to increase the Part size
		partSize = lengthInt / max_PARTS
	}
	partSize64 := int64(partSize)
	// if this doesn't divide evenly, we will add the remainder to the final chunk.
	// otherwise, if we add another chunk for the remainder, it will be too small
	// and upload will fail
	numChunks := lengthInt / partSize

	var chunkNum int
	var start int64 = 0
	var count int64 = partSize64
	type chunkPart struct {
		start int64
		count int64
	}
	chunkIdMap := make(map[int]chunkPart)
	for chunkNum = 1; chunkNum <= numChunks; chunkNum++ {
		end := start + partSize64
		if chunkNum == numChunks {
			count = length64 - start
		}
		chunkIdMap[chunkNum] = chunkPart{
			start: start,
			count: count,
		}
		start = end + 1
	}
	wg := sync.WaitGroup{}
	errCh := make(chan error, 1)
	responseCh := make(chan types.CompletedPart, numChunks)
	completedParts := make([]types.CompletedPart, numChunks)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader := bytes.NewReader(content)
	routines := 0
	for chunkId, chunkOffset := range chunkIdMap {
		wg.Add(1)
		routines++
		go func(chunkId int, chunkOffset chunkPart) {
			defer wg.Done()
			uploadPartResp, err := aw.s3ServicesClient.UploadPart(ctx, &s3.UploadPartInput{
				Bucket:     aws.String(destContainer),
				Key:        aws.String(destFile),
				PartNumber: aws.Int32(int32(chunkId)),
				UploadId:   aws.String(uploadId),
				Body:       io.NewSectionReader(reader, chunkOffset.start, chunkOffset.count),
			})
			if err != nil {
				select {
				case errCh <- err:
					// error was set
				default:
					// some other error is already set
				}
				cancel()
			} else {
				responseCh <- types.CompletedPart{
					ETag:       uploadPartResp.ETag,
					PartNumber: aws.Int32(int32(chunkId)),
				}
			}
		}(chunkId, chunkOffset)
		if routines >= concurrency {
			wg.Wait()
			routines = 0
		}
	}
	wg.Wait()
	close(responseCh)
	select {
	case err = <-errCh:
		// there was an error during staging
		_, _ = aw.s3ServicesClient.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(destContainer),
			Key:      aws.String(destFile),
			UploadId: aws.String(uploadId),
		})
		return wrapError("error staging blocks; copy aborted", err)
	default:
		// no error was encountered
	}

	// arrange parts in ordered list
	for part := range responseCh {
		partNum := *part.PartNumber
		completedParts[partNum-1] = part
	}

	_, err = aw.s3ServicesClient.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(destContainer),
		Key:      aws.String(destFile),
		UploadId: aws.String(uploadId),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return wrapError("unable to complete multipart upload", err)
	}
	return nil
}

func (aw *AWSCloudStorageProxy) CopyFileFromLocalStorage(ctx context.Context, sourceContainer string, sourceFile string,
	destContainer string, destFile string, concurrency int) error {
	source := fmt.Sprintf("%s/%s", sourceContainer, sourceFile)
	metadata, e := aw.GetMetadata(ctx, sourceContainer, sourceFile)
	if e != nil {
		return e
	}
	length := getStringAsInt64(metadata["content_length"])
	if length < size_LARGEOBJECT {
		if _, err := aw.s3ServicesClient.CopyObject(ctx, &s3.CopyObjectInput{
			CopySource: aws.String(source),
			Bucket:     aws.String(destContainer),
			Key:        aws.String(destFile),
		}); err != nil {
			return wrapError("unable to copy object to S3 bucket", err)
		}
	} else {
		lengthInt := int(length)
		upload, err := aw.s3ServicesClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket:   aws.String(destContainer),
			Key:      aws.String(destFile),
			Metadata: metadata,
		})
		if err != nil {
			return wrapError("unable to create multipart upload", err)
		}
		uploadId := *upload.UploadId
		var partSize int = size_5MiB
		if lengthInt > partSize*max_PARTS {
			// we need to increase the Part size
			partSize = lengthInt / max_PARTS
		}
		numChunks := lengthInt / partSize
		//if lengthInt%partSize != 0 {
		//	numChunks++
		//}
		var chunkNum int
		var start = 0
		var end = 0
		chunkIdMap := make(map[int]string)
		for chunkNum = 1; chunkNum <= numChunks; chunkNum++ {
			end = start + partSize - 1
			if chunkNum == numChunks {
				end = lengthInt - 1
			}
			chunkIdMap[chunkNum] = fmt.Sprintf("bytes=%d-%d", start, end)
			start = end + 1
		}
		wg := sync.WaitGroup{}
		errCh := make(chan error, 1)
		responseCh := make(chan types.CompletedPart, numChunks)
		completedParts := make([]types.CompletedPart, numChunks)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		routines := 0
		for chunkId, rangeHeader := range chunkIdMap {
			wg.Add(1)
			routines++
			go func(chunkId int, rangeHeader string) {
				defer wg.Done()
				uploadPartResp, err := aw.s3ServicesClient.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:          aws.String(destContainer),
					CopySource:      aws.String(source),
					CopySourceRange: aws.String(rangeHeader),
					Key:             aws.String(destFile),
					PartNumber:      aws.Int32(int32(chunkId)),
					UploadId:        aws.String(uploadId),
				})
				if err != nil {
					select {
					case errCh <- err:
						// error was set
					default:
						// some other error is already set
					}
					cancel()
				} else {
					responseCh <- types.CompletedPart{
						ETag:       uploadPartResp.CopyPartResult.ETag,
						PartNumber: aws.Int32(int32(chunkId)),
					}
				}

			}(chunkId, rangeHeader)
			if routines >= concurrency {
				wg.Wait()
				routines = 0
			}
		}
		wg.Wait()
		close(responseCh)
		select {
		case err = <-errCh:
			// there was an error during staging
			_, _ = aw.s3ServicesClient.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(destContainer),
				Key:      aws.String(destFile),
				UploadId: aws.String(uploadId),
			})
			return wrapError("error staging blocks; copy aborted", err)
		default:
			// no error was encountered
		}

		// arrange parts in ordered list
		for part := range responseCh {
			partNum := *part.PartNumber
			completedParts[partNum-1] = part
		}

		_, err = aw.s3ServicesClient.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(destContainer),
			Key:      aws.String(destFile),
			UploadId: aws.String(uploadId),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: completedParts,
			},
		})
		if err != nil {
			return wrapError("unable to complete multipart upload", err)
		}
	}
	return nil
}
