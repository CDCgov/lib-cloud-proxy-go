package storage

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type AWSCloudStorageProxy struct {
	s3ServicesClient *s3.Client
}

func newAWSCloudStorageProxyFromIdentity(accountURL string) (*AWSCloudStorageProxy, error) {
	awsConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, wrapError("unable to create AWS service client", err)
	} else {
		client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
			if accountURL != "" {
				o.UsePathStyle = true
				o.BaseEndpoint = aws.String(accountURL)
			}
		})
		return &AWSCloudStorageProxy{s3ServicesClient: client}, nil
	}
}

func (aw *AWSCloudStorageProxy) ListFiles(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error) {
	if maxNumber <= 0 {
		maxNumber = max_RESULT
	}

	fileList := make([]string, 0)
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
			ContinuationToken: token,
		})
		if err != nil {
			return fileList, wrapError("unable to list contents of bucket "+containerName, err)
		} else {
			isTruncated = *result.IsTruncated
			if isTruncated {
				continuationToken = *result.NextContinuationToken
			}
			for _, obj := range result.Contents {
				fileList = append(fileList, *obj.Key)
			}
		}
		if !isTruncated {
			break
		}
	}
	return fileList, nil
}

//ListFolders(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error)
//GetFile(ctx context.Context, containerName string, fileName string) (CloudFile, error)
//GetFileContent(ctx context.Context, containerName string, fileName string) (string, error)
//GetFileContentAsInputStream(ctx context.Context, containerName string, fileName string) (io.Reader, error)
//GetMetadata(ctx context.Context, containerName string, fileName string) (map[string]string, error)
//SaveFileFromText(ctx context.Context, containerName string, fileName string, metadata map[string]string,
//content string) error
//SaveFileFromInputStream(ctx context.Context, containerName string, fileName string, metadata map[string]string,
//inputStream io.Reader) error
//DeleteFile(ctx context.Context, containerName string, fileName string) error
