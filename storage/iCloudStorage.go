package storage

import (
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"io"
)

type CloudStorageProxy interface {
	ListFiles(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error)
	ListFolders(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error)
	GetFile(ctx context.Context, containerName string, fileName string) (CloudFile, error)
	GetFileContent(ctx context.Context, containerName string, fileName string) (string, error)
	GetFileContentAsInputStream(ctx context.Context, containerName string, fileName string) (io.Reader, error)
	GetMetadata(ctx context.Context, containerName string, fileName string) (map[string]string, error)
	//SaveFile(containerName string, file CloudFile) error
	//SaveFileFromStream(containerName string, fileName string, content io.Reader,
	//	size int64, metadata map[string]string) error
	//DeleteFile(containerName string, fileName string) (int, error)
}

type CloudStorageError struct {
	message       string
	internalError error
}

func (err *CloudStorageError) Error() string {
	return fmt.Sprintf("CloudStorage Error: %s", err.message)
}

func (err *CloudStorageError) Unwrap() error {
	return err.internalError
}

type CloudStorageType string

const (
	CloudStorageTypeAzure CloudStorageType = "AZURE_STORAGE"
	//CloudStorageTypeAWSS3 CloudStorageType = "AWS_S3"
)

type CloudStorageConnectionOptions struct {
	UseManagedIdentity bool
	AccountURL         string
	ConnectionString   string
}

func CloudStorageProxyFactory(cloudStorageType CloudStorageType, options CloudStorageConnectionOptions) (CloudStorageProxy, error) {
	if options.UseManagedIdentity && options.AccountURL == "" {
		return nil, errors.New("if using managed identity, AccountURL is required")
	} else if !options.UseManagedIdentity && options.ConnectionString == "" {
		return nil, errors.New("if not using managed identity, ConnectionString is required")
	}
	var err error
	var proxy CloudStorageProxy
	switch cloudStorageType {
	case CloudStorageTypeAzure:
		{
			if options.UseManagedIdentity {
				proxy, err = newAzureCloudStorageProxyFromIdentity(options.AccountURL)
			} else {
				proxy, err = newAzureCloudStorageProxyFromConnectionString(options.ConnectionString)
			}
			return proxy, err
		}
	default:
		return nil, nil
	}
}
