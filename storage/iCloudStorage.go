package storage

import (
	"errors"
	"fmt"
	"golang.org/x/net/context"
	"io"
	"time"
)

const max_RESULT int = 500
const time_FORMAT string = time.RFC3339
const size_5MiB = 5242880
const max_PARTS = 10000

type CloudStorageProxy interface {
	ListFiles(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error)
	ListFolders(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error)
	//GetFile(ctx context.Context, containerName string, fileName string) (CloudFile, error)
	//GetFileContent(ctx context.Context, containerName string, fileName string) (string, error)
	//GetFileContentAsInputStream(ctx context.Context, containerName string, fileName string) (io.Reader, error)
	//GetMetadata(ctx context.Context, containerName string, fileName string) (map[string]string, error)
	SaveFileFromText(ctx context.Context, containerName string, fileName string, metadata map[string]string,
		content string) error
	SaveFileFromInputStream(ctx context.Context, containerName string, fileName string, metadata map[string]string,
		inputStream io.Reader, fileSizeBytes int64) error
	DeleteFile(ctx context.Context, containerName string, fileName string) error
}

type CloudStorageType string

const (
	CloudStorageTypeAzure CloudStorageType = "AZURE_STORAGE"
	CloudStorageTypeAWSS3 CloudStorageType = "AWS_S3"
)

type blobListType string

const (
	listTypeFile   blobListType = "FILE"
	listTypeFolder blobListType = "FOLDER"
)

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

func wrapError(msg string, err error) *CloudStorageError {
	return &CloudStorageError{message: msg, internalError: err}
}

type CloudStorageConnectionOptions struct {
	UseManagedIdentity  bool
	UseConnectionString bool
	UseSASToken         bool
	AccountURL          string
	ConnectionString    string
	URLWithSASToken     string
}

func CloudStorageProxyFactory(cloudStorageType CloudStorageType, options CloudStorageConnectionOptions) (CloudStorageProxy, error) {
	if options.UseManagedIdentity && options.AccountURL == "" {
		return nil, errors.New("if using managed identity, AccountURL is required")
	} else if options.UseConnectionString && options.ConnectionString == "" {
		return nil, errors.New("if using connection string, ConnectionString is required")
	} else if options.UseSASToken && options.URLWithSASToken == "" {
		return nil, errors.New("if using SAS token, URLWithSASToken is required")
	}
	var err error
	var proxy CloudStorageProxy
	switch cloudStorageType {
	case CloudStorageTypeAzure:
		{
			switch {
			case options.UseManagedIdentity:
				proxy, err = newAzureCloudStorageProxyFromIdentity(options.AccountURL)
			case options.UseConnectionString:
				proxy, err = newAzureCloudStorageProxyFromConnectionString(options.ConnectionString)
			case options.UseSASToken:
				proxy, err = newAzureCloudStorageProxyFromSASToken(options.URLWithSASToken)
			default:
				return nil, errors.New("one of UseManagedIdentity, UseConnectionString, or UseSASToken must be true")
			}
		}
	case CloudStorageTypeAWSS3:
		{
			switch {
			case options.UseManagedIdentity:
				proxy, err = newAWSCloudStorageProxyFromIdentity(options.AccountURL)
			default:
				return nil, errors.New("unsupported configuration for AWS client")
			}

		}
	default:
		return nil, errors.New("unknown cloud storage type")
	}
	return proxy, err
}
