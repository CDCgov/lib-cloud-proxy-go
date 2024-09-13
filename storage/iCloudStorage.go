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
	SaveFileFromText(ctx context.Context, containerName string, fileName string, metadata map[string]string,
		content string) error
	SaveFileFromInputStream(ctx context.Context, containerName string, fileName string, metadata map[string]string,
		inputStream io.Reader) error
	DeleteFile(ctx context.Context, containerName string, fileName string) error
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
			return proxy, err
		}
	default:
		return nil, errors.New("unknown cloud storage type")
	}
}
