package storage

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	blob2 "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"golang.org/x/net/context"
)

//implements iCloudStorageReader

type AzureCloudStorageProxy struct {
	blobServiceClient *azblob.Client
}

func wrapError(msg string, err error) *CloudStorageError {
	cloudError := CloudStorageError{message: msg, error: err}
	return &cloudError
}
func getBlobServiceClient(accountURL string, useManagedIdentity bool, connectionString string) (*azblob.Client, error) {
	// Create a new service client with token credential
	var cloudError *CloudStorageError
	if useManagedIdentity {
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err == nil {
			client, er := azblob.NewClient(accountURL, credential, nil)
			if er == nil {
				return client, err
			} else {
				cloudError = wrapError("InitializeServiceClient unable to create Azure blob service client", er)
			}
		} else {
			cloudError = wrapError("InitializeServiceClient unable to obtain managed identity credential", err)
		}
	} else {
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		if err == nil {
			return client, err
		} else {
			cloudError = wrapError("InitializeServiceClient unable to create Azure blob service client from "+
				"connection string", err)
		}
	}
	return nil, cloudError
}

func NewAzureCloudStorageProxyFromIdentity(accountURL string) (*AzureCloudStorageProxy, error) {
	blobClient, err := getBlobServiceClient(accountURL, true, "")
	if err == nil {
		return &AzureCloudStorageProxy{blobServiceClient: blobClient}, nil
	}
	return nil, err
}

func NewAzureCloudStorageProxyFromConnectionString(connectionString string) (*AzureCloudStorageProxy, error) {
	blobClient, err := getBlobServiceClient("", false, connectionString)
	if err == nil {
		return &AzureCloudStorageProxy{blobServiceClient: blobClient}, nil
	}
	return nil, err
}

func (az *AzureCloudStorageProxy) ListFiles(ctx context.Context, containerName string,
	maxNumber int32, prefix string) ([]string, error) {
	if maxNumber <= 0 {
		maxNumber = 5000
	}
	if prefix == "" {
		prefix = "/"
	}
	var cloudError *CloudStorageError
	fileList := make([]string, 0)
	pager := az.blobServiceClient.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include:    azblob.ListBlobsInclude{Deleted: false, Versions: false},
		MaxResults: &maxNumber,
		Prefix:     &prefix,
	})
	maxReached := false
	for pager.More() && !maxReached {
		resp, err := pager.NextPage(ctx)
		if err == nil {
			for _, blob := range resp.Segment.BlobItems {
				if *blob.Properties.BlobType == blob2.BlobTypeBlockBlob {
					if len(fileList) < int(maxNumber) {
						fileList = append(fileList, *blob.Name)
					} else {
						maxReached = true
						break
					}
				}
			}
		} else {
			cloudError = wrapError(fmt.Sprintf("ListFiles error retrieving paged results from %s", containerName), err)
			break
		}
	}
	return fileList, cloudError
}

//func (az AzureCloudStorage) ListFolders(container string) []string {
//
//}
//func (az AzureCloudStorage) GetFile(container string, fileName string) CloudFile     {}
//func (az AzureCloudStorage) GetFileContent(container string, fileName string) string {}
//func (az AzureCloudStorage) GetFileContentAsInputStream(container string, fileName string) io.Reader {
//}
//func (az AzureCloudStorage) GetMetadata(container string, fileName string, urlDecode bool) map[string]string {
//}
//func (az AzureCloudStorage) SaveFile(container string, file CloudFile) {}
//func (az AzureCloudStorage) SaveFileFromStream(container string, fileName string, content io.Reader,
//	size int64, metadata map[string]string) {
//}
//func (az AzureCloudStorage) DeleteFile(container string, fileName string) int {}
