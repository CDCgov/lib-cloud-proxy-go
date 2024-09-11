package storage

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"golang.org/x/net/context"
)

// implements iCloudStorageReader
const max_RESULT int = 500

type AzureCloudStorageProxy struct {
	blobServiceClient *azblob.Client
}

func wrapError(msg string, err error) *CloudStorageError {
	cloudError := CloudStorageError{Message: msg, InternalError: err}
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

type blobListType string

const (
	listTypeFile   blobListType = "FILE"
	listTypeFolder blobListType = "FOLDER"
)

func (az *AzureCloudStorageProxy) listFilesOrFolders(ctx context.Context, containerName string,
	maxNumber int, prefix string, listType blobListType) ([]string, error) {
	if maxNumber <= 0 {
		maxNumber = max_RESULT
	}
	var cloudError *CloudStorageError
	resultsList := make([]string, 0)
	containerClient := az.blobServiceClient.ServiceClient().NewContainerClient(containerName)
	pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
		Include: azblob.ListBlobsInclude{Metadata: true},
		Prefix:  &prefix,
	})
	maxReached := false
	for pager.More() && !maxReached {
		resp, err := pager.NextPage(ctx)
		if err == nil {
			if listType == listTypeFile {
				for _, file := range resp.Segment.BlobItems {
					if len(resultsList) < maxNumber {
						resultsList = append(resultsList, *file.Name)
					} else {
						maxReached = true
						break
					}
				}
			} else {
				for _, folder := range resp.Segment.BlobPrefixes {
					if len(resultsList) < maxNumber {
						resultsList = append(resultsList, *folder.Name)
					} else {
						maxReached = true
						break
					}
				}
			}
		} else {
			cloudError = wrapError("Error listing contents of container", err)
		}
	}

	return resultsList, cloudError
}

func (az *AzureCloudStorageProxy) ListFiles(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error) {
	return az.listFilesOrFolders(ctx, containerName, maxNumber, prefix, listTypeFile)
}

func (az *AzureCloudStorageProxy) ListFolders(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error) {
	return az.listFilesOrFolders(ctx, containerName, maxNumber, prefix, listTypeFolder)
}

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
