package storage

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"golang.org/x/net/context"
	"time"
)

// implements iCloudStorageReader
const max_RESULT int = 500

type AzureCloudStorageProxy struct {
	blobServiceClient *azblob.Client
}

func wrapError(msg string, err error) *CloudStorageError {
	cloudError := CloudStorageError{message: msg, internalError: err}
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

//func (az *AzureCloudStorageProxy) GetFile(containerName string, fileName string) (CloudFile, error) {
//	//CloudFile(bucket, fileName, getMetadata(bucket, fileName), getFileContent(bucket, fileName))
//}
//
//func (az *AzureCloudStorageProxy) GetFileContent(containerName string, fileName string) (string, error) {
//
//}

// func (az AzureCloudStorage) GetFileContentAsInputStream(container string, fileName string) io.Reader {
// }
func (az *AzureCloudStorageProxy) GetMetadata(ctx context.Context, containerName string, fileName string) (map[string]string, error) {
	var cloudError *CloudStorageError
	props := make(map[string]string)
	blobClient := az.blobServiceClient.ServiceClient().NewContainerClient(containerName).NewBlobClient(fileName)
	resp, err := blobClient.GetProperties(ctx, nil)
	if err == nil {
		for key, value := range resp.Metadata {
			if value != nil {
				props[key] = *value
			} else {
				props[key] = ""
			}
		}
		props["last_modified"] = resp.LastModified.Format(time.DateTime)
	} else {
		cloudError = wrapError("Error getting blob metadata", err)
	}
	return props, cloudError
}

//func (az AzureCloudStorage) SaveFile(container string, file CloudFile) {}
//func (az AzureCloudStorage) SaveFileFromStream(container string, fileName string, content io.Reader,
//	size int64, metadata map[string]string) {
//}
//func (az AzureCloudStorage) DeleteFile(container string, fileName string) int {}
