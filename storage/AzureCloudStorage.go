package storage

import (
	"bytes"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"golang.org/x/net/context"
	"lib-cloud-proxy-go/util"
	"time"
)

// implements iCloudStorageReader
const max_RESULT int = 500
const time_FORMAT string = time.RFC3339

type AzureCloudStorageProxy struct {
	blobServiceClient *azblob.Client
}

func wrapError(msg string, err error) *CloudStorageError {
	return &CloudStorageError{message: msg, internalError: err}
}

func getBlobServiceClient(accountURL string, useManagedIdentity bool, connectionString string) (*azblob.Client, error) {
	// Create a new service client with token credential
	if useManagedIdentity {
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err == nil {
			client, er := azblob.NewClient(accountURL, credential, nil)
			if er == nil {
				return client, err
			} else {
				return client, wrapError("InitializeServiceClient unable to create Azure blob service client", er)
			}
		} else {
			return nil, wrapError("InitializeServiceClient unable to obtain managed identity credential", err)
		}
	} else {
		client, err := azblob.NewClientFromConnectionString(connectionString, nil)
		if err == nil {
			return client, err
		} else {
			return client, wrapError("InitializeServiceClient unable to create Azure blob service client from "+
				"connection string", err)
		}
	}
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
			return resultsList, wrapError("Error listing contents of container", err)
		}
	}

	return resultsList, nil
}

func (az *AzureCloudStorageProxy) ListFiles(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error) {
	return az.listFilesOrFolders(ctx, containerName, maxNumber, prefix, listTypeFile)
}

func (az *AzureCloudStorageProxy) ListFolders(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error) {
	return az.listFilesOrFolders(ctx, containerName, maxNumber, prefix, listTypeFolder)
}

func (az *AzureCloudStorageProxy) GetFile(ctx context.Context, containerName string, fileName string) (CloudFile, error) {
	content, metadata, err := az.getFileContentAndMetadata(ctx, containerName, fileName)
	file := CloudFile{Bucket: containerName,
		FileName: fileName,
		Metadata: metadata,
		Content:  content}
	return file, err
}

func (az *AzureCloudStorageProxy) GetFileContent(ctx context.Context, containerName string, fileName string) (string, error) {
	content, _, err := az.getFileContentAndMetadata(ctx, containerName, fileName)
	return content, err
}

func (az *AzureCloudStorageProxy) getFileContentAndMetadata(ctx context.Context, containerName string,
	fileName string) (string, map[string]string, error) {

	metadata := make(map[string]string)
	streamResp, err := az.blobServiceClient.DownloadStream(ctx, containerName, fileName, nil)
	if err != nil {
		return "", metadata, wrapError("Unable to get file content", err)
	} else {
		metadata = readMetadata(streamResp.Metadata)
		metadata["last_modified"] = streamResp.LastModified.Format(time_FORMAT)
		data := bytes.Buffer{}
		retryReader := streamResp.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
		_, err := data.ReadFrom(retryReader)
		if err != nil {
			return data.String(), metadata, wrapError("Error occurred while reading data", err)
		}
		return data.String(), metadata, nil
	}
}

// func (az AzureCloudStorage) GetFileContentAsInputStream(container string, fileName string) io.Reader {
// }
func (az *AzureCloudStorageProxy) GetMetadata(ctx context.Context, containerName string, fileName string) (map[string]string, error) {
	props := make(map[string]string)
	blobClient := az.blobServiceClient.ServiceClient().NewContainerClient(containerName).NewBlobClient(fileName)
	resp, err := blobClient.GetProperties(ctx, nil)
	if err == nil {
		props = readMetadata(resp.Metadata)
		props["last_modified"] = resp.LastModified.Format(time_FORMAT)
	} else {
		return props, wrapError("Error getting blob metadata", err)
	}
	return props, nil
}

func readMetadata(metadata map[string]*string) map[string]string {
	props := make(map[string]string)
	for key, value := range metadata {
		if value != nil {
			props[util.NormalizeString(key)] = *value
		} else {
			props[util.NormalizeString(key)] = ""
		}
	}
	return props
}

//func (az AzureCloudStorage) SaveFile(container string, file CloudFile) {}
//func (az AzureCloudStorage) SaveFileFromStream(container string, fileName string, content io.Reader,
//	size int64, metadata map[string]string) {
//}
//func (az AzureCloudStorage) DeleteFile(container string, fileName string) int {}
