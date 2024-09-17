package storage

import (
	"bytes"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"golang.org/x/net/context"
	"io"
	"lib-cloud-proxy-go/util"
	"strings"
)

type AzureCloudStorageProxy struct {
	blobServiceClient *azblob.Client
}

func wrapError(msg string, err error) *CloudStorageError {
	return &CloudStorageError{message: msg, internalError: err}
}

func newAzureCloudStorageProxyFromIdentity(accountURL string) (*AzureCloudStorageProxy, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err == nil {
		client, er := azblob.NewClient(accountURL, credential, nil)
		if er == nil {
			return &AzureCloudStorageProxy{blobServiceClient: client}, nil
		} else {
			return nil, wrapError("unable to create Azure blob service client", er)
		}
	}
	return nil, err
}

func newAzureCloudStorageProxyFromConnectionString(connectionString string) (*AzureCloudStorageProxy, error) {
	client, err := azblob.NewClientFromConnectionString(connectionString, nil)
	if err == nil {
		return &AzureCloudStorageProxy{blobServiceClient: client}, nil
	} else {
		return nil, wrapError("unable to create Azure blob service client from "+
			"connection string", err)
	}
}

func newAzureCloudStorageProxyFromSASToken(token string) (*AzureCloudStorageProxy, error) {
	client, err := azblob.NewClientWithNoCredential(token, nil)
	if err == nil {
		return &AzureCloudStorageProxy{blobServiceClient: client}, nil
	} else {
		return nil, wrapError("unable to create Azure blob service client from "+
			"url with SAS token", err)
	}
}

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
	file := CloudFile{Container: containerName,
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

func (az *AzureCloudStorageProxy) GetFileContentAsInputStream(ctx context.Context, containerName string, fileName string) (io.Reader, error) {
	streamResp, err := az.blobServiceClient.DownloadStream(ctx, containerName, fileName, nil)
	if err == nil {
		return streamResp.NewRetryReader(ctx, &azblob.RetryReaderOptions{}), nil
	} else {
		return nil, err
	}
}

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

func writeMetadata(metadata map[string]string) map[string]*string {
	props := make(map[string]*string)
	for key, value := range metadata {
		props[key] = to.Ptr(value)
	}
	return props
}

func (az *AzureCloudStorageProxy) SaveFileFromText(ctx context.Context, containerName string, fileName string,
	metadata map[string]string, content string) error {
	contentReader := strings.NewReader(content)
	_, err := az.blobServiceClient.UploadStream(ctx, containerName, fileName, contentReader, &azblob.UploadStreamOptions{
		Metadata: writeMetadata(metadata),
	})
	if err != nil {
		return wrapError("unable to save file from text", err)
	} else {
		return nil
	}
}

func (az *AzureCloudStorageProxy) SaveFileFromInputStream(ctx context.Context, containerName string, fileName string, metadata map[string]string,
	inputStream io.Reader, fileSizeBytes int64) error {
	_, err := az.blobServiceClient.UploadStream(ctx, containerName, fileName, inputStream, &azblob.UploadStreamOptions{
		Concurrency: 5,
		Metadata:    writeMetadata(metadata),
	})
	if err != nil {
		return wrapError("unable to save file from input stream", err)
	} else {
		return nil
	}
}

func (az *AzureCloudStorageProxy) DeleteFile(ctx context.Context, containerName string, fileName string) error {
	_, err := az.blobServiceClient.DeleteBlob(ctx, containerName, fileName, nil)
	if err != nil {
		return wrapError("unable to delete blob", err)
	}
	return nil
}
