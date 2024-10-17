package storage

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/google/uuid"
	"golang.org/x/net/context"
	"io"
	"lib-cloud-proxy-go/util"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AzureCloudStorageProxy struct {
	blobServiceClient *azblob.Client
}

func (handler ProxyAuthHandlerAzureDefaultIdentity) createProxy() (CloudStorageProxy, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err == nil {
		return createProxyFromCredential(handler.AccountURL, credential)
	}
	return nil, err
}

func (handler ProxyAuthHandlerAzureClientSecretIdentity) createProxy() (CloudStorageProxy, error) {
	credential, err := azidentity.NewClientSecretCredential(handler.TenantID, handler.ClientID,
		handler.ClientSecret, nil)
	if err == nil {
		return createProxyFromCredential(handler.AccountURL, credential)
	}
	return nil, err
}

func createProxyFromCredential(accountURL string, credential azcore.TokenCredential) (CloudStorageProxy, error) {
	client, err := azblob.NewClient(accountURL, credential, nil)
	if err == nil {
		return &AzureCloudStorageProxy{blobServiceClient: client}, nil
	}
	return nil, wrapError("unable to create Azure Storage service client", err)

}

func (handler ProxyAuthHandlerAzureConnectionString) createProxy() (CloudStorageProxy, error) {
	client, err := azblob.NewClientFromConnectionString(handler.ConnectionString, nil)
	if err == nil {
		return &AzureCloudStorageProxy{blobServiceClient: client}, nil
	}
	return nil, wrapError("unable to create Azure Storage service client", err)
}

func (handler ProxyAuthHandlerAzureSASToken) createProxy() (CloudStorageProxy, error) {
	accountNameTmp, _ := strings.CutPrefix(handler.AccountURL, "https://")
	accountName := strings.Split(accountNameTmp, ".blob")[0]

	cred, _ := azblob.NewSharedKeyCredential(accountName, handler.AccountKey)
	credClient, err := azblob.NewClientWithSharedKeyCredential(handler.AccountURL, cred, nil)
	sasURL, err := credClient.ServiceClient().GetSASURL(
		sas.AccountResourceTypes{
			Service:   true,
			Container: true,
			Object:    true,
		},
		sas.AccountPermissions{
			Read:                  true,
			Write:                 true,
			Delete:                true,
			DeletePreviousVersion: true,
			PermanentDelete:       true,
			List:                  true,
			Add:                   true,
			Create:                true,
			Update:                true,
			Process:               true,
			Tag:                   true,
		},
		time.Now().Add(time.Duration(handler.ExpirationHours)*time.Hour),
		nil,
	)

	client, err := azblob.NewClientWithNoCredential(sasURL, nil)
	if err == nil {
		return &AzureCloudStorageProxy{blobServiceClient: client}, nil
	}
	return nil, wrapError("unable to create Azure Storage service client", err)
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

func (az *AzureCloudStorageProxy) GetFileContentAsString(ctx context.Context, containerName string, fileName string) (string, error) {
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
		metadata["content_length"] = strconv.Itoa(int(*streamResp.ContentLength))
		data := bytes.Buffer{}
		retryReader := streamResp.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
		_, err := data.ReadFrom(retryReader)
		if err != nil {
			return data.String(), metadata, wrapError("Error occurred while reading data", err)
		}
		return data.String(), metadata, nil
	}
}

func (az *AzureCloudStorageProxy) GetFileContentAsInputStream(ctx context.Context, containerName string, fileName string) (io.ReadCloser, error) {
	streamResp, err := az.blobServiceClient.DownloadStream(ctx, containerName, fileName, nil)
	if err == nil {
		return streamResp.NewRetryReader(ctx, &azblob.RetryReaderOptions{}), nil
	} else {
		return nil, err
	}
}

func (az *AzureCloudStorageProxy) GetLargeFileContentAsByteArray(ctx context.Context, containerName string, fileName string, fileSize int64, concurrency int) ([]byte, error) {
	if concurrency <= 0 {
		concurrency = 5
	}
	if fileSize <= 0 {
		fileSize = 1
	}
	buffer := make([]byte, fileSize)
	blockBlobClient := az.blobServiceClient.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(fileName)
	numBytes, err := blockBlobClient.DownloadBuffer(ctx, buffer, &blob.DownloadBufferOptions{
		BlockSize:   size_5MiB,
		Concurrency: uint16(concurrency),
	})
	if err != nil {
		return nil, wrapError("unable to download to buffer", err)
	}
	if numBytes < fileSize {
		return nil, &CloudStorageError{
			message: fmt.Sprintf("bytes downloaded (%d) did not match file size (%d)", numBytes, fileSize),
		}
	}
	return buffer, nil
}

func (az *AzureCloudStorageProxy) GetMetadata(ctx context.Context, containerName string, fileName string) (map[string]string, error) {
	props := make(map[string]string)
	blobClient := az.blobServiceClient.ServiceClient().NewContainerClient(containerName).NewBlobClient(fileName)
	resp, err := blobClient.GetProperties(ctx, nil)
	if err == nil {
		props = readMetadata(resp.Metadata)
		props["last_modified"] = resp.LastModified.Format(time_FORMAT)
		props["content_length"] = strconv.Itoa(int(*resp.ContentLength))
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

func (az *AzureCloudStorageProxy) UploadFileFromString(ctx context.Context, containerName string, fileName string,
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

func (az *AzureCloudStorageProxy) UploadFileFromInputStream(ctx context.Context, containerName string, fileName string, metadata map[string]string,
	inputStream io.Reader, fileSizeBytes int64, concurrency int) error {
	if concurrency <= 0 {
		concurrency = 5
	}

	_, err := az.blobServiceClient.UploadStream(ctx, containerName, fileName, inputStream, &azblob.UploadStreamOptions{
		BlockSize:   size_5MiB,
		Concurrency: concurrency,
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

func (az *AzureCloudStorageProxy) copyFileFromSignedURL(ctx context.Context, sourceSignedURL string, destContainer string,
	destFile string, metadata map[string]string) error {
	destBlob := az.blobServiceClient.ServiceClient().NewContainerClient(destContainer).NewBlockBlobClient(destFile)

	_, e := destBlob.UploadBlobFromURL(ctx, sourceSignedURL,
		&blockblob.UploadBlobFromURLOptions{
			Metadata: writeMetadata(metadata),
		})

	if e != nil {
		return wrapError("unable to copy blob", e)
	}
	return nil
}

func (az *AzureCloudStorageProxy) GetSourceBlobSignedURL(ctx context.Context, containerName string, fileName string) (string, error) {
	sourceBlob := az.blobServiceClient.ServiceClient().NewContainerClient(containerName).NewBlockBlobClient(fileName)
	sourceURL, er := sourceBlob.GetSASURL(sas.BlobPermissions{
		Read:   true,
		Add:    true,
		Create: true,
		Write:  true,
	}, time.Now().Add(2*time.Hour), nil)
	if er != nil {
		return "", wrapError("unable to get signed url for source blob", er)
	}
	return sourceURL, nil
}
func (az *AzureCloudStorageProxy) CopyFileFromRemoteStorage(ctx context.Context, sourceContainer string, sourceFile string,
	destContainer string, destFile string, sourceProxy *CloudStorageProxy, concurrency int) error {
	// s3 to Azure, or other Azure storage account to Azure
	maxPartsAzure := int64(50000)
	s := *sourceProxy
	metadata, err := s.GetMetadata(ctx, sourceContainer, sourceFile)
	if err != nil {
		return err
	}
	length := getStringAsInt64(metadata["content_length"])
	url, er := s.GetSourceBlobSignedURL(ctx, sourceContainer, sourceFile)
	if er != nil {
		return er
	}
	if length < size_LARGEOBJECT {
		return az.copyFileFromSignedURL(ctx, url, destContainer, destFile, metadata)
	} else {
		var partSize int64 = size_5MiB
		if length > size_5MiB*maxPartsAzure {
			// we need to increase the Part size
			partSize = length / maxPartsAzure
		}
		numChunks := length / partSize
		blockBlobClient := az.blobServiceClient.ServiceClient().NewContainerClient(destContainer).NewBlockBlobClient(destFile)
		blockBase := uuid.New()
		blockIDs := make([]string, numChunks)
		var chunkNum int64
		var start int64 = 0
		var count int64 = partSize
		chunkIdMap := make(map[string]azblob.HTTPRange)
		for chunkNum = 0; chunkNum < numChunks; chunkNum++ {
			end := start + count
			if chunkNum == numChunks-1 {
				count = 0
			}
			chunkId := base64.StdEncoding.EncodeToString([]byte(blockBase.String() + fmt.Sprintf("%05d", chunkNum)))
			blockIDs[chunkNum] = chunkId
			chunkIdMap[chunkId] = azblob.HTTPRange{
				Offset: start,
				Count:  count,
			}
			start = end
		}
		wg := sync.WaitGroup{}
		errCh := make(chan error, 1)
		ctx, cancel := context.WithCancel(ctx)
		routines := 0
		defer cancel()
		for id := range chunkIdMap {
			wg.Add(1)
			routines++
			go func(chunkId string) {
				defer wg.Done()
				_, err := blockBlobClient.StageBlockFromURL(ctx, chunkId, url, &blockblob.StageBlockFromURLOptions{
					Range: chunkIdMap[chunkId],
				})

				if err != nil {
					select {
					case errCh <- err:
						// error was set
					default:
						// some other error is already set
					}
					cancel()
				}
			}(id)
			if routines >= concurrency {
				wg.Wait()
				routines = 0
			}
		}
		wg.Wait()
		select {
		case err = <-errCh:
			// there was an error during staging
			return wrapError("error staging blocks", err)
		default:
			// no error was encountered
		}
		_, err = blockBlobClient.CommitBlockList(ctx, blockIDs,
			&blockblob.CommitBlockListOptions{Metadata: writeMetadata(metadata)})
		if err != nil {
			return wrapError("unable to commit blocks", err)
		}
	}
	return nil
}

func (az *AzureCloudStorageProxy) CopyFileFromLocalStorage(ctx context.Context, sourceContainer string, sourceFile string,
	destContainer string, destFile string, concurrency int) error {
	var s CloudStorageProxy = az
	return az.CopyFileFromRemoteStorage(ctx, sourceContainer, sourceFile, destContainer, destFile, &s, concurrency)
}

func (az *AzureCloudStorageProxy) CreateContainerIfNotExists(ctx context.Context, containerName string) error {
	_, err := az.blobServiceClient.CreateContainer(ctx, containerName, nil)
	var respErr *azcore.ResponseError
	if err != nil && errors.As(err, &respErr) {
		if !bloberror.HasCode(respErr, bloberror.ContainerAlreadyExists) {
			return wrapError("could not create container "+containerName, err)
		}
	}
	return nil
}
