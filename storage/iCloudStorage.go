package storage

import (
	"fmt"
	"golang.org/x/net/context"
	"io"
	"time"
)

const max_RESULT int = 500
const time_FORMAT string = time.RFC3339
const size_5MiB = 5 * 1024 * 1024
const max_PARTS = 10000
const size_LARGEOBJECT = 50 * 1024 * 1024

type CloudStorageProxy interface {
	ListFiles(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error)
	ListFolders(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error)
	GetFile(ctx context.Context, containerName string, fileName string) (CloudFile, error)
	GetFileContentAsString(ctx context.Context, containerName string, fileName string) (string, error)
	GetFileContentAsInputStream(ctx context.Context, containerName string, fileName string) (io.ReadCloser, error)
	GetLargeFileAsByteArray(ctx context.Context, containerName string, fileName string, fileSize int64, concurrency int) ([]byte, error)
	GetMetadata(ctx context.Context, containerName string, fileName string) (map[string]string, error)
	UploadFileFromString(ctx context.Context, containerName string, fileName string, metadata map[string]string,
		content string) error
	UploadFileFromInputStream(ctx context.Context, containerName string, fileName string, metadata map[string]string,
		inputStream io.Reader, fileSizeBytes int64, concurrency int) error
	DeleteFile(ctx context.Context, containerName string, fileName string) error
	// CopyFileToRemoteStorageContainer copies a blob from one container to another.
	// Used for copying to a destination container that is (a) in a different cloud environment and/or
	// (b) uses different credentials. Requires an initialized proxy to the destination.
	CopyFileToRemoteStorageContainer(ctx context.Context, sourceContainer string, sourceFile string,
		destContainer string, destFile string, destinationProxy *CloudStorageProxy, concurrency int) error
	// CopyFileToLocalStorageContainer copies a blob from one container to another.
	// Used for copying to a destination container that is in the same cloud environment and
	// uses the same credentials as the source container.
	CopyFileToLocalStorageContainer(ctx context.Context, sourceContainer string, sourceFile string,
		destContainer string, destFile string) error
}

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

func CloudStorageProxyFactory(handler ProxyAuthHandler) (CloudStorageProxy, error) {
	return handler.createProxy()
}
