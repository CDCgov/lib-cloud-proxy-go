package storage

import (
	"fmt"
	"golang.org/x/net/context"
	"io"
	"strconv"
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
	GetLargeFileContentAsByteArray(ctx context.Context, containerName string, fileName string, fileSize int64, concurrency int) ([]byte, error)
	GetMetadata(ctx context.Context, containerName string, fileName string) (map[string]string, error)
	UploadFileFromString(ctx context.Context, containerName string, fileName string, metadata map[string]string,
		content string) error
	UploadFileFromInputStream(ctx context.Context, containerName string, fileName string, metadata map[string]string,
		inputStream io.Reader, fileSizeBytes int64, concurrency int) error
	DeleteFile(ctx context.Context, containerName string, fileName string) error
	GetSourceBlobSignedURL(ctx context.Context, containerName string, fileName string) (string, error)
	GetDestBlobSignedURL(ctx context.Context, containerName string, fileName string) (string, error)
	CopyFileFromSignedURL(ctx context.Context, sourceBlobSignedURL string, destContainer string,
		destFile string, metadata map[string]string) error
	CopyFileToSignedURL(ctx context.Context, sourceContainer string, sourceFile string,
		destSignedURL string, metadata map[string]string) error
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

func getStringAsInt64(number string) int64 {
	length, _ := strconv.ParseInt(number, 10, 64)
	return length
}
