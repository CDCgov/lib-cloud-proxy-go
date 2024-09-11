package storage

import (
	"fmt"
	"golang.org/x/net/context"
)

type CloudStorageProxy interface {
	ListFiles(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error)
	ListFolders(containerName string) ([]string, error)
	//GetFile(containerName string, fileName string) (CloudFile, InternalError)
	//GetFileContent(containerName string, fileName string) (string, InternalError)
	//GetFileContentAsInputStream(containerName string, fileName string) (io.Reader, InternalError)
	//GetMetadata(containerName string, fileName string, urlDecode bool) (map[string]string, InternalError)
	//SaveFile(containerName string, file CloudFile) InternalError
	//SaveFileFromStream(containerName string, fileName string, content io.Reader,
	//	size int64, metadata map[string]string) InternalError
	//DeleteFile(containerName string, fileName string) (int, InternalError)
}

type CloudStorageError struct {
	Message       string
	InternalError error
}

func (err *CloudStorageError) Error() string {
	return fmt.Sprintf("CloudStorage Error: %s", err.Message)
}

func (err *CloudStorageError) Unwrap() error {
	return err.InternalError
}
