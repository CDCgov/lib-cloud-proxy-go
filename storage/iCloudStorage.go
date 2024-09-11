package storage

import (
	"fmt"
	"golang.org/x/net/context"
)

type CloudStorageProxy interface {
	ListFiles(ctx context.Context, containerName string, maxNumber int, prefix string) ([]string, error)
	ListFolders(containerName string) ([]string, error)
	GetFile(containerName string, fileName string) (CloudFile, error)
	//GetFileContent(containerName string, fileName string) (string, error)
	//GetFileContentAsInputStream(containerName string, fileName string) (io.Reader, error)
	//GetMetadata(containerName string, fileName string, urlDecode bool) (map[string]string, error)
	//SaveFile(containerName string, file CloudFile) error
	//SaveFileFromStream(containerName string, fileName string, content io.Reader,
	//	size int64, metadata map[string]string) error
	//DeleteFile(containerName string, fileName string) (int, error)
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
