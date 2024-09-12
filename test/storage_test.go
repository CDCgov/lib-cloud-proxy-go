package test

import (
	"context"
	"errors"
	"fmt"
	"lib-cloud-proxy-go/storage"
	"os"
	"testing"
)

func printCloudError(err error) {
	if err != nil {
		fmt.Printf("Error occurred: %s \n", err.Error())
		var cloudError *storage.CloudStorageError
		if errors.As(err, &cloudError) {
			if cloudError.Unwrap() != nil {
				fmt.Printf("Cloud error caused by: %s \n", cloudError.Unwrap().Error())
			}
		}
	}
}

func TestAzureInitFromIdentity(t *testing.T) {
	accountURL := os.Getenv("AccountURL")
	_, err := storage.CloudStorageProxyFactory(storage.CloudStorageTypeAzure,
		storage.CloudStorageConnectionOptions{UseManagedIdentity: true, AccountURL: accountURL})
	if err != nil {
		printCloudError(err)
	} else {
		fmt.Println("Success")
	}
}

func TestAzureListFiles(t *testing.T) {
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(storage.CloudStorageTypeAzure,
		storage.CloudStorageConnectionOptions{ConnectionString: connectionString})
	if err == nil {
		files, _ := az.ListFiles(context.Background(), "hl7ingress", 10, "")
		fmt.Printf("Number of files found: %d \n", len(files))
		for _, file := range files {
			fmt.Println(file)
		}
	} else {
		fmt.Println("could not get proxy:")
		printCloudError(err)
	}
}

func TestListFolders(t *testing.T) {
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(storage.CloudStorageTypeAzure,
		storage.CloudStorageConnectionOptions{ConnectionString: connectionString})
	if err == nil {
		folders, _ := az.ListFolders(context.Background(), "hl7ingress", 10, "/")
		fmt.Printf("Number of folders found: %d \n", len(folders))
		for _, folder := range folders {
			fmt.Println(folder)
		}
	} else {
		fmt.Println("could not get proxy:")
		printCloudError(err)
	}
}

func TestGetMetadata(t *testing.T) {
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(storage.CloudStorageTypeAzure,
		storage.CloudStorageConnectionOptions{ConnectionString: connectionString})
	if err == nil {
		metadata, e := az.GetMetadata(context.Background(), "hl7ingress", "/demo/AL_COVID19_test1.txt")
		if e == nil {
			fmt.Println("Success")
			fmt.Println(metadata)
		} else {
			fmt.Println("could not get metadata")
			printCloudError(e)
		}
	} else {
		fmt.Println("could not get proxy:")
		printCloudError(err)
	}
}

func TestGetFileContent(t *testing.T) {
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(storage.CloudStorageTypeAzure,
		storage.CloudStorageConnectionOptions{ConnectionString: connectionString})
	if err == nil {
		content, err := az.GetFileContent(context.Background(), "hl7ingress", "/demo/AL_COVID19_test1.txt")
		if err == nil {
			fmt.Println("Success")
			fmt.Println(content)
		} else {
			printCloudError(err)
		}
	} else {
		printCloudError(err)
	}
}

func TestGetFile(t *testing.T) {
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(storage.CloudStorageTypeAzure,
		storage.CloudStorageConnectionOptions{ConnectionString: connectionString})
	if az != nil {
		cloudFile, err := az.GetFile(context.Background(), "hl7ingress", "/demo/AL_COVID19_test1.txt")
		if err == nil {
			fmt.Println("Success")
			fmt.Println(cloudFile.Metadata)
		} else {
			printCloudError(err)
		}
	} else {
		printCloudError(err)
	}
}
