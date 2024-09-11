package test

import (
	"context"
	"fmt"
	"lib-cloud-proxy-go/storage"
	"os"
	"testing"
)

func TestAzureInitFromIdentity(t *testing.T) {
	accountURL := os.Getenv("AccountURL")
	_, err := storage.NewAzureCloudStorageProxyFromIdentity(accountURL)
	if err != nil {
		fmt.Printf("error occurred: %s \n", err.Error())
	} else {
		fmt.Println("Success")
	}
}

func TestAzureListFiles(t *testing.T) {
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.NewAzureCloudStorageProxyFromConnectionString(connectionString)
	if az != nil {
		files, _ := az.ListFiles(context.Background(), "hl7ingress", 10, "")
		fmt.Printf("Number of files found: %d \n", len(files))
		for _, file := range files {
			fmt.Println(file)
		}
	} else {
		fmt.Printf("could not get proxy: %s", err.Error())
	}
}

func TestListFolders(t *testing.T) {
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.NewAzureCloudStorageProxyFromConnectionString(connectionString)
	if az != nil {
		folders, _ := az.ListFolders(context.Background(), "hl7ingress", 10, "/demo")
		fmt.Printf("Number of folders found: %d \n", len(folders))
		for _, folder := range folders {
			fmt.Println(folder)
		}
	} else {
		fmt.Printf("could not get proxy: %s", err.Error())
	}
}
