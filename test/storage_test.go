package test

import (
	"context"
	"fmt"
	"lib-hl7v2-cloud-proxy-go/storage"
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
	az, _ := storage.NewAzureCloudStorageProxyFromConnectionString(connectionString)
	if az != nil {
		files, _ := az.ListFiles(context.Background(), "hl7ingress", 10, "/Candida")
		fmt.Printf("Number of files found: %d \n", len(files))
		for _, file := range files {
			fmt.Println(file)
		}
	}
}
