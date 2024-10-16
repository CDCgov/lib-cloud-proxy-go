package test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"io"
	"lib-cloud-proxy-go/storage"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
)

var s3container = ""
var azureContainer = ""

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Unable to load .env")
	}
	s3container = os.Getenv("S3ContainerName")
	azureContainer = os.Getenv("AzureContainerName")
}

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

func TestListFiles(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	container := azureContainer
	if err == nil {
		files, err := az.ListFiles(context.Background(), container, 10, "")
		if err == nil {
			fmt.Printf("Number of files found: %d \n", len(files))
			for _, file := range files {
				fmt.Println(file)
			}
			assert.Truef(t, true, "succeeded")
		} else {
			printCloudError(err)
			assert.Fail(t, "failed")
		}

	} else {
		fmt.Println("could not get proxy:")
		printCloudError(err)
		assert.Fail(t, "failed")
	}
}

func TestListFolders(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureSASToken{
		AccountURL:      os.Getenv("AzureAccountURL"),
		AccountKey:      os.Getenv("AzureAccountKey"),
		ExpirationHours: 48,
	})
	container := azureContainer
	if err == nil {
		folders, err := az.ListFolders(context.Background(), container, 10, "hl7_")
		fmt.Printf("Number of folders found: %d \n", len(folders))
		if err == nil {
			for _, folder := range folders {
				fmt.Println(folder)
			}
			assert.Truef(t, true, "succeeded")
		} else {
			printCloudError(err)
			assert.Fail(t, "failed")
		}
	} else {
		fmt.Println("could not get proxy:")
		printCloudError(err)
		assert.Fail(t, "failed")
	}
}

func TestGetMetadata(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	container := s3container
	if err == nil {
		metadata, e := az.GetMetadata(context.Background(), container, "testFolder/test-fldr-upload.HL7")
		if e == nil {
			fmt.Println("Success")
			fmt.Println(metadata)
			assert.Truef(t, true, "succeeded")
		} else {
			printCloudError(err)
			assert.Fail(t, "failed")
		}
	} else {
		fmt.Println("could not get proxy:")
		printCloudError(err)
		assert.Fail(t, "failed")
	}
}

func TestGetFileContent(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	container := azureContainer
	if err == nil {
		content, err := az.GetFileContentAsString(context.Background(), container, "test-stream-upload")
		if err == nil {
			fmt.Println("Success")
			fmt.Println(content)
		} else {
			printCloudError(err)
		}
	} else {
		printCloudError(err)
	}
	assert.True(t, err == nil)
}

func TestGetFileContentAsInputStream(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSConfiguredIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
		AccessID:   os.Getenv("AWS_ACCESS_KEY_ID"),
		AccessKey:  os.Getenv("AWS_SECRET_ACCESS_KEY"),
	})
	if err == nil {
		container := s3container
		readCloser, err := az.GetFileContentAsInputStream(context.Background(),
			container, "testFolder/test-fldr-upload.HL7")
		if err == nil {
			defer readCloser.Close()
			content, err := io.ReadAll(readCloser)
			if err == nil {
				println("Success")
				println(string(content))
			} else {
				printCloudError(err)
			}
			assert.True(t, err == nil)
		} else {
			printCloudError(err)
		}
	} else {
		printCloudError(err)
	}
	assert.True(t, err == nil)
}

func TestGetFile(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	container := azureContainer
	if az != nil {
		cloudFile, err := az.GetFile(context.Background(), container, "test.HL7")
		if err == nil {
			fmt.Println("Success")
			fmt.Println(cloudFile.Metadata)
			fmt.Println(cloudFile.Content)
		} else {
			printCloudError(err)
		}
		assert.True(t, err == nil)
	} else {
		printCloudError(err)
	}
	assert.True(t, err == nil)
}

func TestUploadText(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	container := s3container
	if err == nil {
		content, err := os.ReadFile("test.HL7")
		if err == nil {
			metadata := map[string]string{
				"upload_id":      "1234567890",
				"data_stream_id": "DAART",
			}
			err = az.UploadFileFromString(context.Background(), container,
				"testFolder/test-fldr-upload.HL7",
				metadata, string(content))
			if err != nil {
				printCloudError(err)
			} else {
				println("Success")
			}
		} else {
			printCloudError(err)
		}
	} else {
		printCloudError(err)
	}
	assert.True(t, err == nil)
}

func TestUploadStream(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	container := s3container
	if err == nil {
		file, _ := os.Open("10gb.txt")
		fileInfo, err := file.Stat()
		var fileSize int64
		if err == nil {
			fileSize = fileInfo.Size()
		} else {
			fileSize = 1
		}
		fmt.Printf("file size: %d \n", fileSize)
		metadata := map[string]string{
			"upload_id":      "987654321",
			"data_stream_id": "DAART",
		}
		reader := bufio.NewReader(file)
		err = az.UploadFileFromInputStream(context.Background(), container, "10gb.txt",
			metadata, reader, fileSize, 20)
		if err != nil {
			printCloudError(err)
		} else {
			println("Success")
		}

	} else {
		printCloudError(err)
	}
	assert.True(t, err == nil)
}

func TestDeleteFile(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	container := s3container
	if err == nil {
		err = az.DeleteFile(context.Background(), container, "test-stream-upload")
		if err != nil {
			printCloudError(err)
			var cloudError *storage.CloudStorageError
			if errors.As(err, &cloudError) {
				inner := cloudError.Unwrap()
				if strings.Contains(inner.Error(), "404") {
					// blob does not exist -- fine
					err = nil
				}
			}
		}
	}
	assert.True(t, err == nil)
}

func TestGetLargeFileAsByteArray(t *testing.T) {
	awsProxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failed to get proxy")
		return
	}
	metadata, e := awsProxy.GetMetadata(context.Background(), s3container, "test-stream-jar")
	if e != nil {
		printCloudError(err)
		assert.Fail(t, "failed to get metadata")
		return
	}
	fileSize, _ := strconv.ParseInt(metadata["content_length"], 10, 64)
	println(metadata["content_length"])
	concurrency := 5
	if fileSize > (5 * 1024 * 1024) {
		concurrency = int(math.Round(float64(fileSize / (5 * 1024 * 1024))))
	}
	fmt.Printf("concurrency is %d \n", concurrency)
	fileBytes, er := awsProxy.GetLargeFileContentAsByteArray(context.Background(), s3container,
		"test-stream-jar", fileSize, concurrency)
	if er != nil {
		printCloudError(er)
		assert.Fail(t, "failed to get file contents")
		return
	}
	println(len(fileBytes))
	assert.Truef(t, true, "succeeded")

}

// ***//
func TestCopyS3ToAzure(t *testing.T) {
	// needs 2 proxies
	azureProxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failure getting azure proxy")
		return
	}
	awsProxy, er := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	if er != nil {
		printCloudError(er)
		assert.Fail(t, "failed getting aws proxy")
		return
	}
	e := azureProxy.CopyFileFromRemoteStorage(context.Background(), s3container, "10gb.txt",
		azureContainer, "10gb-from-aws.txt", &awsProxy, 1000)
	if e != nil {
		printCloudError(e)
		assert.Fail(t, "failed copy from s3")
	}
	assert.True(t, e == nil, "Success")
}

func TestCopyAzureToS3(t *testing.T) {
	// needs 2 proxies
	azureProxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failure getting azure proxy")
		return
	}
	awsProxy, er := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	if er != nil {
		printCloudError(er)
		assert.Fail(t, "failed getting aws proxy")
		return
	}
	e := awsProxy.CopyFileFromRemoteStorage(context.Background(), azureContainer, "test-from-aws.zip",
		s3container, "test-from-azure-1.zip", &azureProxy, 100)
	if e != nil {
		printCloudError(e)
		assert.Fail(t, "failed copy from azure")
	}
	assert.True(t, e == nil, "Success")
}

func TestCopyLocalAzure(t *testing.T) {
	azureProxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failure getting azure proxy")
		return
	}
	e := azureProxy.CopyFileFromLocalStorage(context.Background(), "routeingress", "big/2g.txt",
		"proxy-test", "2gtest.txt", 100)
	if e != nil {
		printCloudError(e)
		assert.Fail(t, "failed")
		return
	}
	assert.Truef(t, e == nil, "succeeded")
}

func TestCopyLocalS3(t *testing.T) {
	awsProxy, er := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	if er != nil {
		printCloudError(er)
		assert.Fail(t, "failed getting aws proxy")
		return
	}
	err := awsProxy.CopyFileFromLocalStorage(context.Background(), s3container, "test-from-azure.zip",
		s3container, "testFolder/test-copy-s3-1.zip", 30)
	if err != nil {
		printCloudError(err)
	}
	assert.True(t, err == nil, "succeeded")
}

func TestCreateContainerAzure(t *testing.T) {
	azureProxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failure getting azure proxy")
		return
	}
	err = azureProxy.CreateContainerIfNotExists(context.TODO(), "quq6-container1")
	printCloudError(err)
	assert.True(t, err == nil)
}

func TestCreateContainerS3(t *testing.T) {
	awsProxy, er := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	if er != nil {
		printCloudError(er)
		assert.Fail(t, "failed getting aws proxy")
		return
	}
	err := awsProxy.CreateContainerIfNotExists(context.TODO(), "quq6-container1")
	printCloudError(err)
	assert.True(t, err == nil)
}
