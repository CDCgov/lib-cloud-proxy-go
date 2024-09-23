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
	"os"
	"strconv"
	"strings"
	"testing"
)

var cloudStorageTypeToTest = storage.CloudStorageTypeAWSS3
var s3container = ""
var azureContainer = ""

//var cloudStorageTypeToTest = storage.CloudStorageTypeAzure

func initTests() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Unable to load .env")
	}
	s3container = os.Getenv("S3ContainerName")
	azureContainer = os.Getenv("AzureContainerName")
}

func getProxy(storageType storage.CloudStorageType) (storage.CloudStorageProxy, error) {
	initTests()
	switch storageType {
	case storage.CloudStorageTypeAzure:
		{
			connectionString := os.Getenv("ConnectionString")
			return storage.CloudStorageProxyFactory(storageType,
				storage.CloudStorageConnectionOptions{UseConnectionString: true, ConnectionString: connectionString})
		}
	case storage.CloudStorageTypeAWSS3:
		{
			accountURL := os.Getenv("S3AccountURL")
			return storage.CloudStorageProxyFactory(storageType,
				storage.CloudStorageConnectionOptions{UseManagedIdentity: true, AccountURL: accountURL})
		}
	default:
		return nil, errors.New("unknown storage type")
	}

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
	az, err := getProxy(storage.CloudStorageTypeAWSS3)
	container := s3container
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
	az, err := getProxy(storage.CloudStorageTypeAzure)
	container := azureContainer
	if err == nil {
		folders, err := az.ListFolders(context.Background(), container, 10, "testFolder")
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
	az, err := getProxy(storage.CloudStorageTypeAWSS3)
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
	az, err := getProxy(storage.CloudStorageTypeAzure)
	container := azureContainer
	if err == nil {
		content, err := az.GetFileContent(context.Background(), container, "test-stream-upload")
		if err == nil {
			fmt.Println("Success")
			fmt.Println(content)
			assert.Truef(t, true, "succeeded")
		} else {
			printCloudError(err)
			assert.Fail(t, "failed")
		}
	} else {
		printCloudError(err)
		assert.Fail(t, "failed")
	}
}

func TestGetFileContentAsInputStream(t *testing.T) {
	az, err := getProxy(storage.CloudStorageTypeAWSS3)
	container := s3container
	if err == nil {
		readCloser, err := az.GetFileContentAsInputStream(context.Background(), container, "test-stream-upload")
		if err == nil {
			defer readCloser.Close()
			content, er := io.ReadAll(readCloser)
			if er == nil {
				println("Success")
				assert.Truef(t, true, "succeeded")
				println(string(content))
			} else {
				printCloudError(err)
				assert.Fail(t, "failed")
			}
		} else {
			printCloudError(err)
			assert.Fail(t, "failed")
		}
	} else {
		printCloudError(err)
		assert.Fail(t, "failed")
	}
}

func TestGetFile(t *testing.T) {
	az, err := getProxy(storage.CloudStorageTypeAzure)
	container := azureContainer
	if az != nil {
		cloudFile, err := az.GetFile(context.Background(), container, "testFolder/test-fldr-upload.HL7")
		if err == nil {
			fmt.Println("Success")
			fmt.Println(cloudFile.Metadata)
			fmt.Println(cloudFile.Content)
			assert.Truef(t, true, "succeeded")
		} else {
			printCloudError(err)
			assert.Fail(t, "failed")
		}
	} else {
		printCloudError(err)
		assert.Fail(t, "failed")
	}
}
func TestUploadText(t *testing.T) {
	az, err := getProxy(storage.CloudStorageTypeAWSS3)
	container := s3container
	if err == nil {
		content, err := os.ReadFile("test.HL7")
		if err == nil {
			metadata := map[string]string{
				"upload_id":      "1234567890",
				"data_stream_id": "DAART",
			}
			e := az.SaveFileFromText(context.Background(), container,
				"testFolder/test-fldr-upload.HL7",
				metadata, string(content))
			if e != nil {
				printCloudError(e)
				assert.Fail(t, "failed")
			} else {
				println("Success")
			}
		} else {
			printCloudError(err)
			assert.Fail(t, "failed")
		}
	} else {
		printCloudError(err)
		assert.Fail(t, "failed")
	}
}

func TestUploadStream(t *testing.T) {
	az, err := getProxy(storage.CloudStorageTypeAzure)
	container := azureContainer
	if err == nil {
		file, err := os.Open("jdk.zip")
		if err == nil {
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
			e := az.SaveFileFromInputStream(context.Background(), container, "test-stream-jar",
				metadata, reader, fileSize, 10)
			if e != nil {
				printCloudError(e)
				assert.Fail(t, "failed")
			} else {
				println("Success")
			}
		} else {
			printCloudError(err)
			assert.Fail(t, "failed")
		}
	} else {
		printCloudError(err)
		assert.Fail(t, "failed")
	}

}

func TestDeleteFile(t *testing.T) {
	az, err := getProxy(storage.CloudStorageTypeAWSS3)
	container := s3container
	if err == nil {
		er := az.DeleteFile(context.Background(), container, "test-stream-upload")
		if er != nil {
			printCloudError(er)
			var cloudError *storage.CloudStorageError
			if errors.As(er, &cloudError) {
				inner := cloudError.Unwrap()
				if strings.Contains(inner.Error(), "404") {
					// blob does not exist -- fine
					assert.Truef(t, true, "succeeded")
				} else {
					assert.Fail(t, "failed")
				}
			}
		} else {
			println("Success")
			assert.Truef(t, true, "succeeded")
		}
	}
}

func TestCopyS3StreamToAzureStream(t *testing.T) {
	initTests()
	azureProxy, err := getProxy(storage.CloudStorageTypeAzure)
	if err != nil {
		assert.Fail(t, "failure")
		printCloudError(err)
	} else {
		awsProxy, er := getProxy(storage.CloudStorageTypeAWSS3)
		if er != nil {
			assert.Fail(t, "failure")
			printCloudError(er)
		} else {
			// copy file from s3 to azure
			ctx := context.Background()
			fileData, err := awsProxy.GetMetadata(ctx, s3container, "test-stream-jar")
			if err != nil {
				printCloudError(err)
				assert.Fail(t, "failed")
			} else {
				length, _ := strconv.ParseInt(fileData["content_length"], 10, 64)
				println("length is " + fileData["content_length"])
				fileStream, e := awsProxy.GetFileContentAsInputStream(ctx, s3container, "test-stream-jar")
				if e != nil {
					printCloudError(e)
					assert.Fail(t, "failed")
				} else {
					defer fileStream.Close()
					err := azureProxy.SaveFileFromInputStream(ctx, azureContainer, "jar-from-aws1.jar",
						fileData, fileStream, length, 2)
					if err != nil {
						printCloudError(err)
						assert.Fail(t, "failed")
					} else {
						assert.True(t, true, "succeeded")
					}

				}
			}
		}
	}
}

func TestCopyS3FileToAzureStream(t *testing.T) {
	initTests()
	azureProxy, err := getProxy(storage.CloudStorageTypeAzure)
	if err != nil {
		assert.Fail(t, "failure")
		println("unable to connect to azure: ", err.Error())
	} else {
		awsProxy, er := getProxy(storage.CloudStorageTypeAWSS3)
		if er != nil {
			assert.Fail(t, "failure")
			println("unable to connect to AWS: ", err.Error())
		} else {
			// copy file from s3 to azure
			ctx := context.Background()
			file, e := awsProxy.GetFile(ctx, s3container, "test-stream-jar")
			if e != nil {
				println("unable to get file: ", e.Error())
				assert.Fail(t, "failed")
			} else {
				length, _ := strconv.ParseInt(file.Metadata["content_length"], 2, 64)
				println("length is " + file.Metadata["content_length"])
				fileStream := strings.NewReader(file.Content)
				err := azureProxy.SaveFileFromInputStream(ctx, azureContainer, "jar-from-aws.jar",
					file.Metadata, fileStream, length, 10)
				if err != nil {
					println("unable to copy stream: ", err.Error())
					assert.Fail(t, "failed")
				} else {
					assert.True(t, true, "succeeded")
				}
			}
		}
	}
}
