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
	if az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSConfiguredIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
		AccessID:   os.Getenv("AWS_ACCESS_KEY_ID"),
		AccessKey:  os.Getenv("AWS_SECRET_ACCESS_KEY"),
	}); err == nil {
		container := s3container
		if readCloser, err := az.GetFileContentAsInputStream(context.Background(),
			container, "test-stream-upload"); err == nil {
			defer readCloser.Close()
			if content, err := io.ReadAll(readCloser); err == nil {
				println("Success")
				assert.Truef(t, true, "succeeded")
				println(string(content))
			} else {
				printCloudError(err)
				assert.Fail(t, "reading content failed")
			}
		} else {
			printCloudError(err)
			assert.Fail(t, "getting input stream failed")
		}
	} else {
		printCloudError(err)
		assert.Fail(t, "getting proxy failed")
	}
}

func TestGetFile(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	container := azureContainer
	if az != nil {
		cloudFile, err := az.GetFile(context.Background(), container, "test-text-upload.HL7")
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
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	container := s3container
	if err == nil {
		if content, err := os.ReadFile("test.HL7"); err == nil {
			metadata := map[string]string{
				"upload_id":      "1234567890",
				"data_stream_id": "DAART",
			}
			e := az.UploadFileFromText(context.Background(), container,
				"testFolder/test-fldr-upload.HL7",
				metadata, string(content))
			if e != nil {
				printCloudError(e)
				assert.Fail(t, "upload failed")
			} else {
				println("Success")
			}
		} else {
			printCloudError(err)
			assert.Fail(t, "read file failed")
		}
	} else {
		printCloudError(err)
		assert.Fail(t, "getting proxy failed")
	}
}

func TestUploadStream(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	container := azureContainer
	if err == nil {
		file, _ := os.Open("test.HL7")
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
		e := az.UploadFileFromInputStream(context.Background(), container, "test-stream-test",
			metadata, reader, fileSize, 10)
		if e != nil {
			printCloudError(e)
			assert.Fail(t, "upload failed")
		} else {
			println("Success")
		}

	} else {
		printCloudError(err)
		assert.Fail(t, "getting proxy failed")
	}

}

func TestDeleteFile(t *testing.T) {
	az, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
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
	azureProxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	if err != nil {
		assert.Fail(t, "failure getting azure proxy")
		printCloudError(err)
	} else {
		awsProxy, er := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
			AccountURL: os.Getenv("S3AccountURL"),
		})
		if er != nil {
			assert.Fail(t, "failure getting aws proxy")
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
					err := azureProxy.UploadFileFromInputStream(ctx, azureContainer, "jar-from-aws2.jar",
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
	azureProxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: os.Getenv("ConnectionString"),
	})
	if err != nil {
		assert.Fail(t, "failure getting azure proxy")
		printCloudError(err)
	} else {
		awsProxy, er := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
			AccountURL: os.Getenv("S3AccountURL"),
		})
		if er != nil {
			assert.Fail(t, "failure getting aws proxy")
			printCloudError(err)
		} else {
			// copy file from s3 to azure
			ctx := context.Background()
			file, e := awsProxy.GetFile(ctx, s3container, "test-stream-jar")
			if e != nil {
				printCloudError(err)
				assert.Fail(t, "failed to get file from aws")
			} else {
				length, _ := strconv.ParseInt(file.Metadata["content_length"], 2, 64)
				println("length is " + file.Metadata["content_length"])
				fileStream := strings.NewReader(file.Content)
				err := azureProxy.UploadFileFromInputStream(ctx, azureContainer, "jar-from-aws.jar",
					file.Metadata, fileStream, length, 10)
				if err != nil {
					printCloudError(err)
					assert.Fail(t, "failed to upload from input stream")
				} else {
					assert.True(t, true, "succeeded")
				}
			}
		}
	}
}

func TestCopyS3ToS3WithDifferentCredentials(t *testing.T) {
	awsProxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: os.Getenv("S3AccountURL"),
	})
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failed to get proxy")
		return
	}
	awsLocal, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSConfiguredIdentity{
		AccountURL: "http://localhost:4566",
		AccessID:   "test",
		AccessKey:  "test",
	})
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failed to get local proxy")
		return
	}
	err = awsProxy.CopyFileToRemoteStorageContainer(context.Background(), s3container, "test-stream-jar",
		"my-first-bucket", "test-copy", &awsLocal, 10)
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failed to copy file")
		return
	}
	assert.Truef(t, true, "succeeded")
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
	concurrency := 0
	if fileSize > (5 * 1024 * 1024) {
		concurrency = int(math.Round(float64(fileSize / (5 * 1024 * 1024))))
	}
	fmt.Printf("concurrency is %d \n", concurrency)
	fileBytes, er := awsProxy.GetLargeFileAsByteArray(context.Background(), s3container,
		"test-stream-jar", fileSize, concurrency)
	if er != nil {
		printCloudError(er)
		assert.Fail(t, "failed to get file contents")
		return
	}
	println(len(fileBytes))
	assert.Truef(t, true, "succeeded")

}
