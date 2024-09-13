package test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"lib-cloud-proxy-go/storage"
	"os"
	"strings"
	"testing"
)

var cloudStorageTypeToTest = storage.CloudStorageTypeAzure

func initTests() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Unable to load .env")
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

func TestInitFromIdentity(t *testing.T) {
	initTests()
	accountURL := os.Getenv("AccountURL")
	_, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseManagedIdentity: true, AccountURL: accountURL})
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failed")
	} else {
		assert.Truef(t, true, "Success")
	}
}

func TestInitFromSASToken(t *testing.T) {
	initTests()
	token := os.Getenv("URLWithSASToken")
	_, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseSASToken: true, URLWithSASToken: token})
	if err != nil {
		printCloudError(err)
		assert.Fail(t, "failed")
	} else {
		fmt.Println("Success")
		assert.Truef(t, true, "succeeded")
	}
}

func TestListFiles(t *testing.T) {
	initTests()
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseConnectionString: true, ConnectionString: connectionString})
	if err == nil {
		files, _ := az.ListFiles(context.Background(), "hl7ingress", 10, "")
		fmt.Printf("Number of files found: %d \n", len(files))
		for _, file := range files {
			fmt.Println(file)
		}
		assert.Truef(t, true, "succeeded")
	} else {
		fmt.Println("could not get proxy:")
		printCloudError(err)
		assert.Fail(t, "failed")
	}
}

func TestListFolders(t *testing.T) {
	initTests()
	token := os.Getenv("URLWithSASToken")
	az, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseSASToken: true, URLWithSASToken: token})
	if err == nil {
		folders, _ := az.ListFolders(context.Background(), "hl7ingress", 10, "/")
		fmt.Printf("Number of folders found: %d \n", len(folders))
		for _, folder := range folders {
			fmt.Println(folder)
		}
		assert.Truef(t, true, "succeeded")
	} else {
		fmt.Println("could not get proxy:")
		printCloudError(err)
		assert.Fail(t, "failed")
	}
}

func TestGetMetadata(t *testing.T) {
	initTests()
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseConnectionString: true, ConnectionString: connectionString})
	if err == nil {
		metadata, e := az.GetMetadata(context.Background(), "hl7ingress", "/demo/AL_COVID19_test1.txt")
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
	initTests()
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseConnectionString: true, ConnectionString: connectionString})
	if err == nil {
		content, err := az.GetFileContent(context.Background(), "hl7ingress", "/demo/AL_COVID19_test1.txt")
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

func TestGetFile(t *testing.T) {
	initTests()
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseConnectionString: true, ConnectionString: connectionString})
	if az != nil {
		cloudFile, err := az.GetFile(context.Background(), "hl7ingress", "/demo/AL_COVID19_test1.txt")
		if err == nil {
			fmt.Println("Success")
			fmt.Println(cloudFile.Metadata)
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
	initTests()
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseConnectionString: true, ConnectionString: connectionString})
	if err == nil {
		content, err := os.ReadFile("test.HL7")
		if err == nil {
			metadata := map[string]string{
				"upload_id":      "1234567890",
				"data_stream_id": "DAART",
			}
			e := az.SaveFileFromText(context.Background(), "reports-test", "test-text-upload.HL7",
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
	initTests()
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseConnectionString: true, ConnectionString: connectionString})
	if err == nil {
		file, err := os.Open("test.HL7")
		if err == nil {
			metadata := map[string]string{
				"upload_id":      "1234567890",
				"data_stream_id": "DAART",
			}
			reader := bufio.NewReader(file)
			e := az.SaveFileFromInputStream(context.Background(), "reports-test", "test-stream-upload.HL7",
				metadata, reader)
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
	initTests()
	connectionString := os.Getenv("ConnectionString")
	az, err := storage.CloudStorageProxyFactory(cloudStorageTypeToTest,
		storage.CloudStorageConnectionOptions{UseConnectionString: true, ConnectionString: connectionString})
	if err == nil {
		er := az.DeleteFile(context.Background(), "reports-test", "test-stream-upload.HL7")
		if er != nil {
			printCloudError(er)
			var cloudError *storage.CloudStorageError
			if errors.As(er, &cloudError) {
				inner := cloudError.Unwrap()
				if strings.Contains(inner.Error(), "RESPONSE 404") {
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
