package test

import (
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"lib-cloud-proxy-go/secrets"
	"os"
	"testing"
	"time"
)

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		fmt.Println("Unable to load .env")
	}
}
func printCloudSecretsError(err error) {
	if err != nil {
		fmt.Printf("Error occurred: %s \n", err.Error())
		var cloudError *secrets.CloudSecretsError
		if errors.As(err, &cloudError) {
			if cloudError.Unwrap() != nil {
				fmt.Printf("Cloud error caused by: %s \n", cloudError.Unwrap().Error())
			}
		}
	}
}

func TestAzureGetSecret(t *testing.T) {
	url := os.Getenv("AzureKeyVaultURL")
	secretName := os.Getenv("AzureSecretName")
	az, err := secrets.CloudSecretsProxyFactory(secrets.ProxyAuthHandlerAzureDefaultIdentity{KeyVaultURL: url},
		&secrets.CloudSecretsCacheOptions{
			MaxEntries: 10,
			TTL:        time.Minute * 10,
		})
	if err != nil {
		printCloudSecretsError(err)
	}
	value, err := az.GetSecret(context.Background(), secretName)
	assert.True(t, len(value) > 0 && err == nil)
}

func TestAWSGetSecret(t *testing.T) {
	aw, err := secrets.CloudSecretsProxyFactory(secrets.ProxyAuthHandlerAWSDefaultIdentity{
		Region: os.Getenv("AWS_DEFAULT_REGION"),
	}, &secrets.CloudSecretsCacheOptions{
		MaxEntries: 3,
		TTL:        time.Hour,
	})
	assert.True(t, err == nil)
	s, err := aw.GetSecret(context.Background(), "dev-routing-test-secret-1")
	printCloudSecretsError(err)
	assert.True(t, err == nil)
	assert.True(t, len(s) > 0)
}
