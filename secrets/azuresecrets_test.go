package secrets

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"os"
	"testing"
	"time"
)

var az *AzureCloudSecretsProxy

func init() {
	err := godotenv.Load("../test/.env")
	if err != nil {
		fmt.Println("Unable to load .env")
	}
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	client, err := azsecrets.NewClient(os.Getenv("AzureKeyVaultURL"), credential, nil)
	if err == nil {
		var cache = secretCache{
			secrets:    make(map[string]secret),
			maxEntries: 3,
			ttl:        time.Minute,
		}

		az = &AzureCloudSecretsProxy{
			secretServicesClient: client,
			cache:                &cache,
		}
	}
}
func TestCacheAdd(t *testing.T) {
	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-1")
	az.cache.maxEntries = 2
	assert.True(t, len(az.cache.secrets) == 1)

	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-2")
	assert.True(t, len(az.cache.secrets) == 2)

	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-3")
	assert.True(t, len(az.cache.secrets) == 2)
	_, ok := az.cache.secrets["dev-hl7-test-secret-1"]
	assert.True(t, ok == false)
	_, ok = az.cache.secrets["dev-hl7-test-secret-2"]
	assert.True(t, ok == true)
	_, ok = az.cache.secrets["dev-hl7-test-secret-3"]
	assert.True(t, ok == true)

}
func TestCacheTTL(t *testing.T) {
	az.cache.maxEntries = 3
	az.cache.ttl = time.Second * 10

	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-1")
	assert.True(t, len(az.cache.secrets) == 1)
	time.Sleep(time.Second * 5)

	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-2")
	assert.True(t, len(az.cache.secrets) == 2)
	time.Sleep(time.Second * 5)

	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-3")
	assert.True(t, len(az.cache.secrets) == 3)
	time.Sleep(time.Second * 5)

	// first one should get refreshed now
	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-1")
	secret1, ok := az.cache.secrets["dev-hl7-test-secret-1"]
	assert.True(t, ok == true)
	assert.True(t, time.Now().Sub(secret1.timeAdded) < time.Second)

	// third one should be retrieved from az.cache
	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-3")
	secret3, ok := az.cache.secrets["dev-hl7-test-secret-3"]
	assert.True(t, ok == true)
	assert.True(t, time.Now().Sub(secret3.timeAdded) >= time.Second*5)
}
