package secrets

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"os"
	"testing"
	"time"
)

func init() {
	err := godotenv.Load("../test/.env")
	if err != nil {
		fmt.Println("Unable to load .env")
	}
}
func TestCacheAdd(t *testing.T) {
	az, _ := CloudSecretsProxyFactory(ProxyAuthHandlerAzureDefaultIdentity{KeyVaultURL: os.Getenv("AzureKeyVaultURL")},
		&CloudSecretsCacheOptions{
			MaxEntries: 2,
			TTL:        time.Minute * 10,
		})
	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-1")
	assert.True(t, len(cache.secrets) == 1)

	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-2")
	assert.True(t, len(cache.secrets) == 2)

	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-3")
	assert.True(t, len(cache.secrets) == 2)
	_, ok := cache.secrets["dev-hl7-test-secret-1"]
	assert.True(t, ok == false)
	_, ok = cache.secrets["dev-hl7-test-secret-2"]
	assert.True(t, ok == true)
	_, ok = cache.secrets["dev-hl7-test-secret-3"]
	assert.True(t, ok == true)

}
func TestCacheTTL(t *testing.T) {
	az, _ := CloudSecretsProxyFactory(ProxyAuthHandlerAzureDefaultIdentity{KeyVaultURL: os.Getenv("AzureKeyVaultURL")},
		&CloudSecretsCacheOptions{
			MaxEntries: 3,
			TTL:        time.Second * 10,
		})
	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-1")
	assert.True(t, len(cache.secrets) == 1)
	time.Sleep(time.Second * 5)

	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-2")
	assert.True(t, len(cache.secrets) == 2)
	time.Sleep(time.Second * 5)

	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-3")
	assert.True(t, len(cache.secrets) == 3)
	time.Sleep(time.Second * 5)

	// first one should get refreshed now
	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-1")
	secret1, ok := cache.secrets["dev-hl7-test-secret-1"]
	assert.True(t, ok == true)
	assert.True(t, time.Now().Sub(secret1.timeAdded) < time.Second)

	// third one should be retrieved from cache
	_, _ = az.GetSecret(context.TODO(), "dev-hl7-test-secret-3")
	secret3, ok := cache.secrets["dev-hl7-test-secret-3"]
	assert.True(t, ok == true)
	assert.True(t, time.Now().Sub(secret3.timeAdded) >= time.Second*5)
}
