package secrets

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"golang.org/x/net/context"
	"time"
)

type AzureCloudSecretsProxy struct {
	secretServicesClient *azsecrets.Client
}

type secretCache struct {
	secrets    map[string]secret
	maxEntries int
	ttl        time.Duration
}

type secret struct {
	value     string
	timeAdded time.Time
}

var cache = secretCache{}

func (cache *secretCache) getSecretFromCache(ctx context.Context, name string, az *AzureCloudSecretsProxy) (string, error) {
	s, ok := cache.secrets[name]
	if ok && time.Now().Sub(s.timeAdded) < cache.ttl {
		return s.value, nil
	}
	resp, err := az.secretServicesClient.GetSecret(ctx, name, "", nil)
	if err != nil {
		return "", wrapError("unable to retrieve secret", err)
	}
	cache.secrets[name] = secret{
		value:     *resp.Value,
		timeAdded: time.Now(),
	}
	return name, nil
}

func (handler *ProxyAuthHandlerAzureDefaultIdentity) createSecretsClient(options *CloudSecretsCacheOptions) (CloudSecretsProxy, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err == nil {
		return createProxyFromCredential(handler.KeyVaultURL, credential, options)
	}
	return nil, err
}

func (handler *ProxyAuthHandlerAzureClientSecretIdentity) createSecretsClient(options *CloudSecretsCacheOptions) (CloudSecretsProxy, error) {
	credential, err := azidentity.NewClientSecretCredential(handler.TenantID, handler.ClientID,
		handler.ClientSecret, nil)
	if err == nil {
		return createProxyFromCredential(handler.KeyVaultURL, credential, options)
	}
	return nil, err
}

func createProxyFromCredential(accountURL string, credential azcore.TokenCredential, options *CloudSecretsCacheOptions) (CloudSecretsProxy, error) {
	client, err := azsecrets.NewClient(accountURL, credential, nil)
	if err == nil {
		cache.maxEntries = options.MaxEntries
		cache.ttl = options.TTL
		return &AzureCloudSecretsProxy{secretServicesClient: client}, nil
	}
	return nil, wrapError("unable to create Azure KeyVault service client", err)

}

func (az *AzureCloudSecretsProxy) GetSecret(ctx context.Context, name string) (string, error) {
	return cache.getSecretFromCache(ctx, name, az)
}
