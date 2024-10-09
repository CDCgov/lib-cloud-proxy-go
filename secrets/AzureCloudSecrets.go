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
	cache                *secretCache
}

func (handler ProxyAuthHandlerAzureDefaultIdentity) createProxy(options *CloudSecretsCacheOptions) (CloudSecretsProxy, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err == nil {
		return createProxyFromCredential(handler.KeyVaultURL, credential, options)
	}
	return nil, err
}

func (handler ProxyAuthHandlerAzureClientSecretIdentity) createProxy(options *CloudSecretsCacheOptions) (CloudSecretsProxy, error) {
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
		var cache = secretCache{
			secrets:    make(map[string]secret),
			maxEntries: options.MaxEntries,
			ttl:        options.TTL,
		}

		return &AzureCloudSecretsProxy{
			secretServicesClient: client,
			cache:                &cache,
		}, nil
	}
	return nil, wrapError("unable to create Azure KeyVault service client", err)

}

func (az *AzureCloudSecretsProxy) getSecretFromCache(ctx context.Context, name string) (string, error) {
	s, ok := az.cache.secrets[name]
	if ok && time.Now().Sub(s.timeAdded) < az.cache.ttl {
		return s.value, nil
	}
	resp, err := az.secretServicesClient.GetSecret(ctx, name, "", nil)
	if err != nil {
		return "", wrapError("unable to retrieve secret", err)
	}
	value := *resp.Value
	az.cache.secrets[name] = secret{
		value:     value,
		timeAdded: time.Now(),
	}
	if len(az.cache.secrets) > az.cache.maxEntries {
		az.cache.evict()
	}
	return value, nil
}

func (az *AzureCloudSecretsProxy) GetSecret(ctx context.Context, name string) (string, error) {
	return az.getSecretFromCache(ctx, name)
}

func (az *AzureCloudSecretsProxy) GetBinarySecret(ctx context.Context, name string) ([]byte, error) {
	// Azure Key Vault doesn't really support storing binary secrets, unlike AWS Secret Manager
	value, err := az.getSecretFromCache(ctx, name)
	return []byte(value), err
}
