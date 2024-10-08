package secrets

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"golang.org/x/net/context"
	"sort"
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

var cache = secretCache{
	secrets:    make(map[string]secret),
	maxEntries: 10,
	ttl:        time.Hour,
}

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
	if len(cache.secrets) > cache.maxEntries {
		cache.evict()
	}
	return name, nil
}

func (cache *secretCache) evict() {
	// sort by time added, newest first
	keys := make([]string, len(cache.secrets))
	for k := range cache.secrets {
		keys = append(keys, k)
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return cache.secrets[keys[i]].timeAdded.After(cache.secrets[keys[j]].timeAdded)
	})
	sortedCache := make(map[string]secret, len(cache.secrets))
	// keep the newest, up to max entries
	for keyNum := 0; keyNum < cache.maxEntries; keyNum++ {
		sortedCache[keys[keyNum]] = cache.secrets[keys[keyNum]]
	}
	cache.secrets = sortedCache
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
		cache.maxEntries = options.MaxEntries
		cache.ttl = options.TTL
		return &AzureCloudSecretsProxy{secretServicesClient: client}, nil
	}
	return nil, wrapError("unable to create Azure KeyVault service client", err)

}

func (az *AzureCloudSecretsProxy) GetSecret(ctx context.Context, name string) (string, error) {
	return cache.getSecretFromCache(ctx, name, az)
}
