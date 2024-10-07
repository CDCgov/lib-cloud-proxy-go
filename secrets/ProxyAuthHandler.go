package secrets

type ProxyAuthHandler interface {
	createSecretsClient(options *CloudSecretsCacheOptions) (CloudSecretsProxy, error)
}

type ProxyAuthHandlerAzureDefaultIdentity struct {
	KeyVaultURL string
}

type ProxyAuthHandlerAzureClientSecretIdentity struct {
	KeyVaultURL  string
	TenantID     string
	ClientID     string
	ClientSecret string
}
