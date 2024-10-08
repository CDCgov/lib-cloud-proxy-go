package secrets

type ProxyAuthHandler interface {
	createProxy(options *CloudSecretsCacheOptions) (CloudSecretsProxy, error)
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

type ProxyAuthHandlerAWSDefaultIdentity struct {
	Region string
}

type ProxyAuthHandlerAWSConfiguredIdentity struct {
	AccessID  string
	AccessKey string
	Region    string
}
