package storage

type ProxyAuthHandler interface {
	createProxy() (CloudStorageProxy, error)
}

type ProxyAuthHandlerAzureDefaultIdentity struct {
	AccountURL string
}

type ProxyAuthHandlerAzureClientSecretIdentity struct {
	AccountURL   string
	TenantID     string
	ClientID     string
	ClientSecret string
}

type ProxyAuthHandlerAzureConnectionString struct {
	ConnectionString string
}

type ProxyAuthHandlerAzureSASToken struct {
	AccountURL      string
	AccountKey      string
	ExpirationHours int
}

type ProxyAuthHandlerAWSDefaultIdentity struct {
	AccountURL string
}

type ProxyAuthHandlerAWSConfiguredIdentity struct {
	AccountURL string
	AccessID   string
	AccessKey  string
}
