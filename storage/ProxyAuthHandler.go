package storage

type ProxyAuthHandler interface {
	createProxy() (CloudStorageProxy, error)
}

type ProxyAuthHandlerAzureIdentity struct {
	AccountURL string
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
