package secrets

import (
	"fmt"
	"golang.org/x/net/context"
	"time"
)

type CloudSecretsProxy interface {
	GetSecret(ctx context.Context, name string) (string, error)
}

type CloudSecretsCacheOptions struct {
	MaxEntries int
	TTL        time.Duration
}

func CloudSecretsProxyFactory(handler ProxyAuthHandler, options *CloudSecretsCacheOptions) (CloudSecretsProxy, error) {
	return handler.createProxy(options)
}

type CloudSecretsError struct {
	message       string
	internalError error
}

func (err *CloudSecretsError) Error() string {
	return fmt.Sprintf("CloudSecrets Error: %s", err.message)
}

func (err *CloudSecretsError) Unwrap() error {
	return err.internalError
}

func wrapError(msg string, err error) *CloudSecretsError {
	return &CloudSecretsError{message: msg, internalError: err}
}
