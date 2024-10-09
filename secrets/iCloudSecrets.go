package secrets

import (
	"fmt"
	"golang.org/x/net/context"
	"sort"
	"time"
)

type CloudSecretsProxy interface {
	GetSecret(ctx context.Context, name string) (string, error)
	GetBinarySecret(ctx context.Context, name string) ([]byte, error)
}

type CloudSecretsCacheOptions struct {
	MaxEntries int
	TTL        time.Duration
}

type secretCache struct {
	secrets    map[string]secret
	maxEntries int
	ttl        time.Duration
}

type secret struct {
	value     string
	binary    []byte
	timeAdded time.Time
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
