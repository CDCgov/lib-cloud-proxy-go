package secrets

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"golang.org/x/net/context"
	"time"
)

type AWSCloudSecretsProxy struct {
	secretServicesClient *secretsmanager.Client
	cache                *secretCache
}

func (handler ProxyAuthHandlerAWSDefaultIdentity) createProxy(options *CloudSecretsCacheOptions) (CloudSecretsProxy, error) {
	var awsConfig aws.Config
	var err error
	if handler.Region != "" {
		awsConfig, err = config.LoadDefaultConfig(context.TODO(), config.WithRegion(handler.Region))
	} else {
		awsConfig, err = config.LoadDefaultConfig(context.TODO())
	}
	if err != nil {
		return nil, wrapError("unable to create Secrets Manager service client", err)
	}
	return createProxyFromConfig(handler.Region, &awsConfig, options), nil

}

func (handler ProxyAuthHandlerAWSConfiguredIdentity) createProxy(options *CloudSecretsCacheOptions) (CloudSecretsProxy, error) {
	var awsConfig aws.Config
	var err error
	if handler.Region != "" {
		awsConfig, err = config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(handler.AccessID, handler.AccessKey, "")),
			config.WithRegion(handler.Region))
	} else {
		awsConfig, err = config.LoadDefaultConfig(context.TODO(),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(handler.AccessID, handler.AccessKey, "")))
	}
	if err != nil {
		return nil, wrapError("unable to create Secrets Manager service client", err)
	}
	return createProxyFromConfig(handler.Region, &awsConfig, options), nil
}

func createProxyFromConfig(accountRegion string, awsConfig *aws.Config, options *CloudSecretsCacheOptions) CloudSecretsProxy {
	client := secretsmanager.NewFromConfig(*awsConfig, func(o *secretsmanager.Options) {
		if accountRegion != "" {
			o.Region = accountRegion
		}
	})
	cache := secretCache{
		secrets:    make(map[string]secret),
		maxEntries: options.MaxEntries,
		ttl:        options.TTL,
	}
	return &AWSCloudSecretsProxy{
		secretServicesClient: client,
		cache:                &cache,
	}
}

func (aw *AWSCloudSecretsProxy) getSecretFromCache(ctx context.Context, name string) (secret, error) {
	s, ok := aw.cache.secrets[name]
	if ok && time.Now().Sub(s.timeAdded) < aw.cache.ttl {
		return s, nil
	}
	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(name),
	}

	resp, err := aw.secretServicesClient.GetSecretValue(ctx, input)
	if err != nil {
		return secret{}, wrapError("unable to retrieve secret", err)
	}

	thisSecret := secret{
		value:     *resp.SecretString,
		binary:    resp.SecretBinary,
		timeAdded: time.Now(),
	}
	aw.cache.secrets[name] = thisSecret
	if len(aw.cache.secrets) > aw.cache.maxEntries {
		aw.cache.evict()
	}
	return thisSecret, nil
}

func (aw *AWSCloudSecretsProxy) GetSecret(ctx context.Context, name string) (string, error) {
	s, err := aw.getSecretFromCache(ctx, name)
	if err != nil {
		return "", err
	}
	return s.value, nil
}

func (aw *AWSCloudSecretsProxy) GetBinarySecret(ctx context.Context, name string) ([]byte, error) {
	s, err := aw.getSecretFromCache(ctx, name)
	if err != nil {
		return nil, err
	}
	return s.binary, nil
}
