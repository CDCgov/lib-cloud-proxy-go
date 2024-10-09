# lib-cloud-proxy-go
This library provides an easy, cloud-agnostic way to interact with cloud blob/object storage and 
cloud secret stores.

## Intro
The lib-cloud-proxy-go library is a rework of the [lib-cloud-proxy](https://github.com/CDCgov/lib-cloud-proxy) (Kotlin) 
library and is written in Golang. The design differs from the original library in the following ways:
- It does not require any configuration yaml to determine the cloud provider. 
- Because it does not pre-configure the cloud provider, the user can create more than one 
instance of each proxy class to interact with different cloud providers simultaneously.
- It offers a function to copy files from one cloud provider to another.
- It does not include a messaging proxy.
- It does include a proxy for retrieving secrets by secret ID/name.

Currently, the cloud providers supported by this library are AWS (S3, Secrets Manager)
and Azure (Azure Blob Storage, Azure Key Vault).

## CloudStorageProxy Usage
### Obtaining a Proxy instance
All interactions with cloud storage are done through the `CloudStorageProxy`.
To obtain an instance of `CloudStorageProxy`, you call the `CloudStorageProxyFactory()` method
and pass in a `ProxyAuthHandler` that contains the authentication information needed
to connect to the specific cloud provider and storage account you are targeting. 
The factory method then returns a pointer to the underlying proxy class that targets that provider.

There are several `ProxyAuthHandler` types available for connecting to S3 and Azure Blob Storage, 
allowing the user to choose from multiple authentication strategies including connection string
authentication, externally assigned managed identities, or client ID and secret authentication.

For example, to connect to S3 using credentials that are stored with the application (referred
to as the "default identity"), you would pass the factory method an instance of `ProxyAuthHandlerAWSDefaultIdentity`
and supply the one piece of data it requires, the Account URL:

```go
	proxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAWSDefaultIdentity{
		AccountURL: url,
	})
```
Alternatively, you can supply a client ID and secret directly using the 
`ProxyAuthHandlerAWSConfiguredIdentity` type.

To connect to Azure Storage, the same factory method is used, passing in one of the
Azure ProxyAuthHandlers. For example, this is how to connect to Azure using a connection string:
```go
	proxy, err := storage.CloudStorageProxyFactory(storage.ProxyAuthHandlerAzureConnectionString{
		ConnectionString: connString,
	})
```
### Proxy methods
Once you have a `CloudStorageProxy` instance, the following methods are available:
 - ListFiles
 - ListFolders
 - GetFile
 - GetFileContentAsString
 - GetFileContentAsInputStream
 - GetLargeFileContentAsByteArray
 - GetMetadata
 - UploadFileFromString
 - UploadFileFromInputStream
 - DeleteFile
 - GetSourceBlobSignedURL
 - CopyFileFromRemoteStorage
 - CopyFileFromLocalStorage

In the parlance of this library, "file" and "blob" both refer to the S3 Object or Azure Blob being accessed.

Please see the tests provided in `test\storage_test.go` in this repository
for examples of how to use these methods.

### A note about the "Copy" functions
`CopyFileFromRemoteStorage` is provided for the following
scenarios:
- Copying a file between two storage accounts that use different credentials 
(e.g., S3 bucketA to S3 bucketB needing a different Client ID and Secret)
- Copying a file between two storage accounts that are in different cloud providers
  (i.e., S3 to Azure or Azure to S3)

In either case, 2 proxies must be used to do the copy: the proxy attached to the 
destination storage account calls the method, and the proxy to the source file 
is passed into the method as a parameter.

`CopyFileFromLocalStorage` is provided for the scenario where a file is being
copied from one container to another within the same storage account, or from
one folder to another within the same container. Since the credentials and cloud provider
are the same in this case, only one proxy is needed.

## CloudSecretsProxy Usage
### Obtaining a Proxy instance
All interactions with secret stores are done through the `CloudSecretsProxy`. To obtain an instance
of `CloudSecretsProxy`, you call the `CloudSecretsProxyFactory()` method and pass in two parameters:
1. a `ProxyAuthHandler`that contains the authentication information needed to connect to the specific cloud provider
you are targeting, and
2. a pointer to `CloudSecretsCacheOptions` that configures the proxy's local cache.

### Secrets caching
To save time and round-trips, once a secret has been pulled from the cloud, the `CloudSecretsProxy` stores
it in a local cache in case it is needed again. Each proxy instance maintains its own cache, which
is initialized when the proxy is created via the factory method. To that end, the `CloudSecretsCacheOptions`
has two important settings:
1. **MaxEntries**: the maximum number of secrets to keep in the cache at one time. When adding a secret to the 
cache would cause the maximum entries to be exceeded, the oldest member of the cache is evicted to make room
for the new secret.
2. **TTL**: the "time to live" for entries in the cache. When a secret is requested and it has been in
the cache beyond the configured TTL duration, its value will be pulled from the cloud instead of the cache
and the cache will be refreshed with the new value.

### Proxy methods
The `CloudSecretsProxy` offers these two methods for obtaining a secret:
- GetSecret
- GetBinarySecret

When the proxy is targeting Azure Key Vault, use `GetSecret` to retrieve the decoded secret value
as a string. 
Key Vault does not support storing binary objects as secrets at this time, so calling `GetBinarySecret`
on a `CloudSecretsProxy` targeting Azure will return the secret string value as an array of bytes.

When the proxy is targeting AWS Secrets Manager, `GetSecret` will return all key/value pairs stored
under that secret's name as a JSON string. If the secret is stored as binary instead of one or more
key/value pairs, you must call `GetBinarySecret` to get the secret value.


## Related documents

* [Open Practices](open_practices.md)
* [Rules of Behavior](rules_of_behavior.md)
* [Disclaimer](DISCLAIMER.md)
* [Contribution Notice](CONTRIBUTING.md)
* [Code of Conduct](code-of-conduct.md)

## Public Domain Standard Notice
This repository constitutes a work of the United States Government and is not
subject to domestic copyright protection under 17 USC § 105. This repository is in
the public domain within the United States, and copyright and related rights in
the work worldwide are waived through the [CC0 1.0 Universal public domain dedication](https://creativecommons.org/publicdomain/zero/1.0/).
All contributions to this repository will be released under the CC0 dedication. By
submitting a pull request you are agreeing to comply with this waiver of
copyright interest.

## License Standard Notice
The repository utilizes code licensed under the terms of the Apache Software
License and therefore is licensed under ASL v2 or later.

This source code in this repository is free: you can redistribute it and/or modify it under
the terms of the Apache Software License version 2, or (at your option) any
later version.

This source code in this repository is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the Apache Software License for more details.

You should have received a copy of the Apache Software License along with this
program. If not, see http://www.apache.org/licenses/LICENSE-2.0.html

The source code forked from other open source projects will inherit its license.

## Privacy Standard Notice
This repository contains only non-sensitive, publicly available data and
information. All material and community participation is covered by the
[Disclaimer](DISCLAIMER.md)
and [Code of Conduct](code-of-conduct.md).
For more information about CDC's privacy policy, please visit [http://www.cdc.gov/other/privacy.html](https://www.cdc.gov/other/privacy.html).

## Contributing Standard Notice
Anyone is encouraged to contribute to the repository by [forking](https://help.github.com/articles/fork-a-repo)
and submitting a pull request. (If you are new to GitHub, you might start with a
[basic tutorial](https://help.github.com/articles/set-up-git).) By contributing
to this project, you grant a world-wide, royalty-free, perpetual, irrevocable,
non-exclusive, transferable license to all users under the terms of the
[Apache Software License v2](http://www.apache.org/licenses/LICENSE-2.0.html) or
later.

All comments, messages, pull requests, and other submissions received through
CDC including this GitHub page may be subject to applicable federal law, including but not limited to the Federal Records Act, and may be archived. Learn more at [http://www.cdc.gov/other/privacy.html](http://www.cdc.gov/other/privacy.html).

## Records Management Standard Notice
This repository is not a source of government records, but is a copy to increase
collaboration and collaborative potential. All government records will be
published through the [CDC website](http://www.cdc.gov).

## Additional Standard Notices
Please refer to [CDC's Template Repository](https://github.com/CDCgov/template) for more information about [contributing to this repository](https://github.com/CDCgov/template/blob/main/CONTRIBUTING.md), [public domain notices and disclaimers](https://github.com/CDCgov/template/blob/main/DISCLAIMER.md), and [code of conduct](https://github.com/CDCgov/template/blob/main/code-of-conduct.md).
