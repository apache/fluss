---
title: Azure Blob Storage
sidebar_position: 5
---

# Azure Blob Storage

[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) (Azure Blob Storage) is a massively scalable and secure object storage for cloud-native workloads, archives, data lakes, HPC, and machine learning.

## Install Azure FS Plugin Manually

Azure Blob Storage support is not included in the default Fluss distribution. To enable Azure Blob Storage support, you need to manually install the filesystem plugin into Fluss.

1. **Prepare the plugin JAR**:

    - Download the `fluss-fs-azure-$FLUSS_VERSION$.jar` from the [Maven Repository](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-azure/$FLUSS_VERSION$/fluss-fs-azure-$FLUSS_VERSION$.jar).

2. **Place the plugin**: Place the plugin JAR file in the `${FLUSS_HOME}/plugins/azure/` directory:
   ```bash
   mkdir -p ${FLUSS_HOME}/plugins/azure/
   cp fluss-fs-azure-$FLUSS_VERSION$.jar ${FLUSS_HOME}/plugins/azure/
   ```

3. Restart Fluss if the cluster is already running to ensure the new plugin is loaded.

## Configurations setup

To enabled Azure Blob Storage as remote storage, there are some required configurations that must be added to Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss, use the Azure Data Lake Storage URI
remote.data.dir: abfs://flus@flussblob.dfs.core.windows.net/path
# the access key for the azure blob storage account
fs.azure.account.key: 09a295d5-3da5-4435-a660-f438b331ade8
# The oauth account provider type for Token-based Authentication
fs.azure.account.oauth.provider.type: org.apache.fluss.fs.azure.token.DynamicTemporaryAzureCredentialsProvider
# The oauth2 client id for Token-based Authentication
fs.azure.account.oauth2.client.id: ed953f8a-d5e9-481c-b355-62794f178f66
# The oauth2 client secret for Token-based Authentication
fs.azure.account.oauth2.client.secret: ec29f904-64f6-4372-831a-dc28ec818683
# The oauth2 endpoint to generate access tokens for Token-based Authentication
fs.azure.account.oauth2.client.endpoint: https://login.microsoftonline.com/154b1d91-2d07-4e3a-beb6-9261ab4926ab/oauth2/token
```
