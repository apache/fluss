---
title: Tencent Cloud COS
sidebar_position: 7
---

# Tencent Cloud COS

[Tencent Cloud Object Storage](https://cloud.tencent.com/product/cos) (Tencent Cloud COS) is a distributed storage service offered by Tencent Cloud for storing massive amounts of data. It provides high scalability, low cost, reliability, and security.

## Install COS Plugin Manually

Tencent Cloud COS support is not included in the default Fluss distribution. To enable COS support, you need to manually install the filesystem plugin into Fluss.

1. **Prepare the plugin JAR**:

   - Download the `fluss-fs-cos-$FLUSS_VERSION$.jar` from the [Maven Repository](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-cos/$FLUSS_VERSION$/fluss-fs-cos-$FLUSS_VERSION$.jar).

2. **Place the plugin**: Place the plugin JAR file in the `${FLUSS_HOME}/plugins/cos/` directory:
   ```bash
   mkdir -p ${FLUSS_HOME}/plugins/cos/
   cp fluss-fs-cos-$FLUSS_VERSION$.jar ${FLUSS_HOME}/plugins/cos/
   ```

3. Restart Fluss if the cluster is already running to ensure the new plugin is loaded.

## Configurations setup

To enable Tencent Cloud COS as remote storage, there are some required configurations that must be added to Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss
remote.data.dir: cosn://<your-bucket>/path/to/remote/storage
# COS endpoint suffix, such as: cos.ap-guangzhou.myqcloud.com
fs.cosn.bucket.endpoint_suffix: <your-endpoint-suffix>
# COS region, such as: ap-guangzhou
fs.cosn.userinfo.region: <your-cos-region>

# Authentication (choose one option below)

# Option 1: Direct credentials
# Tencent Cloud secret id
fs.cosn.userinfo.secretId: <your-secret-id>
# Tencent Cloud secret key
fs.cosn.userinfo.secretKey: <your-secret-key>

# Option 2: Secure credential provider
fs.cosn.credentials.provider: <your-credentials-provider>
```
To avoid exposing sensitive access key information directly in the `server.yaml`, you can choose option2 to use a credential provider by setting the `fs.cosn.credentials.provider` property.

For example, to use environment variables for credential management:
```yaml
fs.cosn.credentials.provider: org.apache.hadoop.fs.cosn.auth.EnvironmentVariableCredentialsProvider
```
Then, set the following environment variables before starting the Fluss service:
```bash
export COSN_SECRET_ID=<your-secret-id>
export COSN_SECRET_KEY=<your-secret-key>
```
This approach enhances security by keeping sensitive credentials out of configuration files.

## Token-based Authentication

For client to access the remote storage such as reading snapshot or tiered log, client must obtain a temporary credential (STS token) from Fluss cluster.

Fluss uses Tencent Cloud STS [GetFederationToken](https://cloud.tencent.com/document/product/1312/48195) API to obtain temporary credentials.
Unlike `AssumeRole`, `GetFederationToken` does not require a role ARN — it generates temporary credentials directly from the caller's permanent credentials (`secretId` / `secretKey`).

To enable this, you must configure `fs.cosn.userinfo.region` which is required for calling the STS API.
The `fs.cosn.userinfo.region` specifies the COS region where your bucket is located, such as `ap-guangzhou`, `ap-beijing`, etc. You can
find different regions in [Tencent Cloud COS Regions](https://cloud.tencent.com/document/product/436/6224).

### How it works

1. The Fluss server calls `GetFederationToken` with the configured permanent credentials to obtain a temporary credential consisting of a temporary secret id, a temporary secret key, and a session token.
2. By default, the temporary credential is granted **full COS read-write permission** (`name/cos:*`) scoped to the bucket configured via `remote.data.dir`, and has a **validity period of 1 hour**. You can override this default policy by setting `fs.cosn.security.token.policy` to a custom STS policy JSON.
3. The temporary credential is distributed to clients, which use it to access COS directly.
4. Fluss automatically refreshes the credential before it expires.

:::note
The effective permission of the temporary credential is the **intersection** of the permanent credential's permission and the policy specified in the `GetFederationToken` request.
So the permanent credential (the `secretId` / `secretKey` configured in `server.yaml`) must have sufficient COS permissions.
See more details in [Tencent Cloud COS Access Policy](https://cloud.tencent.com/document/product/436/6884).
:::

## Advanced Configurations

Apart from the above configurations, you can also define the configuration keys mentioned in the [Hadoop-COS documentation](https://hadoop.apache.org/docs/current/hadoop-cos/cloud-storage/index.html)
in the Fluss' `server.yaml`. These configurations defined in Hadoop-COS documentation are advanced configurations which are usually used by performance tuning.
