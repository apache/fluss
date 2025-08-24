---
title: Aliyun OSS
sidebar_position: 3
---

# Aliyun OSS

## OSS: Object Storage Service 

[Aliyun Object Storage Service](https://www.aliyun.com/product/oss) (Aliyun OSS) is widely used, particularly popular among China’s cloud users, and it provides cloud object storage for a variety of use cases.


## Configurations setup

To enable OSS as remote storage, there are some required configurations that must be added to Fluss' `server.yaml`:

```yaml
# The directory used as the remote storage for Fluss
remote.data.dir: oss://<your-bucket>/path/to/remote/storage
# Aliyun OSS endpoint to connect to, such as: oss-cn-hangzhou.aliyuncs.com
fs.oss.endpoint: <your-endpoint>
# Aliyun STS endpoint to connect to obtain a STS token, such as: sts.cn-hangzhou.aliyuncs.com
fs.oss.sts.endpoint: <your-sts-endpoint>
# For the role of the STS token obtained from the STS endpoint, such as: acs:ram::123456789012:role/testrole
fs.oss.roleArn: <your-role-arn>

# Authentication (choose one option below)

# Option 1: Direct credentials
# Aliyun access key ID
fs.oss.accessKeyId: <your-access-key>
# Aliyun access key secret
fs.oss.accessKeySecret: <your-secret-key>

# Option 2: Secure credential provider
fs.oss.credentials.provider: <your-credentials-provider>
```
To avoid exposing sensitive access key information directly in the `server.yaml`, you can choose option 2 to use a credential provider by setting the `fs.oss.credentials.provider` property.

For example, to use environment variables for credential management:
```yaml
fs.oss.credentials.provider: com.aliyun.oss.common.auth.EnvironmentVariableCredentialsProvider
```
Then, set the following environment variables before starting the Fluss service:
```bash
export OSS_ACCESS_KEY_ID=<your-access-key>
export OSS_ACCESS_KEY_SECRET=<your-secret-key>
```
This approach enhances security by keeping sensitive credentials out of configuration files.

## Token-based Authentication

For a client to access the remote storage, such as reading a snapshot or tiered log, it must obtain an STS token from the Fluss cluster. You must configure both `fs.oss.sts.endpoint` and `fs.oss.roleArn`.

`fs.oss.sts.endpoint` is the STS endpoint to obtain an STS token, such as `sts.cn-hangzhou.aliyuncs.com` for the Hangzhou region. You can find endpoints for other regions in the [Aliyun STS Endpoint documentation](https://www.alibabacloud.com/help/en/ram/developer-reference/api-sts-2015-04-01-endpoint).
`fs.oss.roleArn` is for the role of the STS token obtained from the STS endpoint. It should be in the format of `acs:ram::<aliyun-account-id>:role/<role-name>`, such as `acs:ram::123456789012:role/testrole`. Since the client will use the STS token to read the remote storage, the role must be granted read permission for the remote storage. See more details in [AssumeRole](https://www.alibabacloud.com/help/en/ram/developer-reference/api-sts-2015-04-01-assumerole).

Apart from the above configurations, you can also define the configuration keys mentioned in the [Hadoop OSS documentation](http://hadoop.apache.org/docs/current/hadoop-aliyun/tools/hadoop-aliyun/index.html) in the Fluss' `server.yaml`. 
These configurations defined in the Hadoop OSS documentation are advanced configurations, usually used for performance tuning.