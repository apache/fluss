---
title: Tencent Cloud COSN
sidebar_position: 7
---

# Tencent Cloud COSN

## COSN: Cloud Object Storage

[Tencent Cloud Object Storage](https://cloud.tencent.com/product/cos) (COS) is a secure, stable, and efficient cloud distributed storage service that is widely used globally, particularly in China. It provides reliable cloud object storage for a variety of use cases. This plugin enables the `Fluss` system to seamlessly use Tencent Cloud COSN as its remote storage.

## Configurations Setup

To enable COSN as remote storage, you must add some required configurations to the `Fluss` `server.yaml` file.

```yaml
# The directory used for Fluss remote storage
remote.data.dir: cosn://<your-bucket>/path/to/remote/storage

# The region where your COS bucket is located, e.g., ap-guangzhou
fs.cosn.bucket.region: <your-cos-region>

# --- STS Temporary Token Authentication Specific Configuration (Recommended) ---
# The STS service endpoint to obtain a temporary token, 
# default: sts.tencentcloudapi.com
fs.cosn.sts.endpoint: <your-sts-endpoint>
# The region used to initialize the STS client, e.g., ap-guangzhou
fs.cosn.sts.region: <your-sts-region>
# The Amazon Resource Name (ARN) of the role to be assumed, e.g., qcs::cam::uin/1000090866654:roleName/fluss-test
fs.cosn.roleArn: <your-role-arn>

# #################################################
# ## Authentication Options (Choose one)         ##
# #################################################

# Option 1: Direct Credentials (Also required for calling STS)
# Your Tencent Cloud API permanent Secret ID
fs.cosn.secret.id: <your-access-key>
# Your Tencent Cloud API permanent Secret Key
fs.cosn.secret.key: <your-secret-key>

# Option 2: Secure Credential Provider
fs.cosn.credentials.provider: <your-credentials-provider>