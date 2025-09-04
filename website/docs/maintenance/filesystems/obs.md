---
title: HuaweiCloud OBS
sidebar_position: 6
---

# HuaweiCloud OBS

[HuaweiCloud Object Storage Service](https://www.huaweicloud.com/eu/product/obs.html) (HuaweiCloud OBS) is an enterprise-grade object storage solution delivering industry-leading scalability, data durability, security, and cost-efficiency. Trusted by organizations across finance, healthcare, manufacturing, and media, OBS enables you to securely store, manage, analyze, and protect unlimited data volumes for diverse scenarios like AI training, data lakes, multi-cloud backup, and real-time media processing.

## Configurations setup

To enable HuaweiCloud OBS as remote storage, there are some required configurations that must be added to Fluss' `server.yaml`:

```yaml
# The OBS bucket path used as the remote storage for Fluss
remote.data.dir: obs://<your-bucket>/path/to/remote/storage
# The OBS endpoint, such as: https://obs.cn-north-4.myhuaweicloud.com
fs.obs.endpoint: <obs-endpoint-hostname>
# The OBS region, such as: cn-north-4
fs.obs.region: <your-obs-region>

# Authentication (choose one option below)

# Option 1: Direct credentials
# HuaweiCloud Access Key
fs.obs.access.key: <your-access-key>
# HuaweiCloud Secret Key
fs.obs.secret.key: <your-secret-key>

# Option 2: Secure credential provider
fs.obs.security.provider: <your-credentials-provider>
```
To avoid exposing sensitive access key information directly in the `server.yaml`, you can choose option 2 to use a credential provider by setting the `fs.obs.security.provider` property.

For example, to use environment variables for credential management:
```yaml
fs.obs.security.provider: com.obs.services.EnvironmentVariableObsCredentialsProvider
```
Then, set the following environment variables before starting the Fluss service:
```bash
export OBS_ACCESS_KEY_ID=<your-access-key>
export OBS_SECRET_ACCESS_KEY=<your-secret-key>
```
This approach enhances security by keeping sensitive credentials out of configuration files.
