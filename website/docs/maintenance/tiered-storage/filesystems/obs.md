---
title: HuaweiCloud OBS
sidebar_position: 6
---

# HuaweiCloud OBS

[HuaweiCloud Object Storage Service](https://www.huaweicloud.com/product/obs.html) (HuaweiCloud OBS) is an enterprise-grade object storage solution delivering industry-leading scalability, data durability, security, and cost-efficiency. Trusted by organizations across finance, healthcare, manufacturing, and media, OBS enables you to securely store, manage, analyze, and protect unlimited data volumes for diverse scenarios like AI training, data lakes, multi-cloud backup, and real-time media processing.

## Install OBS Plugin Manually

HuaweiCloud OBS support is not included in the default Fluss distribution. To enable OBS support, you need to manually install the filesystem plugin into Fluss.

1. **Prepare the plugin JAR**: 

   - Download the `fluss-fs-obs-$FLUSS_VERSION$.jar` from the [Maven Repository](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-obs/$FLUSS_VERSION$/fluss-fs-obs-$FLUSS_VERSION$.jar).
   
2. **Place the plugin**: Place the plugin JAR file in the `${FLUSS_HOME}/plugins/obs/` directory:
   ```bash
   mkdir -p ${FLUSS_HOME}/plugins/obs/
   cp fluss-fs-obs-$FLUSS_VERSION$.jar ${FLUSS_HOME}/plugins/obs/
   ```

3. Restart Fluss if the cluster is already running to ensure the new plugin is loaded.

## Configurations setup

To enabled HuaweiCloud OBS as remote storage, there are some required configurations that must be added to Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss
remote.data.dir: obs://<your-bucket>/path/to/remote/storage
# obs endpoint, such as: https://obs.cn-north-4.myhuaweicloud.com
fs.obs.endpoint: <obs-endpoint-hostname>
# OBS region, such as: cn-north-4
fs.obs.region: <your-obs-region>

# Authentication (choose one option below)

# Option 1: Direct credentials
# HuaweiCloud access key
fs.obs.access.key: <your-access-key>
# HuaweiCloud secret key
fs.obs.secret.key: <your-secret-key>

# Option 2: Secure credential provider
fs.obs.security.provider: <your-credentials-provider>
```
To avoid exposing sensitive access key information directly in the `server.yaml`, you can choose option2 to use a credential provider by setting the `fs.obs.security.provider` property.

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