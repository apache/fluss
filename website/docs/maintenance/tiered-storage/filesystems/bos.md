---
title: Baidu Cloud BOS
sidebar_position: 8
---

# Baidu Cloud BOS

[Baidu Object Storage](https://cloud.baidu.com/product/bos.html) (BOS) is the object storage service of Baidu AI Cloud.
Unlike the other cloud object stores, BOS issues temporary credentials through the Baidu Cloud STS (BCE STS), which is not compatible
with AWS STS. For this reason, the BOS plugin does not use a Hadoop connector: it talks to BOS directly through the
[Baidu BOS Java SDK](https://github.com/baidubce/bce-sdk-java) and implements the Fluss security token flow on top of BCE STS.

## Dependencies

Apache Fluss publishes the BOS filesystem plugin to Maven Central:

| Artifact | Jar |
|----------|-----|
| Fluss BOS filesystem plugin | [fluss-fs-bos-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-fs-bos/$FLUSS_VERSION$/fluss-fs-bos-$FLUSS_VERSION$.jar) |

Maven coordinates:

```xml
<dependency>
  <groupId>org.apache.fluss</groupId>
  <artifactId>fluss-fs-bos</artifactId>
  <version>$FLUSS_VERSION$</version>
</dependency>
```

Verify downloaded JARs using the [verification instructions](/downloads#verifying-downloads).

## Install BOS Plugin Manually

Baidu BOS support is not included in the default Fluss distribution. To enable BOS support, you need to manually install the filesystem plugin into Fluss.

1. **Prepare the plugin JAR**:

   - Download `fluss-fs-bos-$FLUSS_VERSION$.jar` from the [Dependencies](#dependencies) section above.

2. **Place the plugin**: Place the plugin JAR file in the `${FLUSS_HOME}/plugins/bos/` directory:
   ```bash
   mkdir -p ${FLUSS_HOME}/plugins/bos/
   cp fluss-fs-bos-$FLUSS_VERSION$.jar ${FLUSS_HOME}/plugins/bos/
   ```

3. Restart Fluss if the cluster is already running to ensure the new plugin is loaded.

:::note
Clients that read data from BOS with a security token issued by the server (for example Flink jobs and the tiering service) must also have the
plugin JAR on their classpath, e.g. in `${FLINK_HOME}/lib`, because the security token receiver is loaded from the application class loader.
:::

## Configurations setup

To enable Baidu BOS as remote storage, there are some required configurations that must be added to Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss
remote.data.dir: bos://<your-bucket>/path/to/remote/storage
# BOS endpoint, such as: http://hkg.bcebos.com
fs.bos.endpoint: <bos-endpoint>
# BOS region, such as: hkg. It is used to scope the temporary credentials issued to clients.
fs.bos.region: <your-bos-region>
# Baidu Cloud access key
fs.bos.access.key: <your-access-key>
# Baidu Cloud secret key
fs.bos.secret.key: <your-secret-key>
```

Keys can be written with either the `fs.bos.` or the `bos.` prefix, and `-` and `.` are interchangeable in key names.

### Optional configurations

| Option | Default | Description |
|--------|---------|-------------|
| `fs.bos.sts.endpoint` | `https://sts.bj.baidubce.com` | The BCE STS endpoint used to issue temporary credentials to clients. |
| `fs.bos.sts.duration.seconds` | `43200` | Validity of the issued temporary credentials, in seconds. |
| `fs.bos.sts.permissions` | `READ,LIST` | Permissions granted to the issued temporary credentials. Clients only need to read remote data. |
| `fs.bos.multipart.part.size` | `16777216` | Size in bytes of one part of a multipart upload. One part is buffered in memory per open output stream. |
| `fs.bos.read.window.size` | `8388608` | Size in bytes of a ranged read request issued by an input stream. |
| `fs.bos.max.connections` | `200` | Maximum number of HTTP connections of the BOS client. |

### Security token

Fluss clients do not need any credentials for BOS. The server exchanges its long-term access key and secret key for a short-lived credential
scoped to the bucket through BCE STS and hands it to the clients, which renew it before it expires. Clients must be able to reach the BOS endpoint.

:::warning
The issued credential is scoped to the whole bucket, not to the `remote.data.dir` prefix. Use a dedicated bucket for Fluss if other data lives in the same account.
:::

## Limitations

Listings on BOS can keep returning objects that were just deleted for a few seconds. `exists` and `delete` confirm every listed object with a HEAD request, but `listStatus` returns the listing as is.

## Logging

The BOS SDK logs every `404` response, for example the `HEAD` request that checks whether a key exists, at `INFO` level together with a stack trace (`Unable to execute HTTP request`). These messages are expected and harmless, but they can be frequent. To silence them, raise the level of the SDK's HTTP client logger in `log4j.properties`:

```properties
logger.bce_http.name=com.baidubce.http.BceHttpClient
logger.bce_http.level=WARN
```
