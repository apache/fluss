---
title: Google Cloud Storage
sidebar_position: 7
---

# Google Cloud Storage

[Google Cloud Storage](https://cloud.google.com/storage) (GCS) is a managed object storage service for unstructured data, providing high availability, strong consistency, and integration with the broader Google Cloud ecosystem.

## Dependencies

Apache Fluss publishes the Google Cloud Storage filesystem plugin to Maven Central:

| Artifact | Jar |
|----------|-----|
| Fluss Google Cloud Storage filesystem plugin | [fluss-fs-gs-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-fs-gs/$FLUSS_VERSION$/fluss-fs-gs-$FLUSS_VERSION$.jar) |

Maven coordinates:

```xml
<dependency>
  <groupId>org.apache.fluss</groupId>
  <artifactId>fluss-fs-gs</artifactId>
  <version>$FLUSS_VERSION$</version>
</dependency>
```

Verify downloaded JARs against the [KEYS file](https://downloads.apache.org/incubator/fluss/KEYS) using the [verification instructions](/downloads#verifying-downloads).

## Install GS FS Plugin Manually

Google Cloud Storage support is not included in the default Fluss distribution. To enable Google Cloud Storage support, you need to manually install the filesystem plugin into Fluss.

1. **Prepare the plugin JAR**:

    - Download `fluss-fs-gs-$FLUSS_VERSION$.jar` from the [Dependencies](#dependencies) section above.

2. **Place the plugin**: Place the plugin JAR file in the `${FLUSS_HOME}/plugins/gs/` directory:
   ```bash
   mkdir -p ${FLUSS_HOME}/plugins/gs/
   cp fluss-fs-gs-$FLUSS_VERSION$.jar ${FLUSS_HOME}/plugins/gs/
   ```

3. Restart Fluss if the cluster is already running to ensure the new plugin is loaded.

## Configurations setup

To enable Google Cloud Storage as remote storage, add the required configuration to Fluss' `server.yaml`. The Fluss plugin accepts both `gs.` and `fs.gs.` prefixes; entries are forwarded to the underlying `gcs-connector` with the `fs.gs.` prefix.

### Service account key file

```yaml
# The dir used as remote storage of Fluss, using the gs:// URI scheme
remote.data.dir: gs://my-fluss-bucket/path
# Use a service account JSON key file for authentication
fs.gs.auth.type: SERVICE_ACCOUNT_JSON_KEYFILE
# Path to the service account JSON key file on the Fluss host
fs.gs.auth.service.account.json.keyfile: /etc/fluss/secrets/gcs-sa.json
```

### Application Default Credentials

When running on GCE, GKE, or any environment with [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials) available (for example via Workload Identity), the plugin can pick up credentials automatically:

```yaml
remote.data.dir: gs://my-fluss-bucket/path
# Defer to the GCE/GKE metadata server or ADC for credentials
fs.gs.auth.type: APPLICATION_DEFAULT
```

Other valid values for `fs.gs.auth.type` include `COMPUTE_ENGINE`, `USER_CREDENTIALS`, `ACCESS_TOKEN_PROVIDER`, and `UNAUTHENTICATED`. See the [GCS Hadoop connector documentation](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md) for the full list of `fs.gs.*` options.
