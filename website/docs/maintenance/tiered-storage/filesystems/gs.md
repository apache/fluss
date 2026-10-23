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

Verify downloaded JARs using the [verification instructions](/downloads#verifying-downloads).

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

To enable Google Cloud Storage as remote storage, add the required configuration to Fluss' `server.yaml`. The Fluss plugin accepts both `gs.` and `fs.gs.` prefixes; entries are forwarded to the underlying `gcs-connector` with the `fs.gs.` prefix. Choose one authentication method below.

### Application Default Credentials

Recommended on GCE and GKE. When running in an environment with [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials) available (for example via Workload Identity), the plugin can pick up credentials automatically. When `fs.gs.auth.type` is omitted, this method is used only if `fs.gs.auth.service.account.json.keyfile` is not set. Set `fs.gs.auth.type` to `APPLICATION_DEFAULT` to select it even when a key file is present.

The credentials must be a service account: the GCE or GKE metadata server, or `GOOGLE_APPLICATION_CREDENTIALS` pointing at a service account key. User credentials, such as those from `gcloud auth application-default login`, are rejected. They keep the scopes granted at login, so the read-only Cloud Storage scope would not apply to the token sent to clients.

```yaml
# The dir used as remote storage of Fluss, using the gs:// URI scheme
remote.data.dir: gs://my-fluss-bucket/path
# Defer to the GCE/GKE metadata server or ADC for credentials
fs.gs.auth.type: APPLICATION_DEFAULT
```

### Compute Engine

Read credentials from the GCE instance metadata server.

```yaml
remote.data.dir: gs://my-fluss-bucket/path
fs.gs.auth.type: COMPUTE_ENGINE
```

### Service account key file

When `fs.gs.auth.type` is omitted, this key file is still used if the path below is set. The bundled GCS connector does not read `fs.gs.auth.type` and follows the same rule.

```yaml
remote.data.dir: gs://my-fluss-bucket/path
# Use a service account JSON key file for authentication
fs.gs.auth.type: SERVICE_ACCOUNT_JSON_KEYFILE
# Path to the service account JSON key file on the Fluss host
fs.gs.auth.service.account.json.keyfile: /etc/fluss/secrets/gcs-sa.json
```

Other valid values for `fs.gs.auth.type` include `USER_CREDENTIALS`, `ACCESS_TOKEN_PROVIDER`, and `UNAUTHENTICATED`. See the [GCS Hadoop connector documentation](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md) for the full list of `fs.gs.*` options.
