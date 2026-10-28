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

To enable Google Cloud Storage as remote storage, add the required configuration to Fluss' `server.yaml`. The Fluss plugin accepts both `gs.` and `fs.gs.` prefixes; entries are forwarded to the underlying `gcs-connector` with the `fs.gs.` prefix.

The bundled `gcs-connector` (2.x) selects credentials in this order and does not read `fs.gs.auth.type`:

1. A service account key configured with `fs.gs.auth.service.account.json.keyfile`.
2. The file referenced by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
3. The GCE or GKE metadata server, for example with Workload Identity.

### Metadata server

Recommended on GCE and GKE. Leave the key file and `GOOGLE_APPLICATION_CREDENTIALS` unset, and the attached service account is used:

```yaml
# The dir used as remote storage of Fluss, using the gs:// URI scheme
remote.data.dir: gs://my-fluss-bucket/path
```

### Service account key file

```yaml
remote.data.dir: gs://my-fluss-bucket/path
# Path to the service account JSON key file on the Fluss host
fs.gs.auth.service.account.json.keyfile: /etc/fluss/secrets/gcs-sa.json
```

See the [GCS Hadoop connector documentation](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md) for the full list of `fs.gs.*` options.

### Client access to tiered data

Clients such as the Flink and Spark connectors read tiered data directly from Google Cloud Storage. When a client has no service account key file or access token provider of its own, Fluss servers issue it a short-lived access token for the server's service account, limited to the `https://www.googleapis.com/auth/devstorage.read_only` scope. Clients never receive the key file.

- The token can read every bucket the server's service account can read, not only `remote.data.dir`. Grant that service account access to the Fluss bucket only.
- Only service accounts can issue the token. Servers that authenticate with user credentials (OAuth client settings, or a `GOOGLE_APPLICATION_CREDENTIALS` file created by `gcloud auth application-default login`), with service account impersonation, or with a custom access token provider cannot issue it; clients of such a cluster must configure their own credentials.
- Access tokens are valid for at most one hour. If a client fails to renew its token, it retries after `client.filesystem.security.token.renewal.backoff` (default one hour), so consider lowering that option on clients.
