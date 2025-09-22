---
title: Google Cloud Storage
sidebar_position: 5
---

# Google Cloud Storage

[Google Cloud Storage](https://cloud.google.com/storage) is a RESTful online file storage web service for storing and accessing data on Google Cloud Platform infrastructure. It's designed for online backup and archiving of data and application objects.

## Manual Plugin Installation

Google Cloud Storage support is not included in the default Fluss distribution. To enable GS support, you need to manually install the filesystem plugin.

### Download and Install the Plugin

1. **Prepare the plugin JAR**: 

   - Download the `fluss-fs-gs-$FLUSS_VERSION$.jar` from the [Maven Repository](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-gs/$FLUSS_VERSION$/fluss-fs-gs-$FLUSS_VERSION$.jar).
   
   - Build it from source by following the [building guide](/community/dev/building).
      ```bash
      cd fluss-filesystems/fluss-fs-gs
     ./mvnw clean package -DskipTests
      ```
     The compiled JAR will be located at `fluss-filesystems/fluss-fs-gs/target/fluss-fs-gs-$FLUSS_VERSION$.jar`.

2. **Install the plugin**: Place the downloaded JAR file in the `${FLUSS_HOME}/plugins/gs/` directory:
   ```bash
   mkdir -p ${FLUSS_HOME}/plugins/gs/
   cp fluss-fs-gs-$FLUSS_VERSION$.jar ${FLUSS_HOME}/plugins/gs/
   ```

3. **Restart Fluss**: Restart your Fluss cluster to load the new plugin.

## Configuration Setup

To enable Google Cloud Storage as remote storage, add the required configurations to Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss
remote.data.dir: gs://<your-bucket>/path/to/remote/storage

# Authentication (choose one option below)

# Option 1: Service Account Key File
gs.auth.service.account.json.keyfile: /path/to/service-account-key.json

# Option 2: Application Default Credentials (for GCE instances)
gs.auth.type: APPLICATION_DEFAULT_CREDENTIALS

# Option 3: Service Account Email (for Compute Engine)
gs.auth.service.account.email: your-service-account@your-project.iam.gserviceaccount.com

# Optional: Project ID (if not specified in service account)
gs.project.id: your-gcp-project-id

# Optional: Custom endpoint (for testing with emulators)
fs.gs.storage.http.endpoint: https://storage.googleapis.com
```

### Authentication Options

#### Option 1: Service Account Key File
This is the most common method for authentication:
1. Create a service account in the Google Cloud Console
2. Download the JSON key file
3. Set the path to the key file in the configuration

#### Option 2: Application Default Credentials
For applications running on Google Cloud Platform (GCE, GKE, Cloud Functions, etc.), you can use the default service account:
```yaml
gs.auth.type: APPLICATION_DEFAULT_CREDENTIALS
```

#### Option 3: Service Account Email
For Compute Engine instances with attached service accounts:
```yaml
gs.auth.service.account.email: your-service-account@your-project.iam.gserviceaccount.com
```

### Additional Configuration Options

You can configure additional GCS-specific options by prefixing them with `gs.` or `fs.gs.`:

```yaml
# Storage class for new objects
fs.gs.storage.class: STANDARD

# Request timeout
fs.gs.http.connect-timeout: 60000
fs.gs.http.read-timeout: 60000

# Retry configuration
fs.gs.max.retry.attempts: 10
```

## Permissions

Ensure your service account has the following IAM permissions:
- `storage.objects.create`
- `storage.objects.delete` 
- `storage.objects.get`
- `storage.objects.list`
- `storage.buckets.get`

You can use the predefined `Storage Object Admin` role, or create a custom role with these specific permissions.



