---
title: JuiceFS
sidebar_position: 8
---

# JuiceFS

[JuiceFS](https://juicefs.com) is a distributed POSIX-compatible file system built on top of object storage and a separate metadata engine (Redis, TiKV, MySQL, PostgreSQL, etc.). It ships with a Hadoop-compatible Java SDK, which Fluss uses to store snapshots for Primary-Key Tables and tiered log segments for Log Tables on a JuiceFS volume.

## Install JuiceFS Plugin on Server Nodes

JuiceFS support is not included in the default Fluss distribution. To enable JuiceFS support on the server side (CoordinatorServer and every TabletServer), you need to manually install the filesystem plugin into Fluss.

1. **Prepare the plugin JAR**:

   - Download the `fluss-fs-juicefs-$FLUSS_VERSION$.jar` from the [Maven Repository](https://repo1.maven.org/maven2/org/apache/fluss/fluss-fs-juicefs/$FLUSS_VERSION$/fluss-fs-juicefs-$FLUSS_VERSION$.jar).

2. **Place the plugin**: Place the plugin JAR file in the `${FLUSS_HOME}/plugins/juicefs/` directory:
   ```bash
   mkdir -p ${FLUSS_HOME}/plugins/juicefs/
   cp fluss-fs-juicefs-$FLUSS_VERSION$.jar ${FLUSS_HOME}/plugins/juicefs/
   ```

3. Restart Fluss if the cluster is already running to ensure the new plugin is loaded.

:::note
The `${FLUSS_HOME}/plugins/` directory is only consulted by the Fluss server's plugin manager. Fluss clients (Flink connector, Spark connector, standalone applications, ...) do not load plugins from this directory — see [Client-side Setup](#client-side-setup) for how to enable JuiceFS on the client.
:::

## Prerequisite: Create a JuiceFS Volume

Before Fluss can use JuiceFS as remote storage, a JuiceFS volume must be created in advance using the JuiceFS CLI (`juicefs format ...`). The volume creation step configures:

- The **metadata engine** (e.g. Redis, TiKV, MySQL, PostgreSQL).
- The backing **object storage** and its credentials (baked into the volume so individual clients do not need to re-configure them).
- The **volume name**, which becomes the authority in the `jfs://<volume-name>/...` URI.

Refer to the [JuiceFS Getting Started guide](https://juicefs.com/docs/community/getting-started/installation) for volume creation instructions. Make sure the metadata engine and the object storage are reachable from every Fluss node (CoordinatorServer, TabletServer, and clients).

## Configurations setup

To enable JuiceFS as remote storage, add the following required configurations to Fluss' `server.yaml`:

```yaml
# The dir that used to be as the remote storage of Fluss
remote.data.dir: jfs://<your-volume-name>/path/to/remote/storage
# JuiceFS metadata engine address of the pre-created volume,
# e.g. redis://<host>:<port>/<db>, tikv://..., mysql://..., postgres://...
juicefs.meta: <your-meta-url>
```

Only Fluss configuration keys with the prefix `fs.jfs.` or `juicefs.` are forwarded from `server.yaml` to the underlying Hadoop `Configuration` consumed by the JuiceFS SDK. Any other keys are ignored by the JuiceFS plugin.

The plugin also auto-injects the following defaults when they are not set by the user, so you normally do **not** need to configure them explicitly:

```yaml
fs.jfs.impl: io.juicefs.JuiceFileSystem
fs.jfs.impl.disable.cache: false
```

A typical configuration that additionally enables local disk cache and an access log looks like:

```yaml
remote.data.dir: jfs://<your-volume-name>/path/to/remote/storage
juicefs.meta: redis://<host>:<port>/<db>

# Local cache directories (create them in advance with mode 0777).
# Multiple paths can be separated by ":", wildcards such as "*" are supported.
juicefs.cache-dir: /data*/jfscache
# Total local cache capacity across all cache dirs, in MiB.
juicefs.cache-size: 1024
# Path of the access log file (auto-rotated, latest 7 files retained).
juicefs.access-log: /tmp/juicefs.access.log
```

## Authentication

Unlike the OSS / S3 / COS plugins, Fluss does **not** perform any STS or delegation-token exchange for JuiceFS. The JuiceFS client authenticates locally on each node:

- Against the **metadata engine** using the URL configured via `juicefs.meta`.
- Against the backing **object storage** using the credentials that were provided when the volume was formatted (or via optional `juicefs.access-key` / `juicefs.secret-key` overrides, if applicable).

The Fluss server returns an empty placeholder security token to clients when they request access to the remote storage. This means every process that needs to read remote data (CoordinatorServer, TabletServer, and every client) must:

1. Have direct network access to the JuiceFS metadata engine and object storage.
2. Have the JuiceFS plugin available in its own runtime. Servers load the plugin from `${FLUSS_HOME}/plugins/juicefs/` — see [Install JuiceFS Plugin on Server Nodes](#install-juicefs-plugin-on-server-nodes). Clients use a different mechanism — see [Client-side Setup](#client-side-setup).

## Client-side Setup

Any application that reads Fluss remote data through the Fluss client will fetch tiered log segments and Primary-Key Table snapshots from JuiceFS via a `FileSystem` instance created inside the client process. Enabling JuiceFS on the client therefore requires two independent steps: making the plugin visible to the client's classloader, and forwarding the JuiceFS configuration through `client.fs.*` keys.

### 1. Add the JuiceFS plugin JAR to the application classpath

Client process needs to bundle `fluss-fs-juicefs-$FLUSS_VERSION$.jar` on its own application classpath. The plugin JAR already shades `io.juicefs.JuiceFileSystem` and the required JuiceFS Hadoop SDK classes, so no additional JuiceFS dependency is needed.

Connector-specific placement:

- **Flink connector**: add `fluss-fs-juicefs-$FLUSS_VERSION$.jar` into Flink's `lib/` directory on every JobManager and TaskManager.
- **Spark connector**: add `--jars /path/to/fluss-fs-juicefs-$FLUSS_VERSION$.jar` to `spark-submit` / `spark-sql`, or install it into Spark's `jars/` directory on every driver and executor node.

### 2. Configure JuiceFS via `client.fs.*` keys

`FlussConnection` only forwards Fluss configuration entries whose keys start with `client.fs.`, and the `client.fs.` prefix is stripped before the map is handed to `FileSystem.initialize`. The JuiceFS plugin in turn only picks up keys starting with `fs.jfs.` or `juicefs.`. In practice this means every JuiceFS knob you want to expose on the client must be written as:

```
client.fs.<juicefs-or-fs.jfs-key> = <value>
```

For example, `client.fs.juicefs.access-key=xxxx` is forwarded to the JuiceFS plugin as `juicefs.access-key=xxxx`.

Raw `juicefs.*` or `fs.jfs.*` keys that are **not** prefixed with `client.fs.` are silently dropped by `FlussConnection` and never reach the JuiceFS plugin. This is the most common client-side misconfiguration.

Because Fluss does not perform any credential delegation for JuiceFS (see [Authentication](#authentication)), each client host must also have valid credentials to the object storage backing the volume. For example, `client.fs.juicefs.access-key` / `client.fs.juicefs.secret-key`.

### Connector examples

**Flink SQL / Table API.** Connector options declared in `WITH (...)` are propagated into the underlying client `Configuration`, so JuiceFS options can be set per-table:

```sql
CREATE TABLE fluss_table (
  ...
) WITH (
  'connector' = 'fluss',
  'bootstrap.servers' = '<coordinator-host>:<port>',
  'client.fs.juicefs.meta' = 'redis://<host>:<port>/<db>',
  'client.fs.juicefs.cache-dir' = '/data*/jfscache',
  'client.fs.juicefs.cache-size' = '1024'
);
```

Alternatively, set them globally in `flink-conf.yaml` / at cluster level so they apply to every Fluss table.

**Spark.** Pass them as Fluss catalog options, either on the command line or in `spark-defaults.conf`:

```bash
spark-sql \
  --jars /path/to/fluss-fs-juicefs-$FLUSS_VERSION$.jar \
  --conf spark.sql.catalog.fluss_catalog=org.apache.fluss.spark.catalog.FlussCatalog \
  --conf spark.sql.catalog.fluss_catalog.bootstrap.servers=<coordinator-host>:<port> \
  --conf spark.sql.catalog.fluss_catalog.client.fs.juicefs.meta=redis://<host>:<port>/<db>
```

## Advanced Configurations

Apart from the configurations above, any other JuiceFS Hadoop SDK configuration key can be defined in Fluss' `server.yaml` as long as it starts with `fs.jfs.` or `juicefs.` — the plugin will forward it to the underlying Hadoop configuration. Refer to the [JuiceFS Hadoop Java SDK documentation](https://juicefs.com/docs/community/hadoop_java_sdk) for the complete parameter reference. Commonly used tuning knobs include:

- **Cache**: `juicefs.cache-dir`, `juicefs.cache-size`, `juicefs.cache-full-block`, `juicefs.free-space`, `juicefs.attr-cache`, `juicefs.entry-cache`, `juicefs.dir-entry-cache`
- **I/O**: `juicefs.max-uploads`, `juicefs.max-downloads`, `juicefs.memory-size`, `juicefs.prefetch`, `juicefs.io-retries`, `juicefs.upload-limit`, `juicefs.download-limit`
- **Miscellaneous**: `juicefs.access-log`, `juicefs.debug`, `juicefs.block.size`, `juicefs.bucket`

These configurations are advanced options that are usually used for performance tuning.
