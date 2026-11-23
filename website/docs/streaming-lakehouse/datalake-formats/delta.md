---
title: Delta Lake
sidebar_position: 5
---

# Delta Lake

## Introduction

[Delta Lake](https://delta.io/) is an open table format that adds ACID transactions on top of Parquet files. Spark, Trino, Flink, DuckDB and many other engines can read it.
To integrate Fluss with Delta Lake, you must enable lakehouse storage and configure Delta Lake as the lakehouse storage. For more details, see [Deploying Lakestream](../../install-deploy/deploying-streaming-lakehouse.md).

Fluss tiers data to standard Delta tables through [Delta Kernel](https://github.com/delta-io/delta/tree/master/kernel). Any Delta reader can read these tables without Fluss JARs.

> **NOTE**: Fluss v$FLUSS_VERSION_SHORT$ currently supports tiering Log Tables to Delta Lake only.

## Dependencies

Apache Fluss publishes the Delta Lake connector to Maven Central:

| Artifact | Jar |
|----------|-----|
| Fluss Delta Lake connector | [fluss-lake-delta-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-delta/$FLUSS_VERSION$/fluss-lake-delta-$FLUSS_VERSION$.jar) |

Maven coordinates:

```xml
<dependency>
  <groupId>org.apache.fluss</groupId>
  <artifactId>fluss-lake-delta</artifactId>
  <version>$FLUSS_VERSION$</version>
</dependency>
```

Verify downloaded JARs using the [verification instructions](/downloads#verifying-downloads).

## Version Compatibility

| Component | Required/Tested Versions |
|-----------|--------------------------|
| Delta Kernel | 4.4.1, bundled in `fluss-lake-delta` |
| Java | 11 or later, in every process that loads the Delta Lake connector |
| Hadoop | 3.x client, provided by the user (see [Prepare Hadoop Dependencies](#prepare-hadoop-dependencies)), tested with 3.4.3 |

Delta Kernel is compiled for Java 11, so the Fluss servers and the Flink clusters that tier or read Delta tables must run on Java 11 or later.

## Configure Delta Lake as LakeHouse Storage

### Configure Delta Lake in Cluster Configurations

To configure Delta Lake as the lakehouse storage, configure the following options in `server.yaml`:

```yaml
# Delta Lake configuration
datalake.enabled: true
datalake.format: delta

# The root path of the Delta tables
datalake.delta.warehouse: /tmp/delta
```

Fluss strips the `datalake.delta.` prefix and uses the remaining options as follows:

| Option | Default | Description |
|--------|---------|-------------|
| `warehouse` | (none) | Required. The root path of the Delta tables. The table `db.tbl` is stored at `<warehouse>/db/tbl`. |
| `write.batch-size` | `100000` | The number of rows a tiering writer buffers before it writes them out as Parquet files. A larger value gives fewer and bigger files at the cost of memory. Only the tiering service reads it, so pass it as `--datalake.delta.write.batch-size` when you [start the tiering service](#start-tiering-service-to-delta-lake). A table can override it with the table property `write.batch-size`. |

All options, the two above included, are also set on the Hadoop `Configuration` that Delta Kernel uses for file system access.
Set file system options such as credentials here, for example `datalake.delta.fs.s3a.access.key` for an `s3a://` warehouse.

### Prepare Hadoop Dependencies

Delta Kernel reads and writes files through Hadoop file systems, but the Delta Lake connector doesn't bundle the Hadoop client API (`hadoop-client-api`).
Provide Hadoop 3.x in every process that loads the connector. The Fluss servers and the Flink clusters get it in different ways.

If the warehouse is on an object store, both also need the Hadoop file system for its scheme. For `s3a://`, see [Amazon S3 and S3-Compatible Storage](#amazon-s3-and-s3-compatible-storage).

#### Fluss Servers

The Fluss servers load `${FLUSS_HOME}/plugins/delta/` in an isolated class loader to create and alter Delta tables.
Hadoop finds its file system implementations through service files inside its JARs, and this class loader reads such files only from the plugin directory.
Copy the shaded Hadoop client JARs from a Hadoop 3.x package into the plugin directory:

```bash
# Download and extract Hadoop
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz
tar -xzf hadoop-3.4.3.tar.gz

# Copy the Hadoop client JARs into the Delta Lake plugin directory
cp hadoop-3.4.3/share/hadoop/client/hadoop-client-api-3.4.3.jar \
   hadoop-3.4.3/share/hadoop/client/hadoop-client-runtime-3.4.3.jar \
   ${FLUSS_HOME}/plugins/delta/
```

`HADOOP_CLASSPATH` doesn't replace these JARs, even if it's already set for HDFS remote storage. Without them, creating a table fails with `No FileSystem for scheme "file"`.

Restart the Fluss servers after changing the plugin directory.

#### Flink Clusters

The Flink cluster that runs the tiering service and the Flink clusters that read Delta tiered tables with union read load the connector from `${FLINK_HOME}/lib`.
Export `HADOOP_CLASSPATH` before starting them. Flink adds it to its classpath at startup.

**Option 1: Use an Existing Hadoop Environment (Recommended)**

```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```

**Option 2: Use a Downloaded Hadoop Package**

```bash
# Set HADOOP_HOME to the Hadoop package extracted above
export HADOOP_HOME=$(pwd)/hadoop-3.4.3

# Set HADOOP_CLASSPATH using the downloaded Hadoop
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
```

#### Amazon S3 and S3-Compatible Storage

An `s3a://` warehouse needs `hadoop-aws`, its dependencies, and the AWS SDK v2 bundle that matches the `hadoop-aws` version.
The Hadoop package ships `hadoop-aws` and its dependencies in `share/hadoop/common/lib`, but not the AWS SDK bundle.
For Hadoop 3.4.3, download [bundle-2.35.4.jar](https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar).

On the Fluss servers, copy these JARs into the plugin directory, next to the Hadoop client JARs.
`hadoop-aws` uses the unshaded Hadoop dependencies, which the shaded client JARs don't provide, and the Delta log store for S3 uses Guava:

```bash
HADOOP_LIB=hadoop-3.4.3/share/hadoop/common/lib
cp ${HADOOP_LIB}/hadoop-aws-3.4.3.jar \
   ${HADOOP_LIB}/analyticsaccelerator-s3-1.3.1.jar \
   ${HADOOP_LIB}/commons-lang3-3.18.0.jar \
   ${HADOOP_LIB}/commons-io-2.16.1.jar \
   ${HADOOP_LIB}/commons-collections4-4.4.jar \
   ${HADOOP_LIB}/guava-32.0.1-jre.jar \
   ${HADOOP_LIB}/failureaccess-1.0.1.jar \
   bundle-2.35.4.jar \
   ${FLUSS_HOME}/plugins/delta/
```

On the Flink clusters, the `hadoop classpath` of the Hadoop 3.4.3 package already includes `hadoop-aws` and its dependencies. Put `bundle-2.35.4.jar` into `${FLINK_HOME}/lib`.

Then configure the file system in `server.yaml`:

```yaml
datalake.delta.warehouse: s3a://<bucket>/<path>
datalake.delta.fs.s3a.access.key: <access-key>
datalake.delta.fs.s3a.secret.key: <secret-key>
# For S3-compatible storage, also set the endpoint and path-style access
datalake.delta.fs.s3a.endpoint: <endpoint>
datalake.delta.fs.s3a.path.style.access: true
```

Pass the same options to the tiering service as `--datalake.delta.<option> <value>`.
For union read, the Flink clusters also need the credentials, see [Union Read with Apache Flink](#union-read-with-apache-flink).

### Start Tiering Service to Delta Lake

Then, start the datalake tiering service to tier Fluss data to Delta Lake. For the general process, see [Deploying Lakestream](../../install-deploy/deploying-streaming-lakehouse.md).

For Delta Lake, prepare the following JARs in `${FLINK_HOME}/lib`:

- Put the Fluss Flink connector JAR into `${FLINK_HOME}/lib`; pick the connector matching your Flink version (see [Dependencies](../../engine-flink/getting-started.md#dependencies)). For Flink 1.20, use [fluss-flink-1.20-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar).
- If you are using [Amazon S3](http://aws.amazon.com/s3/), [Aliyun OSS](https://www.aliyun.com/product/oss), or [HDFS](https://hadoop.apache.org/docs/stable/) as Fluss's [remote storage](../../maintenance/tiered-storage/remote-storage.md), download the corresponding Fluss filesystem JAR (see the [Filesystems](../../maintenance/tiered-storage/filesystems/overview.md) section) and put it into `${FLINK_HOME}/lib`.
- Put [fluss-lake-delta-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-delta/$FLUSS_VERSION$/fluss-lake-delta-$FLUSS_VERSION$.jar) into `${FLINK_HOME}/lib`.
- Provide Hadoop to the Flink cluster, see [Flink Clusters](#flink-clusters).
- For an `s3a://` warehouse, also put the AWS SDK bundle into `${FLINK_HOME}/lib`, see [Amazon S3 and S3-Compatible Storage](#amazon-s3-and-s3-compatible-storage).

Start the Flink tiering job with Delta-specific configurations:

```shell
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format delta \
    --datalake.delta.warehouse /tmp/delta
```

Then, the datalake tiering service continuously tiers data from Fluss to Delta Lake. The table option `table.datalake.freshness` controls the target freshness of Delta tables. By default, the data freshness is 3 minutes.

## Table Mapping Between Fluss and Delta Lake

When a Fluss log table is created or altered with the option `'table.datalake.enabled' = 'true'` and Delta Lake is the datalake format, Fluss creates a Delta table at `<warehouse>/<database_name>/<table_name>`.

- The schema of the Delta table matches the Fluss table schema and contains only the user-defined columns. Fluss doesn't add system columns.
- The Fluss partition keys become the partition columns of the Delta table.
- The table has the `domainMetadata` table feature, which Fluss uses to record the tiering progress (see [Commit Metadata](#commit-metadata)).
- If a Delta table already exists at the path, for example from an earlier attempt to create the same Fluss table, Fluss reuses it when its schema and partition columns match the Fluss table and it has the `domainMetadata` feature. Otherwise, creating the table fails. When a new Fluss table is created, the existing Delta table must also be empty: no data files and no `fluss.tiering` domain metadata. Dropping a Fluss table doesn't drop its Delta table, so drop or move the Delta table, or use another table name, before you create a Fluss table with the same name again.

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

CREATE TABLE access_logs (
    `event_id` STRING,
    `user_id` BIGINT,
    `action` STRING,
    `event_time` TIMESTAMP(6)
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

Creating a primary key table with `'table.datalake.enabled' = 'true'`, or enabling datalake on an existing primary key table, fails, since primary key tables aren't supported yet.

### Partitioned Tables

For Fluss partitioned tables, the partition keys become Delta partition columns, and each Fluss partition maps to the Delta files with the same partition values.

```sql title="Flink SQL"
CREATE TABLE daily_access_logs (
    `event_id` STRING,
    `user_id` BIGINT,
    `action` STRING,
    `dt` STRING
) PARTITIONED BY (`dt`)
WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s',
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'day'
);
```

Partition keys can be of type CHAR, STRING, BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DATE, TIMESTAMP and TIMESTAMP_LTZ.

### Delta Table Properties

Table properties starting with `delta.` are passed to the Delta table as they are, since Delta's own properties already use this prefix. For example:

```sql title="Flink SQL"
CREATE TABLE access_logs_with_retention (
    `event_id` STRING,
    `user_id` BIGINT,
    `action` STRING
) WITH (
    'table.datalake.enabled' = 'true',
    'delta.logRetentionDuration' = 'interval 60 days',
    'write.batch-size' = '50000'
);
```

All other Fluss table properties are written to the Delta table with the `fluss.` prefix, for example `fluss.table.datalake.freshness`. They show how the table is configured in Fluss, and Fluss never reads them back.
When `ALTER TABLE ... SET` or `RESET` changes one of these properties, Fluss sets or removes the matching `fluss.*` property of the Delta table.

Fluss rejects:

- `delta.*` properties that turn on column mapping, such as `delta.columnMapping.mode` or `delta.enableIcebergCompatV2`. Delta Kernel can't write to column-mapped tables yet.
- Changes of `delta.*` properties after the table is created.

## Read Tables

### Union Read with Apache Flink

When a table has `'table.datalake.enabled' = 'true'`, its data exists in two layers:

- Fresh data is retained in Fluss.
- Historical data is tiered to Delta Lake.

To read the full dataset, query the Fluss table without any suffix. Fluss performs union read across the Delta layer and the Fluss layer.
The Flink cluster that runs the query needs `fluss-lake-delta-$FLUSS_VERSION$.jar` in `${FLINK_HOME}/lib` and Hadoop, see [Flink Clusters](#flink-clusters).

```sql title="Flink SQL"
-- Set execution mode to streaming or batch; batch mode is used here as an example.
SET 'execution.runtime-mode' = 'batch';

-- Query will union data from Fluss and Delta Lake.
SELECT COUNT(*) FROM access_logs;
```

Fluss passes the `datalake.delta.*` options of `server.yaml` to the query as table options, except options whose names contain `key`, `secret`, or `password`, such as `fs.s3a.access.key`.
Set those options on the Fluss catalog with the `delta.` prefix:

```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'localhost:9123',
  'delta.fs.s3a.access.key' = '<access-key>',
  'delta.fs.s3a.secret.key' = '<secret-key>'
);
```

Fluss reads the Delta version it registered as the latest lake snapshot, so the Delta data lines up with the tiered Fluss offsets.
If a newer Delta version holds the same Fluss data, for example after another engine ran `OPTIMIZE`, Fluss reads the newer version instead.
Column projection is pushed down to the Parquet reader. Filters and limits aren't pushed down to Delta Lake yet.

Key behavior for data retention:

- Expired Fluss log data, controlled by `table.log.ttl`, remains accessible through Delta Lake if it was tiered before expiration.
- Cleaned-up partitions in partitioned tables, controlled by `table.auto-partition.num-retention`, remain accessible through Delta Lake if they were tiered before cleanup.

### Read Delta Tables Directly

The `$lake` table suffix in the Fluss catalog currently supports Paimon and Iceberg only. To read the Delta table alone, use any engine that reads Delta Lake, with the table path:

```sql title="Spark SQL"
SELECT COUNT(*) FROM delta.`/tmp/delta/fluss/access_logs`;
```

```sql title="DuckDB"
SELECT COUNT(*) FROM delta_scan('/tmp/delta/fluss/access_logs');
```

## Data Type Mapping

When integrating with Delta Lake, Fluss converts Fluss data types to Delta data types:

| Fluss Data Type               | Delta Data Type | Notes |
|-------------------------------|-----------------|-------|
| BOOLEAN                       | BOOLEAN         |       |
| TINYINT                       | BYTE            |       |
| SMALLINT                      | SHORT           |       |
| INT                           | INTEGER         |       |
| BIGINT                        | LONG            |       |
| FLOAT                         | FLOAT           |       |
| DOUBLE                        | DOUBLE          |       |
| DECIMAL                       | DECIMAL         |       |
| STRING                        | STRING          |       |
| CHAR                          | STRING          | Converted to STRING |
| DATE                          | DATE            |       |
| TIME                          | Not supported   | Delta Lake has no time type. Creating the table fails. |
| TIMESTAMP                     | TIMESTAMP_NTZ   | Microsecond precision |
| TIMESTAMP WITH LOCAL TIMEZONE | TIMESTAMP       | Microsecond precision, adjusted to UTC |
| BINARY                        | BINARY          |       |
| BYTES                         | BINARY          |       |
| ARRAY                         | ARRAY           |       |
| MAP                           | MAP             |       |
| ROW                           | STRUCT          |       |

Timestamps with a precision above 6 are truncated to microseconds, because Delta Lake stores microseconds.

## Maintenance and Optimization

### Compaction

Fluss doesn't compact Delta tables, and it rejects `'table.datalake.auto-compaction' = 'true'` for them.
Use Spark `OPTIMIZE` to compact the table and `VACUUM` to delete unused files. Both keep the tiering progress that Fluss records in the table.

delta-rs (as of 1.6.6) can read Delta tiered tables but can't write them: append, `OPTIMIZE` and `VACUUM` fail with `Unsupported table features required: [DomainMetadata]`.

### Writes by Other Engines

A Delta tiered table is a standard Delta table, so other engines can write to it. Fluss can only keep the table in line with the Fluss log if they follow these rules:

- **Allowed**: `OPTIMIZE` and `ZORDER`, `VACUUM`, checkpoints and log cleanup, and changes of other table properties outside `fluss.*`, except the ones listed under **Not allowed**.
- **Not allowed**: data changes (`INSERT`, `DELETE`, `UPDATE`, `MERGE`, overwrite), schema or partition changes, changes to the `fluss.tiering` domain metadata, CHECK constraints (`delta.constraints.*`), and column mapping (`delta.columnMapping.mode`). Delta Kernel can't write to a table with CHECK constraints or column mapping, so tiering the table fails.

Before each commit, the tiering service checks that the schema and partition columns of the Delta table still match the Fluss table, and fails the commit with a message naming the difference.
The tiering service then tries the table again in later rounds, until the Delta table matches the Fluss table again.
Data written by other engines can't be detected reliably, so don't write data to the table outside Fluss.

### Retention

Union read compares the Delta version registered by Fluss with the latest version, so keep `delta.logRetentionDuration` (30 days by default) longer than the longest gap between two Fluss commits to the table.
If log cleanup has removed the registered version, union read fails with a message naming `delta.logRetentionDuration`.

`VACUUM` retention (`delta.deletedFileRetentionDuration`) doesn't depend on Fluss commits. It only has to cover the time a read takes, as for any Delta reader.

### Commit Metadata

Each tiering commit writes the data files and the tiering progress in one Delta commit:

- The `fluss.tiering` domain metadata holds the Fluss snapshot properties as JSON. Its `fluss-offsets` entry is the path of a Fluss-managed file that records the Fluss bucket log end offsets covered by the commit.
- The `engineInfo` of the commit is `Kernel-<version>/Apache-Fluss`, for example `Kernel-4.4.1/Apache-Fluss`.

Domain metadata survives checkpoints and log cleanup. When Fluss recovers a lake snapshot that was committed to Delta Lake but not yet registered in Fluss, it reads the `fluss.tiering` domain metadata of the latest Delta version and walks back only as far as the oldest retained version.

## Current Limitations

- Only log tables can be tiered to Delta Lake. Primary key tables aren't supported yet.
- Schema changes such as `ALTER TABLE ... ADD COLUMN` aren't supported for Delta tiered tables yet. Bucket count changes are rejected for tables with bucket keys.
- Column-mapped Delta tables aren't supported.
- The Fluss catalog `$lake` suffix doesn't expose Delta tables yet. Use a Delta Lake reader to read Delta-only data.
- Union read doesn't push filters or limits down to Delta Lake yet.
- Fluss doesn't compact Delta tables. Use Spark `OPTIMIZE` and `VACUUM`.
- Commits through a catalog, such as Unity Catalog, aren't supported. Fluss commits to the Delta table path directly.
- Delta Kernel escapes the directory names of `TIMESTAMP` and `TIMESTAMP_LTZ` partitions twice, for example `t=2026-11-19%2010%253A00%253A00.000000`. Delta Lake readers aren't affected, because they take file paths and partition values from the Delta log. Tools that derive partition values from directory names, such as Hive-style partition discovery, read the escaped text instead, for example `2026-11-19 10%3A00%3A00.000000`.
