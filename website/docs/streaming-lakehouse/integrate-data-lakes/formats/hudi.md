---
title: Hudi
sidebar_position: 4
---

# Hudi

## Introduction

[Apache Hudi](https://hudi.apache.org/) is an open lakehouse table format that provides transactional writes, record-level updates, and incremental processing on data lakes.
To integrate Fluss with Hudi, you must enable lakehouse storage and configure Hudi as the lakehouse storage. For more details, see [Enable Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md#enable-lakehouse-storage).

Fluss tiers data to standard Hudi tables. Primary-key Fluss tables are written as Hudi Merge-On-Read tables, while Fluss log tables are written as Hudi Copy-On-Write tables.

## Dependencies

Apache Fluss publishes the Hudi lake connector to Maven Central:

| Artifact | Jar |
|----------|-----|
| Fluss Hudi lake connector | [fluss-lake-hudi-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-hudi/$FLUSS_VERSION$/fluss-lake-hudi-$FLUSS_VERSION$.jar) |

Maven coordinates:

```xml
<dependency>
  <groupId>org.apache.fluss</groupId>
  <artifactId>fluss-lake-hudi</artifactId>
  <version>$FLUSS_VERSION$</version>
</dependency>
```

Verify downloaded JARs against the [KEYS file](https://downloads.apache.org/incubator/fluss/KEYS) using the [verification instructions](/downloads#verifying-downloads).

## Version Compatibility

| Component | Required/Tested Versions |
|-----------|--------------------------|
| Hudi | 1.1.0 |
| Flink tiering runtime | Flink 1.20 with `hudi-flink1.20-bundle-1.1.0.jar` |

If you run the tiering service on another Flink version, use the Hudi Flink bundle that matches your Flink runtime.

## Configure Hudi as LakeHouse Storage

### Configure Hudi in Cluster Configurations

To configure Hudi as the lakehouse storage, configure the following options in `server.yaml`.
The example below uses Hudi's DFS catalog:

```yaml
# Hudi configuration
datalake.enabled: true
datalake.format: hudi

# Hudi catalog configuration, using DFS catalog mode
datalake.hudi.mode: dfs
datalake.hudi.catalog.path: /tmp/hudi
```

The directory configured by `datalake.hudi.catalog.path` must exist and be accessible to the Fluss servers.

Fluss processes Hudi configurations by stripping the `datalake.hudi.` prefix and passing the remaining options to Hudi.
For example, `datalake.hudi.catalog.path` is passed to Hudi as `catalog.path`.

Fluss supports the Hudi catalog modes implemented by the connector:

- `dfs`: Hudi DFS catalog. Tables are stored under `${catalog.path}/${database_name}/${table_name}`.
- `hms`: Hudi Hive Metastore catalog. Configure Hive Metastore access through Hudi/Hadoop options, for example `hive.conf.dir` or `hive.metastore.uris`.

Example using Hive Metastore catalog mode:

```yaml
datalake.enabled: true
datalake.format: hudi
datalake.hudi.mode: hms
datalake.hudi.catalog.path: hdfs:///warehouse/hudi
datalake.hudi.hive.metastore.uris: thrift://<hive-metastore-host>:9083
```

### Prepare Server-Side JARs

Put the following JARs into `${FLUSS_HOME}/plugins/hudi/` before starting the Fluss servers:

- [fluss-lake-hudi-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-hudi/$FLUSS_VERSION$/fluss-lake-hudi-$FLUSS_VERSION$.jar)
- The Hudi Flink bundle matching your runtime. For Flink 1.20, use [hudi-flink1.20-bundle-1.1.0.jar](https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-flink1.20-bundle/1.1.0/hudi-flink1.20-bundle-1.1.0.jar).
- Any Hadoop, Hive, or filesystem dependencies required by your Hudi catalog and table storage.

Restart Fluss after changing the plugin directory.

### Start Tiering Service to Hudi

Then, start the datalake tiering service to tier Fluss data to Hudi. For the general process, see [Start The Datalake Tiering Service](maintenance/tiered-storage/lakehouse-storage.md#start-the-datalake-tiering-service).

For Hudi, prepare the following JARs in `${FLINK_HOME}/lib`:

- Put the Fluss Flink connector JAR into `${FLINK_HOME}/lib`; pick the connector matching your Flink version (see [Dependencies](../../../engine-flink/getting-started.md#dependencies)). For Flink 1.20, use [fluss-flink-1.20-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar).
- If you are using [Amazon S3](http://aws.amazon.com/s3/), [Aliyun OSS](https://www.aliyun.com/product/oss), or [HDFS](https://hadoop.apache.org/docs/stable/) as Fluss's [remote storage](maintenance/tiered-storage/remote-storage.md), download the corresponding Fluss filesystem JAR (see the [Filesystems](../../../maintenance/filesystems/overview.md) section) and put it into `${FLINK_HOME}/lib`.
- Put [fluss-lake-hudi-$FLUSS_VERSION$.jar]($FLUSS_MAVEN_REPO_URL$/org/apache/fluss/fluss-lake-hudi/$FLUSS_VERSION$/fluss-lake-hudi-$FLUSS_VERSION$.jar) into `${FLINK_HOME}/lib`.
- Put the Hudi Flink bundle into `${FLINK_HOME}/lib`. For Flink 1.20, use [hudi-flink1.20-bundle-1.1.0.jar](https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-flink1.20-bundle/1.1.0/hudi-flink1.20-bundle-1.1.0.jar).
- Put any Hadoop, Hive, or filesystem dependencies required by your Hudi catalog and table storage into `${FLINK_HOME}/lib`.

Start the Flink tiering job with Hudi-specific configurations:

```shell
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format hudi \
    --datalake.hudi.mode dfs \
    --datalake.hudi.catalog.path /tmp/hudi
```

For Hive Metastore catalog mode:

```shell
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format hudi \
    --datalake.hudi.mode hms \
    --datalake.hudi.catalog.path hdfs:///warehouse/hudi \
    --datalake.hudi.hive.metastore.uris thrift://<hive-metastore-host>:9083
```

Then, the datalake tiering service continuously tiers data from Fluss to Hudi. The table option `table.datalake.freshness` controls the target freshness of Hudi tables. By default, the data freshness is 3 minutes.

## Table Mapping Between Fluss and Hudi

When a Fluss table is created with the option `'table.datalake.enabled' = 'true'` and configured with Hudi as the datalake format, Fluss automatically creates a corresponding Hudi table with the same database and table name.

For DFS catalog mode, the Hudi table path is `${catalog.path}/${database_name}/${table_name}` unless the Hudi table path is explicitly set by Hudi options.
For Hive Metastore catalog mode, the table path follows Hudi Hive catalog path inference.

The schema of the Hudi table matches the Fluss table schema, except for three system columns appended by Fluss:

| Column        | Type         | Description                                   |
|---------------|--------------|-----------------------------------------------|
| `__bucket`    | INT          | Fluss bucket identifier for data distribution |
| `__offset`    | BIGINT       | Fluss log offset for ordering and seeking     |
| `__timestamp` | TIMESTAMP(6) | Fluss log timestamp                           |

Do not use user columns named `__bucket`, `__offset`, or `__timestamp`. Hudi metadata column names starting with `_hoodie_` are also reserved.

### Primary Key Tables

Primary-key Fluss tables are mapped to Hudi Merge-On-Read tables:

- Hudi table type is set to `MERGE_ON_READ`.
- Hudi record key fields are derived from the Fluss primary key.
- Hudi bucket index is enabled and uses the Fluss bucket number.
- If Fluss bucket keys are configured, Hudi bucket index key fields are derived from the Fluss bucket keys. Otherwise Hudi uses the record key fields for bucket indexing.
- Writes use Hudi upsert semantics.

Example:

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

CREATE TABLE user_profiles (
    `user_id` BIGINT,
    `username` STRING,
    `email` STRING,
    `last_login` TIMESTAMP,
    `profile_data` STRING,
    PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s',
    'bucket.num' = '4',
    'bucket.key' = 'user_id',
    'hudi.precombine.field' = 'last_login'
);
```

`hudi.precombine.field` is a Hudi table option used by Hudi to order records with the same record key during upsert. Choose a field that reflects your update ordering.

Conceptually, Fluss creates a Hudi table with properties equivalent to:

```sql title="Hudi table properties"
'connector' = 'hudi',
'table.type' = 'MERGE_ON_READ',
'hoodie.datasource.write.recordkey.field' = 'user_id',
'index.type' = 'BUCKET',
'hoodie.bucket.index.hash.field' = 'user_id',
'hoodie.bucket.index.num.buckets' = '4',
'precombine.field' = 'last_login'
```

### Log Tables

Fluss log tables are mapped to Hudi Copy-On-Write tables:

- Hudi table type is set to `COPY_ON_WRITE`.
- Hudi bucket index is enabled and uses the Fluss bucket number.
- Writes use Hudi insert semantics.
- A Hudi record key field must be configured explicitly because Fluss log tables do not have a primary key.

Use the `hudi.` prefix to set Hudi table options. For log tables, configure `hudi.hoodie.datasource.write.recordkey.field`.
If you also configure Fluss bucket keys, the Hudi bucket index key must be a subset of the Hudi record key fields.

Example:

```sql title="Flink SQL"
CREATE TABLE access_logs (
    `event_id` STRING,
    `user_id` BIGINT,
    `action` STRING,
    `event_time` TIMESTAMP
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s',
    'bucket.num' = '8',
    'bucket.key' = 'event_id',
    'hudi.hoodie.datasource.write.recordkey.field' = 'event_id',
    'hudi.precombine.field' = 'event_time'
);
```

Conceptually, Fluss creates a Hudi table with properties equivalent to:

```sql title="Hudi table properties"
'connector' = 'hudi',
'table.type' = 'COPY_ON_WRITE',
'hoodie.datasource.write.recordkey.field' = 'event_id',
'index.type' = 'BUCKET',
'hoodie.bucket.index.hash.field' = 'event_id',
'hoodie.bucket.index.num.buckets' = '8',
'precombine.field' = 'event_time'
```

### Partitioned Tables

For Fluss partitioned tables, Fluss uses the Fluss partition keys as Hudi partition path fields.
The Hudi partition path field is managed by Fluss and should not be set manually.

```sql title="Flink SQL"
CREATE TABLE daily_orders (
    `order_id` BIGINT,
    `amount` DECIMAL(10, 2),
    `order_date` STRING,
    `updated_at` TIMESTAMP,
    PRIMARY KEY (`order_id`, `order_date`) NOT ENFORCED
) PARTITIONED BY (`order_date`)
WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s',
    'bucket.num' = '4',
    'bucket.key' = 'order_id',
    'hudi.precombine.field' = 'updated_at'
);
```

### Hudi Table Properties

You can specify Hudi table properties when creating a datalake-enabled Fluss table by using the `hudi.` prefix within the Fluss table properties clause.
Fluss strips the `hudi.` prefix before passing the option to Hudi.

```sql title="Flink SQL"
CREATE TABLE orders_with_lake (
    `order_id` BIGINT,
    `customer_id` BIGINT,
    `amount` DECIMAL(10, 2),
    `event_time` TIMESTAMP,
    PRIMARY KEY (`order_id`) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s',
    'hudi.precombine.field' = 'event_time',
    'hudi.write.batch.size' = '64',
    'hudi.compaction.delta_commits' = '1'
);
```

Fluss manages the following Hudi behaviors automatically: table type, record key fields for primary-key tables, index type, bucket index key fields, bucket count, and partition path fields. Do not override these options manually. The only exception is that log tables must provide `hoodie.datasource.write.recordkey.field`.

## Read Tables

### Union Read with Apache Flink

When a table has `'table.datalake.enabled' = 'true'`, its data exists in two layers:

- Fresh data is retained in Fluss.
- Historical data is tiered to Hudi.

To read the full dataset, query the Fluss table without any suffix. Fluss performs union read across the Hudi layer and the Fluss layer.

```sql title="Flink SQL"
-- Set execution mode to streaming or batch; batch mode is used here as an example.
SET 'execution.runtime-mode' = 'batch';

-- Query will union data from Fluss and Hudi.
SELECT COUNT(*) FROM access_logs;
```

Union read supports both batch and streaming modes for Hudi log tables and primary-key tables.
In streaming mode, Flink first reads the latest readable Hudi instant tiered by the tiering service, then switches to Fluss from the corresponding log offsets.

Key behavior for data retention:

- Expired Fluss log data, controlled by `table.log.ttl`, remains accessible through Hudi if it was tiered before expiration.
- Cleaned-up partitions in partitioned tables, controlled by `table.auto-partition.num-retention`, remain accessible through Hudi if they were tiered before cleanup.

### Read Hudi Tables Directly

The `$lake` table suffix in the Fluss catalog currently supports Paimon and Iceberg only. To read Hudi-only data, use a native Hudi catalog or any engine that supports Hudi tables.

Example using Hudi's Flink catalog with DFS catalog mode:

```sql title="Flink SQL"
CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'dfs',
    'catalog.path' = '/tmp/hudi'
);

USE CATALOG hudi_catalog;

SELECT COUNT(*) FROM fluss.access_logs;
```

For Hive Metastore catalog mode:

```sql title="Flink SQL"
CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'catalog.path' = 'hdfs:///warehouse/hudi',
    'hive.metastore.uris' = 'thrift://<hive-metastore-host>:9083'
);
```

The Hudi catalog options must match the `datalake.hudi.*` options used by Fluss and the tiering service.

## Data Type Mapping

When integrating with Hudi, Fluss converts Fluss data types to Flink data types accepted by Hudi's Flink catalog:

| Fluss Data Type               | Hudi/Flink Data Type                  | Notes |
|-------------------------------|---------------------------------------|-------|
| BOOLEAN                       | BOOLEAN                               |       |
| TINYINT                       | TINYINT                               |       |
| SMALLINT                      | SMALLINT                              |       |
| INT                           | INT                                   |       |
| BIGINT                        | BIGINT                                |       |
| FLOAT                         | FLOAT                                 |       |
| DOUBLE                        | DOUBLE                                |       |
| DECIMAL                       | DECIMAL                               |       |
| STRING                        | STRING                                |       |
| CHAR                          | STRING                                | Converted to STRING |
| DATE                          | DATE                                  |       |
| TIME                          | TIME                                  |       |
| TIMESTAMP                     | TIMESTAMP                             |       |
| TIMESTAMP WITH LOCAL TIMEZONE | TIMESTAMP WITH LOCAL TIME ZONE / BIGINT | Uses TIMESTAMP WITH LOCAL TIME ZONE in DFS catalog mode and BIGINT in HMS catalog mode |
| BINARY                        | BINARY                                |       |
| BYTES                         | BYTES                                 |       |
| ARRAY                         | ARRAY                                 |       |
| MAP                           | MAP                                   |       |
| ROW                           | ROW                                   |       |

## Maintenance and Optimization

### Auto Compaction

The table option `table.datalake.auto-compaction` enables automatic Hudi compaction for primary-key tables.
It is disabled by default.

When auto compaction is enabled for a Hudi Merge-On-Read table, the tiering service schedules, executes, and commits Hudi compaction during tiering.
Compaction does not apply to Hudi Copy-On-Write log tables.

```sql title="Flink SQL"
CREATE TABLE user_profiles (
    `user_id` BIGINT,
    `username` STRING,
    `updated_at` TIMESTAMP,
    PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.auto-compaction' = 'true',
    'hudi.precombine.field' = 'updated_at',
    'hudi.compaction.delta_commits' = '1'
);
```

The tiering service also accepts Hudi compaction timeout options through Hudi table properties:

| Option | Default | Description |
|--------|---------|-------------|
| `hudi.fluss.tiering.compaction.complete-timeout` | `30min` | Maximum time for a tiering writer to wait for Hudi compaction execution to finish. |
| `hudi.fluss.tiering.compaction.shutdown-timeout` | `30s` | Maximum time to wait when shutting down the Hudi compaction executor. |

### Commit Metadata

Fluss adds metadata to Hudi commits for traceability and recovery:

- `commit-user`: Set to `__fluss_lake_tiering` to identify Hudi instants committed by Fluss.
- `fluss-offsets`: A Fluss-managed offset metadata file path. The file records the Fluss bucket log-end offsets covered by the Hudi instant.

The Fluss lake snapshot ID corresponds to the committed Hudi instant time. During recovery, Fluss finds the latest completed Hudi instant whose `commit-user` is `__fluss_lake_tiering` and uses its extra metadata to recover any lake snapshot that was committed to Hudi but not yet committed back to Fluss.

## Current Limitations

- Hudi lake catalog supports `dfs` and `hms` modes.
- Hudi table alteration through Fluss lake catalog is not supported yet.
- The Fluss catalog `$lake` suffix does not expose Hudi-only tables yet. Use Hudi's native catalog to read Hudi-only data.
- Fluss log tables must configure `hudi.hoodie.datasource.write.recordkey.field`.
- Hudi bucket key fields must be scalar types with deterministic string representations. Composite and binary types such as ARRAY, MAP, ROW, BINARY, and BYTES are not supported as Hudi bucket keys.
- For composite Hudi bucket keys, values containing `,` or colliding with Hudi's reserved placeholders `__null__` and `__empty__` are rejected to keep Fluss bucket routing aligned with Hudi bucket IDs.
- Sorted lake reads for primary-key union read are supported only for Hudi Merge-On-Read tables and require the query projection to include all Hudi record key fields.
