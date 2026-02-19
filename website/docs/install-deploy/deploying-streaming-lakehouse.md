---
title: "Deploying Streaming Lakehouse"
sidebar_position: 6
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Deploying Streaming Lakehouse

This guide covers how to deploy a Fluss cluster with Streaming Lakehouse capabilities. For conceptual overview, see [Lakehouse Overview](../streaming-lakehouse/overview.md).

## Prerequisites

1. A running Fluss cluster (see [Deploying Distributed Cluster](deploying-distributed-cluster.md))
2. A running Flink cluster (for the Tiering Service)
3. Access to a data lake storage system (S3, HDFS, OSS, etc.)

## Cluster Configuration

You can enable Lakehouse storage through:
1. **Static configuration**: Configure in `server.yaml` before starting the cluster
2. **Dynamic configuration**: Enable at runtime using the `set_cluster_configs` procedure

### Method 1: Static Configuration

Configure lakehouse settings in `server.yaml` on all Fluss servers (CoordinatorServer and TabletServer).

<Tabs groupId="datalake-format">
<TabItem value="paimon" label="Paimon" default>

```yaml title="server.yaml"
datalake.format: paimon
datalake.paimon.metastore: filesystem
datalake.paimon.warehouse: /path/to/paimon/warehouse
```

For Hive catalog:
```yaml title="server.yaml"
datalake.format: paimon
datalake.paimon.metastore: hive
datalake.paimon.uri: thrift://<hive-metastore-host>:<port>
datalake.paimon.warehouse: hdfs:///path/to/warehouse
```

</TabItem>
<TabItem value="iceberg" label="Iceberg">

```yaml title="server.yaml"
datalake.format: iceberg
datalake.iceberg.catalog-impl: org.apache.iceberg.jdbc.JdbcCatalog
datalake.iceberg.name: fluss_catalog
datalake.iceberg.uri: jdbc:postgresql://postgres-host:5432/iceberg
datalake.iceberg.jdbc.user: iceberg
datalake.iceberg.jdbc.password: iceberg
datalake.iceberg.warehouse: s3://bucket/iceberg
datalake.iceberg.io-impl: org.apache.iceberg.aws.s3.S3FileIO
```

</TabItem>
<TabItem value="lance" label="Lance">

```yaml title="server.yaml"
datalake.format: lance
datalake.lance.warehouse: s3://bucket/lance
```

</TabItem>
</Tabs>

### Method 2: Dynamic Configuration

Enable lakehouse settings at runtime using Flink SQL:

```sql title="Flink SQL"
USE fluss_catalog;

CALL sys.set_cluster_configs(
  config_pairs => 'datalake.format', 'paimon',
                  'datalake.paimon.metastore', 'filesystem',
                  'datalake.paimon.warehouse', '/path/to/warehouse'
);
```

See [set_cluster_configs](../engine-flink/procedures.md#set_cluster_configs) for more details.

## Adding Required JARs

### Fluss Server JARs

Add JARs to `${FLUSS_HOME}/plugins/<format>/` based on your configuration:

<Tabs groupId="datalake-format">
<TabItem value="paimon" label="Paimon" default>

| Scenario | Required JAR |
|----------|--------------|
| Paimon with S3 | `paimon-s3-<version>.jar` |
| Paimon with OSS | `paimon-oss-<version>.jar` |
| Paimon Hive catalog | Flink SQL Hive connector JAR |

</TabItem>
<TabItem value="iceberg" label="Iceberg">

| Scenario | Required JAR |
|----------|--------------|
| Iceberg with S3 | `iceberg-aws-<version>.jar`, `iceberg-aws-bundle-<version>.jar` |
| Iceberg JDBC catalog | PostgreSQL/MySQL JDBC driver |

</TabItem>
<TabItem value="lance" label="Lance">

Lance support is built into the Fluss distribution. Cloud storage credentials are configured via storage-options.

</TabItem>
</Tabs>

For HDFS, see the [HDFS setup guide](../maintenance/tiered-storage/filesystems/hdfs.md).

## Starting the Tiering Service

The Tiering Service is a Flink job that continuously tiers data from Fluss to the data lake. For architecture details, see [Tiering Service](../streaming-lakehouse/tiering-service.md).

### Prerequisites

1. Download [fluss-flink-tiering-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-tiering/$FLUSS_VERSION$/fluss-flink-tiering-$FLUSS_VERSION$.jar)

### Flink JARs

Add the following to `${FLINK_HOME}/lib`:

<Tabs groupId="datalake-format">
<TabItem value="paimon" label="Paimon" default>

- [fluss-flink-1.20-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar)
- [fluss-lake-paimon-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-paimon/$FLUSS_VERSION$/fluss-lake-paimon-$FLUSS_VERSION$.jar)
- [paimon-bundle-$PAIMON_VERSION$.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-bundle/$PAIMON_VERSION$/paimon-bundle-$PAIMON_VERSION$.jar)
- [flink-shaded-hadoop-2-uber-*.jar](https://flink.apache.org/downloads/)
- Paimon filesystem JAR (e.g., `paimon-s3-<version>.jar` for S3)

</TabItem>
<TabItem value="iceberg" label="Iceberg">

- [fluss-flink-1.20-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar)
- [fluss-lake-iceberg-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-iceberg/$FLUSS_VERSION$/fluss-lake-iceberg-$FLUSS_VERSION$.jar)
- [iceberg-flink-runtime-1.20-*.jar](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/)
- Hadoop client JARs
- JDBC driver (if using JDBC catalog)

</TabItem>
<TabItem value="lance" label="Lance">

- [fluss-flink-1.20-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar)
- [fluss-lake-lance-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-lance/$FLUSS_VERSION$/fluss-lake-lance-$FLUSS_VERSION$.jar)

</TabItem>
</Tabs>

If using S3, OSS, or HDFS as Fluss's [remote storage](../maintenance/tiered-storage/remote-storage.md), also add the corresponding [Fluss filesystem JAR](/downloads#filesystem-jars).

### Start the Service

<Tabs groupId="datalake-format">
<TabItem value="paimon" label="Paimon" default>

```shell
${FLINK_HOME}/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format paimon \
    --datalake.paimon.metastore filesystem \
    --datalake.paimon.warehouse /tmp/paimon
```

</TabItem>
<TabItem value="iceberg" label="Iceberg">

```shell
${FLINK_HOME}/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.catalog-impl org.apache.iceberg.jdbc.JdbcCatalog \
    --datalake.iceberg.name fluss_catalog \
    --datalake.iceberg.uri "jdbc:postgresql://postgres:5432/iceberg" \
    --datalake.iceberg.jdbc.user iceberg \
    --datalake.iceberg.jdbc.password iceberg \
    --datalake.iceberg.warehouse "s3://bucket/iceberg"
```

</TabItem>
<TabItem value="lance" label="Lance">

```shell
${FLINK_HOME}/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format lance \
    --datalake.lance.warehouse s3://bucket/lance
```

</TabItem>
</Tabs>

:::note
- You must pass all `datalake.*` options that were set in `server.yaml` as command-line arguments
- For S3/cloud storage, include additional flags like `--datalake.paimon.s3.endpoint`, `--datalake.paimon.s3.access-key`, etc.
- The Tiering Service is stateless—you can run multiple instances for scalability
- Use `-D` to pass Flink configurations (e.g., `-Dparallelism.default=3`)
- For complete examples with S3 configuration, see the [Lakehouse Quickstart](../quickstart/lakehouse.md)
:::

## Enabling Lakehouse for Tables

Create tables with lakehouse storage enabled:

```sql title="Flink SQL"
CREATE TABLE my_table (
    id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '1min'
);
```

## Verification

1. Check the Tiering Service job is running in Flink Web UI
2. After the freshness interval, query the lake table:

```sql title="Flink SQL"
-- Lake-only query
SELECT * FROM my_table$lake;

-- Union Read (real-time + historical)
SELECT * FROM my_table;
```

See [Union Read](../streaming-lakehouse/union-read.md) for more details.
