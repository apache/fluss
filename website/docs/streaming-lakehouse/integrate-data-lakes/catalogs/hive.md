---
title: Hive Metastore
sidebar_position: 3
---

# Hive Metastore

## Introduction

The **Hive Metastore (HMS)** is a central metadata repository commonly used in Apache Hadoop and other big data ecosystems to store schema and metadata information for tables. Apache Iceberg provides native integration with Hive Metastore, storing Iceberg table names and metadata locations directly within HMS.

This guide explains how to configure Fluss to use Hive Metastore as its Iceberg catalog. For general Iceberg integration details (table mapping, data types, limitations), see [Iceberg](../formats/iceberg.md).

## How It Works

When Fluss is configured with Hive Metastore as its Iceberg catalog:

1. Fluss manages Iceberg databases and tables via HMS thrift API.
2. The [tiering service](maintenance/tiered-storage/lakehouse-storage.md#start-the-datalake-tiering-service) writes parquet data files to HDFS (or S3/OSS) and commits table snapshots via the Hive Metastore client.
3. Other query engines (such as Spark, Trino, Flink, and StarRocks) configured with Hive catalog can discover and query these Iceberg tables directly from HMS.

## Prerequisites

### Running Hive Metastore

Ensure you have a running Hive Metastore service. By default, HMS listens on thrift port `9083` (e.g., `thrift://<metastore-host>:9083`).

### Prepare Required JARs

Because Hive catalog implementation is not bundled in `iceberg-core`, you must supply the Hive catalog and Hadoop client dependencies.

#### For Fluss Servers (Coordinator & Tablet Servers)

Download and place the following JARs in the `FLUSS_HOME/plugins/iceberg/` directory:

1. **Iceberg Hive Runtime**: [iceberg-hive-runtime-1.10.1.jar](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.10.1/iceberg-hive-runtime-1.10.1.jar)
2. **Pre-bundled Hadoop JAR** (if not using an existing Hadoop environment): [hadoop-apache-3.3.5-2.jar](https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar)

#### For the Flink Tiering Service

Place the same JAR files in the `${FLINK_HOME}/lib` directory.

### Hadoop Classpath Configuration

Both Fluss and Flink must be able to load Hadoop-related configuration (e.g., `core-site.xml`, `hdfs-site.xml`) and classes to resolve HDFS file paths.

**Option 1: Export Hadoop Environment Classpath (Recommended)**

Export `HADOOP_CLASSPATH` before launching Fluss servers and Flink:

```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```

**Option 2: Place Hadoop XML Configs**

Ensure that your `core-site.xml` and `hdfs-site.xml` files are copied to the configuration classpath of both Fluss and Flink.

## Configure Fluss with Hive Metastore

### Cluster Configuration

Add the following configuration parameters to your `server.yaml`:

```yaml
datalake.format: iceberg
datalake.iceberg.type: hive
datalake.iceberg.uri: thrift://<hive-metastore-host>:9083
datalake.iceberg.warehouse: hdfs://<namenode-host>:9000/user/hive/warehouse
```

:::note
If your Hive warehouse is located on cloud object storage (like Amazon S3 or Aliyun OSS), set `datalake.iceberg.warehouse` to the corresponding cloud URI (e.g., `s3://<your-bucket>/warehouse`) and configure the required filesystem integration. See [AWS Glue](glue.md) for AWS credentials setup.
:::

### Start Tiering Service

Follow the [Iceberg tiering service setup](../formats/iceberg.md#start-tiering-service-to-iceberg) instructions to prepare the environment. Launch the Flink tiering job with Hive Metastore catalog configurations:

```bash
${FLINK_HOME}/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers <coordinator-host>:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type hive \
    --datalake.iceberg.uri thrift://<hive-metastore-host>:9083 \
    --datalake.iceberg.warehouse hdfs://<namenode-host>:9000/user/hive/warehouse
```

## Usage Example

### Create a Datalake-Enabled Table

Create a Fluss table with data lake tiering enabled using Flink SQL:

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

CREATE TABLE daily_events (
    `event_id` BIGINT,
    `event_type` STRING,
    `severity` STRING,
    `event_date` STRING,
    PRIMARY KEY (`event_id`) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.freshness' = '30s'
);
```

Fluss will create a corresponding Iceberg table inside the Hive Metastore under the database matching your Fluss namespace. The metadata will point to the HDFS warehouse directory.

### Query Data with Spark

Since HMS manages the metadata, you can register HMS as an Iceberg catalog in Apache Spark and query the tiered table immediately:

```sql title="Spark SQL"
-- Query the tiered Iceberg table from Hive Metastore catalog
SELECT * FROM hive_catalog.fluss_database.daily_events;
```

## Further Reading

- [Iceberg Integration](../formats/iceberg.md) - Table mapping, data types, and configurations.
- [Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md) - General tiered storage overview.
- [Iceberg Hive Catalog Docs](https://iceberg.apache.org/docs/latest/hive/) - Official Iceberg Hive documentation.
