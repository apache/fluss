---
title: Iceberg
sidebar_position: 2
---

# Iceberg

## Introduction

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge analytic datasets. It provides ACID transactions, schema evolution, and efficient data organization for data lakes.
To integrate Fluss with Iceberg, you must enable lakehouse storage and configure Iceberg as the lakehouse storage. For more details, see [Enable Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md#enable-lakehouse-storage).

> **NOTE**: Iceberg requires JDK11 or later. Please ensure that both your Fluss deployment and the Flink cluster used for tiering services are running on JDK11+.


## Configure Iceberg as LakeHouse Storage

### Configure Iceberg in Cluster Configurations

To configure Iceberg as the lakehouse storage, you must configure the following configurations in `server.yaml`:
```yaml
# Iceberg configuration
datalake.format: iceberg

# the catalog config about Iceberg, assuming using Hadoop catalog,
datalake.iceberg.type: hadoop
datalake.iceberg.warehouse: /tmp/iceberg
```

#### Configuration Processing

Fluss processes Iceberg configurations by stripping the `datalake.iceberg.` prefix and uses the stripped configurations (without the prefix `datalake.iceberg.`) to initialize the Iceberg catalog.

This approach enables passing custom configurations for Iceberg catalog initialization. Check out the [Iceberg Catalog Properties](https://iceberg.apache.org/docs/1.10.0/configuration/#catalog-properties) for more details on available catalog configurations.

#### Supported Catalog Types

Fluss supports all Iceberg-compatible catalog types:

**Built-in Catalog Types:**
- `hive` - Hive Metastore catalog
- `hadoop` - Hadoop catalog
- `rest` - REST catalog
- `glue` - AWS Glue catalog
- `nessie` - Nessie catalog
- `jdbc` - JDBC catalog

**Custom Catalog Implementation:**
For other catalog types, you can use:
```yaml
datalake.iceberg.catalog-impl: <your_iceberg_catalog_impl_class_name>
```

**Example - Snowflake Catalog:**
```yaml
datalake.iceberg.catalog-impl: org.apache.iceberg.snowflake.SnowflakeCatalog
```

#### Prerequisites

##### 1. Hadoop Dependencies Configuration

Some catalogs (such as `hadoop`, `hive` catalog) require Hadoop-related classes. Please ensure Hadoop-related classes are in your classpath.

**Option 1: Use Existing Hadoop Environment (Recommended)**
```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```
Export Hadoop classpath before starting Fluss. This allows Fluss to automatically load Hadoop dependencies from the machine.

**Option 2: Download Pre-bundled Hadoop JAR**
- Download: [hadoop-apache-3.3.5-2.jar](https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar)
- Place the JAR file into `FLUSS_HOME/plugins/iceberg/` directory

**Option 3: Download Complete Hadoop Package**
- Download: [hadoop-3.3.5.tar.gz](https://archive.apache.org/dist/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz)
- Extract and configure HADOOP_CLASSPATH:
```bash
# Download and extract Hadoop
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz
tar -xzf hadoop-3.3.5.tar.gz

# Set HADOOP_HOME to the extracted directory
export HADOOP_HOME=$(pwd)/hadoop-3.3.5

# Set HADOOP_CLASSPATH using the downloaded Hadoop
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
```

##### 2. Custom Catalog Implementations

Fluss only bundles catalog implementations included in the `iceberg-core` module. For any other catalog implementations not bundled within the `iceberg-core` module (e.g., Hive Catalog), you must place the corresponding JAR file into `FLUSS_HOME/plugins/iceberg/`.

##### 3. Version Compatibility

The Iceberg version that Fluss bundles is based on `1.10.0`. Please ensure the JARs you add are compatible with `Iceberg-1.10.0`.

#### Important Notes

- Ensure all JAR files are compatible with Iceberg 1.10.0
- If using an existing Hadoop environment, it's recommended to use the `HADOOP_CLASSPATH` environment variable
- Configuration changes take effect after restarting the Fluss service

### Start Tiering Service to Iceberg

To tier Fluss's data to Iceberg, you must start the datalake tiering service. For guidance, you can refer to [Start The Datalake Tiering Service](maintenance/tiered-storage/lakehouse-storage.md#start-the-datalake-tiering-service). Although the example uses Paimon, the process is also applicable to Iceberg.

#### Prerequisites: Hadoop Dependencies

**Important**: Iceberg has a strong dependency on Hadoop. You must ensure Hadoop-related classes are available in the classpath before starting the tiering service.

##### Option 1: Use Existing Hadoop Environment (Recommended)

If you already have a Hadoop environment installed:

```bash
# Export Hadoop classpath
export HADOOP_CLASSPATH=`hadoop classpath`
```

Export Hadoop classpath before starting Flink cluster. This approach allows Flink to automatically load Hadoop dependencies from your existing installation.

##### Option 2: Download Pre-bundled Hadoop JARs

If you don't have a Hadoop environment, download the required JARs:

```bash
# Download the pre-bundled Hadoop JAR
wget https://repo1.maven.org/maven2/io/trino/hadoop/hadoop-apache/3.3.5-2/hadoop-apache-3.3.5-2.jar

# Place it in Flink's lib directory
cp hadoop-apache-3.3.5-2.jar ${FLINK_HOME}/lib/
```

##### Option 3: Download Complete Hadoop Package

```bash
# Download and extract Hadoop
wget https://archive.apache.org/dist/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz
tar -xzf hadoop-3.3.5.tar.gz

# Set HADOOP_HOME to the extracted directory
export HADOOP_HOME=$(pwd)/hadoop-3.3.5
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
```

#### Prepare Required JARs

Follow the dependency management guidelines below for the [Prepare required jars](maintenance/tiered-storage/lakehouse-storage.md#prepare-required-jars) step:

##### 1. Core Fluss Components
- **Fluss Flink Connector**: Put [fluss-flink connector jar](/downloads) into `${FLINK_HOME}/lib`
  - Choose a connector version matching your Flink version
  - For Flink 1.20: [fluss-flink-1.20-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar)

##### 2. Remote Storage Support
If you are using remote storage, download the corresponding [Fluss filesystem jar](/downloads#filesystem-jars) and place it into `${FLINK_HOME}/lib`:
- **Amazon S3**: [fluss-fs-s3 jar](/downloads#filesystem-jars)
- **Aliyun OSS**: [fluss-fs-oss jar](/downloads#filesystem-jars)
- **HDFS**: [fluss-fs-hdfs jar](/downloads#filesystem-jars)

##### 3. Iceberg Lake Connector
- **Fluss Lake Iceberg**: Put [fluss-lake-iceberg jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-iceberg/$FLUSS_VERSION$/fluss-lake-iceberg-$FLUSS_VERSION$.jar) into `${FLINK_HOME}/lib`

##### 4. Iceberg Catalog Dependencies
Put the JARs required by your Iceberg Catalog into `${FLINK_HOME}/lib`.

#### 5. Iceberg FileIO Dependencies
Put the JARs required by your Iceberg FileIO into `${FLINK_HOME}/lib`:

**S3 FileIO:**
```bash
# Required JARs for S3 FileIO
iceberg-aws-1.10.0.jar
iceberg-aws-bundle-1.10.0.jar
failsafe-3.3.2.jar
```

#### Start Datalake Tiering Service

When following the [Start Datalake Tiering Service](maintenance/tiered-storage/lakehouse-storage.md#start-datalake-tiering-service) guide, use Iceberg-specific configurations as parameters when starting the Flink tiering job:

```bash
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type hadoop \
    --datalake.iceberg.warehouse /tmp/iceberg
```

#### Important Notes

- Ensure all JAR files are compatible with Iceberg 1.10.0
- Verify that all required dependencies are in the `${FLINK_HOME}/lib` directory
- Check the Flink job logs for any missing dependency errors
- Restart the Flink cluster after adding new JAR files

## Table Mapping Between Fluss and Iceberg

When a Fluss table is created or altered with the option `'table.datalake.enabled' = 'true'` and configured with Iceberg as the datalake format, Fluss will automatically create a corresponding Iceberg table with the same table path.

The schema of the Iceberg table matches that of the Fluss table, except for the addition of three system columns at the end: `__bucket`, `__offset`, and `__timestamp`.  
These system columns help Fluss clients consume data from Iceberg in a streaming fashion, such as seeking by a specific bucket using an offset or timestamp.

### Basic Configuration

Here is an example using Flink SQL to create a table with data lake enabled:

```sql title="Flink SQL"
USE CATALOG fluss_catalog;

CREATE TABLE fluss_order_with_lake (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
 ) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s'
);
```

### Iceberg Table Properties

You can also specify Iceberg [table properties](https://iceberg.apache.org/docs/latest/configuration/#table-properties) when creating a datalake-enabled Fluss table by using the `iceberg.` prefix within the Fluss table properties clause.

Here is an example to change iceberg format to `orc` and set `commit.retry.num-retries` to `5`:

```sql title="Flink SQL"
CREATE TABLE fluss_order_with_lake (
    `order_key` BIGINT,
    `cust_key` INT NOT NULL,
    `total_price` DECIMAL(15, 2),
    `order_date` DATE,
    `order_priority` STRING,
    `clerk` STRING,
    `ptime` AS PROCTIME(),
    PRIMARY KEY (`order_key`) NOT ENFORCED
 ) WITH (
     'table.datalake.enabled' = 'true',
     'table.datalake.freshness' = '30s',
     'table.datalake.auto-maintenance' = 'true',
     'iceberg.write.format.default' = 'orc',
     'iceberg.commit.retry.num-retries' = '5'
);
```

### Primary Key Tables

Primary key tables in Fluss are mapped to Iceberg tables with:

- **Primary key constraints**: The Iceberg table maintains the same primary key definition
- **Merge-on-read (MOR) strategy**: Updates and deletes are handled efficiently using Iceberg's MOR capabilities
- **Bucket partitioning**: Automatically partitioned by the primary key using Iceberg's bucket transform with the bucket num of Fluss to align with Fluss
- **Sorted by system column `__offset`**: Sorted by the system column `__offset` (which is derived from the Fluss change log) to preserve the data order and facilitate mapping back to the original Fluss change log

```sql title="Primary Key Table Example"
CREATE TABLE user_profiles (
    `user_id` BIGINT,
    `username` STRING,
    `email` STRING,
    `last_login` TIMESTAMP,
    `profile_data` STRING,
    PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4',
    'bucket.key' = 'user_id'
);
```

**Corresponding Iceberg table structure:**
```sql
CREATE TABLE user_profiles (
    user_id BIGINT,
    username STRING,
    email STRING,
    last_login TIMESTAMP,
    profile_data STRING,
    __bucket INT,
    __offset BIGINT,
    __timestamp TIMESTAMP_LTZ,
    PRIMARY KEY (user_id) NOT ENFORCED
) PARTITIONED BY (bucket(user_id, 4))
SORTED BY (__offset ASC);
```

### Log Tables

The table mapping for Fluss log tables varies depending on whether the bucket key is specified or not.

#### No Bucket Key

Log tables without bucket in Fluss are mapped to Iceberg tables with:

- **Identity partitioning**: Using identity partitioning on the `__bucket` system column, which enables seeking to the data files in Iceberg if a specified Fluss bucket is given
- **Sorted by system column `__offset`**: Sorted by the system column `__offset` (which is derived from the Fluss log data) to preserve the data order and facilitate mapping back to the original Fluss log data

```sql title="Log Table without Bucket Key"
CREATE TABLE access_logs (
    `timestamp` TIMESTAMP,
    `user_id` BIGINT,
    `action` STRING,
    `ip_address` STRING
) WITH (
    'table.datalake.enabled' = 'true',
    'bucket.num' = '3'
);
```

**Corresponding Iceberg table:**
```sql
CREATE TABLE access_logs (
    timestamp TIMESTAMP,
    user_id BIGINT,
    action STRING,
    ip_address STRING,
    __bucket INT,
    __offset BIGINT,
    __timestamp TIMESTAMP_LTZ
) PARTITIONED BY (IDENTITY(__bucket))
SORTED BY (__offset ASC);
```

#### Single Bucket Key

Log tables with one bucket key in Fluss are mapped to Iceberg tables with:

- **Bucket partitioning**: Automatically partitioned by the bucket key using Iceberg's bucket transform with the bucket num of Fluss to align with Fluss
- **Sorted by system column `__offset`**: Sorted by the system column `__offset` (which is derived from the Fluss log data) to preserve the data order and facilitate mapping back to the original Fluss log data

```sql title="Log Table with Bucket Key"
CREATE TABLE order_events (
    `order_id` BIGINT,
    `item_id` BIGINT,
    `amount` INT,
    `event_time` TIMESTAMP
) WITH (
    'table.datalake.enabled' = 'true',
    'bucket.num' = '5',
    'bucket.key' = 'order_id'
);
```

**Corresponding Iceberg table:**
```sql
CREATE TABLE order_events (
    order_id BIGINT,
    item_id BIGINT,
    amount INT,
    event_time TIMESTAMP,
    __bucket INT,
    __offset BIGINT,
    __timestamp TIMESTAMP_LTZ
) PARTITIONED BY (bucket(order_id, 5))
SORTED BY (__offset ASC);
```

### Partitioned Tables

For Fluss partitioned tables, Iceberg first partitions by Fluss partition keys, then follows the above rules:

```sql title="Partitioned Table Example"
CREATE TABLE daily_sales (
    `sale_id` BIGINT,
    `amount` DECIMAL(10,2),
    `customer_id` BIGINT,
    `sale_date` STRING,
    PRIMARY KEY (`sale_id`) NOT ENFORCED
) PARTITIONED BY (`sale_date`)
WITH (
    'table.datalake.enabled' = 'true',
    'bucket.num' = '4',
    'bucket.key' = 'sale_id'
);
```

**Corresponding Iceberg table:**
```sql
CREATE TABLE daily_sales (
    sale_id BIGINT,
    amount DECIMAL(10,2),
    customer_id BIGINT,
    sale_date STRING,
    __bucket INT,
    __offset BIGINT,
    __timestamp TIMESTAMP_LTZ,
    PRIMARY KEY (sale_id) NOT ENFORCED
) PARTITIONED BY (IDENTITY(sale_date), bucket(sale_id, 4))
SORTED BY (__offset ASC);
```

### System Columns

All Iceberg tables created by Fluss include three system columns:

| Column        | Type          | Description                                   |
|---------------|---------------|-----------------------------------------------|
| `__bucket`    | INT           | Fluss bucket identifier for data distribution |
| `__offset`    | BIGINT        | Fluss log offset for ordering and seeking     |
| `__timestamp` | TIMESTAMP_LTZ | Fluss log timestamp for temporal ordering     |

## Read Tables

### Reading with Apache Flink

When a table has the configuration `table.datalake.enabled = 'true'`, its data exists in two layers:

- Fresh data is retained in Fluss
- Historical data is tiered to Iceberg

#### Union Read of Data in Fluss and Iceberg
You can query a combined view of both layers with second-level latency which is called union read.

##### Prerequisites

You need to place the JARs required by Iceberg to read data into `${FLINK_HOME}/lib`. For detailed dependencies and JAR preparation instructions, refer to [🚀 Start Tiering Service to Iceberg](#start-tiering-service-to-iceberg).

##### Union Read

To read the full dataset, which includes both Fluss (fresh) and Iceberg (historical) data, simply query the table without any suffix. The following example illustrates this:

```sql
-- Set execution mode to streaming or batch, here just take batch as an example
SET 'execution.runtime-mode' = 'batch';

-- Query will union data from Fluss and Iceberg
select SUM(visit_count) from fluss_access_log;
```

It supports both batch and streaming modes, utilizing Iceberg for historical data and Fluss for fresh data:

- **Batch mode** (only log table)

- **Streaming mode** (primary key table and log table)

  Flink first reads the latest Iceberg snapshot (tiered via tiering service), then switches to Fluss starting from the log offset matching that snapshot. This design minimizes Fluss storage requirements (reducing costs) while using Iceberg as a complete historical archive.

Key behavior for data retention:
- **Expired Fluss log data** (controlled by `table.log.ttl`) remains accessible via Iceberg if previously tiered
- **Cleaned-up partitions** in partitioned tables (controlled by `table.auto-partition.num-retention`) remain accessible via Iceberg if previously tiered

### Reading with Other Engines

Since data tiered to Iceberg from Fluss is stored as standard Iceberg tables, you can use any Iceberg-compatible engine. Below is an example using [StarRocks](https://docs.starrocks.io/docs/data_source/catalog/iceberg/iceberg_catalog/):

#### StarRocks with Hadoop Catalog

```sql title="StarRocks SQL"
CREATE EXTERNAL CATALOG iceberg_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hadoop",
    "iceberg.catalog.warehouse" = "/tmp/iceberg"
);
```

#### Query Examples

```sql title="Basic Query"
-- Basic query
SELECT COUNT(*) FROM iceberg_catalog.fluss.orders;
```

```sql title="Time Travel Query"
-- Time travel query
SELECT * FROM iceberg_catalog.fluss.orders FOR SYSTEM_VERSION AS OF 123456789;
```

```sql title="Query with bucket filtering for efficiency"
-- Bucket filtering query
SELECT * FROM iceberg_catalog.fluss.orders WHERE __bucket = 1 AND __offset >= 100;
```

> **NOTE**: The configuration values must match those used when configuring Iceberg as the lakehouse storage for Fluss in `server.yaml`.

## Data Type Mapping

When integrating with Iceberg, Fluss automatically converts between Fluss data types and Iceberg data types:

| Fluss Data Type               | Iceberg Data Type             | Notes               |
|-------------------------------|-------------------------------|---------------------|
| BOOLEAN                       | BOOLEAN                       |                     |
| TINYINT                       | INTEGER                       | Promoted to INT     |
| SMALLINT                      | INTEGER                       | Promoted to INT     |
| INT                           | INTEGER                       |                     |
| BIGINT                        | LONG                          |                     |
| FLOAT                         | FLOAT                         |                     |
| DOUBLE                        | DOUBLE                        |                     |
| DECIMAL                       | DECIMAL                       |                     |
| STRING                        | STRING                        |                     |
| CHAR                          | STRING                        | Converted to STRING |
| DATE                          | DATE                          |                     |
| TIME                          | TIME                          |                     |
| TIMESTAMP                     | TIMESTAMP (without timezone)  |                     |
| TIMESTAMP WITH LOCAL TIMEZONE | TIMESTAMP (with timezone)     |                     |
| BINARY                        | BINARY                        |                     |
| BYTES                         | BINARY                        | Converted to BINARY |
| ARRAY                         | LIST                          |                     |


## Maintenance and Optimization

### Auto Compaction

The table option `table.datalake.auto-compaction` (disabled by default) provides per-table control over automatic compaction.
When enabled for a specific table, compaction is automatically triggered during write operations to that table by the tiering service.

#### Configuration

```sql title="Flink SQL"
CREATE TABLE example_table (
    id BIGINT,
    data STRING,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'table.datalake.enabled' = 'true',
    'table.datalake.auto-compaction' = 'true'
);
```

#### Compaction Benefits

- **Performance**: Reduces file count and improves query performance
- **Storage**: Optimizes storage usage by removing duplicate data
- **Maintenance**: Automatically handles data organization

### Snapshot Metadata

Fluss adds specific metadata to Iceberg snapshots for traceability:

- **commit-user**: Set to `__fluss_lake_tiering` to identify Fluss-generated snapshots
- **fluss-offsets**: JSON string containing the Fluss bucket offset mapping to track the tiering progress

#### Non-Partitioned Tables

For non-partitioned tables, the metadata structure of `fluss-offsets` is:

```json
[
  {"bucket": 0, "offset": 1234},
  {"bucket": 1, "offset": 5678},
  {"bucket": 2, "offset": 9012}
]
```

#### Partitioned Tables

For partitioned tables, the metadata structure includes partition information:

```json
[
  {
    "partition_name": "date=2025",
    "partition_id": 0,
    "bucket": 0,
    "offset": 3
  },
  {
    "partition_name": "date=2025",
    "partition_id": 1,
    "bucket": 0,
    "offset": 3
  }
]
```

#### Metadata Fields Explanation

| Field            | Description                                  | Example                      |
|------------------|----------------------------------------------|------------------------------|
| `partition_id`   | Unique identifier in Fluss for the partition | `0`, `1`                     |
| `bucket`         | Bucket identifier within the partition       | `0`, `1`, `2`                |
| `partition_name` | Human-readable partition name                | `"date=2025"`, `"date=2026"` |
| `offset`         | Offset within the partition's log            | `3`, `1000`                  |


## Current Limitations

- **Complex Types**: Array, Map, and Row types are not supported
- **Multiple bucket keys**: Not supported until Iceberg implements multi-argument partition transforms
