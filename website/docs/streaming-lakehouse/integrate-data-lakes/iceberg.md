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

Fluss processes Iceberg configurations by stripping the `datalake.iceberg.` prefix and uses the stripped configurations (without the prefix `datalake.iceberg.`) to initialize the Iceberg catalog.
This approach enables passing custom configurations for iceberg catalog initiation. Checkout the [Iceberg Catalog Properties](https://iceberg.apache.org/docs/1.9.1/configuration/#catalog-properties) for more details on the available configurations of catalog.

Fluss supports all Iceberg-compatible catalog types. For catalogs such as `hive`, `hadoop`, `rest`, `glue`, `nessie`, and `jdbc`, you can specify them using the configuration `datalake.iceberg.type` with the corresponding value (e.g., `hive`, `hadoop`, etc.).
For other types of catalogs, you can use `datalake.iceberg.catalog-impl: <your_iceberg_catalog_impl_class_name>` to specify the catalog implementation.
For example, configure with `datalake.iceberg.catalog-impl: org.apache.iceberg.snowflake.SnowflakeCatalog` to use Snowflake catalog.

> **NOTE**:  
> 1: Some catalog requires Hadoop related classes such as `hadoop`, `hive` catalog. Make sure hadoop related classes are in your classpath. You can either download from [pre-bundled Hadoop jar](https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar)
or [hadoop.tar.gz](https://archive.apache.org/dist/hadoop/common/hadoop-2.8.5/hadoop-2.8.5.tar.gz) which required to be unzipped. Then put hadoop related jars into `FLUSS_HOME/plugins/iceberg`.   
> 2: Fluss only bundles the catalog implementation included in `iceberg-core` module. For any other catalog implementations not bundled within `iceberg-core` module (e.g., Hive Catalog), you must place the corresponding JAR file into the into `FLUSS_HOME/plugins/iceberg`.   
> 3: The version if Iceberg that Fluss bundles is based on `1.9.2`, please make sure the jars you put is compatible with `Iceberg-1.9.2`

### Start Tiering Service to Iceberg

Then, you must start the datalake tiering service to tier Fluss's data to Iceberg. For guidance, you can refer to [Start The Datalake Tiering Service
](maintenance/tiered-storage/lakehouse-storage.md#start-the-datalake-tiering-service). Although the example uses Paimon, the process is also applicable to Iceberg.

However, for the [Prepare required jars](maintenance/tiered-storage/lakehouse-storage.md#prepare-required-jars) step, adhere to the dependency management guidelines listed below:
- Put [fluss-flink connector jar](/downloads) into `${FLINK_HOME}/lib`, you should choose a connector version matching your Flink version. If you're using Flink 1.20, please use [fluss-flink-1.20-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-flink-1.20/$FLUSS_VERSION$/fluss-flink-1.20-$FLUSS_VERSION$.jar)
- If you are using [Amazon S3](http://aws.amazon.com/s3/), [Aliyun OSS](https://www.aliyun.com/product/oss) or [HDFS(Hadoop Distributed File System)](https://hadoop.apache.org/docs/stable/) as Fluss's [remote storage](maintenance/tiered-storage/remote-storage.md),
  you should download the corresponding [Fluss filesystem jar](/downloads#filesystem-jars) and also put it into `${FLINK_HOME}/lib`
- Put [fluss-lake-iceberg jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-iceberg/$FLUSS_VERSION$/fluss-lake-iceberg-$FLUSS_VERSION$.jar) into `${FLINK_HOME}/lib`
- Put the jars required by Iceberg Catalog into `${FLINK_HOME}/lib`. For example, if you are using Hive catalog, you should put [iceberg-hive-metastore](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-metastore/1.9.2/iceberg-hive-metastore-1.9.2.jar), hadoop related jars, hive related jars into `${FLINK_HOME}/lib`
- Put the jars required by Iceberg FileIO into `${FLINK_HOME}/lib`. For example, if your Iceberg is backed by S3, you should put [iceberg-aws-bundle](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws-bundle/1.9.2) into  `${FLINK_HOME}/lib`

Additionally, when following the [Start Datalake Tiering Service](maintenance/tiered-storage/lakehouse-storage.md#start-datalake-tiering-service) guide, make sure to use Iceberg-specific configurations as parameters when starting the Flink tiering job:
```shell
<FLINK_HOME>/bin/flink run /path/to/fluss-flink-tiering-$FLUSS_VERSION$.jar \
    --fluss.bootstrap.servers localhost:9123 \
    --datalake.format iceberg \
    --datalake.iceberg.type hadoop \
    --datalake.iceberg.warehouse /tmp/iceberg
```

## Table Mapping Between Fluss and Iceberg

When a Fluss table is created or altered with the option `'table.datalake.enabled' = 'true'` and configured with Iceberg as the datalake format, Fluss will automatically create a corresponding Iceberg table with the same table path.

The schema of the Iceberg table matches that of the Fluss table, except for the addition of three system columns at the end: `__bucket`, `__offset`, and `__timestamp`.  
These system columns help Fluss clients consume data from Iceberg in a streaming fashion, such as seeking by a specific bucket using an offset or timestamp.

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

The table mapping for Fluss log table are a little of different depending on whether the bucket key is specified or not.

#### No Bucket Key
Log Table without bucket in Fluss are mapped to Iceberg tables with:
- **Identity partitioning**: Using identity partitioning on the `__bucket` system column, which enables to seek to the data files in iceberg if a specified Fluss bucket is given
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
Log Table with one bucket key in Fluss are mapped to Iceberg tables with:
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

For Fluss partitioned tables, Iceberg first partitions by Fluss partition keys, then by following the above rules:

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

## Read Tables

### Reading with other Engines

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

```sql title="Query Examples"
-- Basic query
SELECT COUNT(*) FROM iceberg_catalog.fluss.orders;

-- Time travel query
SELECT * FROM iceberg_catalog.fluss.orders 
FOR SYSTEM_VERSION AS OF 123456789;

-- Query with bucket filtering for efficiency
SELECT * FROM iceberg_catalog.fluss.orders 
WHERE __bucket = 1 AND __offset >= 100;
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


## Maintenance and Optimization

### Auto Compaction

The table option `table.datalake.auto-compaction` (disabled by default) provides per-table control over automatic compaction. 
When enabled for a specific table, compaction is automatically triggered during write operations to that table by the tiering service.

### Snapshot Metadata

Fluss adds specific metadata to Iceberg snapshots for traceability:

- **commit-user**: Set to `__fluss_lake_tiering` to identify Fluss-generated snapshots
- **fluss-bucket-offset**: JSON string containing the Fluss bucket offset mapping to track the tiering progress:
  ```json
  [
    {"bucket": 0, "offset": 1234},
    {"bucket": 1, "offset": 5678},
    {"bucket": 2, "offset": 9012}
  ]
  ```

## Limitations

When using Iceberg as the lakehouse storage layer with Fluss, the following limitations currently exist:

- **Union Read**: Union read of data from both Fluss and Iceberg layers is not supported
- **Complex Types**: Array, Map, and Row types are not supported
- **Multiple bucket keys**: Not supported until Iceberg implements multi-argument partition transforms
