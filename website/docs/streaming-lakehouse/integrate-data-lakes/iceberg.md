---
title: Iceberg
sidebar_position: 2
---

# Iceberg

[Apache Iceberg](https://iceberg.apache.org/) is an open table format for huge analytic datasets. It provides ACID transactions, schema evolution, and efficient data organization for data lakes.
To integrate Fluss with Iceberg, you must enable lakehouse storage and configure Iceberg as the lakehouse storage. For more details, see [Enable Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md#enable-lakehouse-storage).

Supported Iceberg Versions: Fluss supports both Iceberg v1 and v2 table formats. Log tables (append-only) are compatible with v1 tables, while primary key tables leverage v2 features such as delete files and merge-on-read capabilities for efficient updates and deletes.

## Introduction

When a table is created or altered with the option `'table.datalake.enabled' = 'true'` and configured with Iceberg as the datalake format, Fluss will automatically create a corresponding Iceberg table with the same table path.
The schema of the Iceberg table matches that of the Fluss table, except for the addition of three system columns at the end: `__bucket`, `__offset`, and `__timestamp`.  
These system columns help Fluss clients consume data from Iceberg in a streaming fashion, such as seeking by a specific bucket using an offset or timestamp.

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

Then, the datalake tiering service continuously tiers data from Fluss to Iceberg. The parameter `table.datalake.freshness` controls the frequency that Fluss writes data to Iceberg tables. By default, the data freshness is 3 minutes.  
For primary key tables, updates and deletes are handled using Iceberg's delete files (equality deletes and position deletes) with a merge-on-read (MOR) strategy for efficient change management.

Since Fluss version 0.8, you can also specify Iceberg table properties when creating a datalake-enabled Fluss table by using the `iceberg.` prefix within the Fluss table properties clause.

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
     'iceberg.write.format.default' = 'parquet',
     'iceberg.commit.retry.num-retries' = '5'
);
```

For example, you can specify the Iceberg property `write.format.default` to change the file format of the Iceberg table, or set `commit.retry.num-retries` to configure retry behavior for commits. The `table.datalake.auto-maintenance` option (true by default) enables automatic maintenance tasks such as file compaction and snapshot expiration.

## Table Types and Bucketing Strategy

Fluss uses a special bucketing strategy when integrating with Iceberg to ensure data distribution consistency between Fluss and Iceberg layers. This enables efficient data access and future union read capabilities.

### Bucket Strategy

When Iceberg is configured as the datalake format, Fluss uses `IcebergBucketingFunction` to bucket data following Iceberg's bucketing strategy. This ensures:
- **Data distribution consistency**: The same record goes to the same bucket in both Fluss and Iceberg
- **Efficient data access**: You can quickly locate data for a specific Fluss bucket within Iceberg
### Primary Key Tables

Primary key tables in Fluss are mapped to Iceberg tables with:
- **Primary key constraints**: The Iceberg table maintains the same primary key definition
- **Merge-on-read (MOR) strategy**: Updates and deletes are handled efficiently using Iceberg's MOR capabilities
- **Required bucket keys**: Primary key tables must have exactly one bucket key defined
- **Bucket partitioning**: Automatically partitioned by the bucket key using Iceberg's bucket transform

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

### Log Tables (Append-Only)

Log tables support different bucketing configurations:

#### No Bucket Key
For log tables without a configured bucket key, Iceberg uses identity partitioning on the `__bucket` system column:

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
For log tables with one bucket key, Iceberg uses bucket partitioning:

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

For Fluss partitioned tables, Iceberg first partitions by Fluss partition keys, then by bucket keys:

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

### Reading with Apache Flink

For a table with the option `'table.datalake.enabled' = 'true'` and Iceberg configured as the lakehouse storage, you can read data stored in Iceberg using the `$lake` suffix in the table name.

#### Read Data Only in Iceberg

To read only data stored in Iceberg, use the `$lake` suffix in the table name:

```sql title="Flink SQL"
-- Read from Iceberg
SELECT COUNT(*) FROM orders$lake;
```

```sql title="Flink SQL"
-- Query system tables
SELECT * FROM orders$lake$snapshots;
```

When you specify the `$lake` suffix, the table behaves like a standard Iceberg table with full Iceberg capabilities including time travel, system tables, and more.

#### Union Read Limitations

**Important**: Iceberg does not currently support union read of data from both Fluss and Iceberg layers. You can only read data from:
- **Fluss layer only**: Fluss layer only: Query the table directly without `$lake` suffix (reads all data from Fluss tablet servers - both historical and fresh data)
- **Iceberg layer only**: Query the table with `$lake` suffix (reads only tiered historical data from lakehouse storage)

Union read functionality will be added in future releases.

### Reading with other Engines

Since data tiered to Iceberg from Fluss is stored as standard Iceberg tables, you can use any Iceberg-compatible engine. Below is an example using [StarRocks](https://docs.starrocks.io/docs/data_source/catalog/iceberg_catalog/):

#### StarRocks with Hadoop Catalog

```sql title="StarRocks SQL"
CREATE EXTERNAL CATALOG iceberg_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hadoop",
    "iceberg.catalog.warehouse" = "/tmp/iceberg_data_warehouse"
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

#### StarRocks with Hive Catalog

```sql title="StarRocks SQL with Hive Catalog"
CREATE EXTERNAL CATALOG iceberg_catalog
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://<hive-metastore-host>:<port>",
    "iceberg.catalog.warehouse" = "hdfs:///path/to/warehouse"
);
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

The `table.datalake.auto-compaction` option (disabled by default) provides automatic compaction:

- **File Compaction**: Small files are automatically compacted during tiering
- **Per-bucket Compaction**: Compaction operates within individual buckets for efficiency

### Snapshot Metadata

Fluss adds specific metadata to Iceberg snapshots for traceability:

- **commit-user**: Set to `__fluss_lake_tiering` to identify Fluss-generated snapshots
- **fluss-bucket-offset**: JSON string containing the Fluss bucket offset mapping:
  ```json
  [
    {"bucket": 0, "offset": 1234},
    {"bucket": 1, "offset": 5678},
    {"bucket": 2, "offset": 9012}
  ]
  ```

## Limitations

When using Iceberg as lakehouse storage with Fluss, there are current limitations:

### Current Limitations
- **Union Read**: Union read of data from both Fluss and Iceberg layers is not supported
- **Complex Types**: Array, Map, and Row types are not supported
- **Multiple bucket keys**: Not supported until Iceberg implements multi-argument partition transforms

### Unsupported Scenarios
Tables with multiple bucket keys or unsupported bucket data types:
- **Cannot enable datalake**: Throws UnsupportedOperationException when attempting to create lakehouse-enabled tables 
- **Must be configured at table creation time**: Dynamic datalake enablement is not possible for these configurations