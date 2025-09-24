---
title: Paimon
sidebar_position: 1
---

# Paimon

## Introduction

[Apache Paimon](https://paimon.apache.org/) innovatively combines a lake format with an LSM (Log-Structured Merge-tree) structure, bringing efficient updates into the lake architecture. 
To integrate Fluss with Paimon, you must enable lakehouse storage and configure Paimon as the lakehouse storage. For more details, see [Enable Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md#enable-lakehouse-storage).

## Configure Paimon as LakeHouse Storage

For general guidance on configuring Paimon as the lakehouse storage, you can refer to [Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md) documentation. When starting the tiering service, make sure to use Paimon-specific configurations as parameters.

When a table is created or altered with the option `'table.datalake.enabled' = 'true'`, Fluss will automatically create a corresponding Paimon table with the same table path.
The schema of the Paimon table matches that of the Fluss table, except for the addition of three system columns at the end: `__bucket`, `__offset`, and `__timestamp`.  
These system columns help Fluss clients consume data from Paimon in a streaming fashion, such as seeking by a specific bucket using an offset or timestamp.

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

Then, the datalake tiering service continuously tiers data from Fluss to Paimon. The parameter `table.datalake.freshness` controls the frequency that Fluss writes data to Paimon tables. By default, the data freshness is 3 minutes.  
For primary key tables, changelogs are also generated in the Paimon format, enabling stream-based consumption via Paimon APIs.

Since Fluss version 0.7, you can also specify Paimon table properties when creating a datalake-enabled Fluss table by using the `paimon.` prefix within the Fluss table properties clause.

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
     'paimon.file.format' = 'orc',
     'paimon.deletion-vectors.enabled' = 'true'
);
```

For example, you can specify the Paimon property `file.format` to change the file format of the Paimon table, or set `deletion-vectors.enabled` to enable or disable deletion vectors for the Paimon table.

## Read Tables

### Reading with Apache Flink

For a table with the option `'table.datalake.enabled' = 'true'`, its data exists in two layers: one remains in Fluss, and the other has already been tiered to Paimon.  
You can choose between two views of the table:
- A **Paimon-only view**, which offers minute-level latency but better analytics performance.
- A **combined view** of both Fluss and Paimon data, which provides second-level latency but may result in slightly degraded query performance.

#### Read Data Only in Paimon

##### Prerequisites
Download the [paimon-flink.jar](https://paimon.apache.org/docs/1.2/) that matches your Flink version, and place it in the `FLINK_HOME/lib` directory

##### Read Paimon Data
To read only data stored in Paimon, use the `$lake` suffix in the table name. The following example demonstrates this:

```sql title="Flink SQL"
-- Assume we have a table named `orders`

-- Read from Paimon
SELECT COUNT(*) FROM orders$lake;
```

```sql title="Flink SQL"
-- We can also query the system tables
SELECT * FROM orders$lake$snapshots;
```

When you specify the `$lake` suffix in a query, the table behaves like a standard Paimon table and inherits all its capabilities.  
This allows you to take full advantage of Flink's query support and optimizations on Paimon, such as querying system tables, time travel, and more.  
For further information, refer to Paimon’s [SQL Query documentation](https://paimon.apache.org/docs/0.9/flink/sql-query/#sql-query).

#### Union Read of Data in Fluss and Paimon

##### Prerequisites
Download the [fluss-lake-paimon-$FLUSS_VERSION$.jar](https://repo1.maven.org/maven2/org/apache/fluss/fluss-lake-paimon/$FLUSS_VERSION$/fluss-lake-paimon-$FLUSS_VERSION$.jar), and place it into `${FLINK_HOME}/lib`.

##### Union Read
To read the full dataset, which includes both Fluss (fresh) and Paimon (historical) data, simply query the table without any suffix. The following example illustrates this:

```sql title="Flink SQL"
-- Set execution mode to streaming or batch, here just take batch as an example
SET 'execution.runtime-mode' = 'batch';

-- Query will union data from Fluss and Paimon
SELECT SUM(order_count) AS total_orders FROM ads_nation_purchase_power;
```
It supports both batch and streaming modes, using Paimon for historical data and Fluss for fresh data:
- In batch mode

  The query may run slower than reading only from Paimon because it needs to merge rows from both Paimon and Fluss. However, it returns the most up-to-date results. Multiple executions of the query may produce different outputs due to continuous data ingestion.

- In streaming mode

  Flink first reads the latest Paimon snapshot (tiered via tiering service), then switches to Fluss starting from the log offset aligned with that snapshot, ensuring exactly-once semantics.
  This design enables Fluss to store only a small portion of the dataset in the Fluss cluster, reducing costs, while Paimon serves as the source of complete historical data when needed. 

  More precisely, if Fluss log data is removed due to TTL expiration—controlled by the `table.log.ttl` configuration—it can still be read by Flink through its Union Read capability, as long as the data has already been tiered to Paimon.
  For partitioned tables, if a partition is cleaned up—controlled by the `table.auto-partition.num-retention` configuration—the data in that partition remains accessible from Paimon, provided it has been tiered there beforehand. 


### Reading with other Engines

Since the data tiered to Paimon from Fluss is stored as a standard Paimon table, you can use any engine that supports Paimon to read it. Below is an example using [StarRocks](https://paimon.apache.org/docs/master/engines/starrocks/):

First, create a Paimon catalog in StarRocks:

```sql title="StarRocks SQL"
CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES (
       "type" = "paimon",
       "paimon.catalog.type" = "filesystem",
       "paimon.catalog.warehouse" = "/tmp/paimon_data_warehouse"
);
```

> **NOTE**: The configuration values for `paimon.catalog.type` and `paimon.catalog.warehouse` must match those used when configuring Paimon as the lakehouse storage for Fluss in `server.yaml`.

Then, you can query the `orders` table using StarRocks:

```sql title="StarRocks SQL"
-- The table is in the database `fluss`
SELECT COUNT(*) FROM paimon_catalog.fluss.orders;
```

```sql title="StarRocks SQL"
-- Query the system tables to view snapshots of the table
SELECT * FROM paimon_catalog.fluss.enriched_orders$snapshots;
```

## Data Type Mapping

When integrating with Paimon, Fluss automatically converts between Fluss data types and Paimon data types.  
The following table shows the mapping between [Fluss data types](table-design/data-types.md) and Paimon data types:

| Fluss Data Type               | Paimon Data Type              |
|-------------------------------|-------------------------------|
| BOOLEAN                       | BOOLEAN                       |
| TINYINT                       | TINYINT                       |
| SMALLINT                      | SMALLINT                      |
| INT                           | INT                           |
| BIGINT                        | BIGINT                        |
| FLOAT                         | FLOAT                         |
| DOUBLE                        | DOUBLE                        |
| DECIMAL                       | DECIMAL                       |
| STRING                        | STRING                        |
| CHAR                          | CHAR                          |
| DATE                          | DATE                          |
| TIME                          | TIME                          |
| TIMESTAMP                     | TIMESTAMP                     |
| TIMESTAMP WITH LOCAL TIMEZONE | TIMESTAMP WITH LOCAL TIMEZONE |
| BINARY                        | BINARY                        |
| BYTES                         | BYTES                         |