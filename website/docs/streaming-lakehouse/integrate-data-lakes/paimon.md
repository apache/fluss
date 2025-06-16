---
title: Paimon
sidebar_position: 1
---

<!--
 Copyright (c) 2025 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Paimon

[Apache Paimon](https://paimon.apache.org/) innovatively combines a lake format with an LSM (Log-Structured Merge-tree) structure, bringing efficient updates into the lake architecture .  
To integrate Fluss with Paimon, you must enable lakehouse storage and configure Paimon as the lakehouse storage. For more details, see [Enable Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md#enable-lakehouse-storage).

## Introduction

When a table with the option `'table.datalake.enabled' = 'true'` is created or altered in Fluss, Fluss will automatically create a corresponding Paimon table with the same table path .  
The schema of the Paimon table matches that of the Fluss table, except for the addition of three system columns at the end: `__bucket`, `__offset`, and `__timestamp`.  
These system columns help Fluss clients consume data from Paimon in a streaming fashion—such as seeking by a specific bucket using an offset or timestamp.

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
       'table.datalake.freshness' = '30s');
```

Then, the datalake tiering service continuously tiers data from Fluss to Paimon. The parameter `table.datalake.freshness` controls how soon data written to Fluss should be tiered to Paimon—by default, this delay is 3 minutes.  
For primary key tables, change logs are also generated in Paimon format, enabling stream-based consumption via Paimon APIs.

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
       'paimon.deletion-vectors.enabled' = 'true');
```

For example, you can specify the Paimon property `file.format` to change the file format of the Paimon table, or set `deletion-vectors.enabled` to enable or disable deletion vectors for the Paimon table.

## Read Tables

### Read by Flink

For a table with the option `'table.datalake.enabled' = 'true'`, its data exists in two layers: one remains in Fluss, and the other has already been tiered to Paimon.  
You can choose between two views of the table:
- A **Paimon-only view**, which offers minute-level latency but better analytics performance.
- A **combined view** of both Fluss and Paimon data, which provides second-level latency but may result in slightly degraded query performance.

#### Read Data Only in Paimon

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

To read the full dataset, which includes both Fluss and Paimon data, simply query the table without any suffix. The following example illustrates this:

```sql title="Flink SQL"
-- Query will union data from Fluss and Paimon
SELECT SUM(order_count) AS total_orders FROM ads_nation_purchase_power;
```

This query may run slower than reading only from Paimon, but it returns the most up-to-date data. If you execute the query multiple times, you may observe different results due to continuous data ingestion.

### Read by Other Engines

Since the data tiered to Paimon from Fluss is stored as a standard Paimon table, you can use any engine that supports Paimon to read it. Below is an example using [StarRocks](https://paimon.apache.org/docs/master/engines/starrocks/):

First, create a Paimon catalog in StarRocks:

```sql title="StarRocks SQL"
CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES
(
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