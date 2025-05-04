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

[Apache Paimon](https://paimon.apache.org/) innovatively combines lake format and LSM structure, bringing efficient updates into the lake architecture.
To integrate Fluss with Paimon, you must enable lakehouse storage and configure Paimon as lakehouse storage. See more detail about [Enable Lakehouse Storage](maintenance/tiered-storage/lakehouse-storage.md#enable-lakehouse-storage).

## Introduction
When a table with option `'table.datalake.enabled' = 'true'` is created or altered in Fluss, Fluss will create a corresponding Paimon table with same table path as well.
The schema of the Paimon table is as same as the schema of the Fluss table, except for there are two extra columns `__offset` and `__timestamp` appended to the last.
These two columns are used to help Fluss client to consume the data in Paimon in streaming way like seek by offset/timestamp, etc.

Then datalake tiering service compacts the data from Fluss to Paimon continuously. For primary key table, it will also generate change log in Paimon format which
enables you streaming consume it in Paimon way.

## Read tables

### Read by Flink

For the table with option `'table.datalake.enabled' = 'true'`, there are two part of data: the data remains in Fluss and the data already in Paimon.
Now, you have two view of the table: one view is the Paimon data which has minute-level latency, one view is the full data union Fluss and Paimon data
which is the latest within second-level latency.

Flink empowers you to decide to choose which view:
- Only Paimon means a better analytics performance but with worse data freshness
- Combing Fluss and Paimon means a better data freshness but with analytics performance degrading

#### Read data only in Paimon
To point to read data in Paimon, you must specify the table with `$lake` suffix, the following
SQL shows how to do that:

```sql title="Flink SQL"
-- assume we have a table named `orders`

-- read from paimon
SELECT COUNT(*) FROM orders$lake;
```

```sql title="Flink SQL"
-- we can also query the system tables 
SELECT * FROM orders$lake$snapshots;
```

When specify the table with `$lake` suffix in query, it just acts like a normal Paimon table, so it inherits all ability of Paimon table.
You can enjoy all the features that Flink's query supports/optimization on Paimon, like query system tables, time travel, etc. See more
about Paimon's [sql-query](https://paimon.apache.org/docs/0.9/flink/sql-query/#sql-query).


#### Union read data in Fluss and Paimon
To point to read the full data that union Fluss and Paimon, you just query it as a normal table without any suffix or others, the following
SQL shows how to do that:

```sql title="Flink SQL"
-- query will union data of Fluss and Paimon
SELECT SUM(order_count) as total_orders FROM ads_nation_purchase_power;
```
The query may look slower than only querying data in Paimon, but it queries the full data which means better data freshness. You can
run the query multi-times, you should get different results in every one run as the data is written to the table continuously.

### Read by other engines

As the tiered data in Paimon compacted from Fluss is also a standard Paimon table, you can use
[any engines](https://paimon.apache.org/docs/0.9/engines/overview/) that support Paimon to read the data. Here, we take [StarRocks](https://paimon.apache.org/docs/master/engines/starrocks/) as the engine to read the data:

First, create a Paimon catalog for StarRocks:
```sql title="StarRocks SQL"
CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES
(
  "type" = "paimon",
  "paimon.catalog.type" = "filesystem",
  "paimon.catalog.warehouse" = "/tmp/paimon_data_warehouse"
);
```

**NOTE**: The configuration value `paimon.catalog.type` and `paimon.catalog.warehouse` should be same as how you configure the Paimon as lakehouse storage for Fluss in `server.yaml`.

Then, you can query the `orders` table by StarRocks:
```sql title="StarRocks SQL"
-- the table is in database `fluss`
SELECT COUNT(*) FROM paimon_catalog.fluss.orders;
```

```sql title="StarRocks SQL"
-- query the system tables, to know the snapshots of the table
SELECT * FROM paimon_catalog.fluss.enriched_orders$snapshots;
```


## Data Type Mapping
When integrate with Paimon, Fluss automatically converts between Fluss data type and Paimon data type.
The following content shows the mapping between [Fluss data type](table-design/data-types.md) and Paimon data type:

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
