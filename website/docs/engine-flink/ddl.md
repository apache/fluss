---
sidebar_label: DDL
sidebar_position: 2
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

# Flink DDL

## Create Catalog
Fluss supports creating and managing tables through the Fluss Catalog.
```sql title="Flink SQL"
CREATE CATALOG fluss_catalog WITH (
  'type' = 'fluss',
  'bootstrap.servers' = 'fluss-server-1:9123'
);
```

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

The following properties can be set if using the Fluss catalog:

| Option            | Required | Default | Description                                                 | 
|-------------------|----------|---------|-------------------------------------------------------------|
| type              | required | (none)  | Catalog type, must to be 'fluss' here.                      |
| bootstrap.servers | required | (none)  | Comma separated list of Fluss servers.                      |
| default-database  | optional | fluss   | The default database to use when switching to this catalog. |

The following introduced statements assuming the current catalog is switched to the Fluss catalog using `USE CATALOG <catalog_name>` statement.

## Create Database

By default, FlussCatalog will use the `fluss` database in Flink. Using the following example to create a separate database in order to avoid creating tables under the default `fluss` database:

```sql title="Flink SQL"
CREATE DATABASE my_db;
```

```sql title="Flink SQL"
USE my_db;
```

## Drop Database

To delete a database, this will drop all the tables in the database as well:

```sql title="Flink SQL"
-- Flink doesn't allow drop current database, switch to Fluss default database
USE fluss;
```

```sql title="Flink SQL"
-- drop the database
DROP DATABASE my_db;
```

## Create Table

### PrimaryKey Table

The following SQL statement will create a [PrimaryKey Table](table-design/table-types/pk-table/index.md) with a primary key consisting of shop_id and user_id.
```sql title="Flink SQL"
CREATE TABLE my_pk_table (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (shop_id, user_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4'
);
```

### Log Table

The following SQL statement creates a [Log Table](table-design/table-types/log-table.md) by not specifying primary key clause.

```sql title="Flink SQL"
CREATE TABLE my_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
) WITH (
  'bucket.num' = '8'
);
```

### Partitioned (PrimaryKey/Log) Table

:::note
1. Currently, Fluss only supports one partitioned field with `STRING` type
2. For the Partitioned PrimaryKey Table, the partitioned field (`dt` in this case) must be a subset of the primary key (`dt, shop_id, user_id` in this case)
:::

The following SQL statement creates a Partitioned PrimaryKey Table in Fluss.

```sql title="Flink SQL"
CREATE TABLE my_part_pk_table (
  dt STRING,
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (dt, shop_id, user_id) NOT ENFORCED
) PARTITIONED BY (dt);
```

The following SQL statement creates a Partitioned Log Table in Fluss.

```sql title="Flink SQL"
CREATE TABLE my_part_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING,
  dt STRING
) PARTITIONED BY (dt);
```
:::note
After the Partitioned (PrimaryKey/Log) Table is created, you need first manually create the corresponding partition using the [Add Partition](engine-flink/ddl.md#add-partition) statement
before you write/read data into this partition.
:::

#### Auto partitioned (PrimaryKey/Log) table

Fluss also support creat Auto Partitioned (PrimaryKey/Log) Table. The following SQL statement creates an Auto Partitioned PrimaryKey Table in Fluss.

```sql title="Flink SQL"
CREATE TABLE my_auto_part_pk_table (
  dt STRING,
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (dt, shop_id, user_id) NOT ENFORCED
) PARTITIONED BY (dt) WITH (
  'bucket.num' = '4',
  'table.auto-partition.enabled' = 'true',
  'table.auto-partition.time-unit' = 'day'
);
```

The following SQL statement creates an Auto Partitioned Log Table in Fluss.

```sql title="Flink SQL"
CREATE TABLE my_auto_part_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING,
  dt STRING
) PARTITIONED BY (dt) WITH (
  'bucket.num' = '8',
  'table.auto-partition.enabled' = 'true',
  'table.auto-partition.time-unit' = 'hour'
);
```

For more details about Auto Partitioned (PrimaryKey/Log) Table, refer to [Auto Partitioning Options](table-design/data-distribution/partitioning.md#auto-partitioning-options).

### Options

The supported option in `WITH` parameters when creating a table are listed in [Connector Options](engine-flink/options.md) page.

## Create Table Like

To create a table with the same schema, partitioning, and table properties as another table, use `CREATE TABLE LIKE`.

```sql title="Flink SQL"
-- there is a temporary datagen table
CREATE TEMPORARY TABLE datagen (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10'
);
```

```sql title="Flink SQL"
-- creates Fluss table which derives the metadata from the temporary table excluding options
CREATE TABLE my_table LIKE datagen (EXCLUDING OPTIONS);
```

For more details, refer to the [Flink CREATE TABLE](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/create/#like) documentation.


## Drop Table

To delete a table, run:

```sql title="Flink SQL"
DROP TABLE my_table;
```

This will entirely remove all the data of the table in the Fluss cluster.

## Show Partitions

To show all the partitions of a partitioned table, run:
```sql title="Flink SQL"
SHOW PARTITIONS my_part_pk_table;
```

For more details, refer to the [Flink SHOW PARTITIONS](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/show/#show-partitions) documentation.

:::note
Currently, we only support show all partitions of a partitioned table, but not support show partitions with the given partition spec.
:::

## Add Partition

Fluss support manually add partitions to an exists partitioned table by Fluss Catalog. If the specified partition 
not exists, Fluss will create the partition. If the specified partition already exists, Fluss will ignore the request 
or throw an exception.

To add partitions, run:
```sql title="Flink SQL"
ALTER TABLE my_part_pk_table ADD PARTITION (dt = '2025-03-05');
```

For more details, refer to the [Flink ALTER TABLE(ADD)](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/alter/#add) documentation.

## Drop Partition

Fluss also support manually drop partitions from an exists partitioned table by Fluss Catalog. If the specified partition 
not exists, Fluss will ignore the request or throw an exception.


To drop partitions, run:
```sql title="Flink SQL"
ALTER TABLE my_part_pk_table DROP PARTITION (dt = '2025-03-05');
```

For more details, refer to the [Flink ALTER TABLE(DROP)](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/alter/#drop) documentation.


