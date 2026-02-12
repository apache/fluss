---
sidebar_label: DDL
title: Spark DDL
sidebar_position: 2
---

# Spark DDL

## Create Catalog

Fluss supports creating and managing tables through the Fluss Catalog in Spark. The catalog is configured via Spark's catalog plugin mechanism.

```properties title="spark-defaults.conf"
spark.sql.catalog.fluss_catalog=org.apache.fluss.spark.SparkCatalog
spark.sql.catalog.fluss_catalog.bootstrap.servers=fluss-server-1:9123
```

```sql title="Spark SQL"
USE fluss_catalog;
```

The following properties can be set if using the Fluss catalog:

| Option              | Required | Default | Description                                                                    |
|---------------------|----------|---------|--------------------------------------------------------------------------------|
| bootstrap.servers   | required | (none)  | Comma separated list of Fluss servers.                                         |

The following statements assume that the current catalog has been switched to the Fluss catalog using the `USE fluss_catalog` statement.

## Create Database

By default, FlussCatalog will use the `fluss` database. You can use the following example to create a separate database:

```sql title="Spark SQL"
CREATE DATABASE my_db COMMENT 'created by spark';
```

```sql title="Spark SQL"
USE my_db;
```

## Drop Database

To delete a database, this will drop all the tables in the database as well:

```sql title="Spark SQL"
-- Switch to another database first
USE fluss;
```

```sql title="Spark SQL"
-- Drop the database
DROP DATABASE my_db;
```

## Create Table

### Log Table

The following SQL statement creates a Log Table by not specifying `primary.key` property.

```sql title="Spark SQL"
CREATE TABLE my_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
);
```

### Primary Key Table

The following SQL statement creates a Primary Key Table by specifying `primary.key` in `TBLPROPERTIES`.

```sql title="Spark SQL"
CREATE TABLE my_pk_table (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT
) TBLPROPERTIES (
  'primary.key' = 'shop_id,user_id',
  'bucket.num' = '4'
);
```

### Partitioned Table

The following SQL statement creates a Partitioned Log Table in Fluss.

```sql title="Spark SQL"
CREATE TABLE my_part_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING,
  dt STRING
) PARTITIONED BY (dt);
```

The following SQL statement creates a Partitioned Primary Key Table in Fluss.

```sql title="Spark SQL"
CREATE TABLE my_part_pk_table (
  id INT,
  name STRING,
  pt STRING
) PARTITIONED BY (pt) TBLPROPERTIES (
  'primary.key' = 'id,pt'
);
```

:::note
1. Currently, Fluss only supports partitioned field with `STRING` type.
2. For the Partitioned Primary Key Table, the partitioned field must be a subset of the primary key.
:::

#### Multi-Fields Partitioned Table

Fluss also supports multi-field partitioning. The following SQL statement creates a Multi-Fields Partitioned Table:

```sql title="Spark SQL"
CREATE TABLE my_multi_part_table (
  id INT,
  name STRING,
  pt1 STRING,
  pt2 INT
) PARTITIONED BY (pt1, pt2);
```

### Table Properties

The following table properties can be specified when creating a table:

| Property     | Required | Description                                                                                                                                                                                                                                                                                                                        |
|--------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| primary.key  | optional | The primary keys of the Fluss table. Multiple columns are separated by commas (e.g., `'col1,col2'`).                                                                                                                                                                                                                              |
| bucket.key   | optional | The distribution key of the Fluss table. Data will be distributed to each bucket according to the hash value of the bucket key. Must be a subset of the primary keys (excluding partition keys). If not specified, defaults to the primary key (excluding partition keys) for PK tables, or random distribution for Log tables.     |
| bucket.num   | optional | The number of buckets of the Fluss table.                                                                                                                                                                                                                                                                                          |

You can also pass additional custom properties and Fluss storage options through `TBLPROPERTIES`:

```sql title="Spark SQL"
CREATE TABLE my_table (
  id INT,
  name STRING
) TBLPROPERTIES (
  'primary.key' = 'id',
  'bucket.num' = '4',
  'key1' = 'value1'
);
```

## Describe Table

To describe the schema of a table:

```sql title="Spark SQL"
DESC my_table;
```

## Show Tables

To list all tables in the current database:

```sql title="Spark SQL"
SHOW TABLES;
```

You can also specify a database and use patterns:

```sql title="Spark SQL"
-- Show tables in a specific database
SHOW TABLES IN fluss;

-- Show tables matching a pattern
SHOW TABLES FROM fluss LIKE 'test_*';
```

## Drop Table

To delete a table, run:

```sql title="Spark SQL"
DROP TABLE my_table;
```

This will entirely remove all the data of the table in the Fluss cluster.

## Alter Table

### SET Properties

The `SET TBLPROPERTIES` statement allows users to add or modify table properties:

```sql title="Spark SQL"
ALTER TABLE my_table SET TBLPROPERTIES ('key1' = 'value1', 'key2' = 'value2');
```

### UNSET Properties

The `UNSET TBLPROPERTIES` statement allows users to remove table properties:

```sql title="Spark SQL"
ALTER TABLE my_table UNSET TBLPROPERTIES ('key1', 'key2');
```

:::note
Most table properties with prefix of `table.` are not allowed to be modified.
:::

## Show Partitions

To show all the partitions of a partitioned table, run:

```sql title="Spark SQL"
SHOW PARTITIONS my_part_table;
```

For multi-field partitioned tables, you can use the `SHOW PARTITIONS` command with a partial partition filter to list matching partitions:

```sql title="Spark SQL"
-- Show partitions using a partial partition filter
SHOW PARTITIONS my_multi_part_table PARTITION (pt1 = 'a');
```

## Add Partition

Fluss supports manually adding partitions to an existing partitioned table:

```sql title="Spark SQL"
-- Add a partition
ALTER TABLE my_multi_part_table ADD PARTITION (pt1 = 'b', pt2 = 1);

-- Add a partition if not exists
ALTER TABLE my_multi_part_table ADD IF NOT EXISTS PARTITION (pt1 = 'b', pt2 = 1);
```

## Drop Partition

To drop a partition from a partitioned table:

```sql title="Spark SQL"
-- Drop a partition
ALTER TABLE my_multi_part_table DROP PARTITION (pt1 = 'a', pt2 = 2);

-- Drop a partition if exists
ALTER TABLE my_multi_part_table DROP IF EXISTS PARTITION (pt1 = 'a', pt2 = 2);
```

:::note
Spark does not support dropping partial partitions. You must specify all partition fields when dropping a partition.
:::
