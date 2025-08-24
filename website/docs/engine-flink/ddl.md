---
sidebar_label: DDL
title: Flink DDL
sidebar_position: 2
---

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

| Option                         | Required | Default   | Description                                                                                                                                                                          |
|--------------------------------|----------|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type                           | required | (none)    | Catalog type, must be 'fluss' here.                                                                                                                                               |
| bootstrap.servers              | required | (none)    | Comma separated list of Fluss servers.                                                                                                                                               |
| default-database               | optional | fluss     | The default database to use when switching to this catalog.                                                                                                                          |
| client.security.protocol       | optional | PLAINTEXT | The security protocol used to communicate with brokers. Currently, only `PLAINTEXT` and `SASL` are supported, the configuration value is case insensitive.                           |
| `client.security.{protocol}.*` | optional | (none)    | Client-side configuration properties for a specific authentication protocol. E.g., client.security.sasl.jaas.config. More Details in [authentication](../security/authentication.md) |

The following statements assume that the current catalog has been switched to the Fluss catalog using the `USE CATALOG <catalog_name>` statement.

## Create Database

By default, FlussCatalog will use the `fluss` database in Flink. You can use the following example to create a separate database to avoid creating tables under the default `fluss` database:

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

### Primary Key Table

The following SQL statement will create a [Primary Key Table](table-design/table-types/pk-table/index.md) with a primary key consisting of shop_id and user_id.
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

### Partitioned (Primary Key/Log) Table

:::note
1. Currently, Fluss only supports partitioned field with `STRING` type
2. For the Partitioned Primary Key Table, the partitioned field (`dt` in this case) must be a subset of the primary key (`dt, shop_id, user_id` in this case)
:::

The following SQL statement creates a Partitioned Primary Key Table in Fluss.

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
:::info
Fluss partitioned table supports dynamic partition creation, which means you can write data into a partition without pre-creating it.
You can use the `INSERT INTO` statement to write data into a partitioned table, and Fluss will automatically create the partition if it does not exist.
See the [Dynamic Partitioning](table-design/data-distribution/partitioning.md#dynamic-partitioning) for more details.
But you can still use the [Add Partition](engine-flink/ddl.md#add-partition) statement to manually add partitions if needed.
:::

#### Multi-Fields Partitioned Table

Fluss also supports [Multi-Fields Partitioning](table-design/data-distribution/partitioning.md#multi-field-partitioned-tables), the following SQL statement creates a Multi-Fields Partitioned Log Table in Fluss:

```sql title="Flink SQL"
CREATE TABLE my_multi_fields_part_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING,
  dt STRING,
  nation STRING
) PARTITIONED BY (dt, nation);
```

#### Auto Partitioned (Primary Key/Log) Table

Fluss also supports creating Auto Partitioned (Primary Key/Log) Table. The following SQL statement creates an Auto Partitioned Primary Key Table in Fluss.

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

For more details about Auto Partitioned (Primary Key/Log) Table, refer to [Auto Partitioning](table-design/data-distribution/partitioning.md#auto-partitioning).


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

## Add Partition

Fluss supports manually adding partitions to an existing partitioned table through the Fluss Catalog. If the specified partition 
does not exist, Fluss will create the partition. If the specified partition already exists, Fluss will ignore the request 
or throw an exception.

To add partitions, run:

```sql title="Flink SQL"
-- Add a partition to a single field partitioned table
ALTER TABLE my_part_pk_table ADD PARTITION (dt = '2025-03-05');

-- Add a partition to a multi-field partitioned table
ALTER TABLE my_multi_fields_part_log_table ADD PARTITION (dt = '2025-03-05', nation = 'US');
```

For more details, refer to the [Flink ALTER TABLE(ADD)](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/alter/#add) documentation.

## Show Partitions

To show all the partitions of a partitioned table, run:
```sql title="Flink SQL"
SHOW PARTITIONS my_part_pk_table;
```

For multi-field partitioned tables, you can use the `SHOW PARTITIONS` command with either **partial** or **full** partition field conditions to list matching partitions.

```sql title="Flink SQL"
-- Show partitions using a partial partition filter
SHOW PARTITIONS my_multi_fields_part_log_table PARTITION (dt = '2025-03-05');

-- Show partitions using a full partition filter
SHOW PARTITIONS my_multi_fields_part_log_table PARTITION (dt = '2025-03-05', nation = 'US');
```

For more details, refer to the [Flink SHOW PARTITIONS](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/show/#show-partitions) documentation.

## Drop Partition

Fluss also supports manually dropping partitions from an existing partitioned table through the Fluss Catalog. If the specified partition 
does not exist, Fluss will ignore the request or throw an exception.


To drop partitions, run:
```sql title="Flink SQL"
-- Drop a partition from a single field partitioned table
ALTER TABLE my_part_pk_table DROP PARTITION (dt = '2025-03-05');

-- Drop a partition from a multi-field partitioned table
ALTER TABLE my_multi_fields_part_log_table DROP PARTITION (dt = '2025-03-05', nation = 'US');
```

For more details, refer to the [Flink ALTER TABLE(DROP)](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/alter/#drop) documentation.
