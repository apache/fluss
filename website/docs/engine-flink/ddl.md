---
sidebar_label: DDL
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

The following SQL statement creates a Partitioned PrimaryKey Table in Fluss. Note that the partitioned field (`dt` in this case) must be a subset of the primary key (`dt, shop_id, user_id` in this case).
Currently, Fluss only supports one partitioned field with `STRING` type.

:::note
Currently, partitioned table must enable auto partition and set auto partition time unit.
:::

```sql title="Flink SQL"
CREATE TABLE my_part_pk_table (
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

The following SQL statement creates a Partitioned Log Table in Fluss.

```sql title="Flink SQL"
CREATE TABLE my_part_log_table (
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


The supported option in "with" parameters when creating a table are as follows:

| Option                             | Type     | Required | Default                             | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|------------------------------------|----------|----------|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| bucket.num                         | int      | optional | The bucket number of Fluss cluster. | The number of buckets of a Fluss table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| bucket.key                         | String   | optional | (none)                              | Specific the distribution policy of the Fluss table. Data will be distributed to each bucket according to the hash value of bucket-key.  If you specify multiple fields, delimiter is ','. If the table is with primary key, you can't specific bucket key currently. The bucket keys will always be the primary key(excluding partition key). If the table is not with primary key, you can specific bucket key, and when the bucket key is not specified, the data will be distributed to each bucket randomly.  |
| table.*                            |          |          |                                     | All the [`table.` prefix configuration](/docs/maintenance/configuration.md) are supported to be defined in "with" options.                                                                                                                                                                                                                                                                                                                                                                                         |
| client.*                           |          |          |                                     | All the [`client.` prefix configuration](/docs/maintenance/configuration.md) are supported to be defined in "with" options.                                                                                                                                                                                                                                                                                                                                                                                        |

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


