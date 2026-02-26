---
sidebar_label: "Getting Started"
title: "Getting Started with Spark"
sidebar_position: 1
---

# Getting Started with Spark Engine

## Supported Spark Versions
| Fluss Connector Versions | Supported Spark Versions |
|--------------------------|--------------------------|
| $FLUSS_VERSION_SHORT$    | 3.4, 3.5, 4.1            |

:::note
Spark 4.x requires Scala 2.13. The Fluss connector for Spark 4.1 is built with Scala 2.13, while the connectors for Spark 3.x are built with Scala 2.12.
:::


## Feature Support
Fluss supports Apache Spark's SQL API and Spark Structured Streaming.

| Feature Support                                      | Spark | Notes                                       |
|------------------------------------------------------|-------|---------------------------------------------|
| [SQL Create Catalog](ddl.md#create-catalog)          | ✔️    |                                             |
| [SQL Create Database](ddl.md#create-database)        | ✔️    |                                             |
| [SQL Drop Database](ddl.md#drop-database)            | ✔️    |                                             |
| [SQL Create Table](ddl.md#create-table)              | ✔️    |                                             |
| [SQL Drop Table](ddl.md#drop-table)                  | ✔️    |                                             |
| [SQL Describe Table](ddl.md#describe-table)          | ✔️    |                                             |
| [SQL Show Tables](ddl.md#show-tables)                | ✔️    |                                             |
| [SQL Alter Table](ddl.md#alter-table)                | ✔️    | SET/UNSET TBLPROPERTIES                     |
| [SQL Show Partitions](ddl.md#show-partitions)        | ✔️    |                                             |
| [SQL Add Partition](ddl.md#add-partition)             | ✔️    |                                             |
| [SQL Drop Partition](ddl.md#drop-partition)          | ✔️    |                                             |
| [SQL Select (Batch)](reads.md)                       | ✔️    | Log table and primary-key table             |
| [SQL Insert Into](writes.md)                         | ✔️    | Log table and primary-key table             |
| [Structured Streaming Read](structured-streaming.md#streaming-read) | ✔️ | Log table and primary-key table             |
| [Structured Streaming Write](structured-streaming.md#streaming-write) | ✔️ | Log table and primary-key table           |


## Preparation when using Spark SQL

- **Download Spark**

Spark runs on all UNIX-like environments, i.e., Linux, Mac OS X. You can download the binary release of Spark from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page, then extract the archive:

For Spark 3.5:
```shell
tar -xzf spark-3.5.7-bin-hadoop3.tgz
```

For Spark 4.1:
```shell
tar -xzf spark-4.1.1-bin-hadoop3.tgz
```

- **Copy Fluss Spark Bundled Jar**

Download [Fluss Spark Bundled jar](/downloads) and copy to the `jars` directory of your Spark home.

For Spark 3.5 (Scala 2.12):
```shell
cp fluss-spark-3.5_2.12-$FLUSS_VERSION$.jar <SPARK_HOME>/jars/
```

For Spark 4.1 (Scala 2.13):
```shell
cp fluss-spark-4.1_2.13-$FLUSS_VERSION$.jar <SPARK_HOME>/jars/
```

- **Start Spark SQL**

To quickly start the Spark SQL CLI, you can use the provided script:

```shell
<SPARK_HOME>/bin/spark-sql \
  --conf spark.sql.catalog.fluss_catalog=org.apache.fluss.spark.SparkCatalog \
  --conf spark.sql.catalog.fluss_catalog.bootstrap.servers=localhost:9123 \
  --conf spark.sql.extensions=org.apache.fluss.spark.FlussSparkSessionExtensions
```

Or start Spark Shell:

```shell
<SPARK_HOME>/bin/spark-shell \
  --conf spark.sql.catalog.fluss_catalog=org.apache.fluss.spark.SparkCatalog \
  --conf spark.sql.catalog.fluss_catalog.bootstrap.servers=localhost:9123 \
  --conf spark.sql.extensions=org.apache.fluss.spark.FlussSparkSessionExtensions
```

## Creating a Catalog

The Fluss catalog can be configured in `spark-defaults.conf` or passed as command-line arguments.

Using `spark-defaults.conf`:

```properties
spark.sql.catalog.fluss_catalog=org.apache.fluss.spark.SparkCatalog
spark.sql.catalog.fluss_catalog.bootstrap.servers=localhost:9123
spark.sql.extensions=org.apache.fluss.spark.FlussSparkSessionExtensions
```

Or configure programmatically in Scala/Python:

```scala
val spark = SparkSession.builder()
  .config("spark.sql.catalog.fluss_catalog", "org.apache.fluss.spark.SparkCatalog")
  .config("spark.sql.catalog.fluss_catalog.bootstrap.servers", "localhost:9123")
  .config("spark.sql.extensions", "org.apache.fluss.spark.FlussSparkSessionExtensions")
  .getOrCreate()
```

:::note
1. The `spark.sql.catalog.fluss_catalog.bootstrap.servers` means the Fluss server address. Before you config the `bootstrap.servers`,
   you should start the Fluss server first. See [Deploying Fluss](install-deploy/overview.md#how-to-deploy-fluss)
   for how to build a Fluss cluster.
   Here, it is assumed that there is a Fluss cluster running on your local machine and the CoordinatorServer port is 9123.
2. The `spark.sql.catalog.fluss_catalog.bootstrap.servers` configuration is used to discover all nodes within the Fluss cluster. It can be set with one or more (up to three) Fluss server addresses (either CoordinatorServer or TabletServer) separated by commas.
:::

## Creating a Database

```sql title="Spark SQL"
USE fluss_catalog;
```

```sql title="Spark SQL"
CREATE DATABASE fluss_db;
USE fluss_db;
```

## Creating a Table

```sql title="Spark SQL"
CREATE TABLE pk_table (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT
) TBLPROPERTIES (
  'primary.key' = 'shop_id,user_id',
  'bucket.num' = '4'
);
```

## Data Writing

To append new data to a table, you can use `INSERT INTO`:

```sql title="Spark SQL"
INSERT INTO pk_table VALUES
  (1234, 1234, 1, 1),
  (12345, 12345, 2, 2),
  (123456, 123456, 3, 3);
```

## Data Reading

To retrieve data, you can use a `SELECT` statement:

```sql title="Spark SQL"
SELECT * FROM pk_table ORDER BY shop_id;
```

To preview a subset of data from a log table with projection and filter:

```sql title="Spark SQL"
SELECT shop_id, total_amount FROM pk_table WHERE num_orders > 1;
```


## Type Conversion

Fluss's integration for Spark automatically converts between Spark and Fluss types.

### Fluss -> Apache Spark

| Fluss         | Spark            |
|---------------|------------------|
| BOOLEAN       | BooleanType      |
| TINYINT       | ByteType         |
| SMALLINT      | ShortType        |
| INT           | IntegerType      |
| BIGINT        | LongType         |
| FLOAT         | FloatType        |
| DOUBLE        | DoubleType       |
| CHAR          | CharType         |
| STRING        | StringType       |
| DECIMAL       | DecimalType      |
| DATE          | DateType         |
| TIMESTAMP     | TimestampNTZType |
| TIMESTAMP_LTZ | TimestampType    |
| BYTES         | BinaryType       |
| ARRAY         | ArrayType        |
| MAP           | MapType          |
| ROW           | StructType       |

:::note
The `MAP` type is currently supported for table creation and schema mapping, but **read and write operations on MAP columns are not yet supported**. Full MAP type read/write support will be available soon.
:::

### Apache Spark -> Fluss

| Spark            | Fluss                                         |
|------------------|-----------------------------------------------|
| BooleanType      | BOOLEAN                                       |
| ByteType         | TINYINT                                       |
| ShortType        | SMALLINT                                      |
| IntegerType      | INT                                           |
| LongType         | BIGINT                                        |
| FloatType        | FLOAT                                         |
| DoubleType       | DOUBLE                                        |
| CharType         | CHAR                                          |
| StringType       | STRING                                        |
| VarcharType      | STRING                                        |
| DecimalType      | DECIMAL                                       |
| DateType         | DATE                                          |
| TimestampType    | TIMESTAMP_LTZ                                 |
| TimestampNTZType | TIMESTAMP                                     |
| BinaryType       | BYTES                                         |
| ArrayType        | ARRAY                                         |
| MapType          | MAP (read/write not yet supported)             |
| StructType       | ROW                                           |
