---
sidebar_label: Reads
title: Spark Reads
sidebar_position: 5
---

# Spark Reads

Fluss supports batch read with [Apache Spark](https://spark.apache.org/)'s SQL API for both Log Tables and Primary Key Tables.

:::tip
For streaming read, see the [Structured Streaming Read](streaming.md#streaming-read) section.
:::

## Limitations

:::caution
- For tables with datalake enabled (`table.datalake.enabled = true`), batch read can only read data stored in the Fluss cluster and cannot read data that has been tiered to the underlying lake storage (e.g., Paimon, Iceberg). Reading the full dataset including lake data will be supported in a future release.
:::

## Batch Read

### Log Table

You can read data from a log table using the `SELECT` statement.

#### Example

1. Create a table and prepare data:

```sql title="Spark SQL"
CREATE TABLE log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
);
```

```sql title="Spark SQL"
INSERT INTO log_table VALUES
  (600, 21, 601, 'addr1'),
  (700, 22, 602, 'addr2'),
  (800, 23, 603, 'addr3'),
  (900, 24, 604, 'addr4'),
  (1000, 25, 605, 'addr5');
```

2. Query data:

```sql title="Spark SQL"
SELECT * FROM log_table ORDER BY order_id;
```

#### Projection

Column projection minimizes I/O by reading only the columns used in a query:

```sql title="Spark SQL"
SELECT address, item_id FROM log_table ORDER BY order_id;
```

#### Filter

Filters are applied to reduce the amount of data read:

```sql title="Spark SQL"
SELECT * FROM log_table WHERE amount % 2 = 0 ORDER BY order_id;
```

#### Projection + Filter

Projection and filter can be combined for efficient queries:

```sql title="Spark SQL"
SELECT order_id, item_id FROM log_table
WHERE order_id >= 900 ORDER BY order_id;
```

### Partitioned Log Table

Reading from partitioned log tables supports partition filtering:

```sql title="Spark SQL"
CREATE TABLE part_log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING,
  dt STRING
) PARTITIONED BY (dt);
```

```sql title="Spark SQL"
INSERT INTO part_log_table VALUES
  (600, 21, 601, 'addr1', '2026-01-01'),
  (700, 22, 602, 'addr2', '2026-01-01'),
  (800, 23, 603, 'addr3', '2026-01-02'),
  (900, 24, 604, 'addr4', '2026-01-02'),
  (1000, 25, 605, 'addr5', '2026-01-03');
```

```sql title="Spark SQL"
-- Read with partition filter
SELECT * FROM part_log_table WHERE dt = '2026-01-01' ORDER BY order_id;
```

```sql title="Spark SQL"
-- Read with multiple partitions filter
SELECT order_id, address, dt FROM part_log_table
WHERE dt IN ('2026-01-01', '2026-01-02')
ORDER BY order_id;
```

### Primary Key Table

The Fluss source supports batch read for primary-key tables. It reads data from the latest snapshot and merges it with log changes to provide the most up-to-date view.

#### Example

1. Create a table and prepare data:

```sql title="Spark SQL"
CREATE TABLE pk_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
) TBLPROPERTIES (
  'primary.key' = 'order_id',
  'bucket.num' = '1'
);
```

```sql title="Spark SQL"
INSERT INTO pk_table VALUES
  (600, 21, 601, 'addr1'),
  (700, 22, 602, 'addr2'),
  (800, 23, 603, 'addr3'),
  (900, 24, 604, 'addr4'),
  (1000, 25, 605, 'addr5');
```

2. Query data:

```sql title="Spark SQL"
SELECT * FROM pk_table ORDER BY order_id;
```

3. After upsert, the query reflects the latest values:

```sql title="Spark SQL"
-- Upsert data
INSERT INTO pk_table VALUES
  (700, 220, 602, 'addr2'),
  (900, 240, 604, 'addr4'),
  (1100, 260, 606, 'addr6');
```

```sql title="Spark SQL"
-- Query reflects the latest data
SELECT order_id, item_id, address FROM pk_table
WHERE amount <= 603 ORDER BY order_id;
```

### Partitioned Primary Key Table

Reading from partitioned primary key tables also supports partition filtering:

```sql title="Spark SQL"
CREATE TABLE part_pk_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING,
  dt STRING
) PARTITIONED BY (dt) TBLPROPERTIES (
  'primary.key' = 'order_id,dt',
  'bucket.num' = '1'
);
```

```sql title="Spark SQL"
INSERT INTO part_pk_table VALUES
  (600, 21, 601, 'addr1', '2026-01-01'),
  (700, 22, 602, 'addr2', '2026-01-01'),
  (800, 23, 603, 'addr3', '2026-01-02'),
  (900, 24, 604, 'addr4', '2026-01-02'),
  (1000, 25, 605, 'addr5', '2026-01-03');
```

```sql title="Spark SQL"
-- Read with partition filter
SELECT * FROM part_pk_table
WHERE dt = '2026-01-01'
ORDER BY order_id;
```

```sql title="Spark SQL"
-- Read with multiple partition filters
SELECT * FROM part_pk_table
WHERE dt IN ('2026-01-01', '2026-01-02')
ORDER BY order_id;
```

## All Data Types

Fluss Spark connector supports reading all Fluss data types including nested types:

:::note
The `MAP` type is currently **not supported** for read operations. Full MAP type read support will be available soon.
:::

```sql title="Spark SQL"
CREATE TABLE all_types_table (
  id INT,
  flag BOOLEAN,
  small SHORT,
  value INT,
  big BIGINT,
  real FLOAT,
  amount DOUBLE,
  name STRING,
  decimal_val DECIMAL(10, 2),
  date_val DATE,
  timestamp_ntz_val TIMESTAMP,
  timestamp_ltz_val TIMESTAMP_LTZ,
  arr ARRAY<INT>,
  struct_col STRUCT<col1: INT, col2: STRING>
);
```

```sql title="Spark SQL"
INSERT INTO all_types_table VALUES
  (1, true, 100, 1000, 10000, 12.34, 56.78, 'string_val',
   123.45, DATE '2026-01-01', TIMESTAMP '2026-01-01 12:00:00', TIMESTAMP '2026-01-01 12:00:00',
   ARRAY(1, 2, 3), STRUCT(100, 'nested_value')),
  (2, false, 200, 2000, 20000, 23.45, 67.89, 'another_str',
   223.45, DATE '2026-01-02', TIMESTAMP '2026-01-02 12:00:00', TIMESTAMP '2026-01-02 12:00:00',
   ARRAY(4, 5, 6), STRUCT(200, 'nested_value2'));
```

```sql title="Spark SQL"
SELECT * FROM all_types_table;
```

## Read Optimized Mode

For primary key tables, Fluss by default reads the latest snapshot and merges it with log changes to return the most up-to-date data. You can enable **read-optimized mode** to skip the merge step and read only snapshot data, which improves query performance at the cost of data freshness.

```sql title="Spark SQL"
-- Enable read-optimized mode for primary key tables
SET spark.sql.fluss.read.optimized=true;

-- Query returns only snapshot data (may be stale)
SELECT * FROM pk_table;
```

For more details on all available read options, see the [Connector Options](options.md#read-options) page.
