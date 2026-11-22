---
sidebar_label: Reads
title: Spark Reads
sidebar_position: 5
---

# Spark Reads

Fluss supports batch read with [Apache Spark](https://spark.apache.org/)'s SQL API for both Log Tables and Primary Key Tables.

:::tip
For streaming read, see the [Structured Streaming Read](structured-streaming.md#streaming-read) section.
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

### Lake-Enabled Tables (Union Read)

When a table has the configuration `table.datalake.enabled = 'true'`, its data exists in two layers:

- **Fresh data** is retained in Fluss (real-time layer, sub-second freshness)
- **Historical data** is tiered to the lake storage (e.g., Paimon, Iceberg, Hudi)

Fluss Spark connector supports **union read** that combines both layers to provide a complete, up-to-date view of the data. This allows Fluss to store only a small portion of the dataset in the cluster (reducing costs), while the lake serves as the source of complete historical data.

#### Union Read

To read the full dataset, simply query the table directly. The Spark connector automatically unions data from Fluss and the lake storage:

```sql title="Spark SQL"
-- Query will union data from Fluss and lake
SELECT SUM(total_price) AS total_revenue FROM fluss_order_with_lake;
```

The union read works for both **log tables** and **primary key tables**:

- **Log tables**: Combines Fluss log data with lake historical data
- **Primary key tables**: Combines lake snapshot data with recent KV log changes using sort-merge to provide the most up-to-date view

#### Example

1. Create a lake-enabled table:

```sql title="Spark SQL"
CREATE TABLE fluss_order_with_lake (
  order_key BIGINT,
  cust_key INT NOT NULL,
  total_price DECIMAL(15, 2),
  order_date DATE,
  order_priority STRING,
  clerk STRING
) TBLPROPERTIES (
  'table.datalake.enabled' = 'true',
  'table.datalake.freshness' = '30s'
);
```

2. Insert data (the datalake tiering service will continuously tier data to the lake):

```sql title="Spark SQL"
INSERT INTO fluss_order_with_lake VALUES
  (1001, 101, 150.50, DATE '2026-01-01', 'HIGH', 'clerk_A'),
  (1002, 102, 250.75, DATE '2026-01-01', 'MEDIUM', 'clerk_B'),
  (1003, 103, 350.00, DATE '2026-01-02', 'LOW', 'clerk_C');
```

3. Query with union read:

```sql title="Spark SQL"
-- Returns complete view combining Fluss and lake data
SELECT SUM(total_price) AS total_revenue FROM fluss_order_with_lake;
```

## Time-Range Batch Read

A time-range batch read returns the data written to Fluss within a `[start, end)` time window (left-closed, right-open on the commit timestamp). This is the building block for incremental pipelines, e.g. an hourly job that reads only the rows written in the past hour.

The timestamp value is either epoch milliseconds or a `yyyy-MM-dd HH:mm:ss` datetime interpreted in the Spark session time zone (`spark.sql.session.timeZone`).

### Output semantics

| Table type | Output of a `[t1, t2)` read |
|------------|-----------------------------|
| **Log table** | Every record appended within the window. |
| **Primary key table** | The rows whose keys were **inserted or updated** within the window, folded to their latest value as of `t2`. Keys that were only deleted in the window are excluded (i.e. the result keeps `+I`/`+U` after-images and drops `-U`/`-D`). |
| **Lake-enabled table** | Same as the underlying log or primary key table, but read **only from Fluss**. A time-range read never unions the lake snapshot, so it is limited to the data still retained in Fluss (see below). |

### Using the table-valued function (recommended for SQL)

`fluss_incremental_between_timestamp(table, start[, end])` reads a time window in a single statement. Omit `end` to read up to the moment the statement is analyzed.

```sql title="Spark SQL"
-- Read the past hour on a log table (epoch-millis form)
SELECT * FROM fluss_incremental_between_timestamp('log_table', '1767225600000', '1767312000000')
ORDER BY order_id;
```

```sql title="Spark SQL"
-- Incremental changes on a primary key table (datetime form)
-- Returns the latest value of every key changed in the window; deleted keys are excluded
SELECT * FROM fluss_incremental_between_timestamp(
  'pk_table', '2026-01-01 00:00:00', '2026-01-02 00:00:00')
ORDER BY order_id;
```

```sql title="Spark SQL"
-- Omit the end to read from a start timestamp up to the analysis time
SELECT * FROM fluss_incremental_between_timestamp('log_table', '2026-01-01 00:00:00');
```

The start/end arguments may be any constant expression, so a rolling window does not need to be computed outside SQL:

```sql title="Spark SQL"
-- The past hour, as epoch milliseconds (unix_timestamp() returns seconds)
SELECT * FROM fluss_incremental_between_timestamp(
  'log_table',
  CAST((unix_timestamp() - 3600) * 1000 AS STRING),
  CAST(unix_timestamp() * 1000 AS STRING));

-- The past hour, as datetime strings
SELECT * FROM fluss_incremental_between_timestamp(
  'log_table',
  date_format(now() - INTERVAL 1 HOUR, 'yyyy-MM-dd HH:mm:ss'),
  date_format(now(), 'yyyy-MM-dd HH:mm:ss'));
```

The table argument is a string and accepts `table`, `database.table` or `catalog.database.table`; unqualified names resolve against the current catalog and database. The start/end arguments accept a string (epoch milliseconds or `yyyy-MM-dd HH:mm:ss`), an integral epoch-milliseconds value, a `DATE` (the start of that day), or a `TIMESTAMP`/`TIMESTAMP_NTZ` literal — all interpreted in the Spark session time zone — and may be produced by constant expressions such as the datetime functions above (column references are not allowed). The result is an ordinary relation, so projection, filters and joins work as usual.

:::note
The function is provided by the Fluss Spark session extension, so `spark.sql.extensions=org.apache.fluss.spark.FlussSparkSessionExtensions` must be configured (see [Getting Started](getting-started.md)). Its options apply to that single query only.

Fluss uses `[start, end)` (start inclusive, end exclusive). This differs from Paimon's similarly named `paimon_incremental_between_timestamp`, which is start-exclusive and end-inclusive.
:::

### Using the DataFrame API

The same window can be expressed with scan options, which is how it is configured from the DataFrame API. Setting `scan.incremental.start.timestamp` is what turns a batch read into an incremental one; `scan.incremental.end.timestamp` is optional and the read runs up to the latest committed data when it is left unset:

```scala title="Spark Scala"
spark.read
  .option("scan.incremental.start.timestamp", "2026-01-01 00:00:00")
  .option("scan.incremental.end.timestamp", "2026-01-02 00:00:00")
  .table("fluss_catalog.fluss.log_table")
```

:::note
The `scan.incremental.*` options are per-query read options only. Unlike the options in [Options](options.md), they are **not** picked up from session configuration (`SET spark.sql.fluss.scan.incremental...` has no effect), which keeps a time window from silently applying to later reads in the same session.
:::

:::warning Retention boundary
A time-range read returns the data Fluss still retains, which is bounded by `table.log.ttl` (default 7 days). A window reaching further back is **not** an error: the part that has already been dropped simply yields fewer rows, or none at all, so the result is always a subset of the requested window. Increase `table.log.ttl` if you need to read further back. Data available only in tiered lake storage is not read by this mode.

The start timestamp positions the scan through the server's timestamp-to-offset lookup, and both bounds are then applied on each record's commit timestamp, so the window stays exact even for data already tiered to remote storage.

When the window starts before the table is guaranteed to retain, planning logs a `WARN` on the driver. This is a conservative hint derived from the table's current `table.log.ttl`, not an exact statement about what was deleted: it may over-report (expired segments are deleted lazily, so the data may still be readable) and may under-report (it cannot know that a window predates the table's creation, or that whole partitions were dropped by `table.auto-partition.num-retention`), and it reflects only the current value of `table.log.ttl`, not past changes to it.
:::

:::note Invalid windows fail fast
Malformed window specifications are rejected instead of silently changing semantics: a blank or unparseable start timestamp, an end timestamp set without a start timestamp (it cannot truncate a plain batch read on its own), and a window whose start is not strictly before its end — whether given through the table-valued function (rejected while the statement is analyzed) or through DataFrame options. A bucket that has no data inside a valid window yields an empty result.
:::

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
