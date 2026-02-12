---
title: Primary Key Table
sidebar_position: 1
---

# Primary Key Table

## Basic Concept

Primary Key Table in Fluss ensures the uniqueness of the specified primary key and supports `INSERT`, `UPDATE`,
and `DELETE` operations.

A Primary Key Table is created by specifying a `PRIMARY KEY` clause in the `CREATE TABLE` statement. For example, the
following Flink SQL statement creates a Primary Key Table with `shop_id` and `user_id` as the primary key and distributes
the data into 4 buckets:

```sql title="Flink SQL"
CREATE TABLE pk_table
(
    shop_id      BIGINT,
    user_id      BIGINT,
    num_orders   INT,
    total_amount INT,
    PRIMARY KEY (shop_id, user_id) NOT ENFORCED
) WITH (
    'bucket.num' = '4'
);
```

In Fluss primary key table, each row of data has a unique primary key.
If multiple entries with the same primary key are written to the Fluss primary key table, only the last entry will be
retained.

For [Partitioned Primary Key Table](/table-design/data-distribution/partitioning.md), the primary key must contain the
partition key.

## Bucket Assigning

For primary key tables, Fluss always determines which bucket the data belongs to based on the hash value of the bucket
key (It must be a subset of the primary keys excluding partition keys of the primary key table) for each record. If the bucket key is not specified, the bucket key will be used as the primary key (excluding the partition key).
Data with the same hash value will be distributed to the same bucket.

## Partial Update

For primary key tables, Fluss supports partial column updates, allowing you to write only a subset of columns to
incrementally update the data and ultimately achieve complete data. Note that the columns being written must include the
primary key column.

For example, consider the following Fluss primary key table:

```sql title="Flink SQL"
CREATE TABLE T
(
    k  INT,
    v1 DOUBLE,
    v2 STRING,
    PRIMARY KEY (k) NOT ENFORCED
);
```

Assuming that at the beginning, only the `k` and `v1` columns are written with the data `+I(1, 2.0)`, `+I(2, 3.0)`, the
data in T is as follows:

| k | v1  | v2   |
|---|-----|------|
| 1 | 2.0 | null |
| 2 | 3.0 | null |

Then write to the `k` and `v2` columns with the data `+I(1, 't1')`, `+I(2, 't2')`, resulting in the data in T as
follows:

| k | v1  | v2 |
|---|-----|----|
| 1 | 2.0 | t1 |
| 2 | 3.0 | t2 |

## Merge Engines

The **Merge Engine** in Fluss is a core component designed to efficiently handle and consolidate data updates for Primary Key Tables.
It offers users the flexibility to define how incoming data records are merged with existing records sharing the same primary key.
However, users can specify a different merge engine to customize the merging behavior according to their specific use cases.

The following merge engines are supported:

1. [Default Merge Engine (LastRow)](/table-design/merge-engines/default.md)
2. [FirstRow Merge Engine](/table-design/merge-engines/first-row.md)
3. [Versioned Merge Engine](/table-design/merge-engines/versioned.md)
4. [Aggregation Merge Engine](/table-design/merge-engines/aggregation.md)


## Changelog Generation

Fluss will capture the changes when inserting, updating, deleting records on the Primary Key Table, which is known as
the changelog. Downstream consumers can directly consume the changelog to obtain the changes in the table. For example,
consider the following primary key table in Fluss:

```sql title="Flink SQL"
CREATE TABLE T
(
    k  INT,
    v1 DOUBLE,
    v2 STRING,
    PRIMARY KEY (k) NOT ENFORCED
);
```

If the data written to the Primary Key Table is
sequentially `+I(1, 2.0, 'apple')`, `+I(1, 4.0, 'banana')`, `-D(1, 4.0, 'banana')`, then the following change data will
be generated. For example, the following Flink SQL statements illustrate this behavior:

```sql title="Flink SQL"
-- set to batch mode to execute DELETE and INSERT statements
SET execution.runtime-mode = batch;

-- insert to records with the same primary key k=1
INSERT INTO T (k, v1, v2) VALUES (1, 2.0, 'apple');
INSERT INTO T (k, v1, v2) VALUES (1, 4.0, 'banana');

-- delete the record with primary key k=1
DELETE FROM T WHERE k = 1;

-- set to streaming mode to observe the changelogs
SET execution.runtime-mode = streaming;
SELECT * FROM T;
```

Generate the following output in the Flink SQL CLI:

```
+------+------+------+--------+
| op   | k    | v1   | v2     |
| ---- | ---- | ---- | ------ |
| +I   | 1    | 2.0  | apple  |
| -U   | 1    | 2.0  | apple  |
| +U   | 1    | 4.0  | banana |
| -D   | 1    | 4.0  | banana |
+------+------+------+--------+
4 rows in set
```

## Auto-Increment Column

In Fluss, the auto increment column is a feature that automatically generates a unique numeric value, commonly used to create unique identifiers for each row of data.
Each time a new record is inserted, the auto increment column automatically assigns an incrementing value, eliminating the need for manually specifying the number.

One application scenario of the auto-increment column is to accelerate the counting of distinct values in a high-cardinality column:
an auto-increment column can be used to represent the unique value column in a dictionary.
Compared to directly counting distinct STRING values, counting distinct integer values of the auto-increment column can sometimes improve the query speed by several times or even tens of times.

Furthermore, Fluss provides native support for RoaringBitmap-based aggregations through the built-in `rbm32` and `rbm64` aggregation functions, available in the [Aggregation Merge Engine](/docs/table-design/merge-engines/aggregation.md).
These functions are optimized to work seamlessly with auto-incremented integer columns. A typical usage pattern involves creating a dictionary table that maps raw identifiers (e.g., strings or sparse IDs) to compact, dense integer IDs via an auto-increment column.
These dense IDs are then aggregated into a RoaringBitmap using `rbm32` (for 32-bit IDs) or `rbm64` (for 64-bit IDs), enabling highly efficient count-distinct computations both in storage and during query execution.

### Basic features

#### Uniqueness

Fluss guarantees table-wide uniqueness for values it generates in the auto-increment column.

#### Monotonicity
In order to improve the performance of allocating auto-incremented IDs, each table bucket on TabletServers caches some auto-incremented IDs locally.
In this situation, Fluss cannot guarantee that the values for the auto-increment column are strictly monotonic globally.
It can only be ensured that the values roughly increase in chronological order.

:::note
The number of auto-incremented IDs cached by the TabletServers is controlled by the table property `table.auto-increment.cache-size`,
which defaults to 100,000. A larger cache size can enhance the performance of auto-incremented ID allocation but may result in
less monotonic values in the auto-increment column. You can configure different cache sizes for different tables based on your specific requirements.
However, this property cannot be modified after the table has been created.
:::

For example, create a table named `uid_mapping` with 2 buckets and insert five rows of data as follows:

```sql
CREATE TABLE uid_mapping (
  user_id STRING,
  uid BIGINT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'table.auto-increment.fields' = 'uid_int64',
  'bucket.num' = '2'
);

INSERT INTO uid_mapping VALUES ('user1');
INSERT INTO uid_mapping VALUES ('user2');
INSERT INTO uid_mapping VALUES ('user3');
INSERT INTO uid_mapping VALUES ('user4');
INSERT INTO uid_mapping VALUES ('user5');
```

The auto-incremented IDs in the table `uid_mapping` do not monotonically increase, because the two table buckets cache auto-incremented IDs, [1, 100000] and [100001, 200000], respectively.

```sql
SELECT * FROM uid_mapping;
+---------+---------+
| user_id |   uid   |
+---------+---------+
| user1   |    	 1  |
| user2   | 100001  |
| user3   |  	 2  |
| user4   |  	 3  |
| user5   | 100002  |
+---------+---------+
```

### Limits
- Auto-increment columns can only be used in primary key tables.
- Explicitly specifying values for the auto-increment column is not allowed. The value for an auto-increment column can only be implicitly assigned.
- A table can have only one auto-increment column.
- The auto-increment column must be of type `INT` or `BIGINT`.
- Fluss does not support specifying the starting value and step size for the auto-increment column.


## Data Queries

For primary key tables, Fluss supports various kinds of querying abilities.

### Reads

For a primary key table, the default read method is a full snapshot followed by incremental data. First, the
snapshot data of the table is consumed, followed by the changelog data of the table.

It is also possible to only consume the changelog data of the table. For more details, please refer to the [Flink Reads](/engine-flink/reads.md)

### Lookup

Fluss primary key table can lookup data by the primary keys. If the key exists in Fluss, lookup will return a unique row. It is always used in [Flink Lookup Join](/engine-flink/lookups.md#lookup).

### Prefix Lookup

Fluss primary key table can also do prefix lookup by the prefix subset primary keys. Unlike lookup, prefix lookup
will scan data based on the prefix of primary keys and may return multiple rows. It is always used in [Flink Prefix Lookup Join](/engine-flink/lookups.md#prefix-lookup).
