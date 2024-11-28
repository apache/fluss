---
sidebar_position: 2
---

# PrimaryKey Table

## Basic Concept

PrimaryKey Tablea in Fluss ensure the uniqueness of the specified primary key and supports `INSERT`, `UPDATE`, and `DELETE` operations.

A PrimaryKey Table is created by specifying a `PRIMARY KEY` clause in the `CREATE TABLE` statement. For example, the following Flink SQL statement creates a PrimaryKey Table with `shop_id` and `user_id` as the primary key and distributes the data into 4 buckets:

```sql title="Flink SQL"
CREATE TABLE pk_table (
  shop_id BIGINT,
  user_id BIGINT,
  num_orders INT,
  total_amount INT,
  PRIMARY KEY (shop_id, user_id) NOT ENFORCED
) WITH (
  'bucket.num' = '4'
);
```

In Fluss primary key table, each row of data has a unique primary key.
If multiple entries with the same primary key are written to the Fluss primary key table, only the last entry will be retained.

For [Partitioned PrimaryKey Table](table-design/data-distribution/partitioning.md), the primary key must contain the partition key.

## Bucket Assigning

For primary key tables, Fluss always determines which bucket the data belongs to based on the hash value of the primary key for each record.
Data with the same hash value will be distributed to the same bucket.

## Partial Update
For primary key tables, Fluss supports partial column updates, allowing you to write only a subset of columns to incrementally update the data and ultimately achieve complete data. Note that the columns being written must include the primary key column.

For example, consider the following Fluss primary key table:
```sql title="Flink SQL"
CREATE TABLE T (
  k INT,
  v1 DOUBLE,
  v2 STRING,
  PRIMARY KEY (k) NOT ENFORCED
);
```

Assuming that at the beginning, only the `k` and `v1` columns are written with the data `+I(1, 2.0)`, `+I(2, 3.0)`, the data in T is as follows:

| k | v1  | v2   |
|---|-----|------|
| 1 | 2.0 | null |
| 2 | 3.0 | null |

Then write to the `k` and `v2` columns with the data `+I(1, 't1')`, `+I(2, 't2')`, resulting in the data in T as follows:

| k | v1  | v2 |
|---|-----|----|
| 1 | 2.0 | t1 |
| 2 | 3.0 | t2 |

## Data Queries

For primary key tables, Fluss supports querying data directly based on the key. Please refer to the [Flink Reads](../../engine-flink/reads.md) for detailed instructions.

## Changelog Generation

Fluss will capture the changes when inserting, updating, deleting records on the primary-key table, which is known as the changelog. Downstream consumers can directly consume the changelog to obtain the changes in the table. For example, consider the following primary key table in Fluss:

```sql title="Flink SQL"
CREATE TABLE T (
  k INT,
  v1 DOUBLE,
  v2 STRING,
  PRIMARY KEY (k) NOT ENFORCED
);
```

If the data written to the primary-key table is sequentially `+I(1, 2.0, 'apple')`, `+I(1, 4.0, 'banana')`, `-D(1, 4.0, 'banana')`, then the following change data will be generated.

```text
+I(1, 2.0, 'apple')
-U(1, 2.0, 'apple')
+U(1, 4.0, 'banana')
-D(1, 4.0, 'banana')
```

## Data Consumption
For a primary key table, the default consumption method is a full snapshot followed by incremental data. First, the snapshot data of the table is consumed, followed by the binlog data of the table.

It is also possible to only consume the binlog data of the table. For more details, please refer to the [Flink Reads](../../engine-flink/reads.md)
