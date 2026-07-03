---
sidebar_label: Lookups
title: Flink Lookup Joins
sidebar_position: 6
---

# Flink Lookup Joins
Flink lookup joins are important because they enable efficient, real-time enrichment of streaming data with reference data, a common requirement in many real-time analytics and processing scenarios.


## Lookup

### Instructions
- Use a primary key table as a dimension table, and the join condition must include all primary keys of the dimension table.
- Fluss lookup join is in asynchronous mode by default for higher throughput. You can change the mode of lookup join as synchronous mode by setting the SQL Hint `'lookup.async' = 'false'`.

### Examples
1. Create two tables.

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

```sql title="Flink SQL"
CREATE DATABASE my_db;
```

```sql title="Flink SQL"
USE my_db;
```

```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`orders` (
  `o_orderkey` INT NOT NULL,
  `o_custkey` INT NOT NULL,
  `o_orderstatus` CHAR(1) NOT NULL,
  `o_totalprice` DECIMAL(15, 2) NOT NULL,
  `o_orderdate` DATE NOT NULL,
  `o_orderpriority` CHAR(15) NOT NULL,
  `o_clerk` CHAR(15) NOT NULL,
  `o_shippriority` INT NOT NULL,
  `o_comment` STRING NOT NULL,
  `o_dt` STRING NOT NULL,
  PRIMARY KEY (o_orderkey) NOT ENFORCED
);
```

```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`customer` (
  `c_custkey` INT NOT NULL,
  `c_name` STRING NOT NULL,
  `c_address` STRING NOT NULL,
  `c_nationkey` INT NOT NULL,
  `c_phone` CHAR(15) NOT NULL,
  `c_acctbal` DECIMAL(15, 2) NOT NULL,
  `c_mktsegment` CHAR(10) NOT NULL,
  `c_comment` STRING NOT NULL,
  PRIMARY KEY (c_custkey) NOT ENFORCED
);
```

2. Perform lookup join.

```sql title="Flink SQL"
CREATE TEMPORARY TABLE lookup_join_sink
(
   order_key INT NOT NULL,
   order_totalprice DECIMAL(15, 2) NOT NULL,
   customer_name STRING NOT NULL,
   customer_address STRING NOT NULL
) WITH ('connector' = 'blackhole');
```

```sql title="Flink SQL"
-- look up join in asynchronous mode.
INSERT INTO lookup_join_sink
SELECT `o`.`o_orderkey`, `o`.`o_totalprice`, `c`.`c_name`, `c`.`c_address`
FROM 
(SELECT `orders`.*, proctime() AS ptime FROM `orders`) AS `o`
LEFT JOIN `customer`
FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
ON `o`.`o_custkey` = `c`.`c_custkey`;
```

```sql title="Flink SQL"
-- look up join in synchronous mode.
INSERT INTO lookup_join_sink
SELECT `o`.`o_orderkey`, `o`.`o_totalprice`, `c`.`c_name`, `c`.`c_address`
FROM 
(SELECT `orders`.*, proctime() AS ptime FROM `orders`) AS `o`
LEFT JOIN `customer` /*+ OPTIONS('lookup.async' = 'false') */
FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
ON `o`.`o_custkey` = `c`.`c_custkey`;
```

### Examples (Partitioned Table)

Continuing from the previous example, if our dimension table is a Fluss partitioned primary key table, as follows:

```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`customer_partitioned` (
  `c_custkey` INT NOT NULL,
  `c_name` STRING NOT NULL,
  `c_address` STRING NOT NULL,
  `c_nationkey` INT NOT NULL,
  `c_phone` CHAR(15) NOT NULL,
  `c_acctbal` DECIMAL(15, 2) NOT NULL,
  `c_mktsegment` CHAR(10) NOT NULL,
  `c_comment` STRING NOT NULL,
  `dt` STRING NOT NULL,
  PRIMARY KEY (`c_custkey`, `dt`) NOT ENFORCED
) 
PARTITIONED BY (`dt`)
WITH (
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'year'
);
```

To do a lookup join with the Fluss partitioned primary key table, we need to specify the 
primary keys (including partition key) in the join condition.
```sql title="Flink SQL"
INSERT INTO lookup_join_sink
SELECT `o`.`o_orderkey`, `o`.`o_totalprice`, `c`.`c_name`, `c`.`c_address`
FROM 
(SELECT `orders`.*, proctime() AS ptime FROM `orders`) AS `o`
LEFT JOIN `customer_partitioned`
FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
ON `o`.`o_custkey` = `c`.`c_custkey` AND  `o`.`o_dt` = `c`.`dt`;
```

For more details about Fluss partitioned table, see [Partitioned Tables](table-design/data-distribution/partitioning.md).

## Prefix Lookup

### Instructions

- Use a primary key table as a dimension table, and the join condition must a prefix subset of the primary keys of the dimension table.
- The bucket key of Fluss dimension table need to set as the join key when creating Fluss table. 
- Fluss prefix lookup join is in asynchronous mode by default for higher throughput. You can change the mode of prefix lookup join as synchronous mode by setting the SQL Hint `'lookup.async' = 'false'`.


### Examples
1. Create two tables.

```sql title="Flink SQL"
USE CATALOG fluss_catalog;
```

```sql title="Flink SQL"
CREATE DATABASE my_db;
```

```sql title="Flink SQL"
USE my_db;
```

```sql title="Flink SQL"
CREATE TABLE `fluss_catalog`.`my_db`.`orders_with_dt` (
  `o_orderkey` INT NOT NULL,
  `o_custkey` INT NOT NULL,
  `o_orderstatus` CHAR(1) NOT NULL,
  `o_totalprice` DECIMAL(15, 2) NOT NULL,
  `o_orderdate` DATE NOT NULL,
  `o_orderpriority` CHAR(15) NOT NULL,
  `o_clerk` CHAR(15) NOT NULL,
  `o_shippriority` INT NOT NULL,
  `o_comment` STRING NOT NULL,
  `o_dt` STRING NOT NULL,
  PRIMARY KEY (o_orderkey) NOT ENFORCED
);
```

```sql title="Flink SQL"
-- primary keys are (c_custkey, c_nationkey)
-- bucket key is (c_custkey)
CREATE TABLE `fluss_catalog`.`my_db`.`customer_with_bucket_key` (
  `c_custkey` INT NOT NULL,
  `c_name` STRING NOT NULL,
  `c_address` STRING NOT NULL,
  `c_nationkey` INT NOT NULL,
  `c_phone` CHAR(15) NOT NULL,
  `c_acctbal` DECIMAL(15, 2) NOT NULL,
  `c_mktsegment` CHAR(10) NOT NULL,
  `c_comment` STRING NOT NULL,
  PRIMARY KEY (`c_custkey`, `c_nationkey`) NOT ENFORCED
) WITH (
  'bucket.key' = 'c_custkey' 
);
```

2. Perform prefix lookup.

```sql title="Flink SQL"
CREATE TEMPORARY TABLE prefix_lookup_join_sink
(
   order_key INT NOT NULL,
   order_totalprice DECIMAL(15, 2) NOT NULL,
   customer_name STRING NOT NULL,
   customer_address STRING NOT NULL
) WITH ('connector' = 'blackhole');
```

```sql title="Flink SQL"
-- prefix look up join in asynchronous mode.
INSERT INTO prefix_lookup_join_sink
SELECT `o`.`o_orderkey`, `o`.`o_totalprice`, `c`.`c_name`, `c`.`c_address`
FROM 
(SELECT `orders_with_dt`.*, proctime() AS ptime FROM `orders_with_dt`) AS `o`
LEFT JOIN `customer_with_bucket_key`
FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
ON `o`.`o_custkey` = `c`.`c_custkey`;

-- join key is a prefix set of dimension table primary keys.
```

```sql title="Flink SQL"
-- prefix look up join in synchronous mode.
INSERT INTO prefix_lookup_join_sink
SELECT `o`.`o_orderkey`, `o`.`o_totalprice`, `c`.`c_name`, `c`.`c_address`
FROM 
(SELECT `orders_with_dt`.*, proctime() AS ptime FROM `orders_with_dt`) AS `o`
LEFT JOIN `customer_with_bucket_key` /*+ OPTIONS('lookup.async' = 'false') */
FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
ON `o`.`o_custkey` = `c`.`c_custkey`;
```

### Examples (Partitioned Table)

Continuing from the previous prefix lookup example, if our dimension table is a Fluss partitioned primary key table, as follows:

```sql title="Flink SQL"
-- primary keys are (c_custkey, c_nationkey, dt)
-- bucket key is (c_custkey)
CREATE TABLE `fluss_catalog`.`my_db`.`customer_partitioned_with_bucket_key` (
  `c_custkey` INT NOT NULL,
  `c_name` STRING NOT NULL,
  `c_address` STRING NOT NULL,
  `c_nationkey` INT NOT NULL,
  `c_phone` CHAR(15) NOT NULL,
  `c_acctbal` DECIMAL(15, 2) NOT NULL,
  `c_mktsegment` CHAR(10) NOT NULL,
  `c_comment` STRING NOT NULL,
  `dt` STRING NOT NULL,
  PRIMARY KEY (`c_custkey`, `c_nationkey`, `dt`) NOT ENFORCED
) 
PARTITIONED BY (`dt`)
WITH (
    'bucket.key' = 'c_custkey',
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'year'
);
```

To do a prefix lookup with the Fluss partitioned primary key table, the prefix lookup join key is in pattern of
`a prefix subset of primary keys (excluding partition key)` + `partition key`.
```sql title="Flink SQL"
INSERT INTO prefix_lookup_join_sink
SELECT `o`.`o_orderkey`, `o`.`o_totalprice`, `c`.`c_name`, `c`.`c_address`
FROM 
(SELECT `orders_with_dt`.*, proctime() AS ptime FROM `orders_with_dt`) AS `o`
LEFT JOIN `customer_partitioned_with_bucket_key`
FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
ON `o`.`o_custkey` = `c`.`c_custkey` AND  `o`.`o_dt` = `c`.`dt`;

-- join key is a prefix set of dimension table primary keys (excluding partition key) + partition key.
```

For more details about Fluss partitioned table, see [Partitioned Tables](table-design/data-distribution/partitioning.md).

## Insert If Not Exists

### Overview

When performing a lookup join, if the lookup key does not match any existing row in the dimension table, the default behavior is to skip the join (for `LEFT JOIN`, the dimension side returns `NULL`). By enabling the `lookup.insert-if-not-exists` option, Fluss will automatically insert a new row with the lookup key values when no match is found, and return the newly inserted row as the join result.

This feature is particularly useful when combined with [Auto-Increment Columns](table-design/table-types/pk-table.md#auto-increment-column) to build dictionary tables on the fly during stream processing. A typical use case is mapping high-cardinality string identifiers (e.g., user IDs, device IDs) to compact integer IDs for efficient downstream aggregation, such as [RoaringBitmap-based count-distinct](table-design/merge-engines/aggregation.md).

### Instructions

- Only supported for primary key lookup. Prefix lookup with `insert-if-not-exists` is not supported.
- The dimension table must not contain non-nullable columns other than the primary key columns and auto-increment columns. This is because Fluss cannot fill values for those columns when auto-inserting.
- Enable via SQL Hint: `/*+ OPTIONS('lookup.insert-if-not-exists' = 'true') */`.

### Example

The following example demonstrates how to automatically build a UID dictionary table during a lookup join.

1. Create a dictionary table with an auto-increment column.

```sql title="Flink SQL"
CREATE TABLE uid_mapping (
  uid VARCHAR NOT NULL,
  uid_int32 INT,
  PRIMARY KEY (uid) NOT ENFORCED
) WITH (
  'auto-increment.fields' = 'uid_int32',
  'bucket.num' = '1'
);
```

2. Perform a lookup join with `insert-if-not-exists` enabled. When a `uid` is encountered for the first time, Fluss automatically inserts it into `uid_mapping` and assigns an auto-incremented `uid_int32` value.

```sql title="Flink SQL"
-- UIDs from the streaming table ods_events are automatically registered
-- into the dictionary table uid_mapping, and the corresponding integer
-- ID uid_int32 is returned for each lookup
SELECT
  ods.country,
  ods.prov,
  ods.city,
  ods.ymd,
  ods.uid,
  dim.uid_int32
FROM ods_events AS ods
JOIN uid_mapping /*+ OPTIONS('lookup.insert-if-not-exists' = 'true') */
  FOR SYSTEM_TIME AS OF ods.proctime AS dim
  ON dim.uid = ods.uid;
```

Suppose `ods_events` contains the following data:

| country | prov       | city    | ymd        | uid    |
|---------|------------|---------|------------|--------|
| CN      | Beijing    | Haidian | 2025-01-01 | user_a |
| CN      | Shanghai   | Pudong  | 2025-01-02 | user_b |
| US      | California | LA      | 2025-01-03 | user_a |
| JP      | Tokyo      | Shibuya | 2025-01-04 | user_c |

The join result will be:

| country | prov       | city    | ymd        | uid    | uid_int32 |
|---------|------------|---------|------------|--------|-----------|
| CN      | Beijing    | Haidian | 2025-01-01 | user_a | 1         |
| CN      | Shanghai   | Pudong  | 2025-01-02 | user_b | 2         |
| US      | California | LA      | 2025-01-03 | user_a | 1         |
| JP      | Tokyo      | Shibuya | 2025-01-04 | user_c | 3         |

- `user_a` first appears and gets `uid_int32 = 1`; the second occurrence reuses the same value.
- `user_b` and `user_c` each get a new auto-incremented ID.

After the job runs, the `uid_mapping` dictionary table contains:

| uid    | uid_int32 |
|--------|-----------|
| user_a | 1         |
| user_b | 2         |
| user_c | 3         |

## Lookup Options

Fluss lookup join supports various configuration options. For more details, please refer to the [Connector Options](engine-flink/options.md#lookup-options) page.
