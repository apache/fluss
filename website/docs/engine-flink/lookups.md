---
sidebar_label: Lookups
title: Flink Lookup Joins
sidebar_position: 5
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
CREATE TABLE `fluss_catalog`.`my_db`.`customer_partitioned_with_bukcet_key` (
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
LEFT JOIN `customer_partitioned_with_bukcet_key`
FOR SYSTEM_TIME AS OF `o`.`ptime` AS `c`
ON `o`.`o_custkey` = `c`.`c_custkey` AND  `o`.`o_dt` = `c`.`dt`;

-- join key is a prefix set of dimension table primary keys (excluding partition key) + partition key.
```

For more details about Fluss partitioned table, see [Partitioned Tables](table-design/data-distribution/partitioning.md).

## Lookup Options

Fluss lookup join supports various configuration options. For more details, please refer to the [Connector Options](engine-flink/options.md#lookup-options) page.
