---
sidebar_label: Lookups
sidebar_position: 5
---

# Flink Lookup Joins
Flink lookup joins are important because they enable efficient, real-time enrichment of streaming data with reference data, a common requirement in many real-time analytics and processing scenarios.


## Instructions
- Use a primary key table as a dimension table,  and the join condition must include all primary keys of the dimension table.
- Fluss lookup join is in asynchronous mode by default for higher throughput. You can change the mode of lookup join as synchronous mode by setting the SQL Hint `'lookup.async' = 'false'`.

## Examples
1. Create two tables.
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
USE CATALOG fluss_catalog;
```

```sql title="Flink SQL"
USE my_db;
```

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

## Lookup Options


| Option                                          | Type     | Required | Default     | Description                                                                                                                                                                                                                                                                                                                                             |
|-------------------------------------------------|----------|----------|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| lookup.cache                                    | Enum     | optional | NONE        | The caching strategy for this lookup table, including NONE, PARTIAL.                                                                                                                                                                                                                                                                                    |
| lookup.max-retries                              | Integer  | optional | 3           | The maximum allowed retries if a lookup operation fails.                                                                                                                                                                                                                                                                                                |
| lookup.partial-cache.expire-after-access        | Duration | optional | (none)      | Duration to expire an entry in the cache after accessing.                                                                                                                                                                                                                                                                                               |
| lookup.partial-cache.expire-after-write         | Duration | optional | (none)      | Duration to expire an entry in the cache after writing.                                                                                                                                                                                                                                                                                                 |
| lookup.partial-cache.cache-missing-key          | Boolean  | optional | true        | Whether to store an empty value into the cache if the lookup key doesn't match any rows in the table.                                                                                                                                                                                                                                                   |
| lookup.partial-cache.max-rows                   | Long     | optional | true        | The maximum number of rows to store in the cache.                                                                                                                                                                                                                                                                                                       |
