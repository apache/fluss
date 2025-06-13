---
sidebar_label: Reads
title: Flink Reads
sidebar_position: 4
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

# Flink Reads
Fluss supports streaming and batch read with [Apache Flink](https://flink.apache.org/)'s SQL & Table API. Execute the following SQL command to switch execution mode from streaming to batch, and vice versa:
```sql title="Flink SQL"
-- Execute the flink job in streaming mode for current session context
SET 'execution.runtime-mode' = 'streaming';
```

```sql title="Flink SQL"
-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
```

## Streaming Read
By default, Streaming read produces the latest snapshot on the table upon first startup, and continue to read the latest changes.

Fluss by default ensures that your startup is properly processed with all data included.

Fluss Source in streaming mode is unbounded, like a queue that never ends.
```sql title="Flink SQL"
SET 'execution.runtime-mode' = 'streaming';
```

```sql title="Flink SQL"
SELECT * FROM my_table ;
```

You can also do streaming read without reading the snapshot data, you can use `latest` scan mode, which only reads the changelogs (or logs) from the latest offset:
```sql title="Flink SQL"
SELECT * FROM my_table /*+ OPTIONS('scan.startup.mode' = 'latest') */;
```

### Column Pruning

Column pruning minimizes I/O by reading only the columns used in a query and ignoring unused ones at the storage layer.
In Fluss, column pruning is implemented using [Apache Arrow](https://arrow.apache.org/) as the default log format to optimize streaming reads from Log Tables and change logs of PrimaryKey Tables.
Benchmark results show that column pruning can reach 10x read performance improvement, and reduce unnecessary network traffic (reduce 80% I/O if 80% columns are not used).

:::note
1. Column pruning is only available when the table uses the Arrow log format (`'table.log.format' = 'arrow'`), which is enabled by default.
2. Reading log data from remote storage currently does not support column pruning.
:::

#### Example

**1. Create a table**
```sql title="Flink SQL"
CREATE TABLE `testcatalog`.`testdb`.`log_table` (
    `c_custkey` INT NOT NULL,
    `c_name` STRING NOT NULL,
    `c_address` STRING NOT NULL,
    `c_nationkey` INT NOT NULL,
    `c_phone` STRING NOT NULL,
    `c_acctbal` DECIMAL(15, 2) NOT NULL,
    `c_mktsegment` STRING NOT NULL,
    `c_comment` STRING NOT NULL
);
```

**2. Query a single column:**
```sql title="Flink SQL"
SELECT `c_name` FROM `testcatalog`.`testdb`.`log_table`;
```

**3. Verify with `EXPLAIN`:**
```sql title="Flink SQL"
EXPLAIN SELECT `c_name` FROM `testcatalog`.`testdb`.`log_table`;
```

**Output:**

```
== Optimized Execution Plan ==
TableSourceScan(table=[[testcatalog, testdb, log_table, project=[c_name]]], fields=[c_name])
```

This confirms that only the `c_name` column is being read from storage.

### Partition Pruning

Partition pruning is an optimization technique for Fluss partitioned tables. It reduces the number of partitions scanned during a query by filtering based on partition keys.
This optimization is especially useful in streaming scenarios for [Multi-Field Partitioned Tables](table-design/data-distribution/partitioning.md#multi-field-partitioned-tables) that has many partitions.
The partition pruning also supports dynamically pruning new created partitions during streaming read.

:::note
1. Currently, **only equality conditions** (e.g., `c_nationkey = 'US'`) are supported for partition pruning. Operators like `<`, `>`, `OR`, and `IN` are not yet supported.
:::

#### Example

**1. Create a partitioned table:**
```sql title="Flink SQL"
CREATE TABLE `testcatalog`.`testdb`.`log_partitioned_table` (
    `c_custkey` INT NOT NULL,
    `c_name` STRING NOT NULL,
    `c_address` STRING NOT NULL,
    `c_nationkey` STRING NOT NULL,
    `c_phone` STRING NOT NULL,
    `c_acctbal` DECIMAL(15, 2) NOT NULL,
    `c_mktsegment` STRING NOT NULL,
    `c_comment` STRING NOT NULL,
    `dt` STRING NOT NULL
) PARTITIONED BY (`c_nationkey`,`dt`);
```

**2. Query with partition filter:**
```sql title="Flink SQL"
SELECT * FROM `testcatalog`.`testdb`.`log_partitioned_table` WHERE `c_nationkey` = 'US';
```

Fluss source will scan only the partitions where `c_nationkey = 'US'`.
For example, if the following partitions exist:
- `US,2025-06-13`
- `China,2025-06-13`
- `US,2025-06-14`
- `China,2025-06-14`

Only `US,2025-06-13` and `US,2025-06-14` will be read.

As new partitions like `US,2025-06-15`, `China,2025-06-15` are created, partition `US,2025-06-15` will be automatically included in the stream, while `China,2025-06-15` will be dynamically filtered out based on the partition pruning condition.

**3. Verify with `EXPLAIN`:**

```sql title="Flink SQL"
EXPLAIN SELECT * FROM `testcatalog`.`testdb`.`log_partitioned_table` WHERE `c_nationkey` = 'US';
```

**Output:**

```text
== Optimized Execution Plan ==
TableSourceScan(table=[[testcatalog, testdb, log_partitioned_table, filter=[=(c_nationkey, _UTF-16LE'US':VARCHAR(2147483647) CHARACTER SET "UTF-16LE")]]], fields=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment, dt])
```

This confirms that only partitions matching `c_nationkey = 'US'` will be scanned.

## Batch Read

### Limit Read
The Fluss source supports limiting reads for both primary-key tables and log tables, making it convenient to preview the latest `N` records in a table.

#### Example
1. Create a table and prepare data
```sql title="Flink SQL"
CREATE TABLE log_table (
    `c_custkey` INT NOT NULL,
    `c_name` STRING NOT NULL,
    `c_address` STRING NOT NULL,
    `c_nationkey` INT NOT NULL,
    `c_phone` STRING NOT NULL,
    `c_acctbal` DECIMAL(15, 2) NOT NULL,
    `c_mktsegment` STRING NOT NULL,
    `c_comment` STRING NOT NULL
);
```

```sql title="Flink SQL"
INSERT INTO log_table
VALUES (1, 'Customer1', 'IVhzIApeRb ot,c,E', 15, '25-989-741-2988', 711.56, 'BUILDING', 'comment1'),
       (2, 'Customer2', 'XSTf4,NCwDVaWNe6tEgvwfmRchLXak', 13, '23-768-687-3665', 121.65, 'AUTOMOBILE', 'comment2'),
       (3, 'Customer3', 'MG9kdTD2WBHm', 1, '11-719-748-3364', 7498.12, 'AUTOMOBILE', 'comment3');
;
```

2. Query from table.
```sql title="Flink SQL"
-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
```

```sql title="Flink SQL"
SET 'sql-client.execution.result-mode' = 'tableau';
```

```sql title="Flink SQL"
SELECT * FROM log_table LIMIT 10;
```


### Point Query

The Fluss source supports point queries for primary-key tables, allowing you to inspect specific records efficiently. Currently, this functionality is exclusive to primary-key tables.

#### Example
1. Create a table and prepare data
```sql title="Flink SQL"
CREATE TABLE pk_table (
    `c_custkey` INT NOT NULL,
    `c_name` STRING NOT NULL,
    `c_address` STRING NOT NULL,
    `c_nationkey` INT NOT NULL,
    `c_phone` STRING NOT NULL,
    `c_acctbal` DECIMAL(15, 2) NOT NULL,
    `c_mktsegment` STRING NOT NULL,
    `c_comment` STRING NOT NULL,
    PRIMARY KEY (c_custkey) NOT ENFORCED
);
```

```sql title="Flink SQL"
INSERT INTO pk_table
VALUES (1, 'Customer1', 'IVhzIApeRb ot,c,E', 15, '25-989-741-2988', 711.56, 'BUILDING', 'comment1'),
       (2, 'Customer2', 'XSTf4,NCwDVaWNe6tEgvwfmRchLXak', 13, '23-768-687-3665', 121.65, 'AUTOMOBILE', 'comment2'),
       (3, 'Customer3', 'MG9kdTD2WBHm', 1, '11-719-748-3364', 7498.12, 'AUTOMOBILE', 'comment3');
```

2. Query from table.
```sql title="Flink SQL"
-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
```

```sql title="Flink SQL"
SET 'sql-client.execution.result-mode' = 'tableau';
```

```sql title="Flink SQL"
SELECT * FROM pk_table WHERE c_custkey = 1;
```

### Aggregations
The Fluss source supports pushdown count aggregation for the log table in batch mode. It is useful to preview the total number of the log table;
```sql title="Flink SQL"
-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
```

```sql title="Flink SQL"
SET 'sql-client.execution.result-mode' = 'tableau';
```

```sql title="Flink SQL"
SELECT COUNT(*) FROM log_table;
```


## Read Options

### Start Reading Position

The config option `scan.startup.mode` enables you to specify the starting point for data consumption. Fluss currently supports the following `scan.startup.mode` options:
- `full` (default): For primary key tables, it first consumes the full data set and then consumes incremental data. For log tables, it starts consuming from the earliest offset.
- `earliest`: For primary key tables, it starts consuming from the earliest changelog offset; for log tables, it starts consuming from the earliest log offset.
- `latest`: For primary key tables, it starts consuming from the latest changelog offset; for log tables, it starts consuming from the latest log offset.
- `timestamp`: For primary key tables, it starts consuming the changelog from a specified time (defined by the configuration item `scan.startup.timestamp`); for log tables, it starts consuming from the offset corresponding to the specified time.


You can dynamically apply the scan parameters via SQL hints. For instance, the following SQL statement temporarily sets the `scan.startup.mode` to latest when consuming the `log_table` table.
```sql title="Flink SQL"
SELECT * FROM log_table /*+ OPTIONS('scan.startup.mode' = 'latest') */;
```

Also, the following SQL statement temporarily sets the `scan.startup.mode` to timestamp when consuming the `log_table` table.
```sql title="Flink SQL"
-- timestamp mode with microseconds.
SELECT * FROM log_table
/*+ OPTIONS('scan.startup.mode' = 'timestamp',
'scan.startup.timestamp' = '1678883047356') */;
```

```sql title="Flink SQL"
-- timestamp mode with a time string format
SELECT * FROM log_table
/*+ OPTIONS('scan.startup.mode' = 'timestamp',
'scan.startup.timestamp' = '2023-12-09 23:09:12') */;
```










