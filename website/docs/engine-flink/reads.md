---
sidebar_label: Reads
sidebar_position: 4
---

# Flink Reads
Fluss supports streaming and batch read with [Apache Flink](https://flink.apache.org/)'s SQL & Table API. Execute the following SQL command to switch execution mode from streaming to batch, and vice versa:
```sql 
-- Execute the flink job in streaming mode for current session context
SET 'execution.runtime-mode' = 'streaming';

-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
```

## Streaming Read
By default, Streaming read produces the latest snapshot on the table upon first startup, and continue to read the latest changes.

Fluss by default ensures that your startup is properly processed with all data included.

Fluss Source in streaming mode is unbounded, like a queue that never ends.
```sql
SET 'execution.runtime-mode' = 'streaming';
SELECT * FROM my_table ;
```

You can also do streaming read without reading the snapshot data, you can use `latest` scan mode, which only reads the changelogs (or logs) from the latest offset:
```sql
SELECT * FROM my_table /*+ OPTIONS('scan.startup.mode' = 'latest') */;
```

## Batch Read

### Limit Read
The Fluss source supports limiting reads for both primary-key tables and log tables, making it convenient to preview the latest `N` records in a table.

#### Example
1. Create a table and prepare data
```sql
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

INSERT INTO log_table
VALUES (1, 'Customer1', 'IVhzIApeRb ot,c,E', 15, '25-989-741-2988', 711.56, 'BUILDING', 'comment1'),
       (2, 'Customer2', 'XSTf4,NCwDVaWNe6tEgvwfmRchLXak', 13, '23-768-687-3665', 121.65, 'AUTOMOBILE', 'comment2'),
       (3, 'Customer3', 'MG9kdTD2WBHm', 1, '11-719-748-3364', 7498.12, 'AUTOMOBILE', 'comment3');
;
```

2. Query from table.
```sql
-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

SELECT * FROM log_table LIMIT 10;
```


### Point Query

The Fluss source supports point queries for primary-key tables, allowing you to inspect specific records efficiently. Currently, this functionality is exclusive to primary-key tables.

#### Example
1. Create a table and prepare data
```sql
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
INSERT INTO pk_table
VALUES (1, 'Customer1', 'IVhzIApeRb ot,c,E', 15, '25-989-741-2988', 711.56, 'BUILDING', 'comment1'),
       (2, 'Customer2', 'XSTf4,NCwDVaWNe6tEgvwfmRchLXak', 13, '23-768-687-3665', 121.65, 'AUTOMOBILE', 'comment2'),
       (3, 'Customer3', 'MG9kdTD2WBHm', 1, '11-719-748-3364', 7498.12, 'AUTOMOBILE', 'comment3');
;
```

2. Query from table.
```sql
-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

SELECT * FROM pk_table WHERE c_custkey = 1;
```

### Aggregations
The Fluss source supports pushdown count aggregation for the log table in batch mode. It is useful to preview the total number of the log table;
```sql
-- Execute the flink job in batch mode for current session context
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

SELECT COUNT(*) FROM log_table;
```


## Read Options

### scan.startup.mode
The scan startup mode enables you to specify the starting point for data consumption. Fluss currently supports the following `scan.startup.mode` options:
- `full` (default): For primary key tables, it first consumes the full data set and then consumes incremental data. For log tables, it starts consuming from the earliest offset.
- `earliest`: For primary key tables, it starts consuming from the earliest changelog offset; for log tables, it starts consuming from the earliest log offset.
- `latest`: For primary key tables, it starts consuming from the latest changelog offset; for log tables, it starts consuming from the latest log offset.
- `timestamp`: For primary key tables, it starts consuming the changelog from a specified time (defined by the configuration item `scan.startup.timestamp`); for log tables, it starts consuming from the offset corresponding to the specified time.


You can dynamically apply the scan parameters via SQL hints. For instance, the following SQL statement temporarily sets the `scan.startup.mode` to latest when consuming the `log_table` table.
```sql 
SELECT * FROM log_table /*+ OPTIONS('scan.startup.mode' = 'latest') */;
```

Also, the following SQL statement temporarily sets the `scan.startup.mode` to timestamp when consuming the `log_table` table.
```sql 
-- timestamp mode with microseconds.
SELECT * FROM log_table
/*+ OPTIONS('scan.startup.mode' = 'timestamp',
'scan.startup.timestamp' = '1678883047356') */;

-- timestamp mode with a time string format
SELECT * FROM log_table
/*+ OPTIONS('scan.startup.mode' = 'timestamp',
'scan.startup.timestamp' = '2023-12-09 23:09:12') */;
```

### scan.partition.discovery.interval
The interval in milliseconds for the Fluss source to discover the new partitions for partitioned table while scanning.  The default value is 10s. A non-positive value disables the partition discovery.








