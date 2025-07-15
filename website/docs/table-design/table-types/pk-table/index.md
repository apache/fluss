---
title: PrimaryKey Table
sidebar_position: 1
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# PrimaryKey Table

## Basic Concept

PrimaryKey Table in Fluss ensure the uniqueness of the specified primary key and supports `INSERT`, `UPDATE`,
and `DELETE` operations.

A PrimaryKey Table is created by specifying a `PRIMARY KEY` clause in the `CREATE TABLE` statement. For example, the
following Flink SQL statement creates a PrimaryKey Table with `shop_id` and `user_id` as the primary key and distributes
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

For [Partitioned PrimaryKey Table](table-design/data-distribution/partitioning.md), the primary key must contain the
partition key.

## Bucket Assigning

For primary key tables, Fluss always determines which bucket the data belongs to based on the hash value of the bucket
key (It must be a subset of the primary keys excluding partition keys of the primary key table) for each record. If the bucket key is not specified, the bucket key will used as the primary key (excluding the partition key).
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

The **Merge Engine** in Fluss is a core component designed to efficiently handle and consolidate data updates for PrimaryKey Tables.
It offers users the flexibility to define how incoming data records are merged with existing records sharing the same primary key.
However, users can specify a different merge engine to customize the merging behavior according to their specific use cases

The following merge engines are supported:

1. [Default Merge Engine (LastRow)](table-design/table-types/pk-table/merge-engines/default.md)
2. [FirstRow Merge Engine](table-design/table-types/pk-table/merge-engines/first-row.md)
3. [Versioned Merge Engine](table-design/table-types/pk-table/merge-engines/versioned.md)


## Changelog Generation

Fluss will capture the changes when inserting, updating, deleting records on the primary-key table, which is known as
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

If the data written to the primary-key table is
sequentially `+I(1, 2.0, 'apple')`, `+I(1, 4.0, 'banana')`, `-D(1, 4.0, 'banana')`, then the following change data will
be generated. For example, the following Flink SQL statements illustrate this behavior:

```sql title="Flink SQL"
-- set to batch mode to execute DELETE and INSERT statements
SET execution.runtime-mode = batch;

-- insert to records with the same primary key k=1
INSERT INTO T (k, v1) VALUES (1, 2.0,'apple');
INSERT INTO T (k, v1) VALUES (1, 4.0,'banana');

-- delete the record with primary key k=1
DELETE FROM T WHERE k = 1;

-- set to streaming mode to observe the changelogs
SET execution.runtime-mode = streaming;
SELECT * FROM T;
```

Generate the following output in the Flink SQL CLI:

```
+-----------------------------+
| op   | k    | v1   | v2     |
| ---- | ---- | ---- | ------ |
| +I   | 1    | 2.0  | apple  |
| -U   | 1    | 2.0  | apple  |
| +U   | 1    | 4.0  | banana |
| -D   | 1    | 4.0  | banana |
+-----------------------------+
4 rows in set
```

## Data Queries

For primary key tables, Fluss supports various kinds of querying abilities.

### Reads

For a primary key table, the default read method is a full snapshot followed by incremental data. First, the
snapshot data of the table is consumed, followed by the changelog data of the table.

It is also possible to only consume the changelog data of the table. For more details, please refer to the [Flink Reads](engine-flink/reads.md)

### Lookup

Fluss primary key table can lookup data by the primary keys. If the key exists in Fluss, lookup will return a unique row. it always used in [Flink Lookup Join](engine-flink/lookups.md#lookup).

### Prefix Lookup

Fluss primary key table can also do prefix lookup by the prefix subset primary keys. Unlike lookup, prefix lookup
will scan data based on the prefix of primary keys and may return multiple rows. It always used in [Flink Prefix Lookup Join](engine-flink/lookups.md#prefix-lookup).