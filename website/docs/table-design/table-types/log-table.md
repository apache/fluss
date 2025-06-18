---
title: "Log Table"
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

# Log Table

## Basic Concept
Log Table is a type of table in Fluss that is used to store data in the order in which it was written. Log Table only supports append records, and doesn't support Update/Delete operations.
Usually, Log Table is used to store logs in very high-throughput, like the typical use cases of Apache Kafka.

Log Table is created by not specifying the `PRIMARY KEY` clause in the `CREATE TABLE` statement. For example, the following Flink SQL statement will create a log table with 3 buckets.

```sql title="Flink SQL"
CREATE TABLE log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
)
WITH ('bucket.num' = '3');
```

:::note
The `bucket.num` should be a positive integer. If this value is not provided, a default value from the cluster will be used as the bucket number for the table.
:::

## Bucket Assigning
Bucketing is the fundamental unit of parallelism and scalability in Fluss.  A single table in Fluss is divided into multiple buckets. A bucket is the smallest storage unit for reads and writes. See more details about [Bucketing](table-design/data-distribution/bucketing.md).

When writing records into log table, Fluss will assign each record to a specific bucket based on the bucket assigning strategy. There are 3 strategies for bucket assigning in Fluss:
1. **Sticky Bucket Strategy**: As the default strategy, randomly select a bucket and consistently write into that bucket until a record batch is full. Sets `client.writer.bucket.no-key-assigner=sticky` to the table property to enable this strategy.
2. **Round-Robin Strategy**: Select a bucket in round-robin for each record before writing it in. Sets `client.writer.bucket.no-key-assigner=round_robin` to the table property to enable this strategy.
3. **Hash-based Bucketing**: If `bucket.key` property is set in the table property, Fluss will determine to assign records to bucket based on the hash value of the specified bucket keys, and the property `client.writer.bucket.no-key-assigner` will be ignored. For example, setting `'bucket.key' = 'c1,c2'` will assign buckets based on the values of columns `c1` and `c2`. Different column names should be separated by commas.


## Data Consumption
Log Tables in Fluss allow real-time data consumption, preserving the order of data within each bucket as it was written to the Fluss table. Specifically:
- For two data records from the same table and the same bucket, the data that was written to the Fluss table first will be consumed first.
- For two data records from the same partition but different buckets, the consumption order is not guaranteed because different buckets may be processed concurrently by different data consumption jobs.


## Column Pruning

Column pruning is a technique used to reduce the amount of data that needs to be read from storage by eliminating unnecessary columns from the query.
Fluss supports column pruning for Log Tables and the changelog of PrimaryKey Tables, which can significantly improve query performance by reducing the amount of data that needs to be read from storage and lowering networking costs.

What sets Fluss apart is its ability to apply **column pruning during streaming reads**, a capability that is both unique and industry-leading. This ensures that even in real-time streaming scenarios, only the required columns are processed, minimizing resource usage and maximizing efficiency.

In Fluss, Log Tables are stored in a columnar format by default (i.e., Apache Arrow).
This format stores data column-by-column rather than row-by-row, making it highly efficient for column pruning.
When a query specifies only a subset of columns, Fluss skips reading irrelevant columns entirely.
This enables efficient column pruning during query execution and ensures that only the required columns are read,
minimizing I/O overhead and improving overall system efficiency.

During query execution, query engines like Flink analyzes the query to identify the columns required for processing and tells Fluss to only read the necessary columns.
For example the following streaming query:

```sql
SELECT id, name FROM log_table WHERE timestamp > '2023-01-01';
```

In this query, only the `id`, `name`, and `timestamp` columns are accessed. Other columns (e.g., `address`, `status`) are pruned and not read from storage.


## Log Compression

**Log Table** supports end-to-end compression for the Arrow log format. Fluss leverages [Arrow native compression](https://arrow.apache.org/docs/format/Columnar.html#compression) to implement this feature,
ensuring that compressed data remains compliant with the Arrow format. As a result, the compressed data can be seamlessly decompressed by any Arrow-compatible library.
Additionally, compression is applied to each column independently, preserving the ability to perform column pruning on the compressed data without performance degradation.

When compression is enabled:
- For **Log Tables**, data is compressed by the writer on the client side, written in a compressed format, and decompressed by the log scanner on the client side.
- For **PrimaryKey Table changelogs**, compression is performed server-side since the changelog is generated on the server.

Log compression significantly reduces networking and storage costs. Benchmark results demonstrate that using the ZSTD compression with level 3 achieves a compression ratio of approximately **5x** (e.g., reducing 5GB of data to 1GB).
Furthermore, read/write throughput improves substantially due to reduced networking overhead.

By default, the Log Table uses the `ZSTD` compression codec with a compression level of `3`.
You can change the compression codec by setting the `table.log.arrow.compression.type` property to `NONE`, `LZ4_FRAME`, or `ZSTD`.
You can also adjust the compression level for `ZSTD` by setting the `table.log.arrow.compression.zstd.level` property to a value between `1` and `22`.

For example:

```sql title="Flink SQL"
-- Set the compression codec to LZ4_FRAME
CREATE TABLE log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
)
WITH (
  'table.log.arrow.compression.type' = 'LZ4_FRAME'
);

-- Set the 'ZSTD' compression level to 2
CREATE TABLE log_table (
  order_id BIGINT,
  item_id BIGINT,
  amount INT,
  address STRING
)
WITH (
  'table.log.arrow.compression.zstd.level' = '2'
);
```

In the above example, we set the compression codec to `LZ4_FRAME` and the compression level to `2`.

:::note 
1. Currently, the compression codec and compression level are only supported for arrow format. If you set `'table.log.format'='indexed'`, the compression codec and compression level will be ignored.
2. The valid range of `table.log.arrow.compression.zstd.level` is 1 to 22.
:::

## Log Tiering
Log Table supports tiering data to different storage tiers. See more details about [Remote Log](maintenance/tiered-storage/remote-storage.md).