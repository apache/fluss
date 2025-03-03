---
sidebar_position: 1
---

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


## Log Tiering
Log Table supports tiering data to different storage tiers. See more details about [Remote Log](/docs/maintenance/tiered-storage/remote-storage/).

## Log Compression
Log Table supports the end-to-end block compression feature for arrow log format. If enabled, data will be compressed 
by the writer in client, written in compressed format and decompressed by the log scanner in client. Log compression 
can significantly reduce storage costs on the server side.

Currently, Log Table is defaulted to using `ZSTD` compression codec, with the compression level set to `3`. If you want to
modify the compression codec or compression level of `ZSTD` compression codec, you can create table as:

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

### Supported Compression Codecs
Currently, Fluss supports the following compression codecs for arrow format:
- `NONE`: No compression.
- `LZ4_FRAME`: LZ4 frame compression.
- `ZSTD`: ZSTD compression.