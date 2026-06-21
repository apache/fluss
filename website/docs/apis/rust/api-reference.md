---
sidebar_position: 2
---
# API Reference

Complete API reference for the Fluss Rust client.

## `Config`

| Field                                 | Type            | Default            | Description                                                                          |
|---------------------------------------|-----------------|--------------------|--------------------------------------------------------------------------------------|
| `bootstrap_servers`                   | `String`        | `"127.0.0.1:9123"` | Coordinator server address                                                           |
| `writer_request_max_size`             | `i32`           | `10485760` (10 MB) | Maximum request size in bytes                                                        |
| `writer_acks`                         | `String`        | `"all"`            | Acknowledgment setting (`"all"` waits for all replicas)                              |
| `writer_retries`                      | `i32`           | `i32::MAX`         | Number of retries on failure                                                         |
| `writer_batch_size`                   | `i32`           | `2097152` (2 MB)   | Batch size for writes in bytes. Upper bound when dynamic sizing is on; fixed batch size when off. |
| `writer_dynamic_batch_size_enabled`   | `bool`          | `true`             | Enable per-table dynamic batch sizing: target grows 10% above 80% fill, shrinks 5% below 50%, clamped to `[writer_dynamic_batch_size_min, writer_batch_size]` |
| `writer_dynamic_batch_size_min`       | `i32`           | `262144` (256 KB)  | Lower bound for the dynamic batch size estimator (ignored when `writer_dynamic_batch_size_enabled` is `false`) |
| `writer_batch_timeout_ms`             | `i64`           | `100`              | Maximum time in ms to wait for a writer batch to fill up before sending              |
| `writer_bucket_no_key_assigner`       | `NoKeyAssigner` | `sticky`           | Bucket assignment strategy for tables without bucket keys: `sticky` or `round_robin` |
| `scanner_remote_log_prefetch_num`     | `usize`         | `4`                | Number of remote log segments to prefetch                                            |
| `remote_file_download_thread_num`     | `usize`         | `3`                | Number of threads for remote log downloads                                           |
| `scanner_remote_log_read_concurrency` | `usize`         | `4`                | Streaming read concurrency within a remote log file                                  |
| `scanner_log_max_poll_records`        | `usize`         | `500`              | Maximum number of records returned in a single poll()                                |
| `scanner_log_fetch_max_bytes`         | `i32`           | `16777216` (16 MB) | Maximum bytes per fetch response for LogScanner                                      |
| `scanner_log_fetch_min_bytes`         | `i32`           | `1`                | Minimum bytes the server must accumulate before returning a fetch response           |
| `scanner_log_fetch_wait_max_time_ms`  | `i32`           | `500`              | Maximum time (ms) the server may wait to satisfy min-bytes                           |
| `scanner_log_fetch_max_bytes_for_bucket`| `i32`         | `1048576` (1 MB)   | Maximum bytes per fetch response per bucket for LogScanner                           |
| `connect_timeout_ms`                  | `u64`           | `120000`           | TCP connect timeout in milliseconds                                                  |
| `security_protocol`                   | `String`        | `"PLAINTEXT"`      | `PLAINTEXT` (default) or `sasl` for SASL auth                                        |
| `security_sasl_mechanism`             | `String`        | `"PLAIN"`          | SASL mechanism (only `PLAIN` is supported)                                           |
| `security_sasl_username`              | `String`        | (empty)            | SASL username (required when protocol is `sasl`)                                     |
| `security_sasl_password`              | `String`        | (empty)            | SASL password (required when protocol is `sasl`)                                     |

## `FlussConnection`

| Method                                                                        | Description                                    |
|-------------------------------------------------------------------------------|------------------------------------------------|
| `async fn new(config: Config) -> Result<Self>`                                | Create a new connection to a Fluss cluster     |
| `fn get_admin(&self) -> Result<Arc<FlussAdmin>>`                              | Get the admin interface for cluster management |
| `async fn get_table(&self, table_path: &TablePath) -> Result<FlussTable<'_>>` | Get a table for read/write operations          |
| `fn config(&self) -> &Config`                                                 | Get a reference to the connection config       |

## `FlussAdmin`

### Database Operations

| Method                                                                                                                       | Description                |
|------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `async fn create_database(&self, name: &str, descriptor: Option<&DatabaseDescriptor>, ignore_if_exists: bool) -> Result<()>` | Create a database          |
| `async fn drop_database(&self, name: &str, ignore_if_not_exists: bool, cascade: bool) -> Result<()>`                         | Drop a database            |
| `async fn list_databases(&self) -> Result<Vec<String>>`                                                                      | List all databases         |
| `async fn database_exists(&self, name: &str) -> Result<bool>`                                                                | Check if a database exists |
| `async fn get_database_info(&self, name: &str) -> Result<DatabaseInfo>`                                                      | Get database metadata      |

### Table Operations

| Method                                                                                                                     | Description                                                                 |
|----------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------|
| `async fn create_table(&self, table_path: &TablePath, descriptor: &TableDescriptor, ignore_if_exists: bool) -> Result<()>` | Create a table                                                              |
| `async fn drop_table(&self, table_path: &TablePath, ignore_if_not_exists: bool) -> Result<()>`                             | Drop a table                                                                |
| `async fn get_table_info(&self, table_path: &TablePath) -> Result<TableInfo>`                                              | Get table metadata                                                          |
| `async fn get_table_schema(&self, table_path: &TablePath, schema_id: Option<i32>) -> Result<SchemaInfo>`                   | Get a table's schema by id, or the latest schema when `schema_id` is `None` |
| `async fn list_tables(&self, database_name: &str) -> Result<Vec<String>>`                                                  | List tables in a database                                                   |
| `async fn table_exists(&self, table_path: &TablePath) -> Result<bool>`                                                     | Check if a table exists                                                     |

### Partition Operations

| Method                                                                                                                               | Description                     |
|--------------------------------------------------------------------------------------------------------------------------------------|---------------------------------|
| `async fn list_partition_infos(&self, table_path: &TablePath) -> Result<Vec<PartitionInfo>>`                                         | List all partitions             |
| `async fn list_partition_infos_with_spec(&self, table_path: &TablePath, spec: Option<&PartitionSpec>) -> Result<Vec<PartitionInfo>>` | List partitions matching a spec |
| `async fn create_partition(&self, table_path: &TablePath, spec: &PartitionSpec, ignore_if_exists: bool) -> Result<()>`               | Create a partition              |
| `async fn drop_partition(&self, table_path: &TablePath, spec: &PartitionSpec, ignore_if_not_exists: bool) -> Result<()>`             | Drop a partition                |

### Offset Operations

| Method                                                                                                                                                           |  Description                          |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| `async fn list_offsets(&self, table_path: &TablePath, bucket_ids: &[i32], offset_spec: OffsetSpec) -> Result<HashMap<i32, i64>>`                                 | Get offsets for buckets               |
| `async fn list_partition_offsets(&self, table_path: &TablePath, partition_name: &str, bucket_ids: &[i32], offset_spec: OffsetSpec) -> Result<HashMap<i32, i64>>` | Get offsets for a partition's buckets |

### Lake Operations

| Method                                                                                     |  Description                 |
|--------------------------------------------------------------------------------------------|------------------------------|
| `async fn get_latest_lake_snapshot(&self, table_path: &TablePath) -> Result<LakeSnapshot>` | Get the latest lake snapshot |

### Cluster Operations

| Method                                                        | Description                                         |
|---------------------------------------------------------------|-----------------------------------------------------|
| `async fn get_server_nodes(&self) -> Result<Vec<ServerNode>>` | Get all alive server nodes (coordinator + tablets)  |

## `ServerNode`

| Method                            | Description                                          |
|-----------------------------------|------------------------------------------------------|
| `fn id(&self) -> i32`            | Server node ID                                       |
| `fn host(&self) -> &str`         | Hostname of the server                               |
| `fn port(&self) -> u32`          | Port number                                          |
| `fn server_type(&self) -> &ServerType` | Server type (`CoordinatorServer` or `TabletServer`) |
| `fn uid(&self) -> &str`          | Unique identifier (e.g. `"cs-0"`, `"ts-1"`)         |

## `FlussTable<'a>`

| Method                                        | Description                             |
|-----------------------------------------------|-----------------------------------------|
| `fn get_table_info(&self) -> &TableInfo`      | Get table metadata                      |
| `fn new_append(&self) -> Result<TableAppend>` | Create an append builder for log tables |
| `fn new_scan(&self) -> TableScan<'_>`         | Create a scan builder                   |
| `fn new_lookup(&self) -> Result<TableLookup>` | Create a lookup builder for PK tables   |
| `fn new_upsert(&self) -> Result<TableUpsert>` | Create an upsert builder for PK tables  |
| `fn has_primary_key(&self) -> bool`           | Check if the table has a primary key    |
| `fn table_path(&self) -> &TablePath`          | Get the table path                      |

## `TableAppend`

| Method                                            | Description             |
|---------------------------------------------------|-------------------------|
| `fn create_writer(&self) -> Result<AppendWriter>` | Create an append writer |

## `AppendWriter`

| Method                                                                          | Description                                       |
|---------------------------------------------------------------------------------|---------------------------------------------------|
| `fn append(&self, row: &impl InternalRow) -> Result<WriteResultFuture>`         | Append a row; returns a future for acknowledgment |
| `fn append_arrow_batch(&self, batch: RecordBatch) -> Result<WriteResultFuture>` | Append an Arrow RecordBatch                       |
| `async fn flush(&self) -> Result<()>`                                           | Flush all pending writes to the server            |

## `TableScan<'a>`

| Method                                                                      | Description                             |
|-----------------------------------------------------------------------------|-----------------------------------------|
| `fn project(self, indices: &[usize]) -> Result<Self>`                       | Project columns by index                |
| `fn project_by_name(self, names: &[&str]) -> Result<Self>`                  | Project columns by name                 |
| `fn limit(self, n: i32) -> Result<Self>`                                    | Set a row limit (enables `create_bucket_batch_scanner`; rejected by log scanners) |
| `fn create_log_scanner(self) -> Result<LogScanner>`                         | Create a record-based log scanner       |
| `fn create_record_batch_log_scanner(self) -> Result<RecordBatchLogScanner>` | Create an Arrow batch-based log scanner |
| `fn create_bucket_batch_scanner(self, bucket: TableBucket) -> Result<LimitBatchScanner>` | Bounded scan of one bucket (requires `limit`; runs on first `next_batch`) |

## `LogScanner`

Single-consumer: do not call `poll` concurrently on the same scanner (e.g. from `tokio::join!` or two tasks sharing an `Arc`). Mirrors Java's `LogScannerImpl.acquire()` guard. Debug builds surface overlapping calls via a `debug_assert!`; release builds skip the check for performance and produce skewed poll-timing metrics (`fluss.client.scanner.time_between_poll_ms`, `fluss.client.scanner.poll_idle_ratio`) if the contract is violated.

All `fluss.client.scanner.*` metrics carry `database` and `table` labels (matching Java's per-`TablePath` `ScannerMetricGroup`), so multi-table consumers get one time series per scanned table.

| Method                                                                                                    | Description                                              |
|-----------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| `async fn subscribe(&self, bucket_id: i32, start_offset: i64) -> Result<()>`                              | Subscribe to a bucket                                    |
| `async fn subscribe_buckets(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()>`                     | Subscribe to multiple buckets                            |
| `async fn subscribe_partition(&self, partition_id: i64, bucket_id: i32, start_offset: i64) -> Result<()>` | Subscribe to a partition bucket                          |
| `async fn subscribe_partition_buckets(&self, offsets: &HashMap<(i64, i32), i64>) -> Result<()>`           | Subscribe to multiple partition-bucket pairs             |
| `async fn unsubscribe(&self, bucket_id: i32) -> Result<()>`                                               | Unsubscribe from a bucket (non-partitioned tables)       |
| `async fn unsubscribe_partition(&self, partition_id: i64, bucket_id: i32) -> Result<()>`                  | Unsubscribe from a partition bucket (partitioned tables) |
| `async fn poll(&self, timeout: Duration) -> Result<ScanRecords>`                                          | Poll for records                                         |

## `RecordBatchLogScanner`

Single-consumer: overlapping `poll` calls on handles that share state, or `poll` concurrent with `RecordBatchLogReader::next_batch`, are not supported — use one active polling/consumption call at a time per underlying scanner state. Mirrors Java's `LogScannerImpl.acquire()` guard. Debug builds surface overlapping calls via a `debug_assert!`; release builds skip the check for performance and produce skewed poll-timing metrics (`fluss.client.scanner.time_between_poll_ms`, `fluss.client.scanner.poll_idle_ratio`) if the contract is violated.

| Method                                                                                                    | Description                                              |
|-----------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| `async fn subscribe(&self, bucket_id: i32, start_offset: i64) -> Result<()>`                              | Subscribe to a bucket                                    |
| `async fn subscribe_buckets(&self, bucket_offsets: &HashMap<i32, i64>) -> Result<()>`                     | Subscribe to multiple buckets                            |
| `async fn subscribe_partition(&self, partition_id: i64, bucket_id: i32, start_offset: i64) -> Result<()>` | Subscribe to a partition bucket                          |
| `async fn subscribe_partition_buckets(&self, offsets: &HashMap<(i64, i32), i64>) -> Result<()>`           | Subscribe to multiple partition-bucket pairs             |
| `async fn unsubscribe(&self, bucket_id: i32) -> Result<()>`                                               | Unsubscribe from a bucket (non-partitioned tables)       |
| `async fn unsubscribe_partition(&self, partition_id: i64, bucket_id: i32) -> Result<()>`                  | Unsubscribe from a partition bucket (partitioned tables) |
| `async fn poll(&self, timeout: Duration) -> Result<Vec<ScanBatch>>`                                       | Poll for Arrow record batches                            |
| `fn is_partitioned(&self) -> bool`                                                                        | Check if the table is partitioned                        |
| `fn get_subscribed_buckets(&self) -> Vec<(TableBucket, i64)>`                                             | Get all current subscriptions as (bucket, offset) pairs  |
| `fn schema(&self) -> SchemaRef`                                                                           | Get the Arrow schema for batches produced by this scanner|
| `fn table_path(&self) -> &TablePath`                                                                      | Get the table path                                       |
| `fn table_id(&self) -> TableId`                                                                           | Get the table ID                                         |

## `RecordBatchLogReader`

Bounded log reader that consumes data up to specified stopping offsets, then terminates.
Unlike `RecordBatchLogScanner` which polls indefinitely, this reader stops automatically.

| Method                                                                                                      | Description                                              |
|-------------------------------------------------------------------------------------------------------------|----------------------------------------------------------|
| `async fn new_until_latest(scanner: RecordBatchLogScanner, admin: &FlussAdmin) -> Result<Self>`              | Read until the latest offsets at time of creation         |
| `fn new_until_offsets(scanner: RecordBatchLogScanner, stopping_offsets: HashMap<TableBucket, i64>) -> Result<Self>` | Read until custom stopping offsets per bucket             |
| `async fn next_batch(&mut self) -> Result<Option<ScanBatch>>`                                                | Get the next batch with bucket/offset metadata, or `None` when all buckets caught up |
| `async fn collect_all_batches(&mut self) -> Result<Vec<ScanBatch>>`                                          | Drain all batches (with metadata) until stopping offsets are satisfied |
| `fn schema(&self) -> SchemaRef`                                                                              | Arrow schema for produced batches                        |
| `fn to_record_batch_reader(self, handle: tokio::runtime::Handle) -> SyncRecordBatchLogReader`                | Sync adapter implementing `arrow::RecordBatchReader` (see below) |

## `SyncRecordBatchLogReader`

Synchronous adapter for `RecordBatchLogReader`. Created via
`RecordBatchLogReader::to_record_batch_reader(handle)`.

Implements both [`Iterator`] and [`arrow::record_batch::RecordBatchReader`], so it
plugs into the wider Arrow ecosystem — FFI, PyArrow's
`pa.RecordBatchReader.from_batches`, the C++ Arrow `RecordBatchReader` interface,
DataFusion sources, etc.

Each `next()` call drives the underlying async reader via
`tokio::runtime::Handle::block_on`. **Do not call from inside a Tokio worker
thread that belongs to the same runtime** — nested `block_on` panics. Prefer
`RecordBatchLogReader::next_batch` in async Rust code; use this adapter only at
sync/FFI boundaries.

Bucket and offset metadata carried by `ScanBatch` is **dropped** here, because
the Arrow trait contract yields plain `RecordBatch`. If you need offsets or
bucket identity per batch, use `next_batch` instead.

| Method                                                          | Description                                      |
|-----------------------------------------------------------------|--------------------------------------------------|
| `fn next(&mut self) -> Option<Result<RecordBatch, ArrowError>>` | Iterator: next batch, or `None` when caught up   |
| `fn schema(&self) -> SchemaRef`                                 | Arrow schema for produced batches                |

## `LimitBatchScanner`

One-shot bounded scanner from `TableScan::limit(n).create_bucket_batch_scanner(bucket)`.
Poll it with `next_batch` until it returns `None` (mirrors `RecordBatchLogReader`).
Supports both log and primary-key tables (the latter returns the current,
server-deduplicated state); yields a single batch of at most `n` rows.

| Method                                                        | Description                          |
|---------------------------------------------------------------|--------------------------------------|
| `async fn next_batch(&mut self) -> Result<Option<ScanBatch>>` | Rows on the first call, `None` after |
| `async fn collect_all_batches(&mut self) -> Result<Vec<ScanBatch>>` | Drain into all batches         |
| `fn bucket(&self) -> &TableBucket`                            | The scanned bucket                   |

## `ScanRecord`

| Method                                 | Description                            |
|----------------------------------------|----------------------------------------|
| `fn row(&self) -> &dyn InternalRow`    | Get the row data                       |
| `fn offset(&self) -> i64`              | Record offset in the log               |
| `fn timestamp(&self) -> i64`           | Record timestamp                       |
| `fn change_type(&self) -> &ChangeType` | Change type (AppendOnly, Insert, etc.) |

## `ScanRecords`

| Method                                                                   | Description                       |
|--------------------------------------------------------------------------|-----------------------------------|
| `fn count(&self) -> usize`                                               | Number of records                 |
| `fn is_empty(&self) -> bool`                                             | Whether the result set is empty   |
| `fn records(&self, bucket: &TableBucket) -> &[ScanRecord]`               | Get records for a specific bucket |
| `fn records_by_buckets(&self) -> &HashMap<TableBucket, Vec<ScanRecord>>` | Get all records grouped by bucket |

`ScanRecords` also implements `IntoIterator`, so you can iterate over all records directly:

```rust
for record in records {
    println!("offset={}", record.offset());
}
```

## `ScanBatch`

| Method                             | Description                    |
|------------------------------------|--------------------------------|
| `fn bucket(&self) -> &TableBucket` | Bucket this batch belongs to   |
| `fn batch(&self) -> &RecordBatch`  | Arrow RecordBatch data         |
| `fn base_offset(&self) -> i64`     | First record offset            |
| `fn last_offset(&self) -> i64`     | Last record offset             |
| `fn num_records(&self) -> usize`   | Number of records in the batch |

## `TableUpsert`

| Method                                                                                | Description                                       |
|---------------------------------------------------------------------------------------|---------------------------------------------------|
| `fn create_writer(&self) -> Result<UpsertWriter>`                                     | Create an upsert writer                           |
| `fn partial_update(&self, column_indices: Option<Vec<usize>>) -> Result<TableUpsert>` | Create a partial update builder by column indices |
| `fn partial_update_with_column_names(&self, names: &[&str]) -> Result<TableUpsert>`   | Create a partial update builder by column names   |

## `UpsertWriter`

| Method                                                                  | Description                           |
|-------------------------------------------------------------------------|---------------------------------------|
| `fn upsert(&self, row: &impl InternalRow) -> Result<WriteResultFuture>` | Upsert a row (insert or update by PK) |
| `fn delete(&self, row: &impl InternalRow) -> Result<WriteResultFuture>` | Delete a row by primary key           |
| `async fn flush(&self) -> Result<()>`                                   | Flush all pending operations          |

## `TableLookup`

| Method                                          |  Description                        |
|-------------------------------------------------|-------------------------------------|
| `fn create_lookuper(&self) -> Result<Lookuper>` | Create a lookuper for point lookups |

## `Lookuper`

| Method                                                                       |  Description                |
|------------------------------------------------------------------------------|-----------------------------|
| `async fn lookup(&mut self, key: &impl InternalRow) -> Result<LookupResult>` | Lookup a row by primary key |

## `LookupResult`

| Method                                                         |  Description                     |
|----------------------------------------------------------------|----------------------------------|
| `fn get_single_row(&self) -> Result<Option<impl InternalRow>>` | Get a single row from the result |
| `fn get_rows(&self) -> Result<Vec<impl InternalRow>>`          | Get all rows from the result     |
| `fn to_record_batch(&self) -> Result<RecordBatch>`             | Convert all rows to an Arrow `RecordBatch` for DataFusion or other Arrow-based tools    |

## `WriteResultFuture`

| Description                                                                                                                                   |
|-----------------------------------------------------------------------------------------------------------------------------------------------|
| Implements `Future<Output = Result<(), Error>>`. Await to wait for server acknowledgment. Returned by `append()`, `upsert()`, and `delete()`. |

Usage:

```rust
// Fire-and-forget (batched)
writer.append(&row)?;
writer.flush().await?;

// Per-record acknowledgment
writer.append(&row)?.await?;
```

## `Schema`

| Method                                         |  Description                             |
|------------------------------------------------|------------------------------------------|
| `fn builder() -> SchemaBuilder`                | Create a schema builder                  |
| `fn columns(&self) -> &[Column]`               | Get all columns                          |
| `fn primary_key(&self) -> Option<&PrimaryKey>` | Get primary key (None if no primary key) |
| `fn column_names(&self) -> Vec<&str>`          | Get all column names                     |
| `fn primary_key_indexes(&self) -> Vec<usize>`  | Get primary key column indices           |

## `SchemaBuilder`

| Method                                               |  Description            |
|------------------------------------------------------|-------------------------|
| `fn column(name: &str, data_type: DataType) -> Self` | Add a column            |
| `fn primary_key(keys: Vec<&str>) -> Self`            | Set primary key columns |
| `fn build() -> Result<Schema>`                       | Build the schema        |

## `SchemaInfo`

A schema together with its server-assigned version id. Returned by [`FlussAdmin::get_table_schema`](#flussadmin).

| Method                                           | Description                              |
|--------------------------------------------------|------------------------------------------|
| `fn new(schema: Schema, schema_id: i32) -> Self` | Construct from a schema and id           |
| `fn schema(&self) -> &Schema`                    | Borrow the schema                        |
| `fn schema_id(&self) -> i32`                     | Get the server-assigned schema id        |
| `fn into_parts(self) -> (Schema, i32)`           | Consume and return `(schema, schema_id)` |

## `TableDescriptor`

| Method                                                    | Description                          |
|-----------------------------------------------------------|--------------------------------------|
| `fn builder() -> TableDescriptorBuilder`                  | Create a table descriptor builder    |
| `fn schema(&self) -> &Schema`                             | Get the table schema                 |
| `fn partition_keys(&self) -> &[String]`                   | Get partition key column names       |
| `fn has_primary_key(&self) -> bool`                       | Check if the table has a primary key |
| `fn properties(&self) -> &HashMap<String, String>`        | Get all table properties             |
| `fn custom_properties(&self) -> &HashMap<String, String>` | Get custom properties                |
| `fn comment(&self) -> Option<&str>`                       | Get table comment                    |

## `TableDescriptorBuilder`

| Method                                                                                    | Description                                 |
|-------------------------------------------------------------------------------------------|---------------------------------------------|
| `fn schema(schema: Schema) -> Self`                                                       | Set the schema                              |
| `fn log_format(format: LogFormat) -> Self`                                                | Set log format (e.g., `LogFormat::ARROW`)   |
| `fn kv_format(format: KvFormat) -> Self`                                                  | Set KV format (e.g., `KvFormat::COMPACTED`) |
| `fn property(key: &str, value: &str) -> Self`                                             | Set a table property                        |
| `fn custom_property(key: impl Into<String>, value: impl Into<String>) -> Self`            | Set a single custom property                |
| `fn custom_properties(properties: HashMap<impl Into<String>, impl Into<String>>) -> Self` | Set custom properties                       |
| `fn partitioned_by(keys: Vec<&str>) -> Self`                                              | Set partition columns                       |
| `fn distributed_by(bucket_count: Option<i32>, bucket_keys: Vec<String>) -> Self`          | Set bucket distribution                     |
| `fn comment(comment: &str) -> Self`                                                       | Set table comment                           |
| `fn build() -> Result<TableDescriptor>`                                                   | Build the table descriptor                  |

## `TablePath`

| Method                                                |  Description        |
|-------------------------------------------------------|---------------------|
| `TablePath::new(database: &str, table: &str) -> Self` | Create a table path |
| `fn database(&self) -> &str`                          | Get database name   |
| `fn table(&self) -> &str`                             | Get table name      |

## `TableInfo`

| Field / Method       | Description                                         |
|----------------------|-----------------------------------------------------|
| `.table_path`        | `TablePath` -- Table path                           |
| `.table_id`          | `i64` -- Table ID                                   |
| `.schema_id`         | `i32` -- Schema ID                                  |
| `.schema`            | `Schema` -- Table schema                            |
| `.primary_keys`      | `Vec<String>` -- Primary key column names           |
| `.partition_keys`    | `Vec<String>` -- Partition key column names         |
| `.num_buckets`       | `i32` -- Number of buckets                          |
| `.properties`        | `HashMap<String, String>` -- All table properties   |
| `.custom_properties` | `HashMap<String, String>` -- Custom properties only |
| `.comment`           | `Option<String>` -- Table comment                   |
| `.created_time`      | `i64` -- Creation timestamp                         |
| `.modified_time`     | `i64` -- Last modification timestamp                |

## `TableBucket`

| Method                                                                                              | Description                                |
|-----------------------------------------------------------------------------------------------------|--------------------------------------------|
| `TableBucket::new(table_id: i64, bucket_id: i32) -> Self`                                           | Create a non-partitioned bucket            |
| `TableBucket::new_with_partition(table_id: i64, partition_id: Option<i64>, bucket_id: i32) -> Self` | Create a partitioned bucket                |
| `fn table_id(&self) -> i64`                                                                         | Get table ID                               |
| `fn partition_id(&self) -> Option<i64>`                                                             | Get partition ID (None if non-partitioned) |
| `fn bucket_id(&self) -> i32`                                                                        | Get bucket ID                              |

## `PartitionSpec`

| Method                                                      | Description                                           |
|-------------------------------------------------------------|-------------------------------------------------------|
| `PartitionSpec::new(spec_map: HashMap<&str, &str>) -> Self` | Create from a map of partition column names to values |
| `fn get_spec_map(&self) -> &HashMap<String, String>`        | Get the partition spec map                            |

## `PartitionInfo`

| Method                                   |  Description       |
|------------------------------------------|--------------------|
| `fn get_partition_id(&self) -> i64`      | Get partition ID   |
| `fn get_partition_name(&self) -> String` | Get partition name |

## `DatabaseDescriptor`

| Method                                                    | Description                          |
|-----------------------------------------------------------|--------------------------------------|
| `fn builder() -> DatabaseDescriptorBuilder`               | Create a database descriptor builder |
| `fn comment(&self) -> Option<&str>`                       | Get database comment                 |
| `fn custom_properties(&self) -> &HashMap<String, String>` | Get custom properties                |

## `DatabaseDescriptorBuilder`

| Method                                                                                    | Description                   |
|-------------------------------------------------------------------------------------------|-------------------------------|
| `fn comment(comment: impl Into<String>) -> Self`                                          | Set database comment          |
| `fn custom_properties(properties: HashMap<impl Into<String>, impl Into<String>>) -> Self` | Set custom properties         |
| `fn custom_property(key: impl Into<String>, value: impl Into<String>) -> Self`            | Set a single custom property  |
| `fn build() -> DatabaseDescriptor`                                                        | Build the database descriptor |

## `DatabaseInfo`

| Method                                                 | Description                     |
|--------------------------------------------------------|---------------------------------|
| `fn database_name(&self) -> &str`                      | Get database name               |
| `fn created_time(&self) -> i64`                        | Get creation timestamp          |
| `fn modified_time(&self) -> i64`                       | Get last modification timestamp |
| `fn database_descriptor(&self) -> &DatabaseDescriptor` | Get the database descriptor     |

## `LakeSnapshot`

| Field                   | Description                                       |
|-------------------------|---------------------------------------------------|
| `.snapshot_id`          | `i64` -- Snapshot ID                              |
| `.table_buckets_offset` | `HashMap<TableBucket, i64>` -- All bucket offsets |

## `GenericRow<'a>`

| Method                                                             | Description                                      |
|--------------------------------------------------------------------|--------------------------------------------------|
| `GenericRow::new(field_count: usize) -> Self`                      | Create a new row with the given number of fields |
| `fn set_field(&mut self, pos: usize, value: impl Into<Datum<'a>>)` | Set a field value by position                    |
| `GenericRow::from_data(data: Vec<impl Into<Datum<'a>>>) -> Self`   | Create a row from existing field data            |

Implements the `InternalRow` trait (see below).

## `InternalRow` trait

| Method                                                                                 | Description                             |
|----------------------------------------------------------------------------------------|-----------------------------------------|
| `fn is_null_at(&self, idx: usize) -> Result<bool>`                                     | Check if a field is null                |
| `fn get_boolean(&self, idx: usize) -> Result<bool>`                                    | Get boolean value                       |
| `fn get_byte(&self, idx: usize) -> Result<i8>`                                         | Get tinyint value                       |
| `fn get_short(&self, idx: usize) -> Result<i16>`                                       | Get smallint value                      |
| `fn get_int(&self, idx: usize) -> Result<i32>`                                         | Get int value                           |
| `fn get_long(&self, idx: usize) -> Result<i64>`                                        | Get bigint value                        |
| `fn get_float(&self, idx: usize) -> Result<f32>`                                       | Get float value                         |
| `fn get_double(&self, idx: usize) -> Result<f64>`                                      | Get double value                        |
| `fn get_string(&self, idx: usize) -> Result<&str>`                                     | Get string value                        |
| `fn get_decimal(&self, idx: usize, precision: usize, scale: usize) -> Result<Decimal>` | Get decimal value                       |
| `fn get_date(&self, idx: usize) -> Result<Date>`                                       | Get date value                          |
| `fn get_time(&self, idx: usize) -> Result<Time>`                                       | Get time value                          |
| `fn get_timestamp_ntz(&self, idx: usize, precision: u32) -> Result<TimestampNtz>`      | Get timestamp value                     |
| `fn get_timestamp_ltz(&self, idx: usize, precision: u32) -> Result<TimestampLtz>`      | Get timestamp with local timezone value |
| `fn get_bytes(&self, idx: usize) -> Result<&[u8]>`                                     | Get bytes value                         |
| `fn get_binary(&self, idx: usize, length: usize) -> Result<&[u8]>`                     | Get fixed-length binary value           |
| `fn get_char(&self, idx: usize, length: usize) -> Result<&str>`                        | Get fixed-length char value             |
| `fn get_array(&self, idx: usize) -> Result<FlussArray>`                                | Get array value                         |
| `fn get_map(&self, idx: usize) -> Result<FlussMap>`                                    | Get map value                           |

## `FlussArray`

`FlussArray` is the Rust row representation for `ARRAY` values. You usually obtain it from `InternalRow::get_array()`.

| Method | Description |
|--------|-------------|
| `fn size(&self) -> usize` | Number of elements in the array |
| `fn is_null_at(&self, pos: usize) -> bool` | Check whether an element is null |
| `fn as_bytes(&self) -> &[u8]` | Get encoded bytes of the array |

Element getters mirror `InternalRow` typed getters and return `Result<T>`. For example, use `get_int()`, `get_long()`, and `get_double()` for primitive elements, and `get_string()`, `get_binary()`, `get_decimal()`, `get_timestamp_ntz()`, `get_timestamp_ltz()`, and `get_array()` for variable-length or nested elements.

## `FlussMap`

`FlussMap` is the Rust row representation for `MAP` values. You usually obtain it from `InternalRow::get_map()`.

| Method | Description |
|--------|-------------|
| `fn size(&self) -> usize` | Number of entries in the map |
| `fn as_bytes(&self) -> &[u8]` | Get encoded bytes of the map |
| `fn key_type(&self) -> &DataType` | Schema-declared type of keys |
| `fn value_type(&self) -> &DataType` | Schema-declared type of values |
| `fn entries(&self) -> Entries<'_>` | Iterator yielding `Result<(Datum, Datum)>` pairs |
| `fn get(&self, key: &Datum) -> Result<Option<Datum>>` | Linear-scan lookup by key (`O(n)`) |
| `fn key_array(&self) -> &FlussArray` | Parallel keys array (zero-copy view) |
| `fn value_array(&self) -> &FlussArray` | Parallel values array (zero-copy view) |

Most user code should prefer `entries()` (iteration) and `get()` (lookup). The `key_array()` / `value_array()` views are for serdes and Arrow-adapter code that needs zero-copy access to the underlying parallel-array layout.

## `FlussMapWriter`

`FlussMapWriter` builds a `FlussMap` for write paths.

| Method | Description |
|--------|-------------|
| `fn new(capacity: usize, key_type: &DataType, value_type: &DataType) -> Self` | Create a writer sized for `capacity` entries |
| `fn write_entry(&mut self, key: Datum, value: Datum) -> Result<()>` | Append a single entry; rejects null keys and type mismatches |
| `fn extend<I, K, V>(&mut self, entries: I) -> Result<()>` | Append every pair from `entries: IntoIterator<Item = (K, V)>` |
| `fn complete(self) -> Result<FlussMap>` | Finalize the writer and produce the `FlussMap` |

## `ChangeType`

| Value                      | Short String  | Description                      |
|----------------------------|---------------|----------------------------------|
| `ChangeType::AppendOnly`   | `+A`          | Append-only record               |
| `ChangeType::Insert`       | `+I`          | Inserted row                     |
| `ChangeType::UpdateBefore` | `-U`          | Previous value of an updated row |
| `ChangeType::UpdateAfter`  | `+U`          | New value of an updated row      |
| `ChangeType::Delete`       | `-D`          | Deleted row                      |

| Method                           | Description                         |
|----------------------------------|-------------------------------------|
| `fn short_string(&self) -> &str` | Get the short string representation |

## `OffsetSpec`

| Variant                      | Description                                     |
|------------------------------|-------------------------------------------------|
| `OffsetSpec::Earliest`       | Start from the earliest available offset        |
| `OffsetSpec::Latest`         | Start from the latest offset (only new records) |
| `OffsetSpec::Timestamp(i64)` | Start from a specific timestamp in milliseconds |

## Constants

| Constant                         | Value  | Description                                             |
|----------------------------------|--------|---------------------------------------------------------|
| `fluss::client::EARLIEST_OFFSET` | `-2`   | Start reading from the earliest available offset        |

To start reading from the latest offset (only new records), resolve the current offset via `list_offsets` before subscribing:

```rust
use fluss::rpc::message::OffsetSpec;

let offsets = admin.list_offsets(&table_path, &[0], OffsetSpec::Latest).await?;
let latest = offsets[&0];
log_scanner.subscribe(0, latest).await?;
```

## `DataTypes` factory

| Method                                           | Returns    | Description                        |
|--------------------------------------------------|------------|------------------------------------|
| `DataTypes::boolean()`                           | `DataType` | Boolean type                       |
| `DataTypes::tinyint()`                           | `DataType` | 8-bit signed integer               |
| `DataTypes::smallint()`                          | `DataType` | 16-bit signed integer              |
| `DataTypes::int()`                               | `DataType` | 32-bit signed integer              |
| `DataTypes::bigint()`                            | `DataType` | 64-bit signed integer              |
| `DataTypes::float()`                             | `DataType` | 32-bit floating point              |
| `DataTypes::double()`                            | `DataType` | 64-bit floating point              |
| `DataTypes::string()`                            | `DataType` | Variable-length string             |
| `DataTypes::bytes()`                             | `DataType` | Variable-length byte array         |
| `DataTypes::date()`                              | `DataType` | Date (days since epoch)            |
| `DataTypes::time()`                              | `DataType` | Time (milliseconds since midnight) |
| `DataTypes::timestamp()`                         | `DataType` | Timestamp without timezone         |
| `DataTypes::timestamp_ltz()`                     | `DataType` | Timestamp with local timezone      |
| `DataTypes::decimal(precision: u32, scale: u32)` | `DataType` | Fixed-point decimal                |
| `DataTypes::char(length: u32)`                   | `DataType` | Fixed-length string                |
| `DataTypes::binary(length: usize)`               | `DataType` | Fixed-length byte array            |
| `DataTypes::array(element: DataType)`            | `DataType` | Array of elements                  |
| `DataTypes::map(key: DataType, value: DataType)` | `DataType` | Map of key-value pairs             |
| `DataTypes::row(fields: Vec<DataField>)`         | `DataType` | Nested row type                    |

## `DataField`

| Method                                                                                                   | Description         |
|----------------------------------------------------------------------------------------------------------|---------------------|
| `DataField::new(name: impl Into<String>, data_type: DataType, description: Option<String>) -> DataField` | Create a data field |
| `fn name(&self) -> &str`                                                                                 | Get the field name  |
