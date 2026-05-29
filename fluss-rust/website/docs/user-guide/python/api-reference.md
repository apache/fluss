---
sidebar_position: 2
---
# API Reference

Complete API reference for the Fluss Python client.

## `Config`

| Method / Property                     | Config Key                            | Description                                                                             |
|---------------------------------------|---------------------------------------|-----------------------------------------------------------------------------------------|
| `Config(properties: dict = None)`     |                                       | Create config from a dict of key-value pairs                                            |
| `bootstrap_servers`                   | `bootstrap.servers`                   | Get/set coordinator server address                                                      |
| `writer_request_max_size`             | `writer.request-max-size`             | Get/set max request size in bytes                                                       |
| `writer_acks`                         | `writer.acks`                         | Get/set acknowledgment setting (`"all"` for all replicas)                               |
| `writer_retries`                      | `writer.retries`                      | Get/set number of retries on failure                                                    |
| `writer_batch_size`                   | `writer.batch-size`                   | Get/set write batch size in bytes. Upper bound when dynamic sizing is on; fixed batch size when off |
| `writer_dynamic_batch_size_enabled`   | `writer.dynamic-batch-size.enabled`   | Get/set whether the per-table dynamic batch size estimator is enabled (default `true`)  |
| `writer_dynamic_batch_size_min`       | `writer.dynamic-batch-size-min`       | Get/set the lower bound for the dynamic batch size estimator (default 256 KB; ignored when disabled) |
| `writer_batch_timeout_ms`             | `writer.batch-timeout-ms`             | Get/set max time in ms to wait for a writer batch to fill up before sending             |
| `writer_bucket_no_key_assigner`       | `writer.bucket.no-key-assigner`       | Get/set bucket assignment strategy (`"sticky"` or `"round_robin"`)                      |
| `scanner_remote_log_prefetch_num`     | `scanner.remote-log.prefetch-num`     | Get/set number of remote log segments to prefetch                                       |
| `remote_file_download_thread_num`     | `remote-file.download-thread-num`     | Get/set number of threads for remote log downloads                                      |
| `scanner_remote_log_read_concurrency` | `scanner.remote-log.read-concurrency` | Get/set streaming read concurrency within a remote log file                             |
| `scanner_log_max_poll_records`        | `scanner.log.max-poll-records`        | Get/set max number of records returned in a single poll()                               |
| `scanner_log_fetch_max_bytes`         | `scanner.log.fetch.max-bytes`         | Get/set maximum bytes per fetch response for LogScanner                                 |
| `scanner_log_fetch_min_bytes`         | `scanner.log.fetch.min-bytes`         | Get/set minimum bytes the server must accumulate before returning a fetch response      |
| `scanner_log_fetch_wait_max_time_ms`  | `scanner.log.fetch.wait-max-time-ms`  | Get/set maximum time (ms) the server may wait to satisfy min-bytes                      |
| `scanner_log_fetch_max_bytes_for_bucket` | `scanner.log.fetch.max-bytes-for-bucket` | Get/set maximum bytes per fetch response per bucket for LogScanner                |
| `connect_timeout_ms`                  | `connect-timeout`                     | Get/set TCP connect timeout in milliseconds                                             |
| `security_protocol`                   | `security.protocol`                   | Get/set security protocol (`"PLAINTEXT"` or `"sasl"`)                                   |
| `security_sasl_mechanism`             | `security.sasl.mechanism`             | Get/set SASL mechanism (only `"PLAIN"` is supported)                                    |
| `security_sasl_username`              | `security.sasl.username`              | Get/set SASL username (required when protocol is `"sasl"`)                              |
| `security_sasl_password`              | `security.sasl.password`              | Get/set SASL password (required when protocol is `"sasl"`)                              |

## `FlussConnection`

| Method                                                    |  Description                          |
|-----------------------------------------------------------|---------------------------------------|
| `await FlussConnection.create(config) -> FlussConnection` | Connect to a Fluss cluster            |
| `conn.get_admin() -> FlussAdmin`                        | Get admin interface                   |
| `await conn.get_table(table_path) -> FlussTable`          | Get a table for read/write operations |
| `await conn.close()`                                      | Close the connection                  |

Supports `async with` statement (async context manager).

## `FlussAdmin`

| Method                                                                                                                |  Description                          |
|-----------------------------------------------------------------------------------------------------------------------|---------------------------------------|
| `await create_database(name, database_descriptor=None, ignore_if_exists=False)`                                       | Create a database                     |
| `await drop_database(name, ignore_if_not_exists=False, cascade=True)`                                                 | Drop a database                       |
| `await list_databases() -> list[str]`                                                                                 | List all databases                    |
| `await database_exists(name) -> bool`                                                                                 | Check if a database exists            |
| `await get_database_info(name) -> DatabaseInfo`                                                                       | Get database metadata                 |
| `await create_table(table_path, table_descriptor, ignore_if_exists=False)`                                            | Create a table                        |
| `await drop_table(table_path, ignore_if_not_exists=False)`                                                            | Drop a table                          |
| `await get_table_info(table_path) -> TableInfo`                                                                       | Get table metadata                    |
| `await list_tables(database_name) -> list[str]`                                                                       | List tables in a database             |
| `await table_exists(table_path) -> bool`                                                                              | Check if a table exists               |
| `await list_offsets(table_path, bucket_ids, offset_spec) -> dict[int, int]`                           | Get offsets for buckets               |
| `await list_partition_offsets(table_path, partition_name, bucket_ids, offset_spec) -> dict[int, int]` | Get offsets for a partition's buckets |
| `await create_partition(table_path, partition_spec, ignore_if_exists=False)`                                          | Create a partition                    |
| `await drop_partition(table_path, partition_spec, ignore_if_not_exists=False)`                                        | Drop a partition                      |
| `await list_partition_infos(table_path) -> list[PartitionInfo]`                                                       | List partitions                       |
| `await get_latest_lake_snapshot(table_path) -> LakeSnapshot`                                                          | Get latest lake snapshot              |
| `await get_server_nodes() -> list[ServerNode]`                                                                        | Get all alive server nodes            |

## `ServerNode`

| Property                 | Description                                                |
|--------------------------|------------------------------------------------------------|
| `.id -> int`             | Server node ID                                             |
| `.host -> str`           | Hostname of the server                                     |
| `.port -> int`           | Port number                                                |
| `.server_type -> str`    | Server type (`"CoordinatorServer"` or `"TabletServer"`)    |
| `.uid -> str`            | Unique identifier (e.g. `"cs-0"`, `"ts-1"`)               |

## `FlussTable`

| Method                          |  Description                            |
|---------------------------------|-----------------------------------------|
| `new_scan() -> TableScan`       | Create a scan builder                   |
| `new_append() -> TableAppend`   | Create an append builder for log tables |
| `new_upsert() -> TableUpsert`   | Create an upsert builder for PK tables  |
| `new_lookup() -> TableLookup`   | Create a lookup builder for PK tables   |
| `get_table_info() -> TableInfo` | Get table metadata                      |
| `get_table_path() -> TablePath` | Get table path                          |
| `has_primary_key() -> bool`     | Check if table has a primary key        |

## `TableScan`

| Method                                                   |  Description                                                        |
|----------------------------------------------------------|---------------------------------------------------------------------|
| `.project(indices) -> TableScan`                         | Project columns by index                                            |
| `.project_by_name(names) -> TableScan`                   | Project columns by name                                             |
| `await .create_log_scanner() -> LogScanner`              | Create record-based scanner (for `poll()`)                          |
| `await .create_record_batch_log_scanner() -> LogScanner` | Create batch-based scanner (for `poll_arrow()`, `to_arrow()`, etc.) |

## `TableAppend`

Builder for creating an `AppendWriter`. Obtain via `FlussTable.new_append()`.

| Method                             |  Description             |
|------------------------------------|--------------------------|
| `.create_writer() -> AppendWriter` | Create the append writer |

## `TableUpsert`

Builder for creating an `UpsertWriter`. Obtain via `FlussTable.new_upsert()`.

| Method                                             |  Description                               |
|----------------------------------------------------|--------------------------------------------|
| `.partial_update_by_name(columns) -> TableUpsert`  | Configure partial update by column names   |
| `.partial_update_by_index(indices) -> TableUpsert` | Configure partial update by column indices |
| `.create_writer() -> UpsertWriter`                 | Create the upsert writer                   |

## `TableLookup`

Builder for creating a `Lookuper` or `PrefixLookuper`. Obtain via `FlussTable.new_lookup()`.

| Method                                              |  Description                              |
|-----------------------------------------------------|-------------------------------------------|
| `.create_lookuper() -> Lookuper`                    | Create a primary key lookuper             |
| `.lookup_by(column_names) -> TablePrefixLookup`     | Switch to prefix-scan mode for the given columns (partition keys + bucket keys) |

## `TablePrefixLookup`

Builder for creating a `PrefixLookuper`. Obtain via `TableLookup.lookup_by(columns)`.

| Method                                     |  Description              |
|--------------------------------------------|---------------------------|
| `.create_lookuper() -> PrefixLookuper`     | Create the prefix lookuper |

## `AppendWriter`

| Method                                           |  Description                        |
|--------------------------------------------------|-------------------------------------|
| `.append(row) -> WriteResultHandle`              | Append a row (dict, list, or tuple) |
| `.write_arrow(table)`                            | Write a PyArrow Table               |
| `.write_arrow_batch(batch) -> WriteResultHandle` | Write a PyArrow RecordBatch         |
| `.write_pandas(df)`                              | Write a Pandas DataFrame            |
| `await .flush()`                                 | Flush all pending writes            |

## `UpsertWriter`

| Method                              |  Description                          |
|-------------------------------------|---------------------------------------|
| `.upsert(row) -> WriteResultHandle` | Upsert a row (insert or update by PK) |
| `.delete(pk) -> WriteResultHandle`  | Delete a row by primary key           |
| `await .flush()`                    | Flush all pending operations          |

## `WriteResultHandle`

| Method          |  Description                                 |
|-----------------|----------------------------------------------|
| `await .wait()` | Wait for server acknowledgment of this write |

## `Lookuper`

| Method                              |  Description                |
|-------------------------------------|-----------------------------|
| `await .lookup(pk) -> dict \| None` | Lookup a row by primary key |

## `PrefixLookuper`

| Method                                        |  Description                                |
|-----------------------------------------------|---------------------------------------------|
| `await .lookup(prefix) -> list[dict]`         | Lookup all rows matching a prefix key       |

## `LogScanner`

| Method                                                        |  Description                                                                     |
|---------------------------------------------------------------|----------------------------------------------------------------------------------|
| `.subscribe(bucket_id, start_offset)`                         | Subscribe to a bucket                                                            |
| `.subscribe_buckets(bucket_offsets)`                          | Subscribe to multiple buckets (`{bucket_id: offset}`)                            |
| `.subscribe_partition(partition_id, bucket_id, start_offset)` | Subscribe to a partition bucket                                                  |
| `.subscribe_partition_buckets(partition_bucket_offsets)`      | Subscribe to multiple partition+bucket combos (`{(part_id, bucket_id): offset}`) |
| `.unsubscribe(bucket_id)`                                     | Unsubscribe from a bucket (non-partitioned tables)                               |
| `.unsubscribe_partition(partition_id, bucket_id)`             | Unsubscribe from a partition bucket                                              |
| `await .poll(timeout_ms) -> ScanRecords`                      | Poll individual records (record scanner only)                                    |
| `await .poll_arrow(timeout_ms) -> pa.Table`                   | Poll as Arrow Table (batch scanner only)                                         |
| `await .poll_record_batch(timeout_ms) -> list[RecordBatch]`   | Poll batches with metadata (batch scanner only)                                  |
| `.to_arrow_batch_reader() -> pa.RecordBatchReader`            | Lazy Arrow RecordBatchReader reading until latest offsets (batch scanner only)    |
| `await .to_arrow() -> pa.Table`                               | Read all subscribed data as Arrow Table (batch scanner only)                     |
| `await .to_pandas() -> pd.DataFrame`                          | Read all subscribed data as DataFrame (batch scanner only)                       |

> **Note:** Overlapping `poll_*` / `to_arrow*` / `to_arrow_batch_reader` calls on the same underlying scanner are not supported. Use only one active polling/consumption path at a time.

## `ScanRecords`

Returned by `LogScanner.poll()`. Records are grouped by bucket.

> **Note:** Flat iteration and integer indexing traverse buckets in an arbitrary order that is consistent within a single `ScanRecords` instance but may differ between `poll()` calls. Use per-bucket access (`.items()`, `.records(bucket)`) when bucket ordering matters.

```python
scan_records = await scanner.poll(timeout_ms=5000)

# Sequence access
scan_records[0]                              # first record
scan_records[-1]                             # last record
scan_records[:5]                             # first 5 records

# Per-bucket access
for bucket, records in scan_records.items():
    for record in records:
        print(f"bucket={bucket.bucket_id}, offset={record.offset}, row={record.row}")

# Flat iteration
for record in scan_records:
    print(record.row)
```

### Methods

| Method                                 |  Description                                                     |
|----------------------------------------|------------------------------------------------------------------|
| `.buckets() -> list[TableBucket]`      | List of distinct buckets                                         |
| `.records(bucket) -> list[ScanRecord]` | Records for a specific bucket (empty list if bucket not present) |
| `.count() -> int`                      | Total record count across all buckets                            |
| `.is_empty() -> bool`                  | Check if empty                                                   |

### Indexing

| Expression                   | Returns              | Description                       |
|------------------------------|----------------------|-----------------------------------|
| `scan_records[0]`           | `ScanRecord`         | Record by flat index              |
| `scan_records[-1]`          | `ScanRecord`         | Negative indexing                  |
| `scan_records[1:5]`         | `list[ScanRecord]`   | Slice                             |
| `scan_records[bucket]`      | `list[ScanRecord]`   | Records for a bucket              |

### Mapping Protocol

| Method / Protocol              | Description                                     |
|--------------------------------|-------------------------------------------------|
| `.keys()`                      | Same as `.buckets()`                            |
| `.values()`                    | Lazy iterator over record lists, one per bucket |
| `.items()`                     | Lazy iterator over `(bucket, records)` pairs    |
| `len(scan_records)`           | Same as `.count()`                              |
| `bucket in scan_records`      | Membership test                                 |
| `for record in scan_records`  | Flat iteration over all records                 |

## `ScanRecord`

| Property                     |  Description                                                        |
|------------------------------|---------------------------------------------------------------------|
| `.offset -> int`             | Record offset in the log                                            |
| `.timestamp -> int`          | Record timestamp                                                    |
| `.change_type -> ChangeType` | Change type (AppendOnly, Insert, UpdateBefore, UpdateAfter, Delete) |
| `.row -> dict`               | Row data as `{column_name: value}`                                  |

## `RecordBatch`

| Property                   | Description                  |
|----------------------------|------------------------------|
| `.batch -> pa.RecordBatch` | Arrow RecordBatch data       |
| `.bucket -> TableBucket`   | Bucket this batch belongs to |
| `.base_offset -> int`      | First record offset          |
| `.last_offset -> int`      | Last record offset           |

## `Schema`

| Method                                         |  Description               |
|------------------------------------------------|----------------------------|
| `Schema(schema: pa.Schema, primary_keys=None)` | Create from PyArrow schema. Field nullability (`pa.field(..., nullable=False)`) is preserved. |
| `.get_column_names() -> list[str]`             | Get column names           |
| `.get_column_types() -> list[str]`             | Get column type names. Non-nullable types include a `" NOT NULL"` suffix (e.g., `"int NOT NULL"`). |
| `.get_columns() -> list[tuple[str, str]]`      | Get `(name, type)` pairs. Type strings follow the same nullability formatting as `.get_column_types()`. |
| `.get_primary_keys() -> list[str]`             | Get primary key columns    |

## `TableDescriptor`

| Method                                                                                                                                                                         | Description             |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| `TableDescriptor(schema, *, partition_keys=None, bucket_count=None, bucket_keys=None, comment=None, log_format=None, kv_format=None, properties=None, custom_properties=None)` | Create table descriptor |
| `.get_schema() -> Schema`                                                                                                                                                      | Get the schema          |

## `TablePath`

| Method / Property            | Description         |
|------------------------------|---------------------|
| `TablePath(database, table)` | Create a table path |
| `.database_name -> str`      | Database name       |
| `.table_name -> str`         | Table name          |

## `TableInfo`

| Property / Method                    |  Description                |
|--------------------------------------|-----------------------------|
| `.table_id -> int`                   | Table ID                    |
| `.table_path -> TablePath`           | Table path                  |
| `.num_buckets -> int`                | Number of buckets           |
| `.schema_id -> int`                  | Schema ID                   |
| `.comment -> str \| None`            | Table comment               |
| `.created_time -> int`               | Creation timestamp          |
| `.modified_time -> int`              | Last modification timestamp |
| `.get_primary_keys() -> list[str]`   | Primary key columns         |
| `.get_partition_keys() -> list[str]` | Partition columns           |
| `.get_bucket_keys() -> list[str]`    | Bucket key columns          |
| `.has_primary_key() -> bool`         | Has primary key?            |
| `.is_partitioned() -> bool`          | Is partitioned?             |
| `.get_schema() -> Schema`            | Get table schema            |
| `.get_column_names() -> list[str]`   | Column names                |
| `.get_column_count() -> int`         | Number of columns           |
| `.get_properties() -> dict`          | All table properties        |
| `.get_custom_properties() -> dict`   | Custom properties only      |

## `PartitionInfo`

| Property                 |  Description   |
|--------------------------|----------------|
| `.partition_id -> int`   | Partition ID   |
| `.partition_name -> str` | Partition name |

## `DatabaseDescriptor`

| Method / Property                                          | Description       |
|------------------------------------------------------------|-------------------|
| `DatabaseDescriptor(comment=None, custom_properties=None)` | Create descriptor |
| `.comment -> str \| None`                                  | Database comment  |
| `.get_custom_properties() -> dict`                         | Custom properties |

## `DatabaseInfo`

| Property / Method                                  | Description                 |
|----------------------------------------------------|-----------------------------|
| `.database_name -> str`                            | Database name               |
| `.created_time -> int`                             | Creation timestamp          |
| `.modified_time -> int`                            | Last modification timestamp |
| `.get_database_descriptor() -> DatabaseDescriptor` | Get descriptor              |

## `LakeSnapshot`

| Property / Method                                 | Description             |
|---------------------------------------------------|-------------------------|
| `.snapshot_id -> int`                             | Snapshot ID             |
| `.table_buckets_offset -> dict[TableBucket, int]` | All bucket offsets      |
| `.get_bucket_offset(bucket) -> int \| None`       | Get offset for a bucket |
| `.get_table_buckets() -> list[TableBucket]`       | Get all buckets         |

## `TableBucket`

| Method / Property                                            | Description                            |
|--------------------------------------------------------------|----------------------------------------|
| `TableBucket(table_id, bucket)`                              | Create non-partitioned bucket          |
| `TableBucket.with_partition(table_id, partition_id, bucket)` | Create partitioned bucket              |
| `.table_id -> int`                                           | Table ID                               |
| `.bucket_id -> int`                                          | Bucket ID                              |
| `.partition_id -> int \| None`                               | Partition ID (None if non-partitioned) |

## `FlussError`

| Property             | Description                                                                         |
|----------------------|-------------------------------------------------------------------------------------|
| `.message -> str`    | Error message                                                                       |
| `.error_code -> int` | Error code (`ErrorCode.CLIENT_ERROR` for client-side errors, server code otherwise) |

Raised for all Fluss-specific errors (connection failures, table not found, schema mismatches, etc.). Inherits from `Exception`. See [Error Handling](./error-handling.md) for details on matching specific error codes.

## Constants

| Constant                     | Value         | Description                                         |
|------------------------------|---------------|-----------------------------------------------------|
| `fluss.EARLIEST_OFFSET`      | `-2`          | Start reading from earliest available offset        |

## `OffsetSpec`

| Method                      | Description                                      |
|-----------------------------|--------------------------------------------------|
| `OffsetSpec.earliest()`     | Earliest available offset                        |
| `OffsetSpec.latest()`       | Latest offset                                    |
| `OffsetSpec.timestamp(ts)`  | Offset at or after the given timestamp (millis)  |

To start reading from the latest offset (only new records), resolve the current offset via `list_offsets` before subscribing:

```python
offsets = await admin.list_offsets(table_path, [0], fluss.OffsetSpec.latest())
scanner.subscribe(bucket_id=0, start_offset=offsets[0])
```

## `ChangeType`

| Value                         | Short String | Description                   |
|-------------------------------|--------------|-------------------------------|
| `ChangeType.AppendOnly` (0)   | `+A`         | Append-only                   |
| `ChangeType.Insert` (1)       | `+I`         | Insert                        |
| `ChangeType.UpdateBefore` (2) | `-U`         | Previous value of updated row |
| `ChangeType.UpdateAfter` (3)  | `+U`         | New value of updated row      |
| `ChangeType.Delete` (4)       | `-D`         | Delete                        |
