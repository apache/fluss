---
sidebar_position: 2
---
# API Reference

Complete API reference for the Fluss C++ client.

## `Result`

| Field / Method  | Type          | Description                                                    |
|-----------------|---------------|----------------------------------------------------------------|
| `error_code`    | `int32_t`     | 0 for success, non-zero for errors                             |
| `error_message` | `std::string` | Human-readable error description                               |
| `Ok()`          | `bool`        | Returns `true` if operation succeeded (`error_code == 0`)      |

## `Configuration`

| Field                                 | Type          | Default              | Description                                                                              |
|---------------------------------------|---------------|----------------------|------------------------------------------------------------------------------------------|
| `bootstrap_servers`                   | `std::string` | `"127.0.0.1:9123"`   | Coordinator server address                                                               |
| `writer_request_max_size`             | `int32_t`     | `10485760` (10 MB)   | Maximum request size in bytes                                                            |
| `writer_acks`                         | `std::string` | `"all"`              | Acknowledgment setting (`"all"`, `"0"`, `"1"`, or `"-1"`)                                |
| `writer_retries`                      | `int32_t`     | `INT32_MAX`          | Number of retries on failure                                                             |
| `writer_batch_size`                   | `int32_t`     | `2097152` (2 MB)     | Batch size for writes in bytes. Upper bound when dynamic sizing is on; fixed batch size when off |
| `writer_dynamic_batch_size_enabled`   | `bool`        | `true`               | Enable per-table dynamic batch sizing: target grows 10% above 80% fill, shrinks 5% below 50% |
| `writer_dynamic_batch_size_min`       | `int32_t`     | `262144` (256 KB)    | Lower bound for the dynamic batch size estimator (ignored when disabled)                 |
| `writer_batch_timeout_ms`             | `int64_t`     | `100`                | Maximum time in ms to wait for a writer batch to fill up before sending                  |
| `writer_bucket_no_key_assigner`       | `std::string` | `"sticky"`           | Bucket assignment strategy for tables without bucket keys: `"sticky"` or `"round_robin"` |
| `scanner_remote_log_prefetch_num`     | `size_t`      | `4`                  | Number of remote log segments to prefetch                                                |
| `remote_file_download_thread_num`     | `size_t`      | `3`                  | Number of threads for remote log downloads                                               |
| `scanner_remote_log_read_concurrency` | `size_t`      | `4`                  | Streaming read concurrency within a remote log file                                      |
| `scanner_log_max_poll_records`        | `size_t`      | `500`                | Maximum number of records returned in a single Poll()                                    |
| `scanner_log_fetch_max_bytes`         | `int32_t`     | `16777216` (16 MB)   | Maximum bytes per fetch response for LogScanner                                          |
| `scanner_log_fetch_min_bytes`         | `int32_t`     | `1`                  | Minimum bytes the server must accumulate before returning a fetch response               |
| `scanner_log_fetch_wait_max_time_ms`  | `int32_t`     | `500`                | Maximum time (ms) the server may wait to satisfy min-bytes                               |
| `scanner_log_fetch_max_bytes_for_bucket`| `int32_t`   | `1048576` (1 MB)     | Maximum bytes per fetch response per bucket for LogScanner                               |
| `connect_timeout_ms`                  | `uint64_t`    | `120000`             | TCP connect timeout in milliseconds                                                      |
| `security_protocol`                   | `std::string` | `"PLAINTEXT"`        | `"PLAINTEXT"` (default) or `"sasl"` for SASL auth                                        |
| `security_sasl_mechanism`             | `std::string` | `"PLAIN"`            | SASL mechanism (only `"PLAIN"` is supported)                                             |
| `security_sasl_username`              | `std::string` | (empty)              | SASL username (required when protocol is `"sasl"`)                                       |
| `security_sasl_password`              | `std::string` | (empty)              | SASL password (required when protocol is `"sasl"`)                                       |

## `Connection`

| Method                                                                  | Description                                       |
|-------------------------------------------------------------------------|---------------------------------------------------|
| `static Create(const Configuration& config, Connection& out) -> Result` | Create a connection to a Fluss cluster            |
| `GetAdmin(Admin& out) -> Result`                                        | Get the admin interface                           |
| `GetTable(const TablePath& table_path, Table& out) -> Result`           | Get a table for read/write operations             |
| `Available() -> bool`                                                   | Check if the connection is valid and initialized  |

## `Admin`

### Database Operations

| Method                                                                                                                    | Description              |
|---------------------------------------------------------------------------------------------------------------------------|--------------------------|
| `CreateDatabase(const std::string& database_name, const DatabaseDescriptor& descriptor, bool ignore_if_exists) -> Result` | Create a database        |
| `DropDatabase(const std::string& name, bool ignore_if_not_exists, bool cascade) -> Result`                                | Drop a database          |
| `ListDatabases(std::vector<std::string>& out) -> Result`                                                                  | List all databases       |
| `DatabaseExists(const std::string& name, bool& out) -> Result`                                                            | Check if a database exists |
| `GetDatabaseInfo(const std::string& name, DatabaseInfo& out) -> Result`                                                   | Get database metadata    |

### Table Operations

| Method                                                                                                     | Description                 |
|------------------------------------------------------------------------------------------------------------|-----------------------------|
| `CreateTable(const TablePath& path, const TableDescriptor& descriptor, bool ignore_if_exists) -> Result`   | Create a table              |
| `DropTable(const TablePath& path, bool ignore_if_not_exists) -> Result`                                    | Drop a table                |
| `GetTableInfo(const TablePath& path, TableInfo& out) -> Result`                                            | Get table metadata          |
| `ListTables(const std::string& database_name, std::vector<std::string>& out) -> Result`                    | List tables in a database   |
| `TableExists(const TablePath& path, bool& out) -> Result`                                                  | Check if a table exists     |

### Partition Operations

| Method                                                                                                                                          | Description              |
|-------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|
| `CreatePartition(const TablePath& path, const std::unordered_map<std::string, std::string>& partition_spec, bool ignore_if_exists) -> Result`   | Create a partition       |
| `DropPartition(const TablePath& path, const std::unordered_map<std::string, std::string>& partition_spec, bool ignore_if_not_exists) -> Result` | Drop a partition         |
| `ListPartitionInfos(const TablePath& path, std::vector<PartitionInfo>& out) -> Result`                                                          | List partition metadata  |

### Offset Operations

| Method                                                                                                                                                                                                  | Description                             |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------|
| `ListOffsets(const TablePath& path, const std::vector<int32_t>& bucket_ids, const OffsetSpec& query, std::unordered_map<int32_t, int64_t>& out) -> Result`                                             | Get offsets for buckets                 |
| `ListPartitionOffsets(const TablePath& path, const std::string& partition_name, const std::vector<int32_t>& bucket_ids, const OffsetSpec& query, std::unordered_map<int32_t, int64_t>& out) -> Result` | Get offsets for a partition's buckets   |

### Lake Operations

| Method                                                                      | Description                  |
|-----------------------------------------------------------------------------|------------------------------|
| `GetLatestLakeSnapshot(const TablePath& path, LakeSnapshot& out) -> Result` | Get the latest lake snapshot |

### Cluster Operations

| Method                                                    | Description                                        |
|-----------------------------------------------------------|----------------------------------------------------|
| `GetServerNodes(std::vector<ServerNode>& out) -> Result`  | Get all alive server nodes (coordinator + tablets) |

## `ServerNode`

| Field         | Type          | Description                                              |
|---------------|---------------|----------------------------------------------------------|
| `id`          | `int32_t`     | Server node ID                                           |
| `host`        | `std::string` | Hostname of the server                                   |
| `port`        | `uint32_t`    | Port number                                              |
| `server_type` | `std::string` | Server type (`"CoordinatorServer"`, `"TabletServer"`, or `"Unknown"`)  |
| `uid`         | `std::string` | Unique identifier (e.g. `"cs-0"`, `"ts-1"`)             |

`"Unknown"` is used when the client has not yet determined the endpoint type, such as bootstrap endpoints.

## `Table`

| Method                        | Description                              |
|-------------------------------|------------------------------------------|
| `NewRow() -> GenericRow`      | Create a schema-aware row for this table |
| `NewAppend() -> TableAppend`  | Create an append builder for log tables  |
| `NewUpsert() -> TableUpsert`  | Create an upsert builder for PK tables   |
| `NewLookup() -> TableLookup`  | Create a lookup builder for PK tables    |
| `NewPrefixLookup(std::vector<std::string> cols, PrefixLookuper& out) -> Result` | Create a prefix (bucket-key) lookuper |
| `NewScan() -> TableScan`      | Create a scan builder                    |
| `GetTableInfo() -> TableInfo` | Get table metadata                       |
| `GetTablePath() -> TablePath` | Get the table path                       |
| `HasPrimaryKey() -> bool`     | Check if the table has a primary key     |

## `TableAppend`

| Method                                       | Description             |
|----------------------------------------------|-------------------------|
| `CreateWriter(AppendWriter& out) -> Result`  | Create an append writer |

## `TableUpsert`

| Method                                                                       | Description                                |
|------------------------------------------------------------------------------|--------------------------------------------|
| `PartialUpdateByIndex(std::vector<size_t> column_indices) -> TableUpsert&`   | Configure partial update by column indices |
| `PartialUpdateByName(std::vector<std::string> column_names) -> TableUpsert&` | Configure partial update by column names   |
| `CreateWriter(UpsertWriter& out) -> Result`                                  | Create an upsert writer                    |

## `TableLookup`

| Method                                    | Description                         |
|-------------------------------------------|-------------------------------------|
| `CreateLookuper(Lookuper& out) -> Result` | Create a lookuper for point lookups |

## `TableScan`

| Method                                                               | Description                                   |
|----------------------------------------------------------------------|-----------------------------------------------|
| `ProjectByIndex(std::vector<size_t> column_indices) -> TableScan&`   | Project columns by index                      |
| `ProjectByName(std::vector<std::string> column_names) -> TableScan&` | Project columns by name                       |
| `Limit(int32_t row_number) -> TableScan&`                            | Set a positive row limit (enables `CreateBucketBatchScanner`; rejected by log scanners) |
| `CreateLogScanner(LogScanner& out) -> Result`                        | Create a record-based log scanner; on a primary-key table, subscribes to its CDC changelog (per-record `change_type`) |
| `CreateRecordBatchLogScanner(LogScanner& out) -> Result`             | Create an Arrow RecordBatch-based log scanner (log tables only — no per-record change types) |
| `CreateBucketBatchScanner(const TableBucket& bucket, BatchScanner& out) -> Result` | Bounded scan of one bucket (requires `Limit`) |

## `AppendWriter`

| Method                                                      | Description                            |
|-------------------------------------------------------------|----------------------------------------|
| `Append(const GenericRow& row) -> Result`                   | Append a row (fire-and-forget)         |
| `Append(const GenericRow& row, WriteResult& out) -> Result` | Append a row with write acknowledgment |
| `Flush() -> Result`                                         | Flush all pending writes               |

## `UpsertWriter`

| Method                                                      | Description                                   |
|-------------------------------------------------------------|-----------------------------------------------|
| `Upsert(const GenericRow& row) -> Result`                   | Upsert a row (fire-and-forget)                |
| `Upsert(const GenericRow& row, WriteResult& out) -> Result` | Upsert a row with write acknowledgment        |
| `Delete(const GenericRow& row) -> Result`                   | Delete a row by primary key (fire-and-forget) |
| `Delete(const GenericRow& row, WriteResult& out) -> Result` | Delete a row with write acknowledgment        |
| `Flush() -> Result`                                         | Flush all pending operations                  |

## `WriteResult`

| Method             | Description                                 |
|--------------------|---------------------------------------------|
| `Wait() -> Result` | Wait for server acknowledgment of the write |

## `Lookuper`

| Method                                                        |  Description                |
|---------------------------------------------------------------|-----------------------------|
| `Lookup(const GenericRow& pk_row, LookupResult& out) -> Result` | Lookup a row by primary key |

## `PrefixLookuper`

Performs prefix (bucket-key) lookups, returning all rows whose primary key starts with the given prefix. Obtained from `Table::NewPrefixLookup()`. See the [Prefix Lookup example](./example/prefix-lookup.md).

| Method                                                                          |  Description                                  |
|---------------------------------------------------------------------------------|-----------------------------------------------|
| `PrefixLookup(const GenericRow& prefix_row, PrefixLookupResult& out) -> Result` | Look up all rows matching the prefix columns  |

## `LogScanner`

| Method                                                                                               |  Description                              |
|------------------------------------------------------------------------------------------------------|-------------------------------------------|
| `Subscribe(int32_t bucket_id, int64_t offset) -> Result`                                             | Subscribe to a single bucket at an offset |
| `Subscribe(const std::vector<BucketSubscription>& bucket_offsets) -> Result`                         | Subscribe to multiple buckets             |
| `SubscribePartitionBuckets(int64_t partition_id, int32_t bucket_id, int64_t start_offset) -> Result` | Subscribe to a single partition bucket    |
| `SubscribePartitionBuckets(const std::vector<PartitionBucketSubscription>& subscriptions) -> Result` | Subscribe to multiple partition buckets   |
| `Unsubscribe(int32_t bucket_id) -> Result`                                                           | Unsubscribe from a non-partitioned bucket |
| `UnsubscribePartition(int64_t partition_id, int32_t bucket_id) -> Result`                            | Unsubscribe from a partition bucket       |
| `Poll(int64_t timeout_ms, ScanRecords& out) -> Result`                                               | Poll individual records                   |
| `PollRecordBatch(int64_t timeout_ms, ArrowRecordBatches& out) -> Result`                             | Poll Arrow RecordBatches                  |

## `BatchScanner`

One-shot bounded scan of a single bucket. Obtained from `TableScan::CreateBucketBatchScanner()`. The scan runs on the first call below, yields its single batch once, then is spent.

| Method                                                       |  Description                                        |
|--------------------------------------------------------------|-----------------------------------------------------|
| `NextBatch(ArrowRecordBatches& out) -> Result`               | Run the scan; returns the batch once, then empty    |
| `CollectAllBatches(ArrowRecordBatches& out) -> Result`       | Drain the scanner into all of its batches           |

## `GenericRow`

`GenericRow` is a **write-only** row used for append, upsert, delete, and lookup key construction. For reading field values from scan or lookup results, see [`RowView`](#rowview) and [`LookupResult`](#lookupresult).

### Index-Based Setters

| Method                                                    |  Description                   |
|-----------------------------------------------------------|--------------------------------|
| `SetNull(size_t idx)`                                     | Set field to null              |
| `SetBool(size_t idx, bool value)`                         | Set boolean value              |
| `SetInt32(size_t idx, int32_t value)`                     | Set 32-bit integer             |
| `SetInt64(size_t idx, int64_t value)`                     | Set 64-bit integer             |
| `SetFloat32(size_t idx, float value)`                     | Set 32-bit float               |
| `SetFloat64(size_t idx, double value)`                    | Set 64-bit float               |
| `SetString(size_t idx, const std::string& value)`         | Set string value               |
| `SetBytes(size_t idx, const std::vector<uint8_t>& value)` | Set binary data                |
| `SetDate(size_t idx, const Date& value)`                  | Set date value                 |
| `SetTime(size_t idx, const Time& value)`                  | Set time value                 |
| `SetTimestampNtz(size_t idx, const Timestamp& value)`     | Set timestamp without timezone |
| `SetTimestampLtz(size_t idx, const Timestamp& value)`     | Set timestamp with timezone    |
| `SetDecimal(size_t idx, const std::string& value)`        | Set decimal from string        |
| `SetArray(size_t idx, ArrayWriter&& writer)`              | Set array value (consumes the writer) |
| `SetMap(size_t idx, MapWriter&& writer)`                  | Set map value (consumes the writer)   |
| `SetRow(size_t idx, GenericRow&& nested)`                 | Set ROW value from a nested row (consumes it) |

### Name-Based Setters

When using `table.NewRow()`, the `Set()` method auto-routes to the correct type based on the schema:

| Method                                                   | Description                       |
|----------------------------------------------------------|-----------------------------------|
| `Set(const std::string& name, std::nullptr_t)`           | Set field to null by column name  |
| `Set(const std::string& name, bool value)`               | Set boolean by column name        |
| `Set(const std::string& name, int32_t value)`            | Set integer by column name        |
| `Set(const std::string& name, int64_t value)`            | Set big integer by column name    |
| `Set(const std::string& name, float value)`              | Set float by column name          |
| `Set(const std::string& name, double value)`             | Set double by column name         |
| `Set(const std::string& name, const std::string& value)` | Set string/decimal by column name |
| `Set(const std::string& name, const Date& value)`        | Set date by column name           |
| `Set(const std::string& name, const Time& value)`        | Set time by column name           |
| `Set(const std::string& name, const Timestamp& value)`   | Set timestamp by column name      |
| `Set(const std::string& name, ArrayWriter&& writer)`     | Set array value by column name    |
| `Set(const std::string& name, MapWriter&& writer)`       | Set map value by column name      |
| `Set(const std::string& name, GenericRow&& nested)`      | Set ROW value by column name      |

## `RowView`

Read-only row view for scan results. Provides zero-copy access to string and bytes data. `RowView` shares ownership of the underlying scan data via reference counting, so it can safely outlive the `ScanRecords` that produced it.

:::note string_view Lifetime
`GetString()` returns `std::string_view` that borrows from the underlying data. The `string_view` is valid as long as any `RowView` (or `ScanRecord`) referencing the same poll result is alive. Copy to `std::string` if you need the value after all references are gone.
:::

### Index-Based Getters

| Method                                                     |  Description                   |
|------------------------------------------------------------|--------------------------------|
| `FieldCount() -> size_t`                                   | Get the number of fields       |
| `GetType(size_t idx) -> TypeId`                            | Get the type at index          |
| `IsNull(size_t idx) -> bool`                               | Check if field is null         |
| `GetBool(size_t idx) -> bool`                              | Get boolean value at index     |
| `GetInt32(size_t idx) -> int32_t`                          | Get 32-bit integer at index    |
| `GetInt64(size_t idx) -> int64_t`                          | Get 64-bit integer at index    |
| `GetFloat32(size_t idx) -> float`                          | Get 32-bit float at index      |
| `GetFloat64(size_t idx) -> double`                         | Get 64-bit float at index      |
| `GetString(size_t idx) -> std::string_view`                | Get string at index (zero-copy)|
| `GetBytes(size_t idx) -> std::pair<const uint8_t*, size_t>`| Get binary data at index (zero-copy)|
| `GetDate(size_t idx) -> Date`                              | Get date at index              |
| `GetTime(size_t idx) -> Time`                              | Get time at index              |
| `GetTimestamp(size_t idx) -> Timestamp`                    | Get timestamp at index         |
| `IsDecimal(size_t idx) -> bool`                            | Check if field is a decimal type|
| `GetDecimalString(size_t idx) -> std::string`              | Get decimal as string at index |

### Complex Getters (ARRAY / MAP / ROW)

| Method                                       |  Description                                                 |
|----------------------------------------------|--------------------------------------------------------------|
| `GetValue(size_t idx) -> Value`              | Get a complex column as a recursive [`Value`](#value) handle |
| `GetValue(const std::string& name) -> Value` | Same, by column name                                         |

Navigate the returned [`Value`](#value) with `At` / `KeyAt` / `ValueAt` / `Field` and read leaves with its `Get*()` methods.

### Name-Based Getters

| Method                                                  |  Description                       |
|---------------------------------------------------------|------------------------------------|
| `IsNull(const std::string& name) -> bool`               | Check if field is null by name     |
| `GetBool(const std::string& name) -> bool`              | Get boolean by column name         |
| `GetInt32(const std::string& name) -> int32_t`          | Get 32-bit integer by column name  |
| `GetInt64(const std::string& name) -> int64_t`          | Get 64-bit integer by column name  |
| `GetFloat32(const std::string& name) -> float`          | Get 32-bit float by column name    |
| `GetFloat64(const std::string& name) -> double`         | Get 64-bit float by column name    |
| `GetString(const std::string& name) -> std::string_view`| Get string by column name          |
| `GetBytes(const std::string& name) -> std::pair<const uint8_t*, size_t>` | Get binary data by column name |
| `GetDate(const std::string& name) -> Date`              | Get date by column name            |
| `GetTime(const std::string& name) -> Time`              | Get time by column name            |
| `GetTimestamp(const std::string& name) -> Timestamp`    | Get timestamp by column name       |
| `GetDecimalString(const std::string& name) -> std::string` | Get decimal as string by column name |

## `ScanRecord`

`ScanRecord` is a value type that can be freely copied, stored, and accumulated across multiple `Poll()` calls. It shares ownership of the underlying scan data via reference counting.

| Field         | Type         |  Description                                                        |
|---------------|--------------|---------------------------------------------------------------------|
| `offset`      | `int64_t`    | Record offset in the log                                            |
| `timestamp`   | `int64_t`    | Record timestamp                                                    |
| `change_type` | `ChangeType` | Change type (AppendOnly, Insert, UpdateBefore, UpdateAfter, Delete) |
| `row`         | `RowView`    | Row data (value type, shares ownership via reference counting)      |

## `ScanRecords`

### Flat Access

| Method                                  |  Description                               |
|-----------------------------------------|--------------------------------------------|
| `Count() -> size_t`                     | Total number of records across all buckets |
| `IsEmpty() -> bool`                     | Check if empty                             |
| `begin() / end()`                       | Iterator support for range-based for loops |

Flat iteration over all records (regardless of bucket):

```cpp
for (const auto& rec : records) {
    std::cout << "offset=" << rec.offset << std::endl;
}
```

### Per-Bucket Access

| Method                                                          |  Description                                                          |
|-----------------------------------------------------------------|-----------------------------------------------------------------------|
| `BucketCount() -> size_t`                                       | Number of distinct buckets                                            |
| `Buckets() -> std::vector<TableBucket>`                         | List of distinct buckets                                              |
| `Records(const TableBucket& bucket) -> BucketRecords`              | Records for a specific bucket (empty if bucket not present)           |
| `BucketAt(size_t idx) -> BucketRecords`                            | Records by bucket index (0-based, O(1))                               |

## `BucketRecords`

A bundle of scan records belonging to a single bucket. Obtained from `ScanRecords::Records()` or `ScanRecords::BucketAt()`. `BucketRecords` is a value type — it shares ownership of the underlying scan data via reference counting, so it can safely outlive the `ScanRecords` that produced it.

| Method                                         |  Description                               |
|------------------------------------------------|--------------------------------------------|
| `Size() -> size_t`                         | Number of records in this bucket           |
| `Empty() -> bool`                          | Check if empty                             |
| `Bucket() -> const TableBucket&`           | Get the bucket                             |
| `operator[](size_t idx) -> ScanRecord`     | Access record by index within this bucket  |
| `begin() / end()`                          | Iterator support for range-based for loops |

## `TableBucket`

| Field / Method                        |  Description                                    |
|---------------------------------------|-------------------------------------------------|
| `table_id -> int64_t`                    | Table ID                                        |
| `bucket_id -> int32_t`                   | Bucket ID                                       |
| `partition_id -> std::optional<int64_t>` | Partition ID (empty if non-partitioned)         |
| `operator==(const TableBucket&) -> bool` | Equality comparison                             |

## `LookupResult`

Read-only result for lookup operations. Provides zero-copy access to field values.

### Metadata

| Method                      |  Description                   |
|-----------------------------|--------------------------------|
| `Found() -> bool`           | Whether a matching row was found |
| `FieldCount() -> size_t`    | Get the number of fields       |

### Index-Based Getters

| Method                                                     |  Description                   |
|------------------------------------------------------------|--------------------------------|
| `GetType(size_t idx) -> TypeId`                            | Get the type at index          |
| `IsNull(size_t idx) -> bool`                               | Check if field is null         |
| `GetBool(size_t idx) -> bool`                              | Get boolean value at index     |
| `GetInt32(size_t idx) -> int32_t`                          | Get 32-bit integer at index    |
| `GetInt64(size_t idx) -> int64_t`                          | Get 64-bit integer at index    |
| `GetFloat32(size_t idx) -> float`                          | Get 32-bit float at index      |
| `GetFloat64(size_t idx) -> double`                         | Get 64-bit float at index      |
| `GetString(size_t idx) -> std::string_view`                | Get string at index (zero-copy)|
| `GetBytes(size_t idx) -> std::pair<const uint8_t*, size_t>`| Get binary data at index (zero-copy)|
| `GetDate(size_t idx) -> Date`                              | Get date at index              |
| `GetTime(size_t idx) -> Time`                              | Get time at index              |
| `GetTimestamp(size_t idx) -> Timestamp`                    | Get timestamp at index         |
| `IsDecimal(size_t idx) -> bool`                            | Check if field is a decimal type|
| `GetDecimalString(size_t idx) -> std::string`              | Get decimal as string at index |

### Complex Getters (ARRAY / MAP / ROW)

`GetValue(size_t idx) -> Value` and `GetValue(const std::string& name) -> Value`, same as on [`RowView`](#rowview). Navigate the returned [`Value`](#value).

### Name-Based Getters

| Method                                                  |  Description                       |
|---------------------------------------------------------|------------------------------------|
| `IsNull(const std::string& name) -> bool`               | Check if field is null by name     |
| `GetBool(const std::string& name) -> bool`              | Get boolean by column name         |
| `GetInt32(const std::string& name) -> int32_t`          | Get 32-bit integer by column name  |
| `GetInt64(const std::string& name) -> int64_t`          | Get 64-bit integer by column name  |
| `GetFloat32(const std::string& name) -> float`          | Get 32-bit float by column name    |
| `GetFloat64(const std::string& name) -> double`         | Get 64-bit float by column name    |
| `GetString(const std::string& name) -> std::string_view`| Get string by column name          |
| `GetBytes(const std::string& name) -> std::pair<const uint8_t*, size_t>` | Get binary data by column name |
| `GetDate(const std::string& name) -> Date`              | Get date by column name            |
| `GetTime(const std::string& name) -> Time`              | Get time by column name            |
| `GetTimestamp(const std::string& name) -> Timestamp`    | Get timestamp by column name       |
| `GetDecimalString(const std::string& name) -> std::string` | Get decimal as string by column name |

## `PrefixLookupResult`

Read-only result of a prefix lookup — zero or more matched rows. Each row is a `PrefixRowView` exposing the same scalar getters as [`RowView`](#rowview) (index- and name-based) plus `GetValue(...)` for complex (ARRAY / MAP / ROW) columns.

| Method                                  | Description                                       |
|-----------------------------------------|---------------------------------------------------|
| `Size() -> size_t`                      | Number of matched rows                            |
| `IsEmpty() -> bool`                     | Whether the result has no rows                    |
| `GetRow(size_t index) -> PrefixRowView` | Get the row at `index` (throws if out of range)   |

## `ArrowRecordBatch`

| Method                                                         | Description                          |
|----------------------------------------------------------------|--------------------------------------|
| `GetArrowRecordBatch() -> std::shared_ptr<arrow::RecordBatch>` | Get the underlying Arrow RecordBatch |
| `Available() -> bool`                                          | Check if the batch is valid          |
| `NumRows() -> int64_t`                                         | Number of rows in the batch          |
| `GetTableId() -> int64_t`                                      | Table ID                             |
| `GetPartitionId() -> int64_t`                                  | Partition ID                         |
| `GetBucketId() -> int32_t`                                     | Bucket ID                            |
| `GetBaseOffset() -> int64_t`                                   | First record offset                  |
| `GetLastOffset() -> int64_t`                                   | Last record offset                   |

## `ArrowRecordBatches`

| Method                   |  Description                               |
|--------------------------|--------------------------------------------|
| `Size() -> size_t`       | Number of batches                          |
| `Empty() -> bool`        | Check if empty                             |
| `operator[](size_t idx)` | Access batch by index                      |
| `begin() / end()`        | Iterator support for range-based for loops |

## `Schema`

| Method                            |  Description                |
|-----------------------------------|-----------------------------|
| `NewBuilder() -> Schema::Builder` | Create a new schema builder |
| `FromArrow(std::shared_ptr<arrow::Schema> schema, std::vector<std::string> primary_keys = {}) -> Schema` | Build a schema from an existing Arrow schema (escape hatch; prefer `DataType::Map` / `DataType::Row`) |

## `Schema::Builder`

| Method                                                                 |  Description            |
|------------------------------------------------------------------------|-------------------------|
| `AddColumn(const std::string& name, const DataType& type) -> Builder&` | Add a column            |
| `SetPrimaryKeys(const std::vector<std::string>& keys) -> Builder&`     | Set primary key columns |
| `Build() -> Schema`                                                    | Build the schema        |

## `TableDescriptor`

| Method                                     |  Description                          |
|--------------------------------------------|---------------------------------------|
| `NewBuilder() -> TableDescriptor::Builder` | Create a new table descriptor builder |

## `TableDescriptor::Builder`

| Method                                                                            | Description                |
|-----------------------------------------------------------------------------------|----------------------------|
| `SetSchema(const Schema& schema) -> Builder&`                                     | Set the table schema       |
| `SetPartitionKeys(const std::vector<std::string>& keys) -> Builder&`              | Set partition key columns  |
| `SetBucketCount(int32_t count) -> Builder&`                                       | Set the number of buckets  |
| `SetBucketKeys(const std::vector<std::string>& keys) -> Builder&`                 | Set bucket key columns     |
| `SetProperty(const std::string& key, const std::string& value) -> Builder&`       | Set a table property       |
| `SetCustomProperty(const std::string& key, const std::string& value) -> Builder&` | Set a custom property      |
| `SetComment(const std::string& comment) -> Builder&`                              | Set a table comment        |
| `Build() -> TableDescriptor`                                                      | Build the table descriptor |

## `DataType`

### Factory Methods

| Method                                        |  Description                       |
|-----------------------------------------------|------------------------------------|
| `DataType::Boolean()`                         | Boolean type                       |
| `DataType::TinyInt()`                         | 8-bit signed integer               |
| `DataType::SmallInt()`                        | 16-bit signed integer              |
| `DataType::Int()`                             | 32-bit signed integer              |
| `DataType::BigInt()`                          | 64-bit signed integer              |
| `DataType::Float()`                           | 32-bit floating point              |
| `DataType::Double()`                          | 64-bit floating point              |
| `DataType::String()`                          | UTF-8 string                       |
| `DataType::Bytes()`                           | Binary data                        |
| `DataType::Date()`                            | Date (days since epoch)            |
| `DataType::Time()`                            | Time (milliseconds since midnight) |
| `DataType::Timestamp(int precision)`          | Timestamp without timezone         |
| `DataType::TimestampLtz(int precision)`       | Timestamp with timezone            |
| `DataType::Decimal(int precision, int scale)` | Decimal with precision and scale   |
| `DataType::Array(DataType element)`           | Array of the given element type    |
| `DataType::Map(DataType key, DataType value)` | Map of key/value types (either may be complex) |
| `DataType::Row(std::vector<std::pair<std::string, DataType>> fields)` | Row of named fields (types may be complex) |

`MAP` / `ROW` (and arrays nesting them) compose to any depth; the binding lowers them to Arrow internally when the table is created or a writer is built.

### Accessors

| Method                              |  Description                                |
|-------------------------------------|---------------------------------------------|
| `id() -> TypeId`                    | Get the type ID                             |
| `precision() -> int`               | Get precision (for Decimal/Timestamp types) |
| `scale() -> int`                   | Get scale (for Decimal type)                |
| `nullable() -> bool`               | Returns `true` if this type is nullable (default), `false` if `NOT NULL` |
| `element_type() -> const DataType*` | Get element type (for Array type, nullptr otherwise) |
| `key_type() / value_type() -> const DataType*` | MAP key / value types (nullptr otherwise) |
| `field_count() -> size_t`          | ROW field count (0 otherwise)               |
| `field_name(size_t i) -> const std::string&` | ROW field name at `i`             |
| `field_type(size_t i) -> const DataType*` | ROW field type at `i`                  |
| `NotNull() -> DataType`            | Returns a copy of this type with nullable set to `false` |

## `ArrayWriter`

Write-only builder for array column values. Constructed with a fixed size and element type, then populated element-by-element. Move-only — consumed by `GenericRow::SetArray()` or `ArrayWriter::SetArray()` for nested arrays. For arrays whose element is a `ROW` / `MAP`, construct from an Arrow element type (`arrow::struct_(...)` / `arrow::map(...)`).

| Method                                                    |  Description                              |
|-----------------------------------------------------------|-------------------------------------------|
| `ArrayWriter(size_t size, DataType element_type)`         | Create an array writer; element may be complex (`DataType::Map`/`Row`) |
| `ArrayWriter(size_t size, std::shared_ptr<arrow::DataType> element_type)` | Arrow escape hatch for the element type |
| `SetNull(size_t idx)`                                     | Set element to null                       |
| `SetBool(size_t idx, bool value)`                         | Set boolean element                       |
| `SetInt32(size_t idx, int32_t value)`                     | Set 32-bit integer element                |
| `SetInt64(size_t idx, int64_t value)`                     | Set 64-bit integer element                |
| `SetFloat32(size_t idx, float value)`                     | Set 32-bit float element                  |
| `SetFloat64(size_t idx, double value)`                    | Set 64-bit float element                  |
| `SetString(size_t idx, const std::string& value)`         | Set string element                        |
| `SetBytes(size_t idx, const std::vector<uint8_t>& value)` | Set binary element                        |
| `SetDate(size_t idx, const Date& value)`                  | Set date element                          |
| `SetTime(size_t idx, const Time& value)`                  | Set time element                          |
| `SetTimestampNtz(size_t idx, const Timestamp& value)`     | Set timestamp without timezone element    |
| `SetTimestampLtz(size_t idx, const Timestamp& value)`     | Set timestamp with timezone element       |
| `SetDecimal(size_t idx, const std::string& value)`        | Set decimal element from string           |
| `SetArray(size_t idx, ArrayWriter&& nested)`              | Set nested array element (consumes nested)|
| `SetRow(size_t idx, GenericRow&& row)`                    | Set ROW element (consumes the row)        |
| `SetMap(size_t idx, MapWriter&& map)`                     | Set MAP element (consumes the map)        |

## `MapWriter`

Write-only builder for map column values. Construct with the key/value types, then for each entry set the key and value and call `Commit()`. Keys cannot be null; an unset value defaults to null. Move-only — consumed by `GenericRow::SetMap()`, `ArrayWriter::SetMap()`, and `MapWriter::SetValueMap()`. For a key/value that is itself a `ROW` / `MAP` / `ARRAY`, construct from Arrow types and use the `SetValue{Row,Map,Array}` setters.

| Method                                                                                                              | Description                                                       |
|---------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| `MapWriter(size_t capacity, DataType key_type, DataType value_type)`                                                | Create a map writer; key/value may be complex (`DataType::Map`/`Row`/`Array`) |
| `MapWriter(size_t capacity, std::shared_ptr<arrow::DataType> key_type, std::shared_ptr<arrow::DataType> value_type)`| Arrow escape hatch for key/value types                            |
| `SetKey{Bool,Int32,Int64,Float32,Float64,String,Bytes,Date,Time,Timestamp,Decimal}(...)`                            | Stage the entry key (NTZ vs LTZ chosen from the declared key type)|
| `SetValueNull()`                                                                                                    | Stage a null entry value                                          |
| `SetValue{Bool,Int32,Int64,Float32,Float64,String,Bytes,Date,Time,Timestamp,Decimal}(...)`                          | Stage the entry value (NTZ vs LTZ from the declared value type)   |
| `SetValueRow(GenericRow&&)` / `SetValueMap(MapWriter&&)` / `SetValueArray(ArrayWriter&&)`                            | Stage a compound entry value (consumes the writer)                |
| `Commit()`                                                                                                          | Append the staged key/value as one map entry                      |

## `Value`

Read-only handle for a complex (`ARRAY` / `MAP` / `ROW`) column value, obtained from `RowView::GetValue()`, `LookupResult::GetValue()`, or `PrefixRowView::GetValue()`. **Navigate** to children with `At` / `KeyAt` / `ValueAt` / `Field` (each returns a child `Value`); **read** a leaf with the `Get*()` methods. A typed read on a null or wrong-typed value throws. Move-only; owns a Rust handle released on destruction.

### Leaf reads

| Method                              |  Description                  |
|-------------------------------------|-------------------------------|
| `Type() -> TypeId`                  | Type of this value            |
| `IsNull() -> bool`                  | Whether this value is null    |
| `GetBool() -> bool`                 | Read a boolean leaf           |
| `GetInt32() -> int32_t`             | Read a 32-bit integer leaf    |
| `GetInt64() -> int64_t`             | Read a 64-bit integer leaf    |
| `GetFloat32() -> float`             | Read a 32-bit float leaf      |
| `GetFloat64() -> double`            | Read a 64-bit float leaf      |
| `GetString() -> std::string`        | Read a string leaf            |
| `GetBytes() -> std::vector<uint8_t>`| Read a binary leaf            |
| `GetDate() -> Date`                 | Read a date leaf              |
| `GetTime() -> Time`                 | Read a time leaf              |
| `GetTimestamp() -> Timestamp`       | Read a timestamp leaf         |
| `GetDecimalString() -> std::string` | Read a decimal leaf as string |

### Navigation

| Method                                       |  Description                       |
|----------------------------------------------|------------------------------------|
| `Size() -> size_t`                           | Element count (ARRAY) / entries (MAP) |
| `FieldCount() -> size_t`                     | Field count (ROW)                  |
| `At(size_t i) -> Value`                      | ARRAY element `i`                  |
| `KeyAt(size_t i) -> Value`                   | MAP key at entry `i`               |
| `ValueAt(size_t i) -> Value`                 | MAP value at entry `i`             |
| `Field(size_t i) -> Value`                   | ROW field `i`                      |
| `Field(const std::string& name) -> Value`    | ROW field by name                  |

## `TablePath`

| Method / Field                                                     |  Description          |
|--------------------------------------------------------------------|-----------------------|
| `TablePath(const std::string& database, const std::string& table)` | Create a table path   |
| `database_name -> std::string`                                     | Database name         |
| `table_name -> std::string`                                        | Table name            |
| `ToString() -> std::string`                                        | String representation |

## `TableInfo`

| Field               | Type                                           | Description                         |
|---------------------|------------------------------------------------|-------------------------------------|
| `table_id`          | `int64_t`                                      | Table ID                            |
| `schema_id`         | `int32_t`                                      | Schema ID                           |
| `table_path`        | `TablePath`                                    | Table path                          |
| `created_time`      | `int64_t`                                      | Creation timestamp                  |
| `modified_time`     | `int64_t`                                      | Last modification timestamp         |
| `primary_keys`      | `std::vector<std::string>`                     | Primary key columns                 |
| `bucket_keys`       | `std::vector<std::string>`                     | Bucket key columns                  |
| `partition_keys`    | `std::vector<std::string>`                     | Partition key columns               |
| `num_buckets`       | `int32_t`                                      | Number of buckets                   |
| `has_primary_key`   | `bool`                                         | Whether the table has a primary key |
| `is_partitioned`    | `bool`                                         | Whether the table is partitioned    |
| `properties`        | `std::unordered_map<std::string, std::string>` | Table properties                    |
| `custom_properties` | `std::unordered_map<std::string, std::string>` | Custom properties                   |
| `comment`           | `std::string`                                  | Table comment                       |
| `schema`            | `Schema`                                       | Table schema                        |

## Temporal Types

### `Date`

| Method                                        |  Description                 |
|-----------------------------------------------|------------------------------|
| `Date::FromDays(int32_t days)`                | Create from days since epoch |
| `Date::FromYMD(int year, int month, int day)` | Create from year, month, day |
| `Year() -> int`                               | Get year                     |
| `Month() -> int`                              | Get month                    |
| `Day() -> int`                                | Get day                      |

### `Time`

| Method                                            |  Description                                 |
|---------------------------------------------------|----------------------------------------------|
| `Time::FromMillis(int32_t millis)`                | Create from milliseconds since midnight      |
| `Time::FromHMS(int hour, int minute, int second)` | Create from hour, minute, second             |
| `Hour() -> int`                                   | Get hour                                     |
| `Minute() -> int`                                 | Get minute                                   |
| `Second() -> int`                                 | Get second                                   |
| `Millis() -> int64_t`                             | Get sub-second millisecond component (0-999) |

### `Timestamp`

| Method                                                               |  Description                             |
|----------------------------------------------------------------------|------------------------------------------|
| `Timestamp::FromMillis(int64_t millis)`                              | Create from milliseconds since epoch     |
| `Timestamp::FromMillisNanos(int64_t millis, int32_t nanos)`          | Create from milliseconds and nanoseconds |
| `Timestamp::FromTimePoint(std::chrono::system_clock::time_point tp)` | Create from a time point                 |

## `PartitionInfo`

| Field            | Type          |  Description   |
|------------------|---------------|----------------|
| `partition_id`   | `int64_t`     | Partition ID   |
| `partition_name` | `std::string` | Partition name |

## `DatabaseDescriptor`

| Field        | Type                                           | Description       |
|--------------|------------------------------------------------|-------------------|
| `comment`    | `std::string`                                  | Database comment  |
| `properties` | `std::unordered_map<std::string, std::string>` | Custom properties |

## `DatabaseInfo`

| Field           | Type                                           |  Description                |
|-----------------|------------------------------------------------|-----------------------------|
| `database_name` | `std::string`                                  | Database name               |
| `comment`       | `std::string`                                  | Database comment            |
| `properties`    | `std::unordered_map<std::string, std::string>` | Custom properties           |
| `created_time`  | `int64_t`                                      | Creation timestamp          |
| `modified_time` | `int64_t`                                      | Last modification timestamp |

## `LakeSnapshot`

| Field            | Type                        |  Description       |
|------------------|-----------------------------|--------------------|
| `snapshot_id`    | `int64_t`                   | Snapshot ID        |
| `bucket_offsets` | `std::vector<BucketOffset>` | All bucket offsets |

## `BucketOffset`

| Field          | Type      | Description  |
|----------------|-----------|--------------|
| `table_id`     | `int64_t` | Table ID     |
| `partition_id` | `int64_t` | Partition ID |
| `bucket_id`    | `int32_t` | Bucket ID    |
| `offset`       | `int64_t` | Offset value |

## `OffsetSpec`

| Method                                             | Description                             |
|----------------------------------------------------|-----------------------------------------|
| `OffsetSpec::Earliest()`                          | Query for the earliest available offset |
| `OffsetSpec::Latest()`                            | Query for the latest offset             |
| `OffsetSpec::Timestamp(int64_t timestamp_ms)`     | Query offset at a specific timestamp    |

## Constants

| Constant                 |  Value |  Description                                            |
|--------------------------|--------|---------------------------------------------------------|
| `fluss::EARLIEST_OFFSET` | `-2`   | Start reading from the earliest available offset        |

To start reading from the latest offset (only new records), resolve the current offset via `ListOffsets` before subscribing:

```cpp
std::unordered_map<int32_t, int64_t> offsets;
admin.ListOffsets(table_path, {0}, fluss::OffsetSpec::Latest(), offsets);
scanner.Subscribe(0, offsets[0]);
```

## Enums

### `ChangeType`

| Value          | Short String | Description                      |
|----------------|--------------|----------------------------------|
| `AppendOnly`   | `+A`         | Append-only record               |
| `Insert`       | `+I`         | Inserted row                     |
| `UpdateBefore` | `-U`         | Previous value of an updated row |
| `UpdateAfter`  | `+U`         | New value of an updated row      |
| `Delete`       | `-D`         | Deleted row                      |

You may refer to the following example to convert ChangeType enum to its short string representation.

```cpp
inline const char* ChangeTypeShortString(ChangeType ct) {
    switch (ct) {
        case ChangeType::AppendOnly: return "+A";
        case ChangeType::Insert: return "+I";
        case ChangeType::UpdateBefore: return "-U";
        case ChangeType::UpdateAfter: return "+U";
        case ChangeType::Delete: return "-D";
    }
    throw std::invalid_argument("Unknown ChangeType");
}
```

### `TypeId`

| Value          |  Description               |
|----------------|----------------------------|
| `Boolean`      | Boolean type               |
| `TinyInt`      | 8-bit signed integer       |
| `SmallInt`     | 16-bit signed integer      |
| `Int`          | 32-bit signed integer      |
| `BigInt`       | 64-bit signed integer      |
| `Float`        | 32-bit floating point      |
| `Double`       | 64-bit floating point      |
| `String`       | UTF-8 string               |
| `Bytes`        | Binary data                |
| `Date`         | Date                       |
| `Time`         | Time                       |
| `Timestamp`    | Timestamp without timezone |
| `TimestampLtz` | Timestamp with timezone    |
| `Decimal`      | Decimal                    |
| `Array`        | Array of elements          |
| `Map`          | Map of key/value pairs     |
| `Row`          | Row (struct) of fields     |

### `ChangeType`

| Value          |  Description                                |
|----------------|---------------------------------------------|
| `AppendOnly`   | Append-only record (log tables)             |
| `Insert`       | Inserted row (PK tables)                    |
| `UpdateBefore` | Row value before an update (PK tables)      |
| `UpdateAfter`  | Row value after an update (PK tables)       |
| `Delete`       | Deleted row (PK tables)                     |

### `OffsetSpec`

| Value       |  Description                   |
|-------------|--------------------------------|
| `Earliest`  | Earliest available offset      |
| `Latest`    | Latest offset                  |
| `Timestamp` | Offset at a specific timestamp |
