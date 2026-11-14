---
sidebar_position: 4
---
# Log Tables

Log tables are append-only tables without primary keys, suitable for event streaming.

## Creating a Log Table

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("event_id", fluss::DataType::Int())
    .AddColumn("event_type", fluss::DataType::String())
    .AddColumn("timestamp", fluss::DataType::BigInt())
    .Build();

auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .Build();

fluss::TablePath table_path("fluss", "events");
admin.CreateTable(table_path, descriptor, true);
```

## Writing to Log Tables

```cpp
fluss::Table table;
conn.GetTable(table_path, table);

fluss::AppendWriter writer;
table.NewAppend().CreateWriter(writer);

fluss::GenericRow row;
row.SetInt32(0, 1);           // event_id
row.SetString(1, "user_login");  // event_type
row.SetInt64(2, 1704067200000L); // timestamp
writer.Append(row);

writer.Flush();
```

## Reading from Log Tables

```cpp
fluss::LogScanner scanner;
table.NewScan().CreateLogScanner(scanner);

auto info = table.GetTableInfo();
for (int b = 0; b < info.num_buckets; ++b) {
    scanner.Subscribe(b, 0);
}

fluss::ScanRecords records;
scanner.Poll(5000, records);  // timeout in ms

for (const auto& rec : records) {
    std::cout << "event_id=" << rec.row.GetInt32(0)
              << " event_type=" << rec.row.GetString(1)
              << " timestamp=" << rec.row.GetInt64(2)
              << " @ offset=" << rec.offset << std::endl;
}

// Or per-bucket access
for (const auto& bucket : records.Buckets()) {
    auto view = records.Records(bucket);
    std::cout << "Bucket " << bucket.bucket_id << ": "
              << view.Size() << " records" << std::endl;
    for (const auto& rec : view) {
        std::cout << "  event_id=" << rec.row.GetInt32(0)
                  << " event_type=" << rec.row.GetString(1)
                  << " @ offset=" << rec.offset << std::endl;
    }
}
```

**Continuous polling:**

```cpp
while (running) {
    fluss::ScanRecords records;
    scanner.Poll(1000, records);
    for (const auto& rec : records) {
        process(rec);
    }
}
```

**Accumulating records across polls:**

`ScanRecord` is a value type — it can be freely copied, stored, and accumulated. The underlying data stays alive via reference counting (zero-copy).

```cpp
std::vector<fluss::ScanRecord> all_records;
while (all_records.size() < 1000) {
    fluss::ScanRecords records;
    scanner.Poll(1000, records);
    for (const auto& rec : records) {
        all_records.push_back(rec);  // ref-counted, no data copy
    }
}
// all_records is valid — each record keeps its data alive
```

**Batch subscribe:**

```cpp
std::vector<fluss::BucketSubscription> subscriptions;
subscriptions.push_back({0, 0});    // bucket 0, offset 0
subscriptions.push_back({1, 100});  // bucket 1, offset 100
scanner.Subscribe(subscriptions);
```

**Unsubscribe from a bucket:**

```cpp
// Stop receiving records from bucket 1
scanner.Unsubscribe(1);
```

**Arrow RecordBatch polling (high performance):**

```cpp
#include <arrow/record_batch.h>

fluss::RecordBatchLogScanner arrow_scanner;
table.NewScan().CreateRecordBatchLogScanner(arrow_scanner);

for (int b = 0; b < info.num_buckets; ++b) {
    arrow_scanner.Subscribe(b, 0);
}

fluss::ArrowRecordBatches batches;
arrow_scanner.Poll(5000, batches);

for (size_t i = 0; i < batches.Size(); ++i) {
    const auto& batch = batches[i];
    if (batch->Available()) {
        auto arrow_batch = batch->GetArrowRecordBatch();
        std::cout << "Batch " << i << ": " << arrow_batch->num_rows() << " rows"
                  << ", partition_id=" << batch->GetPartitionId()
                  << ", bucket_id=" << batch->GetBucketId() << std::endl;
    }
}
```

## Filter Pushdown

Configure statistics for columns used by filters:

```cpp
auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .SetProperty("table.statistics.columns", "event_id,event_type")
    .Build();
```

Then attach a predicate to the scan:

```cpp
auto predicate =
    fluss::Col("event_id")
        .GreaterOrEqual(100)
        .And(fluss::Col("event_type").StartsWith("user_"));

fluss::RecordBatchLogScanner scanner;
auto result = table.NewScan()
                  .Filter(std::move(predicate))
                  .ProjectByName({"event_id", "event_type"})
                  .CreateRecordBatchLogScanner(scanner);
```

Fluss uses RecordBatch statistics to skip batches that cannot match. It does not filter individual
rows, so consumers must evaluate the predicate again on returned batches. Batches without usable
statistics are retained conservatively.

## Bounded Arrow RecordBatch Reading

Use `RecordBatchLogReader` when the scan should finish after reaching a fixed offset for every
bucket. Query engines can pass the complete per-bucket ranges directly:

```cpp
auto info = table.GetTableInfo();

std::vector<int32_t> bucket_ids;
for (int32_t bucket_id = 0; bucket_id < info.num_buckets; ++bucket_id) {
    bucket_ids.push_back(bucket_id);
}

std::unordered_map<int32_t, int64_t> latest_offsets;
admin.ListOffsets(table_path, bucket_ids, fluss::OffsetSpec::Latest(), latest_offsets);

std::vector<fluss::RecordBatchLogReadRange> ranges;
for (int32_t bucket_id : bucket_ids) {
    ranges.push_back(
        {fluss::TableBucket{info.table_id, bucket_id}, 0, latest_offsets.at(bucket_id)});
}

fluss::RecordBatchLogReader reader;
table.NewScan().CreateRecordBatchLogReader(ranges, reader);

const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
bool finished = false;
while (std::chrono::steady_clock::now() < deadline) {
    fluss::RecordBatchReadResult result;
    auto read_result = reader.NextBatch(1000, result);
    if (!read_result.Ok()) {
        if (!read_result.IsRetriable()) {
            throw std::runtime_error(read_result.error_message);
        }
        continue;
    }
    if (result.status == fluss::BoundedReadStatus::TimedOut) {
        continue;
    }
    if (result.status == fluss::BoundedReadStatus::Finished) {
        finished = true;
        break;
    }

    std::cout << "bucket=" << result.batch->GetBucketId()
              << " base_offset=" << result.batch->GetBaseOffset()
              << " last_offset=" << result.batch->GetLastOffset()
              << " rows=" << result.batch->NumRows() << std::endl;
}
if (!finished) {
    throw std::runtime_error("Bounded read exceeded its execution deadline");
}
```

Inspect `result.status` only when `NextBatch()` returns an `Ok()` result. `TimedOut` does not
exhaust the reader; it lets a query engine periodically check cancellation or deadlines before
retrying. `Finished` means all stopping offsets have been reached.

To read a half-open log timestamp range `[starting_timestamp_ms, stopping_timestamp_ms)`, pass the
assigned buckets and timestamps. Fluss resolves both timestamps with `OffsetSpec::Timestamp` for
every bucket, then uses the same bounded offset reader:

```cpp
fluss::RecordBatchLogReader reader;
table.NewScan().CreateRecordBatchLogReader(
    admin, assigned_buckets,
    fluss::TimestampRange{starting_timestamp_ms, stopping_timestamp_ms}, reader);

const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
bool finished = false;
while (std::chrono::steady_clock::now() < deadline) {
    fluss::RecordBatchReadResult result;
    auto read_result = reader.NextBatch(1000, result);
    if (!read_result.Ok()) {
        if (!read_result.IsRetriable()) {
            throw std::runtime_error(read_result.error_message);
        }
        continue;
    }
    if (result.status == fluss::BoundedReadStatus::TimedOut) {
        continue;
    }
    if (result.status == fluss::BoundedReadStatus::Finished) {
        finished = true;
        break;
    }
    process(result.batch->GetArrowRecordBatch());
}
if (!finished) {
    throw std::runtime_error("Bounded read exceeded its execution deadline");
}
```

For the common case where the client should read everything currently available, let the
reader query the latest offsets:

```cpp
fluss::RecordBatchLogScanner latest_scanner;
table.NewScan().CreateRecordBatchLogScanner(latest_scanner);
for (int32_t bucket_id : bucket_ids) {
    latest_scanner.Subscribe(bucket_id, 0);
}

fluss::RecordBatchLogReader latest_reader;
std::move(latest_scanner).CreateRecordBatchLogReaderUntilLatest(admin, latest_reader);

fluss::ArrowRecordBatches batches;
// Use the remaining query execution time as the budget for the whole collection.
auto collect_result = latest_reader.CollectAllBatches(30000, batches);
if (!collect_result.Ok()) {
    // `batches` may be partial. Propagate the timeout/error instead of treating
    // it as a complete bounded result or retrying unconditionally.
    throw std::runtime_error(collect_result.error_message);
}
```

The scanner-level creation methods transfer ownership on success, so the scanner becomes
unavailable after it is moved into the reader.

:::caution Partial results on timeout

`CollectAllBatches()` is not all-or-nothing. It appends complete batches to its output while
reading, so a `REQUEST_TIME_OUT` result may leave `batches` partially populated. Only an `Ok()`
result means all stopping offsets have been reached.

The supplied timeout is the total execution budget for the whole `CollectAllBatches()` call, not a
per-batch polling timeout. Once the budget expires, the method stops collecting and returns
`REQUEST_TIME_OUT` if unread work remains; it does not internally retry with a fresh budget. It
does not wait for more scanner data after the deadline, but it still drains already-buffered
batches and reports `Ok()` if every stopping offset has been reached. The reader remains valid
after timeout, but applications should normally propagate the incomplete result. Resuming with the
same reader and output should only be done as an explicit higher-level policy with its own deadline.

:::

## Column Projection

```cpp
// Project by column index
fluss::LogScanner projected_scanner;
table.NewScan().ProjectByIndex({0, 2}).CreateLogScanner(projected_scanner);

// Project by column name
fluss::LogScanner name_projected_scanner;
table.NewScan().ProjectByName({"event_id", "timestamp"}).CreateLogScanner(name_projected_scanner);

// Arrow RecordBatch with projection
fluss::RecordBatchLogScanner projected_arrow_scanner;
table.NewScan().ProjectByIndex({0, 2}).CreateRecordBatchLogScanner(projected_arrow_scanner);
```

## Limit Scan

For a bounded read of up to `n` rows from a single bucket, use a batch scanner instead of subscribing. It issues one request; `NextBatch` yields the batch once, then reports empty.

```cpp
int64_t table_id = table.GetTableInfo().table_id;
fluss::TableBucket bucket{table_id, 0};

fluss::BatchScanner scanner;
table.NewScan().Limit(10).CreateBucketBatchScanner(bucket, scanner);

fluss::ArrowRecordBatches batches;
scanner.NextBatch(batches);  // or CollectAllBatches(batches)
for (const auto& batch : batches) {
    std::cout << "rows: " << batch->NumRows() << std::endl;
}
```

The limit applies per bucket; scan each bucket to cover a multi-bucket table.
