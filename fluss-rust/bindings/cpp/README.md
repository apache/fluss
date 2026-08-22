# Apache Fluss™ C++ Bindings

C++ bindings for Fluss, built on top of the [fluss-rust](../../crates/fluss) client. The API is exposed via a C++ header ([include/fluss.hpp](include/fluss.hpp)) and implemented with Rust FFI.

## Requirements

- Rust (see [rust-toolchain.toml](../../rust-toolchain.toml) at repo root)
- C++17-capable compiler
- CMake 3.18+ and/or Bazel
- Apache Arrow (for Arrow-based APIs)

## Build

From the repository root or from `bindings/cpp`:

**With CMake:**

```bash
cd bindings/cpp
mkdir build && cd build
cmake ..
cmake --build .
```

By default, CMake now uses `Release` when `CMAKE_BUILD_TYPE` is not specified.

**With Bazel:**

```bash
cd bindings/cpp
bazel build //...
```
`ci.sh` defaults to optimized builds via `-c opt` (override with `BAZEL_BUILD_FLAGS` if needed).
See [ci.sh](ci.sh) for the CI build sequence.

## Examples and Documentation

- [examples/example.cpp](examples/example.cpp) demonstrates log-table writes, continuous scans,
  bounded Arrow record-batch scans, projections, and offset queries.
- [examples/admin_example.cpp](examples/admin_example.cpp) demonstrates database, table,
  partition, and cluster administration.
- [examples/kv_example.cpp](examples/kv_example.cpp) and
  [examples/kv_changelog_example.cpp](examples/kv_changelog_example.cpp) demonstrate
  primary-key table access.
- The website documentation includes the
  [C++ API reference](../../website/docs/user-guide/cpp/api-reference.md) and
  [log-table examples](../../website/docs/user-guide/cpp/example/log-tables.md).

For a bounded log scan, pass the per-bucket offset ranges directly to `TableScan`. The returned
reader yields one Arrow batch at a time until every `[starting_offset, stopping_offset)` range
is complete:

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
        // Bail out on unretriable failures (auth, invalid table, ...); the
        // reader's status field is only meaningful when `Ok()` is true.
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

Timestamp-bounded reads use the same iterator after resolving the timestamps independently for
each bucket:

```cpp
fluss::RecordBatchLogReader timestamp_reader;
table.NewScan().CreateRecordBatchLogReader(
    admin, table_buckets,
    fluss::TimestampRange{starting_timestamp_ms, stopping_timestamp_ms}, timestamp_reader);
```

`CollectAllBatches(timeout_ms, out)` is available when materializing the complete bounded result
is preferred. `timeout_ms` is the total execution budget for the whole call, so callers should
normally pass the query's remaining execution time and call the method once. It appends complete
batches to `out` as they arrive. If the budget expires before every stopping offset is reached, it
stops collecting and returns a retriable `REQUEST_TIME_OUT`; `out` may contain a partial result,
and only an `Ok()` result means the bounded result is complete. The timeout is checked between
complete Arrow batches and never splits a batch already being returned. The reader remains valid
after timeout if a caller has an explicit resume policy, but unconditional retry is not the
intended usage. `NextBatch()` remains the per-poll API for engines that need to check cancellation
between batches.

## TODO

- [ ] How to introduce fluss-cpp in your own project, https://github.com/apache/opendal/blob/main/bindings/cpp/README.md is a good reference
- [ ] Add CMake/Bazel install and packaging instructions.
- [ ] Add more C++ examples (upsert, partitioned bounded scans, etc.).
