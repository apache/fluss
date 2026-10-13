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

## Log filter pushdown

`TableScan::Filter()` pushes a predicate to Arrow log scans for server-side
RecordBatch pruning:

```cpp
fluss::RecordBatchLogScanner scanner;
auto predicate =
    fluss::Col("amount")
        .GreaterOrEqual(100)
        .And(fluss::Col("region").In({"CN", "SG"}));

auto result = table.NewScan()
                  .Filter(std::move(predicate))
                  .ProjectByName({"order_id", "amount"})
                  .CreateRecordBatchLogScanner(scanner);
```

Supported expressions include comparisons, `IS NULL` / `IS NOT NULL`, string
prefix/infix/suffix matching, `IN` / `NOT IN`, and `AND` / `OR`. Scalar
literals include booleans, integers, floating-point values, strings, bytes,
decimals, dates, times, and timestamps.

Pushdown is conservative: Fluss skips only whole RecordBatches whose statistics
prove that they cannot match. Returned batches may still contain non-matching
rows, so callers must evaluate the predicate again. Configure
`table.statistics.columns` for the referenced columns; batches without usable
statistics are retained. Filter pushdown requires the Arrow log format and does
not apply to `CreateBucketBatchScanner()`.

## Examples and Documentation

- [examples/example.cpp](examples/example.cpp) demonstrates log-table writes with Wait and bounded callbacks, continuous scans,
  bounded Arrow record-batch scans, projections, and offset queries.
- [examples/admin_example.cpp](examples/admin_example.cpp) demonstrates database, table,
  partition, and cluster administration.
- [examples/kv_example.cpp](examples/kv_example.cpp) and
  [examples/kv_changelog_example.cpp](examples/kv_changelog_example.cpp) demonstrate
  primary-key table access.
- The website documentation includes the
  [C++ API reference](../../website/docs/user-guide/cpp/api-reference.md) and
  [log-table examples](../../website/docs/user-guide/cpp/example/log-tables.md).

The SDK executes `WriteCallback` (`void(const WriteCompletion&)`) on one shared
worker, serially in dispatch order and off the I/O threads. `WriteCompletion.result`
is the write outcome; copy it before passing it to another worker.
`CreateWriter(writer)` uses the default `WriteCallbackOptions`; the overload
`CreateWriter(writer, options)` accepts a positive `max_pending_operations` limit
(default 262144) per writer. This operation-count budget is independent of the
Connection's byte-counted write buffer. Slow callbacks can fill it even when the
write buffer has room. The Rust write-buffer permit is released when the batch
completes, before the user callback returns, but the callback object and its
captures remain retained until callback completion. Once the per-writer callback
limit is full, callback-based submissions wait or fail according to
`client.writer.buffer.wait-timeout`; its default is unbounded.

Callback-capacity and buffer waits share `client.writer.buffer.wait-timeout`.
Zero makes those waits fail fast; this is not a deadline for the entire API call,
ACKs, retries, or callback execution. See the
[buffer sizing guidance](../../website/docs/user-guide/cpp/api-reference.md#sizing-callback-capacity-and-write-buffers)
for independent capacity and byte budgets. Callback worker initialization failure
rejects the submission before any data is accepted; there is no parallel fallback.

A failed callback does not prove that the record was not written. Application
resubmission can duplicate it, even with SDK idempotence enabled. The example only
counts and logs outcomes; it does not implement durable recovery. Keep callbacks
short, protect shared state, and handle retries outside the callback with an
application recovery policy.

After submissions stop, `Flush()` first flushes writes and, on success, blocks until
pending callbacks finish, acting as a barrier. A callback that never returns hangs it.
Calling it from a write callback is rejected before flushing. If a write flush
returns an error, keep callback state alive; if
it succeeds, still check individual write results.
See the [callback guarantees and recovery guidance](../../website/docs/user-guide/cpp/api-reference.md#write-guarantees-and-recovery)
for result semantics, callback implementation, and shutdown requirements.

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

Timestamp-bounded reads use the same iterator after resolving the half-open timestamp range
independently for each bucket:

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
and only an `Ok()` result means the bounded result is complete. Once the budget is exhausted, the
reader stops waiting for scanner data but still drains already-buffered batches and observes
completion before reporting a timeout. This means an already-complete reader returns `Ok()` even
with a non-positive timeout. The reader remains valid after timeout if a caller has an explicit
resume policy, but unconditional retry is not the intended usage. `NextBatch()` remains the
per-poll API for engines that need to check cancellation between batches.

## TODO

- [ ] How to introduce fluss-cpp in your own project, https://github.com/apache/opendal/blob/main/bindings/cpp/README.md is a good reference
- [ ] Add CMake/Bazel install and packaging instructions.
- [ ] Add more C++ examples (upsert, partitioned bounded scans, etc.).
