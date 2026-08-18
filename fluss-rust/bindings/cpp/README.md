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

## Log predicate pushdown

`TableScan::Filter()` pushes a predicate to Arrow log scans for server-side
RecordBatch pruning:

```cpp
fluss::LogScanner scanner;
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
rows, so callers must evaluate the predicate again. Filter pushdown requires
the Arrow log format and does not apply to `CreateBucketBatchScanner()`.

## TODO

- [] How to introduce fluss-cpp in your own project, https://github.com/apache/opendal/blob/main/bindings/cpp/README.md is a good reference
- [ ] Add CMake/Bazel install and packaging instructions.
- [ ] Document API usage and minimal example in this README.
- [ ] Add more C++ examples (log scan, upsert, etc.).
