# Development Guide

Welcome to the development guide of `fluss-rust`! This project builds `fluss-rust` client and language specific bindings.  

## Pre-requisites

- rust

You can install these using your favourite package / version manager. Example installation using mise:

```bash
mise install rust
```

`protobuf` (the `protoc` compiler) is only needed when regenerating the protobuf code after the canonical `FlussApi.proto` changes (see `crates/fluss/regen.sh`). Regular builds use the checked-in `crates/fluss/src/proto/fluss.rs`.

## IDE Setup

We recommend [RustRover](https://www.jetbrains.com/rust/) IDE to work with fluss-rust code base.

### Importing fluss-rust

1. On your terminal, clone fluss-rust project from GitHub
   ```bash
   git clone https://github.com/apache/fluss.git
   ```
1. Open RustRover, on `Projects` tab, click `Open` and navigate to the root directory of fluss-rust
1. Click `Open`

### Copyright Profile

Fluss and Fluss-rust are Apache projects and as such every files need to have Apache licence header. This can be automated in RustRover by adding a Copyright profile:

1. Go to `Settings` -> `Editor` -> `Copyright` -> `Copyright Profiles`.
1. Add a new profile and name it `Apache`.
1. Add the following text as the license text:
   ```
   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
   ```
1. Go to `Editor` -> `Copyright` and choose the `Apache` profile as the default profile for this project.
1. Click `Apply`

We also use line comment formatting for licence headers. 
1. Go to `Editor` -> `Copyright` -> `Formatting` -> `Rust`
1. Choose `Use custom formatting`  
1. Choose `Use line comment`

## Project directories

Source files are organized in the following manner

1. `crates/fluss` - fluss rust client crate source
1. `crates/examples` - fluss rust client examples
1. `bindings` - bindings to other languages e.g. C++ under `bindings/cpp` and Python under `bindings/python`
1. Click `Apply`
2. 
## Building & Testing

See [quickstart](README.md#quick-start) for steps to run example code.

Running all unit tests for fluss rust client: 

```bash
cargo test --workspace
```

Running all integration test cases:

```bash
cargo test --features integration_tests --workspace
```

### Reading persisted KV snapshots

Enable `kv-snapshot` on `fluss-rs` to use `FlussTable::create_kv_snapshot_reader(bucket, snapshot_id)`. This downloads the selected snapshot under `scanner_io_tmp_dir`, opens the complete RocksDB database read-only, and returns Arrow batches aligned to the table handle's fixed schema. `next_batch(max_rows)` returns `None` at EOF; `close()` or dropping the reader releases its local files. It supports COMPACTED values with plain or tagged layouts. INDEXED values are currently unsupported by the Rust row decoder.

The feature is off by default. It adds `rocksdb 0.24` / native RocksDB 10.4.2, requiring a C++ compiler and libclang during builds, with compression libraries built by the native dependencies. Their declared MSRV is 1.85, matching this workspace; no existing dependency is upgraded. RocksDB and the wrapper offer Apache-2.0 licensing. Distribution builds that enable the feature must include the additional native dependency licenses. Python/C++/Elixir bindings do not enable this feature by default.

Initialization leases the exact snapshot, renews the lease during download, and releases it before returning. Cancellation finishes an in-flight file download before cleanup; keep the connection and Tokio runtime alive for lease release. Runtime shutdown or a failed release leaves the server lease to expire. Reading pauses on the first occurrence of an unknown schema, fetches it asynchronously, and caches its decoder before resuming at that row. This avoids a preliminary full snapshot scan and buffering copies of raw values; keep the connection alive until reading finishes. Missing source schemas fail the affected batch. Output is bounded by row count and batches may end at schema boundaries; no throughput benchmark has been run.

`create_kv_snapshot_reader_with_lease_duration(bucket, snapshot_id, Duration::from_secs(600))` selects a ten-minute lease; the default is five minutes. Durations are rounded down to milliseconds and must be at least one millisecond. After acquisition or a successful renewal, the reader waits one third of the duration before renewing. Transient renewal errors retry with exponential backoff from one to ten seconds, capped at one third of the duration and bounded by the last confirmed expiration. Permanent errors, unavailable snapshots, and expiration fail creation; a late success does not restore an expired lease. This controls tolerance for missed renewals, not total download time. Normal completion releases the lease immediately; a longer duration can retain remote files longer after a crash.

Snapshot file downloads share a connection-level limit from `remote_file_download_thread_num` (default 3). Each reader downloads its files sequentially, and cancellation while waiting for a slot does not start another download. `scanner_remote_log_read_concurrency` controls reads within each snapshot or log file. Existing log scanners retain their separate per-scanner download budgets; the snapshot and log paths do not share one combined limit.

For a bounded read, select the snapshot and its matching log offset `S`, acquire a lease covering that exact snapshot, then capture stop `E`. Use the snapshot rows as the baseline and consume the fixed `[S, E)` log range for that partition/bucket. `reader.log_offset()` reports `S` and should match the planned seam. Neither missing files nor unavailable snapshots cause a switch to a newer snapshot or an online scan. The UnionRead planner and deduplicate merge are separate integration work.

`create_kv_snapshot_reader_with_options` accepts `KvSnapshotReaderOptions`: `target_schema_id` fixes the output schema independently of the table handle; `projected_fields` selects columns in that target schema's order; both default to the table handle's full schema. Set `lease_mode` to `KvSnapshotLeaseMode::CallerManaged` when planning already holds the lease. The caller must renew it until creation and any cancellation cleanup finish. The reader never modifies that lease; after successful creation it reads local files. The default `Managed(Duration)` mode retains the automatic download lease. Historical rows are aligned to the target schema before projection, including nulls for columns absent in older rows.

Run the relevant integration test against the normal container harness, or set `FLUSS_KV_SNAPSHOT_BOOTSTRAP` to a test cluster configured with `kv.snapshot.interval: 1s`, `kv.snapshot.num-retained: 2`, and host-accessible local remote storage:

```bash
cargo test -p fluss-rs --features kv-snapshot,integration_tests --test test_fluss persisted_snapshot_current_view_and_schema
```


### License check (cargo-deny)

We use [cargo-deny](https://embarkstudios.github.io/cargo-deny/) to ensure all dependency licenses are Apache-compatible. When present, configuration lives in a `deny.toml` file at the repo root and should enforce an Apache-compatible license policy.

```bash
cargo install cargo-deny --locked
cargo deny check licenses
```

### Formatting and Clippy

Our CI runs cargo formatting and clippy to help keep the code base styling tidy and readable. Run the following commands and address any errors or warnings to ensure that your PR can complete CI successfully.

```bash
cargo fmt --all
cargo clippy --all-targets --fix --allow-dirty --allow-staged
```

