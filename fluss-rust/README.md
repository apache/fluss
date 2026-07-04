<!--
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
-->

# Apache Fluss™ Rust Client (Incubating)

![Experimental](https://img.shields.io/badge/status-experimental-orange)
[![crates.io](https://img.shields.io/crates/v/fluss-rs.svg)](https://crates.io/crates/fluss-rs)
[![docs.rs](https://img.shields.io/docsrs/fluss-rs)](https://docs.rs/fluss-rs/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

The official Rust **client** library for [Apache Fluss™](https://fluss.apache.org/) (Incubating) — a streaming storage built for real-time analytics, serving as the real-time data layer for Lakehouse architectures. This is a **client SDK**, not the Fluss server itself.

This repository contains:

- **`fluss-rs`** — the Rust core client (crates.io: [`fluss-rs`](https://crates.io/crates/fluss-rs))
- **Language bindings** — Python, C++, and Elixir clients built on top of `fluss-rs`

---

## What is Fluss?

[Fluss](https://fluss.apache.org/) bridges the gap between streaming data and the data Lakehouse by enabling **low-latency, high-throughput data ingestion and processing** while seamlessly integrating with popular compute engines (Flink, Spark, Trino).

Key concepts:

- **Log table** — append-only table (no primary key). Immutable records, ideal for event streams and audit trails.
- **Primary Key (KV) table** — keyed table supporting upsert, delete, and point/prefixed lookups.
- **Bucket** — parallelism unit within a table (similar to Kafka partitions).
- **Partition** — data organization by column values (e.g., by date or region).

---

## Features

### Core Client (`fluss-rs`)

| Category        | Capabilities                                                              |
| --------------- | ------------------------------------------------------------------------- |
| **Connection**  | Bootstrap to Fluss cluster, SASL authentication, graceful shutdown        |
| **Admin**       | Create/drop/list databases & tables, manage partitions, list offsets      |
| **Log Tables**  | Append (single-row + Arrow `RecordBatch`), scan with subscribe/poll       |
| **KV Tables**   | Upsert, delete, point lookup, **prefix lookup**, partitioned KV support   |
| **Data Types**  | Int, BigInt, String, Float, Double, Boolean, Bytes, Decimal, Date, Time, Timestamp, TimestampLTZ, Char, Binary |
| **Config**      | Batch sizing, buffering, retries, compression, timeouts, prefetch, concurrency |
| **Storage**     | Memory, Filesystem, S3, OSS (via [OpenDAL](https://opendal.apache.org/)) |
| **Observability** | Connection, writer, and scanner [metrics](https://clients.fluss.apache.org/docs/user-guide/rust/metrics/) via the [`metrics`](https://docs.rs/metrics) facade (Prometheus, StatsD, etc.) |
| **WASM**        | Compiles for `wasm32` target                                             |

### Language Bindings

| Language   | Package / Build          | Async Runtime            | Data Format                 |
| ---------- | ------------------------ | ------------------------ | --------------------------- |
| **Rust**   | [fluss-rs](https://crates.io/crates/fluss-rs) (crates.io) | Tokio                    | Arrow `RecordBatch` / `GenericRow` |
| **Python** | Build from source (PyO3) | asyncio                  | PyArrow / Pandas / dict     |
| **C++**    | CMake / Bazel (FFI)      | Synchronous (Tokio internally) | Arrow RecordBatch / GenericRow |
| **Elixir** | [Rustler](https://github.com/rusterlium/rustler) NIFs | Erlang processes         | Elixir values               |

---

## Project Structure

```
fluss-rust/
├── crates/
│   ├── fluss/                # Core Rust client (fluss-rs)
│   │   ├── src/client/       #   Connection, Admin, Table, Scan, Upsert, Lookup
│   │   ├── src/metadata/     #   Schema, TableDescriptor, DataTypes, Partitions
│   │   ├── src/row/          #   GenericRow, InternalRow, Arrow integration
│   │   ├── src/rpc/          #   gRPC transport layer
│   │   └── src/config.rs     #   Client configuration
│   ├── examples/             # runnable examples (log, KV, partitioned, prefix lookup, metrics)
│   └── fluss-test-cluster/   # Test harness for integration tests
├── bindings/
│   ├── python/               # Python binding (PyO3)
│   ├── cpp/                  # C++ binding (FFI + header)
│   └── elixir/               # Elixir binding (Rustler NIF)
├── website/                  # Docusaurus documentation site
├── docs/                     # Supplementary documentation
└── scripts/                  # Release & version management
```

---

## Quick Start

### Prerequisites

- [Java 17+](https://adoptium.net/) for running the Fluss cluster
- [Rust](https://www.rust-lang.org/tools/install) (latest stable)
- Linux or macOS (Windows is not currently supported)

### 1. Start a Fluss Cluster

```shell
# Download and extract Fluss (0.8.0+)
curl -LO https://dlcdn.apache.org/incubator/fluss/0.8.0/fluss-0.8.0-incubating-bin.tgz
tar -xzf fluss-0.8.0-incubating-bin.tgz
cd fluss-0.8.0-incubating/

# Start a local cluster
./bin/local-cluster.sh start
```

### 2. Add `fluss-rs` to Your Project

```toml
[dependencies]
fluss = { package = "fluss-rs", version = "0.2" }
tokio = { version = "1", features = ["full"] }
```

### 3. Write Code

#### Log Table: Append + Scan

```rust
use fluss::client::{EARLIEST_OFFSET, FlussConnection};
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{DataGetters, GenericRow};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::default();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();
    let conn = FlussConnection::new(config).await?;
    let admin = conn.get_admin()?;

    // Create a log table
    let table_path = TablePath::new("fluss", "events");
    let schema = Schema::builder()
        .column("ts", DataTypes::bigint())
        .column("message", DataTypes::string())
        .build()?;
    let descriptor = TableDescriptor::builder().schema(schema).build()?;
    admin.create_table(&table_path, &descriptor, true).await?;

    // Append rows
    let table = conn.get_table(&table_path).await?;
    let writer = table.new_append()?.create_writer()?;
    let mut row = GenericRow::new(2);
    row.set_field(0, 1_700_000_000_000i64);
    row.set_field(1, "hello fluss");
    writer.append(&row)?;
    writer.flush().await?;

    // Scan logs
    let scanner = table.new_scan().create_log_scanner()?;
    scanner.subscribe(0, EARLIEST_OFFSET).await?;
    loop {
        let records = scanner.poll(Duration::from_secs(5)).await?;
        for record in records {
            let row = record.row();
            println!("offset={}, ts={}, message={}",
                     record.offset(), row.get_long(0)?, row.get_string(1)?);
        }
    }
}
```

#### KV Table: Upsert + Lookup

```rust
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{DataGetters, GenericRow};

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::default();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();
    let conn = FlussConnection::new(config).await?;
    let admin = conn.get_admin()?;

    // Create a KV table
    let table_path = TablePath::new("fluss", "users");
    let schema = Schema::builder()
        .column("id", DataTypes::int())
        .column("name", DataTypes::string())
        .column("score", DataTypes::bigint())
        .primary_key(vec!["id"])
        .build()?;
    let descriptor = TableDescriptor::builder().schema(schema).build()?;
    admin.create_table(&table_path, &descriptor, true).await?;

    // Upsert rows
    let table = conn.get_table(&table_path).await?;
    let writer = table.new_upsert()?.create_writer()?;
    for (id, name, score) in [(1, "Alice", 95i64), (2, "Bob", 87)] {
        let mut row = GenericRow::new(3);
        row.set_field(0, id);
        row.set_field(1, name);
        row.set_field(2, score);
        writer.upsert(&row)?;
    }
    writer.flush().await?;

    // Point lookup by primary key
    let lookuper = table.new_lookup()?.create_lookuper()?;
    let mut key = GenericRow::new(1);
    key.set_field(0, 1i32);
    if let Some(row) = lookuper.lookup(&key).await?.get_single_row()? {
        println!("id={}, name={}, score={}",
                 row.get_int(0)?, row.get_string(1)?, row.get_long(2)?);
    }

    Ok(())
}
```

#### More Examples

| Example                                  | Description                                    |
| ---------------------------------------- | ---------------------------------------------- |
| `example-table`                          | Log table: append + scan with Arrow batch      |
| `example-upsert-lookup`                  | KV table: upsert + point lookup                |
| `example-partitioned-upsert-lookup`      | KV table with partitions                       |
| `example-prefix-lookup`                  | Prefix lookup on bucket keys                   |
| `example-partitioned-prefix-lookup`      | Prefix lookup on partitioned tables            |
| `example-prometheus-metrics`             | Expose client metrics on a Prometheus endpoint |

Build and run any example:

```shell
cargo build --example example-table --release
./target/release/examples/example-table
```

---

## Configuration

`Config` supports the following key options (all with sensible defaults):

| Option                                | Default           | Description                                   |
| ------------------------------------- | ----------------- | --------------------------------------------- |
| `bootstrap_servers`                    | `127.0.0.1:9123`  | Fluss coordinator address                     |
| `writer_batch_size`                    | 2 MB              | Max batch size before flushing                |
| `writer_batch_timeout_ms`              | 100 ms            | Max time before auto-flush                    |
| `writer_buffer_memory_size`            | 64 MB             | Total buffer memory for pending writes        |
| `writer_retries`                       | `i32::MAX`        | Max write retries                             |
| `scanner_log_fetch_max_bytes`          | 16 MB             | Max bytes per fetch request                   |
| `scanner_log_fetch_wait_max_time_ms`   | 500 ms            | Max wait time for fetch                       |
| `scanner_remote_log_read_concurrency`  | 4                 | Concurrency for remote log reads              |
| `connect_timeout_ms`                   | 120 s             | Connection timeout                            |
| `security_sasl_username` / `security_sasl_password` | — | SASL PLAIN authentication             |

Configuration can be set programmatically or via CLI flags (using [`clap`](https://docs.rs/clap)).

---

## Documentation

- **[User Guide](https://clients.fluss.apache.org/)** — Full documentation for Rust, Python, C++, and Elixir clients
- **[API Docs (docs.rs)](https://docs.rs/fluss-rs/)** — Rust crate API reference
- **[Development Guide](DEVELOPMENT.md)** — Build, test, and contribute
- **[Release Guide](website/docs/release/create-release.md)** — How to create an official release

---

## Development

```shell
# Build
cargo build

# Run tests
cargo test

# Run integration tests (requires Docker for test cluster)
cargo test --features integration_tests

# Build C++ bindings
cd bindings/cpp && mkdir build && cd build && cmake .. && cmake --build .

# Build Python bindings
cd bindings/python && maturin develop

# Elixir tests
cd bindings/elixir && mix test
```

---

## Contributing

This project is part of the Apache Fluss (Incubating) community. Contributions are welcome!

- Join the [dev mailing list](https://fluss.apache.org/community/welcome/)
- Check out [DEVELOPMENT.md](DEVELOPMENT.md) for setup instructions
- Submit PRs following the [Apache contribution guidelines](https://www.apache.org/foundation/policies/conduct.html)

---

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

```
Copyright 2025-2026 The Apache Software Foundation

This product includes software developed at
The Apache Software Foundation (https://www.apache.org/).
```

Apache Fluss, Fluss, Apache, the Apache feather logo, and the Apache Fluss project logo
are either registered trademarks or trademarks of The Apache Software Foundation
in the United States and other countries.