---
title: "Rust Client"
sidebar_position: 4
---

# Fluss Rust Client

## Overview
The Fluss Rust Client is a high-performance, asynchronous library powered by the `tokio` runtime. It provides a native interface for interacting with Fluss clusters with minimal overhead.

The client provides two main APIs:
* **Admin API**: For managing databases, tables, and partitions.
* **Table API**: For high-throughput reading (Scanners/Lookups) and writing (Writers).

## Installation
The Fluss Rust client is published to [crates.io](https://crates.io/crates/fluss-rs) as `fluss-rs`. Note that the library name for imports is `fluss`.

Add the following to your `Cargo.toml`:

```toml
[dependencies]
fluss-rs = "0.1"
tokio = { version = "1", features = ["full"] }
```

:::tip
This page provides a lightweight introduction to the Fluss Rust Client.

For a complete and up-to-date Rust client reference, including advanced APIs and examples, see the
[official Rust client documentation](https://github.com/apache/fluss-rust/blob/main/docs/rust-client.md).
:::


## Initialization

The FlussConnection is the entry point for interacting with a cluster. It is thread-safe and should be reused across your application

```rust
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::default();
    config.bootstrap_server = "127.0.0.1:9123".to_string();

    let conn = FlussConnection::new(config).await?;
    
    // Obtain Admin interface
    let admin = conn.get_admin().await?;
    
    Ok(())
}
```

## Admin API

The Admin interface allows you to manage the lifecycle of your data.

### Creating a Table

```rust
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};

let schema = Schema::builder()
.column("id", DataTypes::int())
.column("name", DataTypes::string())
.column("score", DataTypes::float())
.primary_key(vec!["id"])
.build()?;

let descriptor = TableDescriptor::builder()
.schema(schema)
.bucket_count(3)
.build()?;

let table_path = TablePath::new("my_db", "my_table");
admin.create_table(&table_path, &descriptor, true).await?;
```

## Table API

### Writers

Fluss writers use a fire-and-forget pattern. Use flush() to ensure batch persistence or await the append call for per-record acknowledgment

```rust
use fluss::row::GenericRow;

let table = conn.get_table(&table_path).await?;
let writer = table.new_append()?.create_writer()?;

// Prepare a row with 3 fields
let mut row = GenericRow::new(3);
row.set_field(0, 1);            // id (int)
row.set_field(1, "Alice");      // name (string)
row.set_field(2, 95.5f32);      // score (float)

// Queue the write
writer.append(&row)?;

// Ensure it is sent to the server
writer.flush().await?;
```

#### Writing to a Primary Key Table

```rust
let table = conn.get_table(&table_path).await?;
let upsert_writer = table.new_upsert()?.create_writer()?;

let mut row = GenericRow::new(3);
row.set_field(0, 1);            // id (PK)
row.set_field(1, "Bob");
row.set_field(2, 88.0f32);

// Upsert logic: inserts if new, updates if ID exists
upsert_writer.upsert(&row)?;
upsert_writer.flush().await?;
```

### Scanner

To read data, create a LogScanner. The poll method returns ScanRecords, which requires nested iteration.

```rust
use std::time::Duration;
use fluss::client::EARLIEST_OFFSET;

let mut scanner = table.new_scan().create_log_scanner()?;
scanner.subscribe(0, EARLIEST_OFFSET).await?;

loop {
    let records = scanner.poll(Duration::from_secs(5)).await?;
    
    // Iterate through bucket groups
    for (_bucket, bucket_records) in records.records_by_buckets() {
        for record in bucket_records {
            let row = record.row();
            println!("Received row: {:?} at offset {}", row, record.offset());
        }
    }
}
```

### Lookup

Perform point lookups on Primary Key tables using a key row.

```rust
let mut lookuper = table.new_lookup()?.create_lookuper()?;

// Key row must only contain Primary Key columns
let mut key = GenericRow::new(1);
key.set_field(0, 1); 

let result = lookuper.lookup(&key).await?;
if let Some(row) = result.get_single_row()? {
    println!("Found: id={}, name={}", row.get_int(0), row.get_string(1));
}
```

## Type Mapping

The Rust client maps Fluss types to Rust types as follows:

| Fluss Type | Rust Type | Method Example |
|-----------|-----------|----------------|
| `INT` | `i32` | `row.get_int(idx)` |
| `BIGINT` | `i64` | `row.get_long(idx)` |
| `STRING` | `&str` | `row.get_string(idx)` |
| `BOOLEAN` | `bool` | `row.get_boolean(idx)` |
| `FLOAT` | `f32` | `row.get_float(idx)` |
| `DOUBLE` | `f64` | `row.get_double(idx)` |
| `BYTES` | `&[u8]` | `row.get_bytes(idx)` |

