# Apache Fluss (Incubating) Official Rust Client

Official Rust client library for [Apache Fluss (Incubating)](https://fluss.apache.org/).

[![crates.io](https://img.shields.io/crates/v/fluss-rs.svg)](https://crates.io/crates/fluss-rs)
[![docs.rs](https://img.shields.io/docsrs/fluss-rs)](https://docs.rs/fluss-rs/)

## Usage

The following example shows both **primary key (KV) tables** and **log tables** in one flow: connect, create a KV table (upsert + lookup), then create a log table (append + scan).

```rust
use fluss::client::EARLIEST_OFFSET;
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{GenericRow, InternalRow};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::default();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();
    let connection = FlussConnection::new(config).await?;
    let admin = connection.get_admin()?;

    // ---- Primary key (KV) table: upsert and lookup ----
    let kv_path = TablePath::new("fluss", "users");
    let mut kv_schema = Schema::builder()
        .column("id", DataTypes::int())
        .column("name", DataTypes::string())
        .column("age", DataTypes::bigint())
        .primary_key(vec!["id"]);
    let kv_descriptor = TableDescriptor::builder()
        .schema(kv_schema.build()?)
        .build()?;
    admin.create_table(&kv_path, &kv_descriptor, false).await?;

    let kv_table = connection.get_table(&kv_path).await?;
    let upsert_writer = kv_table.new_upsert()?.create_writer()?;
    let mut row = GenericRow::new(3);
    row.set_field(0, 1i32);
    row.set_field(1, "Alice");
    row.set_field(2, 30i64);
    upsert_writer.upsert(&row)?;
    upsert_writer.flush().await?;

    let mut lookuper = kv_table.new_lookup()?.create_lookuper()?;
    let mut key = GenericRow::new(1);
    key.set_field(0, 1i32);
    let result = lookuper.lookup(&key).await?;
    if let Some(r) = result.get_single_row()? {
        println!("KV lookup: id={}, name={}, age={}",
                 r.get_int(0)?, r.get_string(1)?, r.get_long(2)?);
    }

    // ---- Log table: append and scan ----
    let log_path = TablePath::new("fluss", "events");
    let log_schema = Schema::builder()
        .column("ts", DataTypes::bigint())
        .column("message", DataTypes::string())
        .build()?;
    let log_descriptor = TableDescriptor::builder()
        .schema(log_schema)
        .build()?;
    admin.create_table(&log_path, &log_descriptor, false).await?;

    let log_table = connection.get_table(&log_path).await?;
    let append_writer = log_table.new_append()?.create_writer()?;
    let mut event = GenericRow::new(2);
    event.set_field(0, 1700000000i64);
    event.set_field(1, "hello");
    append_writer.append(&event)?;
    append_writer.flush().await?;

    let scanner = log_table.new_scan().create_log_scanner()?;
    scanner.subscribe(0, EARLIEST_OFFSET).await?;
    let scan_records = scanner.poll(Duration::from_secs(1)).await?;
    for record in scan_records {
        let r = record.row();
        println!("Log scan: ts={}, message={}", r.get_long(0)?, r.get_string(1)?);
    }

    Ok(())
}
```

## Storage Support

The Fluss client reads remote data by accessing Fluss’s **remote files** (e.g. log segments and snapshots) directly. The following **remote file systems** are supported; enable the matching feature(s) for your deployment:

| Storage Backend | Feature Flag | Status | Description |
|----------------|--------------|--------|-------------|
| Local Filesystem | `storage-fs` | ✅ Stable | Local filesystem storage |
| Amazon S3 | `storage-s3` | ✅ Stable | Amazon S3 storage |
| Alibaba Cloud OSS | `storage-oss` | ✅ Stable | Alibaba Cloud Object Storage Service |

You can enable all storage backends at once using the `storage-all` feature flag.

Example usage in Cargo.toml:
```toml
[dependencies]
fluss-rs = { version = "0.x.x", features = ["storage-s3", "storage-fs"] }
```
