---
title: "Rust Client"
sidebar_position: 3
---

# Fluss Rust Client

## Overview
The Fluss Rust Client provides an interface for interacting with Fluss clusters. It supports asynchronous operations for managing resources and handling data.

The client provides two main APIs:
* **Admin API**: For managing databases, tables, partitions, and retrieving metadata.
* **Table API**: For reading from and writing to Fluss tables.

## Installation
To use the Fluss Rust client, add the following dependency to your `Cargo.toml` file:

```toml
[dependencies]
fluss-client = "0.6.0" # Replace with the latest version
tokio = { version = "1", features = ["full"] }
```

## Initialization

The `Connection` object is the entry point for interacting with Fluss. It is created using `Connection::create()` and requires a `Configuration` object.

The `Connection` object is thread-safe and can be shared across multiple tasks. It is recommended to create a single `Connection` instance per application and use it to create multiple `Admin` and `Table` instances.

```rust
use fluss_client::Connection;
use fluss_client::config::Configuration;
use fluss_client::admin::Admin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create configuration
    let mut conf = Configuration::default();
    conf.set_string("bootstrap.servers", "localhost:9123");

    // Create connection
    let connection = Connection::create(conf).await?;

    // Obtain Admin instance
    let admin = connection.get_admin();
    let databases = admin.list_databases().await?;
    println!("Databases: {:?}", databases);

    Ok(())
}
```

### SASL Authentication
If your Fluss cluster uses SASL authentication, configure the security properties:

```rust
    let mut conf = Configuration::default();
    conf.set_string("bootstrap.servers", "localhost:9123");
    conf.set_string("client.security.protocol", "sasl");
    conf.set_string("client.security.sasl.mechanism", "PLAIN");
    conf.set_string("client.security.sasl.username", "alice");
    conf.set_string("client.security.sasl.password", "alice-secret");

    let connection = Connection::create(conf).await?;
```

## Async Operations
All operations in the Fluss Rust client are asynchronous and return a `Future`. You should use the `.await` syntax to wait for the result of an operation. The client is designed to work with the `tokio` runtime.

## Admin API

The `Admin` API allows you to manage databases and tables.

### Creating a Database

```rust
use fluss_client::admin::DatabaseDescriptor;

// Create database descriptor
let descriptor = DatabaseDescriptor::builder()
    .comment("This is a test database")
    .add_custom_property("owner", "data-team")
    .build();

// Create database (ignore_if_exists=true)
admin.create_database("my_db", descriptor, true).await?;
println!("Database created successfully");
```

### Creating a Table

```rust
use fluss_client::metadata::{Schema, TableDescriptor};
use fluss_client::types::DataTypes;

// Define schema
let schema = Schema::builder()
    .column("id", DataTypes::STRING())
    .column("age", DataTypes::INT())
    .column("created_at", DataTypes::TIMESTAMP())
    .column("is_active", DataTypes::BOOLEAN())
    .primary_key(vec!["id"])
    .build();

// Create table descriptor
let table_descriptor = TableDescriptor::builder()
    .schema(schema)
    .distributed_by(1, vec!["id"])
    .build();

// Create table
let table_path = "my_db.user_table";
admin.create_table(table_path, table_descriptor, false).await?;

// Get table info
let table_info = admin.get_table_info(table_path).await?;
println!("Table Info: {:?}", table_info);
```

## Table API

### Writers

To write data, first obtain a `Table` instance. Fluss supports `UpsertWriter` for Primary Key tables and `AppendWriter` for Log tables.

```rust
let table = connection.get_table("my_db.user_table").await?;
```

#### Writing to a Primary Key Table

```rust
use fluss_client::row::Row;
use fluss_client::types::{Timestamp, TimestampNtz};
use std::time::SystemTime;

// Create writer
let mut writer = table.new_upsert().create_writer().await?;

// Prepare data
// Note: Data must be passed as a Row object matching the schema
let row1 = Row::new()
    .set("id", "1")
    .set("age", 20)
    .set("created_at", TimestampNtz::from(SystemTime::now()))
    .set("is_active", true);

let row2 = Row::new()
    .set("id", "2")
    .set("age", 22)
    .set("created_at", TimestampNtz::from(SystemTime::now()))
    .set("is_active", true);

// Upsert data
writer.upsert(row1).await?;
writer.upsert(row2).await?;

// Flush to ensure data is sent
writer.flush().await?;
```

#### Writing to a Log Table

```rust
// Create append writer
let mut writer = table.new_append().create_writer().await?;

// Append data
let row = Row::new()
    .set("user_id", "user_log_1")
    .set("event", "login_event");
    
writer.append(row).await?;
writer.flush().await?;
```

### Scanner

To read data, create a `LogScanner` and subscribe to buckets.

```rust
use fluss_client::scanner::ScanRecord;
use std::time::Duration;

// Create scanner
let mut scanner = table.new_scan().create_log_scanner().await?;

// Subscribe to all buckets from the beginning
let num_buckets = table.get_table_info().num_buckets();
for i in 0..num_buckets {
    scanner.subscribe_from_beginning(i);
}

// Poll for records
loop {
    let scan_records = scanner.poll(Duration::from_millis(1000)).await?;
    for bucket in scan_records.buckets() {
        let records = scan_records.records(bucket);
        for record in records {
            let row = record.get_row();
            println!("Received row: {:?}", row);
        }
    }
}
```

### Lookup

You can perform key-based lookups on Primary Key tables.

```rust
// Create lookuper
let lookuper = table.new_lookup().create_lookuper().await?;

// Lookup by key
// Key must be passed as a Row with PK columns
let key = Row::new().set("id", "1");
let result_row = lookuper.lookup(key).await?;

match result_row {
    Some(row) => println!("Found row: {:?}", row),
    None => println!("Row not found"),
}
```

## Type Mapping

The Rust client maps Fluss types to Rust types as follows:

| Fluss Type | Rust Type |
|---|---|
| INT | i32 |
| BIGINT | i64 |
| STRING | String |
| BOOLEAN | bool |
| FLOAT | f32 |
| DOUBLE | f64 |
| DECIMAL | rust_decimal::Decimal |
| DATE | chrono::NaiveDate |
| TIME | chrono::NaiveTime |
| TIMESTAMP | chrono::NaiveDateTime |
| TIMESTAMP_LTZ | `chrono::DateTime<Utc>` |
| BINARY / BYTES | `Vec<u8>` |
