---
sidebar_position: 1
---
# Example

Minimal working examples: connect to Fluss, create a table, write data, and read it back.

```rust
use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::error::Result;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::row::{GenericRow, InternalRow};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Connect
    let mut config = Config::default();
    config.bootstrap_servers = "127.0.0.1:9123".to_string();
    let conn = FlussConnection::new(config).await?;
    let admin = conn.get_admin()?;

    // Create a log table
    let table_path = TablePath::new("fluss", "quickstart_rust");
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()?,
        )
        .build()?;
    admin.create_table(&table_path, &descriptor, true).await?;

    // Write
    let table = conn.get_table(&table_path).await?;
    let writer = table.new_append()?.create_writer()?;
    let mut row = GenericRow::new(2);
    row.set_field(0, 1);
    row.set_field(1, "hello");
    writer.append(&row)?;
    writer.flush().await?;

    // Read
    let scanner = table.new_scan().create_log_scanner()?;
    scanner.subscribe(0, 0).await?;
    let records = scanner.poll(Duration::from_secs(5)).await?;
    for record in records {
        let row = record.row();
        println!("id={}, name={}", row.get_int(0)?, row.get_string(1)?);
    }

    Ok(())
}
```
