---
sidebar_position: 4
---
# Log Tables

Log tables are append-only tables without primary keys, suitable for event streaming.

## Creating a Log Table

```rust
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};

let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("event_id", DataTypes::int())
            .column("event_type", DataTypes::string())
            .column("timestamp", DataTypes::bigint())
            .build()?,
    )
    .build()?;

let table_path = TablePath::new("fluss", "events");
admin.create_table(&table_path, &table_descriptor, true).await?;
```

## Writing to Log Tables

```rust
use fluss::row::{GenericRow, InternalRow};

let table = conn.get_table(&table_path).await?;
let append_writer = table.new_append()?.create_writer()?;

let mut row = GenericRow::new(3);
row.set_field(0, 1);                    // event_id
row.set_field(1, "user_login");         // event_type
row.set_field(2, 1704067200000i64);     // timestamp

append_writer.append(&row)?;
append_writer.flush().await?;
```

Write operations use a **fire-and-forget** pattern for efficient batching. Each call queues the write and returns a `WriteResultFuture` immediately. Call `flush()` to ensure all queued writes are sent to the server.

For per-record acknowledgment:

```rust
append_writer.append(&row)?.await?;
```

## Reading from Log Tables

```rust
use std::time::Duration;

let table = conn.get_table(&table_path).await?;
let log_scanner = table.new_scan().create_log_scanner()?;

// Subscribe to bucket 0 starting from offset 0
log_scanner.subscribe(0, 0).await?;

// Poll for records
let records = log_scanner.poll(Duration::from_secs(10)).await?;

// Per-bucket access
for (bucket, bucket_records) in records.records_by_buckets() {
    println!("Bucket {}: {} records", bucket.bucket_id(), bucket_records.len());
    for record in bucket_records {
        let row = record.row();
        println!(
            "  event_id={}, event_type={} @ offset={}",
            row.get_int(0)?,
            row.get_string(1)?,
            record.offset()
        );
    }
}

// Or flat iteration (consumes ScanRecords)
for record in records {
    let row = record.row();
    println!(
        "event_id={}, event_type={}, timestamp={} @ offset={}",
        row.get_int(0)?,
        row.get_string(1)?,
        row.get_long(2)?,
        record.offset()
    );
}
```

**Subscribe from special offsets:**

```rust
use fluss::client::EARLIEST_OFFSET;

log_scanner.subscribe(0, EARLIEST_OFFSET).await?;  // from earliest
log_scanner.subscribe(0, 42).await?;                // from specific offset
```

**Subscribe from latest offset (only new records):**

To start reading only new records, first resolve the current latest offset via `list_offsets`, then subscribe at that offset:

```rust
use fluss::rpc::message::OffsetSpec;

let admin = conn.get_admin()?;
let offsets = admin.list_offsets(&table_path, &[0], OffsetSpec::Latest).await?;
let latest = offsets[&0];
log_scanner.subscribe(0, latest).await?;
```

**Subscribe to all buckets:**

```rust
let num_buckets = table.get_table_info().get_num_buckets();
for bucket_id in 0..num_buckets {
    log_scanner.subscribe(bucket_id, 0).await?;
}
```

**Subscribe to multiple buckets at once:**

```rust
use std::collections::HashMap;

let mut bucket_offsets = HashMap::new();
bucket_offsets.insert(0, 0i64);
bucket_offsets.insert(1, 100i64);
log_scanner.subscribe_buckets(&bucket_offsets).await?;
```

**Unsubscribe from a bucket:**

```rust
// Non-partitioned tables
log_scanner.unsubscribe(bucket_id).await?;

// Partitioned tables
log_scanner.unsubscribe_partition(partition_id, bucket_id).await?;
```

## Column Projection

```rust
// Project by column index
let scanner = table.new_scan().project(&[0, 2])?.create_log_scanner()?;

// Project by column name
let scanner = table.new_scan()
    .project_by_name(&["event_id", "timestamp"])?
    .create_log_scanner()?;
```

## Limit Scan

For a bounded read of up to `n` rows from a single bucket, use a batch scanner
instead of subscribing. It issues one request; poll it with `next_batch` until
it returns `None`.

```rust
let bucket = TableBucket::new(table.get_table_info().table_id, 0);
let mut scanner = table.new_scan().limit(10)?.create_bucket_batch_scanner(bucket)?;

while let Some(batch) = scanner.next_batch().await? {
    println!("rows: {}", batch.batch().num_rows());
}
```

Limit applies per bucket; scan each bucket to cover a multi-bucket table.
