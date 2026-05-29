---
sidebar_position: 5
---
# Primary Key Tables

Primary key tables (KV tables) support upsert, delete, and lookup operations.

## Creating a Primary Key Table

```rust
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};

let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("age", DataTypes::bigint())
            .primary_key(vec!["id"])
            .build()?,
    )
    .build()?;

let table_path = TablePath::new("fluss", "users");
admin.create_table(&table_path, &table_descriptor, true).await?;
```

## Upserting Records

```rust
use fluss::row::{GenericRow, InternalRow};

let table = conn.get_table(&table_path).await?;
let table_upsert = table.new_upsert()?;
let upsert_writer = table_upsert.create_writer()?;

for (id, name, age) in [(1, "Alice", 25i64), (2, "Bob", 30), (3, "Charlie", 35)] {
    let mut row = GenericRow::new(3);
    row.set_field(0, id);
    row.set_field(1, name);
    row.set_field(2, age);
    upsert_writer.upsert(&row)?;
}
upsert_writer.flush().await?;
```

## Updating Records

Upsert with the same primary key to update an existing record.

```rust
let mut row = GenericRow::new(3);
row.set_field(0, 1);        // id (primary key)
row.set_field(1, "Alice");
row.set_field(2, 26i64);    // updated age

upsert_writer.upsert(&row)?;
upsert_writer.flush().await?;
```

## Deleting Records

```rust
// Only primary key field needs to be set
let mut row = GenericRow::new(3);
row.set_field(0, 2);  // id of record to delete

upsert_writer.delete(&row)?;
upsert_writer.flush().await?;
```

## Partial Updates

Update only specific columns while preserving others.

```rust
// By column indices
let partial_upsert = table_upsert.partial_update(Some(vec![0, 2]))?;
let partial_writer = partial_upsert.create_writer()?;

let mut row = GenericRow::new(3);
row.set_field(0, 1);       // id (primary key, required)
row.set_field(2, 27i64);   // age (will be updated)
// name will remain unchanged

partial_writer.upsert(&row)?;
partial_writer.flush().await?;

// By column names
let partial_upsert = table_upsert.partial_update_with_column_names(&["id", "age"])?;
let partial_writer = partial_upsert.create_writer()?;
```

## Looking Up Records

```rust
let mut lookuper = table.new_lookup()?.create_lookuper()?;

let mut key = GenericRow::new(1);
key.set_field(0, 1);  // id to lookup

let result = lookuper.lookup(&key).await?;

if let Some(row) = result.get_single_row()? {
    println!(
        "Found: id={}, name={}, age={}",
        row.get_int(0)?,
        row.get_string(1)?,
        row.get_long(2)?
    );
} else {
    println!("Record not found");
}
```
## Looking Up Records as Arrow RecordBatch

Use `to_record_batch()` to get lookup results in Arrow format, for example when integrating with DataFusion.
```rust
let result = lookuper.lookup(&key).await?;
let batch = result.to_record_batch()?;
println!("Rows: {}", batch.num_rows());
```

## Subscribing to the Changelog (CDC)

Every primary key table maintains a changelog of its row-level changes. Read it
with a record-mode `LogScanner` — the same API used for [log tables](./log-tables.md) —
to stream CDC events. Each `ScanRecord` carries a `ChangeType`:

- `+I` — a new key was inserted
- `-U` / `+U` — the old and new images of an updated key
- `-D` — a key was deleted (the record carries the old row)

```rust
use fluss::client::EARLIEST_OFFSET;
use std::time::Duration;

let table = conn.get_table(&table_path).await?;
let log_scanner = table.new_scan().create_log_scanner()?;

// Subscribe to every bucket from the start of the changelog.
let num_buckets = table.get_table_info().get_num_buckets();
for bucket in 0..num_buckets {
    log_scanner.subscribe(bucket, EARLIEST_OFFSET).await?;
}

loop {
    let records = log_scanner.poll(Duration::from_secs(1)).await?;
    for record in records {
        let row = record.row();
        println!(
            "{} id={} name={}",
            record.change_type().short_string(), // +I / -U / +U / -D
            row.get_int(0)?,
            row.get_string(1)?,
        );
    }
}
```

With the default changelog mode (`'table.changelog.image' = 'FULL'`), updating a
key emits a `-U`/`+U` pair and deleting it emits `-D`; the `WAL` mode emits only
`+U` on update. To resume from a previously committed position rather than the
start, `subscribe` at a specific offset instead of `EARLIEST_OFFSET`.

## Prefix Lookup

To fetch all rows sharing a common primary-key prefix (by choosing a bucket key that's a strict prefix of the primary key), see [Prefix Lookup](./prefix-lookup.md).

## Limit Scan

To read up to `n` rows of a bucket's current state without supplying keys, use a batch scanner. The server returns the deduplicated current rows as Arrow batches, which is convenient for previews or DataFusion sources.

```rust
let bucket = TableBucket::new(table.get_table_info().table_id, 0);
let mut scanner = table.new_scan().limit(10)?.create_bucket_batch_scanner(bucket)?;

while let Some(batch) = scanner.next_batch().await? {
    println!("rows: {}", batch.batch().num_rows());
}
```

Limit applies per bucket; scan each bucket to cover a multi-bucket table.
