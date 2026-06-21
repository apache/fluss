---
sidebar_position: 6
---
# Partitioned Tables

Partitioned tables distribute data across partitions based on partition column values, enabling efficient data organization and querying. Both log tables and primary key tables support partitioning.

## Partitioned Log Tables

### Creating a Partitioned Log Table

```rust
use fluss::metadata::{DataTypes, LogFormat, Schema, TableDescriptor, TablePath};

let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("event_id", DataTypes::int())
            .column("event_type", DataTypes::string())
            .column("dt", DataTypes::string())
            .column("region", DataTypes::string())
            .build()?,
    )
    .partitioned_by(vec!["dt", "region"])
    .log_format(LogFormat::ARROW)
    .build()?;

let table_path = TablePath::new("fluss", "partitioned_events");
admin.create_table(&table_path, &table_descriptor, true).await?;
```

### Writing to Partitioned Log Tables

**Partitions must exist before writing data, otherwise the client will by default retry indefinitely.** Include partition column values in each row, the client routes records to the correct partition automatically.

```rust
use fluss::metadata::PartitionSpec;
use std::collections::HashMap;

let table = conn.get_table(&table_path).await?;

// Create the partition before writing
let mut partition_values = HashMap::new();
partition_values.insert("dt", "2024-01-15");
partition_values.insert("region", "US");
admin.create_partition(&table_path, &PartitionSpec::new(partition_values), true).await?;

let append_writer = table.new_append()?.create_writer()?;

let mut row = GenericRow::new(4);
row.set_field(0, 1);              // event_id
row.set_field(1, "user_login");   // event_type
row.set_field(2, "2024-01-15");   // dt (partition column)
row.set_field(3, "US");           // region (partition column)

append_writer.append(&row)?;
append_writer.flush().await?;
```

### Reading from Partitioned Log Tables

For partitioned tables, use partition-aware subscribe methods.

```rust
use std::time::Duration;

let table = conn.get_table(&table_path).await?;
let admin = conn.get_admin()?;
let partitions = admin.list_partition_infos(&table_path).await?;

let log_scanner = table.new_scan().create_log_scanner()?;

// Subscribe to each partition's buckets
for partition_info in &partitions {
    let partition_id = partition_info.get_partition_id();
    let num_buckets = table.get_table_info().get_num_buckets();
    for bucket_id in 0..num_buckets {
        log_scanner.subscribe_partition(partition_id, bucket_id, 0).await?;
    }
}

let records = log_scanner.poll(Duration::from_secs(10)).await?;
for record in records {
    println!("Record: {:?}", record.row());
}
```

Subscribe to multiple partition-buckets at once:

```rust
use std::collections::HashMap;

let mut partition_bucket_offsets = HashMap::new();
partition_bucket_offsets.insert((partition_id, 0), 0i64);
partition_bucket_offsets.insert((partition_id, 1), 0i64);
log_scanner.subscribe_partition_buckets(&partition_bucket_offsets).await?;
```

### Managing Partitions

```rust
use fluss::metadata::PartitionSpec;
use std::collections::HashMap;

// Create a partition
let mut partition_values = HashMap::new();
partition_values.insert("dt", "2024-01-15");
partition_values.insert("region", "EMEA");
let spec = PartitionSpec::new(partition_values);
admin.create_partition(&table_path, &spec, true).await?;

// List all partitions
let partitions = admin.list_partition_infos(&table_path).await?;
for partition in &partitions {
    println!(
        "Partition: id={}, name={}",
        partition.get_partition_id(),
        partition.get_partition_name()
    );
}

// List with filter
let mut partial_values = HashMap::new();
partial_values.insert("dt", "2024-01-15");
let partial_spec = PartitionSpec::new(partial_values);
let filtered = admin.list_partition_infos_with_spec(
    &table_path, Some(&partial_spec),
).await?;

// Drop a partition
admin.drop_partition(&table_path, &spec, true).await?;
```

## Partitioned Primary Key Tables

Partitioned KV tables combine partitioning with primary key operations. Partition columns must be part of the primary key.

### Creating a Partitioned Primary Key Table

```rust
use fluss::metadata::{DataTypes, KvFormat, Schema, TableDescriptor, TablePath};

let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("user_id", DataTypes::int())
            .column("region", DataTypes::string())
            .column("zone", DataTypes::bigint())
            .column("score", DataTypes::bigint())
            .primary_key(vec!["user_id", "region", "zone"])
            .build()?,
    )
    .partitioned_by(vec!["region", "zone"])
    .kv_format(KvFormat::COMPACTED)
    .build()?;

let table_path = TablePath::new("fluss", "partitioned_users");
admin.create_table(&table_path, &table_descriptor, true).await?;
```

### Writing to Partitioned Primary Key Tables

**Partitions must exist before upserting data, otherwise the client will by default retry indefinitely.**

```rust
use fluss::metadata::PartitionSpec;
use std::collections::HashMap;

let table = conn.get_table(&table_path).await?;

// Create partitions first
for (region, zone) in [("APAC", "1"), ("EMEA", "2"), ("US", "3")] {
    let mut values = HashMap::new();
    values.insert("region", region);
    values.insert("zone", zone);
    admin.create_partition(&table_path, &PartitionSpec::new(values), true).await?;
}

let table_upsert = table.new_upsert()?;
let upsert_writer = table_upsert.create_writer()?;

for (user_id, region, zone, score) in [
    (1001, "APAC", 1i64, 1234i64),
    (1002, "EMEA", 2, 2234),
    (1003, "US", 3, 3234),
] {
    let mut row = GenericRow::new(4);
    row.set_field(0, user_id);
    row.set_field(1, region);
    row.set_field(2, zone);
    row.set_field(3, score);
    upsert_writer.upsert(&row)?;
}
upsert_writer.flush().await?;
```

### Looking Up Records in Partitioned Tables

Lookup requires all primary key columns including partition columns.

```rust
let mut lookuper = table.new_lookup()?.create_lookuper()?;

let mut key = GenericRow::new(3);
key.set_field(0, 1001);    // user_id
key.set_field(1, "APAC");  // region (partition column)
key.set_field(2, 1i64);    // zone (partition column)

let result = lookuper.lookup(&key).await?;
if let Some(row) = result.get_single_row()? {
    println!("Found: score={}", row.get_long(3)?);
}
```

### Prefix Lookup on Partitioned Tables

See [Prefix Lookup — Partitioned Table](./prefix-lookup.md#partitioned-table) for details and a full runnable example.

> **Note:** Scanning partitioned primary key tables is not supported. Use lookup operations instead.
