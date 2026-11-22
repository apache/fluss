---
sidebar_position: 3
---
# Admin Operations

## Get Admin Interface

```rust
let admin = conn.get_admin()?;
```

## Database Operations

```rust
// Create database
admin.create_database("my_database", None, true).await?;

// List all databases
let databases = admin.list_databases().await?;
println!("Databases: {:?}", databases);

// Check if database exists
let exists = admin.database_exists("my_database").await?;

// Get database information
let db_info = admin.get_database_info("my_database").await?;

// Drop database
admin.drop_database("my_database", true, false).await?;
```

## Table Operations

```rust
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};

let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .column("amount", DataTypes::bigint())
            .build()?,
    )
    .build()?;

let table_path = TablePath::new("my_database", "my_table");

// Create table
admin.create_table(&table_path, &table_descriptor, true).await?;

// Get table information
let table_info = admin.get_table_info(&table_path).await?;
println!("Table: {}", table_info);

// List tables in database
let tables = admin.list_tables("my_database").await?;

// Check if table exists
let exists = admin.table_exists(&table_path).await?;

// Drop table
admin.drop_table(&table_path, true).await?;
```

## Partition Operations

```rust
use fluss::metadata::PartitionSpec;
use std::collections::HashMap;

// List all partitions
let partitions = admin.list_partition_infos(&table_path).await?;

// List partitions matching a spec
let mut filter = HashMap::new();
filter.insert("year", "2024");
let spec = PartitionSpec::new(filter);
let partitions = admin.list_partition_infos_with_spec(&table_path, Some(&spec)).await?;

// Create partition
admin.create_partition(&table_path, &spec, true).await?;

// Drop partition
admin.drop_partition(&table_path, &spec, true).await?;
```

## Offset Operations

```rust
use fluss::rpc::message::OffsetSpec;

let bucket_ids = vec![0, 1, 2];

// Get earliest offsets
let earliest = admin.list_offsets(&table_path, &bucket_ids, OffsetSpec::Earliest).await?;

// Get latest offsets
let latest = admin.list_offsets(&table_path, &bucket_ids, OffsetSpec::Latest).await?;

// Get offsets for a specific timestamp
let timestamp_ms = 1704067200000; // 2024-01-01 00:00:00 UTC
let offsets = admin.list_offsets(
    &table_path, &bucket_ids, OffsetSpec::Timestamp(timestamp_ms),
).await?;

// Get offsets for a specific partition
let partition_offsets = admin.list_partition_offsets(
    &table_path, "partition_name", &bucket_ids, OffsetSpec::Latest,
).await?;
```

## Lake Snapshot

:::note
Lake snapshots require [lake integration](https://fluss.apache.org/docs/maintenance/tiered-storage/overview/) (e.g. Paimon or Iceberg) to be enabled on the server. Without it, `get_latest_lake_snapshot` will return an error.
:::

```rust
let snapshot = admin.get_latest_lake_snapshot(&table_path).await?;
println!("Snapshot ID: {}", snapshot.snapshot_id);
```
