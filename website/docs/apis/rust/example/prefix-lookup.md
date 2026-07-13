---
sidebar_position: 7
---
# Prefix Lookup

Prefix lookup returns all rows whose primary key starts with a given prefix. It's enabled by choosing a **bucket key that is a strict prefix of the primary key** — rows sharing the same bucket-key prefix land in the same bucket, so one bucket lookup returns them all.

## Table Requirements

- The table must have a primary key.
- The bucket key must be a strict prefix of the primary key (on partitioned tables, of the *non-partition* portion of the primary key).
- The bucket key cannot equal the full primary key — that's a normal primary-key lookup, use [`Lookuper`](./primary-key-tables.md#looking-up-records) instead.
- The `lookup_by` columns passed to the client must equal `partition_keys ++ bucket_key` (in that order, if partitioned).

`create_lookuper()` validates these rules and returns `Err(Error::IllegalArgument { .. })` on mismatch, with a message describing the violation.

## Non-Partitioned Table

Pick a schema where the bucket key is a prefix of the primary key:

```rust
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};

let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("user_id", DataTypes::int())
            .column("session_id", DataTypes::string())
            .column("event_seq", DataTypes::bigint())
            .column("event_data", DataTypes::string())
            .primary_key(vec!["user_id", "session_id", "event_seq"])
            .build()?,
    )
    // Bucket key (user_id, session_id) is a prefix of the primary key.
    .distributed_by(Some(3), vec!["user_id".to_string(), "session_id".to_string()])
    .build()?;
```

Create the lookuper with `lookup_by(columns)` naming the prefix columns, then call `lookup(prefix_row)`:

```rust
use fluss::row::{GenericRow, InternalRow};

let mut prefix_lookuper = table
    .new_lookup()?
    .lookup_by(vec!["user_id".to_string(), "session_id".to_string()])
    .create_lookuper()?;

let mut prefix = GenericRow::new(2);
prefix.set_field(0, 1);                // user_id
prefix.set_field(1, "sess-a");         // session_id

let result = prefix_lookuper.lookup(&prefix).await?;
for row in result.get_rows()? {
    println!(
        "seq={}, data={}",
        row.get_long(2)?,
        row.get_string(3)?,
    );
}
```

Unlike primary-key lookup (which uses `get_single_row()`), prefix lookup returns zero or more rows via `get_rows()`.

## Partitioned Table

On a partitioned table, the partition columns are stripped from the primary key before the bucket-prefix rule is evaluated. The lookup key, though, must still carry the partition values so the client can route the request to the right partition — so the `lookup_by` columns are `partition_keys ++ bucket_key`.

```rust
let table_descriptor = TableDescriptor::builder()
    .schema(
        Schema::builder()
            .column("region", DataTypes::string())
            .column("user_id", DataTypes::int())
            .column("session_id", DataTypes::string())
            .column("event_seq", DataTypes::bigint())
            .column("event_data", DataTypes::string())
            .primary_key(vec!["region", "user_id", "session_id", "event_seq"])
            .build()?,
    )
    .partitioned_by(vec!["region"])
    // Bucket key (user_id, session_id) is a prefix of the pk minus partition cols.
    .distributed_by(Some(3), vec!["user_id".to_string(), "session_id".to_string()])
    .build()?;
```

```rust
let mut prefix_lookuper = table
    .new_lookup()?
    .lookup_by(vec![
        "region".to_string(),
        "user_id".to_string(),
        "session_id".to_string(),
    ])
    .create_lookuper()?;

let mut prefix = GenericRow::new(3);
prefix.set_field(0, "US");             // region (partition column)
prefix.set_field(1, 1);                // user_id
prefix.set_field(2, "sess-a");         // session_id

let result = prefix_lookuper.lookup(&prefix).await?;
for row in result.get_rows()? {
    println!(
        "seq={}, data={}",
        row.get_long(3)?,
        row.get_string(4)?,
    );
}
```
