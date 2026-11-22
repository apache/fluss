---
sidebar_position: 7
---
# Prefix Lookup

Prefix lookup returns all rows whose primary key starts with a given prefix. It's enabled by choosing a **bucket key that is a strict prefix of the primary key** — rows sharing the same bucket-key prefix land in the same bucket, so one bucket lookup returns them all.

## Table Requirements

- The table must have a primary key.
- The bucket key must be a strict prefix of the primary key (on partitioned tables, of the *non-partition* portion of the primary key).
- The bucket key cannot equal the full primary key — that's a normal primary-key lookup, use [`Lookuper`](./primary-key-tables.md#upsert-delete-lookup) instead.
- The `lookup_by` columns passed to the client must equal `partition_keys ++ bucket_key` (in that order, if partitioned).

`create_lookuper()` validates the prefix rules and raises `fluss.FlussError` on mismatch, with a message describing the violation.

## Non-Partitioned Table

Pick a schema where the bucket key is a prefix of the primary key:

```python
import pyarrow as pa
import fluss

schema = fluss.Schema(
    pa.schema([
        pa.field("user_id", pa.int32()),
        pa.field("session_id", pa.string()),
        pa.field("event_seq", pa.int64()),
        pa.field("event_data", pa.string()),
    ]),
    primary_keys=["user_id", "session_id", "event_seq"],
)
# Bucket key (user_id, session_id) is a prefix of the primary key.
table_descriptor = fluss.TableDescriptor(
    schema, bucket_count=3, bucket_keys=["user_id", "session_id"]
)
```

Create the lookuper with `lookup_by(columns)` naming the prefix columns, then call `lookup(prefix_row)`:

```python
prefix_lookuper = (
    table.new_lookup()
    .lookup_by(["user_id", "session_id"])
    .create_lookuper()
)

prefix = {"user_id": 1, "session_id": "sess-a"}

result = await prefix_lookuper.lookup(prefix)
for row in result:
    print(f"seq={row['event_seq']}, data={row['event_data']}")
```

Unlike primary-key lookup (which returns a single row dictionary or `None`), prefix lookup returns zero or more rows as a list of full-row dictionaries.

## Partitioned Table

On a partitioned table, the partition columns are stripped from the primary key before the bucket-prefix rule is evaluated. The lookup key, though, must still carry the partition values so the client can route the request to the right partition — so the `lookup_by` columns are `partition_keys ++ bucket_key`.

```python
schema = fluss.Schema(
    pa.schema([
        pa.field("region", pa.string()),
        pa.field("user_id", pa.int32()),
        pa.field("session_id", pa.string()),
        pa.field("event_seq", pa.int64()),
        pa.field("event_data", pa.string()),
    ]),
    primary_keys=["region", "user_id", "session_id", "event_seq"],
)
# Bucket key (user_id, session_id) is a prefix of the pk minus partition cols.
table_descriptor = fluss.TableDescriptor(
    schema,
    partition_keys=["region"],
    bucket_count=3,
    bucket_keys=["user_id", "session_id"],
)
```

```python
prefix_lookuper = (
    table.new_lookup()
    .lookup_by(["region", "user_id", "session_id"])
    .create_lookuper()
)

prefix = {
    "region": "US",  # region (partition column)
    "user_id": 1,
    "session_id": "sess-a",
}

result = await prefix_lookuper.lookup(prefix)
for row in result:
    print(f"seq={row['event_seq']}, data={row['event_data']}")
```
