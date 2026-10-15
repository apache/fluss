---
sidebar_position: 7
---
# Prefix Lookup

Prefix lookup retrieves all rows whose primary key starts with a given set of key values. For example, a table keyed by `(user_id, session_id, event_seq)` can return all events for one user session without scanning the table.

## Table Requirements

- The table must have a primary key.
- The bucket key must be a strict prefix of the primary key. For partitioned tables, it must be a strict prefix of the primary key after removing partition columns.
- The columns passed to `lookup_by()` must equal the bucket keys. For partitioned tables, pass the partition keys followed by the bucket keys, in that order.
- If the bucket key equals the full primary key, use a [primary-key lookup](./primary-key-tables.md#upsert-delete-lookup) instead.

Invalid lookup configurations raise `fluss.FlussError` when constructing the lookup.

## Non-Partitioned Table

Set the primary key to `(user_id, session_id, event_seq)` and the bucket key to `(user_id, session_id)`. Events for the same user session are routed to the same bucket.

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
descriptor = fluss.TableDescriptor(
    schema, bucket_count=3, bucket_keys=["user_id", "session_id"]
)
table_path = fluss.TablePath("fluss", "example_prefix_lookup")
admin = conn.get_admin()
await admin.create_table(table_path, descriptor, ignore_if_exists=True)
table = await conn.get_table(table_path)
```

Here, `conn` is an open connection as shown in the [connection example](./index.md).

### Writing and Looking Up Session Events

Flush the writer before looking up the data so that the server has acknowledged all writes.

```python
writer = table.new_upsert().create_writer()
for user_id, session_id, event_seq, event_data in [
    (1, "sess-a", 1, "open"),
    (1, "sess-a", 2, "click"),
    (1, "sess-a", 3, "close"),
    (1, "sess-b", 1, "open"),
    (2, "sess-c", 1, "open"),
]:
    writer.upsert({
        "user_id": user_id,
        "session_id": session_id,
        "event_seq": event_seq,
        "event_data": event_data,
    })
await writer.flush()

lookuper = (
    table.new_lookup().lookup_by(["user_id", "session_id"]).create_lookuper()
)
events = await lookuper.lookup({"user_id": 1, "session_id": "sess-a"})
for event in events:
    print(f"seq={event['event_seq']}, data={event['event_data']}")
```

This returns the three events for user `1`'s session `sess-a`, excluding that user's session `sess-b` and user `2`'s session `sess-c`. Reuse the lookuper for subsequent requests.

Prefix lookup returns a **list of full-row dictionaries**, with an empty list when there are no matches. Primary-key lookup returns a single dictionary or `None`.

```python
assert await lookuper.lookup({"user_id": 2, "session_id": "sess-missing"}) == []
```

You can also pass a list or tuple of prefix values in the column order supplied to `lookup_by()`:

```python
events = await lookuper.lookup([1, "sess-b"])
events = await lookuper.lookup((2, "sess-c"))
```

## Partitioned Table

On a [partitioned table](./partitioned-tables.md), partition columns are removed from the primary key before checking the bucket-prefix rule. Include the partition values in the lookup so the client can route the request to the right partition.

For example, add `region` as the partition key and keep `(user_id, session_id)` as the bucket key:

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
descriptor = fluss.TableDescriptor(
    schema,
    partition_keys=["region"],
    bucket_count=3,
    bucket_keys=["user_id", "session_id"],
)
table_path = fluss.TablePath("fluss", "example_partitioned_prefix_lookup")
await admin.create_table(table_path, descriptor, ignore_if_exists=True)
table = await conn.get_table(table_path)
```

The lookup columns are the partition keys followed by the bucket keys:

```python
lookuper = (
    table.new_lookup()
    .lookup_by(["region", "user_id", "session_id"])
    .create_lookuper()
)
events = await lookuper.lookup({
    "region": "US", "user_id": 1, "session_id": "sess-a"
})
for event in events:
    print(f"seq={event['event_seq']}, data={event['event_data']}")
```

The result is empty until matching events have been written to that partition.

## Running the Example

The complete script, including connection setup, result assertions, and table cleanup, is `fluss-rust/bindings/python/example/prefix_lookup.py`. From `fluss-rust/bindings/python`, run:

```bash
uv run python example/prefix_lookup.py

# Use a specific cluster:
FLUSS_BOOTSTRAP_SERVERS=host:port uv run python example/prefix_lookup.py
```

The default address is `127.0.0.1:9123`. The script recreates `fluss.example_prefix_lookup` and drops it when finished. CI runs the script against a test cluster through `test/test_examples.py`.
