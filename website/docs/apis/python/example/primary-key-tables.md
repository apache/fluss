---
sidebar_position: 5
---
# Primary Key Tables

Primary key tables support upsert, delete, and point lookup operations.

## Creating a Primary Key Table

Pass `primary_keys` to `fluss.Schema`:

```python
import pyarrow as pa

schema = fluss.Schema(
    pa.schema([
        pa.field("id", pa.int32()),
        pa.field("name", pa.string()),
        pa.field("age", pa.int64()),
    ]),
    primary_keys=["id"],
)
table_path = fluss.TablePath("fluss", "users")
await admin.create_table(table_path, fluss.TableDescriptor(schema, bucket_count=3), ignore_if_exists=True)
```

## Upsert, Delete, Lookup

```python
table = await conn.get_table(table_path)

# Upsert (fire-and-forget, flush at the end)
writer = table.new_upsert().create_writer()
writer.upsert({"id": 1, "name": "Alice", "age": 25})
writer.upsert({"id": 2, "name": "Bob", "age": 30})
await writer.flush()

# Per-record acknowledgment (for read-after-write)
handle = writer.upsert({"id": 3, "name": "Charlie", "age": 35})
await handle.wait()

# Delete by primary key
handle = writer.delete({"id": 2})
await handle.wait()

# Lookup
lookuper = table.new_lookup().create_lookuper()
result = await lookuper.lookup({"id": 1})
if result:
    print(f"Found: name={result['name']}, age={result['age']}")
```

## Partial Updates

Update specific columns while preserving others:

```python
partial_writer = table.new_upsert().partial_update_by_name(["id", "age"]).create_writer()
partial_writer.upsert({"id": 1, "age": 27})  # only updates age
await partial_writer.flush()
```

## Subscribing to the Changelog (CDC)

Every primary key table maintains a changelog of its row-level changes. Read it with a record-mode scanner — the same API used for [log tables](./log-tables.md) — to stream CDC events. Each `ScanRecord` carries a `change_type`: `+I` (insert), `-U` / `+U` (the old and new images of an update), and `-D` (delete, carrying the old row).

```python
table = await conn.get_table(table_path)
scanner = await table.new_scan().create_log_scanner()

# Subscribe to every bucket from the start of the changelog.
num_buckets = (await admin.get_table_info(table_path)).num_buckets
scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

while True:
    records = await scanner.poll(3000)
    if records.is_empty():
        break
    for record in records:
        print(record.change_type.short_string(), record.row)  # +I / -U / +U / -D
```

With the default changelog mode (`'table.changelog.image' = 'FULL'`), updating a key emits a `-U`/`+U` pair and deleting it emits `-D`; the `WAL` mode emits only `+U` on update. To resume from a committed position instead of the start, `subscribe` at a specific offset.

## Limit Scan

To read up to `n` rows of a bucket's current state without supplying keys, use a batch scanner. The server returns the deduplicated current rows as Arrow batches — convenient for previews or DataFusion sources.

```python
bucket = fluss.TableBucket(table.get_table_info().table_id, 0)
scanner = table.new_scan().limit(10).create_bucket_batch_scanner(bucket)
arrow_table = await scanner.to_arrow()
```

Limit applies per bucket; scan each bucket to cover a multi-bucket table.
