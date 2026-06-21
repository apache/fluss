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
