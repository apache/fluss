---
sidebar_position: 3
---
# Admin Operations

```python
admin = conn.get_admin()
```

## Databases

```python
await admin.create_database("my_database", ignore_if_exists=True)
databases = await admin.list_databases()
exists = await admin.database_exists("my_database")
await admin.drop_database("my_database", ignore_if_not_exists=True, cascade=True)
```

## Tables

Schemas are defined using PyArrow and wrapped in `fluss.Schema`:

```python
import pyarrow as pa

schema = fluss.Schema(pa.schema([
    pa.field("id", pa.int32()),
    pa.field("name", pa.string()),
    pa.field("amount", pa.int64()),
]))

table_path = fluss.TablePath("my_database", "my_table")
await admin.create_table(table_path, fluss.TableDescriptor(schema), ignore_if_exists=True)

table_info = await admin.get_table_info(table_path)
tables = await admin.list_tables("my_database")
await admin.drop_table(table_path, ignore_if_not_exists=True)
```

### TableDescriptor Options

`TableDescriptor` accepts these optional parameters:

| Parameter           | Description                                                                         |
|---------------------|-------------------------------------------------------------------------------------|
| `partition_keys`    | Column names to partition by (e.g. `["region"]`)                                    |
| `bucket_count`      | Number of buckets (parallelism units) for the table                                 |
| `bucket_keys`       | Columns used to determine bucket assignment                                         |
| `comment`           | Table comment / description                                                         |
| `log_format`        | Log storage format: `"ARROW"` or `"INDEXED"`                                        |
| `kv_format`         | KV storage format for primary key tables: `"INDEXED"` or `"COMPACTED"`              |
| `properties`        | Table configuration properties as a dict (e.g. `{"table.replication.factor": "1"}`) |
| `custom_properties` | User-defined properties as a dict                                                   |

## Offsets

```python
# Latest offsets for buckets
offsets = await admin.list_offsets(table_path, bucket_ids=[0, 1], offset_spec=fluss.OffsetSpec.latest())

# By timestamp
offsets = await admin.list_offsets(table_path, bucket_ids=[0], offset_spec=fluss.OffsetSpec.timestamp(1704067200000))

# Per-partition offsets
offsets = await admin.list_partition_offsets(table_path, partition_name="US", bucket_ids=[0], offset_spec=fluss.OffsetSpec.latest())
```

## Lake Snapshot

:::note
Lake snapshots require [lake integration](https://fluss.apache.org/docs/maintenance/tiered-storage/overview/) (e.g. Paimon or Iceberg) to be enabled on the server. Without it, `get_latest_lake_snapshot` will raise an error.
:::

```python
snapshot = await admin.get_latest_lake_snapshot(table_path)
print(f"Snapshot ID: {snapshot.snapshot_id}")
print(f"Table buckets: {snapshot.get_table_buckets()}")

bucket = fluss.TableBucket(table_id=1, bucket=0)
offset = snapshot.get_bucket_offset(bucket)
```
