---
sidebar_position: 6
---
# Partitioned Tables

Partitioned tables distribute data across partitions based on column values. A write to a partition that doesn't exist creates it, unless `writer.dynamic-create-partition.enabled` is `false`, in which case the write fails with `ErrorCode.PARTITION_NOT_EXISTS`. The server still validates a created partition, e.g. its values and, from Fluss 1.0 on, whether it is within an auto-partitioned table's retention; older servers create such a partition.

## Creating and Managing Partitions

```python
import pyarrow as pa

schema = fluss.Schema(pa.schema([
    pa.field("id", pa.int32()),
    pa.field("region", pa.string()),
    pa.field("value", pa.int64()),
]))

table_path = fluss.TablePath("fluss", "partitioned_events")
await admin.create_table(
    table_path,
    fluss.TableDescriptor(schema, partition_keys=["region"], bucket_count=1),
    ignore_if_exists=True,
)

# Create partitions
await admin.create_partition(table_path, {"region": "US"}, ignore_if_exists=True)
await admin.create_partition(table_path, {"region": "EU"}, ignore_if_exists=True)

# List partitions
partition_infos = await admin.list_partition_infos(table_path)
```

## Writing

Same as non-partitioned tables - include partition column values in each row. A missing partition is created on write by default.

```python
table = await conn.get_table(table_path)
writer = table.new_append().create_writer()
writer.append({"id": 1, "region": "US", "value": 100})
writer.append({"id": 2, "region": "EU", "value": 200})
await writer.flush()
```

## Reading

Use `subscribe_partition()` or `subscribe_partition_buckets()` instead of `subscribe()`:

```python
scanner = await table.new_scan().create_record_batch_log_scanner()

# Subscribe to individual partitions
for p in partition_infos:
    scanner.subscribe_partition(partition_id=p.partition_id, bucket_id=0, start_offset=fluss.EARLIEST_OFFSET)

# Or batch-subscribe
scanner.subscribe_partition_buckets({
    (p.partition_id, 0): fluss.EARLIEST_OFFSET for p in partition_infos
})

print(await scanner.to_pandas())
```

### Unsubscribing

To stop consuming from a specific partition bucket, use `unsubscribe_partition()`:

```python
scanner.unsubscribe_partition(partition_id=partition_infos[0].partition_id, bucket_id=0)
```

## Partitioned Primary Key Tables

Partition columns must be part of the primary key. A missing partition is created on upsert by default.

```python
schema = fluss.Schema(
    pa.schema([
        pa.field("user_id", pa.int32()),
        pa.field("region", pa.string()),
        pa.field("score", pa.int64()),
    ]),
    primary_keys=["user_id", "region"],
)

table_path = fluss.TablePath("fluss", "partitioned_users")
await admin.create_table(
    table_path,
    fluss.TableDescriptor(schema, partition_keys=["region"]),
    ignore_if_exists=True,
)

await admin.create_partition(table_path, {"region": "US"}, ignore_if_exists=True)

table = await conn.get_table(table_path)
writer = table.new_upsert().create_writer()
writer.upsert({"user_id": 1, "region": "US", "score": 1234})
await writer.flush()

# Lookup includes partition columns
lookuper = table.new_lookup().create_lookuper()
result = await lookuper.lookup({"user_id": 1, "region": "US"})
```
