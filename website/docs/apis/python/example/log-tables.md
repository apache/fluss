---
sidebar_position: 4
---
# Log Tables

Log tables are append-only tables without primary keys, suitable for event streaming.

## Creating a Log Table

```python
import pyarrow as pa

schema = fluss.Schema(pa.schema([
    pa.field("id", pa.int32()),
    pa.field("name", pa.string()),
    pa.field("score", pa.float32()),
]))

table_path = fluss.TablePath("fluss", "events")
await admin.create_table(table_path, fluss.TableDescriptor(schema), ignore_if_exists=True)
```

## Writing

Rows can be appended as dicts, lists, or tuples. For bulk writes, use `write_arrow()`, `write_arrow_batch()`, or `write_pandas()`.

Write methods like `append()` and `write_arrow_batch()` return a `WriteResultHandle`. You can ignore it for fire-and-forget semantics (flush at the end), or `await handle.wait()` to block until the server acknowledges that specific write.

```python
table = await conn.get_table(table_path)
writer = table.new_append().create_writer()

# Fire-and-forget: queue writes, flush at the end
writer.append({"id": 1, "name": "Alice", "score": 95.5})
writer.append([2, "Bob", 87.0])
await writer.flush()

# Per-record acknowledgment
handle = writer.append({"id": 3, "name": "Charlie", "score": 91.0})
await handle.wait()

# Bulk writes
writer.write_arrow(pa_table)          # PyArrow Table
writer.write_arrow_batch(record_batch) # PyArrow RecordBatch
writer.write_pandas(df)                # Pandas DataFrame
await writer.flush()
```

## Reading

There are two scanner types:
- **Batch scanner** (`create_record_batch_log_scanner()`): returns Arrow Tables or DataFrames, best for analytics
- **Record scanner** (`create_log_scanner()`): returns individual records with metadata (offset, timestamp, change type), best for streaming

And two reading modes:
- **`to_arrow()` / `to_pandas()`**: reads all data from subscribed buckets up to the current latest offset, then returns. Best for one-shot batch reads.
- **`poll_arrow()` / `poll()` / `poll_record_batch()`**: returns whatever data is available within the timeout, then returns. Call in a loop for continuous streaming.

### Batch Read (One-Shot)

```python
num_buckets = (await admin.get_table_info(table_path)).num_buckets

scanner = await table.new_scan().create_record_batch_log_scanner()
scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

# Reads everything up to current latest offset, then returns
arrow_table = await scanner.to_arrow()
df = await scanner.to_pandas()
```

### Continuous Polling

Use `poll_arrow()` or `poll()` in a loop for streaming consumption:

```python
# Batch scanner: poll as Arrow Tables
scanner = await table.new_scan().create_record_batch_log_scanner()
scanner.subscribe(bucket_id=0, start_offset=fluss.EARLIEST_OFFSET)

while True:
    result = await scanner.poll_arrow(timeout_ms=5000)
    if result.num_rows > 0:
        print(result.to_pandas())

# Record scanner: poll individual records
scanner = await table.new_scan().create_log_scanner()
scanner.subscribe_buckets({i: fluss.EARLIEST_OFFSET for i in range(num_buckets)})

while True:
    scan_records = await scanner.poll(timeout_ms=5000)

    for record in scan_records:
        print(f"offset={record.offset}, change={record.change_type.short_string()}, row={record.row}")

    # Or per-bucket access (dict-like)
    for bucket, records in scan_records.items():
        for record in records:
            print(f"bucket={bucket.bucket_id}, offset={record.offset}, row={record.row}")
```

### Unsubscribing

To stop consuming from a bucket, use `unsubscribe()`:

```python
scanner.unsubscribe(bucket_id=0)
```

### Subscribe from Latest Offset

To only consume new records (skip existing data), first resolve the current latest offset via `list_offsets`, then subscribe at that offset:

```python
admin = conn.get_admin()
offsets = await admin.list_offsets(table_path, [0], fluss.OffsetSpec.latest())
latest = offsets[0]

scanner = await table.new_scan().create_record_batch_log_scanner()
scanner.subscribe(bucket_id=0, start_offset=latest)
```

## Column Projection

```python
scanner = await table.new_scan().project([0, 2]).create_record_batch_log_scanner()
# or by name
scanner = await table.new_scan().project_by_name(["id", "score"]).create_record_batch_log_scanner()
```

## Filter Pushdown

A filter is pushed to the server, which uses each batch's statistics to skip
batches that cannot match. Enable statistics on the table first:

```python
descriptor = fluss.TableDescriptor(
    fluss.Schema(schema),
    properties={"table.statistics.columns": "*"},
)
```

Then build a predicate with `fluss.col(...)` and pass it to the scan:

```python
from fluss import col

predicate = (col("id") >= 200) & col("name").starts_with("high")
scanner = await table.new_scan().filter(predicate).create_log_scanner()
scanner.subscribe_buckets({0: fluss.EARLIEST_OFFSET})

records = await scanner.poll(1000)
# Pruning is batch-granular, so a kept batch can still contain rows that do
# not match. Re-apply the predicate on the rows you receive.
matching = [r.row for r in records if r.row["id"] >= 200]
```

Filters work with projection, including on a column that is not projected:

```python
scanner = await (
    table.new_scan()
    .project_by_name(["name"])
    .filter(col("id") >= 200)
    .create_log_scanner()
)
```

Null counts drive `is_null()` and `is_not_null()` (a comparison against `None`
is rejected), and `decimal.Decimal`,
`datetime.date`, `datetime.time` and `datetime.datetime` filter their matching
column types. See the [`col` reference](../api-reference.md#col) for the full
literal mapping.

## Limit Scan

For a bounded read of up to `n` rows from a single bucket, use a batch scanner instead of subscribing. It issues one request; poll it with `next_batch()` until it returns `None`.

```python
bucket = fluss.TableBucket(table.get_table_info().table_id, 0)
scanner = table.new_scan().limit(10).create_bucket_batch_scanner(bucket)

while (batch := await scanner.next_batch()) is not None:
    print(f"rows: {batch.batch.num_rows}")
```

`to_arrow()`, `to_pandas()`, and `collect_all_batches()` drain the scan in one call instead. Limit applies per bucket; scan each bucket to cover a multi-bucket table.
