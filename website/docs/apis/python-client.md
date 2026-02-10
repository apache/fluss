---
title: "Python Client"
sidebar_position: 3
---

# Fluss Python Client

## Overview
The Fluss Python Client provides a high-performance, asynchronous interface for interacting with Fluss clusters. Built on top of the Rust core via PyO3, it leverages PyArrow for efficient data interchange and supports idiomatic integration with Pandas.

The client provides two main APIs:
* **Admin API**: For managing databases, tables, partitions, and retrieving metadata.
* **Table API**: For high-performance data reading (via Scanners) and writing (via Append/Upsert writers).

## Installation
The Fluss Python client is currently provided as bindings within the Rust client repository. To install it from source:

```bash
git clone https://github.com/apache/fluss-rust.git
cd fluss-rust/bindings/python
pip install .
```
:::note
After installation, ensure you run your Python scripts or interpreter from a directory other than the fluss-rust/bindings/python source folder. This prevents Python from attempting to import the local source package instead of the installed binary.
:::

:::tip
For the full technical API reference, including low-level methods and constants, visit the [Python Bindings directory](https://github.com/apache/fluss-rust/tree/main/bindings/python).
:::

## Initialization

The FlussConnection object is the entry point for interacting with Fluss. It is asynchronous and supports Python's async with context manager.

```python
import asyncio
import fluss

async def main():
    # Configuration is passed as a dictionary
    config = fluss.Config({"bootstrap.servers": "127.0.0.1:9123"})
    
    # Connect to the cluster
    conn = await fluss.FlussConnection.connect(config)
    async with conn:
        # Obtain Admin instance
        admin = await conn.get_admin()
        
        # List databases
        databases = await admin.list_databases()
        print(f"Available databases: {databases}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Admin API

The FlussAdmin API allows you to manage the lifecycle of your data structures.

### Creating a Table

Schemas are defined using PyArrow types and wrapped in a fluss.Schema.

```python
import pyarrow as pa
import fluss

# Define schema
schema = fluss.Schema(pa.schema([
    pa.field("id", pa.int32()),
    pa.field("name", pa.string()),
    pa.field("score", pa.float32()),
]), primary_keys=["id"])

# Create table descriptor
# Properties must be passed via the 'properties' dictionary
descriptor = fluss.TableDescriptor(
    schema, 
    bucket_count=3,
    properties={"table.replication.factor": "1"}
)

table_path = fluss.TablePath("my_db", "user_table")
await admin.create_table(table_path, descriptor, ignore_if_exists=True)
```

## Table API

### Writers

Fluss supports UpsertWriter for Primary Key tables and AppendWriter for Log tables. Writers support both individual row appends and bulk writes (Arrow/Pandas).

```python
# Obtain a table instance
table_path = fluss.TablePath("my_db", "user_table")
table = await conn.get_table(table_path)

# For Primary Key Tables (Upsert)
writer = table.new_upsert()
writer.upsert({"id": 1, "name": "Alice", "score": 95.5})
await writer.flush()

# For Log Tables (Append)
append_writer = await table.new_append_writer()
append_writer.append({"id": 2, "name": "Bob", "score": 88.0})
await append_writer.flush()
```

### Scanner

To read data, create a scanner. You can subscribe to buckets using EARLIEST_OFFSET or fetch the LATEST_OFFSET via the Admin API.

```python
# Create a batch scanner for Arrow/Pandas support
scanner = await table.new_scan().create_batch_scanner()

# Subscribe to a bucket from the beginning
scanner.subscribe(bucket_id=0, start_offset=fluss.EARLIEST_OFFSET)

# Read data directly into a Pandas DataFrame
df = scanner.to_pandas()
print(df)
```

### Lookup

For Primary Key tables, you can perform point lookups.

```python
lookuper = table.new_lookup()
# Point lookups are asynchronous
result = await lookuper.lookup({"id": 1})

if result:
    print(f"Found row: {result}")
```

## Type Mapping

The Python client maps Fluss/Arrow types to Python native types as follows:

| PyArrow Type | Fluss Type | Python Type |
|---|---|---|
| `pa.int32()` / `int64()` | Int / BigInt | `int` |
| `pa.string()` | String | `str` |
| `pa.boolean()` | Boolean | `bool` |
| `pa.float32()` / `float64()` | Float / Double | `float` |
| `pa.decimal128()` | Decimal | `decimal.Decimal` |
| `pa.timestamp("us")` | Timestamp | `datetime.datetime` |
| `pa.binary()` | Bytes | `bytes` |~~
