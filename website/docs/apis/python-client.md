---
title: "Python Client"
sidebar_position: 3
---

# Fluss Python Client

## Overview
The Fluss Python Client provides an interface for interacting with Fluss clusters. It supports asynchronous operations for managing resources and handling data.

The client provides two main APIs:
* **Admin API**: For managing databases, tables, partitions, and retrieving metadata.
* **Table API**: For reading from and writing to Fluss tables.

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

## Initialization

The `Connection` object is the entry point for interacting with Fluss. It is created using `Connection.create()` and requires a configuration dictionary.

```python
from fluss.client import Connection
from fluss.config import Configuration

# Create configuration
conf = Configuration()
# Adjust according to where your Fluss cluster is running
conf.set_string("bootstrap.servers", "localhost:9123")

# Create connection
connection = Connection(conf)

# Obtain Admin instance
admin = connection.get_admin()
databases = admin.list_databases()
print(databases)

# Obtain Table instance
table = connection.get_table("my_db.my_table")
print(table.get_table_info())
```

### SASL Authentication
If your Fluss cluster uses SASL authentication, configure the security properties:

```python
conf = Configuration()
conf.set_string("bootstrap.servers", "localhost:9123")
conf.set_string("client.security.protocol", "sasl")
conf.set_string("client.security.sasl.mechanism", "PLAIN")
conf.set_string("client.security.sasl.username", "alice")
conf.set_string("client.security.sasl.password", "alice-secret")

connection = Connection(conf)
```

## Admin API

The `Admin` API allows you to manage databases and tables.

### Creating a Database

```python
from fluss.client.admin import DatabaseDescriptor

# Create database descriptor
descriptor = DatabaseDescriptor()
descriptor.comment = "This is a test database"
descriptor.add_custom_property("owner", "data-team")

# Create database (ignore_if_exists=True)
admin.create_database("my_db", descriptor, True)
```

### Creating a Table

```python
from fluss.types import DataTypes
from fluss.metadata import Schema, TableDescriptor

# Define schema
schema = Schema.new_builder() \
    .column("id", DataTypes.STRING()) \
    .column("age", DataTypes.INT()) \
    .column("created_at", DataTypes.TIMESTAMP()) \
    .column("is_active", DataTypes.BOOLEAN()) \
    .primary_key(["id"]) \
    .build()

# Create table descriptor
table_descriptor = TableDescriptor.builder() \
    .schema(schema) \
    .distributed_by(1, ["id"]) \
    .build()

# Create table
table_path = "my_db.user_table"
admin.create_table(table_path, table_descriptor, False)

# Get table info
table_info = admin.get_table_info(table_path)
print(table_info)
```

## Table API

### Writers

To write data, first obtain a `Table` instance. Fluss supports `UpsertWriter` for Primary Key tables and `AppendWriter` for Log tables.

```python
table = connection.get_table("my_db.user_table")
```

#### Writing to a Primary Key Table

```python
from datetime import datetime

# Create writer
writer = table.new_upsert().create_writer()

# Prepare data as a list of values matching the schema
row1 = ["1", 20, datetime.now(), True]
row2 = ["2", 22, datetime.now(), True]

# Upsert data and flush
writer.upsert(row1)
writer.upsert(row2)

# Flush to ensure data is sent
writer.flush()
```

#### Writing to a Log Table

```python
# Create append writer
writer = table.new_append().create_writer()

# Append data
writer.append(["user_log_1", "login_event"])
writer.flush()
```

### Scanner

To read data, create a `LogScanner` and subscribe to buckets.

```python
import time

# Create scanner
scanner = table.new_scan().create_log_scanner()

# Subscribe to all buckets from the beginning
num_buckets = table.get_table_info().num_buckets
for i in range(num_buckets):
    scanner.subscribe_from_beginning(i)

# Poll for records
while True:
    scan_records = scanner.poll(1000) # timeout in ms
    for bucket in scan_records.buckets():
        for record in scan_records.records(bucket):
            row = record.get_row()
            print(f"Received row: {row}")
    time.sleep(0.1)
```

### Lookup

You can perform key-based lookups on Primary Key tables.

```python
# Create lookuper
lookuper = table.new_lookup().create_lookuper()

# Lookup by key
key = ["1"] 
result_row = lookuper.lookup(key)

if result_row:
    print(f"Found row: {result_row}")
else:
    print("Row not found")
```

## Type Mapping

The Python client maps Fluss types to Python types as follows:

| Fluss Type | Python Type |
|---|---|
| INT | int |
| BIGINT | int |
| STRING | str |
| BOOLEAN | bool |
| FLOAT | float |
| DOUBLE | float |
| DECIMAL | decimal.Decimal |
| DATE | datetime.date |
| TIME | datetime.time |
| TIMESTAMP | datetime.datetime |
| TIMESTAMP_LTZ | datetime.datetime |
| BINARY / BYTES | bytes |
