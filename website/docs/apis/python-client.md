---
title: "Python Client"
sidebar_position: 3
---

# Fluss Python Client

The Fluss Python Client provides a high-performance, asynchronous interface for
interacting with Fluss clusters. Built on top of the Rust core via
[PyO3](https://pyo3.rs/), it leverages PyArrow for efficient data interchange
and supports idiomatic integration with Pandas.

The client provides two main APIs:

- **[Admin API](https://clients.fluss.apache.org/user-guide/python/api-reference#flussadmin)**: For managing databases, tables, and partitions.
- **[Table API](https://clients.fluss.apache.org/user-guide/python/api-reference#flusstable)**: For reading and writing to Log and Primary Key tables

## Installation
```bash
pip install pyfluss
```

## Quick Example
```python
import asyncio
import fluss

async def main():
    config = fluss.Config({"bootstrap.servers": "127.0.0.1:9123"})
    conn = await fluss.FlussConnection.create(config)
    async with conn:
        admin = await conn.get_admin()
        databases = await admin.list_databases()
        print(f"Available databases: {databases}")

if __name__ == "__main__":
    asyncio.run(main())
```
For more examples, see [Fluss Python Client documentation](https://clients.fluss.apache.org/user-guide/python/example/).

## Full Documentation

For the complete Python client reference including all configuration options,
API methods, data types, error handling, and worked examples — see the
**[Fluss Python Client documentation](https://clients.fluss.apache.org/user-guide/python/installation)**.