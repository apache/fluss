---
sidebar_position: 2
---
# Configuration

## Connection Setup

```python
import fluss

config = fluss.Config({"bootstrap.servers": "127.0.0.1:9123"})
conn = await fluss.FlussConnection.create(config)
```

The connection also supports async context managers:

```python
async with await fluss.FlussConnection.create(config) as conn:
    ...
```

## Connection Configurations

Configuration options can be set either via dict keys in the `Config()` constructor, or via Python property setters.

See the [`Config`](../api-reference.md#config) section in the API Reference for the full list of options, their config keys, and descriptions.

## SASL Authentication

To connect to a Fluss cluster with SASL/PLAIN authentication enabled:

```python
config = fluss.Config({
    "bootstrap.servers": "127.0.0.1:9123",
    "security.protocol": "sasl",
    "security.sasl.mechanism": "PLAIN",
    "security.sasl.username": "admin",
    "security.sasl.password": "admin-secret",
})
conn = await fluss.FlussConnection.create(config)
```

## Connection Lifecycle

Remember to close the connection when done:

```python
await conn.close()
```
