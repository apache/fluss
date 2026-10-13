---
sidebar_position: 7
---
# Prefix Lookup

Prefix lookup retrieves all rows whose primary key starts with a given set of key values. For example, a table keyed by `(customer_id, order_id)` can return all orders for one customer without scanning the table.

## Table Requirements

- The table must have a primary key.
- The bucket key must be a strict prefix of the primary key. For partitioned tables, it must be a strict prefix of the primary key after removing partition columns.
- The columns passed to `lookup_by()` must equal the bucket keys. For partitioned tables, pass the partition keys followed by the bucket keys, in that order.
- If the bucket key equals the full primary key, use a [primary-key lookup](./primary-key-tables.md#upsert-delete-lookup) instead.

`create_lookuper()` validates these requirements and raises `fluss.FlussError` if they are not satisfied.

## Creating an Orders Table

Set the primary key to `(customer_id, order_id)` and the bucket key to `customer_id`. Orders for the same customer are routed to the same bucket.

```python
import pyarrow as pa
import fluss

schema = fluss.Schema(
    pa.schema([
        pa.field("customer_id", pa.int32()),
        pa.field("order_id", pa.int64()),
        pa.field("item", pa.string()),
    ]),
    primary_keys=["customer_id", "order_id"],
)
descriptor = fluss.TableDescriptor(
    schema, bucket_count=3, bucket_keys=["customer_id"]
)
table_path = fluss.TablePath("fluss", "example_prefix_lookup")
admin = conn.get_admin()
await admin.create_table(table_path, descriptor, ignore_if_exists=True)
table = await conn.get_table(table_path)
```

Here, `conn` is an open connection as shown in the [connection example](./index.md).

## Writing and Looking Up Orders

Flush the writer before looking up the data so that the server has acknowledged all writes.

```python
writer = table.new_upsert().create_writer()
writer.upsert({"customer_id": 1, "order_id": 102, "item": "Notebook"})
writer.upsert({"customer_id": 1, "order_id": 101, "item": "Keyboard"})
writer.upsert({"customer_id": 2, "order_id": 101, "item": "Mouse"})
await writer.flush()

lookuper = table.new_lookup().lookup_by(["customer_id"]).create_lookuper()
orders = await lookuper.lookup({"customer_id": 1})
for order in orders:
    print(f"order_id={order['order_id']}, item={order['item']}")
```

This returns both orders for customer `1`, excluding customer `2`'s order. Reuse the lookuper for subsequent requests.

Prefix lookup returns a **list of full-row dictionaries**, with an empty list when there are no matches. Primary-key lookup returns a single dictionary or `None`.

```python
assert await lookuper.lookup({"customer_id": 999}) == []
```

You can also pass a list or tuple of prefix values in the column order supplied to `lookup_by()`:

```python
orders = await lookuper.lookup([2])
orders = await lookuper.lookup((2,))
```

On a [partitioned table](./partitioned-tables.md), include the partition values in the lookup. For example, with partition key `region` and bucket key `customer_id`, use `lookup_by(["region", "customer_id"])` and `lookup({"region": "US", "customer_id": 1})`.

## Running the Example

The complete script, including connection setup, result assertions, and table cleanup, is `fluss-rust/bindings/python/example/prefix_lookup.py`. From `fluss-rust/bindings/python`, run:

```bash
uv run python example/prefix_lookup.py

# Use a specific cluster:
FLUSS_BOOTSTRAP_SERVERS=host:port uv run python example/prefix_lookup.py
```

The default address is `127.0.0.1:9123`. CI runs the script against a test cluster through `test/test_examples.py`.
