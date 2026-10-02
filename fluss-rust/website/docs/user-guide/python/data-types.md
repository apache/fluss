---
sidebar_position: 3
---
# Data Types

The Python client uses PyArrow types for schema definitions:

| PyArrow Type                                    | Fluss Type                        | Python Type         |
|-------------------------------------------------|-----------------------------------|---------------------|
| `pa.bool_()`                                    | Boolean                           | `bool`              |
| `pa.int8()` / `int16()` / `int32()` / `int64()` | TinyInt / SmallInt / Int / BigInt | `int`               |
| `pa.float32()` / `float64()`                    | Float / Double                    | `float`             |
| `pa.string()`                                   | String                            | `str`               |
| `pa.binary()`                                   | Bytes                             | `bytes`             |
| `pa.binary(n)`                                  | Binary(n)                         | `bytes`             |
| `pa.date32()`                                   | Date                              | `datetime.date`     |
| `pa.time32("ms")`                               | Time                              | `datetime.time`     |
| `pa.timestamp("us")`                            | Timestamp (NTZ)                   | `datetime.datetime` |
| `pa.timestamp("us", tz="UTC")`                  | TimestampLTZ                      | `datetime.datetime` |
| `pa.decimal128(precision, scale)`               | Decimal                           | `decimal.Decimal`   |
| `pa.list_(type)`                                  | Array                             | `list`              |
| `pa.map_(key_type, value_type)`                   | Map                               | `list[(key, value)]`|
| `pa.struct([(name, type), ...])`                  | Row                               | `dict`              |

All Python native types (`date`, `time`, `datetime`, `Decimal`) work when appending rows via dicts.

## Nullability

PyArrow field nullability is preserved when constructing Fluss schemas. By default, fields are nullable. Use `nullable=False` on `pa.field()` to create a `NOT NULL` column:

```python
schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("name", pa.string()),          # nullable by default
])
fluss_schema = fluss.Schema(schema)
fluss_schema.get_column_types()  # ["int NOT NULL", "string"]
```

Primary key columns are automatically forced `NOT NULL` regardless of the PyArrow field setting.

For nested types, element nullability is also preserved:

```python
schema = pa.schema([
    pa.field("tags", pa.list_(pa.field("item", pa.string(), nullable=False))),
])
fluss_schema = fluss.Schema(schema)
fluss_schema.get_column_types()  # ["array<string NOT NULL>"]
```

## Writing Data

Rows can be dicts, lists, or tuples:

```python
from datetime import date, time, datetime
from decimal import Decimal

row = {
    "user_id": 1,
    "name": "Alice",
    "active": True,
    "score": 95.5,
    "balance": Decimal("1234.56"),
    "birth_date": date(1990, 3, 15),
    "login_time": time(9, 30, 0),
    "created_at": datetime(2024, 1, 1, 0, 0, 0),
    "nickname": None,  # null value
    "tags": ["active", "premium"],  # Array of strings
    "scores": [10, None, 30],       # Array with null values
}
handle = writer.append(row)
```

When a row is written as a **dict**, a nullable column may be omitted — it
defaults to `null`. A non-nullable column (including primary keys) must be
present, otherwise the write is rejected with a clear error. The same rule
applies to the fields of a `ROW` value.

Lists and tuples must have values in column order:

```python
row = [1, "Alice", True, 95.5, Decimal("1234.56"), date(1990, 3, 15), time(9, 30, 0), datetime(2024, 1, 1), None]
handle = writer.append(row)
```

## Reading Data

```python
records = await scanner.poll(timeout_ms=1000)
for record in records:
    row = record.row  # dict[str, Any]
    print(row["user_id"])     # int
    print(row["name"])        # str
    print(row["balance"])     # decimal.Decimal
    print(row["birth_date"])  # datetime.date
    print(row["created_at"])  # datetime.datetime

    if row["nickname"] is None:
        print("nickname is null")
```

## Complex Types (Array, Map, Row)

`ARRAY`, `MAP`, and `ROW` columns can be nested arbitrarily (for example
`array<map<string, row<...>>>`). On read they materialize to native Python
objects; on write they accept the shapes below:

| Fluss type  | Read-back value                | Write input accepted                        |
|-------------|--------------------------------|---------------------------------------------|
| `ARRAY<T>`  | `list`                         | `list` / `tuple`                            |
| `MAP<K, V>` | `list` of `(key, value)` tuples| `dict`, or a sequence of `(key, value)` pairs |
| `ROW<...>`  | `dict` keyed by field name     | `dict` (by name) or `list`/`tuple` (by position) |

The MAP read shape matches pyarrow's `MapArray.to_pylist()` (it preserves
duplicate keys and ordering); ROW matches `StructArray.to_pylist()`.

### Arrays

```python
schema = pa.schema([
    pa.field("id", pa.int32()),
    pa.field("tags", pa.list_(pa.string())),
    pa.field("matrix", pa.list_(pa.list_(pa.int32()))),  # nested
])
writer.append({"id": 1, "tags": ["a", "b"], "matrix": [[1, 2], [3, 4]]})

row = await lookuper.lookup({"id": 1})
row["tags"]    # ["a", "b"]
row["matrix"]  # [[1, 2], [3, 4]]
```

### Maps

Use `pa.map_(key_type, value_type)`. Write a `dict` or a list of
`(key, value)` pairs; reads return a list of `(key, value)` tuples (wrap with
`dict(...)` for keyed access). Map keys must be non-null.

```python
schema = pa.schema([
    pa.field("id", pa.int32()),
    pa.field("attrs", pa.map_(pa.string(), pa.int32())),
])
writer.append({"id": 1, "attrs": {"a": 1, "b": None}})       # dict input
# or a sequence of pairs: {"id": 2, "attrs": [("a", 1), ("b", None)]}

row = await lookuper.lookup({"id": 1})
row["attrs"]        # [("a", 1), ("b", None)]
dict(row["attrs"])  # {"a": 1, "b": None}
```

### Rows

Use `pa.struct([...])`. Write a `dict` keyed by field name (or a `list`/`tuple`
in field order); reads return a `dict`.

```python
schema = pa.schema([
    pa.field("id", pa.int32()),
    pa.field("profile", pa.struct([("age", pa.int32()), ("city", pa.string())])),
])
writer.append({"id": 1, "profile": {"age": 30, "city": "NYC"}})

row = await lookuper.lookup({"id": 1})
row["profile"]  # {"age": 30, "city": "NYC"}
```

### Constraints

`ARRAY`, `MAP`, and `ROW` may be used as row values and nested inside one
another, but not as primary-key or bucket-key columns — the server rejects
complex key types.

### Bulk (Arrow) reads

The per-row paths above (`append`/`upsert` and the record-based scanner's
`record.row` dict, point `lookup`) materialize each value into a Python object.
For high-throughput scans, prefer the Arrow path — a record-batch scanner's
`to_arrow()` / `poll_arrow()` returns nested columns as native pyarrow
`ListArray` / `MapArray` / `StructArray` with no per-element conversion.
