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
