---
sidebar_position: 3
---
# Data Types

| Fluss Type      | Rust Type      | Getter                               | Setter                         |
|-----------------|----------------|--------------------------------------|--------------------------------|
| `BOOLEAN`       | `bool`         | `get_boolean()`                      | `set_field(idx, bool)`         |
| `TINYINT`       | `i8`           | `get_byte()`                         | `set_field(idx, i8)`           |
| `SMALLINT`      | `i16`          | `get_short()`                        | `set_field(idx, i16)`          |
| `INT`           | `i32`          | `get_int()`                          | `set_field(idx, i32)`          |
| `BIGINT`        | `i64`          | `get_long()`                         | `set_field(idx, i64)`          |
| `FLOAT`         | `f32`          | `get_float()`                        | `set_field(idx, f32)`          |
| `DOUBLE`        | `f64`          | `get_double()`                       | `set_field(idx, f64)`          |
| `CHAR`          | `&str`         | `get_char(idx, length)`              | `set_field(idx, &str)`         |
| `STRING`        | `&str`         | `get_string()`                       | `set_field(idx, &str)`         |
| `DECIMAL`       | `Decimal`      | `get_decimal(idx, precision, scale)` | `set_field(idx, Decimal)`      |
| `DATE`          | `Date`         | `get_date()`                         | `set_field(idx, Date)`         |
| `TIME`          | `Time`         | `get_time()`                         | `set_field(idx, Time)`         |
| `TIMESTAMP`     | `TimestampNtz` | `get_timestamp_ntz(idx, precision)`  | `set_field(idx, TimestampNtz)` |
| `TIMESTAMP_LTZ` | `TimestampLtz` | `get_timestamp_ltz(idx, precision)`  | `set_field(idx, TimestampLtz)` |
| `BYTES`         | `&[u8]`        | `get_bytes()`                        | `set_field(idx, &[u8])`        |
| `BINARY(n)`     | `&[u8]`        | `get_binary(idx, length)`            | `set_field(idx, &[u8])`        |
| `ARRAY<T>`      | `FlussArray`   | `get_array()`                        | `set_field(idx, FlussArray)`   |
| `MAP<K, V>`     | `FlussMap`     | `get_map(idx)`                       | `set_field(idx, FlussMap)`     |

## Constructing Special Types

Primitive types (`bool`, `i8`, `i16`, `i32`, `i64`, `f32`, `f64`, `&str`, `&[u8]`) can be passed directly to `set_field`. The following types require explicit construction:

```rust
use fluss::row::{Date, Time, TimestampNtz, TimestampLtz, Decimal};

// Date: days since Unix epoch
let date = Date::new(19738);

// Time: milliseconds since midnight
let time = Time::new(43200000);

// Timestamp without timezone: milliseconds since epoch
// DataTypes::timestamp() defaults to precision 6 (microseconds).
// Use DataTypes::timestamp_with_precision(p) for a different precision (0–9).
let ts = TimestampNtz::new(1704067200000);

// Timestamp with local timezone: milliseconds since epoch
// DataTypes::timestamp_ltz() also defaults to precision 6.
let ts_ltz = TimestampLtz::new(1704067200000);

// Decimal: from an unscaled long value with precision and scale
let decimal = Decimal::from_unscaled_long(12345, 10, 2)?; // represents 123.45
```

## Creating Rows from Data

`GenericRow::from_data` accepts a `Vec<Datum>`. Because multiple crates implement `From<&str>`, Rust cannot infer the target type from `.into()` alone. Annotate the vector type explicitly:

```rust
use fluss::row::{Datum, GenericRow};

let data: Vec<Datum> = vec![1i32.into(), "hello".into(), Datum::Null];
let row = GenericRow::from_data(data);
```

## Arrays

Use `DataTypes::array(element_type)` in schema definitions. At runtime, read arrays with `row.get_array(idx)?`.

To construct array values for writes, build a `FlussArray` and wrap it with `Datum::Array`:

```rust
use fluss::metadata::DataTypes;
use fluss::row::binary_array::FlussArrayWriter;
use fluss::row::{Datum, GenericRow};

let mut writer = FlussArrayWriter::new(3, &DataTypes::int());
writer.write_int(0, 10);
writer.write_int(1, 20);
writer.set_null_at(2);
let arr = writer.complete()?;

let mut row = GenericRow::new(1);
row.set_field(0, Datum::Array(arr));
```

`ARRAY` is supported for row values and nested row fields. For key encoding, Rust follows Java parity: `ARRAY` can be encoded by the compacted key encoder, while table-level key constraints are validated by the server (which may reject unsupported key types).

## Maps

Use `DataTypes::map(key_type, value_type)` in schema definitions. At runtime, read maps with `row.get_map(idx)?` — the row knows its schema, so no extra type arguments are needed.

### Writing

Build a `FlussMap` entry-by-entry, then wrap it with `Datum::Map`:

```rust
use fluss::metadata::DataTypes;
use fluss::row::binary_map::FlussMapWriter;
use fluss::row::{Datum, GenericRow};

let mut writer = FlussMapWriter::new(2, &DataTypes::string(), &DataTypes::int());
writer.write_entry("key1".into(), 100.into())?;
writer.write_entry("key2".into(), Datum::Null)?;
let map = writer.complete()?;

let mut row = GenericRow::new(1);
row.set_field(0, Datum::Map(map));
```

For bulk writes from any iterator of `(key, value)` pairs (including a `HashMap`), use `extend`:

```rust
use std::collections::HashMap;

let entries: HashMap<&str, i32> = HashMap::from([("a", 1), ("b", 2)]);
let mut writer = FlussMapWriter::new(entries.len(), &DataTypes::string(), &DataTypes::int());
writer.extend(entries)?;
let map = writer.complete()?;
```

### Reading

The `entries()` iterator yields `(key, value)` pairs as schema-typed `Datum`s, folding the null check in:

```rust
use fluss::row::InternalRow;

let m = row.get_map(0)?;
for entry in m.entries() {
    let (k, v) = entry?;
    println!("{k:?} => {v:?}");          // Datum's Debug handles null
}
```

For point lookups, `get(&key)` does a linear scan and returns `Option<Datum>`:

```rust
use fluss::row::Datum;

if let Some(v) = m.get(&Datum::from("attr_size"))? {
    println!("size = {v:?}");
}
```

Lookup is `O(n)` — the binary MAP layout has no key index. If you need repeated lookups against the same map, collect the entries once:

```rust
use std::collections::HashMap;

let snapshot: HashMap<String, Datum<'_>> = m
    .entries()
    .map(|e| e.map(|(k, v)| (format!("{k:?}"), v)))
    .collect::<Result<_, _>>()?;
```

For raw access to the underlying parallel-array representation (zero-copy, used by serdes / Arrow adapters), `m.key_array()` and `m.value_array()` are still available.

### Constraints

`MAP` keys cannot be null. `MAP` is supported for row values and nested row fields. `MAP` cannot be used as a primary key or bucket key column — the Rust client rejects it at the compacted key encoder, and the Fluss server bans `MAP` (along with `ARRAY` and `ROW`) from key columns.

## Reading Row Data

```rust
use fluss::row::InternalRow;

for record in scan_records {
    let row = record.row();

    if row.is_null_at(0)? {
        // field is null
    }
    let id: i32 = row.get_int(0)?;
    let name: &str = row.get_string(1)?;
    let score: f32 = row.get_float(2)?;
    let date: Date = row.get_date(3)?;
    let ts: TimestampNtz = row.get_timestamp_ntz(4, 6)?;
    let decimal: Decimal = row.get_decimal(5, 10, 2)?;
}
```
