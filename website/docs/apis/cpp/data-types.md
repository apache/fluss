---
sidebar_position: 3
---
# Data Types

## Schema DataTypes

| DataType                   | Description                                                    |
|----------------------------|----------------------------------------------------------------|
| `DataType::Boolean()`      | Boolean value                                                  |
| `DataType::TinyInt()`      | 8-bit signed integer                                           |
| `DataType::SmallInt()`     | 16-bit signed integer                                          |
| `DataType::Int()`          | 32-bit signed integer                                          |
| `DataType::BigInt()`       | 64-bit signed integer                                          |
| `DataType::Float()`        | 32-bit floating point                                          |
| `DataType::Double()`       | 64-bit floating point                                          |
| `DataType::String()`       | UTF-8 string                                                   |
| `DataType::Bytes()`        | Binary data                                                    |
| `DataType::Date()`         | Date (days since epoch)                                        |
| `DataType::Time()`         | Time (milliseconds since midnight)                             |
| `DataType::Timestamp()`    | Timestamp without timezone (default precision 6, microseconds) |
| `DataType::TimestampLtz()` | Timestamp with timezone (default precision 6, microseconds)    |
| `DataType::Decimal(p, s)`  | Decimal with precision and scale                               |
| `DataType::Array(element)` | Array of the given element type (supports nesting)             |
| `DataType::Map(key, value)`| Map of key/value types (either may itself be complex)          |
| `DataType::Row({{name, type}, ...})` | Row (struct) of named fields (types may be complex) |

`MAP` / `ROW` columns (and arrays nesting them) compose to any depth — see
[Declaring MAP and ROW columns](#declaring-map-and-row-columns).

## Nullability

All DataTypes are nullable by default. Use `.NotNull()` to create a `NOT NULL` type:

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("id", fluss::DataType::Int().NotNull())
    .AddColumn("name", fluss::DataType::String())          // nullable by default
    .Build();
```

Primary key columns are automatically forced `NOT NULL` regardless of the `DataType` setting.

For nested types, nullability is preserved at each array level and at the leaf element:

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("tags", fluss::DataType::Array(fluss::DataType::String().NotNull()))
    .AddColumn("ids", fluss::DataType::Array(fluss::DataType::Int()).NotNull())
    .AddColumn("nested", fluss::DataType::Array(
        fluss::DataType::Array(fluss::DataType::Int()).NotNull()))
    .Build();
// "tags":   ARRAY<STRING NOT NULL>         (outer nullable, elements NOT NULL)
// "ids":    ARRAY<INT> NOT NULL            (outer NOT NULL, elements nullable)
// "nested": ARRAY<ARRAY<INT> NOT NULL>     (outer nullable, inner array NOT NULL)
```

You can query nullability at runtime:

```cpp
auto info = table.GetTableInfo();
bool is_nullable = info.schema.columns[0].data_type.nullable();
```

## Declaring MAP and ROW columns

`MAP<K,V>` and `ROW<...>` columns are declared with the `DataType::Map` and
`DataType::Row` factories, composed like any other type — to any depth:

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("id", fluss::DataType::Int())
    .AddColumn("attrs", fluss::DataType::Map(fluss::DataType::String(),
                                             fluss::DataType::Int()))
    .AddColumn("profile", fluss::DataType::Row({
        {"seq",   fluss::DataType::Int()},
        {"label", fluss::DataType::String()},
    }))
    // arbitrarily nested, e.g. array<row<seq: int, attrs: map<string,int>>>:
    .AddColumn("events", fluss::DataType::Array(fluss::DataType::Row({
        {"seq",   fluss::DataType::Int()},
        {"attrs", fluss::DataType::Map(fluss::DataType::String(), fluss::DataType::Int())},
    })))
    .SetPrimaryKeys({"id"})
    .Build();
```

:::note Arrow escape hatch
If you already have an `arrow::Schema`, pass it directly with
`fluss::Schema::FromArrow(arrow_schema, /*primary_keys=*/{"id"})`. It's
equivalent — the native factories above lower to the same Arrow types
internally, without pulling Arrow into your code.
:::

## GenericRow Setters

`SetInt32` is used for `TinyInt`, `SmallInt`, and `Int` columns. For `TinyInt` and `SmallInt`, the value is validated at write time — an error is returned if it overflows the column's range (e.g., \[-128, 127\] for `TinyInt`, \[-32768, 32767\] for `SmallInt`).

```cpp
fluss::GenericRow row;
row.SetNull(0);
row.SetBool(1, true);
row.SetInt32(2, 42);
row.SetInt64(3, 1234567890L);
row.SetFloat32(4, 3.14f);
row.SetFloat64(5, 2.71828);
row.SetString(6, "hello");
row.SetBytes(7, {0x01, 0x02, 0x03});
```

### Array Columns

Array values are built element-by-element using `ArrayWriter`, then attached to the row via `SetArray`:

```cpp
fluss::ArrayWriter aw(3, fluss::DataType::Int());
aw.SetInt32(0, 10);
aw.SetInt32(1, 20);
aw.SetNull(2);
row.SetArray(8, std::move(aw));
```

For nested arrays (e.g., `ARRAY<ARRAY<INT>>`), build inner arrays first:

```cpp
fluss::ArrayWriter inner(2, fluss::DataType::Int());
inner.SetInt32(0, 1);
inner.SetInt32(1, 2);

fluss::ArrayWriter outer(1, fluss::DataType::Array(fluss::DataType::Int()));
outer.SetArray(0, std::move(inner));
row.SetArray(9, std::move(outer));
```

### Map Columns

Map values are built with `MapWriter`: stage each entry's key and value, then
`Commit()`. Keys cannot be null; an unset value defaults to null.

```cpp
fluss::MapWriter mw(2, fluss::DataType::String(), fluss::DataType::Int());
mw.SetKeyString("a"); mw.SetValueInt32(1); mw.Commit();
mw.SetKeyString("b"); mw.SetValueNull();  mw.Commit();
row.SetMap(10, std::move(mw));
```

When the key or value is itself a `ROW` / `MAP` / `ARRAY`, pass the compound type
natively and stage values with the compound setters (`SetValueRow` /
`SetValueMap` / `SetValueArray`):

```cpp
fluss::MapWriter mr(1, fluss::DataType::String(),
                    fluss::DataType::Row({{"seq", fluss::DataType::Int()}}));
mr.SetKeyString("k");
fluss::GenericRow v(1); v.SetInt32(0, 7);
mr.SetValueRow(std::move(v));
mr.Commit();
row.SetMap(11, std::move(mr));
```

### Row Columns

A `ROW` value is a nested, schema-less `GenericRow`, attached via `SetRow`:

```cpp
fluss::GenericRow nested(2);
nested.SetInt32(0, 7);
nested.SetString(1, "seven");
row.SetRow(12, std::move(nested));
```

## Name-Based Setters

When using `table.NewRow()`, you can set fields by column name. The setter automatically routes to the correct type based on the schema:

```cpp
auto row = table.NewRow();
row.Set("user_id", 1);
row.Set("name", "Alice");
row.Set("score", 95.5f);
row.Set("balance", "1234.56");   // decimal as string
row.Set("birth_date", fluss::Date::FromYMD(1990, 3, 15));
row.Set("login_time", fluss::Time::FromHMS(9, 30, 0));
row.Set("created_at", fluss::Timestamp::FromMillis(1700000000000));
row.Set("nickname", nullptr);    // set to null
```

## Reading Field Values

Field values are read through `RowView` (from scan results) and `LookupResult` (from lookups), not through `GenericRow`. Both provide the same getter interface with zero-copy access to string and bytes data.

`ScanRecord` is a value type — it can be freely copied, stored, and accumulated across multiple `Poll()` calls via reference counting.

:::note string_view Lifetime
`GetString()` returns `std::string_view` that borrows from the underlying data. The `string_view` is valid as long as any `ScanRecord` referencing the same poll result is alive. Copy to `std::string` if you need the value after all records are gone.
:::

```cpp
// ScanRecord is a value type — safe to store and accumulate:
std::vector<fluss::ScanRecord> all_records;
fluss::ScanRecords records;
scanner.Poll(5000, records);
for (const auto& rec : records) {
    all_records.push_back(rec);                    // safe! ref-counted
    auto name = rec.row.GetString(0);              // zero-copy string_view
    auto owned = std::string(rec.row.GetString(0)); // explicit copy when needed
}

// DON'T — string_view dangles after all records referencing the data are destroyed:
std::string_view dangling;
{
    fluss::ScanRecords records;
    scanner.Poll(5000, records);
    dangling = records[0].row.GetString(0);
}
// dangling is undefined behavior here — no ScanRecord keeps the data alive!
```

### From Scan Results (RowView)

```cpp
for (const auto& rec : records) {
    auto name = rec.row.GetString(1);          // zero-copy string_view
    float score = rec.row.GetFloat32(3);
    auto balance = rec.row.GetDecimalString(4); // std::string (already owned)
    fluss::Date date = rec.row.GetDate(5);
    fluss::Time time = rec.row.GetTime(6);
    fluss::Timestamp ts = rec.row.GetTimestamp(7);
}
```

### From Lookup Results (LookupResult)

```cpp
fluss::LookupResult result;
lookuper.Lookup(pk_row, result);
if (result.Found()) {
    auto name = result.GetString(1);  // zero-copy string_view
    int64_t age = result.GetInt64(2);
}
```

### Reading Complex Columns (ARRAY / MAP / ROW)

Complex columns are read through a single recursive `Value` handle returned by
`GetValue(idx)` / `GetValue(name)`. **Navigate** the structure with
`At` / `KeyAt` / `ValueAt` / `Field` (each returns a child `Value`); **read** a
leaf with the `Get*()` methods (the handle already points at one value, so no
index):

```cpp
// ARRAY<INT>
auto arr = rec.row.GetValue("ids");
for (size_t i = 0; i < arr.Size(); i++) {
    if (!arr.At(i).IsNull()) {
        int32_t v = arr.At(i).GetInt32();
    }
}

// MAP<STRING, INT> — entries addressed by position
auto m = rec.row.GetValue("attrs");
for (size_t i = 0; i < m.Size(); i++) {
    auto key = m.KeyAt(i).GetString();
    if (!m.ValueAt(i).IsNull()) {
        int32_t v = m.ValueAt(i).GetInt32();
    }
}

// ROW<seq: INT, label: STRING> — fields by index or name
auto r = rec.row.GetValue("profile");
int32_t seq = r.Field(0).GetInt32();
auto label = r.Field("label").GetString();

// Nesting is just chained navigation, with the same Get* leaf reads:
auto rows = rec.row.GetValue("arr_of_rows");   // ARRAY<ROW<...>>
int32_t first_seq = rows.At(0).Field("seq").GetInt32();
```

`GetValue` works the same on `RowView` (scan), `LookupResult`, and
`PrefixRowView`. A typed `Get*()` on a null or wrong-typed value throws.

## TypeId Enum

`TinyInt` and `SmallInt` values are widened to `int32_t` on read.

| TypeId          | C++ Type                                    | Getter                    |
|-----------------|---------------------------------------------|---------------------------|
| `Boolean`       | `bool`                                      | `GetBool(idx)`            |
| `TinyInt`       | `int32_t`                                   | `GetInt32(idx)`           |
| `SmallInt`      | `int32_t`                                   | `GetInt32(idx)`           |
| `Int`           | `int32_t`                                   | `GetInt32(idx)`           |
| `BigInt`        | `int64_t`                                   | `GetInt64(idx)`           |
| `Float`         | `float`                                     | `GetFloat32(idx)`         |
| `Double`        | `double`                                    | `GetFloat64(idx)`         |
| `String`        | `std::string_view`                          | `GetString(idx)`          |
| `Bytes`         | `std::pair<const uint8_t*, size_t>`         | `GetBytes(idx)`           |
| `Date`          | `Date`                                      | `GetDate(idx)`            |
| `Time`          | `Time`                                      | `GetTime(idx)`            |
| `Timestamp`     | `Timestamp`                                 | `GetTimestamp(idx)`       |
| `TimestampLtz`  | `Timestamp`                                 | `GetTimestamp(idx)`       |
| `Decimal`       | `std::string`                               | `GetDecimalString(idx)`   |
| `Array`         | `Value`                                     | `GetValue(idx)`           |
| `Map`           | `Value`                                     | `GetValue(idx)`           |
| `Row`           | `Value`                                     | `GetValue(idx)`           |

## Type Checking

```cpp
if (rec.row.GetType(0) == fluss::TypeId::Int) {
    int32_t value = rec.row.GetInt32(0);
}
if (rec.row.IsNull(1)) {
    // field is null
}
if (rec.row.IsDecimal(2)) {
    std::string decimal_str = rec.row.GetDecimalString(2);
}
```

## Constants

```cpp
constexpr int64_t fluss::EARLIEST_OFFSET = -2;  // Start from earliest
```

To start reading from the latest offset, resolve the current offset via `ListOffsets` before subscribing:

```cpp
std::unordered_map<int32_t, int64_t> offsets;
admin.ListOffsets(table_path, {0}, fluss::OffsetSpec::Latest(), offsets);
scanner.Subscribe(0, offsets[0]);
```
