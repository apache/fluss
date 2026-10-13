---
sidebar_position: 7
---
# Prefix Lookup

Prefix lookup returns all rows whose primary key starts with a given prefix. It is enabled by choosing a **bucket key that is a strict prefix of the primary key** — rows sharing the same bucket-key prefix land in the same bucket, so one bucket lookup returns them all.

## Table Requirements

- The table must have a primary key.
- The bucket key must be a strict prefix of the primary key (on partitioned tables, of the *non-partition* portion of the primary key).
- The bucket key cannot equal the full primary key — that is a normal primary-key lookup; use [`Lookuper`](./primary-key-tables.md#looking-up-records) instead.
- The columns passed to `NewPrefixLookup()` must equal `partition_keys ++ bucket_key` (in that order, if partitioned).

`NewPrefixLookup()` validates these rules and returns a non-`Ok` `Result` (with `error_code == ErrorCode::CLIENT_ERROR`) describing the violation.

## Non-Partitioned Table

Pick a schema where the bucket key is a prefix of the primary key:

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("user_id", fluss::DataType::Int())
    .AddColumn("session_id", fluss::DataType::String())
    .AddColumn("event_seq", fluss::DataType::BigInt())
    .AddColumn("event_data", fluss::DataType::String())
    .SetPrimaryKeys({"user_id", "session_id", "event_seq"})
    .Build();

// Bucket key (user_id, session_id) is a prefix of the primary key.
auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .SetBucketKeys({"user_id", "session_id"})
    .SetBucketCount(3)
    .Build();

fluss::TablePath table_path("fluss", "sessions");
admin.CreateTable(table_path, descriptor, true);
```

Create the lookuper with the prefix columns, then call `PrefixLookup`:

```cpp
fluss::Table table;
conn.GetTable(table_path, table);

fluss::PrefixLookuper prefix_lookuper;
table.NewPrefixLookup({"user_id", "session_id"}, prefix_lookuper);

auto prefix = table.NewRow();
prefix.Set("user_id", 1);
prefix.Set("session_id", "sess-a");

fluss::PrefixLookupResult result;
prefix_lookuper.PrefixLookup(prefix, result);

for (size_t i = 0; i < result.Size(); ++i) {
    auto row = result.GetRow(i);
    std::cout << "seq=" << row.GetInt64("event_seq")
              << ", data=" << row.GetString("event_data") << std::endl;
}
```

Unlike primary-key lookup (which returns a single row via `LookupResult::Found()`), prefix lookup returns zero or more rows via `Size()` / `GetRow(i)`, in primary-key order.

Each `GetRow(i)` is a `PrefixRowView` with the same getters as a scan `RowView`: scalars by index or name, and complex (`ARRAY` / `MAP` / `ROW`) columns via `GetValue(...)` — see [Reading Complex Columns](../data-types.md#reading-complex-columns-array--map--row).

## Partitioned Table

On a partitioned table, the partition columns are stripped from the primary key before the bucket-prefix rule is evaluated. The lookup key must still carry the partition values so the client can route the request to the right partition — so the columns passed to `NewPrefixLookup()` are `partition_keys ++ bucket_key`.

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("region", fluss::DataType::String())
    .AddColumn("user_id", fluss::DataType::Int())
    .AddColumn("session_id", fluss::DataType::String())
    .AddColumn("event_seq", fluss::DataType::BigInt())
    .AddColumn("event_data", fluss::DataType::String())
    .SetPrimaryKeys({"region", "user_id", "session_id", "event_seq"})
    .Build();

auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .SetPartitionKeys({"region"})
    // Bucket key (user_id, session_id) is a prefix of the PK minus the partition column.
    .SetBucketKeys({"user_id", "session_id"})
    .SetBucketCount(3)
    .Build();
```

```cpp
fluss::PrefixLookuper prefix_lookuper;
// lookup columns = partition keys ++ bucket key
table.NewPrefixLookup({"region", "user_id", "session_id"}, prefix_lookuper);

auto prefix = table.NewRow();
prefix.Set("region", "US");          // partition column
prefix.Set("user_id", 1);
prefix.Set("session_id", "sess-a");

fluss::PrefixLookupResult result;
prefix_lookuper.PrefixLookup(prefix, result);

for (size_t i = 0; i < result.Size(); ++i) {
    auto row = result.GetRow(i);
    std::cout << "seq=" << row.GetInt64("event_seq")
              << ", data=" << row.GetString("event_data") << std::endl;
}
```
