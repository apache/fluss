---
sidebar_position: 5
---
# Primary Key Tables

Primary key tables (KV tables) support upsert, delete, and lookup operations.

## Creating a Primary Key Table

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("id", fluss::DataType::Int())
    .AddColumn("name", fluss::DataType::String())
    .AddColumn("age", fluss::DataType::BigInt())
    .SetPrimaryKeys({"id"})
    .Build();

auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .SetBucketCount(3)
    .Build();

fluss::TablePath table_path("fluss", "users");
admin.CreateTable(table_path, descriptor, true);
```

## Upserting Records

```cpp
fluss::Table table;
conn.GetTable(table_path, table);

fluss::UpsertWriter upsert_writer;
table.NewUpsert().CreateWriter(upsert_writer);

// Fire-and-forget upserts
{
    auto row = table.NewRow();
    row.Set("id", 1);
    row.Set("name", "Alice");
    row.Set("age", static_cast<int64_t>(25));
    upsert_writer.Upsert(row);
}
{
    auto row = table.NewRow();
    row.Set("id", 2);
    row.Set("name", "Bob");
    row.Set("age", static_cast<int64_t>(30));
    upsert_writer.Upsert(row);
}
upsert_writer.Flush();

// Per-record acknowledgment
{
    auto row = table.NewRow();
    row.Set("id", 3);
    row.Set("name", "Charlie");
    row.Set("age", static_cast<int64_t>(35));
    fluss::WriteResult wr;
    upsert_writer.Upsert(row, wr);
    wr.Wait();
}
```

## Updating Records

Upsert with the same primary key to update an existing record.

```cpp
auto row = table.NewRow();
row.Set("id", 1);
row.Set("name", "Alice Updated");
row.Set("age", static_cast<int64_t>(26));
fluss::WriteResult wr;
upsert_writer.Upsert(row, wr);
wr.Wait();
```

## Deleting Records

```cpp
auto pk_row = table.NewRow();
pk_row.Set("id", 2);
fluss::WriteResult wr;
upsert_writer.Delete(pk_row, wr);
wr.Wait();
```

## Partial Updates

Update only specific columns while preserving others.

```cpp
// By column names
fluss::UpsertWriter partial_writer;
table.NewUpsert()
    .PartialUpdateByName({"id", "age"})
    .CreateWriter(partial_writer);

auto row = table.NewRow();
row.Set("id", 1);
row.Set("age", static_cast<int64_t>(27));
fluss::WriteResult wr;
partial_writer.Upsert(row, wr);
wr.Wait();

// By column indices
fluss::UpsertWriter partial_writer_idx;
table.NewUpsert()
    .PartialUpdateByIndex({0, 2})
    .CreateWriter(partial_writer_idx);
```

## Looking Up Records

```cpp
fluss::Lookuper lookuper;
table.NewLookup().CreateLookuper(lookuper);

auto pk_row = table.NewRow();
pk_row.Set("id", 1);

fluss::LookupResult result;
lookuper.Lookup(pk_row, result);

if (result.Found()) {
    std::cout << "Found: name=" << result.GetString(1)
              << ", age=" << result.GetInt64(2) << std::endl;
} else {
    std::cout << "Not found" << std::endl;
}
```
