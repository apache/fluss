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

## Subscribing to the Changelog (CDC)

Every primary key table maintains a changelog of its row-level changes. Read it with a record-mode `LogScanner` — the same API used for [log tables](./log-tables.md) — to stream CDC events. Each `ScanRecord` carries a `change_type`: `+I` (insert), `-U` / `+U` (the old and new images of an update), and `-D` (delete, carrying the old row).

```cpp
fluss::LogScanner log_scanner;
table.NewScan().CreateLogScanner(log_scanner);

// Subscribe to every bucket from the start of the changelog.
int32_t num_buckets = table.GetTableInfo().num_buckets;
for (int32_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id) {
    log_scanner.Subscribe(bucket_id, fluss::EARLIEST_OFFSET);
}

while (true) {
    fluss::ScanRecords records;
    log_scanner.Poll(3000, records);
    if (records.IsEmpty()) {
        break;  // caught up to the end of the changelog
    }
    for (auto rec : records) {
        // rec.change_type is +I / -U / +U / -D (see ChangeTypeShortString in the API reference)
        std::cout << ChangeTypeShortString(rec.change_type) << " " << rec.row.GetInt32(0) << " "
                  << rec.row.GetString(1) << std::endl;
    }
}
```

With the default changelog mode (`'table.changelog.image' = 'FULL'`), updating a key emits a `-U`/`+U` pair and deleting it emits `-D`; the `WAL` mode emits only `+U` on update. To resume from a committed position instead of the start, `Subscribe` at a specific offset.

## Limit Scan

To read up to `n` rows of a bucket's current state without supplying keys, use a batch scanner. The server returns the deduplicated current rows as Arrow batches, convenient for previews or analytics.

```cpp
int64_t table_id = table.GetTableInfo().table_id;
fluss::TableBucket bucket{table_id, 0};

fluss::BatchScanner scanner;
table.NewScan().Limit(10).CreateBucketBatchScanner(bucket, scanner);

fluss::ArrowRecordBatches batches;
scanner.CollectAllBatches(batches);
```

The limit applies per bucket; scan each bucket to cover a multi-bucket table.
