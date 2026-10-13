---
sidebar_position: 6
---
# Partitioned Tables

Partitioned tables distribute data across partitions based on partition column values, enabling efficient data organization and querying. Both log tables and primary key tables support partitioning.

## Partitioned Log Tables

### Creating a Partitioned Log Table

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("event_id", fluss::DataType::Int())
    .AddColumn("event_type", fluss::DataType::String())
    .AddColumn("dt", fluss::DataType::String())
    .AddColumn("region", fluss::DataType::String())
    .Build();

auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .SetPartitionKeys({"dt", "region"})
    .SetBucketCount(3)
    .Build();

fluss::TablePath table_path("fluss", "partitioned_events");
admin.CreateTable(table_path, descriptor, true);
```

### Writing to Partitioned Log Tables

**Partitions must exist before writing data, otherwise the client will by default retry indefinitely.** Include partition column values in each row, the client routes records to the correct partition automatically.

```cpp
fluss::Table table;
conn.GetTable(table_path, table);

fluss::AppendWriter writer;
table.NewAppend().CreateWriter(writer);

fluss::GenericRow row;
row.SetInt32(0, 1);
row.SetString(1, "user_login");
row.SetString(2, "2024-01-15");
row.SetString(3, "US");
writer.Append(row);
writer.Flush();
```

### Reading from Partitioned Log Tables

For partitioned tables, use partition-aware subscribe methods.

```cpp
fluss::Table table;
conn.GetTable(table_path, table);

fluss::LogScanner scanner;
table.NewScan().CreateLogScanner(scanner);

// Subscribe to individual partitions
for (const auto& pi : partition_infos) {
    scanner.SubscribePartitionBuckets(pi.partition_id, 0, 0);
}

fluss::ScanRecords records;
scanner.Poll(5000, records);

for (const auto& rec : records) {
    std::cout << "bucket_id=" << rec.bucket_id
              << " offset=" << rec.offset << std::endl;
}

// Or batch-subscribe to all partitions at once
fluss::LogScanner batch_scanner;
table.NewScan().CreateLogScanner(batch_scanner);

std::vector<fluss::PartitionBucketSubscription> subs;
for (const auto& pi : partition_infos) {
    subs.push_back({pi.partition_id, 0, 0});
}
batch_scanner.SubscribePartitionBuckets(subs);
```

**Unsubscribe from a partition bucket:**

```cpp
// Stop receiving records from a specific partition bucket
scanner.UnsubscribePartition(partition_infos[0].partition_id, 0);
```

### Managing Partitions

```cpp
// Create a partition
admin.CreatePartition(table_path, {{"dt", "2024-01-15"}, {"region", "EMEA"}}, true);

// List partitions
std::vector<fluss::PartitionInfo> partition_infos;
admin.ListPartitionInfos(table_path, partition_infos);

// Query partition offsets
std::vector<int32_t> bucket_ids = {0, 1, 2};
std::unordered_map<int32_t, int64_t> offsets;
admin.ListPartitionOffsets(table_path, "2024-01-15$US",
                           bucket_ids, fluss::OffsetSpec::Latest(), offsets);
```

## Partitioned Primary Key Tables

Partitioned KV tables combine partitioning with primary key operations. Partition columns must be part of the primary key.

### Creating a Partitioned Primary Key Table

```cpp
auto schema = fluss::Schema::NewBuilder()
    .AddColumn("user_id", fluss::DataType::Int())
    .AddColumn("region", fluss::DataType::String())
    .AddColumn("zone", fluss::DataType::BigInt())
    .AddColumn("score", fluss::DataType::BigInt())
    .SetPrimaryKeys({"user_id", "region", "zone"})
    .Build();

auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .SetPartitionKeys({"region", "zone"})
    .SetBucketCount(3)
    .Build();

fluss::TablePath table_path("fluss", "partitioned_users");
admin.CreateTable(table_path, descriptor, true);
```

### Writing to Partitioned Primary Key Tables

**Partitions must exist before upserting data, otherwise the client will by default retry indefinitely.**

```cpp
fluss::Table table;
conn.GetTable(table_path, table);

// Create partitions first
admin.CreatePartition(table_path, {{"region", "APAC"}, {"zone", "1"}}, true);
admin.CreatePartition(table_path, {{"region", "EMEA"}, {"zone", "2"}}, true);
admin.CreatePartition(table_path, {{"region", "US"}, {"zone", "3"}}, true);

fluss::UpsertWriter writer;
table.NewUpsert().CreateWriter(writer);

auto row = table.NewRow();
row.Set("user_id", 1001);
row.Set("region", "APAC");
row.Set("zone", static_cast<int64_t>(1));
row.Set("score", static_cast<int64_t>(1234));
writer.Upsert(row);
writer.Flush();
```

### Looking Up Records in Partitioned Tables

Lookup requires all primary key columns including partition columns.

> **Note:** Scanning partitioned primary key tables is not supported. Use lookup operations instead.

```cpp
fluss::Lookuper lookuper;
table.NewLookup().CreateLookuper(lookuper);

auto pk = table.NewRow();
pk.Set("user_id", 1001);
pk.Set("region", "APAC");
pk.Set("zone", static_cast<int64_t>(1));

fluss::LookupResult result;
lookuper.Lookup(pk, result);
if (result.Found()) {
    std::cout << "score=" << result.GetInt64(3) << std::endl;
}
```
