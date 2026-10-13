---
sidebar_position: 3
---
# Admin Operations

## Get Admin Interface

```cpp
fluss::Admin admin;
conn.GetAdmin(admin);
```

## Database Operations

```cpp
// Create database
fluss::DatabaseDescriptor db_descriptor;
db_descriptor.comment = "My database";
admin.CreateDatabase("my_database", db_descriptor, true);

// List all databases
std::vector<std::string> databases;
admin.ListDatabases(databases);
for (const auto& db : databases) {
    std::cout << "Database: " << db << std::endl;
}

// Check if database exists
bool exists = false;
admin.DatabaseExists("my_database", exists);

// Get database information
fluss::DatabaseInfo db_info;
admin.GetDatabaseInfo("my_database", db_info);
std::cout << "Database: " << db_info.database_name << std::endl;

// Drop database
admin.DropDatabase("my_database", true, false);
```

## Table Operations

```cpp
fluss::TablePath table_path("fluss", "my_table");

auto schema = fluss::Schema::NewBuilder()
    .AddColumn("id", fluss::DataType::Int())
    .AddColumn("name", fluss::DataType::String())
    .AddColumn("score", fluss::DataType::Float())
    .AddColumn("age", fluss::DataType::Int())
    .Build();

auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .SetBucketCount(3)
    .SetComment("Example table")
    .Build();

// Create table
admin.CreateTable(table_path, descriptor, true);

// Get table information
fluss::TableInfo table_info;
admin.GetTableInfo(table_path, table_info);
std::cout << "Table ID: " << table_info.table_id << std::endl;
std::cout << "Number of buckets: " << table_info.num_buckets << std::endl;
std::cout << "Has primary key: " << table_info.has_primary_key << std::endl;
std::cout << "Is partitioned: " << table_info.is_partitioned << std::endl;

// Drop table
admin.DropTable(table_path, true);
```

## Schema Builder Options

```cpp
// Schema with primary key
auto pk_schema = fluss::Schema::NewBuilder()
    .AddColumn("id", fluss::DataType::Int())
    .AddColumn("name", fluss::DataType::String())
    .AddColumn("value", fluss::DataType::Double())
    .SetPrimaryKeys({"id"})
    .Build();

// Table descriptor with partitioning
auto descriptor = fluss::TableDescriptor::NewBuilder()
    .SetSchema(schema)
    .SetPartitionKeys({"date"})
    .SetBucketCount(3)
    .SetBucketKeys({"user_id"})
    .SetProperty("retention_days", "7")
    .SetComment("Sample table")
    .Build();
```

## Partition Operations

```cpp
// Create a partition
std::unordered_map<std::string, std::string> partition_spec = {{"region", "US"}};
admin.CreatePartition(table_path, partition_spec, true);

// List all partitions
std::vector<fluss::PartitionInfo> partitions;
admin.ListPartitionInfos(table_path, partitions);
for (const auto& p : partitions) {
    std::cout << "Partition: id=" << p.partition_id
              << ", name=" << p.partition_name << std::endl;
}

// Drop a partition
admin.DropPartition(table_path, partition_spec, true);
```

## Offset Operations

```cpp
std::vector<int32_t> bucket_ids = {0, 1, 2};

// Query earliest offsets
std::unordered_map<int32_t, int64_t> earliest_offsets;
admin.ListOffsets(table_path, bucket_ids,
                  fluss::OffsetSpec::Earliest(), earliest_offsets);

// Query latest offsets
std::unordered_map<int32_t, int64_t> latest_offsets;
admin.ListOffsets(table_path, bucket_ids,
                  fluss::OffsetSpec::Latest(), latest_offsets);

// Query offsets for a specific timestamp
std::unordered_map<int32_t, int64_t> timestamp_offsets;
admin.ListOffsets(table_path, bucket_ids,
                  fluss::OffsetSpec::Timestamp(timestamp_ms),
                  timestamp_offsets);

// Query partition offsets
std::unordered_map<int32_t, int64_t> partition_offsets;
admin.ListPartitionOffsets(table_path, "partition_name",
                           bucket_ids, fluss::OffsetSpec::Latest(),
                           partition_offsets);
```

## Lake Snapshot

:::note
Lake snapshots require [lake integration](https://fluss.apache.org/docs/maintenance/tiered-storage/overview/) (e.g. Paimon or Iceberg) to be enabled on the server. Without it, `GetLatestLakeSnapshot` will return an error.
:::

```cpp
fluss::LakeSnapshot snapshot;
admin.GetLatestLakeSnapshot(table_path, snapshot);
std::cout << "Snapshot ID: " << snapshot.snapshot_id << std::endl;
for (const auto& bucket_offset : snapshot.bucket_offsets) {
    std::cout << "  Table " << bucket_offset.table_id
              << ", Bucket " << bucket_offset.bucket_id
              << ": offset=" << bucket_offset.offset << std::endl;
}
```
