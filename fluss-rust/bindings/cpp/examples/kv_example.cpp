// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <iostream>
#include <string>
#include <vector>

#include "fluss.hpp"

static void check(const char* step, const fluss::Result& r) {
    if (!r.Ok()) {
        std::cerr << step << " failed: code=" << r.error_code << " msg=" << r.error_message
                  << std::endl;
        std::exit(1);
    }
}

int main() {
    // 1) Connect and get Admin
    fluss::Configuration config;
    config.bootstrap_servers = "127.0.0.1:9123";

    fluss::Connection conn;
    check("create", fluss::Connection::Create(config, conn));

    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));

    fluss::TablePath kv_table_path("fluss", "kv_table_cpp_v1");

    // Drop if exists
    admin.DropTable(kv_table_path, true);

    // 2) Create a KV table with primary key, including decimal and temporal types
    auto kv_schema = fluss::Schema::NewBuilder()
                         .AddColumn("user_id", fluss::DataType::Int())
                         .AddColumn("name", fluss::DataType::String())
                         .AddColumn("email", fluss::DataType::String())
                         .AddColumn("score", fluss::DataType::Float())
                         .AddColumn("balance", fluss::DataType::Decimal(10, 2))
                         .AddColumn("birth_date", fluss::DataType::Date())
                         .AddColumn("login_time", fluss::DataType::Time())
                         .AddColumn("created_at", fluss::DataType::Timestamp())
                         .AddColumn("last_seen", fluss::DataType::TimestampLtz())
                         .SetPrimaryKeys({"user_id"})
                         .Build();

    auto kv_descriptor = fluss::TableDescriptor::NewBuilder()
                             .SetSchema(kv_schema)
                             .SetBucketCount(3)
                             .SetComment("cpp kv table example")
                             .Build();

    check("create_kv_table", admin.CreateTable(kv_table_path, kv_descriptor, false));
    std::cout << "Created KV table with primary key" << std::endl;

    fluss::Table kv_table;
    check("get_kv_table", conn.GetTable(kv_table_path, kv_table));

    // 3) Upsert rows using name-based Set()
    //    - Set("balance", "1234.56") auto-routes to SetDecimal (schema-aware)
    //    - Set("created_at", ts) auto-routes to SetTimestampNtz (schema-aware)
    //    - Set("last_seen", ts) auto-routes to SetTimestampLtz (schema-aware)
    std::cout << "\n--- Upsert Rows ---" << std::endl;
    fluss::UpsertWriter upsert_writer;
    check("new_upsert_writer", kv_table.NewUpsert().CreateWriter(upsert_writer));

    // Fire-and-forget upserts (flush at the end)
    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 1);
        row.Set("name", "Alice");
        row.Set("email", "alice@example.com");
        row.Set("score", 95.5f);
        row.Set("balance", "1234.56");
        row.Set("birth_date", fluss::Date::FromYMD(1990, 3, 15));
        row.Set("login_time", fluss::Time::FromHMS(9, 30, 0));
        row.Set("created_at", fluss::Timestamp::FromMillis(1700000000000));
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000060000));
        check("upsert_1", upsert_writer.Upsert(row));
    }
    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 2);
        row.Set("name", "Bob");
        row.Set("email", "bob@example.com");
        row.Set("score", 87.3f);
        row.Set("balance", "567.89");
        row.Set("birth_date", fluss::Date::FromYMD(1985, 7, 22));
        row.Set("login_time", fluss::Time::FromHMS(14, 15, 30));
        row.Set("created_at", fluss::Timestamp::FromMillis(1700000100000));
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000200000));
        check("upsert_2", upsert_writer.Upsert(row));
    }

    // Per-record acknowledgment
    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 3);
        row.Set("name", "Charlie");
        row.Set("email", "charlie@example.com");
        row.Set("score", 92.0f);
        row.Set("balance", "99999.99");
        row.Set("birth_date", fluss::Date::FromYMD(2000, 1, 1));
        row.Set("login_time", fluss::Time::FromHMS(23, 59, 59));
        row.Set("created_at", fluss::Timestamp::FromMillis(1700000300000));
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000400000));
        fluss::WriteResult wr;
        check("upsert_3", upsert_writer.Upsert(row, wr));
        check("upsert_3_wait", wr.Wait());
        std::cout << "Upsert acknowledged by server" << std::endl;
    }

    check("upsert_flush", upsert_writer.Flush());
    std::cout << "Upserted 3 rows" << std::endl;

    // 4) Lookup by primary key — verify all types round-trip
    std::cout << "\n--- Lookup by Primary Key ---" << std::endl;
    fluss::Lookuper lookuper;
    check("new_lookuper", kv_table.NewLookup().CreateLookuper(lookuper));

    // Lookup existing key
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 1);

        fluss::LookupResult result;
        check("lookup_1", lookuper.Lookup(pk_row, result));
        if (result.Found()) {
            // Name-based getters — same data as index-based but self-documenting
            auto date = result.GetDate("birth_date");
            auto time = result.GetTime("login_time");
            auto created = result.GetTimestamp("created_at");
            auto seen = result.GetTimestamp("last_seen");
            std::cout << "Found user_id=1:"
                      << "\n  name=" << result.GetString("name")
                      << "\n  email=" << result.GetString("email")
                      << "\n  score=" << result.GetFloat32("score")
                      << "\n  balance=" << result.GetDecimalString("balance")
                      << "\n  birth_date=" << date.Year() << "-" << date.Month() << "-"
                      << date.Day() << "\n  login_time=" << time.Hour() << ":" << time.Minute()
                      << ":" << time.Second() << "\n  created_at(ms)=" << created.epoch_millis
                      << "\n  last_seen(ms)=" << seen.epoch_millis << std::endl;
        } else {
            std::cerr << "ERROR: Expected to find user_id=1" << std::endl;
            std::exit(1);
        }
    }

    // Lookup non-existing key
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 999);

        fluss::LookupResult result;
        check("lookup_999", lookuper.Lookup(pk_row, result));
        if (!result.Found()) {
            std::cout << "user_id=999 not found (expected)" << std::endl;
        } else {
            std::cerr << "ERROR: Expected user_id=999 to not be found" << std::endl;
            std::exit(1);
        }
    }

    // 4b) Null row round-trip (matches Rust kv_table.rs all_supported_datatypes)
    //     Upsert a row with all non-PK fields null, lookup, verify IsNull
    std::cout << "\n--- Null Row Round-Trip ---" << std::endl;
    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 100);
        row.SetNull(1);  // name
        row.SetNull(2);  // email
        row.SetNull(3);  // score
        row.SetNull(4);  // balance
        row.SetNull(5);  // birth_date
        row.SetNull(6);  // login_time
        row.SetNull(7);  // created_at
        row.SetNull(8);  // last_seen
        fluss::WriteResult wr;
        check("upsert_null_row", upsert_writer.Upsert(row, wr));
        check("upsert_null_row_wait", wr.Wait());
    }
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 100);

        fluss::LookupResult result;
        check("lookup_null_row", lookuper.Lookup(pk_row, result));
        if (!result.Found()) {
            std::cerr << "ERROR: Expected to find user_id=100 (null row)" << std::endl;
            std::exit(1);
        }

        // Verify PK is not null
        if (result.IsNull(0)) {
            std::cerr << "ERROR: PK (user_id) should not be null" << std::endl;
            std::exit(1);
        }

        // Verify all nullable columns are null (matches Rust is_null_at assertions)
        bool null_ok = true;
        for (size_t i = 1; i < result.FieldCount(); ++i) {
            if (!result.IsNull(i)) {
                std::cerr << "ERROR: column " << i << " should be null" << std::endl;
                null_ok = false;
            }
        }
        if (null_ok) {
            std::cout << "Null row verified: all " << (result.FieldCount() - 1)
                      << " nullable fields are null" << std::endl;
        } else {
            std::exit(1);
        }
    }

    // 5) Update via upsert (overwrite existing key)
    std::cout << "\n--- Update via Upsert ---" << std::endl;
    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 1);
        row.Set("name", "Alice Updated");
        row.Set("email", "alice.new@example.com");
        row.Set("score", 99.0f);
        row.Set("balance", "9999.00");
        row.Set("birth_date", fluss::Date::FromYMD(1990, 3, 15));
        row.Set("login_time", fluss::Time::FromHMS(10, 0, 0));
        row.Set("created_at", fluss::Timestamp::FromMillis(1700000000000));
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000500000));
        fluss::WriteResult wr;
        check("upsert_update", upsert_writer.Upsert(row, wr));
        check("upsert_update_wait", wr.Wait());
    }

    // Verify update
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 1);

        fluss::LookupResult result;
        check("lookup_updated", lookuper.Lookup(pk_row, result));
        if (result.Found() && result.GetString(1) == "Alice Updated") {
            std::cout << "Update verified: name=" << result.GetString(1)
                      << " balance=" << result.GetDecimalString(4)
                      << " last_seen(ms)=" << result.GetTimestamp(8).epoch_millis << std::endl;
        } else {
            std::cerr << "ERROR: Update verification failed" << std::endl;
            std::exit(1);
        }
    }

    // 6) Delete by primary key
    std::cout << "\n--- Delete by Primary Key ---" << std::endl;
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 2);
        fluss::WriteResult wr;
        check("delete_2", upsert_writer.Delete(pk_row, wr));
        check("delete_2_wait", wr.Wait());
        std::cout << "Deleted user_id=2" << std::endl;
    }

    // Verify deletion
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 2);

        fluss::LookupResult result;
        check("lookup_deleted", lookuper.Lookup(pk_row, result));
        if (!result.Found()) {
            std::cout << "Delete verified: user_id=2 not found" << std::endl;
        } else {
            std::cerr << "ERROR: Expected user_id=2 to be deleted" << std::endl;
            std::exit(1);
        }
    }

    // 7) Partial update by column names
    std::cout << "\n--- Partial Update by Column Names ---" << std::endl;
    fluss::UpsertWriter partial_writer;
    check("new_partial_upsert_writer", kv_table.NewUpsert()
                                           .PartialUpdateByName({"user_id", "balance", "last_seen"})
                                           .CreateWriter(partial_writer));

    {
        auto row = kv_table.NewRow();
        row.Set("user_id", 3);
        row.Set("balance", "50000.00");
        row.Set("last_seen", fluss::Timestamp::FromMillis(1700000999000));
        fluss::WriteResult wr;
        check("partial_upsert", partial_writer.Upsert(row, wr));
        check("partial_upsert_wait", wr.Wait());
        std::cout << "Partial update: set balance=50000.00, last_seen for user_id=3" << std::endl;
    }

    // Verify partial update (other fields unchanged)
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 3);

        fluss::LookupResult result;
        check("lookup_partial", lookuper.Lookup(pk_row, result));
        if (result.Found()) {
            std::cout << "Partial update verified:"
                      << "\n  name=" << result.GetString(1) << " (unchanged)"
                      << "\n  balance=" << result.GetDecimalString(4) << " (updated)"
                      << "\n  last_seen(ms)=" << result.GetTimestamp(8).epoch_millis << " (updated)"
                      << std::endl;
        } else {
            std::cerr << "ERROR: Expected to find user_id=3" << std::endl;
            std::exit(1);
        }
    }

    // 8) Partial update by column indices (using index-based setters for lower overhead)
    std::cout << "\n--- Partial Update by Column Indices ---" << std::endl;
    fluss::UpsertWriter partial_writer_idx;
    // Columns: 0=user_id (PK), 1=name — update name only
    check("new_partial_upsert_writer_idx",
          kv_table.NewUpsert().PartialUpdateByIndex({0, 1}).CreateWriter(partial_writer_idx));

    {
        // Index-based setters: lighter than name-based, useful for hot paths
        fluss::GenericRow row;
        row.SetInt32(0, 3);                   // user_id (PK)
        row.SetString(1, "Charlie Updated");  // name
        fluss::WriteResult wr;
        check("partial_upsert_idx", partial_writer_idx.Upsert(row, wr));
        check("partial_upsert_idx_wait", wr.Wait());
        std::cout << "Partial update by indices: set name='Charlie Updated' for user_id=3"
                  << std::endl;
    }

    // Verify: name changed, balance/last_seen unchanged from previous partial update
    {
        auto pk_row = kv_table.NewRow();
        pk_row.Set("user_id", 3);

        fluss::LookupResult result;
        check("lookup_partial_idx", lookuper.Lookup(pk_row, result));
        if (result.Found()) {
            std::cout << "Partial update by indices verified:"
                      << "\n  name=" << result.GetString(1) << " (updated)"
                      << "\n  balance=" << result.GetDecimalString(4) << " (unchanged)"
                      << "\n  last_seen(ms)=" << result.GetTimestamp(8).epoch_millis
                      << " (unchanged)" << std::endl;
        } else {
            std::cerr << "ERROR: Expected to find user_id=3" << std::endl;
            std::exit(1);
        }
    }

    // Cleanup
    check("drop_kv_table", admin.DropTable(kv_table_path, true));

    // 9) Partitioned KV table
    std::cout << "\n--- Partitioned KV Table ---" << std::endl;
    fluss::TablePath partitioned_kv_path("fluss", "partitioned_kv_cpp_v1");
    admin.DropTable(partitioned_kv_path, true);

    // PK columns intentionally interleaved with non-PK columns to verify
    // that lookup correctly builds a dense PK-only row (not sparse full-width).
    auto partitioned_kv_schema = fluss::Schema::NewBuilder()
                                     .AddColumn("region", fluss::DataType::String())
                                     .AddColumn("score", fluss::DataType::BigInt())
                                     .AddColumn("user_id", fluss::DataType::Int())
                                     .AddColumn("name", fluss::DataType::String())
                                     .SetPrimaryKeys({"region", "user_id"})
                                     .Build();

    auto partitioned_kv_descriptor = fluss::TableDescriptor::NewBuilder()
                                         .SetSchema(partitioned_kv_schema)
                                         .SetPartitionKeys({"region"})
                                         .SetComment("partitioned kv table example")
                                         .Build();

    check("create_partitioned_kv",
          admin.CreateTable(partitioned_kv_path, partitioned_kv_descriptor, false));
    std::cout << "Created partitioned KV table" << std::endl;

    // Create partitions
    check("create_US", admin.CreatePartition(partitioned_kv_path, {{"region", "US"}}));
    check("create_EU", admin.CreatePartition(partitioned_kv_path, {{"region", "EU"}}));
    check("create_APAC", admin.CreatePartition(partitioned_kv_path, {{"region", "APAC"}}));
    std::cout << "Created partitions: US, EU, APAC" << std::endl;

    fluss::Table partitioned_kv_table;
    check("get_partitioned_kv_table", conn.GetTable(partitioned_kv_path, partitioned_kv_table));

    fluss::UpsertWriter partitioned_writer;
    check("new_partitioned_writer",
          partitioned_kv_table.NewUpsert().CreateWriter(partitioned_writer));

    // Upsert rows across partitions
    // Column order: region(0), score(1), user_id(2), name(3)
    struct TestRow {
        const char* region;
        int64_t score;
        int32_t user_id;
        const char* name;
    };
    TestRow test_data[] = {
        {"US", 100, 1, "Gustave"}, {"US", 200, 2, "Lune"},   {"EU", 150, 1, "Sciel"},
        {"EU", 250, 2, "Maelle"},  {"APAC", 300, 1, "Noco"},
    };

    for (const auto& td : test_data) {
        auto row = partitioned_kv_table.NewRow();
        row.Set("region", td.region);
        row.Set("score", td.score);
        row.Set("user_id", td.user_id);
        row.Set("name", td.name);
        check("partitioned_upsert", partitioned_writer.Upsert(row));
    }
    check("partitioned_flush", partitioned_writer.Flush());
    std::cout << "Upserted 5 rows across 3 partitions" << std::endl;

    // Lookup all rows
    fluss::Lookuper partitioned_lookuper;
    check("new_partitioned_lookuper",
          partitioned_kv_table.NewLookup().CreateLookuper(partitioned_lookuper));

    for (const auto& td : test_data) {
        auto pk = partitioned_kv_table.NewRow();
        pk.Set("region", td.region);
        pk.Set("user_id", td.user_id);

        fluss::LookupResult result;
        check("partitioned_lookup", partitioned_lookuper.Lookup(pk, result));
        if (!result.Found()) {
            std::cerr << "ERROR: Expected to find region=" << td.region << " user_id=" << td.user_id
                      << std::endl;
            std::exit(1);
        }
        if (result.GetString(3) != td.name || result.GetInt64(1) != td.score) {
            std::cerr << "ERROR: Data mismatch for region=" << td.region
                      << " user_id=" << td.user_id << std::endl;
            std::exit(1);
        }
    }
    std::cout << "All 5 rows verified across partitions" << std::endl;

    // Update within a partition
    {
        auto row = partitioned_kv_table.NewRow();
        row.Set("region", "US");
        row.Set("score", static_cast<int64_t>(999));
        row.Set("user_id", 1);
        row.Set("name", "Gustave Updated");
        fluss::WriteResult wr;
        check("partitioned_update", partitioned_writer.Upsert(row, wr));
        check("partitioned_update_wait", wr.Wait());
    }
    {
        auto pk = partitioned_kv_table.NewRow();
        pk.Set("region", "US");
        pk.Set("user_id", 1);
        fluss::LookupResult result;
        check("partitioned_lookup_updated", partitioned_lookuper.Lookup(pk, result));
        if (!result.Found() || result.GetString(3) != "Gustave Updated" ||
            result.GetInt64(1) != 999) {
            std::cerr << "ERROR: Partition update verification failed" << std::endl;
            std::exit(1);
        }
        std::cout << "Update verified: US/1 name=" << result.GetString(3)
                  << " score=" << result.GetInt64(1) << std::endl;
    }

    // Lookup in non-existent partition
    {
        auto pk = partitioned_kv_table.NewRow();
        pk.Set("region", "UNKNOWN");
        pk.Set("user_id", 1);
        fluss::LookupResult result;
        check("partitioned_lookup_unknown", partitioned_lookuper.Lookup(pk, result));
        if (result.Found()) {
            std::cerr << "ERROR: Expected UNKNOWN partition lookup to return not found"
                      << std::endl;
            std::exit(1);
        }
        std::cout << "UNKNOWN partition lookup: not found (expected)" << std::endl;
    }

    // Delete within a partition
    {
        auto pk = partitioned_kv_table.NewRow();
        pk.Set("region", "EU");
        pk.Set("user_id", 1);
        fluss::WriteResult wr;
        check("partitioned_delete", partitioned_writer.Delete(pk, wr));
        check("partitioned_delete_wait", wr.Wait());
    }
    {
        auto pk = partitioned_kv_table.NewRow();
        pk.Set("region", "EU");
        pk.Set("user_id", 1);
        fluss::LookupResult result;
        check("partitioned_lookup_deleted", partitioned_lookuper.Lookup(pk, result));
        if (result.Found()) {
            std::cerr << "ERROR: Expected EU/1 to be deleted" << std::endl;
            std::exit(1);
        }
        std::cout << "Delete verified: EU/1 not found" << std::endl;
    }

    // Verify other record in same partition still exists
    {
        auto pk = partitioned_kv_table.NewRow();
        pk.Set("region", "EU");
        pk.Set("user_id", 2);
        fluss::LookupResult result;
        check("partitioned_lookup_eu2", partitioned_lookuper.Lookup(pk, result));
        if (!result.Found() || result.GetString(3) != "Maelle") {
            std::cerr << "ERROR: Expected EU/2 (Maelle) to still exist" << std::endl;
            std::exit(1);
        }
        std::cout << "EU/2 still exists: name=" << result.GetString(3) << std::endl;
    }

    check("drop_partitioned_kv", admin.DropTable(partitioned_kv_path, true));
    std::cout << "\nKV table example completed successfully!" << std::endl;

    return 0;
}
