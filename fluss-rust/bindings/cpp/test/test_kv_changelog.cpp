/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Integration tests for primary-key (KV) table changelog (CDC) scanning.
// Mirrors crates/fluss/tests/integration/kv_changelog.rs.

#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "test_utils.h"

class KvChangelogTest : public ::testing::Test {
   protected:
    fluss::Admin& admin() { return fluss_test::FlussTestEnvironment::Instance()->GetAdmin(); }

    fluss::Connection& connection() {
        return fluss_test::FlussTestEnvironment::Instance()->GetConnection();
    }
};

// A record-mode scanner over a PK table yields its CDC changelog. With the
// default FULL changelog image: inserting a new key emits +I, overwriting an
// existing key emits -U (old image) then +U (new image), and a delete emits -D
// (old image). A single bucket keeps the offsets contiguous.
TEST_F(KvChangelogTest, SubscribeKvTableChangelog) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_kv_changelog_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .SetPrimaryKeys({"id"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketCount(1)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto table_upsert = table.NewUpsert();
    fluss::UpsertWriter upsert_writer;
    ASSERT_OK(table_upsert.CreateWriter(upsert_writer));

    // Await each write so the changelog offsets are produced in a fixed order.
    {  // +I (1, alice)
        fluss::GenericRow row(2);
        row.SetInt32(0, 1);
        row.SetString(1, "alice");
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(row, wr));
        ASSERT_OK(wr.Wait());
    }
    {  // +I (2, bob)
        fluss::GenericRow row(2);
        row.SetInt32(0, 2);
        row.SetString(1, "bob");
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(row, wr));
        ASSERT_OK(wr.Wait());
    }
    {  // overwrite id=1 -> -U (1, alice) then +U (1, alice2)
        fluss::GenericRow row(2);
        row.SetInt32(0, 1);
        row.SetString(1, "alice2");
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(row, wr));
        ASSERT_OK(wr.Wait());
    }
    {  // -D (2, bob)
        fluss::GenericRow del(2);
        del.SetInt32(0, 2);
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Delete(del, wr));
        ASSERT_OK(wr.Wait());
    }

    auto table_scan = table.NewScan();
    fluss::LogScanner log_scanner;
    ASSERT_OK(table_scan.CreateLogScanner(log_scanner));
    ASSERT_OK(log_scanner.Subscribe(0, fluss::EARLIEST_OFFSET));

    struct Decoded {
        int64_t offset;
        fluss::ChangeType change_type;
        int32_t id;
        std::string name;
    };

    std::vector<Decoded> records;
    fluss_test::PollRecords(
        log_scanner, 5,
        [](const fluss::ScanRecord& rec) {
            return Decoded{rec.offset, rec.change_type, rec.row.GetInt32(0),
                           std::string(rec.row.GetString(1))};
        },
        records);

    ASSERT_EQ(records.size(), 5u);
    std::sort(records.begin(), records.end(),
              [](const Decoded& a, const Decoded& b) { return a.offset < b.offset; });

    const std::vector<fluss::ChangeType> expected_types = {
        fluss::ChangeType::Insert, fluss::ChangeType::Insert, fluss::ChangeType::UpdateBefore,
        fluss::ChangeType::UpdateAfter, fluss::ChangeType::Delete};
    const std::vector<std::pair<int32_t, std::string>> expected_rows = {
        {1, "alice"},    // +I
        {2, "bob"},      // +I
        {1, "alice"},    // -U (old image)
        {1, "alice2"},   // +U (new image)
        {2, "bob"},      // -D (old image)
    };

    for (size_t i = 0; i < records.size(); ++i) {
        EXPECT_EQ(records[i].offset, static_cast<int64_t>(i));
        EXPECT_EQ(static_cast<int>(records[i].change_type), static_cast<int>(expected_types[i]))
            << "change_type mismatch at " << i;
        EXPECT_EQ(records[i].id, expected_rows[i].first) << "id mismatch at " << i;
        EXPECT_EQ(records[i].name, expected_rows[i].second) << "name mismatch at " << i;
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

// The Arrow batch scanner carries no per-record change types, so it rejects
// primary-key tables (mirrors the core / Java restriction).
TEST_F(KvChangelogTest, RecordBatchScannerRejectsPrimaryKey) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_kv_changelog_batch_reject_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .SetPrimaryKeys({"id"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto table_scan = table.NewScan();
    fluss::LogScanner batch_scanner;
    auto result = table_scan.CreateRecordBatchLogScanner(batch_scanner);
    EXPECT_FALSE(result.Ok()) << "batch scanner should reject a primary-key table";

    ASSERT_OK(adm.DropTable(table_path, false));
}
