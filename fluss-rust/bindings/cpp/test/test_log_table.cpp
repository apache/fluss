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

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <limits>
#include <thread>
#include <tuple>

#include "test_utils.h"

class LogTableTest : public ::testing::Test {
   protected:
    fluss::Admin& admin() { return fluss_test::FlussTestEnvironment::Instance()->GetAdmin(); }

    fluss::Connection& connection() {
        return fluss_test::FlussTestEnvironment::Instance()->GetConnection();
    }
};

TEST_F(LogTableTest, AppendRecordBatchAndScan) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_append_record_batch_and_scan_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("c1", fluss::DataType::Int())
                      .AddColumn("c2", fluss::DataType::String())
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketCount(3)
                                .SetBucketKeys({"c1"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    // Create append writer
    auto table_append = table.NewAppend();
    fluss::AppendWriter append_writer;
    ASSERT_OK(table_append.CreateWriter(append_writer));

    // Append Arrow record batches
    {
        auto c1 = arrow::Int32Builder();
        c1.AppendValues({1, 2, 3}).ok();
        auto c2 = arrow::StringBuilder();
        c2.AppendValues({"a1", "a2", "a3"}).ok();

        auto batch = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("c1", arrow::int32()), arrow::field("c2", arrow::utf8())}),
            3, {c1.Finish().ValueOrDie(), c2.Finish().ValueOrDie()});

        ASSERT_OK(append_writer.AppendArrowBatch(batch));
    }

    {
        auto c1 = arrow::Int32Builder();
        c1.AppendValues({4, 5, 6}).ok();
        auto c2 = arrow::StringBuilder();
        c2.AppendValues({"a4", "a5", "a6"}).ok();

        auto batch = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("c1", arrow::int32()), arrow::field("c2", arrow::utf8())}),
            3, {c1.Finish().ValueOrDie(), c2.Finish().ValueOrDie()});

        ASSERT_OK(append_writer.AppendArrowBatch(batch));
    }

    ASSERT_OK(append_writer.Flush());

    // Create scanner and subscribe to all 3 buckets
    fluss::Table scan_table;
    ASSERT_OK(conn.GetTable(table_path, scan_table));
    int32_t num_buckets = scan_table.GetTableInfo().num_buckets;
    ASSERT_EQ(num_buckets, 3) << "Table should have 3 buckets";

    auto table_scan = scan_table.NewScan();
    fluss::LogScanner log_scanner;
    ASSERT_OK(table_scan.CreateLogScanner(log_scanner));

    for (int32_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id) {
        ASSERT_OK(log_scanner.Subscribe(bucket_id, fluss::EARLIEST_OFFSET));
    }

    // Poll for records across all buckets
    std::vector<std::pair<int32_t, std::string>> records;
    fluss_test::PollRecords(log_scanner, 6, [](const fluss::ScanRecord& rec) {
        return std::make_pair(rec.row.GetInt32(0), std::string(rec.row.GetString(1)));
    }, records);
    ASSERT_EQ(records.size(), 6u) << "Expected 6 records";
    std::sort(records.begin(), records.end());

    std::vector<std::pair<int32_t, std::string>> expected = {
        {1, "a1"}, {2, "a2"}, {3, "a3"}, {4, "a4"}, {5, "a5"}, {6, "a6"}};
    EXPECT_EQ(records, expected);

    // Verify per-bucket iteration via BucketRecords
    {
        fluss::Table bucket_table;
        ASSERT_OK(conn.GetTable(table_path, bucket_table));
        auto bucket_scan = bucket_table.NewScan();
        fluss::LogScanner bucket_scanner;
        ASSERT_OK(bucket_scan.CreateLogScanner(bucket_scanner));

        for (int32_t bid = 0; bid < num_buckets; ++bid) {
            ASSERT_OK(bucket_scanner.Subscribe(bid, fluss::EARLIEST_OFFSET));
        }

        std::vector<std::pair<int32_t, std::string>> bucket_records;
        auto bucket_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        size_t buckets_with_data = 0;
        while (bucket_records.size() < 6 && std::chrono::steady_clock::now() < bucket_deadline) {
            fluss::ScanRecords scan_records;
            ASSERT_OK(bucket_scanner.Poll(500, scan_records));

            // Iterate by bucket
            for (size_t b = 0; b < scan_records.BucketCount(); ++b) {
                auto bkt_records = scan_records.BucketAt(b);
                if (!bkt_records.Empty()) {
                    buckets_with_data++;
                }
                for (auto rec : bkt_records) {
                    bucket_records.emplace_back(rec.row.GetInt32(0),
                                                std::string(rec.row.GetString(1)));
                }
            }
        }

        ASSERT_EQ(bucket_records.size(), 6u) << "Expected 6 records via per-bucket iteration";
        EXPECT_GT(buckets_with_data, 1u) << "Records should be distributed across multiple buckets";

        std::sort(bucket_records.begin(), bucket_records.end());
        EXPECT_EQ(bucket_records, expected);
    }

    // Test unsubscribe
    ASSERT_OK(log_scanner.Unsubscribe(0));

    // Verify unsubscribe_partition fails on a non-partitioned table
    auto unsub_result = log_scanner.UnsubscribePartition(0, 0);
    ASSERT_FALSE(unsub_result.Ok())
        << "unsubscribe_partition should fail on a non-partitioned table";

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, ListOffsets) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_list_offsets_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    // Wait for table initialization
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Earliest offset should be 0 for empty table
    std::unordered_map<int32_t, int64_t> earliest_offsets;
    ASSERT_OK(adm.ListOffsets(table_path, {0}, fluss::OffsetSpec::Earliest(), earliest_offsets));
    EXPECT_EQ(earliest_offsets[0], 0) << "Earliest offset should be 0 for bucket 0";

    // Latest offset should be 0 for empty table
    std::unordered_map<int32_t, int64_t> latest_offsets;
    ASSERT_OK(adm.ListOffsets(table_path, {0}, fluss::OffsetSpec::Latest(), latest_offsets));
    EXPECT_EQ(latest_offsets[0], 0) << "Latest offset should be 0 for empty table";

    auto before_append_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();

    // Append records
    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));
    auto table_append = table.NewAppend();
    fluss::AppendWriter append_writer;
    ASSERT_OK(table_append.CreateWriter(append_writer));

    {
        auto id_builder = arrow::Int32Builder();
        id_builder.AppendValues({1, 2, 3}).ok();
        auto name_builder = arrow::StringBuilder();
        name_builder.AppendValues({"alice", "bob", "charlie"}).ok();

        auto batch = arrow::RecordBatch::Make(
            arrow::schema(
                {arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())}),
            3, {id_builder.Finish().ValueOrDie(), name_builder.Finish().ValueOrDie()});

        ASSERT_OK(append_writer.AppendArrowBatch(batch));
    }
    ASSERT_OK(append_writer.Flush());

    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto after_append_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();

    // Latest offset after appending should be 3
    std::unordered_map<int32_t, int64_t> latest_after;
    ASSERT_OK(adm.ListOffsets(table_path, {0}, fluss::OffsetSpec::Latest(), latest_after));
    EXPECT_EQ(latest_after[0], 3) << "Latest offset should be 3 after appending 3 records";

    // Earliest offset should still be 0
    std::unordered_map<int32_t, int64_t> earliest_after;
    ASSERT_OK(adm.ListOffsets(table_path, {0}, fluss::OffsetSpec::Earliest(), earliest_after));
    EXPECT_EQ(earliest_after[0], 0) << "Earliest offset should still be 0";

    // Timestamp before append should resolve to offset 0
    std::unordered_map<int32_t, int64_t> ts_offsets;
    ASSERT_OK(adm.ListOffsets(table_path, {0}, fluss::OffsetSpec::Timestamp(before_append_ms),
                              ts_offsets));
    EXPECT_EQ(ts_offsets[0], 0)
        << "Timestamp before append should resolve to offset 0";

    // Timestamp after append should resolve to offset 3
    std::unordered_map<int32_t, int64_t> ts_after_offsets;
    ASSERT_OK(adm.ListOffsets(table_path, {0}, fluss::OffsetSpec::Timestamp(after_append_ms),
                              ts_after_offsets));
    EXPECT_EQ(ts_after_offsets[0], 3)
        << "Timestamp after append should resolve to offset 3";

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, TestProject) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_project_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("col_a", fluss::DataType::Int())
                      .AddColumn("col_b", fluss::DataType::String())
                      .AddColumn("col_c", fluss::DataType::Int())
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    // Append 3 records
    auto table_append = table.NewAppend();
    fluss::AppendWriter append_writer;
    ASSERT_OK(table_append.CreateWriter(append_writer));

    {
        auto col_a_builder = arrow::Int32Builder();
        col_a_builder.AppendValues({1, 2, 3}).ok();
        auto col_b_builder = arrow::StringBuilder();
        col_b_builder.AppendValues({"x", "y", "z"}).ok();
        auto col_c_builder = arrow::Int32Builder();
        col_c_builder.AppendValues({10, 20, 30}).ok();

        auto batch = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("col_a", arrow::int32()),
                           arrow::field("col_b", arrow::utf8()),
                           arrow::field("col_c", arrow::int32())}),
            3,
            {col_a_builder.Finish().ValueOrDie(), col_b_builder.Finish().ValueOrDie(),
             col_c_builder.Finish().ValueOrDie()});

        ASSERT_OK(append_writer.AppendArrowBatch(batch));
    }
    ASSERT_OK(append_writer.Flush());

    // Test project_by_name: select col_b and col_c only
    {
        fluss::Table proj_table;
        ASSERT_OK(conn.GetTable(table_path, proj_table));
        auto scan = proj_table.NewScan();
        scan.ProjectByName({"col_b", "col_c"});
        fluss::LogScanner scanner;
        ASSERT_OK(scan.CreateLogScanner(scanner));

        ASSERT_OK(scanner.Subscribe(0, 0));

        fluss::ScanRecords records;
        ASSERT_OK(scanner.Poll(10000, records));

        ASSERT_EQ(records.Count(), 3u) << "Should have 3 records with project_by_name";

        std::vector<std::string> expected_col_b = {"x", "y", "z"};
        std::vector<int32_t> expected_col_c = {10, 20, 30};

        // Collect and sort by col_c to get deterministic order
        std::vector<std::pair<std::string, int32_t>> collected;
        for (auto rec : records) {
            collected.emplace_back(std::string(rec.row.GetString(0)), rec.row.GetInt32(1));
        }
        std::sort(collected.begin(), collected.end(),
                  [](const auto& a, const auto& b) { return a.second < b.second; });

        for (size_t i = 0; i < 3; ++i) {
            EXPECT_EQ(collected[i].first, expected_col_b[i]) << "col_b mismatch at index " << i;
            EXPECT_EQ(collected[i].second, expected_col_c[i]) << "col_c mismatch at index " << i;
        }
    }

    // Test project by column indices: select col_b (1) and col_a (0) in that order
    {
        fluss::Table proj_table;
        ASSERT_OK(conn.GetTable(table_path, proj_table));
        auto scan = proj_table.NewScan();
        scan.ProjectByIndex({1, 0});
        fluss::LogScanner scanner;
        ASSERT_OK(scan.CreateLogScanner(scanner));

        ASSERT_OK(scanner.Subscribe(0, 0));

        fluss::ScanRecords records;
        ASSERT_OK(scanner.Poll(10000, records));

        ASSERT_EQ(records.Count(), 3u);

        std::vector<std::string> expected_col_b = {"x", "y", "z"};
        std::vector<int32_t> expected_col_a = {1, 2, 3};

        std::vector<std::pair<std::string, int32_t>> collected;
        for (auto rec : records) {
            collected.emplace_back(std::string(rec.row.GetString(0)), rec.row.GetInt32(1));
        }
        std::sort(collected.begin(), collected.end(),
                  [](const auto& a, const auto& b) { return a.second < b.second; });

        for (size_t i = 0; i < 3; ++i) {
            EXPECT_EQ(collected[i].first, expected_col_b[i]) << "col_b mismatch at index " << i;
            EXPECT_EQ(collected[i].second, expected_col_a[i]) << "col_a mismatch at index " << i;
        }
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, TestPollBatches) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_poll_batches_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    std::this_thread::sleep_for(std::chrono::seconds(1));

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto scan = table.NewScan();
    fluss::LogScanner scanner;
    ASSERT_OK(scan.CreateRecordBatchLogScanner(scanner));
    ASSERT_OK(scanner.Subscribe(0, 0));

    // Test 1: Empty table should return empty result
    {
        fluss::ArrowRecordBatches batches;
        ASSERT_OK(scanner.PollRecordBatch(500, batches));
        ASSERT_TRUE(batches.Empty());
    }

    // Append data
    auto table_append = table.NewAppend();
    fluss::AppendWriter writer;
    ASSERT_OK(table_append.CreateWriter(writer));

    auto make_batch = [](std::vector<int32_t> ids, std::vector<std::string> names) {
        auto id_builder = arrow::Int32Builder();
        id_builder.AppendValues(ids).ok();
        auto name_builder = arrow::StringBuilder();
        name_builder.AppendValues(names).ok();
        return arrow::RecordBatch::Make(
            arrow::schema(
                {arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())}),
            static_cast<int64_t>(ids.size()),
            {id_builder.Finish().ValueOrDie(), name_builder.Finish().ValueOrDie()});
    };

    ASSERT_OK(writer.AppendArrowBatch(make_batch({1, 2}, {"a", "b"})));
    ASSERT_OK(writer.AppendArrowBatch(make_batch({3, 4}, {"c", "d"})));
    ASSERT_OK(writer.AppendArrowBatch(make_batch({5, 6}, {"e", "f"})));
    ASSERT_OK(writer.Flush());

    // Extract ids from Arrow batches
    auto extract_ids = [](const fluss::ArrowRecordBatches& batches) {
        std::vector<int32_t> ids;
        for (const auto& batch : batches) {
            auto arr =
                std::static_pointer_cast<arrow::Int32Array>(batch->GetArrowRecordBatch()->column(0));
            for (int64_t i = 0; i < arr->length(); ++i) {
                ids.push_back(arr->Value(i));
            }
        }
        return ids;
    };

    // Test 2: Poll until we get all 6 records
    std::vector<int32_t> all_ids;
    fluss_test::PollRecordBatches(scanner, 6, extract_ids, all_ids);
    ASSERT_EQ(all_ids, (std::vector<int32_t>{1, 2, 3, 4, 5, 6}));

    // Test 3: Append more and verify offset continuation (no duplicates)
    ASSERT_OK(writer.AppendArrowBatch(make_batch({7, 8}, {"g", "h"})));
    ASSERT_OK(writer.Flush());

    std::vector<int32_t> new_ids;
    fluss_test::PollRecordBatches(scanner, 2, extract_ids, new_ids);
    ASSERT_EQ(new_ids, (std::vector<int32_t>{7, 8}));

    // Test 4: Subscribing from mid-offset should truncate batch
    {
        fluss::Table trunc_table;
        ASSERT_OK(conn.GetTable(table_path, trunc_table));
        auto trunc_scan = trunc_table.NewScan();
        fluss::LogScanner trunc_scanner;
        ASSERT_OK(trunc_scan.CreateRecordBatchLogScanner(trunc_scanner));
        ASSERT_OK(trunc_scanner.Subscribe(0, 3));

        std::vector<int32_t> trunc_ids;
        fluss_test::PollRecordBatches(trunc_scanner, 5, extract_ids, trunc_ids);
        ASSERT_EQ(trunc_ids, (std::vector<int32_t>{4, 5, 6, 7, 8}));
    }

    // Test 5: Projection should only return requested columns
    {
        fluss::Table proj_table;
        ASSERT_OK(conn.GetTable(table_path, proj_table));
        auto proj_scan = proj_table.NewScan();
        proj_scan.ProjectByName({"id"});
        fluss::LogScanner proj_scanner;
        ASSERT_OK(proj_scan.CreateRecordBatchLogScanner(proj_scanner));
        ASSERT_OK(proj_scanner.Subscribe(0, 0));

        fluss::ArrowRecordBatches proj_batches;
        ASSERT_OK(proj_scanner.PollRecordBatch(10000, proj_batches));

        ASSERT_FALSE(proj_batches.Empty());
        EXPECT_EQ(proj_batches[0]->GetArrowRecordBatch()->num_columns(), 1)
            << "Projected batch should have 1 column (id), not 2";
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, AllSupportedDatatypes) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_log_all_datatypes_cpp");

    // Create a log table with all supported datatypes
    auto schema =
        fluss::Schema::NewBuilder()
            .AddColumn("col_tinyint", fluss::DataType::TinyInt())
            .AddColumn("col_smallint", fluss::DataType::SmallInt())
            .AddColumn("col_int", fluss::DataType::Int())
            .AddColumn("col_bigint", fluss::DataType::BigInt())
            .AddColumn("col_float", fluss::DataType::Float())
            .AddColumn("col_double", fluss::DataType::Double())
            .AddColumn("col_boolean", fluss::DataType::Boolean())
            .AddColumn("col_char", fluss::DataType::Char(10))
            .AddColumn("col_string", fluss::DataType::String())
            .AddColumn("col_decimal", fluss::DataType::Decimal(10, 2))
            .AddColumn("col_date", fluss::DataType::Date())
            .AddColumn("col_time", fluss::DataType::Time())
            .AddColumn("col_timestamp", fluss::DataType::Timestamp())
            .AddColumn("col_timestamp_ltz", fluss::DataType::TimestampLtz())
            .AddColumn("col_bytes", fluss::DataType::Bytes())
            .AddColumn("col_binary", fluss::DataType::Binary(4))
            .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    size_t field_count = table.GetTableInfo().schema.columns.size();

    auto table_append = table.NewAppend();
    fluss::AppendWriter append_writer;
    ASSERT_OK(table_append.CreateWriter(append_writer));

    // Test data
    int32_t col_tinyint = 127;
    int32_t col_smallint = 32767;
    int32_t col_int = 2147483647;
    int64_t col_bigint = 9223372036854775807LL;
    float col_float = 3.14f;
    double col_double = 2.718281828459045;
    bool col_boolean = true;
    std::string col_char = "hello";
    std::string col_string = "world of fluss rust client";
    std::string col_decimal = "123.45";
    auto col_date = fluss::Date::FromDays(20476);           // 2026-01-23
    auto col_time = fluss::Time::FromMillis(36827000);       // 10:13:47
    auto col_timestamp = fluss::Timestamp::FromMillisNanos(1769163227123, 456000);
    auto col_timestamp_ltz = fluss::Timestamp::FromMillisNanos(1769163227123, 456000);
    std::vector<uint8_t> col_bytes = {'b', 'i', 'n', 'a', 'r', 'y', ' ', 'd', 'a', 't', 'a'};
    std::vector<uint8_t> col_binary = {0xDE, 0xAD, 0xBE, 0xEF};

    // Append a row with all datatypes
    {
        fluss::GenericRow row(field_count);
        row.SetInt32(0, col_tinyint);
        row.SetInt32(1, col_smallint);
        row.SetInt32(2, col_int);
        row.SetInt64(3, col_bigint);
        row.SetFloat32(4, col_float);
        row.SetFloat64(5, col_double);
        row.SetBool(6, col_boolean);
        row.SetString(7, col_char);
        row.SetString(8, col_string);
        row.SetDecimal(9, col_decimal);
        row.SetDate(10, col_date);
        row.SetTime(11, col_time);
        row.SetTimestampNtz(12, col_timestamp);
        row.SetTimestampLtz(13, col_timestamp_ltz);
        row.SetBytes(14, col_bytes);
        row.SetBytes(15, col_binary);
        ASSERT_OK(append_writer.Append(row));
    }

    // Append a row with null values
    {
        fluss::GenericRow row_with_nulls(field_count);
        for (size_t i = 0; i < field_count; ++i) {
            row_with_nulls.SetNull(i);
        }
        ASSERT_OK(append_writer.Append(row_with_nulls));
    }

    ASSERT_OK(append_writer.Flush());

    // Scan the records
    fluss::Table scan_table;
    ASSERT_OK(conn.GetTable(table_path, scan_table));
    auto table_scan = scan_table.NewScan();
    fluss::LogScanner log_scanner;
    ASSERT_OK(table_scan.CreateLogScanner(log_scanner));
    ASSERT_OK(log_scanner.Subscribe(0, 0));

    // Poll until we get 2 records
    std::vector<fluss::ScanRecord> all_records;
    fluss_test::PollRecords(log_scanner, 2,
        [](const fluss::ScanRecord& rec) { return rec; }, all_records);
    ASSERT_EQ(all_records.size(), 2u) << "Expected 2 records";

    // Verify first record (all values)
    auto& row = all_records[0].row;

    EXPECT_EQ(row.GetInt32(0), col_tinyint) << "col_tinyint mismatch";
    EXPECT_EQ(row.GetInt32(1), col_smallint) << "col_smallint mismatch";
    EXPECT_EQ(row.GetInt32(2), col_int) << "col_int mismatch";
    EXPECT_EQ(row.GetInt64(3), col_bigint) << "col_bigint mismatch";
    EXPECT_NEAR(row.GetFloat32(4), col_float, 1e-6f) << "col_float mismatch";
    EXPECT_NEAR(row.GetFloat64(5), col_double, 1e-15) << "col_double mismatch";
    EXPECT_EQ(row.GetBool(6), col_boolean) << "col_boolean mismatch";
    EXPECT_EQ(row.GetString(7), col_char) << "col_char mismatch";
    EXPECT_EQ(row.GetString(8), col_string) << "col_string mismatch";
    EXPECT_EQ(row.GetDecimalString(9), col_decimal) << "col_decimal mismatch";
    EXPECT_EQ(row.GetDate(10).days_since_epoch, col_date.days_since_epoch) << "col_date mismatch";
    EXPECT_EQ(row.GetTime(11).millis_since_midnight, col_time.millis_since_midnight)
        << "col_time mismatch";
    EXPECT_EQ(row.GetTimestamp(12).epoch_millis, col_timestamp.epoch_millis)
        << "col_timestamp millis mismatch";
    EXPECT_EQ(row.GetTimestamp(12).nano_of_millisecond, col_timestamp.nano_of_millisecond)
        << "col_timestamp nanos mismatch";
    EXPECT_EQ(row.GetTimestamp(13).epoch_millis, col_timestamp_ltz.epoch_millis)
        << "col_timestamp_ltz millis mismatch";
    EXPECT_EQ(row.GetTimestamp(13).nano_of_millisecond, col_timestamp_ltz.nano_of_millisecond)
        << "col_timestamp_ltz nanos mismatch";

    auto [bytes_ptr, bytes_len] = row.GetBytes(14);
    EXPECT_EQ(bytes_len, col_bytes.size()) << "col_bytes length mismatch";
    EXPECT_TRUE(std::memcmp(bytes_ptr, col_bytes.data(), bytes_len) == 0)
        << "col_bytes mismatch";

    auto [binary_ptr, binary_len] = row.GetBytes(15);
    EXPECT_EQ(binary_len, col_binary.size()) << "col_binary length mismatch";
    EXPECT_TRUE(std::memcmp(binary_ptr, col_binary.data(), binary_len) == 0)
        << "col_binary mismatch";

    // Verify second record (all nulls)
    auto& null_row = all_records[1].row;
    for (size_t i = 0; i < field_count; ++i) {
        EXPECT_TRUE(null_row.IsNull(i)) << "column " << i << " should be null";
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, PartitionedTableAppendScan) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_partitioned_log_append_cpp");

    // Create a partitioned log table
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("region", fluss::DataType::String())
                      .AddColumn("value", fluss::DataType::BigInt())
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetPartitionKeys({"region"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    // Create partitions
    fluss_test::CreatePartitions(adm, table_path, "region", {"US", "EU"});

    // Wait for partitions
    std::this_thread::sleep_for(std::chrono::seconds(2));

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto table_append = table.NewAppend();
    fluss::AppendWriter append_writer;
    ASSERT_OK(table_append.CreateWriter(append_writer));

    // Append rows
    struct TestData {
        int32_t id;
        std::string region;
        int64_t value;
    };
    std::vector<TestData> test_data = {{1, "US", 100}, {2, "US", 200}, {3, "EU", 300}, {4, "EU", 400}};

    for (const auto& d : test_data) {
        fluss::GenericRow row(3);
        row.SetInt32(0, d.id);
        row.SetString(1, d.region);
        row.SetInt64(2, d.value);
        ASSERT_OK(append_writer.Append(row));
    }
    ASSERT_OK(append_writer.Flush());

    // Append arrow batches per partition
    {
        auto id_builder = arrow::Int32Builder();
        id_builder.AppendValues({5, 6}).ok();
        auto region_builder = arrow::StringBuilder();
        region_builder.AppendValues({"US", "US"}).ok();
        auto value_builder = arrow::Int64Builder();
        value_builder.AppendValues({500, 600}).ok();

        auto batch = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("id", arrow::int32()),
                           arrow::field("region", arrow::utf8()),
                           arrow::field("value", arrow::int64())}),
            2,
            {id_builder.Finish().ValueOrDie(), region_builder.Finish().ValueOrDie(),
             value_builder.Finish().ValueOrDie()});

        ASSERT_OK(append_writer.AppendArrowBatch(batch));
    }

    {
        auto id_builder = arrow::Int32Builder();
        id_builder.AppendValues({7, 8}).ok();
        auto region_builder = arrow::StringBuilder();
        region_builder.AppendValues({"EU", "EU"}).ok();
        auto value_builder = arrow::Int64Builder();
        value_builder.AppendValues({700, 800}).ok();

        auto batch = arrow::RecordBatch::Make(
            arrow::schema({arrow::field("id", arrow::int32()),
                           arrow::field("region", arrow::utf8()),
                           arrow::field("value", arrow::int64())}),
            2,
            {id_builder.Finish().ValueOrDie(), region_builder.Finish().ValueOrDie(),
             value_builder.Finish().ValueOrDie()});

        ASSERT_OK(append_writer.AppendArrowBatch(batch));
    }
    ASSERT_OK(append_writer.Flush());

    // Test list partition offsets
    std::unordered_map<int32_t, int64_t> us_offsets;
    ASSERT_OK(adm.ListPartitionOffsets(table_path, "US", {0}, fluss::OffsetSpec::Latest(),
                                       us_offsets));
    EXPECT_EQ(us_offsets[0], 4) << "US partition should have 4 records";

    std::unordered_map<int32_t, int64_t> eu_offsets;
    ASSERT_OK(adm.ListPartitionOffsets(table_path, "EU", {0}, fluss::OffsetSpec::Latest(),
                                       eu_offsets));
    EXPECT_EQ(eu_offsets[0], 4) << "EU partition should have 4 records";

    // Subscribe to all partitions and scan
    fluss::Table scan_table;
    ASSERT_OK(conn.GetTable(table_path, scan_table));
    auto table_scan = scan_table.NewScan();
    fluss::LogScanner log_scanner;
    ASSERT_OK(table_scan.CreateLogScanner(log_scanner));

    std::vector<fluss::PartitionInfo> partition_infos;
    ASSERT_OK(adm.ListPartitionInfos(table_path, partition_infos));

    for (const auto& pi : partition_infos) {
        ASSERT_OK(log_scanner.SubscribePartitionBuckets(pi.partition_id, 0, 0));
    }

    // Collect all records
    using Record = std::tuple<int32_t, std::string, int64_t>;
    auto extract_record = [](const fluss::ScanRecord& rec) -> Record {
        return {rec.row.GetInt32(0), std::string(rec.row.GetString(1)), rec.row.GetInt64(2)};
    };
    std::vector<Record> collected;
    fluss_test::PollRecords(log_scanner, 8, extract_record, collected);

    ASSERT_EQ(collected.size(), 8u) << "Expected 8 records total";
    std::sort(collected.begin(), collected.end());

    std::vector<Record> expected = {{1, "US", 100},  {2, "US", 200},  {3, "EU", 300},
                                    {4, "EU", 400},  {5, "US", 500},  {6, "US", 600},
                                    {7, "EU", 700},  {8, "EU", 800}};
    EXPECT_EQ(collected, expected);

    // Test unsubscribe_partition: unsubscribe EU, should only get US data
    {
        fluss::Table unsub_table;
        ASSERT_OK(conn.GetTable(table_path, unsub_table));
        auto unsub_scan = unsub_table.NewScan();
        fluss::LogScanner unsub_scanner;
        ASSERT_OK(unsub_scan.CreateLogScanner(unsub_scanner));

        int64_t eu_partition_id = -1;
        for (const auto& pi : partition_infos) {
            ASSERT_OK(unsub_scanner.SubscribePartitionBuckets(pi.partition_id, 0, 0));
            if (pi.partition_name == "EU") {
                eu_partition_id = pi.partition_id;
            }
        }
        ASSERT_GE(eu_partition_id, 0) << "EU partition should exist";

        ASSERT_OK(unsub_scanner.UnsubscribePartition(eu_partition_id, 0));

        std::vector<Record> us_only;
        fluss_test::PollRecords(unsub_scanner, 4, extract_record, us_only);

        ASSERT_EQ(us_only.size(), 4u) << "Should receive exactly 4 US records";
        for (const auto& [id, region, val] : us_only) {
            EXPECT_EQ(region, "US") << "After unsubscribe EU, only US data should be read";
        }
    }

    // Test subscribe_partition_buckets (batch subscribe)
    {
        fluss::Table batch_table;
        ASSERT_OK(conn.GetTable(table_path, batch_table));
        auto batch_scan = batch_table.NewScan();
        fluss::LogScanner batch_scanner;
        ASSERT_OK(batch_scan.CreateLogScanner(batch_scanner));

        std::vector<fluss::PartitionBucketSubscription> subs;
        for (const auto& pi : partition_infos) {
            subs.push_back({pi.partition_id, 0, 0});
        }
        ASSERT_OK(batch_scanner.SubscribePartitionBuckets(subs));

        std::vector<Record> batch_collected;
        fluss_test::PollRecords(batch_scanner, 8, extract_record, batch_collected);
        ASSERT_EQ(batch_collected.size(), 8u);
        std::sort(batch_collected.begin(), batch_collected.end());
        EXPECT_EQ(batch_collected, expected);
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

// ============================================================================
// Array data type tests
// ============================================================================

TEST_F(LogTableTest, AppendAndScanWithArray) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_append_scan_with_array_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("tags", fluss::DataType::Array(fluss::DataType::String()))
                      .AddColumn("scores", fluss::DataType::Array(fluss::DataType::Int()))
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketCount(1)
                                .SetBucketKeys({"id"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto info = table.GetTableInfo();
    ASSERT_GE(info.schema.columns.size(), 3u);
    const auto& tags_type = info.schema.columns[1].data_type;
    ASSERT_EQ(tags_type.id(), fluss::TypeId::Array);
    ASSERT_NE(tags_type.element_type(), nullptr);
    ASSERT_EQ(tags_type.element_type()->id(), fluss::TypeId::String);
    const auto& scores_type = info.schema.columns[2].data_type;
    ASSERT_EQ(scores_type.id(), fluss::TypeId::Array);
    ASSERT_NE(scores_type.element_type(), nullptr);
    ASSERT_EQ(scores_type.element_type()->id(), fluss::TypeId::Int);

    fluss::AppendWriter append_writer;
    ASSERT_OK(table.NewAppend().CreateWriter(append_writer));

    {
        auto row = table.NewRow();
        row.Set("id", 1);

        fluss::ArrayWriter tags(2, fluss::DataType::String());
        tags.SetString(0, "hello");
        tags.SetString(1, "world");
        row.SetArray(1, std::move(tags));

        fluss::ArrayWriter scores(3, fluss::DataType::Int());
        scores.SetInt32(0, 10);
        scores.SetInt32(1, 20);
        scores.SetInt32(2, 30);
        row.SetArray(2, std::move(scores));

        ASSERT_OK(append_writer.Append(row));
    }
    {
        auto row = table.NewRow();
        row.Set("id", 2);

        fluss::ArrayWriter tags(1, fluss::DataType::String());
        tags.SetNull(0);
        row.SetArray(1, std::move(tags));

        fluss::ArrayWriter scores(0, fluss::DataType::Int());
        row.SetArray(2, std::move(scores));

        ASSERT_OK(append_writer.Append(row));
    }

    ASSERT_OK(append_writer.Flush());

    auto scan = table.NewScan();
    fluss::LogScanner scanner;
    ASSERT_OK(scan.CreateLogScanner(scanner));
    ASSERT_OK(scanner.Subscribe(0, 0));

    struct Record {
        int32_t id;
        size_t tag_count;
        std::vector<std::string> tags;
        size_t score_count;
        std::vector<int32_t> scores;
    };

    std::vector<Record> collected;
    auto extract = [](const fluss::ScanRecord& scan_rec) {
        const auto& rv = scan_rec.row;
        Record rec;
        rec.id = rv.GetInt32(0);

        rec.tag_count = rv.GetArraySize(1);
        for (size_t i = 0; i < rec.tag_count; ++i) {
            if (rv.IsArrayElementNull(1, i)) {
                rec.tags.push_back("<null>");
            } else {
                rec.tags.push_back(rv.GetArrayString(1, i));
            }
        }

        rec.score_count = rv.GetArraySize(2);
        for (size_t i = 0; i < rec.score_count; ++i) {
            rec.scores.push_back(rv.GetArrayInt32(2, i));
        }

        return rec;
    };

    fluss_test::PollRecords(scanner, 2, extract, collected);

    ASSERT_EQ(collected.size(), 2u);

    std::sort(collected.begin(), collected.end(),
              [](const Record& a, const Record& b) { return a.id < b.id; });

    EXPECT_EQ(collected[0].id, 1);
    ASSERT_EQ(collected[0].tag_count, 2u);
    EXPECT_EQ(collected[0].tags[0], "hello");
    EXPECT_EQ(collected[0].tags[1], "world");
    ASSERT_EQ(collected[0].score_count, 3u);
    EXPECT_EQ(collected[0].scores[0], 10);
    EXPECT_EQ(collected[0].scores[1], 20);
    EXPECT_EQ(collected[0].scores[2], 30);

    EXPECT_EQ(collected[1].id, 2);
    ASSERT_EQ(collected[1].tag_count, 1u);
    EXPECT_EQ(collected[1].tags[0], "<null>");
    ASSERT_EQ(collected[1].score_count, 0u);

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, AppendAndScanWithNestedArray) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_append_scan_nested_array_cpp");

    auto schema =
        fluss::Schema::NewBuilder()
            .AddColumn("id", fluss::DataType::Int())
            .AddColumn("matrix",
                        fluss::DataType::Array(fluss::DataType::Array(fluss::DataType::Int())))
            .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketCount(1)
                                .SetBucketKeys({"id"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    fluss::AppendWriter append_writer;
    ASSERT_OK(table.NewAppend().CreateWriter(append_writer));

    {
        auto row = table.NewRow();
        row.Set("id", 1);

        fluss::ArrayWriter inner1(2, fluss::DataType::Int());
        inner1.SetInt32(0, 1);
        inner1.SetInt32(1, 2);

        fluss::ArrayWriter inner2(2, fluss::DataType::Int());
        inner2.SetInt32(0, 3);
        inner2.SetInt32(1, 4);

        fluss::ArrayWriter outer(2, fluss::DataType::Array(fluss::DataType::Int()));
        outer.SetArray(0, std::move(inner1));
        outer.SetArray(1, std::move(inner2));

        row.SetArray(1, std::move(outer));
        ASSERT_OK(append_writer.Append(row));
    }

    ASSERT_OK(append_writer.Flush());

    auto scan = table.NewScan();
    fluss::LogScanner scanner;
    ASSERT_OK(scan.CreateLogScanner(scanner));
    ASSERT_OK(scanner.Subscribe(0, 0));

    struct Record {
        int32_t id;
        size_t outer_count;
        fluss::TypeId element_type;
        std::vector<std::vector<int32_t>> values;
    };

    std::vector<Record> collected;
    auto extract = [](const fluss::ScanRecord& scan_rec) {
        const auto& rv = scan_rec.row;
        Record rec;
        rec.id = rv.GetInt32(0);
        rec.outer_count = rv.GetArraySize(1);
        rec.element_type = rv.GetArrayElementType(1);
        auto outer = rv.GetArrayView(1);
        rec.values.reserve(outer.Size());
        for (size_t i = 0; i < outer.Size(); ++i) {
            auto inner = outer.GetArray(i);
            std::vector<int32_t> row;
            row.reserve(inner.Size());
            for (size_t j = 0; j < inner.Size(); ++j) {
                row.push_back(inner.GetInt32(j));
            }
            rec.values.push_back(std::move(row));
        }
        return rec;
    };

    fluss_test::PollRecords(scanner, 1, extract, collected);
    ASSERT_EQ(collected.size(), 1u);
    EXPECT_EQ(collected[0].id, 1);
    EXPECT_EQ(collected[0].outer_count, 2u);
    EXPECT_EQ(collected[0].element_type, fluss::TypeId::Array);
    ASSERT_EQ(collected[0].values.size(), 2u);
    EXPECT_EQ(collected[0].values[0], (std::vector<int32_t>{1, 2}));
    EXPECT_EQ(collected[0].values[1], (std::vector<int32_t>{3, 4}));

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, AppendAndScanWithArrayRichTypes) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_append_scan_array_rich_types_cpp");

    auto schema =
        fluss::Schema::NewBuilder()
            .AddColumn("id", fluss::DataType::Int())
            .AddColumn("arr_bytes", fluss::DataType::Array(fluss::DataType::Bytes()))
            .AddColumn("arr_date", fluss::DataType::Array(fluss::DataType::Date()))
            .AddColumn("arr_time", fluss::DataType::Array(fluss::DataType::Time()))
            .AddColumn("arr_ts", fluss::DataType::Array(fluss::DataType::Timestamp(6)))
            .AddColumn("arr_decimal", fluss::DataType::Array(fluss::DataType::Decimal(10, 2)))
            .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketCount(1)
                                .SetBucketKeys({"id"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();
    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));
    fluss::AppendWriter append_writer;
    ASSERT_OK(table.NewAppend().CreateWriter(append_writer));

    {
        auto row = table.NewRow();
        row.Set("id", 1);

        fluss::ArrayWriter arr_bytes(2, fluss::DataType::Bytes());
        arr_bytes.SetBytes(0, std::vector<uint8_t>{0x10, 0x20, 0x30});
        arr_bytes.SetNull(1);
        row.SetArray(1, std::move(arr_bytes));

        fluss::ArrayWriter arr_date(2, fluss::DataType::Date());
        auto d0 = fluss::Date::FromDays(20000);
        arr_date.SetDate(0, d0);
        arr_date.SetNull(1);
        row.SetArray(2, std::move(arr_date));

        fluss::ArrayWriter arr_time(1, fluss::DataType::Time());
        auto t0 = fluss::Time::FromMillis(3600000);
        arr_time.SetTime(0, t0);
        row.SetArray(3, std::move(arr_time));

        fluss::ArrayWriter arr_ts(1, fluss::DataType::Timestamp(6));
        auto ts0 = fluss::Timestamp::FromMillisNanos(1769163227123, 456000);
        arr_ts.SetTimestampNtz(0, ts0);
        row.SetArray(4, std::move(arr_ts));

        fluss::ArrayWriter arr_decimal(2, fluss::DataType::Decimal(10, 2));
        arr_decimal.SetDecimal(0, "123.45");
        arr_decimal.SetNull(1);
        row.SetArray(5, std::move(arr_decimal));

        ASSERT_OK(append_writer.Append(row));
    }

    ASSERT_OK(append_writer.Flush());

    auto scan = table.NewScan();
    fluss::LogScanner scanner;
    ASSERT_OK(scan.CreateLogScanner(scanner));
    ASSERT_OK(scanner.Subscribe(0, 0));

    fluss::ScanRecords records;
    ASSERT_OK(scanner.Poll(10000, records));
    ASSERT_EQ(records.Count(), 1u);

    auto it = records.begin();
    ASSERT_TRUE(it != records.end());
    auto rec = *it;
    const auto& rv = rec.row;

    EXPECT_EQ(rv.GetArraySize(1), 2u);
    auto bytes0 = rv.GetArrayBytes(1, 0);
    ASSERT_EQ(bytes0.size(), 3u);
    EXPECT_EQ(bytes0[0], 0x10);
    EXPECT_EQ(bytes0[1], 0x20);
    EXPECT_EQ(bytes0[2], 0x30);
    EXPECT_TRUE(rv.IsArrayElementNull(1, 1));

    EXPECT_EQ(rv.GetArraySize(2), 2u);
    EXPECT_EQ(rv.GetArrayDate(2, 0).days_since_epoch, fluss::Date::FromDays(20000).days_since_epoch);
    EXPECT_TRUE(rv.IsArrayElementNull(2, 1));

    EXPECT_EQ(rv.GetArraySize(3), 1u);
    EXPECT_EQ(rv.GetArrayTime(3, 0).millis_since_midnight, fluss::Time::FromMillis(3600000).millis_since_midnight);

    EXPECT_EQ(rv.GetArraySize(4), 1u);
    auto ts = rv.GetArrayTimestamp(4, 0);
    EXPECT_EQ(ts.epoch_millis, 1769163227123);
    EXPECT_EQ(ts.nano_of_millisecond, 456000);

    EXPECT_EQ(rv.GetArraySize(5), 2u);
    EXPECT_EQ(rv.GetArrayDecimalString(5, 0), "123.45");
    EXPECT_TRUE(rv.IsArrayElementNull(5, 1));

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, ArrayApiValidationErrors) {
    // Type mismatch setter should fail through FFI Result propagation.
    {
        fluss::ArrayWriter bool_array(1, fluss::DataType::Boolean());
        bool threw = false;
        try {
            bool_array.SetInt32(0, 42);
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw);
    }

    auto& adm = admin();
    auto& conn = connection();
    fluss::TablePath table_path("fluss", "test_array_api_validation_errors_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("vals", fluss::DataType::Array(fluss::DataType::Int()))
                      .Build();
    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketCount(1)
                                .SetBucketKeys({"id"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();
    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));
    fluss::AppendWriter append_writer;
    ASSERT_OK(table.NewAppend().CreateWriter(append_writer));
    auto row = table.NewRow();
    row.Set("id", 1);
    fluss::ArrayWriter vals(2, fluss::DataType::Int());
    vals.SetInt32(0, 7);
    vals.SetNull(1);
    row.SetArray(1, std::move(vals));
    ASSERT_OK(append_writer.Append(row));
    ASSERT_OK(append_writer.Flush());

    auto scan = table.NewScan();
    fluss::LogScanner scanner;
    ASSERT_OK(scan.CreateLogScanner(scanner));
    ASSERT_OK(scanner.Subscribe(0, 0));
    fluss::ScanRecords records;
    ASSERT_OK(scanner.Poll(10000, records));
    ASSERT_EQ(records.Count(), 1u);
    auto it = records.begin();
    ASSERT_TRUE(it != records.end());
    auto rec = *it;

    bool oob_threw = false;
    try {
        (void)rec.row.GetArrayInt32(1, 5);
    } catch (const std::exception&) {
        oob_threw = true;
    }
    EXPECT_TRUE(oob_threw);

    bool wrong_type_threw = false;
    try {
        (void)rec.row.GetArrayInt64(1, 0);
    } catch (const std::exception&) {
        wrong_type_threw = true;
    }
    EXPECT_TRUE(wrong_type_threw);

    bool null_typed_getter_threw = false;
    try {
        (void)rec.row.GetArrayInt32(1, 1);
    } catch (const std::exception&) {
        null_typed_getter_threw = true;
    }
    EXPECT_TRUE(null_typed_getter_threw);

    auto view = rec.row.GetArrayView(1);
    EXPECT_EQ(view.Size(), 2u);
    EXPECT_TRUE(view.IsNull(1));

    bool view_wrong_type_threw = false;
    try {
        (void)view.GetInt64(0);
    } catch (const std::exception&) {
        view_wrong_type_threw = true;
    }
    EXPECT_TRUE(view_wrong_type_threw);

    bool view_null_typed_getter_threw = false;
    try {
        (void)view.GetInt32(1);
    } catch (const std::exception&) {
        view_null_typed_getter_threw = true;
    }
    EXPECT_TRUE(view_null_typed_getter_threw);

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, AppendAndScanWithArrayEncodingEdgeCases) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_array_encoding_edge_cases_cpp");

    auto schema =
        fluss::Schema::NewBuilder()
            .AddColumn("id", fluss::DataType::Int())
            .AddColumn("arr_long_str", fluss::DataType::Array(fluss::DataType::String()))
            .AddColumn("arr_big_decimal", fluss::DataType::Array(fluss::DataType::Decimal(22, 5)))
            .AddColumn("arr_ts_nano", fluss::DataType::Array(fluss::DataType::Timestamp(9)))
            .AddColumn("arr_float", fluss::DataType::Array(fluss::DataType::Float()))
            .AddColumn("arr_double", fluss::DataType::Array(fluss::DataType::Double()))
            .AddColumn("arr_binary", fluss::DataType::Array(fluss::DataType::Binary(4)))
            .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketCount(1)
                                .SetBucketKeys({"id"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();
    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));
    fluss::AppendWriter append_writer;
    ASSERT_OK(table.NewAppend().CreateWriter(append_writer));

    {
        auto row = table.NewRow();
        row.Set("id", 1);

        // >= 8 bytes forces the heap-pointer variable-length path (threshold: 7)
        fluss::ArrayWriter arr_long_str(2, fluss::DataType::String());
        arr_long_str.SetString(0, "abcdefgh");
        arr_long_str.SetString(1, "this is a much longer string that definitely exceeds inline");
        row.SetArray(1, std::move(arr_long_str));

        // precision > 18 forces non-compact decimal encoding
        fluss::ArrayWriter arr_big_decimal(2, fluss::DataType::Decimal(22, 5));
        arr_big_decimal.SetDecimal(0, "12345678901234567.12345");
        arr_big_decimal.SetDecimal(1, "-99999999999999999.99999");
        row.SetArray(2, std::move(arr_big_decimal));

        // precision > 3 forces non-compact timestamp (millis + nanos-of-millis)
        fluss::ArrayWriter arr_ts_nano(1, fluss::DataType::Timestamp(9));
        auto ts_nano = fluss::Timestamp::FromMillisNanos(1769163227123, 456789);
        arr_ts_nano.SetTimestampNtz(0, ts_nano);
        row.SetArray(3, std::move(arr_ts_nano));

        // IEEE 754 special values: NaN, +Infinity, -Infinity
        fluss::ArrayWriter arr_float(3, fluss::DataType::Float());
        arr_float.SetFloat32(0, std::numeric_limits<float>::quiet_NaN());
        arr_float.SetFloat32(1, std::numeric_limits<float>::infinity());
        arr_float.SetFloat32(2, -std::numeric_limits<float>::infinity());
        row.SetArray(4, std::move(arr_float));

        fluss::ArrayWriter arr_double(3, fluss::DataType::Double());
        arr_double.SetFloat64(0, std::numeric_limits<double>::quiet_NaN());
        arr_double.SetFloat64(1, std::numeric_limits<double>::infinity());
        arr_double.SetFloat64(2, -std::numeric_limits<double>::infinity());
        row.SetArray(5, std::move(arr_double));

        // Fixed-length binary
        fluss::ArrayWriter arr_binary(2, fluss::DataType::Binary(4));
        arr_binary.SetBytes(0, std::vector<uint8_t>{0xDE, 0xAD, 0xBE, 0xEF});
        arr_binary.SetNull(1);
        row.SetArray(6, std::move(arr_binary));

        ASSERT_OK(append_writer.Append(row));
    }

    ASSERT_OK(append_writer.Flush());

    auto scan = table.NewScan();
    fluss::LogScanner scanner;
    ASSERT_OK(scan.CreateLogScanner(scanner));
    ASSERT_OK(scanner.Subscribe(0, 0));

    fluss::ScanRecords records;
    ASSERT_OK(scanner.Poll(10000, records));
    ASSERT_EQ(records.Count(), 1u);

    auto it = records.begin();
    ASSERT_TRUE(it != records.end());
    auto rec = *it;
    const auto& rv = rec.row;

    // Long strings: heap-encoded variable-length round-trip
    EXPECT_EQ(rv.GetArraySize(1), 2u);
    EXPECT_EQ(rv.GetArrayString(1, 0), "abcdefgh");
    EXPECT_EQ(rv.GetArrayString(1, 1), "this is a much longer string that definitely exceeds inline");

    // Non-compact decimal (precision 22 > MAX_COMPACT_PRECISION 18)
    EXPECT_EQ(rv.GetArraySize(2), 2u);
    EXPECT_EQ(rv.GetArrayDecimalString(2, 0), "12345678901234567.12345");
    EXPECT_EQ(rv.GetArrayDecimalString(2, 1), "-99999999999999999.99999");

    // Non-compact timestamp (precision 9 > MAX_COMPACT_TIMESTAMP_PRECISION 3)
    EXPECT_EQ(rv.GetArraySize(3), 1u);
    auto ts = rv.GetArrayTimestamp(3, 0);
    EXPECT_EQ(ts.epoch_millis, 1769163227123);
    EXPECT_EQ(ts.nano_of_millisecond, 456789);

    // Float NaN / Infinity round-trip
    EXPECT_EQ(rv.GetArraySize(4), 3u);
    EXPECT_TRUE(std::isnan(rv.GetArrayFloat32(4, 0)));
    EXPECT_TRUE(std::isinf(rv.GetArrayFloat32(4, 1)));
    EXPECT_GT(rv.GetArrayFloat32(4, 1), 0.0f);
    EXPECT_TRUE(std::isinf(rv.GetArrayFloat32(4, 2)));
    EXPECT_LT(rv.GetArrayFloat32(4, 2), 0.0f);

    // Double NaN / Infinity round-trip
    EXPECT_EQ(rv.GetArraySize(5), 3u);
    EXPECT_TRUE(std::isnan(rv.GetArrayFloat64(5, 0)));
    EXPECT_TRUE(std::isinf(rv.GetArrayFloat64(5, 1)));
    EXPECT_GT(rv.GetArrayFloat64(5, 1), 0.0);
    EXPECT_TRUE(std::isinf(rv.GetArrayFloat64(5, 2)));
    EXPECT_LT(rv.GetArrayFloat64(5, 2), 0.0);

    // Fixed-length binary round-trip
    EXPECT_EQ(rv.GetArraySize(6), 2u);
    auto bin = rv.GetArrayBytes(6, 0);
    ASSERT_EQ(bin.size(), 4u);
    EXPECT_EQ(bin[0], 0xDE);
    EXPECT_EQ(bin[1], 0xAD);
    EXPECT_EQ(bin[2], 0xBE);
    EXPECT_EQ(bin[3], 0xEF);
    EXPECT_TRUE(rv.IsArrayElementNull(6, 1));

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(LogTableTest, ArrayWriterOverflowDetection) {
    // SetInt32 on TINYINT array must throw when value overflows i8 range (-128..127)
    {
        fluss::ArrayWriter tinyint_arr(1, fluss::DataType::TinyInt());
        EXPECT_EQ(tinyint_arr.Size(), 1u);
        bool threw = false;
        try {
            tinyint_arr.SetInt32(0, 1000);
        } catch (const std::exception& e) {
            threw = true;
            std::string msg(e.what());
            EXPECT_NE(msg.find("TINYINT"), std::string::npos);
        }
        EXPECT_TRUE(threw);
    }

    // SetInt32 on SMALLINT array must throw when value overflows i16 range (-32768..32767)
    {
        fluss::ArrayWriter smallint_arr(1, fluss::DataType::SmallInt());
        bool threw = false;
        try {
            smallint_arr.SetInt32(0, 40000);
        } catch (const std::exception& e) {
            threw = true;
            std::string msg(e.what());
            EXPECT_NE(msg.find("SMALLINT"), std::string::npos);
        }
        EXPECT_TRUE(threw);
    }

    // Negative overflow: -200 doesn't fit TINYINT
    {
        fluss::ArrayWriter tinyint_arr(1, fluss::DataType::TinyInt());
        bool threw = false;
        try {
            tinyint_arr.SetInt32(0, -200);
        } catch (const std::exception&) {
            threw = true;
        }
        EXPECT_TRUE(threw);
    }

    // Values within range must succeed
    {
        fluss::ArrayWriter tinyint_arr(1, fluss::DataType::TinyInt());
        EXPECT_NO_THROW(tinyint_arr.SetInt32(0, 127));
    }
    {
        fluss::ArrayWriter tinyint_arr(1, fluss::DataType::TinyInt());
        EXPECT_NO_THROW(tinyint_arr.SetInt32(0, -128));
    }
    {
        fluss::ArrayWriter smallint_arr(1, fluss::DataType::SmallInt());
        EXPECT_NO_THROW(smallint_arr.SetInt32(0, 32767));
    }
}

TEST_F(LogTableTest, NullabilityPreservedInTableInfo) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_nullability_table_info_cpp");

    auto schema =
        fluss::Schema::NewBuilder()
            .AddColumn("id", fluss::DataType::Int())
            .AddColumn("name", fluss::DataType::String())
            .AddColumn("tags", fluss::DataType::Array(fluss::DataType::String().NotNull()))
            .AddColumn("ids", fluss::DataType::Array(fluss::DataType::Int()).NotNull())
            .AddColumn("nested",
                       fluss::DataType::Array(
                           fluss::DataType::Array(fluss::DataType::Int()).NotNull()))
            .SetPrimaryKeys({"id"})
            .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));
    auto info = table.GetTableInfo();

    ASSERT_EQ(info.schema.columns.size(), 5u);
    EXPECT_EQ(info.primary_keys, std::vector<std::string>{"id"});

    // Primary key columns are forced NOT NULL by schema normalization.
    EXPECT_EQ(info.schema.columns[0].data_type.id(), fluss::TypeId::Int);
    EXPECT_FALSE(info.schema.columns[0].data_type.nullable());

    // "name" STRING (nullable)
    EXPECT_EQ(info.schema.columns[1].data_type.id(), fluss::TypeId::String);
    EXPECT_TRUE(info.schema.columns[1].data_type.nullable());

    // "tags" ARRAY<STRING NOT NULL> (outer nullable)
    EXPECT_EQ(info.schema.columns[2].data_type.id(), fluss::TypeId::Array);
    EXPECT_TRUE(info.schema.columns[2].data_type.nullable());
    ASSERT_NE(info.schema.columns[2].data_type.element_type(), nullptr);
    EXPECT_FALSE(info.schema.columns[2].data_type.element_type()->nullable());

    // "ids" ARRAY<INT> NOT NULL (outer not null, element nullable)
    EXPECT_EQ(info.schema.columns[3].data_type.id(), fluss::TypeId::Array);
    EXPECT_FALSE(info.schema.columns[3].data_type.nullable());
    ASSERT_NE(info.schema.columns[3].data_type.element_type(), nullptr);
    EXPECT_TRUE(info.schema.columns[3].data_type.element_type()->nullable());

    // "nested" ARRAY<ARRAY<INT> NOT NULL> (outer nullable, inner array not null)
    EXPECT_EQ(info.schema.columns[4].data_type.id(), fluss::TypeId::Array);
    EXPECT_TRUE(info.schema.columns[4].data_type.nullable());
    ASSERT_NE(info.schema.columns[4].data_type.element_type(), nullptr);
    EXPECT_FALSE(info.schema.columns[4].data_type.element_type()->nullable());
    ASSERT_NE(info.schema.columns[4].data_type.element_type()->element_type(), nullptr);
    EXPECT_TRUE(info.schema.columns[4].data_type.element_type()->element_type()->nullable());

    ASSERT_OK(adm.DropTable(table_path, false));
}
