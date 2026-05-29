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

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <chrono>
#include <iostream>
#include <unordered_map>
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
    // 1) Connect
    fluss::Configuration config;
    config.bootstrap_servers = "127.0.0.1:9123";

    fluss::Connection conn;
    check("create", fluss::Connection::Create(config, conn));

    // 2) Admin
    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));

    fluss::TablePath table_path("fluss", "sample_table_cpp_v1");

    // 2.1) Drop table if exists
    std::cout << "Dropping table if exists..." << std::endl;
    auto drop_result = admin.DropTable(table_path, true);
    if (drop_result.Ok()) {
        std::cout << "Table dropped successfully" << std::endl;
    } else {
        std::cout << "Table drop result: " << drop_result.error_message << std::endl;
    }

    // 3) Schema with scalar and temporal columns
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .AddColumn("score", fluss::DataType::Float())
                      .AddColumn("age", fluss::DataType::Int())
                      .AddColumn("event_date", fluss::DataType::Date())
                      .AddColumn("event_time", fluss::DataType::Time())
                      .AddColumn("created_at", fluss::DataType::Timestamp())
                      .AddColumn("updated_at", fluss::DataType::TimestampLtz())
                      .Build();

    auto descriptor = fluss::TableDescriptor::NewBuilder()
                          .SetSchema(schema)
                          .SetBucketCount(3)
                          .SetComment("cpp example table with 3 buckets")
                          .Build();

    std::cout << "Creating table with 3 buckets..." << std::endl;
    check("create_table", admin.CreateTable(table_path, descriptor, false));

    // 4) Get table
    fluss::Table table;
    check("get_table", conn.GetTable(table_path, table));

    // 5) Write rows with scalar and temporal values
    fluss::AppendWriter writer;
    check("new_append_writer", table.NewAppend().CreateWriter(writer));

    struct RowData {
        int id;
        const char* name;
        float score;
        int age;
        fluss::Date date;
        fluss::Time time;
        fluss::Timestamp ts_ntz;
        fluss::Timestamp ts_ltz;
    };

    auto tp_now = std::chrono::system_clock::now();
    std::vector<RowData> rows = {
        {1, "Alice", 95.2f, 25, fluss::Date::FromYMD(2024, 6, 15), fluss::Time::FromHMS(14, 30, 45),
         fluss::Timestamp::FromTimePoint(tp_now), fluss::Timestamp::FromMillis(1718467200000)},
        {2, "Bob", 87.2f, 30, fluss::Date::FromYMD(2025, 1, 1), fluss::Time::FromHMS(0, 0, 0),
         fluss::Timestamp::FromMillis(1735689600000),
         fluss::Timestamp::FromMillisNanos(1735689600000, 500000)},
        {3, "Charlie", 92.1f, 35, fluss::Date::FromYMD(1999, 12, 31),
         fluss::Time::FromHMS(23, 59, 59), fluss::Timestamp::FromMillis(946684799999),
         fluss::Timestamp::FromMillis(946684799999)},
    };

    // Fire-and-forget: queue rows, flush at end
    for (const auto& r : rows) {
        fluss::GenericRow row;
        row.SetInt32(0, r.id);
        row.SetString(1, r.name);
        row.SetFloat32(2, r.score);
        row.SetInt32(3, r.age);
        row.SetDate(4, r.date);
        row.SetTime(5, r.time);
        row.SetTimestampNtz(6, r.ts_ntz);
        row.SetTimestampLtz(7, r.ts_ltz);
        check("append", writer.Append(row));
    }
    check("flush", writer.Flush());
    std::cout << "Wrote " << rows.size() << " rows (fire-and-forget + flush)" << std::endl;

    // Per-record acknowledgment
    {
        fluss::GenericRow row;
        row.SetInt32(0, 100);
        row.SetString(1, "AckTest");
        row.SetFloat32(2, 99.9f);
        row.SetInt32(3, 42);
        row.SetDate(4, fluss::Date::FromYMD(2025, 3, 1));
        row.SetTime(5, fluss::Time::FromHMS(12, 0, 0));
        row.SetTimestampNtz(6, fluss::Timestamp::FromMillis(1740787200000));
        row.SetTimestampLtz(7, fluss::Timestamp::FromMillis(1740787200000));
        fluss::WriteResult wr;
        check("append", writer.Append(row, wr));
        check("wait", wr.Wait());
        std::cout << "Row acknowledged by server" << std::endl;
    }

    // Append a row with all fields null (matches Rust log_table.rs all_supported_datatypes)
    {
        fluss::GenericRow row;
        size_t field_count = 8;
        for (size_t i = 0; i < field_count; ++i) {
            row.SetNull(i);
        }
        check("append_null_row", writer.Append(row));
    }
    check("flush_null", writer.Flush());
    std::cout << "Wrote row with all fields null" << std::endl;

    // 6) Full scan — verify all column types including temporal
    fluss::LogScanner scanner;
    check("new_log_scanner", table.NewScan().CreateLogScanner(scanner));

    auto info = table.GetTableInfo();
    int buckets = info.num_buckets;
    for (int b = 0; b < buckets; ++b) {
        check("subscribe", scanner.Subscribe(b, 0));
    }

    fluss::ScanRecords records;
    check("poll", scanner.Poll(5000, records));

    // Flat iteration over all records (regardless of bucket)
    std::cout << "Scanned records: " << records.Count() << " across " << records.BucketCount()
              << " buckets" << std::endl;
    for (const auto& rec : records) {
        std::cout << "  offset=" << rec.offset << " timestamp=" << rec.timestamp << std::endl;
    }

    // Per-bucket access (with type verification)
    bool scan_ok = true;
    bool found_null_row = false;
    for (const auto& tb : records.Buckets()) {
        auto view = records.Records(tb);
        std::cout << "  Bucket " << tb.bucket_id;
        if (tb.partition_id.has_value()) {
            std::cout << " (partition=" << *tb.partition_id << ")";
        }
        std::cout << ": " << view.Size() << " records" << std::endl;
        for (const auto& rec : view) {
            // Check if this is the all-null row
            if (rec.row.IsNull(0)) {
                found_null_row = true;
                for (size_t i = 0; i < rec.row.FieldCount(); ++i) {
                    if (!rec.row.IsNull(i)) {
                        std::cerr << "ERROR: column " << i << " should be null" << std::endl;
                        scan_ok = false;
                    }
                }
                std::cout << "    [null row] all " << rec.row.FieldCount() << " fields are null"
                          << std::endl;
                continue;
            }

            // Non-null rows: verify types
            if (rec.row.GetType(4) != fluss::TypeId::Date) {
                std::cerr << "ERROR: field 4 expected Date, got "
                          << static_cast<int>(rec.row.GetType(4)) << std::endl;
                scan_ok = false;
            }
            if (rec.row.GetType(5) != fluss::TypeId::Time) {
                std::cerr << "ERROR: field 5 expected Time, got "
                          << static_cast<int>(rec.row.GetType(5)) << std::endl;
                scan_ok = false;
            }
            if (rec.row.GetType(6) != fluss::TypeId::Timestamp) {
                std::cerr << "ERROR: field 6 expected Timestamp, got "
                          << static_cast<int>(rec.row.GetType(6)) << std::endl;
                scan_ok = false;
            }
            if (rec.row.GetType(7) != fluss::TypeId::TimestampLtz) {
                std::cerr << "ERROR: field 7 expected TimestampLtz, got "
                          << static_cast<int>(rec.row.GetType(7)) << std::endl;
                scan_ok = false;
            }

            // Name-based getters
            auto date = rec.row.GetDate("event_date");
            auto time = rec.row.GetTime("event_time");
            auto ts_ntz = rec.row.GetTimestamp("created_at");
            auto ts_ltz = rec.row.GetTimestamp("updated_at");

            std::cout << "    id=" << rec.row.GetInt32("id")
                      << " name=" << rec.row.GetString("name")
                      << " score=" << rec.row.GetFloat32("score")
                      << " age=" << rec.row.GetInt32("age") << " date=" << date.Year() << "-"
                      << date.Month() << "-" << date.Day() << " time=" << time.Hour() << ":"
                      << time.Minute() << ":" << time.Second() << " ts_ntz=" << ts_ntz.epoch_millis
                      << " ts_ltz=" << ts_ltz.epoch_millis << "+" << ts_ltz.nano_of_millisecond
                      << "ns" << std::endl;
        }
    }

    if (!found_null_row) {
        std::cerr << "ERROR: did not find the all-null row" << std::endl;
        scan_ok = false;
    }

    if (!scan_ok) {
        std::cerr << "Full scan type verification FAILED!" << std::endl;
        std::exit(1);
    }

    // 7a) Projected scan by index — project [id, updated_at(TimestampLtz)] to verify
    //     NTZ/LTZ disambiguation works with column index remapping
    std::vector<size_t> projected_columns = {0, 7};
    fluss::LogScanner projected_scanner;
    check("new_log_scanner_with_projection",
          table.NewScan().ProjectByIndex(projected_columns).CreateLogScanner(projected_scanner));

    for (int b = 0; b < buckets; ++b) {
        check("subscribe_projected", projected_scanner.Subscribe(b, 0));
    }

    fluss::ScanRecords projected_records;
    check("poll_projected", projected_scanner.Poll(5000, projected_records));

    std::cout << "Projected records: " << projected_records.Count() << std::endl;
    for (const auto& tb : projected_records.Buckets()) {
        for (const auto& rec : projected_records.Records(tb)) {
            if (rec.row.FieldCount() != 2) {
                std::cerr << "ERROR: expected 2 fields, got " << rec.row.FieldCount() << std::endl;
                scan_ok = false;
                continue;
            }
            // Skip the all-null row
            if (rec.row.IsNull(0)) {
                std::cout << "  [null row] skipped" << std::endl;
                continue;
            }
            if (rec.row.GetType(0) != fluss::TypeId::Int) {
                std::cerr << "ERROR: projected field 0 expected Int, got "
                          << static_cast<int>(rec.row.GetType(0)) << std::endl;
                scan_ok = false;
            }
            if (rec.row.GetType(1) != fluss::TypeId::TimestampLtz) {
                std::cerr << "ERROR: projected field 1 expected TimestampLtz, got "
                          << static_cast<int>(rec.row.GetType(1)) << std::endl;
                scan_ok = false;
            }

            auto ts = rec.row.GetTimestamp(1);
            std::cout << "  id=" << rec.row.GetInt32(0) << " updated_at=" << ts.epoch_millis << "+"
                      << ts.nano_of_millisecond << "ns" << std::endl;
        }
    }

    // 7b) Projected scan by column names — same columns as above but using names
    fluss::LogScanner name_projected_scanner;
    check("project_by_name_scanner", table.NewScan()
                                         .ProjectByName({"id", "updated_at"})
                                         .CreateLogScanner(name_projected_scanner));

    for (int b = 0; b < buckets; ++b) {
        check("subscribe_name_projected", name_projected_scanner.Subscribe(b, 0));
    }

    fluss::ScanRecords name_projected_records;
    check("poll_name_projected", name_projected_scanner.Poll(5000, name_projected_records));

    std::cout << "Name-projected records: " << name_projected_records.Count() << std::endl;
    for (const auto& tb : name_projected_records.Buckets()) {
        for (const auto& rec : name_projected_records.Records(tb)) {
            if (rec.row.FieldCount() != 2) {
                std::cerr << "ERROR: expected 2 fields, got " << rec.row.FieldCount() << std::endl;
                scan_ok = false;
                continue;
            }
            // Skip the all-null row
            if (rec.row.IsNull(0)) {
                std::cout << "  [null row] skipped" << std::endl;
                continue;
            }
            if (rec.row.GetType(0) != fluss::TypeId::Int) {
                std::cerr << "ERROR: name-projected field 0 expected Int, got "
                          << static_cast<int>(rec.row.GetType(0)) << std::endl;
                scan_ok = false;
            }
            if (rec.row.GetType(1) != fluss::TypeId::TimestampLtz) {
                std::cerr << "ERROR: name-projected field 1 expected TimestampLtz, got "
                          << static_cast<int>(rec.row.GetType(1)) << std::endl;
                scan_ok = false;
            }

            auto ts = rec.row.GetTimestamp(1);
            std::cout << "  id=" << rec.row.GetInt32(0) << " updated_at=" << ts.epoch_millis << "+"
                      << ts.nano_of_millisecond << "ns" << std::endl;
        }
    }

    if (scan_ok) {
        std::cout << "Scan verification passed!" << std::endl;
    } else {
        std::cerr << "Scan verification FAILED!" << std::endl;
        std::exit(1);
    }

    // 8) List offsets examples
    std::cout << "\n=== List Offsets Examples ===" << std::endl;

    std::vector<int32_t> all_bucket_ids;
    all_bucket_ids.reserve(buckets);
    for (int b = 0; b < buckets; ++b) {
        all_bucket_ids.push_back(b);
    }

    std::unordered_map<int32_t, int64_t> earliest_offsets;
    check("list_earliest_offsets",
          admin.ListOffsets(table_path, all_bucket_ids, fluss::OffsetSpec::Earliest(),
                            earliest_offsets));
    std::cout << "Earliest offsets:" << std::endl;
    for (const auto& [bucket_id, offset] : earliest_offsets) {
        std::cout << "  Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    std::unordered_map<int32_t, int64_t> latest_offsets;
    check("list_latest_offsets", admin.ListOffsets(table_path, all_bucket_ids,
                                                   fluss::OffsetSpec::Latest(), latest_offsets));
    std::cout << "Latest offsets:" << std::endl;
    for (const auto& [bucket_id, offset] : latest_offsets) {
        std::cout << "  Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    auto now = std::chrono::system_clock::now();
    auto one_hour_ago = now - std::chrono::hours(1);
    auto timestamp_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(one_hour_ago.time_since_epoch())
            .count();

    std::unordered_map<int32_t, int64_t> timestamp_offsets;
    check("list_timestamp_offsets",
          admin.ListOffsets(table_path, all_bucket_ids, fluss::OffsetSpec::Timestamp(timestamp_ms),
                            timestamp_offsets));
    std::cout << "Offsets for timestamp " << timestamp_ms << " (1 hour ago):" << std::endl;
    for (const auto& [bucket_id, offset] : timestamp_offsets) {
        std::cout << "  Bucket " << bucket_id << ": offset=" << offset << std::endl;
    }

    // 9) Batch subscribe
    std::cout << "\n=== Batch Subscribe Example ===" << std::endl;
    fluss::LogScanner batch_scanner;
    check("new_log_scanner_for_batch", table.NewScan().CreateLogScanner(batch_scanner));

    std::vector<fluss::BucketSubscription> subscriptions;
    for (const auto& [bucket_id, offset] : earliest_offsets) {
        subscriptions.push_back({bucket_id, offset});
        std::cout << "Preparing subscription: bucket=" << bucket_id << ", offset=" << offset
                  << std::endl;
    }

    check("subscribe_buckets", batch_scanner.Subscribe(subscriptions));
    std::cout << "Batch subscribed to " << subscriptions.size() << " buckets" << std::endl;

    fluss::ScanRecords batch_records;
    check("poll_batch", batch_scanner.Poll(5000, batch_records));

    std::cout << "Scanned " << batch_records.Count() << " records from batch subscription"
              << std::endl;
    for (const auto& tb : batch_records.Buckets()) {
        size_t shown = 0;
        for (const auto& rec : batch_records.Records(tb)) {
            if (shown < 5) {
                std::cout << "  bucket_id=" << tb.bucket_id << ", offset=" << rec.offset
                          << ", timestamp=" << rec.timestamp << std::endl;
            }
            ++shown;
        }
        if (shown > 5) {
            std::cout << "  ... and " << (shown - 5) << " more records in bucket " << tb.bucket_id
                      << std::endl;
        }
    }

    // 9.1) Unsubscribe from a bucket
    std::cout << "\n=== Unsubscribe Example ===" << std::endl;
    check("unsubscribe", batch_scanner.Unsubscribe(subscriptions[0].bucket_id));
    std::cout << "Unsubscribed from bucket " << subscriptions[0].bucket_id << std::endl;

    // 10) Arrow record batch polling
    std::cout << "\n=== Testing Arrow Record Batch Polling ===" << std::endl;

    fluss::LogScanner arrow_scanner;
    check("new_record_batch_log_scanner",
          table.NewScan().CreateRecordBatchLogScanner(arrow_scanner));

    for (int b = 0; b < buckets; ++b) {
        check("subscribe_arrow", arrow_scanner.Subscribe(b, 0));
    }

    fluss::ArrowRecordBatches arrow_batches;
    check("poll_record_batch", arrow_scanner.PollRecordBatch(5000, arrow_batches));

    std::cout << "Polled " << arrow_batches.Size() << " Arrow record batches" << std::endl;
    for (size_t i = 0; i < arrow_batches.Size(); ++i) {
        const auto& batch = arrow_batches[i];
        if (batch->Available()) {
            std::cout << "  Batch " << i << ": " << batch->GetArrowRecordBatch()->num_rows()
                      << " rows" << std::endl;
        } else {
            std::cout << "  Batch " << i << ": not available" << std::endl;
        }
    }

    // 11) Arrow record batch polling with projection
    std::cout << "\n=== Testing Arrow Record Batch Polling with Projection ===" << std::endl;

    fluss::LogScanner projected_arrow_scanner;
    check("new_record_batch_log_scanner_with_projection",
          table.NewScan()
              .ProjectByIndex(projected_columns)
              .CreateRecordBatchLogScanner(projected_arrow_scanner));

    for (int b = 0; b < buckets; ++b) {
        check("subscribe_projected_arrow", projected_arrow_scanner.Subscribe(b, 0));
    }

    fluss::ArrowRecordBatches projected_arrow_batches;
    check("poll_projected_record_batch",
          projected_arrow_scanner.PollRecordBatch(5000, projected_arrow_batches));

    std::cout << "Polled " << projected_arrow_batches.Size() << " projected Arrow record batches"
              << std::endl;
    for (size_t i = 0; i < projected_arrow_batches.Size(); ++i) {
        const auto& batch = projected_arrow_batches[i];
        if (batch->Available()) {
            std::cout << "  Batch " << i << ": " << batch->GetArrowRecordBatch()->num_rows()
                      << " rows" << std::endl;
        } else {
            std::cout << "  Batch " << i << ": not available" << std::endl;
        }
    }

    // 12) AppendArrowBatch — write an Arrow RecordBatch directly
    std::cout << "\n=== AppendArrowBatch Example ===" << std::endl;
    {
        // Build an Arrow RecordBatch matching sample_table_cpp_v1 schema:
        //   id:INT, name:STRING, score:FLOAT, age:INT,
        //   event_date:DATE, event_time:TIME, created_at:TIMESTAMP, updated_at:TIMESTAMP_LTZ
        auto arrow_schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("name", arrow::utf8()),
            arrow::field("score", arrow::float32()),
            arrow::field("age", arrow::int32()),
            arrow::field("event_date", arrow::date32()),
            arrow::field("event_time", arrow::time32(arrow::TimeUnit::MILLI)),
            arrow::field("created_at", arrow::timestamp(arrow::TimeUnit::MICRO)),
            arrow::field("updated_at", arrow::timestamp(arrow::TimeUnit::MICRO)),
        });

        arrow::Int32Builder id_builder;
        arrow::StringBuilder name_builder;
        arrow::FloatBuilder score_builder;
        arrow::Int32Builder age_builder;
        arrow::Date32Builder date_builder;
        arrow::Time32Builder time_builder(arrow::time32(arrow::TimeUnit::MILLI),
                                          arrow::default_memory_pool());
        arrow::TimestampBuilder ts_ntz_builder(arrow::timestamp(arrow::TimeUnit::MICRO),
                                               arrow::default_memory_pool());
        arrow::TimestampBuilder ts_ltz_builder(arrow::timestamp(arrow::TimeUnit::MICRO),
                                               arrow::default_memory_pool());

        // Row 1
        (void)id_builder.Append(200);
        (void)name_builder.Append("ArrowAlice");
        (void)score_builder.Append(88.5f);
        (void)age_builder.Append(28);
        (void)date_builder.Append(19888);               // days since epoch (2024-06-15 ≈ 19888)
        (void)time_builder.Append(52245000);            // 14:30:45 in ms
        (void)ts_ntz_builder.Append(1718467200000000);  // micros
        (void)ts_ltz_builder.Append(1718467200000000);

        // Row 2
        (void)id_builder.Append(201);
        (void)name_builder.Append("ArrowBob");
        (void)score_builder.Append(91.3f);
        (void)age_builder.Append(33);
        (void)date_builder.Append(20089);    // 2025-01-02
        (void)time_builder.Append(3600000);  // 01:00:00
        (void)ts_ntz_builder.Append(1735689600000000);
        (void)ts_ltz_builder.Append(1735689600000000);

        auto batch_result = arrow::RecordBatch::Make(
            arrow_schema, 2,
            {id_builder.Finish().ValueOrDie(), name_builder.Finish().ValueOrDie(),
             score_builder.Finish().ValueOrDie(), age_builder.Finish().ValueOrDie(),
             date_builder.Finish().ValueOrDie(), time_builder.Finish().ValueOrDie(),
             ts_ntz_builder.Finish().ValueOrDie(), ts_ltz_builder.Finish().ValueOrDie()});

        check("append_arrow_batch", writer.AppendArrowBatch(batch_result));
        check("flush_arrow", writer.Flush());
        std::cout << "Wrote 2 rows via AppendArrowBatch" << std::endl;

        // Verify by scanning from latest offsets
        fluss::LogScanner arrow_write_scanner;
        check("new_arrow_write_scanner", table.NewScan().CreateLogScanner(arrow_write_scanner));
        for (const auto& [bid, off] : latest_offsets) {
            check("subscribe_arrow_write", arrow_write_scanner.Subscribe(bid, off));
        }

        fluss::ScanRecords arrow_write_records;
        check("poll_arrow_write", arrow_write_scanner.Poll(5000, arrow_write_records));
        std::cout << "Scanned " << arrow_write_records.Count()
                  << " records written via AppendArrowBatch:" << std::endl;
        for (const auto& tb : arrow_write_records.Buckets()) {
            for (const auto& rec : arrow_write_records.Records(tb)) {
                std::cout << "  id=" << rec.row.GetInt32(0) << " name=" << rec.row.GetString(1)
                          << " score=" << rec.row.GetFloat32(2) << std::endl;
            }
        }
    }

    // 13) Decimal support example
    std::cout << "\n=== Decimal Support Example ===" << std::endl;

    fluss::TablePath decimal_table_path("fluss", "decimal_table_cpp_v1");

    // Drop table if exists
    admin.DropTable(decimal_table_path, true);

    // Create schema with decimal columns
    auto decimal_schema = fluss::Schema::NewBuilder()
                              .AddColumn("id", fluss::DataType::Int())
                              .AddColumn("price", fluss::DataType::Decimal(10, 2))   // compact
                              .AddColumn("amount", fluss::DataType::Decimal(28, 8))  // i128
                              .Build();

    auto decimal_descriptor = fluss::TableDescriptor::NewBuilder()
                                  .SetSchema(decimal_schema)
                                  .SetBucketCount(1)
                                  .SetComment("cpp decimal example table")
                                  .Build();

    check("create_decimal_table", admin.CreateTable(decimal_table_path, decimal_descriptor, false));

    // Get table and writer
    fluss::Table decimal_table;
    check("get_decimal_table", conn.GetTable(decimal_table_path, decimal_table));

    fluss::AppendWriter decimal_writer;
    check("new_decimal_writer", decimal_table.NewAppend().CreateWriter(decimal_writer));

    // Just provide the value — Rust resolves (p,s) from schema
    {
        fluss::GenericRow row;
        row.SetInt32(0, 1);
        row.SetDecimal(1, "123.45");      // Rust knows DECIMAL(10,2)
        row.SetDecimal(2, "1.00000000");  // Rust knows DECIMAL(28,8)
        check("append_decimal", decimal_writer.Append(row));
    }
    {
        fluss::GenericRow row;
        row.SetInt32(0, 2);
        row.SetDecimal(1, "-999.99");
        row.SetDecimal(2, "3.14159265");
        check("append_decimal", decimal_writer.Append(row));
    }
    {
        fluss::GenericRow row;
        row.SetInt32(0, 3);
        row.SetDecimal(1, "500.00");
        row.SetDecimal(2, "2.71828182");
        check("append_decimal", decimal_writer.Append(row));
    }
    check("flush_decimal", decimal_writer.Flush());
    std::cout << "Wrote 3 decimal rows" << std::endl;

    // Scan and read back
    fluss::LogScanner decimal_scanner;
    check("new_decimal_scanner", decimal_table.NewScan().CreateLogScanner(decimal_scanner));
    check("subscribe_decimal", decimal_scanner.Subscribe(0, 0));

    fluss::ScanRecords decimal_records;
    check("poll_decimal", decimal_scanner.Poll(5000, decimal_records));

    std::cout << "Scanned decimal records: " << decimal_records.Count() << std::endl;
    for (const auto& tb : decimal_records.Buckets()) {
        for (const auto& rec : decimal_records.Records(tb)) {
            std::cout << "  id=" << rec.row.GetInt32(0) << " price=" << rec.row.GetDecimalString(1)
                      << " amount=" << rec.row.GetDecimalString(2)
                      << " is_decimal=" << rec.row.IsDecimal(1) << std::endl;
        }
    }

    // 14) Partitioned table example
    std::cout << "\n=== Partitioned Table Example ===" << std::endl;

    fluss::TablePath partitioned_table_path("fluss", "partitioned_table_cpp_v1");

    // Drop if exists
    check("drop_partitioned_table_if_exists", admin.DropTable(partitioned_table_path, true));

    // Create a partitioned table with a "region" partition key
    auto partitioned_schema = fluss::Schema::NewBuilder()
                                  .AddColumn("id", fluss::DataType::Int())
                                  .AddColumn("region", fluss::DataType::String())
                                  .AddColumn("value", fluss::DataType::BigInt())
                                  .Build();

    auto partitioned_descriptor = fluss::TableDescriptor::NewBuilder()
                                      .SetSchema(partitioned_schema)
                                      .SetPartitionKeys({"region"})
                                      .SetBucketCount(1)
                                      .SetComment("cpp partitioned table example")
                                      .Build();

    check("create_partitioned_table",
          admin.CreateTable(partitioned_table_path, partitioned_descriptor, false));
    std::cout << "Created partitioned table" << std::endl;

    // Create partitions
    check("create_partition_US",
          admin.CreatePartition(partitioned_table_path, {{"region", "US"}}, true));
    check("create_partition_EU",
          admin.CreatePartition(partitioned_table_path, {{"region", "EU"}}, true));
    std::cout << "Created partitions: US, EU" << std::endl;

    // List all partitions
    std::vector<fluss::PartitionInfo> partition_infos;
    check("list_partition_infos",
          admin.ListPartitionInfos(partitioned_table_path, partition_infos));
    for (const auto& pi : partition_infos) {
        std::cout << "  Partition: " << pi.partition_name << " (id=" << pi.partition_id << ")"
                  << std::endl;
    }

    // List partitions with partial spec filter
    std::vector<fluss::PartitionInfo> us_partition_infos;
    check("list_partition_infos_with_spec",
          admin.ListPartitionInfos(partitioned_table_path, {{"region", "US"}}, us_partition_infos));
    std::cout << "  Filtered (region=US): " << us_partition_infos.size() << " partition(s)"
              << std::endl;

    // Write data to partitioned table
    fluss::Table partitioned_table;
    check("get_partitioned_table", conn.GetTable(partitioned_table_path, partitioned_table));

    fluss::AppendWriter partitioned_writer;
    check("new_partitioned_writer", partitioned_table.NewAppend().CreateWriter(partitioned_writer));

    struct PartitionedRow {
        int id;
        const char* region;
        int64_t value;
    };

    std::vector<PartitionedRow> partitioned_rows = {
        {1, "US", 100},
        {2, "US", 200},
        {3, "EU", 300},
        {4, "EU", 400},
    };

    for (const auto& r : partitioned_rows) {
        fluss::GenericRow row;
        row.SetInt32(0, r.id);
        row.SetString(1, r.region);
        row.SetInt64(2, r.value);
        check("append_partitioned", partitioned_writer.Append(row));
    }
    check("flush_partitioned", partitioned_writer.Flush());
    std::cout << "Wrote " << partitioned_rows.size() << " rows to partitioned table" << std::endl;

    // 14.1) subscribe_partition_buckets: subscribe to each partition individually
    std::cout << "\n--- Testing SubscribePartitionBuckets ---" << std::endl;
    fluss::LogScanner partition_scanner;
    check("new_partition_scanner", partitioned_table.NewScan().CreateLogScanner(partition_scanner));

    for (const auto& pi : partition_infos) {
        check("subscribe_partition_buckets",
              partition_scanner.SubscribePartitionBuckets(pi.partition_id, 0, 0));
        std::cout << "Subscribed to partition " << pi.partition_name << std::endl;
    }

    fluss::ScanRecords partition_records;
    check("poll_partitioned", partition_scanner.Poll(5000, partition_records));
    std::cout << "Scanned " << partition_records.Count() << " records from partitioned table"
              << std::endl;
    for (const auto& tb : partition_records.Buckets()) {
        for (const auto& rec : partition_records.Records(tb)) {
            std::cout << "  partition_id="
                      << (tb.partition_id.has_value() ? std::to_string(*tb.partition_id) : "none")
                      << ", id=" << rec.row.GetInt32(0) << ", region=" << rec.row.GetString(1)
                      << ", value=" << rec.row.GetInt64(2) << std::endl;
        }
    }

    // 14.2) subscribe_partition_buckets: batch subscribe to all partitions at once
    std::cout << "\n--- Testing SubscribePartitionBuckets (batch) ---" << std::endl;
    fluss::LogScanner partition_batch_scanner;
    check("new_partition_batch_scanner",
          partitioned_table.NewScan().CreateLogScanner(partition_batch_scanner));

    std::vector<fluss::PartitionBucketSubscription> partition_subs;
    for (const auto& pi : partition_infos) {
        partition_subs.push_back({pi.partition_id, 0, 0});
    }
    check("subscribe_partition_buckets",
          partition_batch_scanner.SubscribePartitionBuckets(partition_subs));
    std::cout << "Batch subscribed to " << partition_subs.size() << " partition+bucket combinations"
              << std::endl;

    fluss::ScanRecords partition_batch_records;
    check("poll_partition_batch", partition_batch_scanner.Poll(5000, partition_batch_records));
    std::cout << "Scanned " << partition_batch_records.Count()
              << " records from batch partition subscription" << std::endl;
    for (const auto& tb : partition_batch_records.Buckets()) {
        for (const auto& rec : partition_batch_records.Records(tb)) {
            std::cout << "  id=" << rec.row.GetInt32(0) << ", region=" << rec.row.GetString(1)
                      << ", value=" << rec.row.GetInt64(2) << std::endl;
        }
    }

    // 14.3) UnsubscribePartition: unsubscribe from one partition, verify remaining
    std::cout << "\n--- Testing UnsubscribePartition ---" << std::endl;
    fluss::LogScanner unsub_partition_scanner;
    check("new_unsub_partition_scanner",
          partitioned_table.NewScan().CreateLogScanner(unsub_partition_scanner));

    for (const auto& pi : partition_infos) {
        check("subscribe_for_unsub",
              unsub_partition_scanner.SubscribePartitionBuckets(pi.partition_id, 0, 0));
    }
    // Unsubscribe from the first partition
    check("unsubscribe_partition",
          unsub_partition_scanner.UnsubscribePartition(partition_infos[0].partition_id, 0));
    std::cout << "Unsubscribed from partition " << partition_infos[0].partition_name << std::endl;

    fluss::ScanRecords unsub_records;
    check("poll_after_unsub", unsub_partition_scanner.Poll(5000, unsub_records));
    std::cout << "After unsubscribe, scanned " << unsub_records.Count() << " records" << std::endl;
    for (const auto& tb : unsub_records.Buckets()) {
        for (const auto& rec : unsub_records.Records(tb)) {
            std::cout << "  id=" << rec.row.GetInt32(0) << ", region=" << rec.row.GetString(1)
                      << ", value=" << rec.row.GetInt64(2) << std::endl;
        }
    }

    // Cleanup
    check("drop_partitioned_table", admin.DropTable(partitioned_table_path, true));
    std::cout << "Dropped partitioned table" << std::endl;
    return 0;
}
