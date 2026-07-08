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

#include <gtest/gtest.h>

#include <arrow/api.h>

#include "test_utils.h"

class KvTableTest : public ::testing::Test {
   protected:
    fluss::Admin& admin() { return fluss_test::FlussTestEnvironment::Instance()->GetAdmin(); }

    fluss::Connection& connection() {
        return fluss_test::FlussTestEnvironment::Instance()->GetConnection();
    }
};

TEST_F(KvTableTest, UpsertDeleteAndLookup) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_upsert_and_lookup_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .AddColumn("age", fluss::DataType::BigInt())
                      .SetPrimaryKeys({"id"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    // Create upsert writer
    auto table_upsert = table.NewUpsert();
    fluss::UpsertWriter upsert_writer;
    ASSERT_OK(table_upsert.CreateWriter(upsert_writer));

    // Upsert 3 rows (fire-and-forget, then flush)
    struct TestData {
        int32_t id;
        std::string name;
        int64_t age;
    };
    std::vector<TestData> test_data = {{1, "Verso", 32}, {2, "Noco", 25}, {3, "Esquie", 35}};

    for (const auto& d : test_data) {
        fluss::GenericRow row(3);
        row.SetInt32(0, d.id);
        row.SetString(1, d.name);
        row.SetInt64(2, d.age);
        ASSERT_OK(upsert_writer.Upsert(row));
    }
    ASSERT_OK(upsert_writer.Flush());

    // Create lookuper
    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));

    // Verify lookup results
    for (const auto& d : test_data) {
        fluss::GenericRow key(3);
        key.SetInt32(0, d.id);

        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found()) << "Row with id=" << d.id << " should exist";

        EXPECT_EQ(result.GetInt32(0), d.id) << "id mismatch";
        EXPECT_EQ(result.GetString(1), d.name) << "name mismatch";
        EXPECT_EQ(result.GetInt64(2), d.age) << "age mismatch";
    }

    // Update record with id=1 (await acknowledgment)
    {
        fluss::GenericRow updated_row(3);
        updated_row.SetInt32(0, 1);
        updated_row.SetString(1, "Verso");
        updated_row.SetInt64(2, 33);
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(updated_row, wr));
        ASSERT_OK(wr.Wait());
    }

    // Verify the update
    {
        fluss::GenericRow key(3);
        key.SetInt32(0, 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetInt64(2), 33) << "Age should be updated";
        EXPECT_EQ(result.GetString(1), "Verso") << "Name should remain unchanged";
    }

    // Delete record with id=1 (await acknowledgment)
    {
        fluss::GenericRow delete_row(3);
        delete_row.SetInt32(0, 1);
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Delete(delete_row, wr));
        ASSERT_OK(wr.Wait());
    }

    // Verify deletion
    {
        fluss::GenericRow key(3);
        key.SetInt32(0, 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_FALSE(result.Found()) << "Record 1 should not exist after delete";
    }

    // Verify other records still exist
    for (int id : {2, 3}) {
        fluss::GenericRow key(3);
        key.SetInt32(0, id);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found()) << "Record " << id
                                    << " should still exist after deleting record 1";
    }

    // Lookup non-existent key
    {
        fluss::GenericRow key(3);
        key.SetInt32(0, 999);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_FALSE(result.Found()) << "Non-existent key should return not found";
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, LimitScan) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_limit_scan_pk_cpp");

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
    std::vector<std::pair<int32_t, std::string>> rows = {
        {1, "Verso"}, {2, "Noco"}, {3, "Esquie"}, {4, "Aline"}, {5, "Gustave"}};
    for (const auto& [id, name] : rows) {
        fluss::GenericRow row(2);
        row.SetInt32(0, id);
        row.SetString(1, name);
        ASSERT_OK(upsert_writer.Upsert(row));
    }
    ASSERT_OK(upsert_writer.Flush());

    int64_t table_id = table.GetTableInfo().table_id;
    fluss::TableBucket bucket{table_id, 0};

    fluss::BatchScanner scanner;
    ASSERT_OK(table.NewScan().Limit(3).CreateBucketBatchScanner(bucket, scanner));
    EXPECT_TRUE(scanner.Bucket() == bucket);
    fluss::ArrowRecordBatches first;
    ASSERT_OK(scanner.NextBatch(first));
    ASSERT_FALSE(first.Empty()) << "first NextBatch should return a batch";
    int64_t scanned = 0;
    for (const auto& b : first) {
        EXPECT_EQ(b->GetBucketId(), 0);
        scanned += b->NumRows();
    }
    EXPECT_GT(scanned, 0);
    EXPECT_LE(scanned, 3);

    fluss::ArrowRecordBatches spent;
    ASSERT_OK(scanner.NextBatch(spent));
    EXPECT_TRUE(spent.Empty()) << "scanner must be spent after one batch";

    fluss::BatchScanner scanner2;
    ASSERT_OK(table.NewScan().Limit(100).CreateBucketBatchScanner(bucket, scanner2));
    fluss::ArrowRecordBatches all;
    ASSERT_OK(scanner2.CollectAllBatches(all));
    int64_t total = 0;
    for (const auto& b : all) {
        total += b->NumRows();
    }
    EXPECT_EQ(total, 5);

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, LookupWithNestedArray) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_lookup_nested_array_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("matrix",
                                 fluss::DataType::Array(fluss::DataType::Array(fluss::DataType::Int())))
                      .SetPrimaryKeys({"id"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto upsert = table.NewUpsert();
    fluss::UpsertWriter writer;
    ASSERT_OK(upsert.CreateWriter(writer));

    {
        auto row = table.NewRow();
        row.Set("id", 1);

        fluss::ArrayWriter inner1(2, fluss::DataType::Int());
        inner1.SetInt32(0, 11);
        inner1.SetInt32(1, 12);

        fluss::ArrayWriter inner2(2, fluss::DataType::Int());
        inner2.SetInt32(0, 21);
        inner2.SetInt32(1, 22);

        fluss::ArrayWriter outer(2, fluss::DataType::Array(fluss::DataType::Int()));
        outer.SetArray(0, std::move(inner1));
        outer.SetArray(1, std::move(inner2));
        row.Set("matrix", std::move(outer));

        ASSERT_OK(writer.Upsert(row));
        ASSERT_OK(writer.Flush());
    }

    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));

    auto key = table.NewRow();
    key.Set("id", 1);

    fluss::LookupResult result;
    ASSERT_OK(lookuper.Lookup(key, result));
    ASSERT_TRUE(result.Found());
    auto outer = result.GetValue("matrix");
    EXPECT_EQ(outer.Type(), fluss::TypeId::Array);
    ASSERT_EQ(outer.Size(), 2u);

    auto first = outer.At(0);
    EXPECT_EQ(first.Type(), fluss::TypeId::Array);
    ASSERT_EQ(first.Size(), 2u);
    EXPECT_EQ(first.At(0).GetInt32(), 11);
    EXPECT_EQ(first.At(1).GetInt32(), 12);

    auto second = outer.At(1);
    ASSERT_EQ(second.Size(), 2u);
    EXPECT_EQ(second.At(0).GetInt32(), 21);
    EXPECT_EQ(second.At(1).GetInt32(), 22);

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, LookupComplexTypesMatrix) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_lookup_complex_matrix_cpp");

    auto row_seq_label = arrow::struct_(
        {arrow::field("seq", arrow::int32()), arrow::field("label", arrow::utf8())});

    auto arrow_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("m_str_int", arrow::map(arrow::utf8(), arrow::int32())),
        arrow::field("m_str_row", arrow::map(arrow::utf8(), row_seq_label)),
        arrow::field("m_str_map",
                     arrow::map(arrow::utf8(), arrow::map(arrow::utf8(), arrow::int32()))),
        arrow::field("m_str_arr", arrow::map(arrow::utf8(), arrow::list(arrow::int32()))),
        arrow::field("arr_map", arrow::list(arrow::map(arrow::utf8(), arrow::int32()))),
        arrow::field("arr_row", arrow::list(row_seq_label)),
        arrow::field("r_deep", arrow::struct_({arrow::field(
                                   "inner", arrow::struct_({arrow::field("n", arrow::int32())}))})),
        arrow::field("r_with_arr",
                     arrow::struct_({arrow::field("f_int", arrow::int32()),
                                     arrow::field("f_arr", arrow::list(arrow::int32()))})),
        // row_rich: every scalar type + an array field in one ROW.
        arrow::field("r_rich",
                     arrow::struct_({
                         arrow::field("f_bool", arrow::boolean()),
                         arrow::field("f_int", arrow::int32()),
                         arrow::field("f_long", arrow::int64()),
                         arrow::field("f_float", arrow::float32()),
                         arrow::field("f_double", arrow::float64()),
                         arrow::field("f_str", arrow::utf8()),
                         arrow::field("f_bytes", arrow::binary()),
                         arrow::field("f_decimal", arrow::decimal128(10, 2)),
                         arrow::field("f_date", arrow::date32()),
                         arrow::field("f_time", arrow::time32(arrow::TimeUnit::MILLI)),
                         arrow::field("f_ts_ntz", arrow::timestamp(arrow::TimeUnit::MICRO)),
                         arrow::field("f_ts_ltz", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC")),
                         arrow::field("f_binary", arrow::fixed_size_binary(4)),
                         arrow::field("f_arr", arrow::list(arrow::int32())),
                     })),
        arrow::field("m_str_tiny", arrow::map(arrow::utf8(), arrow::int8())),
        arrow::field("arr_small", arrow::list(arrow::int16())),
    });
    auto schema = fluss::Schema::FromArrow(arrow_schema, {"id"});

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();
    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));
    auto upsert = table.NewUpsert();
    fluss::UpsertWriter writer;
    ASSERT_OK(upsert.CreateWriter(writer));

    {
        auto row = table.NewRow();
        row.Set("id", 1);

        // map<string,int> — second entry has a NULL value.
        {
            fluss::MapWriter m(2, fluss::DataType::String(), fluss::DataType::Int());
            m.SetKeyString("a");
            m.SetValueInt32(1);
            m.Commit();
            m.SetKeyString("b");
            m.SetValueNull();
            m.Commit();
            row.Set("m_str_int", std::move(m));
        }
        // map<string, row<seq,label>> — value is a ROW, so the Arrow ctor.
        {
            fluss::MapWriter m(1, arrow::utf8(), row_seq_label);
            m.SetKeyString("k");
            fluss::GenericRow v(2);
            v.SetInt32(0, 7);
            v.SetString(1, "seven");
            m.SetValueRow(std::move(v));
            m.Commit();
            row.Set("m_str_row", std::move(m));
        }
        // map<string, map<string,int>>
        {
            fluss::MapWriter m(1, arrow::utf8(), arrow::map(arrow::utf8(), arrow::int32()));
            m.SetKeyString("k");
            fluss::MapWriter inner(1, fluss::DataType::String(), fluss::DataType::Int());
            inner.SetKeyString("x");
            inner.SetValueInt32(9);
            inner.Commit();
            m.SetValueMap(std::move(inner));
            m.Commit();
            row.Set("m_str_map", std::move(m));
        }
        // map<string, array<int>> — value array fits the flat ctor.
        {
            fluss::MapWriter m(1, fluss::DataType::String(),
                               fluss::DataType::Array(fluss::DataType::Int()));
            m.SetKeyString("k");
            fluss::ArrayWriter v(2, fluss::DataType::Int());
            v.SetInt32(0, 10);
            v.SetInt32(1, 20);
            m.SetValueArray(std::move(v));
            m.Commit();
            row.Set("m_str_arr", std::move(m));
        }
        // array<map<string,int>> — element is a MAP, so the Arrow ctor.
        {
            fluss::ArrayWriter a(1, arrow::map(arrow::utf8(), arrow::int32()));
            fluss::MapWriter e(1, fluss::DataType::String(), fluss::DataType::Int());
            e.SetKeyString("p");
            e.SetValueInt32(5);
            e.Commit();
            a.SetMap(0, std::move(e));
            row.Set("arr_map", std::move(a));
        }
        // array<row<seq,label>> — element is a ROW, so the Arrow ctor.
        {
            fluss::ArrayWriter a(2, row_seq_label);
            fluss::GenericRow e0(2);
            e0.SetInt32(0, 1);
            e0.SetString(1, "one");
            fluss::GenericRow e1(2);
            e1.SetInt32(0, 2);
            e1.SetString(1, "two");
            a.SetRow(0, std::move(e0));
            a.SetRow(1, std::move(e1));
            row.Set("arr_row", std::move(a));
        }
        // row<inner: row<n>>
        {
            fluss::GenericRow inner(1);
            inner.SetInt32(0, 42);
            fluss::GenericRow outer(1);
            outer.SetRow(0, std::move(inner));
            row.Set("r_deep", std::move(outer));
        }
        // row<f_int, f_arr: array<int>>
        {
            fluss::GenericRow r(2);
            r.SetInt32(0, 100);
            fluss::ArrayWriter arr(3, fluss::DataType::Int());
            arr.SetInt32(0, 1);
            arr.SetInt32(1, 2);
            arr.SetInt32(2, 3);
            r.SetArray(1, std::move(arr));
            row.Set("r_with_arr", std::move(r));
        }
        // row_rich: exercise every Value leaf getter + an array field.
        {
            fluss::GenericRow rr(14);
            rr.SetBool(0, true);
            rr.SetInt32(1, 100000);
            rr.SetInt64(2, 9876543210LL);
            rr.SetFloat32(3, 3.5F);
            rr.SetFloat64(4, 2.5);
            rr.SetString(5, "hello world");
            rr.SetBytes(6, {'b', 'i', 'n'});
            rr.SetDecimal(7, "123.45");
            rr.SetDate(8, fluss::Date{20476});
            rr.SetTime(9, fluss::Time{36827123});
            rr.SetTimestampNtz(10, fluss::Timestamp{1769163227123LL, 456000});
            rr.SetTimestampLtz(11, fluss::Timestamp{1769163227456LL, 0});
            rr.SetBytes(12, {1, 2, 3, 4});
            fluss::ArrayWriter farr(3, fluss::DataType::Int());
            farr.SetInt32(0, 7);
            farr.SetNull(1);
            farr.SetInt32(2, 11);
            rr.SetArray(13, std::move(farr));
            row.Set("r_rich", std::move(rr));
        }
        // map<string,tinyint>
        {
            fluss::MapWriter m(2, fluss::DataType::String(), fluss::DataType::TinyInt());
            m.SetKeyString("lo");
            m.SetValueInt32(-128);
            m.Commit();
            m.SetKeyString("hi");
            m.SetValueInt32(127);
            m.Commit();
            row.Set("m_str_tiny", std::move(m));
        }
        // array<smallint>
        {
            fluss::ArrayWriter a(2, fluss::DataType::SmallInt());
            a.SetInt32(0, 1000);
            a.SetInt32(1, -2000);
            row.Set("arr_small", std::move(a));
        }

        ASSERT_OK(writer.Upsert(row));
        ASSERT_OK(writer.Flush());
    }

    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));
    auto key = table.NewRow();
    key.Set("id", 1);
    fluss::LookupResult result;
    ASSERT_OK(lookuper.Lookup(key, result));
    ASSERT_TRUE(result.Found());

    // map<string,int> — entry 1 has a NULL value.
    {
        auto m = result.GetValue("m_str_int");
        EXPECT_EQ(m.Type(), fluss::TypeId::Map);
        ASSERT_EQ(m.Size(), 2u);
        EXPECT_EQ(m.KeyAt(0).GetString(), "a");
        EXPECT_FALSE(m.ValueAt(0).IsNull());
        EXPECT_EQ(m.ValueAt(0).GetInt32(), 1);
        EXPECT_EQ(m.KeyAt(1).GetString(), "b");
        EXPECT_TRUE(m.ValueAt(1).IsNull());
    }
    // map<string, row<seq,label>>
    {
        auto m = result.GetValue("m_str_row");
        ASSERT_EQ(m.Size(), 1u);
        EXPECT_EQ(m.KeyAt(0).GetString(), "k");
        auto v = m.ValueAt(0);
        EXPECT_EQ(v.Field(0).GetInt32(), 7);
        EXPECT_EQ(v.Field(1).GetString(), "seven");
    }
    // map<string, map<string,int>>
    {
        auto inner = result.GetValue("m_str_map").ValueAt(0);
        ASSERT_EQ(inner.Size(), 1u);
        EXPECT_EQ(inner.KeyAt(0).GetString(), "x");
        EXPECT_EQ(inner.ValueAt(0).GetInt32(), 9);
    }
    // map<string, array<int>>
    {
        auto av = result.GetValue("m_str_arr").ValueAt(0);
        ASSERT_EQ(av.Size(), 2u);
        EXPECT_EQ(av.At(0).GetInt32(), 10);
        EXPECT_EQ(av.At(1).GetInt32(), 20);
    }
    // array<map<string,int>>
    {
        auto a = result.GetValue("arr_map");
        ASSERT_EQ(a.Size(), 1u);
        auto e = a.At(0);
        EXPECT_EQ(e.KeyAt(0).GetString(), "p");
        EXPECT_EQ(e.ValueAt(0).GetInt32(), 5);
    }
    // array<row<seq,label>>
    {
        auto a = result.GetValue("arr_row");
        EXPECT_EQ(a.Type(), fluss::TypeId::Array);
        ASSERT_EQ(a.Size(), 2u);
        EXPECT_EQ(a.At(0).Field(0).GetInt32(), 1);
        EXPECT_EQ(a.At(0).Field(1).GetString(), "one");
        EXPECT_EQ(a.At(1).Field(0).GetInt32(), 2);
        EXPECT_EQ(a.At(1).Field(1).GetString(), "two");
    }
    // row<inner: row<n>>
    {
        auto inner = result.GetValue("r_deep").Field(0);
        EXPECT_EQ(inner.Field(0).GetInt32(), 42);
    }
    // row<f_int, f_arr>
    {
        auto r = result.GetValue("r_with_arr");
        EXPECT_EQ(r.Type(), fluss::TypeId::Row);
        EXPECT_EQ(r.Field(0).GetInt32(), 100);
        EXPECT_EQ(r.Field("f_int").GetInt32(), 100);  // ROW field by name
        auto arr = r.Field(1);
        ASSERT_EQ(arr.Size(), 3u);
        EXPECT_EQ(arr.At(2).GetInt32(), 3);
    }
    // row_rich — every leaf getter on one Value handle.
    {
        auto rr = result.GetValue("r_rich");
        ASSERT_EQ(rr.FieldCount(), 14u);
        EXPECT_TRUE(rr.Field(0).GetBool());
        EXPECT_EQ(rr.Field(1).GetInt32(), 100000);
        EXPECT_EQ(rr.Field(2).GetInt64(), 9876543210LL);
        EXPECT_FLOAT_EQ(rr.Field(3).GetFloat32(), 3.5F);
        EXPECT_DOUBLE_EQ(rr.Field(4).GetFloat64(), 2.5);
        EXPECT_EQ(rr.Field(5).GetString(), "hello world");
        auto by = rr.Field(6).GetBytes();
        ASSERT_EQ(by.size(), 3u);
        EXPECT_EQ(by[0], 'b');
        EXPECT_EQ(rr.Field(7).GetDecimalString(), "123.45");
        EXPECT_EQ(rr.Field(8).GetDate().days_since_epoch, 20476);
        EXPECT_EQ(rr.Field(9).GetTime().millis_since_midnight, 36827123);
        EXPECT_EQ(rr.Field(10).GetTimestamp().epoch_millis, 1769163227123LL);
        EXPECT_EQ(rr.Field(11).GetTimestamp().epoch_millis, 1769163227456LL);
        auto bin = rr.Field(12).GetBytes();
        ASSERT_EQ(bin.size(), 4u);
        EXPECT_EQ(bin[3], 4);
        auto fa = rr.Field(13);
        ASSERT_EQ(fa.Size(), 3u);
        EXPECT_TRUE(fa.At(1).IsNull());
        EXPECT_EQ(fa.At(2).GetInt32(), 11);
    }
    // map<string,tinyint>
    {
        auto m = result.GetValue("m_str_tiny");
        ASSERT_EQ(m.Size(), 2u);
        EXPECT_EQ(m.ValueAt(0).GetInt32(), -128);
        EXPECT_EQ(m.ValueAt(1).GetInt32(), 127);
    }
    // array<smallint>
    {
        auto a = result.GetValue("arr_small");
        ASSERT_EQ(a.Size(), 2u);
        EXPECT_EQ(a.At(0).GetInt32(), 1000);
        EXPECT_EQ(a.At(1).GetInt32(), -2000);
    }

    // Row 2 (id=2) — every compound column NULL.
    {
        const int column_count = arrow_schema->num_fields();
        auto row = table.NewRow();
        row.SetInt32(0, 2);
        for (int i = 1; i < column_count; ++i) {
            row.SetNull(i);
        }
        ASSERT_OK(writer.Upsert(row));
        ASSERT_OK(writer.Flush());

        auto key2 = table.NewRow();
        key2.SetInt32(0, 2);
        fluss::LookupResult result2;
        ASSERT_OK(lookuper.Lookup(key2, result2));
        ASSERT_TRUE(result2.Found());
        EXPECT_EQ(result2.GetInt32(0), 2);
        for (int i = 1; i < column_count; ++i) {
            EXPECT_TRUE(result2.IsNull(i)) << "column " << i << " should be null";
        }
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

// Regression: timestamp map values built via the Arrow MapWriter ctor must pick
// NTZ vs LTZ from the declared value type (previously always wrote NTZ).
TEST_F(KvTableTest, MapWithTimestampValuesNtzAndLtz) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_map_timestamp_values_cpp");

    auto arrow_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("mn", arrow::map(arrow::utf8(), arrow::timestamp(arrow::TimeUnit::MICRO))),
        arrow::field("ml",
                     arrow::map(arrow::utf8(), arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))),
    });
    auto schema = fluss::Schema::FromArrow(arrow_schema, {"id"});

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();
    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));
    fluss::UpsertWriter writer;
    ASSERT_OK(table.NewUpsert().CreateWriter(writer));

    const fluss::Timestamp ntz_val{1769163227123LL, 456000};
    const fluss::Timestamp ltz_val{1700000000789LL, 123000};
    {
        auto row = table.NewRow();
        row.SetInt32(0, 1);
        // mn: map<string, timestamp> (NTZ), via the Arrow ctor.
        fluss::MapWriter mn(1, arrow::utf8(), arrow::timestamp(arrow::TimeUnit::MICRO));
        mn.SetKeyString("k");
        mn.SetValueTimestamp(ntz_val);
        mn.Commit();
        row.SetMap(1, std::move(mn));
        // ml: map<string, timestamp_ltz> (LTZ), via the Arrow ctor.
        fluss::MapWriter ml(1, arrow::utf8(), arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"));
        ml.SetKeyString("k");
        ml.SetValueTimestamp(ltz_val);
        ml.Commit();
        row.SetMap(2, std::move(ml));
        ASSERT_OK(writer.Upsert(row));
        ASSERT_OK(writer.Flush());
    }

    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));
    auto key = table.NewRow();
    key.SetInt32(0, 1);
    fluss::LookupResult result;
    ASSERT_OK(lookuper.Lookup(key, result));
    ASSERT_TRUE(result.Found());

    auto mn = result.GetValue("mn");
    ASSERT_EQ(mn.Size(), 1u);
    EXPECT_EQ(mn.ValueAt(0).GetTimestamp().epoch_millis, ntz_val.epoch_millis);
    EXPECT_EQ(mn.ValueAt(0).GetTimestamp().nano_of_millisecond, ntz_val.nano_of_millisecond);

    auto ml = result.GetValue("ml");
    ASSERT_EQ(ml.Size(), 1u);
    EXPECT_EQ(ml.ValueAt(0).GetTimestamp().epoch_millis, ltz_val.epoch_millis);
    EXPECT_EQ(ml.ValueAt(0).GetTimestamp().nano_of_millisecond, ltz_val.nano_of_millisecond);

    ASSERT_OK(adm.DropTable(table_path, false));
}

// Deeply nested MAP/ROW via native DataType::Map / DataType::Row only — no Arrow.
TEST_F(KvTableTest, NativeNestedBuilderNoArrow) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_native_nested_builder_cpp");

    // array<row<seq: int, attrs: map<string, int>>>
    auto event = fluss::DataType::Row({
        {"seq", fluss::DataType::Int()},
        {"attrs", fluss::DataType::Map(fluss::DataType::String(), fluss::DataType::Int())},
    });
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("events", fluss::DataType::Array(event))
                      .AddColumn("profile", fluss::DataType::Row({
                                                {"name", fluss::DataType::String()},
                                                {"score", fluss::DataType::Double()},
                                            }))
                      .SetPrimaryKeys({"id"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();
    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));
    fluss::UpsertWriter writer;
    ASSERT_OK(table.NewUpsert().CreateWriter(writer));

    {
        auto row = table.NewRow();
        row.Set("id", 1);

        // events = [ {0, {"a":0}}, {1, {"a":10,"b":11}} ]
        fluss::ArrayWriter events(2, event);
        for (int i = 0; i < 2; i++) {
            fluss::GenericRow ev(2);
            ev.SetInt32(0, i);
            fluss::MapWriter attrs(static_cast<size_t>(i + 1), fluss::DataType::String(),
                                   fluss::DataType::Int());
            attrs.SetKeyString("a");
            attrs.SetValueInt32(i * 10);
            attrs.Commit();
            if (i == 1) {
                attrs.SetKeyString("b");
                attrs.SetValueInt32(11);
                attrs.Commit();
            }
            ev.SetMap(1, std::move(attrs));
            events.SetRow(i, std::move(ev));
        }
        row.Set("events", std::move(events));

        fluss::GenericRow profile(2);
        profile.SetString(0, "alice");
        profile.SetFloat64(1, 9.5);
        row.Set("profile", std::move(profile));

        ASSERT_OK(writer.Upsert(row));
        ASSERT_OK(writer.Flush());
    }

    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));
    auto key = table.NewRow();
    key.SetInt32(0, 1);
    fluss::LookupResult result;
    ASSERT_OK(lookuper.Lookup(key, result));
    ASSERT_TRUE(result.Found());

    auto events = result.GetValue("events");  // ARRAY<ROW<int, MAP<string,int>>>
    ASSERT_EQ(events.Type(), fluss::TypeId::Array);
    ASSERT_EQ(events.Size(), 2u);
    EXPECT_EQ(events.At(0).Field("seq").GetInt32(), 0);
    EXPECT_EQ(events.At(0).Field("attrs").ValueAt(0).GetInt32(), 0);
    auto e1_attrs = events.At(1).Field("attrs");
    ASSERT_EQ(e1_attrs.Size(), 2u);
    EXPECT_EQ(e1_attrs.KeyAt(1).GetString(), "b");
    EXPECT_EQ(e1_attrs.ValueAt(1).GetInt32(), 11);

    auto profile = result.GetValue("profile");  // ROW<name, score>
    EXPECT_EQ(profile.Field("name").GetString(), "alice");
    EXPECT_DOUBLE_EQ(profile.Field("score").GetFloat64(), 9.5);

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, ComplexTypesPartialUpdate) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_complex_partial_update_cpp");

    auto arrow_schema = arrow::schema({
        arrow::field("id", arrow::int32()),
        arrow::field("name", arrow::utf8()),
        arrow::field("score", arrow::int64()),
        arrow::field("nested", arrow::struct_({arrow::field("seq", arrow::int32()),
                                               arrow::field("label", arrow::utf8())})),
        arrow::field("attrs", arrow::map(arrow::utf8(), arrow::int32())),
        arrow::field("tags", arrow::list(arrow::utf8())),
    });
    auto schema = fluss::Schema::FromArrow(arrow_schema, {"id"});

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();
    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    {
        auto upsert = table.NewUpsert();
        fluss::UpsertWriter w;
        ASSERT_OK(upsert.CreateWriter(w));
        auto row = table.NewRow();
        row.SetInt32(0, 1);
        row.SetString(1, "Verso");
        row.SetInt64(2, 100);
        fluss::GenericRow nested(2);
        nested.SetInt32(0, 10);
        nested.SetString(1, "alpha");
        row.SetRow(3, std::move(nested));
        fluss::MapWriter attrs(2, fluss::DataType::String(), fluss::DataType::Int());
        attrs.SetKeyString("a");
        attrs.SetValueInt32(1);
        attrs.Commit();
        attrs.SetKeyString("b");
        attrs.SetValueInt32(2);
        attrs.Commit();
        row.SetMap(4, std::move(attrs));
        fluss::ArrayWriter tags(2, fluss::DataType::String());
        tags.SetString(0, "x");
        tags.SetString(1, "y");
        row.SetArray(5, std::move(tags));
        ASSERT_OK(w.Upsert(row));
        ASSERT_OK(w.Flush());
    }

    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));
    auto key = table.NewRow();
    key.SetInt32(0, 1);

    // Partial update of a scalar column — compound columns must be preserved.
    {
        auto pu = table.NewUpsert();
        pu.PartialUpdateByName({"id", "score"});
        fluss::UpsertWriter pw;
        ASSERT_OK(pu.CreateWriter(pw));
        auto row = table.NewRow();
        row.SetInt32(0, 1);
        row.SetNull(1);
        row.SetInt64(2, 420);
        row.SetNull(3);
        row.SetNull(4);
        row.SetNull(5);
        ASSERT_OK(pw.Upsert(row));
        ASSERT_OK(pw.Flush());

        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetString(1), "Verso");
        EXPECT_EQ(result.GetInt64(2), 420);
        auto n = result.GetValue(3);
        EXPECT_EQ(n.Field(0).GetInt32(), 10);
        EXPECT_EQ(n.Field(1).GetString(), "alpha");
        EXPECT_EQ(result.GetValue(4).Size(), 2u);
        EXPECT_EQ(result.GetValue(5).Size(), 2u);
    }

    // Partial update of the ROW column — other compound columns preserved.
    {
        auto pu = table.NewUpsert();
        pu.PartialUpdateByName({"id", "nested"});
        fluss::UpsertWriter pw;
        ASSERT_OK(pu.CreateWriter(pw));
        auto row = table.NewRow();
        row.SetInt32(0, 1);
        row.SetNull(1);
        row.SetNull(2);
        fluss::GenericRow nn(2);
        nn.SetInt32(0, 99);
        nn.SetString(1, "omega");
        row.SetRow(3, std::move(nn));
        row.SetNull(4);
        row.SetNull(5);
        ASSERT_OK(pw.Upsert(row));
        ASSERT_OK(pw.Flush());

        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetInt64(2), 420);
        auto n = result.GetValue(3);
        EXPECT_EQ(n.Field(0).GetInt32(), 99);
        EXPECT_EQ(n.Field(1).GetString(), "omega");
        EXPECT_EQ(result.GetValue(4).Size(), 2u);
        EXPECT_EQ(result.GetValue(5).Size(), 2u);
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, PartitionedComplexTypes) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_partitioned_complex_cpp");

    auto arrow_schema = arrow::schema({
        arrow::field("region", arrow::utf8()),
        arrow::field("user_id", arrow::int32()),
        arrow::field("nested", arrow::struct_({arrow::field("seq", arrow::int32()),
                                               arrow::field("label", arrow::utf8())})),
        arrow::field("attrs", arrow::map(arrow::utf8(), arrow::int32())),
    });
    auto schema = fluss::Schema::FromArrow(arrow_schema, {"region", "user_id"});

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetPartitionKeys({"region"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();
    fluss_test::CreateTable(adm, table_path, table_descriptor);
    fluss_test::CreatePartitions(adm, table_path, "region", {"US", "EU"});

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    fluss::UpsertWriter writer;
    ASSERT_OK(table.NewUpsert().CreateWriter(writer));

    struct Rec {
        const char* region;
        int32_t user_id;
        int32_t seq;
        const char* label;
    };
    const Rec data[] = {{"US", 1, 11, "alpha"}, {"EU", 2, 22, "beta"}};

    for (const auto& d : data) {
        auto row = table.NewRow();
        row.SetString(0, d.region);
        row.SetInt32(1, d.user_id);
        fluss::GenericRow nested(2);
        nested.SetInt32(0, d.seq);
        nested.SetString(1, d.label);
        row.SetRow(2, std::move(nested));
        fluss::MapWriter attrs(1, fluss::DataType::String(), fluss::DataType::Int());
        attrs.SetKeyString(d.label);
        attrs.SetValueInt32(d.seq);
        attrs.Commit();
        row.SetMap(3, std::move(attrs));
        ASSERT_OK(writer.Upsert(row));
    }
    ASSERT_OK(writer.Flush());

    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));

    for (const auto& d : data) {
        auto key = table.NewRow();
        key.SetString(0, d.region);
        key.SetInt32(1, d.user_id);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        auto nested = result.GetValue(2);
        EXPECT_EQ(nested.Field(0).GetInt32(), d.seq);
        EXPECT_EQ(nested.Field(1).GetString(), d.label);
        auto attrs = result.GetValue(3);
        ASSERT_EQ(attrs.Size(), 1u);
        EXPECT_EQ(attrs.KeyAt(0).GetString(), d.label);
        EXPECT_EQ(attrs.ValueAt(0).GetInt32(), d.seq);
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, LookupArrayValidationErrors) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_lookup_array_validation_errors_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("vals", fluss::DataType::Array(fluss::DataType::Int()))
                      .SetPrimaryKeys({"id"})
                      .Build();
    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();
    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));
    auto upsert = table.NewUpsert();
    fluss::UpsertWriter writer;
    ASSERT_OK(upsert.CreateWriter(writer));

    auto row = table.NewRow();
    row.Set("id", 1);
    fluss::ArrayWriter vals(2, fluss::DataType::Int());
    vals.SetInt32(0, 99);
    vals.SetNull(1);
    row.Set("vals", std::move(vals));
    ASSERT_OK(writer.Upsert(row));
    ASSERT_OK(writer.Flush());

    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));

    auto key = table.NewRow();
    key.Set("id", 1);
    fluss::LookupResult result;
    ASSERT_OK(lookuper.Lookup(key, result));
    ASSERT_TRUE(result.Found());

    auto view = result.GetValue("vals");
    EXPECT_EQ(view.Type(), fluss::TypeId::Array);
    ASSERT_EQ(view.Size(), 2u);
    EXPECT_EQ(view.At(0).GetInt32(), 99);
    EXPECT_TRUE(view.At(1).IsNull());

    // A wrong-type leaf read throws.
    bool wrong_type_threw = false;
    try {
        (void)view.At(0).GetInt64();
    } catch (const std::exception&) {
        wrong_type_threw = true;
    }
    EXPECT_TRUE(wrong_type_threw);

    // A typed read of a null element throws.
    bool null_typed_getter_threw = false;
    try {
        (void)view.At(1).GetInt32();
    } catch (const std::exception&) {
        null_typed_getter_threw = true;
    }
    EXPECT_TRUE(null_typed_getter_threw);

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, CompositePrimaryKeys) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_composite_pk_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("region", fluss::DataType::String())
                      .AddColumn("score", fluss::DataType::BigInt())
                      .AddColumn("user_id", fluss::DataType::Int())
                      .SetPrimaryKeys({"region", "user_id"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto table_upsert = table.NewUpsert();
    fluss::UpsertWriter upsert_writer;
    ASSERT_OK(table_upsert.CreateWriter(upsert_writer));

    // Insert records with composite keys
    struct TestData {
        std::string region;
        int32_t user_id;
        int64_t score;
    };
    std::vector<TestData> test_data = {
        {"US", 1, 100}, {"US", 2, 200}, {"EU", 1, 150}, {"EU", 2, 250}};

    for (const auto& d : test_data) {
        auto row = table.NewRow();
        row.Set("region", d.region);
        row.Set("score", d.score);
        row.Set("user_id", d.user_id);
        ASSERT_OK(upsert_writer.Upsert(row));
    }
    ASSERT_OK(upsert_writer.Flush());

    // Create lookuper
    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));

    // Lookup (US, 1) - should return score 100
    {
        auto key = table.NewRow();
        key.Set("region", "US");
        key.Set("user_id", 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetInt64("score"), 100) << "Score for (US, 1) should be 100";
    }

    // Lookup (EU, 2) - should return score 250
    {
        auto key = table.NewRow();
        key.Set("region", "EU");
        key.Set("user_id", 2);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetInt64("score"), 250) << "Score for (EU, 2) should be 250";
    }

    // Update (US, 1) score (await acknowledgment)
    {
        auto update_row = table.NewRow();
        update_row.Set("region", "US");
        update_row.Set("user_id", 1);
        update_row.Set("score", static_cast<int64_t>(500));
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(update_row, wr));
        ASSERT_OK(wr.Wait());
    }

    // Verify update
    {
        auto key = table.NewRow();
        key.Set("region", "US");
        key.Set("user_id", 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetInt64("score"), 500) << "Row score should be updated";
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, PrefixLookupByBucketKey) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_prefix_lookup_cpp");

    // Bucket key (a, b) is a strict prefix of the PK (a, b, c), enabling prefix lookup on (a, b).
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("a", fluss::DataType::Int())
                      .AddColumn("b", fluss::DataType::String())
                      .AddColumn("c", fluss::DataType::BigInt())
                      .AddColumn("d", fluss::DataType::String())
                      .SetPrimaryKeys({"a", "b", "c"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketKeys({"a", "b"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto table_upsert = table.NewUpsert();
    fluss::UpsertWriter upsert_writer;
    ASSERT_OK(table_upsert.CreateWriter(upsert_writer));

    struct TestData {
        int32_t a;
        std::string b;
        int64_t c;
        std::string d;
    };
    std::vector<TestData> test_data = {
        {1, "aaaaaaaaa", 1, "value1"},
        {1, "aaaaaaaaa", 2, "value2"},
        {1, "aaaaaaaaa", 3, "value3"},
        {2, "aaaaaaaaa", 4, "value4"},
    };

    for (const auto& row_data : test_data) {
        auto row = table.NewRow();
        row.Set("a", row_data.a);
        row.Set("b", row_data.b);
        row.Set("c", row_data.c);
        row.Set("d", row_data.d);
        ASSERT_OK(upsert_writer.Upsert(row));
    }
    ASSERT_OK(upsert_writer.Flush());

    fluss::PrefixLookuper prefix_lookuper;
    ASSERT_OK(table.NewPrefixLookup({"a", "b"}, prefix_lookuper));

    // Prefix (1, "aaaaaaaaa") matches 3 rows, returned in primary-key order.
    {
        auto key = table.NewRow();
        key.Set("a", 1);
        key.Set("b", "aaaaaaaaa");
        fluss::PrefixLookupResult result;
        ASSERT_OK(prefix_lookuper.PrefixLookup(key, result));
        ASSERT_EQ(result.Size(), 3u);
        for (size_t i = 0; i < result.Size(); ++i) {
            auto row = result.GetRow(i);
            EXPECT_EQ(row.GetInt32("a"), 1);
            EXPECT_EQ(row.GetString("b"), "aaaaaaaaa");
            EXPECT_EQ(row.GetInt64("c"), static_cast<int64_t>(i) + 1);
            EXPECT_EQ(row.GetString("d"), "value" + std::to_string(i + 1));
        }
    }

    // Prefix (2, "aaaaaaaaa") matches a single row.
    {
        auto key = table.NewRow();
        key.Set("a", 2);
        key.Set("b", "aaaaaaaaa");
        fluss::PrefixLookupResult result;
        ASSERT_OK(prefix_lookuper.PrefixLookup(key, result));
        ASSERT_EQ(result.Size(), 1u);
        auto row = result.GetRow(0);
        EXPECT_EQ(row.GetInt64("c"), 4);
        EXPECT_EQ(row.GetString("d"), "value4");
    }

    // A prefix with no matching rows yields an empty result.
    {
        auto key = table.NewRow();
        key.Set("a", 3);
        key.Set("b", "a");
        fluss::PrefixLookupResult result;
        ASSERT_OK(prefix_lookuper.PrefixLookup(key, result));
        EXPECT_TRUE(result.IsEmpty());
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, PrefixLookupValidationErrors) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_prefix_lookup_validation_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("a", fluss::DataType::Int())
                      .AddColumn("b", fluss::DataType::String())
                      .AddColumn("c", fluss::DataType::BigInt())
                      .SetPrimaryKeys({"a", "b", "c"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketKeys({"a", "b"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    // Lookup columns must list the bucket keys (a, b) in order.
    {
        fluss::PrefixLookuper lookuper;
        auto result = table.NewPrefixLookup({"b", "a"}, lookuper);
        ASSERT_FALSE(result.Ok());
        EXPECT_EQ(result.error_code, fluss::ErrorCode::CLIENT_ERROR);
        EXPECT_NE(result.error_message.find("must contain all bucket keys"), std::string::npos);
    }

    // Lookup columns must equal the bucket keys, not a longer prefix of the PK.
    {
        fluss::PrefixLookuper lookuper;
        auto result = table.NewPrefixLookup({"a", "b", "c"}, lookuper);
        ASSERT_FALSE(result.Ok());
        EXPECT_EQ(result.error_code, fluss::ErrorCode::CLIENT_ERROR);
        EXPECT_NE(result.error_message.find("must contain all bucket keys"), std::string::npos);
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, PrefixLookupPartitioned) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_prefix_lookup_partitioned_cpp");

    // Partitioned by region; bucket key (a, b) is a prefix of the PK minus the partition column.
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("region", fluss::DataType::String())
                      .AddColumn("a", fluss::DataType::Int())
                      .AddColumn("b", fluss::DataType::String())
                      .AddColumn("c", fluss::DataType::BigInt())
                      .AddColumn("d", fluss::DataType::String())
                      .SetPrimaryKeys({"region", "a", "b", "c"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetPartitionKeys({"region"})
                                .SetBucketKeys({"a", "b"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);
    fluss_test::CreatePartitions(adm, table_path, "region", {"US", "EU"});

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto table_upsert = table.NewUpsert();
    fluss::UpsertWriter upsert_writer;
    ASSERT_OK(table_upsert.CreateWriter(upsert_writer));

    struct TestData {
        std::string region;
        int32_t a;
        std::string b;
        int64_t c;
        std::string d;
    };
    std::vector<TestData> test_data = {
        {"US", 1, "aaaaaaaaa", 1, "us-1"},
        {"US", 1, "aaaaaaaaa", 2, "us-2"},
        {"US", 2, "aaaaaaaaa", 3, "us-3"},
        {"EU", 1, "aaaaaaaaa", 4, "eu-1"},
        {"EU", 1, "bbbbbbbbb", 5, "eu-2"},
    };

    for (const auto& row_data : test_data) {
        auto row = table.NewRow();
        row.Set("region", row_data.region);
        row.Set("a", row_data.a);
        row.Set("b", row_data.b);
        row.Set("c", row_data.c);
        row.Set("d", row_data.d);
        ASSERT_OK(upsert_writer.Upsert(row));
    }
    ASSERT_OK(upsert_writer.Flush());

    fluss::PrefixLookuper prefix_lookuper;
    ASSERT_OK(table.NewPrefixLookup({"region", "a", "b"}, prefix_lookuper));

    // (US, 1, "aaaaaaaaa") matches 2 rows.
    {
        auto key = table.NewRow();
        key.Set("region", "US");
        key.Set("a", 1);
        key.Set("b", "aaaaaaaaa");
        fluss::PrefixLookupResult result;
        ASSERT_OK(prefix_lookuper.PrefixLookup(key, result));
        ASSERT_EQ(result.Size(), 2u);
        for (size_t i = 0; i < result.Size(); ++i) {
            auto row = result.GetRow(i);
            EXPECT_EQ(row.GetString("region"), "US");
            EXPECT_EQ(row.GetInt32("a"), 1);
            EXPECT_EQ(row.GetString("b"), "aaaaaaaaa");
        }
    }

    // (EU, 1, "bbbbbbbbb") matches a single row.
    {
        auto key = table.NewRow();
        key.Set("region", "EU");
        key.Set("a", 1);
        key.Set("b", "bbbbbbbbb");
        fluss::PrefixLookupResult result;
        ASSERT_OK(prefix_lookuper.PrefixLookup(key, result));
        ASSERT_EQ(result.Size(), 1u);
        EXPECT_EQ(result.GetRow(0).GetString("d"), "eu-2");
    }

    // A non-existent partition yields an empty result.
    {
        auto key = table.NewRow();
        key.Set("region", "APAC");
        key.Set("a", 1);
        key.Set("b", "aaaaaaaaa");
        fluss::PrefixLookupResult result;
        ASSERT_OK(prefix_lookuper.PrefixLookup(key, result));
        EXPECT_TRUE(result.IsEmpty());
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, PartialUpdate) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_partial_update_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .AddColumn("age", fluss::DataType::BigInt())
                      .AddColumn("score", fluss::DataType::BigInt())
                      .SetPrimaryKeys({"id"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    // Insert initial record with all columns
    auto table_upsert = table.NewUpsert();
    fluss::UpsertWriter upsert_writer;
    ASSERT_OK(table_upsert.CreateWriter(upsert_writer));

    {
        fluss::GenericRow row(4);
        row.SetInt32(0, 1);
        row.SetString(1, "Verso");
        row.SetInt64(2, 32);
        row.SetInt64(3, 6942);
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(row, wr));
        ASSERT_OK(wr.Wait());
    }

    // Verify initial record
    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));

    {
        fluss::GenericRow key(4);
        key.SetInt32(0, 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetInt32(0), 1);
        EXPECT_EQ(result.GetString(1), "Verso");
        EXPECT_EQ(result.GetInt64(2), 32);
        EXPECT_EQ(result.GetInt64(3), 6942);
    }

    // Create partial update writer to update only score column
    auto partial_upsert = table.NewUpsert();
    partial_upsert.PartialUpdateByName({"id", "score"});
    fluss::UpsertWriter partial_writer;
    ASSERT_OK(partial_upsert.CreateWriter(partial_writer));

    // Update only the score column (await acknowledgment)
    {
        fluss::GenericRow partial_row(4);
        partial_row.SetInt32(0, 1);
        partial_row.SetNull(1);  // not in partial update
        partial_row.SetNull(2);  // not in partial update
        partial_row.SetInt64(3, 420);
        fluss::WriteResult wr;
        ASSERT_OK(partial_writer.Upsert(partial_row, wr));
        ASSERT_OK(wr.Wait());
    }

    // Verify partial update - name and age should remain unchanged
    {
        fluss::GenericRow key(4);
        key.SetInt32(0, 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetInt32(0), 1) << "id should remain 1";
        EXPECT_EQ(result.GetString(1), "Verso") << "name should remain unchanged";
        EXPECT_EQ(result.GetInt64(2), 32) << "age should remain unchanged";
        EXPECT_EQ(result.GetInt64(3), 420) << "score should be updated to 420";
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, PartialUpdateByIndex) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_partial_update_by_index_cpp");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .AddColumn("age", fluss::DataType::BigInt())
                      .AddColumn("score", fluss::DataType::BigInt())
                      .SetPrimaryKeys({"id"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    // Insert initial record with all columns
    auto table_upsert = table.NewUpsert();
    fluss::UpsertWriter upsert_writer;
    ASSERT_OK(table_upsert.CreateWriter(upsert_writer));

    {
        fluss::GenericRow row(4);
        row.SetInt32(0, 1);
        row.SetString(1, "Verso");
        row.SetInt64(2, 32);
        row.SetInt64(3, 6942);
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(row, wr));
        ASSERT_OK(wr.Wait());
    }

    // Verify initial record
    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));

    {
        fluss::GenericRow key(4);
        key.SetInt32(0, 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetInt32(0), 1);
        EXPECT_EQ(result.GetString(1), "Verso");
        EXPECT_EQ(result.GetInt64(2), 32);
        EXPECT_EQ(result.GetInt64(3), 6942);
    }

    // Create partial update writer using column indices: 0 (id) and 3 (score)
    auto partial_upsert = table.NewUpsert();
    partial_upsert.PartialUpdateByIndex({0, 3});
    fluss::UpsertWriter partial_writer;
    ASSERT_OK(partial_upsert.CreateWriter(partial_writer));

    // Update only the score column (await acknowledgment)
    {
        fluss::GenericRow partial_row(4);
        partial_row.SetInt32(0, 1);
        partial_row.SetNull(1);  // not in partial update
        partial_row.SetNull(2);  // not in partial update
        partial_row.SetInt64(3, 420);
        fluss::WriteResult wr;
        ASSERT_OK(partial_writer.Upsert(partial_row, wr));
        ASSERT_OK(wr.Wait());
    }

    // Verify partial update - name and age should remain unchanged
    {
        fluss::GenericRow key(4);
        key.SetInt32(0, 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(result.GetInt32(0), 1) << "id should remain 1";
        EXPECT_EQ(result.GetString(1), "Verso") << "name should remain unchanged";
        EXPECT_EQ(result.GetInt64(2), 32) << "age should remain unchanged";
        EXPECT_EQ(result.GetInt64(3), 420) << "score should be updated to 420";
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, PartitionedTableUpsertAndLookup) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_partitioned_kv_table_cpp");

    // Create a partitioned KV table with region as partition key
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("region", fluss::DataType::String())
                      .AddColumn("user_id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .AddColumn("score", fluss::DataType::BigInt())
                      .SetPrimaryKeys({"region", "user_id"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetPartitionKeys({"region"})
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    // Create partitions
    fluss_test::CreatePartitions(adm, table_path, "region", {"US", "EU", "APAC"});

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto table_upsert = table.NewUpsert();
    fluss::UpsertWriter upsert_writer;
    ASSERT_OK(table_upsert.CreateWriter(upsert_writer));

    // Insert records with different partitions
    struct TestData {
        std::string region;
        int32_t user_id;
        std::string name;
        int64_t score;
    };
    std::vector<TestData> test_data = {{"US", 1, "Gustave", 100}, {"US", 2, "Lune", 200},
                                       {"EU", 1, "Sciel", 150},   {"EU", 2, "Maelle", 250},
                                       {"APAC", 1, "Noco", 300}};

    for (const auto& d : test_data) {
        fluss::GenericRow row(4);
        row.SetString(0, d.region);
        row.SetInt32(1, d.user_id);
        row.SetString(2, d.name);
        row.SetInt64(3, d.score);
        ASSERT_OK(upsert_writer.Upsert(row));
    }
    ASSERT_OK(upsert_writer.Flush());

    // Create lookuper
    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));

    // Lookup records
    for (const auto& d : test_data) {
        fluss::GenericRow key(4);
        key.SetString(0, d.region);
        key.SetInt32(1, d.user_id);

        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());

        EXPECT_EQ(std::string(result.GetString(0)), d.region) << "region mismatch";
        EXPECT_EQ(result.GetInt32(1), d.user_id) << "user_id mismatch";
        EXPECT_EQ(std::string(result.GetString(2)), d.name) << "name mismatch";
        EXPECT_EQ(result.GetInt64(3), d.score) << "score mismatch";
    }

    // Update within a partition (await acknowledgment)
    {
        fluss::GenericRow updated_row(4);
        updated_row.SetString(0, "US");
        updated_row.SetInt32(1, 1);
        updated_row.SetString(2, "Gustave Updated");
        updated_row.SetInt64(3, 999);
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(updated_row, wr));
        ASSERT_OK(wr.Wait());
    }

    // Verify the update
    {
        fluss::GenericRow key(4);
        key.SetString(0, "US");
        key.SetInt32(1, 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(std::string(result.GetString(2)), "Gustave Updated");
        EXPECT_EQ(result.GetInt64(3), 999);
    }

    // Lookup in non-existent partition should return not found
    {
        fluss::GenericRow key(4);
        key.SetString(0, "UNKNOWN_REGION");
        key.SetInt32(1, 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_FALSE(result.Found()) << "Lookup in non-existent partition should return not found";
    }

    // Delete a record within a partition (await acknowledgment)
    {
        fluss::GenericRow delete_key(4);
        delete_key.SetString(0, "EU");
        delete_key.SetInt32(1, 1);
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Delete(delete_key, wr));
        ASSERT_OK(wr.Wait());
    }

    // Verify deletion
    {
        fluss::GenericRow key(4);
        key.SetString(0, "EU");
        key.SetInt32(1, 1);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_FALSE(result.Found()) << "Deleted record should not exist";
    }

    // Verify other records in same partition still exist
    {
        fluss::GenericRow key(4);
        key.SetString(0, "EU");
        key.SetInt32(1, 2);
        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());
        EXPECT_EQ(std::string(result.GetString(2)), "Maelle");
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}

TEST_F(KvTableTest, AllSupportedDatatypes) {
    auto& adm = admin();
    auto& conn = connection();

    fluss::TablePath table_path("fluss", "test_all_datatypes_cpp");

    // Create a table with all supported datatypes
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("pk_int", fluss::DataType::Int())
                      .AddColumn("col_boolean", fluss::DataType::Boolean())
                      .AddColumn("col_tinyint", fluss::DataType::TinyInt())
                      .AddColumn("col_smallint", fluss::DataType::SmallInt())
                      .AddColumn("col_int", fluss::DataType::Int())
                      .AddColumn("col_bigint", fluss::DataType::BigInt())
                      .AddColumn("col_float", fluss::DataType::Float())
                      .AddColumn("col_double", fluss::DataType::Double())
                      .AddColumn("col_char", fluss::DataType::Char(10))
                      .AddColumn("col_string", fluss::DataType::String())
                      .AddColumn("col_decimal", fluss::DataType::Decimal(10, 2))
                      .AddColumn("col_date", fluss::DataType::Date())
                      .AddColumn("col_time", fluss::DataType::Time())
                      .AddColumn("col_timestamp", fluss::DataType::Timestamp())
                      .AddColumn("col_timestamp_ltz", fluss::DataType::TimestampLtz())
                      .AddColumn("col_bytes", fluss::DataType::Bytes())
                      .AddColumn("col_binary", fluss::DataType::Binary(20))
                      .SetPrimaryKeys({"pk_int"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetProperty("table.replication.factor", "1")
                                .Build();

    fluss_test::CreateTable(adm, table_path, table_descriptor);

    fluss::Table table;
    ASSERT_OK(conn.GetTable(table_path, table));

    auto table_upsert = table.NewUpsert();
    fluss::UpsertWriter upsert_writer;
    ASSERT_OK(table_upsert.CreateWriter(upsert_writer));

    // Test data
    int32_t pk_int = 1;
    bool col_boolean = true;
    int32_t col_tinyint = 127;
    int32_t col_smallint = 32767;
    int32_t col_int = 2147483647;
    int64_t col_bigint = 9223372036854775807LL;
    float col_float = 3.14f;
    double col_double = 2.718281828459045;
    std::string col_char = "hello";
    std::string col_string = "world of fluss rust client";
    std::string col_decimal = "123.45";
    auto col_date = fluss::Date::FromDays(20476);           // 2026-01-23
    auto col_time = fluss::Time::FromMillis(36827000);       // 10:13:47
    auto col_timestamp = fluss::Timestamp::FromMillis(1769163227123);      // 2026-01-23 10:13:47.123
    auto col_timestamp_ltz = fluss::Timestamp::FromMillis(1769163227123);
    std::vector<uint8_t> col_bytes = {'b', 'i', 'n', 'a', 'r', 'y', ' ', 'd', 'a', 't', 'a'};
    std::vector<uint8_t> col_binary = {'f', 'i', 'x', 'e', 'd', ' ', 'b', 'i', 'n', 'a',
                                       'r', 'y', ' ', 'd', 'a', 't', 'a', '!', '!', '!'};

    // Upsert a row with all datatypes
    {
        fluss::GenericRow row(17);
        row.SetInt32(0, pk_int);
        row.SetBool(1, col_boolean);
        row.SetInt32(2, col_tinyint);
        row.SetInt32(3, col_smallint);
        row.SetInt32(4, col_int);
        row.SetInt64(5, col_bigint);
        row.SetFloat32(6, col_float);
        row.SetFloat64(7, col_double);
        row.SetString(8, col_char);
        row.SetString(9, col_string);
        row.SetDecimal(10, col_decimal);
        row.SetDate(11, col_date);
        row.SetTime(12, col_time);
        row.SetTimestampNtz(13, col_timestamp);
        row.SetTimestampLtz(14, col_timestamp_ltz);
        row.SetBytes(15, col_bytes);
        row.SetBytes(16, col_binary);
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(row, wr));
        ASSERT_OK(wr.Wait());
    }

    // Lookup the record
    fluss::Lookuper lookuper;
    ASSERT_OK(table.NewLookup().CreateLookuper(lookuper));

    {
        fluss::GenericRow key(17);
        key.SetInt32(0, pk_int);

        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());

        // Verify all datatypes
        EXPECT_EQ(result.GetInt32(0), pk_int) << "pk_int mismatch";
        EXPECT_EQ(result.GetBool(1), col_boolean) << "col_boolean mismatch";
        EXPECT_EQ(result.GetInt32(2), col_tinyint) << "col_tinyint mismatch";
        EXPECT_EQ(result.GetInt32(3), col_smallint) << "col_smallint mismatch";
        EXPECT_EQ(result.GetInt32(4), col_int) << "col_int mismatch";
        EXPECT_EQ(result.GetInt64(5), col_bigint) << "col_bigint mismatch";
        EXPECT_NEAR(result.GetFloat32(6), col_float, 1e-6f) << "col_float mismatch";
        EXPECT_NEAR(result.GetFloat64(7), col_double, 1e-15) << "col_double mismatch";
        EXPECT_EQ(result.GetString(8), col_char) << "col_char mismatch";
        EXPECT_EQ(result.GetString(9), col_string) << "col_string mismatch";
        EXPECT_EQ(result.GetDecimalString(10), col_decimal) << "col_decimal mismatch";
        EXPECT_EQ(result.GetDate(11).days_since_epoch, col_date.days_since_epoch) << "col_date mismatch";
        EXPECT_EQ(result.GetTime(12).millis_since_midnight, col_time.millis_since_midnight) << "col_time mismatch";
        EXPECT_EQ(result.GetTimestamp(13).epoch_millis, col_timestamp.epoch_millis)
            << "col_timestamp mismatch";
        EXPECT_EQ(result.GetTimestamp(14).epoch_millis, col_timestamp_ltz.epoch_millis)
            << "col_timestamp_ltz mismatch";

        auto [bytes_ptr, bytes_len] = result.GetBytes(15);
        EXPECT_EQ(bytes_len, col_bytes.size()) << "col_bytes length mismatch";
        EXPECT_TRUE(std::memcmp(bytes_ptr, col_bytes.data(), bytes_len) == 0)
            << "col_bytes mismatch";

        auto [binary_ptr, binary_len] = result.GetBytes(16);
        EXPECT_EQ(binary_len, col_binary.size()) << "col_binary length mismatch";
        EXPECT_TRUE(std::memcmp(binary_ptr, col_binary.data(), binary_len) == 0)
            << "col_binary mismatch";
    }

    // Test with null values for nullable columns
    {
        fluss::GenericRow row_with_nulls(17);
        row_with_nulls.SetInt32(0, 2);  // pk_int = 2
        for (size_t i = 1; i < 17; ++i) {
            row_with_nulls.SetNull(i);
        }
        fluss::WriteResult wr;
        ASSERT_OK(upsert_writer.Upsert(row_with_nulls, wr));
        ASSERT_OK(wr.Wait());
    }

    // Lookup row with nulls
    {
        fluss::GenericRow key(17);
        key.SetInt32(0, 2);

        fluss::LookupResult result;
        ASSERT_OK(lookuper.Lookup(key, result));
        ASSERT_TRUE(result.Found());

        EXPECT_EQ(result.GetInt32(0), 2) << "pk_int mismatch";
        for (size_t i = 1; i < 17; ++i) {
            EXPECT_TRUE(result.IsNull(i)) << "column " << i << " should be null";
        }
    }

    ASSERT_OK(adm.DropTable(table_path, false));
}
