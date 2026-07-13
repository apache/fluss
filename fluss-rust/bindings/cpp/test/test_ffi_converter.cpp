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

#include <stdexcept>

#include "ffi_converter.hpp"

namespace {

using fluss::Column;
using fluss::DataType;
using fluss::TypeId;

// Encode a column to the FFI node arena and decode it back, exercising the
// full create-table / get-table-info type transport.
Column RoundTrip(const Column& col) {
    auto ffi_col = fluss::utils::to_ffi_column(col);
    return fluss::utils::from_ffi_column(ffi_col);
}

fluss::ffi::FfiTypeNode Node(int32_t type_id, uint32_t child_count = 0, bool nullable = true,
                             int32_t precision = 0, int32_t scale = 0,
                             const char* field_name = "") {
    fluss::ffi::FfiTypeNode n;
    n.type_id = type_id;
    n.nullable = nullable;
    n.precision = precision;
    n.scale = scale;
    n.field_name = rust::String(field_name);
    n.child_count = child_count;
    return n;
}

}  // namespace

// --- DataType value semantics ---

TEST(DataTypeTest, DefaultNullable) { EXPECT_TRUE(DataType::Int().nullable()); }

TEST(DataTypeTest, NotNullMethod) {
    auto dt = DataType::Int().NotNull();
    EXPECT_FALSE(dt.nullable());
    EXPECT_EQ(dt.id(), TypeId::Int);
}

TEST(DataTypeTest, NotNullPreservesPrecisionScale) {
    auto dt = DataType::Decimal(10, 2).NotNull();
    EXPECT_FALSE(dt.nullable());
    EXPECT_EQ(dt.precision(), 10);
    EXPECT_EQ(dt.scale(), 2);
}

// --- Node-arena round trips ---

TEST(FfiConverterTest, ScalarRoundTrip) {
    Column col{"id", DataType::Int(), "primary id"};
    auto back = RoundTrip(col);
    EXPECT_EQ(back.name, "id");
    EXPECT_EQ(back.comment, "primary id");
    EXPECT_EQ(back.data_type.id(), TypeId::Int);
    EXPECT_TRUE(back.data_type.nullable());
}

TEST(FfiConverterTest, ScalarNotNullRoundTrip) {
    auto back = RoundTrip(Column{"id", DataType::Int().NotNull(), ""});
    EXPECT_EQ(back.data_type.id(), TypeId::Int);
    EXPECT_FALSE(back.data_type.nullable());
}

TEST(FfiConverterTest, DecimalPrecisionScalePreserved) {
    auto back = RoundTrip(Column{"amount", DataType::Decimal(18, 4), ""});
    EXPECT_EQ(back.data_type.id(), TypeId::Decimal);
    EXPECT_EQ(back.data_type.precision(), 18);
    EXPECT_EQ(back.data_type.scale(), 4);
}

// Non-canonical precisions are exactly what the old Arrow-schema path quantized;
// the node arena must preserve them.
TEST(FfiConverterTest, TimestampPrecisionPreserved) {
    auto back = RoundTrip(Column{"ts", DataType::Timestamp(2), ""});
    EXPECT_EQ(back.data_type.id(), TypeId::Timestamp);
    EXPECT_EQ(back.data_type.precision(), 2);

    auto ltz = RoundTrip(Column{"ts_ltz", DataType::TimestampLtz(9), ""});
    EXPECT_EQ(ltz.data_type.id(), TypeId::TimestampLtz);
    EXPECT_EQ(ltz.data_type.precision(), 9);
}

TEST(FfiConverterTest, TimePrecisionPreserved) {
    auto back = RoundTrip(Column{"t", DataType::Time(3), ""});
    EXPECT_EQ(back.data_type.id(), TypeId::Time);
    EXPECT_EQ(back.data_type.precision(), 3);
}

TEST(FfiConverterTest, CharBinaryLengthPreserved) {
    auto ch = RoundTrip(Column{"code", DataType::Char(12), ""});
    EXPECT_EQ(ch.data_type.id(), TypeId::Char);
    EXPECT_EQ(ch.data_type.precision(), 12);

    auto bin = RoundTrip(Column{"hash", DataType::Binary(32), ""});
    EXPECT_EQ(bin.data_type.id(), TypeId::Binary);
    EXPECT_EQ(bin.data_type.precision(), 32);
}

TEST(FfiConverterTest, ArrayRoundTrip) {
    auto back = RoundTrip(Column{"tags", DataType::Array(DataType::String()), ""});
    EXPECT_EQ(back.data_type.id(), TypeId::Array);
    ASSERT_NE(back.data_type.element_type(), nullptr);
    EXPECT_EQ(back.data_type.element_type()->id(), TypeId::String);
}

TEST(FfiConverterTest, NestedArrayPerLevelNullabilityRoundTrip) {
    // array<array<int NOT NULL> NOT NULL> (outer nullable)
    Column col{"nested",
               DataType::Array(DataType::Array(DataType::Int().NotNull()).NotNull()), ""};
    auto back = RoundTrip(col);
    EXPECT_TRUE(back.data_type.nullable());
    ASSERT_NE(back.data_type.element_type(), nullptr);
    EXPECT_FALSE(back.data_type.element_type()->nullable());
    ASSERT_NE(back.data_type.element_type()->element_type(), nullptr);
    EXPECT_EQ(back.data_type.element_type()->element_type()->id(), TypeId::Int);
    EXPECT_FALSE(back.data_type.element_type()->element_type()->nullable());
}

TEST(FfiConverterTest, MapRoundTrip) {
    Column col{"attrs", DataType::Map(DataType::String(), DataType::Int().NotNull()), ""};
    auto back = RoundTrip(col);
    EXPECT_EQ(back.data_type.id(), TypeId::Map);
    ASSERT_NE(back.data_type.key_type(), nullptr);
    ASSERT_NE(back.data_type.value_type(), nullptr);
    EXPECT_EQ(back.data_type.key_type()->id(), TypeId::String);
    EXPECT_EQ(back.data_type.value_type()->id(), TypeId::Int);
    EXPECT_FALSE(back.data_type.value_type()->nullable());
}

TEST(FfiConverterTest, RowRoundTrip) {
    Column col{"profile",
               DataType::Row({{"age", DataType::Int().NotNull()}, {"city", DataType::String()}}),
               "user profile"};
    auto back = RoundTrip(col);
    EXPECT_EQ(back.comment, "user profile");
    EXPECT_EQ(back.data_type.id(), TypeId::Row);
    ASSERT_EQ(back.data_type.field_count(), 2u);
    EXPECT_EQ(back.data_type.field_name(0), "age");
    ASSERT_NE(back.data_type.field_type(0), nullptr);
    EXPECT_EQ(back.data_type.field_type(0)->id(), TypeId::Int);
    EXPECT_FALSE(back.data_type.field_type(0)->nullable());
    EXPECT_EQ(back.data_type.field_name(1), "city");
    EXPECT_EQ(back.data_type.field_type(1)->id(), TypeId::String);
    EXPECT_TRUE(back.data_type.field_type(1)->nullable());
}

// array<map<string, row<amount: decimal(18,4), ts: timestamp(3)>>> — every kind
// of nesting plus precision-bearing leaves, round-tripped exactly.
TEST(FfiConverterTest, DeeplyNestedRoundTrip) {
    auto row = DataType::Row({{"amount", DataType::Decimal(18, 4)}, {"ts", DataType::Timestamp(3)}});
    Column col{"events", DataType::Array(DataType::Map(DataType::String(), std::move(row))), ""};
    auto back = RoundTrip(col);

    ASSERT_EQ(back.data_type.id(), TypeId::Array);
    const DataType* map = back.data_type.element_type();
    ASSERT_NE(map, nullptr);
    ASSERT_EQ(map->id(), TypeId::Map);
    EXPECT_EQ(map->key_type()->id(), TypeId::String);
    const DataType* inner = map->value_type();
    ASSERT_NE(inner, nullptr);
    ASSERT_EQ(inner->id(), TypeId::Row);
    ASSERT_EQ(inner->field_count(), 2u);
    EXPECT_EQ(inner->field_name(0), "amount");
    EXPECT_EQ(inner->field_type(0)->precision(), 18);
    EXPECT_EQ(inner->field_type(0)->scale(), 4);
    EXPECT_EQ(inner->field_name(1), "ts");
    EXPECT_EQ(inner->field_type(1)->precision(), 3);
}

TEST(FfiConverterTest, NodeArenaShape) {
    EXPECT_EQ(fluss::utils::to_ffi_column(Column{"a", DataType::Int(), ""}).type_nodes.size(), 1u);
    EXPECT_EQ(
        fluss::utils::to_ffi_column(Column{"a", DataType::Array(DataType::Int()), ""}).type_nodes.size(),
        2u);
    EXPECT_EQ(fluss::utils::to_ffi_column(
                  Column{"a", DataType::Map(DataType::String(), DataType::Int()), ""})
                  .type_nodes.size(),
              3u);
    EXPECT_EQ(fluss::utils::to_ffi_column(
                  Column{"a", DataType::Row({{"x", DataType::Int()}, {"y", DataType::String()}}), ""})
                  .type_nodes.size(),
              3u);
}

// --- Decoder rejects malformed arenas ---

TEST(FfiConverterTest, RejectsEmptyTypeTree) {
    fluss::ffi::FfiColumn col;
    col.name = rust::String("broken");
    col.comment = rust::String("");
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}

TEST(FfiConverterTest, RejectsUnknownTypeId) {
    fluss::ffi::FfiColumn col;
    col.name = rust::String("broken");
    col.comment = rust::String("");
    col.type_nodes.push_back(Node(999));
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}

TEST(FfiConverterTest, RejectsTrailingNodes) {
    fluss::ffi::FfiColumn col;
    col.name = rust::String("broken");
    col.comment = rust::String("");
    col.type_nodes.push_back(Node(static_cast<int32_t>(TypeId::Int)));
    col.type_nodes.push_back(Node(static_cast<int32_t>(TypeId::Int)));
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}

TEST(FfiConverterTest, RejectsRowMissingField) {
    fluss::ffi::FfiColumn col;
    col.name = rust::String("broken");
    col.comment = rust::String("");
    // ROW claims two fields but only one follows.
    col.type_nodes.push_back(Node(static_cast<int32_t>(TypeId::Row), /*child_count=*/2));
    col.type_nodes.push_back(Node(static_cast<int32_t>(TypeId::Int), 0, true, 0, 0, "x"));
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}
