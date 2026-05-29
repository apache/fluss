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

fluss::ffi::FfiColumn MakeArrayColumn(int32_t nesting, int32_t element_type,
                                      bool nullable = true, bool leaf_nullable = true,
                                      std::vector<uint8_t> per_level_nullability = {}) {
    fluss::ffi::FfiColumn col;
    col.name = rust::String("bad_array");
    col.data_type = static_cast<int32_t>(fluss::TypeId::Array);
    col.nullable = nullable;
    col.comment = rust::String("");
    col.precision = 0;
    col.scale = 0;
    col.array_nesting = nesting;
    if (!per_level_nullability.empty()) {
        for (auto v : per_level_nullability) {
            col.array_nullability.push_back(v);
        }
    } else {
        for (int32_t i = 0; i < nesting; ++i) {
            col.array_nullability.push_back((i == 0 ? nullable : true) ? 1 : 0);
        }
        col.array_nullability.push_back(leaf_nullable ? 1 : 0);
    }
    col.element_data_type = element_type;
    col.element_precision = 0;
    col.element_scale = 0;
    return col;
}

fluss::ffi::FfiColumn MakeScalarColumn(const char* name, fluss::TypeId type_id,
                                       bool nullable = true, int32_t precision = 0,
                                       int32_t scale = 0) {
    fluss::ffi::FfiColumn col;
    col.name = rust::String(name);
    col.data_type = static_cast<int32_t>(type_id);
    col.nullable = nullable;
    col.comment = rust::String("");
    col.precision = precision;
    col.scale = scale;
    col.array_nesting = 0;
    col.element_data_type = 0;
    col.element_precision = 0;
    col.element_scale = 0;
    return col;
}

}  // namespace

TEST(FfiConverterTest, RejectsArrayWithoutElementType) {
    auto col = MakeArrayColumn(1, 0);
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}

TEST(FfiConverterTest, RejectsArrayWithArrayLeafType) {
    auto col = MakeArrayColumn(2, static_cast<int32_t>(fluss::TypeId::Array));
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}

TEST(FfiConverterTest, RejectsArrayWithUnknownLeafType) {
    auto col = MakeArrayColumn(1, 999);
    EXPECT_THROW((void)fluss::utils::from_ffi_column(col), std::runtime_error);
}

TEST(FfiConverterTest, SupportsLegacyOneLevelArrayMetadata) {
    auto col = MakeArrayColumn(0, static_cast<int32_t>(fluss::TypeId::Int));
    auto converted = fluss::utils::from_ffi_column(col);
    EXPECT_EQ(converted.data_type.id(), fluss::TypeId::Array);
    ASSERT_NE(converted.data_type.element_type(), nullptr);
    EXPECT_EQ(converted.data_type.element_type()->id(), fluss::TypeId::Int);
}

// --- Nullability tests ---

TEST(DataTypeTest, DefaultNullable) {
    auto dt = fluss::DataType::Int();
    EXPECT_TRUE(dt.nullable());
}

TEST(DataTypeTest, NotNullMethod) {
    auto dt = fluss::DataType::Int().NotNull();
    EXPECT_FALSE(dt.nullable());
    EXPECT_EQ(dt.id(), fluss::TypeId::Int);
}

TEST(DataTypeTest, NotNullPreservesPrecisionScale) {
    auto dt = fluss::DataType::Decimal(10, 2).NotNull();
    EXPECT_FALSE(dt.nullable());
    EXPECT_EQ(dt.precision(), 10);
    EXPECT_EQ(dt.scale(), 2);
}

TEST(DataTypeTest, ArrayElementNullability) {
    auto dt = fluss::DataType::Array(fluss::DataType::Int().NotNull());
    EXPECT_TRUE(dt.nullable());
    ASSERT_NE(dt.element_type(), nullptr);
    EXPECT_FALSE(dt.element_type()->nullable());
}

TEST(DataTypeTest, NotNullArrayNullableElement) {
    auto dt = fluss::DataType::Array(fluss::DataType::Int()).NotNull();
    EXPECT_FALSE(dt.nullable());
    ASSERT_NE(dt.element_type(), nullptr);
    EXPECT_TRUE(dt.element_type()->nullable());
}

TEST(DataTypeTest, NotNullArrayNotNullElement) {
    auto dt = fluss::DataType::Array(fluss::DataType::Int().NotNull()).NotNull();
    EXPECT_FALSE(dt.nullable());
    ASSERT_NE(dt.element_type(), nullptr);
    EXPECT_FALSE(dt.element_type()->nullable());
}

TEST(FfiConverterTest, ScalarNullableRoundTrip) {
    fluss::Column col{"id", fluss::DataType::Int(), ""};
    auto ffi_col = fluss::utils::to_ffi_column(col);
    EXPECT_TRUE(ffi_col.nullable);
    auto back = fluss::utils::from_ffi_column(ffi_col);
    EXPECT_TRUE(back.data_type.nullable());
}

TEST(FfiConverterTest, ScalarNotNullRoundTrip) {
    fluss::Column col{"id", fluss::DataType::Int().NotNull(), ""};
    auto ffi_col = fluss::utils::to_ffi_column(col);
    EXPECT_FALSE(ffi_col.nullable);
    auto back = fluss::utils::from_ffi_column(ffi_col);
    EXPECT_FALSE(back.data_type.nullable());
}

TEST(FfiConverterTest, ArrayNotNullElementRoundTrip) {
    fluss::Column col{"tags", fluss::DataType::Array(fluss::DataType::String().NotNull()), ""};
    auto ffi_col = fluss::utils::to_ffi_column(col);
    EXPECT_TRUE(ffi_col.nullable);
    ASSERT_EQ(ffi_col.array_nullability.size(), 2u);
    EXPECT_EQ(ffi_col.array_nullability[1], 0);
    auto back = fluss::utils::from_ffi_column(ffi_col);
    EXPECT_TRUE(back.data_type.nullable());
    ASSERT_NE(back.data_type.element_type(), nullptr);
    EXPECT_FALSE(back.data_type.element_type()->nullable());
}

TEST(FfiConverterTest, NotNullArrayNullableElementRoundTrip) {
    fluss::Column col{"ids", fluss::DataType::Array(fluss::DataType::Int()).NotNull(), ""};
    auto ffi_col = fluss::utils::to_ffi_column(col);
    EXPECT_FALSE(ffi_col.nullable);
    ASSERT_EQ(ffi_col.array_nullability.size(), 2u);
    EXPECT_EQ(ffi_col.array_nullability[1], 1);
    auto back = fluss::utils::from_ffi_column(ffi_col);
    EXPECT_FALSE(back.data_type.nullable());
    ASSERT_NE(back.data_type.element_type(), nullptr);
    EXPECT_TRUE(back.data_type.element_type()->nullable());
}

TEST(FfiConverterTest, NotNullArrayNotNullElementRoundTrip) {
    fluss::Column col{
        "strict_ids",
        fluss::DataType::Array(fluss::DataType::Int().NotNull()).NotNull(),
        "",
    };
    auto ffi_col = fluss::utils::to_ffi_column(col);
    EXPECT_FALSE(ffi_col.nullable);
    ASSERT_EQ(ffi_col.array_nullability.size(), 2u);
    EXPECT_EQ(ffi_col.array_nullability[1], 0);
    auto back = fluss::utils::from_ffi_column(ffi_col);
    EXPECT_FALSE(back.data_type.nullable());
    ASSERT_NE(back.data_type.element_type(), nullptr);
    EXPECT_FALSE(back.data_type.element_type()->nullable());
}

TEST(FfiConverterTest, NestedArrayIntermediateNullabilityRoundTrip) {
    fluss::Column col{
        "nested",
        fluss::DataType::Array(fluss::DataType::Array(fluss::DataType::Int()).NotNull()),
        "",
    };
    auto ffi_col = fluss::utils::to_ffi_column(col);
    auto back = fluss::utils::from_ffi_column(ffi_col);

    EXPECT_TRUE(back.data_type.nullable());
    ASSERT_NE(back.data_type.element_type(), nullptr);
    EXPECT_FALSE(back.data_type.element_type()->nullable());
    ASSERT_NE(back.data_type.element_type()->element_type(), nullptr);
    EXPECT_TRUE(back.data_type.element_type()->element_type()->nullable());
}

TEST(FfiConverterTest, NestedArrayAllLevelsNullabilityRoundTrip) {
    fluss::Column col{
        "strict_nested",
        fluss::DataType::Array(
            fluss::DataType::Array(fluss::DataType::Int().NotNull()).NotNull())
            .NotNull(),
        "",
    };
    auto ffi_col = fluss::utils::to_ffi_column(col);
    auto back = fluss::utils::from_ffi_column(ffi_col);

    EXPECT_FALSE(back.data_type.nullable());
    ASSERT_NE(back.data_type.element_type(), nullptr);
    EXPECT_FALSE(back.data_type.element_type()->nullable());
    ASSERT_NE(back.data_type.element_type()->element_type(), nullptr);
    EXPECT_FALSE(back.data_type.element_type()->element_type()->nullable());
}

TEST(FfiConverterTest, FfiColumnNonNullableScalarReconstructed) {
    auto col = MakeScalarColumn("id", fluss::TypeId::Int, false);
    auto converted = fluss::utils::from_ffi_column(col);
    EXPECT_FALSE(converted.data_type.nullable());
    EXPECT_EQ(converted.data_type.id(), fluss::TypeId::Int);
}

TEST(FfiConverterTest, FfiColumnNonNullableArrayReconstructed) {
    auto col = MakeArrayColumn(1, static_cast<int32_t>(fluss::TypeId::String), false, false);
    auto converted = fluss::utils::from_ffi_column(col);
    EXPECT_FALSE(converted.data_type.nullable());
    ASSERT_NE(converted.data_type.element_type(), nullptr);
    EXPECT_FALSE(converted.data_type.element_type()->nullable());
}
