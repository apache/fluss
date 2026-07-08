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

#pragma once

#include <arrow/c/bridge.h>
#include <arrow/type.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "fluss.hpp"

namespace fluss {
namespace detail {

inline arrow::TimeUnit::type arrow_time_unit(int32_t precision) {
    if (precision <= 0) return arrow::TimeUnit::SECOND;
    if (precision <= 3) return arrow::TimeUnit::MILLI;
    if (precision <= 6) return arrow::TimeUnit::MICRO;
    return arrow::TimeUnit::NANO;
}

/// Mirrors core's `to_arrow_type` so a lowered type round-trips through `from_arrow_field`.
inline std::shared_ptr<arrow::DataType> to_arrow_type(const DataType& dt) {
    switch (dt.id()) {
        case TypeId::Boolean:
            return arrow::boolean();
        case TypeId::TinyInt:
            return arrow::int8();
        case TypeId::SmallInt:
            return arrow::int16();
        case TypeId::Int:
            return arrow::int32();
        case TypeId::BigInt:
            return arrow::int64();
        case TypeId::Float:
            return arrow::float32();
        case TypeId::Double:
            return arrow::float64();
        case TypeId::String:
        case TypeId::Char:
            return arrow::utf8();
        case TypeId::Bytes:
            return arrow::binary();
        case TypeId::Binary:
            return arrow::fixed_size_binary(dt.precision());
        case TypeId::Decimal:
            return arrow::decimal128(dt.precision(), dt.scale());
        case TypeId::Date:
            return arrow::date32();
        case TypeId::Time: {
            auto unit = arrow_time_unit(dt.precision());
            return (unit == arrow::TimeUnit::SECOND || unit == arrow::TimeUnit::MILLI)
                       ? arrow::time32(unit)
                       : arrow::time64(unit);
        }
        case TypeId::Timestamp:
            return arrow::timestamp(arrow_time_unit(dt.precision()));
        case TypeId::TimestampLtz:
            return arrow::timestamp(arrow_time_unit(dt.precision()), "UTC");
        case TypeId::Array: {
            const DataType* elem = dt.element_type();
            if (!elem) throw std::runtime_error("ARRAY DataType is missing its element type");
            return arrow::list(arrow::field("element", to_arrow_type(*elem), elem->nullable()));
        }
        case TypeId::Map: {
            const DataType* k = dt.key_type();
            const DataType* v = dt.value_type();
            if (!k || !v) throw std::runtime_error("MAP DataType is missing its key/value type");
            return arrow::map(to_arrow_type(*k), to_arrow_type(*v));
        }
        case TypeId::Row: {
            std::vector<std::shared_ptr<arrow::Field>> fields;
            fields.reserve(dt.field_count());
            for (size_t i = 0; i < dt.field_count(); i++) {
                const DataType* ft = dt.field_type(i);
                fields.push_back(
                    arrow::field(dt.field_name(i), to_arrow_type(*ft), ft->nullable()));
            }
            return arrow::struct_(std::move(fields));
        }
        default:
            throw std::runtime_error("Unsupported DataType for Arrow lowering");
    }
}

/// True if the type is (or nests) a MAP/ROW the flat FFI column can't express,
/// so it must route through Arrow. Array-of-scalar stays on the flat path.
inline bool is_compound(const DataType& dt) {
    switch (dt.id()) {
        case TypeId::Map:
        case TypeId::Row:
            return true;
        case TypeId::Array: {
            const DataType* elem = dt.element_type();
            return elem != nullptr && is_compound(*elem);
        }
        default:
            return false;
    }
}

inline std::shared_ptr<arrow::Schema> columns_to_arrow_schema(const std::vector<Column>& columns) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(columns.size());
    for (const auto& col : columns) {
        fields.push_back(
            arrow::field(col.name, to_arrow_type(col.data_type), col.data_type.nullable()));
    }
    return arrow::schema(std::move(fields));
}

/// Exports an Arrow schema to a heap FFI_ArrowSchema; the Rust bridge takes
/// ownership of the returned pointer. Throws on failure.
inline size_t export_arrow_schema(const arrow::Schema& schema) {
    ArrowSchema c_schema;
    auto status = arrow::ExportSchema(schema, &c_schema);
    if (!status.ok()) {
        throw std::runtime_error("Failed to export Arrow schema: " + status.ToString());
    }
    return reinterpret_cast<size_t>(new ArrowSchema(std::move(c_schema)));
}

}  // namespace detail
}  // namespace fluss
