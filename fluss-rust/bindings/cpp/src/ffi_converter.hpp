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

#include <cassert>
#include <stdexcept>

#include "fluss.hpp"
#include "lib.rs.h"

namespace fluss {
namespace utils {

/// Compact FFI representation of a (possibly nested) array type.
///
/// `nesting` counts the number of ARRAY wrappers stripped to reach the leaf
/// element type. `leaf_type`/`leaf_precision`/`leaf_scale` describe that leaf
/// scalar. A non-array input produces a zero-initialised value (nesting == 0).
/// `array_nullability` has `nesting + 1` entries: one per ARRAY wrapper
/// (outermost first) plus a trailing entry for the leaf scalar's nullability.
///
/// Using a flat representation — rather than serialising a recursive
/// `DataType` — keeps the cxx bridge contract small while preserving schema
/// fidelity across the FFI boundary when paired with rebuild_array_type().
struct FlattenedArrayType {
    int32_t nesting{0};
    int32_t leaf_type{0};
    int32_t leaf_precision{0};
    int32_t leaf_scale{0};
    std::vector<uint8_t> array_nullability;
};

/// Flattens an `ARRAY<ARRAY<...<leaf>>>` DataType into a FlattenedArrayType.
///
/// Contract:
///   - If `data_type` is not an ARRAY, returns a zero-valued FlattenedArrayType
///     and callers must use the column's own `id/precision/scale` instead.
///   - If `data_type` is an ARRAY but has a null element_type() chain (which
///     should only happen on malformed input), returns a zero-valued result to
///     signal the caller to reject the schema.
///   - Otherwise, `nesting >= 1`, array_nullability has `nesting + 1` entries
///     (last = leaf scalar nullability), and leaf_* describe the innermost scalar.
inline FlattenedArrayType flatten_array_type(const DataType& data_type) {
    FlattenedArrayType out;
    if (data_type.id() != TypeId::Array) {
        return out;
    }

    const DataType* current = &data_type;
    while (current && current->id() == TypeId::Array) {
        out.nesting += 1;
        out.array_nullability.push_back(current->nullable() ? 1 : 0);
        current = current->element_type();
    }
    if (!current) {
        return FlattenedArrayType{};
    }

    out.leaf_type = static_cast<int32_t>(current->id());
    out.leaf_precision = current->precision();
    out.leaf_scale = current->scale();
    out.array_nullability.push_back(current->nullable() ? 1 : 0);
    return out;
}

/// Inverse of flatten_array_type: rebuilds an `ARRAY<ARRAY<...<leaf>>>` type
/// from the compact flat form. Requires `flat.nesting >= 1`; callers handle
/// the `nesting == 0` case by using a plain scalar DataType directly.
/// `array_nullability` must have `nesting + 1` entries (last = leaf).
inline DataType rebuild_array_type(const FlattenedArrayType& flat) {
    bool leaf_nullable = (static_cast<size_t>(flat.nesting) < flat.array_nullability.size())
                             ? (flat.array_nullability[static_cast<size_t>(flat.nesting)] != 0)
                             : true;
    DataType dt(static_cast<TypeId>(flat.leaf_type), flat.leaf_precision, flat.leaf_scale,
                leaf_nullable);
    for (int32_t i = flat.nesting - 1; i >= 0; --i) {
        bool nullable = (static_cast<size_t>(i) < flat.array_nullability.size())
                            ? (flat.array_nullability[static_cast<size_t>(i)] != 0)
                            : true;
        auto arr = DataType::Array(std::move(dt));
        if (!nullable) {
            arr = arr.NotNull();
        }
        dt = std::move(arr);
    }
    return dt;
}

inline Result make_error(int32_t code, std::string msg) { return Result{code, std::move(msg)}; }

inline Result make_client_error(std::string msg) {
    return Result{ErrorCode::CLIENT_ERROR, std::move(msg)};
}

inline Result make_ok() { return Result{0, {}}; }

inline Result from_ffi_result(const ffi::FfiResult& ffi_result) {
    return Result{ffi_result.error_code, std::string(ffi_result.error_message)};
}

template <typename T>
inline T* ptr_from_ffi(const ffi::FfiPtrResult& r) {
    assert(r.ptr != 0 && "ptr_from_ffi: null pointer in FfiPtrResult");
    return reinterpret_cast<T*>(r.ptr);
}

inline ffi::FfiTablePath to_ffi_table_path(const TablePath& path) {
    ffi::FfiTablePath ffi_path;
    ffi_path.database_name = rust::String(path.database_name);
    ffi_path.table_name = rust::String(path.table_name);
    return ffi_path;
}

inline ffi::FfiConfig to_ffi_config(const Configuration& config) {
    ffi::FfiConfig ffi_config;
    ffi_config.bootstrap_servers = rust::String(config.bootstrap_servers);
    ffi_config.writer_request_max_size = config.writer_request_max_size;
    ffi_config.writer_acks = rust::String(config.writer_acks);
    ffi_config.writer_retries = config.writer_retries;
    ffi_config.writer_batch_size = config.writer_batch_size;
    ffi_config.writer_dynamic_batch_size_enabled = config.writer_dynamic_batch_size_enabled;
    ffi_config.writer_dynamic_batch_size_min = config.writer_dynamic_batch_size_min;
    ffi_config.writer_bucket_no_key_assigner = rust::String(config.writer_bucket_no_key_assigner);
    ffi_config.scanner_remote_log_prefetch_num = config.scanner_remote_log_prefetch_num;
    ffi_config.remote_file_download_thread_num = config.remote_file_download_thread_num;
    ffi_config.scanner_remote_log_read_concurrency = config.scanner_remote_log_read_concurrency;
    ffi_config.scanner_log_max_poll_records = config.scanner_log_max_poll_records;
    ffi_config.scanner_log_fetch_max_bytes = config.scanner_log_fetch_max_bytes;
    ffi_config.scanner_log_fetch_min_bytes = config.scanner_log_fetch_min_bytes;
    ffi_config.scanner_log_fetch_wait_max_time_ms = config.scanner_log_fetch_wait_max_time_ms;
    ffi_config.scanner_log_fetch_max_bytes_for_bucket = config.scanner_log_fetch_max_bytes_for_bucket;
    ffi_config.writer_batch_timeout_ms = config.writer_batch_timeout_ms;
    ffi_config.writer_enable_idempotence = config.writer_enable_idempotence;
    ffi_config.writer_max_inflight_requests_per_bucket =
        config.writer_max_inflight_requests_per_bucket;
    ffi_config.writer_buffer_memory_size = config.writer_buffer_memory_size;
    ffi_config.writer_buffer_wait_timeout_ms = config.writer_buffer_wait_timeout_ms;
    ffi_config.connect_timeout_ms = config.connect_timeout_ms;
    ffi_config.security_protocol = rust::String(config.security_protocol);
    ffi_config.security_sasl_mechanism = rust::String(config.security_sasl_mechanism);
    ffi_config.security_sasl_username = rust::String(config.security_sasl_username);
    ffi_config.security_sasl_password = rust::String(config.security_sasl_password);
    ffi_config.lookup_queue_size = config.lookup_queue_size;
    ffi_config.lookup_max_batch_size = config.lookup_max_batch_size;
    ffi_config.lookup_batch_timeout_ms = config.lookup_batch_timeout_ms;
    ffi_config.lookup_max_inflight_requests = config.lookup_max_inflight_requests;
    ffi_config.lookup_max_retries = config.lookup_max_retries;
    return ffi_config;
}

inline ffi::FfiColumn to_ffi_column(const Column& col) {
    ffi::FfiColumn ffi_col;
    ffi_col.name = rust::String(col.name);
    ffi_col.data_type = static_cast<int32_t>(col.data_type.id());
    ffi_col.nullable = col.data_type.nullable();
    ffi_col.comment = rust::String(col.comment);
    ffi_col.precision = col.data_type.precision();
    ffi_col.scale = col.data_type.scale();
    auto flat = flatten_array_type(col.data_type);
    ffi_col.array_nesting = flat.nesting;
    for (auto nullable : flat.array_nullability) {
        ffi_col.array_nullability.push_back(nullable);
    }
    if (flat.nesting > 0 && flat.leaf_type != 0) {
        ffi_col.element_data_type = flat.leaf_type;
        ffi_col.element_precision = flat.leaf_precision;
        ffi_col.element_scale = flat.leaf_scale;
    } else {
        ffi_col.element_data_type = 0;
        ffi_col.element_precision = 0;
        ffi_col.element_scale = 0;
    }
    return ffi_col;
}

inline ffi::FfiSchema to_ffi_schema(const Schema& schema) {
    ffi::FfiSchema ffi_schema;

    rust::Vec<ffi::FfiColumn> cols;
    for (const auto& col : schema.columns) {
        cols.push_back(to_ffi_column(col));
    }
    ffi_schema.columns = std::move(cols);

    rust::Vec<rust::String> pks;
    for (const auto& pk : schema.primary_keys) {
        pks.push_back(rust::String(pk));
    }
    ffi_schema.primary_keys = std::move(pks);

    return ffi_schema;
}

inline ffi::FfiTableDescriptor to_ffi_table_descriptor(const TableDescriptor& desc) {
    ffi::FfiTableDescriptor ffi_desc;

    ffi_desc.schema = to_ffi_schema(desc.schema);

    rust::Vec<rust::String> partition_keys;
    for (const auto& pk : desc.partition_keys) {
        partition_keys.push_back(rust::String(pk));
    }
    ffi_desc.partition_keys = std::move(partition_keys);

    ffi_desc.bucket_count = desc.bucket_count;

    rust::Vec<rust::String> bucket_keys;
    for (const auto& bk : desc.bucket_keys) {
        bucket_keys.push_back(rust::String(bk));
    }
    ffi_desc.bucket_keys = std::move(bucket_keys);

    rust::Vec<ffi::HashMapValue> props;
    for (const auto& [k, v] : desc.properties) {
        ffi::HashMapValue prop;
        prop.key = rust::String(k);
        prop.value = rust::String(v);
        props.push_back(prop);
    }
    ffi_desc.properties = std::move(props);

    rust::Vec<ffi::HashMapValue> custom_props;
    for (const auto& [k, v] : desc.custom_properties) {
        ffi::HashMapValue prop;
        prop.key = rust::String(k);
        prop.value = rust::String(v);
        custom_props.push_back(prop);
    }
    ffi_desc.custom_properties = std::move(custom_props);

    ffi_desc.comment = rust::String(desc.comment);

    return ffi_desc;
}

inline Column from_ffi_column(const ffi::FfiColumn& ffi_col) {
    auto type_id = static_cast<TypeId>(ffi_col.data_type);
    if (type_id == TypeId::Array) {
        auto is_supported_leaf_type = [](int32_t leaf_type) {
            switch (static_cast<TypeId>(leaf_type)) {
                case TypeId::Boolean:
                case TypeId::TinyInt:
                case TypeId::SmallInt:
                case TypeId::Int:
                case TypeId::BigInt:
                case TypeId::Float:
                case TypeId::Double:
                case TypeId::String:
                case TypeId::Bytes:
                case TypeId::Date:
                case TypeId::Time:
                case TypeId::Timestamp:
                case TypeId::TimestampLtz:
                case TypeId::Decimal:
                case TypeId::Char:
                case TypeId::Binary:
                    return true;
                default:
                    return false;
            }
        };
        // ROW/MAP element schema can't pass through the flat FFI column; give the
        // array a non-null element of the right kind so element_type() is safe to deref.
        auto element_id = static_cast<TypeId>(ffi_col.element_data_type);
        if (element_id == TypeId::Map || element_id == TypeId::Row) {
            return Column{std::string(ffi_col.name), DataType::Array(DataType(element_id)),
                          std::string(ffi_col.comment)};
        }
        if (ffi_col.element_data_type == 0) {
            throw std::runtime_error("Malformed ARRAY column '" + std::string(ffi_col.name) +
                                     "': missing element_data_type");
        }
        if (ffi_col.array_nesting < 0) {
            throw std::runtime_error("Malformed ARRAY column '" + std::string(ffi_col.name) +
                                     "': array_nesting must be non-negative");
        }
        if (ffi_col.element_data_type == static_cast<int32_t>(TypeId::Array)) {
            throw std::runtime_error("Malformed ARRAY column '" + std::string(ffi_col.name) +
                                     "': leaf element_data_type cannot be ARRAY");
        }
        if (!is_supported_leaf_type(ffi_col.element_data_type)) {
            throw std::runtime_error("Malformed ARRAY column '" + std::string(ffi_col.name) +
                                     "': unsupported leaf element_data_type " +
                                     std::to_string(ffi_col.element_data_type));
        }

        int32_t nesting = ffi_col.array_nesting > 0 ? ffi_col.array_nesting : 1;
        std::vector<uint8_t> array_nullability;
        for (auto nullable : ffi_col.array_nullability) {
            array_nullability.push_back(nullable);
        }
        auto dt = rebuild_array_type(
            FlattenedArrayType{
                nesting,
                ffi_col.element_data_type,
                ffi_col.element_precision,
                ffi_col.element_scale,
                std::move(array_nullability),
            });
        return Column{std::string(ffi_col.name), std::move(dt), std::string(ffi_col.comment)};
    }
    DataType dt(type_id, ffi_col.precision, ffi_col.scale, ffi_col.nullable);
    return Column{std::string(ffi_col.name), std::move(dt), std::string(ffi_col.comment)};
}

inline Schema from_ffi_schema(const ffi::FfiSchema& ffi_schema) {
    Schema schema;

    for (const auto& col : ffi_schema.columns) {
        schema.columns.push_back(from_ffi_column(col));
    }

    for (const auto& pk : ffi_schema.primary_keys) {
        schema.primary_keys.push_back(std::string(pk));
    }

    return schema;
}

inline TableInfo from_ffi_table_info(const ffi::FfiTableInfo& ffi_info) {
    TableInfo info;

    info.table_id = ffi_info.table_id;
    info.schema_id = ffi_info.schema_id;
    info.table_path = TablePath{std::string(ffi_info.table_path.database_name),
                                std::string(ffi_info.table_path.table_name)};
    info.created_time = ffi_info.created_time;
    info.modified_time = ffi_info.modified_time;

    for (const auto& pk : ffi_info.primary_keys) {
        info.primary_keys.push_back(std::string(pk));
    }

    for (const auto& bk : ffi_info.bucket_keys) {
        info.bucket_keys.push_back(std::string(bk));
    }

    for (const auto& pk : ffi_info.partition_keys) {
        info.partition_keys.push_back(std::string(pk));
    }

    info.num_buckets = ffi_info.num_buckets;
    info.has_primary_key = ffi_info.has_primary_key;
    info.is_partitioned = ffi_info.is_partitioned;

    for (const auto& prop : ffi_info.properties) {
        info.properties[std::string(prop.key)] = std::string(prop.value);
    }

    for (const auto& prop : ffi_info.custom_properties) {
        info.custom_properties[std::string(prop.key)] = std::string(prop.value);
    }

    info.comment = std::string(ffi_info.comment);
    info.schema = from_ffi_schema(ffi_info.schema);

    return info;
}

inline LakeSnapshot from_ffi_lake_snapshot(const ffi::FfiLakeSnapshot& ffi_snapshot) {
    LakeSnapshot snapshot;
    snapshot.snapshot_id = ffi_snapshot.snapshot_id;

    for (const auto& offset : ffi_snapshot.bucket_offsets) {
        snapshot.bucket_offsets.push_back(
            BucketOffset{offset.table_id, offset.partition_id, offset.bucket_id, offset.offset});
    }

    return snapshot;
}

inline ffi::FfiDatabaseDescriptor to_ffi_database_descriptor(const DatabaseDescriptor& desc) {
    ffi::FfiDatabaseDescriptor ffi_desc;
    ffi_desc.comment = rust::String(desc.comment);
    for (const auto& [k, v] : desc.properties) {
        ffi::HashMapValue kv;
        kv.key = rust::String(k);
        kv.value = rust::String(v);
        ffi_desc.properties.push_back(std::move(kv));
    }
    return ffi_desc;
}

inline DatabaseInfo from_ffi_database_info(const ffi::FfiDatabaseInfo& ffi_info) {
    DatabaseInfo info;
    info.database_name = std::string(ffi_info.database_name);
    info.comment = std::string(ffi_info.comment);
    info.created_time = ffi_info.created_time;
    info.modified_time = ffi_info.modified_time;
    for (const auto& prop : ffi_info.properties) {
        info.properties[std::string(prop.key)] = std::string(prop.value);
    }
    return info;
}

}  // namespace utils
}  // namespace fluss
