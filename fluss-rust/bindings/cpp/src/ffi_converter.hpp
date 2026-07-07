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

/// Flattened view of an `ARRAY<ARRAY<...<scalar>>>` element type, used by the
/// data-writer path: `nesting` counts the ARRAY wrappers stripped to reach the
/// leaf scalar, which `leaf_*` describe. A non-array input yields `nesting == 0`
/// and callers use the type's own id/precision/scale.
struct FlattenedArrayType {
    int32_t nesting{0};
    int32_t leaf_type{0};
    int32_t leaf_precision{0};
    int32_t leaf_scale{0};
};

inline FlattenedArrayType flatten_array_type(const DataType& data_type) {
    FlattenedArrayType out;
    if (data_type.id() != TypeId::Array) {
        return out;
    }
    const DataType* current = &data_type;
    while (current && current->id() == TypeId::Array) {
        out.nesting += 1;
        current = current->element_type();
    }
    if (!current) {
        return FlattenedArrayType{};
    }
    out.leaf_type = static_cast<int32_t>(current->id());
    out.leaf_precision = current->precision();
    out.leaf_scale = current->scale();
    return out;
}

/// Serialize a type tree into a preorder node arena (see `FfiTypeNode`): the
/// node itself, then ARRAY's element / MAP's key+value / ROW's field subtrees.
/// `field_name` is set only for ROW field nodes. Inverse of `nodes_to_data_type`.
inline void data_type_to_nodes(const DataType& dt, const std::string& field_name,
                               rust::Vec<ffi::FfiTypeNode>& out) {
    ffi::FfiTypeNode node;
    node.type_id = static_cast<int32_t>(dt.id());
    node.nullable = dt.nullable();
    node.precision = dt.precision();
    node.scale = dt.scale();
    node.field_name = rust::String(field_name);
    switch (dt.id()) {
        case TypeId::Array:
            node.child_count = 1;
            break;
        case TypeId::Map:
            node.child_count = 2;
            break;
        case TypeId::Row:
            node.child_count = static_cast<uint32_t>(dt.field_count());
            break;
        default:
            node.child_count = 0;
            break;
    }
    out.push_back(std::move(node));

    switch (dt.id()) {
        case TypeId::Array:
            data_type_to_nodes(*dt.element_type(), "", out);
            break;
        case TypeId::Map:
            data_type_to_nodes(*dt.key_type(), "", out);
            data_type_to_nodes(*dt.value_type(), "", out);
            break;
        case TypeId::Row:
            for (size_t i = 0; i < dt.field_count(); ++i) {
                data_type_to_nodes(*dt.field_type(i), dt.field_name(i), out);
            }
            break;
        default:
            break;
    }
}

/// Builds a scalar DataType from a leaf node, restoring precision/scale/length.
inline DataType scalar_node_to_data_type(const ffi::FfiTypeNode& node) {
    switch (static_cast<TypeId>(node.type_id)) {
        case TypeId::Boolean:
            return DataType::Boolean();
        case TypeId::TinyInt:
            return DataType::TinyInt();
        case TypeId::SmallInt:
            return DataType::SmallInt();
        case TypeId::Int:
            return DataType::Int();
        case TypeId::BigInt:
            return DataType::BigInt();
        case TypeId::Float:
            return DataType::Float();
        case TypeId::Double:
            return DataType::Double();
        case TypeId::String:
            return DataType::String();
        case TypeId::Bytes:
            return DataType::Bytes();
        case TypeId::Date:
            return DataType::Date();
        case TypeId::Time:
            return DataType::Time(node.precision);
        case TypeId::Timestamp:
            return DataType::Timestamp(node.precision);
        case TypeId::TimestampLtz:
            return DataType::TimestampLtz(node.precision);
        case TypeId::Decimal:
            return DataType::Decimal(node.precision, node.scale);
        case TypeId::Char:
            return DataType::Char(node.precision);
        case TypeId::Binary:
            return DataType::Binary(node.precision);
        default:
            throw std::runtime_error("Unknown scalar type id in type tree: " +
                                     std::to_string(node.type_id));
    }
}

/// Inverse of `data_type_to_nodes`: reconstruct one type from the arena,
/// advancing `cursor` past the nodes it consumes.
inline DataType nodes_to_data_type(const rust::Vec<ffi::FfiTypeNode>& nodes, size_t& cursor) {
    if (cursor >= nodes.size()) {
        throw std::runtime_error("type tree ended before all nodes were read");
    }
    const ffi::FfiTypeNode& node = nodes[cursor++];
    DataType dt = [&]() -> DataType {
        switch (static_cast<TypeId>(node.type_id)) {
            case TypeId::Array: {
                DataType element = nodes_to_data_type(nodes, cursor);
                return DataType::Array(std::move(element));
            }
            case TypeId::Map: {
                DataType key = nodes_to_data_type(nodes, cursor);
                DataType value = nodes_to_data_type(nodes, cursor);
                return DataType::Map(std::move(key), std::move(value));
            }
            case TypeId::Row: {
                std::vector<std::pair<std::string, DataType>> fields;
                fields.reserve(node.child_count);
                for (uint32_t i = 0; i < node.child_count; ++i) {
                    if (cursor >= nodes.size()) {
                        throw std::runtime_error("ROW field missing from type tree");
                    }
                    std::string fname(nodes[cursor].field_name);
                    DataType ftype = nodes_to_data_type(nodes, cursor);
                    fields.emplace_back(std::move(fname), std::move(ftype));
                }
                return DataType::Row(std::move(fields));
            }
            default:
                return scalar_node_to_data_type(node);
        }
    }();
    if (!node.nullable) {
        dt = dt.NotNull();
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
    ffi_col.comment = rust::String(col.comment);
    data_type_to_nodes(col.data_type, "", ffi_col.type_nodes);
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
    size_t cursor = 0;
    DataType dt = nodes_to_data_type(ffi_col.type_nodes, cursor);
    if (cursor != ffi_col.type_nodes.size()) {
        throw std::runtime_error("Column '" + std::string(ffi_col.name) +
                                 "': type tree has trailing nodes");
    }
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
