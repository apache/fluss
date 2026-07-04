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

#include "ffi_converter.hpp"
#include "fluss.hpp"
#include "lib.rs.h"
#include "rust/cxx.h"
#include "type_lowering.hpp"
#include <arrow/c/bridge.h>
#include <exception>

namespace fluss {

Admin::Admin() noexcept = default;

Admin::Admin(ffi::Admin* admin) noexcept : admin_(admin) {}

Admin::~Admin() noexcept { Destroy(); }

void Admin::Destroy() noexcept {
    if (admin_) {
        ffi::delete_admin(admin_);
        admin_ = nullptr;
    }
}

Admin::Admin(Admin&& other) noexcept : admin_(other.admin_) { other.admin_ = nullptr; }

Admin& Admin::operator=(Admin&& other) noexcept {
    if (this != &other) {
        Destroy();
        admin_ = other.admin_;
        other.admin_ = nullptr;
    }
    return *this;
}

bool Admin::Available() const { return admin_ != nullptr; }

Result Admin::CreateTable(const TablePath& table_path, const TableDescriptor& descriptor,
                          bool ignore_if_exists) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);

    // A MAP/ROW column can't go through the flat FFI encoding, so the schema is
    // sent over Arrow instead (explicit via FromArrow, or lowered from native
    // columns). Rust derives the columns from it, so the flat columns are dropped
    // here; primary keys / metadata still come from the descriptor.
    std::shared_ptr<arrow::Schema> arrow_schema = descriptor.schema.arrow_schema;
    if (!arrow_schema) {
        for (const auto& col : descriptor.schema.columns) {
            if (detail::is_compound(col.data_type)) {
                arrow_schema = detail::columns_to_arrow_schema(descriptor.schema.columns);
                break;
            }
        }
    }

    if (arrow_schema) {
        TableDescriptor arrow_desc = descriptor;
        arrow_desc.schema.columns.clear();
        auto ffi_desc = utils::to_ffi_table_descriptor(arrow_desc);
        size_t schema_ptr = 0;
        try {
            schema_ptr = detail::export_arrow_schema(*arrow_schema);
        } catch (const std::exception& e) {
            return utils::make_client_error(e.what());
        }
        auto ffi_result =
            admin_->create_table_arrow(ffi_path, ffi_desc, schema_ptr, ignore_if_exists);
        return utils::from_ffi_result(ffi_result);
    }

    auto ffi_desc = utils::to_ffi_table_descriptor(descriptor);
    auto ffi_result = admin_->create_table(ffi_path, ffi_desc, ignore_if_exists);
    return utils::from_ffi_result(ffi_result);
}

Result Admin::DropTable(const TablePath& table_path, bool ignore_if_not_exists) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);
    auto ffi_result = admin_->drop_table(ffi_path, ignore_if_not_exists);
    return utils::from_ffi_result(ffi_result);
}

Result Admin::GetTableInfo(const TablePath& table_path, TableInfo& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);
    auto ffi_result = admin_->get_table_info(ffi_path);

    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        try {
            out = utils::from_ffi_table_info(ffi_result.table_info);
        } catch (const std::exception& e) {
            return utils::make_client_error(std::string("Failed to parse table metadata: ") + e.what());
        }
    }

    return result;
}

Result Admin::GetLatestLakeSnapshot(const TablePath& table_path, LakeSnapshot& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);
    auto ffi_result = admin_->get_latest_lake_snapshot(ffi_path);

    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out = utils::from_ffi_lake_snapshot(ffi_result.lake_snapshot);
    }

    return result;
}

// function for common list offsets functionality
Result Admin::DoListOffsets(const TablePath& table_path, const std::vector<int32_t>& bucket_ids,
                            const OffsetSpec& offset_spec,
                            std::unordered_map<int32_t, int64_t>& out,
                            const std::string* partition_name) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);

    rust::Vec<int32_t> rust_bucket_ids;
    for (int32_t id : bucket_ids) {
        rust_bucket_ids.push_back(id);
    }

    ffi::FfiOffsetQuery ffi_query;
    ffi_query.offset_type = static_cast<int32_t>(offset_spec.type);
    ffi_query.timestamp = offset_spec.timestamp;

    ffi::FfiListOffsetsResult ffi_result;
    if (partition_name != nullptr) {
        ffi_result = admin_->list_partition_offsets(ffi_path, rust::String(*partition_name),
                                                    std::move(rust_bucket_ids), ffi_query);
    } else {
        ffi_result = admin_->list_offsets(ffi_path, std::move(rust_bucket_ids), ffi_query);
    }

    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.clear();
        for (const auto& pair : ffi_result.bucket_offsets) {
            out[pair.bucket_id] = pair.offset;
        }
    }

    return result;
}

Result Admin::ListOffsets(const TablePath& table_path, const std::vector<int32_t>& bucket_ids,
                          const OffsetSpec& offset_spec,
                          std::unordered_map<int32_t, int64_t>& out) {
    return DoListOffsets(table_path, bucket_ids, offset_spec, out);
}

Result Admin::ListPartitionOffsets(const TablePath& table_path, const std::string& partition_name,
                                   const std::vector<int32_t>& bucket_ids,
                                   const OffsetSpec& offset_spec,
                                   std::unordered_map<int32_t, int64_t>& out) {
    return DoListOffsets(table_path, bucket_ids, offset_spec, out, &partition_name);
}

Result Admin::ListPartitionInfos(const TablePath& table_path, std::vector<PartitionInfo>& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);
    auto ffi_result = admin_->list_partition_infos(ffi_path);

    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.clear();
        out.reserve(ffi_result.partition_infos.size());
        for (const auto& pi : ffi_result.partition_infos) {
            out.push_back({pi.partition_id, std::string(pi.partition_name)});
        }
    }

    return result;
}

Result Admin::ListPartitionInfos(const TablePath& table_path,
                                 const std::unordered_map<std::string, std::string>& partition_spec,
                                 std::vector<PartitionInfo>& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);

    rust::Vec<ffi::FfiPartitionKeyValue> rust_spec;
    for (const auto& [key, value] : partition_spec) {
        ffi::FfiPartitionKeyValue kv;
        kv.key = rust::String(key);
        kv.value = rust::String(value);
        rust_spec.push_back(std::move(kv));
    }

    auto ffi_result = admin_->list_partition_infos_with_spec(ffi_path, std::move(rust_spec));

    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.clear();
        out.reserve(ffi_result.partition_infos.size());
        for (const auto& pi : ffi_result.partition_infos) {
            out.push_back({pi.partition_id, std::string(pi.partition_name)});
        }
    }

    return result;
}

Result Admin::CreatePartition(const TablePath& table_path,
                              const std::unordered_map<std::string, std::string>& partition_spec,
                              bool ignore_if_exists) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);

    rust::Vec<ffi::FfiPartitionKeyValue> rust_spec;
    for (const auto& [key, value] : partition_spec) {
        ffi::FfiPartitionKeyValue kv;
        kv.key = rust::String(key);
        kv.value = rust::String(value);
        rust_spec.push_back(std::move(kv));
    }

    auto ffi_result = admin_->create_partition(ffi_path, std::move(rust_spec), ignore_if_exists);
    return utils::from_ffi_result(ffi_result);
}

Result Admin::DropPartition(const TablePath& table_path,
                            const std::unordered_map<std::string, std::string>& partition_spec,
                            bool ignore_if_not_exists) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);

    rust::Vec<ffi::FfiPartitionKeyValue> rust_spec;
    for (const auto& [key, value] : partition_spec) {
        ffi::FfiPartitionKeyValue kv;
        kv.key = rust::String(key);
        kv.value = rust::String(value);
        rust_spec.push_back(std::move(kv));
    }

    auto ffi_result = admin_->drop_partition(ffi_path, std::move(rust_spec), ignore_if_not_exists);
    return utils::from_ffi_result(ffi_result);
}

Result Admin::CreateDatabase(const std::string& database_name, const DatabaseDescriptor& descriptor,
                             bool ignore_if_exists) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_desc = utils::to_ffi_database_descriptor(descriptor);
    auto ffi_result = admin_->create_database(rust::Str(database_name), ffi_desc, ignore_if_exists);
    return utils::from_ffi_result(ffi_result);
}

Result Admin::DropDatabase(const std::string& database_name, bool ignore_if_not_exists,
                           bool cascade) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_result =
        admin_->drop_database(rust::Str(database_name), ignore_if_not_exists, cascade);
    return utils::from_ffi_result(ffi_result);
}

Result Admin::ListDatabases(std::vector<std::string>& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_result = admin_->list_databases();
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.clear();
        out.reserve(ffi_result.database_names.size());
        for (const auto& name : ffi_result.database_names) {
            out.push_back(std::string(name));
        }
    }
    return result;
}

Result Admin::DatabaseExists(const std::string& database_name, bool& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_result = admin_->database_exists(rust::Str(database_name));
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out = ffi_result.value;
    }
    return result;
}

Result Admin::GetDatabaseInfo(const std::string& database_name, DatabaseInfo& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_result = admin_->get_database_info(rust::Str(database_name));
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out = utils::from_ffi_database_info(ffi_result.database_info);
    }
    return result;
}

Result Admin::ListTables(const std::string& database_name, std::vector<std::string>& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_result = admin_->list_tables(rust::Str(database_name));
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.clear();
        out.reserve(ffi_result.table_names.size());
        for (const auto& name : ffi_result.table_names) {
            out.push_back(std::string(name));
        }
    }
    return result;
}

Result Admin::TableExists(const TablePath& table_path, bool& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);
    auto ffi_result = admin_->table_exists(ffi_path);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out = ffi_result.value;
    }
    return result;
}

Result Admin::GetServerNodes(std::vector<ServerNode>& out) {
    if (!Available()) {
        return utils::make_client_error("Admin not available");
    }

    auto ffi_result = admin_->get_server_nodes();
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.clear();
        out.reserve(ffi_result.server_nodes.size());
        for (const auto& node : ffi_result.server_nodes) {
            out.push_back({node.node_id, std::string(node.host), node.port,
                           std::string(node.server_type), std::string(node.uid)});
        }
    }
    return result;
}

}  // namespace fluss
