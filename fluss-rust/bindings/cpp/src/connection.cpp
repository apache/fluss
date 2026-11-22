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

namespace fluss {

Connection::Connection() noexcept = default;

Connection::~Connection() noexcept { Destroy(); }

void Connection::Destroy() noexcept {
    if (conn_) {
        ffi::delete_connection(conn_);
        conn_ = nullptr;
    }
}

Connection::Connection(Connection&& other) noexcept : conn_(other.conn_) { other.conn_ = nullptr; }

Connection& Connection::operator=(Connection&& other) noexcept {
    if (this != &other) {
        Destroy();
        conn_ = other.conn_;
        other.conn_ = nullptr;
    }
    return *this;
}

Result Connection::Create(const Configuration& config, Connection& out) {
    auto ffi_config = utils::to_ffi_config(config);
    auto ffi_result = ffi::new_connection(ffi_config);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.conn_ = utils::ptr_from_ffi<ffi::Connection>(ffi_result);
    }
    return result;
}

bool Connection::Available() const { return conn_ != nullptr; }

Result Connection::GetAdmin(Admin& out) {
    if (!Available()) {
        return utils::make_client_error("Connection not available");
    }

    auto ffi_result = conn_->get_admin();
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.admin_ = utils::ptr_from_ffi<ffi::Admin>(ffi_result);
    }
    return result;
}

Result Connection::GetTable(const TablePath& table_path, Table& out) {
    if (!Available()) {
        return utils::make_client_error("Connection not available");
    }

    auto ffi_path = utils::to_ffi_table_path(table_path);
    auto ffi_result = conn_->get_table(ffi_path);
    auto result = utils::from_ffi_result(ffi_result.result);
    if (result.Ok()) {
        out.table_ = utils::ptr_from_ffi<ffi::Table>(ffi_result);
    }
    return result;
}

}  // namespace fluss
