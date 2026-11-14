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

// The server assigns the auto-increment column, so writes must be partial updates
// that omit it. A full-row write is rejected.

#include <iostream>
#include <set>
#include <string>
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
    fluss::Configuration config;
    config.bootstrap_servers = "127.0.0.1:9123";

    fluss::Connection conn;
    check("create", fluss::Connection::Create(config, conn));

    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));

    fluss::TablePath table_path("fluss", "auto_increment_table_cpp");
    admin.DropTable(table_path, true);

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("uid", fluss::DataType::String())
                      .AddColumn("region", fluss::DataType::String())
                      .AddColumn("uid_int", fluss::DataType::BigInt())
                      .SetPrimaryKeys({"uid"})
                      .SetAutoIncrementColumn("uid_int")
                      .Build();
    auto descriptor = fluss::TableDescriptor::NewBuilder()
                          .SetSchema(schema)
                          .SetBucketCount(1)
                          .Build();
    check("create_table", admin.CreateTable(table_path, descriptor, true));

    fluss::TableInfo table_info;
    check("get_table_info", admin.GetTableInfo(table_path, table_info));
    std::cout << "Created table with auto-increment column: "
              << table_info.schema.auto_increment_columns.at(0) << std::endl;

    fluss::Table table;
    check("get_table", conn.GetTable(table_path, table));

    std::cout << "\n--- Full-row write ---" << std::endl;
    {
        fluss::UpsertWriter writer;
        auto result = table.NewUpsert().CreateWriter(writer);
        if (result.Ok()) {
            std::cerr << "ERROR: expected a full-row writer to be rejected" << std::endl;
            std::exit(1);
        }
        std::cout << "Rejected: " << result.error_message << std::endl;
    }

    std::cout << "\n--- Partial update, omitting the auto-increment column ---" << std::endl;
    fluss::UpsertWriter writer;
    check("new_upsert_writer",
          table.NewUpsert().PartialUpdateByName({"uid", "region"}).CreateWriter(writer));

    const std::vector<std::pair<std::string, std::string>> rows = {
        {"alice", "eu"}, {"bob", "us"}, {"carol", "apac"}};
    for (const auto& [uid, region] : rows) {
        auto row = table.NewRow();
        row.Set("uid", uid);
        row.Set("region", region);
        check("upsert", writer.Upsert(row));
        std::cout << "Upserted uid=" << uid << std::endl;
    }
    check("flush", writer.Flush());

    fluss::Lookuper lookuper;
    check("new_lookuper", table.NewLookup().CreateLookuper(lookuper));

    std::set<int64_t> assigned;
    for (const auto& [uid, region] : rows) {
        auto key = table.NewRow();
        key.Set("uid", uid);

        fluss::LookupResult result;
        check("lookup", lookuper.Lookup(key, result));
        if (!result.Found()) {
            std::cerr << "ERROR: expected to find uid=" << uid << std::endl;
            std::exit(1);
        }
        int64_t uid_int = result.GetInt64("uid_int");
        assigned.insert(uid_int);
        std::cout << "uid=" << uid << " region=" << result.GetString("region")
                  << " uid_int=" << uid_int << std::endl;
    }
    if (assigned.size() != rows.size()) {
        std::cerr << "ERROR: server assigned duplicate values" << std::endl;
        std::exit(1);
    }

    check("drop_table", admin.DropTable(table_path, true));
    std::cout << "\nDropped table" << std::endl;
    return 0;
}
