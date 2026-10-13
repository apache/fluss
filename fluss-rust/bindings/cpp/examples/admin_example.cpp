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

#include <iostream>
#include <string>
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
    const std::string db_name = "admin_example_db";
    const std::string table_name = "admin_example_table";

    // 1) Connect and get Admin
    fluss::Configuration config;
    config.bootstrap_servers = "127.0.0.1:9123";

    fluss::Connection conn;
    check("create", fluss::Connection::Create(config, conn));

    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));

    // 2) Database operations
    std::cout << "--- Database operations ---" << std::endl;

    bool exists = false;
    check("database_exists (before create)", admin.DatabaseExists(db_name, exists));
    std::cout << "Database " << db_name << " exists before create: " << (exists ? "yes" : "no")
              << std::endl;

    fluss::DatabaseDescriptor db_desc;
    db_desc.comment = "Example database for Admin API";
    db_desc.properties["owner"] = "admin_example";
    check("create_database", admin.CreateDatabase(db_name, db_desc, true));

    check("database_exists (after create)", admin.DatabaseExists(db_name, exists));
    std::cout << "Database " << db_name << " exists after create: " << (exists ? "yes" : "no")
              << std::endl;

    fluss::DatabaseInfo db_info;
    check("get_database_info", admin.GetDatabaseInfo(db_name, db_info));
    std::cout << "Database info: name=" << db_info.database_name << " comment=" << db_info.comment
              << " created_time=" << db_info.created_time << std::endl;

    std::vector<std::string> databases;
    check("list_databases", admin.ListDatabases(databases));
    std::cout << "List databases (" << databases.size() << "): ";
    for (size_t i = 0; i < databases.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << databases[i];
    }
    std::cout << std::endl;

    // 3) Table operations in the new database
    std::cout << "--- Table operations ---" << std::endl;

    fluss::TablePath table_path(db_name, table_name);

    bool table_exists_flag = false;
    check("table_exists (before create)", admin.TableExists(table_path, table_exists_flag));
    std::cout << "Table " << db_name << "." << table_name
              << " exists before create: " << (table_exists_flag ? "yes" : "no") << std::endl;

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .Build();
    auto descriptor = fluss::TableDescriptor::NewBuilder()
                          .SetSchema(schema)
                          .SetBucketCount(1)
                          .SetComment("admin example table")
                          .Build();

    check("create_table", admin.CreateTable(table_path, descriptor, true));

    check("table_exists (after create)", admin.TableExists(table_path, table_exists_flag));
    std::cout << "Table exists after create: " << (table_exists_flag ? "yes" : "no") << std::endl;

    std::vector<std::string> tables;
    check("list_tables", admin.ListTables(db_name, tables));
    std::cout << "List tables in " << db_name << " (" << tables.size() << "): ";
    for (size_t i = 0; i < tables.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << tables[i];
    }
    std::cout << std::endl;

    // 4) Cleanup: drop table, then drop database
    std::cout << "--- Cleanup ---" << std::endl;
    check("drop_table", admin.DropTable(table_path, true));
    check("drop_database", admin.DropDatabase(db_name, true, true));

    check("database_exists (after drop)", admin.DatabaseExists(db_name, exists));
    std::cout << "Database exists after drop: " << (exists ? "yes" : "no") << std::endl;

    std::cout << "Admin example completed successfully." << std::endl;
    return 0;
}
