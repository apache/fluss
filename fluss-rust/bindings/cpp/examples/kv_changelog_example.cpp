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

// Primary-key table changelog (CDC) example: subscribe to a KV table's
// changelog and print each row-level change as a +I / -U / +U / -D event.

#include <iostream>
#include <string>

#include "fluss.hpp"

static void check(const char* step, const fluss::Result& r) {
    if (!r.Ok()) {
        std::cerr << step << " failed: code=" << r.error_code << " msg=" << r.error_message
                  << std::endl;
        std::exit(1);
    }
}

static const char* change_symbol(fluss::ChangeType ct) {
    switch (ct) {
        case fluss::ChangeType::AppendOnly:
            return "+A";
        case fluss::ChangeType::Insert:
            return "+I";
        case fluss::ChangeType::UpdateBefore:
            return "-U";
        case fluss::ChangeType::UpdateAfter:
            return "+U";
        case fluss::ChangeType::Delete:
            return "-D";
    }
    return "?";
}

int main() {
    fluss::Configuration config;
    config.bootstrap_servers = "127.0.0.1:9123";

    fluss::Connection conn;
    check("create", fluss::Connection::Create(config, conn));

    fluss::Admin admin;
    check("get_admin", conn.GetAdmin(admin));

    fluss::TablePath table_path("fluss", "kv_changelog_cpp");
    admin.DropTable(table_path, true);

    // A single bucket keeps the changelog on one bucket and in order, which
    // makes the CDC output easy to follow.
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .SetPrimaryKeys({"id"})
                      .Build();

    auto descriptor = fluss::TableDescriptor::NewBuilder()
                          .SetSchema(schema)
                          .SetBucketCount(1)
                          .SetComment("cpp kv changelog example")
                          .Build();

    check("create_table", admin.CreateTable(table_path, descriptor, false));
    std::cout << "Created PK table: " << table_path.ToString() << std::endl;

    fluss::Table table;
    check("get_table", conn.GetTable(table_path, table));

    fluss::UpsertWriter writer;
    check("new_upsert_writer", table.NewUpsert().CreateWriter(writer));

    // Insert three keys (+I), update one (-U / +U) and delete one (-D).
    for (const auto& kv : {std::pair<int32_t, const char*>{1, "alice"},
                           {2, "bob"}, {3, "carol"}}) {
        fluss::GenericRow row(2);
        row.SetInt32(0, kv.first);
        row.SetString(1, kv.second);
        check("upsert", writer.Upsert(row));
    }
    {
        fluss::GenericRow row(2);
        row.SetInt32(0, 2);
        row.SetString(1, "bob-v2");
        check("update", writer.Upsert(row));
    }
    {
        fluss::GenericRow del(2);
        del.SetInt32(0, 3);
        check("delete", writer.Delete(del));
    }
    check("flush", writer.Flush());

    // Subscribe from the start of the changelog and print each CDC event until
    // we reach the end of the log.
    auto table_scan = table.NewScan();
    fluss::LogScanner log_scanner;
    check("create_log_scanner", table_scan.CreateLogScanner(log_scanner));
    check("subscribe", log_scanner.Subscribe(0, fluss::EARLIEST_OFFSET));

    std::cout << "Changelog (change_type id name):" << std::endl;
    while (true) {
        fluss::ScanRecords records;
        check("poll", log_scanner.Poll(3000, records));
        if (records.IsEmpty()) {
            break;
        }
        for (auto rec : records) {
            std::cout << "  " << change_symbol(rec.change_type) << " " << rec.row.GetInt32(0) << " "
                      << rec.row.GetString(1) << std::endl;
        }
    }

    check("drop_table", admin.DropTable(table_path, true));
    std::cout << "\nKV changelog example completed successfully!" << std::endl;
    return 0;
}
