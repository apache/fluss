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

#include "test_utils.h"

class AdminTest : public ::testing::Test {
   protected:
    fluss::Admin& admin() { return fluss_test::FlussTestEnvironment::Instance()->GetAdmin(); }
};

TEST_F(AdminTest, CreateDatabase) {
    auto& adm = admin();

    std::string db_name = "test_create_database_cpp";

    // Database should not exist initially
    bool exists = true;
    ASSERT_OK(adm.DatabaseExists(db_name, exists));
    ASSERT_FALSE(exists);

    // Create database with descriptor
    fluss::DatabaseDescriptor descriptor;
    descriptor.comment = "test_db";
    descriptor.properties = {{"k1", "v1"}, {"k2", "v2"}};
    ASSERT_OK(adm.CreateDatabase(db_name, descriptor, false));

    // Database should exist now
    ASSERT_OK(adm.DatabaseExists(db_name, exists));
    ASSERT_TRUE(exists);

    // Get database info
    fluss::DatabaseInfo db_info;
    ASSERT_OK(adm.GetDatabaseInfo(db_name, db_info));
    EXPECT_EQ(db_info.database_name, db_name);
    EXPECT_EQ(db_info.comment, "test_db");
    EXPECT_EQ(db_info.properties.at("k1"), "v1");
    EXPECT_EQ(db_info.properties.at("k2"), "v2");

    // Drop database
    ASSERT_OK(adm.DropDatabase(db_name, false, true));

    // Database should not exist now
    ASSERT_OK(adm.DatabaseExists(db_name, exists));
    ASSERT_FALSE(exists);
}

TEST_F(AdminTest, CreateTable) {
    auto& adm = admin();

    std::string db_name = "test_create_table_cpp_db";
    fluss::DatabaseDescriptor db_desc;
    db_desc.comment = "Database for test_create_table";

    bool exists = false;
    ASSERT_OK(adm.DatabaseExists(db_name, exists));
    ASSERT_FALSE(exists);

    ASSERT_OK(adm.CreateDatabase(db_name, db_desc, false));

    std::string table_name = "test_user_table";
    fluss::TablePath table_path(db_name, table_name);

    // Build schema
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .AddColumn("age", fluss::DataType::Int(), "User's age (optional)")
                      .AddColumn("email", fluss::DataType::String())
                      .SetPrimaryKeys({"id"})
                      .Build();

    // Build table descriptor
    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetComment("Test table for user data (id, name, age, email)")
                                .SetBucketCount(3)
                                .SetBucketKeys({"id"})
                                .SetProperty("table.replication.factor", "1")
                                .SetLogFormat("arrow")
                                .SetKvFormat("indexed")
                                .Build();

    // Create table
    ASSERT_OK(adm.CreateTable(table_path, table_descriptor, false));

    // Table should exist
    ASSERT_OK(adm.TableExists(table_path, exists));
    ASSERT_TRUE(exists);

    // List tables
    std::vector<std::string> tables;
    ASSERT_OK(adm.ListTables(db_name, tables));
    ASSERT_EQ(tables.size(), 1u);
    EXPECT_TRUE(std::find(tables.begin(), tables.end(), table_name) != tables.end());

    // Get table info
    fluss::TableInfo table_info;
    ASSERT_OK(adm.GetTableInfo(table_path, table_info));

    EXPECT_EQ(table_info.comment, "Test table for user data (id, name, age, email)");
    EXPECT_EQ(table_info.primary_keys, std::vector<std::string>{"id"});
    EXPECT_EQ(table_info.num_buckets, 3);
    EXPECT_EQ(table_info.bucket_keys, std::vector<std::string>{"id"});

    // Drop table
    ASSERT_OK(adm.DropTable(table_path, false));
    ASSERT_OK(adm.TableExists(table_path, exists));
    ASSERT_FALSE(exists);

    // Drop database
    ASSERT_OK(adm.DropDatabase(db_name, false, true));
    ASSERT_OK(adm.DatabaseExists(db_name, exists));
    ASSERT_FALSE(exists);
}

TEST_F(AdminTest, PartitionApis) {
    auto& adm = admin();

    std::string db_name = "test_partition_apis_cpp_db";
    fluss::DatabaseDescriptor db_desc;
    db_desc.comment = "Database for test_partition_apis";
    ASSERT_OK(adm.CreateDatabase(db_name, db_desc, true));

    fluss::TablePath table_path(db_name, "partitioned_table");

    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .AddColumn("dt", fluss::DataType::String())
                      .AddColumn("region", fluss::DataType::String())
                      .SetPrimaryKeys({"id", "dt", "region"})
                      .Build();

    auto table_descriptor = fluss::TableDescriptor::NewBuilder()
                                .SetSchema(schema)
                                .SetBucketCount(3)
                                .SetBucketKeys({"id"})
                                .SetPartitionKeys({"dt", "region"})
                                .SetProperty("table.replication.factor", "1")
                                .SetLogFormat("arrow")
                                .SetKvFormat("compacted")
                                .Build();

    ASSERT_OK(adm.CreateTable(table_path, table_descriptor, true));

    // No partitions initially
    std::vector<fluss::PartitionInfo> partitions;
    ASSERT_OK(adm.ListPartitionInfos(table_path, partitions));
    ASSERT_TRUE(partitions.empty());

    // Create a partition
    std::unordered_map<std::string, std::string> partition_spec = {
        {"dt", "2024-01-15"}, {"region", "EMEA"}};
    ASSERT_OK(adm.CreatePartition(table_path, partition_spec, false));

    // Should have one partition
    ASSERT_OK(adm.ListPartitionInfos(table_path, partitions));
    ASSERT_EQ(partitions.size(), 1u);
    EXPECT_EQ(partitions[0].partition_name, "2024-01-15$EMEA");

    // List with partial spec filter - should find the partition
    std::unordered_map<std::string, std::string> partial_spec = {{"dt", "2024-01-15"}};
    std::vector<fluss::PartitionInfo> partitions_with_spec;
    ASSERT_OK(adm.ListPartitionInfos(table_path, partial_spec, partitions_with_spec));
    ASSERT_EQ(partitions_with_spec.size(), 1u);
    EXPECT_EQ(partitions_with_spec[0].partition_name, "2024-01-15$EMEA");

    // List with non-matching spec - should find no partitions
    std::unordered_map<std::string, std::string> non_matching_spec = {{"dt", "2024-01-16"}};
    std::vector<fluss::PartitionInfo> empty_partitions;
    ASSERT_OK(adm.ListPartitionInfos(table_path, non_matching_spec, empty_partitions));
    ASSERT_TRUE(empty_partitions.empty());

    // Drop partition
    ASSERT_OK(adm.DropPartition(table_path, partition_spec, false));

    ASSERT_OK(adm.ListPartitionInfos(table_path, partitions));
    ASSERT_TRUE(partitions.empty());

    // Cleanup
    ASSERT_OK(adm.DropTable(table_path, true));
    ASSERT_OK(adm.DropDatabase(db_name, true, true));
}

TEST_F(AdminTest, FlussErrorResponse) {
    auto& adm = admin();

    fluss::TablePath table_path("fluss", "not_exist_cpp");

    fluss::TableInfo info;
    auto result = adm.GetTableInfo(table_path, info);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::TABLE_NOT_EXIST);
}

TEST_F(AdminTest, ErrorDatabaseNotExist) {
    auto& adm = admin();

    // get_database_info for non-existent database
    fluss::DatabaseInfo info;
    auto result = adm.GetDatabaseInfo("no_such_db_cpp", info);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::DATABASE_NOT_EXIST);

    // drop_database without ignore flag
    result = adm.DropDatabase("no_such_db_cpp", false, false);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::DATABASE_NOT_EXIST);

    // list_tables for non-existent database
    std::vector<std::string> tables;
    result = adm.ListTables("no_such_db_cpp", tables);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::DATABASE_NOT_EXIST);
}

TEST_F(AdminTest, ErrorDatabaseAlreadyExist) {
    auto& adm = admin();

    std::string db_name = "test_error_db_already_exist_cpp";
    fluss::DatabaseDescriptor descriptor;

    ASSERT_OK(adm.CreateDatabase(db_name, descriptor, false));

    // Create same database again without ignore flag
    auto result = adm.CreateDatabase(db_name, descriptor, false);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::DATABASE_ALREADY_EXIST);

    // With ignore flag should succeed
    ASSERT_OK(adm.CreateDatabase(db_name, descriptor, true));

    // Cleanup
    ASSERT_OK(adm.DropDatabase(db_name, true, true));
}

TEST_F(AdminTest, ErrorTableAlreadyExist) {
    auto& adm = admin();

    std::string db_name = "test_error_tbl_already_exist_cpp_db";
    fluss::DatabaseDescriptor db_desc;
    ASSERT_OK(adm.CreateDatabase(db_name, db_desc, true));

    fluss::TablePath table_path(db_name, "my_table");
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .Build();
    auto table_desc = fluss::TableDescriptor::NewBuilder()
                          .SetSchema(schema)
                          .SetBucketCount(1)
                          .SetProperty("table.replication.factor", "1")
                          .Build();

    ASSERT_OK(adm.CreateTable(table_path, table_desc, false));

    // Create same table again without ignore flag
    auto result = adm.CreateTable(table_path, table_desc, false);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::TABLE_ALREADY_EXIST);

    // With ignore flag should succeed
    ASSERT_OK(adm.CreateTable(table_path, table_desc, true));

    // Cleanup
    ASSERT_OK(adm.DropTable(table_path, true));
    ASSERT_OK(adm.DropDatabase(db_name, true, true));
}

TEST_F(AdminTest, GetServerNodes) {
    auto& adm = admin();

    std::vector<fluss::ServerNode> nodes;
    ASSERT_OK(adm.GetServerNodes(nodes));

    ASSERT_GT(nodes.size(), 0u) << "Expected at least one server node";

    bool has_coordinator = false;
    bool has_tablet = false;
    for (const auto& node : nodes) {
        EXPECT_FALSE(node.host.empty()) << "Server node host should not be empty";
        EXPECT_GT(node.port, 0u) << "Server node port should be > 0";
        EXPECT_FALSE(node.uid.empty()) << "Server node uid should not be empty";

        if (node.server_type == "CoordinatorServer") {
            has_coordinator = true;
        } else if (node.server_type == "TabletServer") {
            has_tablet = true;
        }
    }
    EXPECT_TRUE(has_coordinator) << "Expected a coordinator server node";
    EXPECT_TRUE(has_tablet) << "Expected at least one tablet server node";
}

TEST_F(AdminTest, ErrorTableNotExist) {
    auto& adm = admin();

    fluss::TablePath table_path("fluss", "no_such_table_cpp");

    // Drop without ignore flag
    auto result = adm.DropTable(table_path, false);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::TABLE_NOT_EXIST);

    // Drop with ignore flag should succeed
    ASSERT_OK(adm.DropTable(table_path, true));
}

TEST_F(AdminTest, ErrorTableNotPartitioned) {
    auto& adm = admin();

    std::string db_name = "test_error_not_partitioned_cpp_db";
    fluss::DatabaseDescriptor db_desc;
    ASSERT_OK(adm.CreateDatabase(db_name, db_desc, true));

    fluss::TablePath table_path(db_name, "non_partitioned_table");
    auto schema = fluss::Schema::NewBuilder()
                      .AddColumn("id", fluss::DataType::Int())
                      .AddColumn("name", fluss::DataType::String())
                      .Build();
    auto table_desc = fluss::TableDescriptor::NewBuilder()
                          .SetSchema(schema)
                          .SetBucketCount(1)
                          .SetProperty("table.replication.factor", "1")
                          .Build();

    ASSERT_OK(adm.CreateTable(table_path, table_desc, false));

    // list_partition_infos on non-partitioned table
    std::vector<fluss::PartitionInfo> partitions;
    auto result = adm.ListPartitionInfos(table_path, partitions);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::TABLE_NOT_PARTITIONED_EXCEPTION);

    // Cleanup
    ASSERT_OK(adm.DropTable(table_path, true));
    ASSERT_OK(adm.DropDatabase(db_name, true, true));
}
