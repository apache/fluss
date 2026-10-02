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

class SaslAuthTest : public ::testing::Test {
   protected:
    const std::string& sasl_servers() {
        return fluss_test::FlussTestEnvironment::Instance()->GetSaslBootstrapServers();
    }
    const std::string& plaintext_servers() {
        return fluss_test::FlussTestEnvironment::Instance()->GetBootstrapServers();
    }
};

TEST_F(SaslAuthTest, SaslConnectWithValidCredentials) {
    fluss::Configuration config;
    config.bootstrap_servers = sasl_servers();
    config.security_protocol = "sasl";
    config.security_sasl_mechanism = "PLAIN";
    config.security_sasl_username = "admin";
    config.security_sasl_password = "admin-secret";

    fluss::Connection conn;
    ASSERT_OK(fluss::Connection::Create(config, conn));

    fluss::Admin admin;
    ASSERT_OK(conn.GetAdmin(admin));

    // Perform a basic operation to confirm the connection is fully functional
    std::string db_name = "cpp_sasl_test_valid_db";
    fluss::DatabaseDescriptor descriptor;
    descriptor.comment = "created via SASL auth";
    ASSERT_OK(admin.CreateDatabase(db_name, descriptor, true));

    bool exists = false;
    ASSERT_OK(admin.DatabaseExists(db_name, exists));
    ASSERT_TRUE(exists);

    ASSERT_OK(admin.DropDatabase(db_name, true, true));
}

TEST_F(SaslAuthTest, SaslConnectWithSecondUser) {
    fluss::Configuration config;
    config.bootstrap_servers = sasl_servers();
    config.security_protocol = "sasl";
    config.security_sasl_mechanism = "PLAIN";
    config.security_sasl_username = "alice";
    config.security_sasl_password = "alice-secret";

    fluss::Connection conn;
    ASSERT_OK(fluss::Connection::Create(config, conn));

    fluss::Admin admin;
    ASSERT_OK(conn.GetAdmin(admin));

    // Basic operation to confirm functional connection
    bool exists = false;
    ASSERT_OK(admin.DatabaseExists("some_nonexistent_db_alice", exists));
    ASSERT_FALSE(exists);
}

TEST_F(SaslAuthTest, SaslConnectWithWrongPassword) {
    fluss::Configuration config;
    config.bootstrap_servers = sasl_servers();
    config.security_protocol = "sasl";
    config.security_sasl_mechanism = "PLAIN";
    config.security_sasl_username = "admin";
    config.security_sasl_password = "wrong-password";

    fluss::Connection conn;
    auto result = fluss::Connection::Create(config, conn);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::AUTHENTICATE_EXCEPTION);
    EXPECT_NE(result.error_message.find("Authentication failed"), std::string::npos)
        << "Expected 'Authentication failed' in: " << result.error_message;
}

TEST_F(SaslAuthTest, SaslConnectWithUnknownUser) {
    fluss::Configuration config;
    config.bootstrap_servers = sasl_servers();
    config.security_protocol = "sasl";
    config.security_sasl_mechanism = "PLAIN";
    config.security_sasl_username = "nonexistent_user";
    config.security_sasl_password = "some-password";

    fluss::Connection conn;
    auto result = fluss::Connection::Create(config, conn);
    ASSERT_FALSE(result.Ok());
    EXPECT_EQ(result.error_code, fluss::ErrorCode::AUTHENTICATE_EXCEPTION);
    EXPECT_NE(result.error_message.find("Authentication failed"), std::string::npos)
        << "Expected 'Authentication failed' in: " << result.error_message;
}

TEST_F(SaslAuthTest, SaslClientToPlaintextServer) {
    fluss::Configuration config;
    config.bootstrap_servers = plaintext_servers();
    config.security_protocol = "sasl";
    config.security_sasl_mechanism = "PLAIN";
    config.security_sasl_username = "admin";
    config.security_sasl_password = "admin-secret";

    fluss::Connection conn;
    auto result = fluss::Connection::Create(config, conn);
    ASSERT_FALSE(result.Ok()) << "SASL client connecting to plaintext server should fail";
}
