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

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

#include "fluss.hpp"

#define ASSERT_OK(result) ASSERT_TRUE((result).Ok()) << (result).error_message
#define EXPECT_OK(result) EXPECT_TRUE((result).Ok()) << (result).error_message

namespace fluss_test {

inline std::string FindCliBinary() {
    const char* env_bin = std::getenv("FLUSS_TEST_CLUSTER_BIN");
    if (env_bin && std::strlen(env_bin) > 0) {
        if (std::ifstream(env_bin).good()) {
            return env_bin;
        }
        std::cerr << "FLUSS_TEST_CLUSTER_BIN is set to '" << env_bin
                  << "' but that file does not exist." << std::endl;
        std::abort();
    }
    FILE* pipe = popen("cargo locate-project --workspace --message-format plain", "r");
    if (pipe) {
        char buf[512];
        std::string root;
        while (fgets(buf, sizeof(buf), pipe)) root += buf;
        if (pclose(pipe) == 0) {
            // cargo returns path to Cargo.toml; strip filename + trailing whitespace.
            while (!root.empty() && (root.back() == '\n' || root.back() == '\r')) root.pop_back();
            auto slash = root.rfind('/');
            if (slash != std::string::npos) {
                std::string dir = root.substr(0, slash);
                for (const char* profile : {"debug", "release"}) {
                    std::string path = dir + "/target/" + profile + "/fluss-test-cluster";
                    if (std::ifstream(path).good()) return path;
                }
            }
        }
    }
    return "fluss-test-cluster";
}

constexpr const char* kClusterName = "shared-test";

inline std::string CliStartCmd() {
    return FindCliBinary() + " start --sasl --name " + kClusterName;
}

constexpr const char* kClusterJsonPrefix = "CLUSTER_JSON: ";

inline bool ParseClusterJson(const std::string& output, std::string& bootstrap,
                             std::string& sasl_bootstrap) {
    // Look for the CLUSTER_JSON: token in output lines.
    std::istringstream stream(output);
    std::string line;
    while (std::getline(stream, line)) {
        if (line.rfind(kClusterJsonPrefix, 0) != 0) continue;
        std::string json_str = line.substr(std::strlen(kClusterJsonPrefix));
        try {
            auto info = nlohmann::json::parse(json_str);
            bootstrap = info.at("bootstrap_servers").get<std::string>();
            if (info.contains("sasl_bootstrap_servers") &&
                !info["sasl_bootstrap_servers"].is_null()) {
                sasl_bootstrap = info["sasl_bootstrap_servers"].get<std::string>();
            }
            return true;
        } catch (const nlohmann::json::exception& e) {
            std::cerr << "Failed to parse cluster JSON: " << e.what() << "\n"
                      << "Line: " << line << std::endl;
            return false;
        }
    }
    std::cerr << "No CLUSTER_JSON token found in output:\n" << output << std::endl;
    return false;
}

class FlussTestCluster {
   public:
    FlussTestCluster() = default;

    bool Start() {
        const char* env = std::getenv("FLUSS_BOOTSTRAP_SERVERS");
        if (env && std::strlen(env) > 0) {
            bootstrap_servers_ = env;
            const char* env_sasl = std::getenv("FLUSS_SASL_BOOTSTRAP_SERVERS");
            sasl_bootstrap_servers_ = (env_sasl && std::strlen(env_sasl) > 0) ? env_sasl : env;
            return true;
        }

        std::string cli_cmd = CliStartCmd();
        FILE* pipe = popen(cli_cmd.c_str(), "r");
        if (!pipe) {
            std::cerr << "Failed to launch fluss-test-cluster binary" << std::endl;
            return false;
        }
        std::string output;
        char buf[512];
        while (fgets(buf, sizeof(buf), pipe)) output += buf;
        int rc = pclose(pipe);
        if (rc != 0) {
            std::cerr << "fluss-test-cluster start failed (exit " << rc << "):\n"
                      << output << std::endl;
            return false;
        }
        if (!ParseClusterJson(output, bootstrap_servers_, sasl_bootstrap_servers_)) {
            std::cerr << "Failed to parse cluster JSON from:\n" << output << std::endl;
            return false;
        }
        return true;
    }

    static void StopAll() {
        std::string cmd = FindCliBinary() + " stop --name " + kClusterName;
        system(cmd.c_str());
    }

    const std::string& GetBootstrapServers() const { return bootstrap_servers_; }
    const std::string& GetSaslBootstrapServers() const { return sasl_bootstrap_servers_; }

   private:
    std::string bootstrap_servers_;
    std::string sasl_bootstrap_servers_;
};

class FlussTestEnvironment : public ::testing::Environment {
   public:
    static FlussTestEnvironment* Instance() {
        static FlussTestEnvironment* instance = nullptr;
        if (!instance) {
            instance = new FlussTestEnvironment();
        }
        return instance;
    }

    void SetUp() override {
        if (!cluster_.Start()) {
            GTEST_SKIP() << "Failed to start Fluss cluster. Skipping integration tests.";
        }

        fluss::Configuration config;
        config.bootstrap_servers = cluster_.GetBootstrapServers();
        auto result = fluss::Connection::Create(config, connection_);
        if (!result.Ok()) {
            GTEST_SKIP() << "Failed to connect: " << result.error_message;
        }
        auto admin_result = connection_.GetAdmin(admin_);
        if (!admin_result.Ok()) {
            GTEST_SKIP() << "Failed to get admin: " << admin_result.error_message;
        }
    }

    void TearDown() override {}

    fluss::Connection& GetConnection() { return connection_; }
    fluss::Admin& GetAdmin() { return admin_; }
    const std::string& GetBootstrapServers() { return cluster_.GetBootstrapServers(); }
    const std::string& GetSaslBootstrapServers() { return cluster_.GetSaslBootstrapServers(); }

   private:
    FlussTestEnvironment() = default;

    FlussTestCluster cluster_;
    fluss::Connection connection_;
    fluss::Admin admin_;
};

inline void CreateTable(fluss::Admin& admin, const fluss::TablePath& path,
                        const fluss::TableDescriptor& descriptor) {
    admin.DropTable(path, true);  // ignore if not exists
    auto result = admin.CreateTable(path, descriptor, false);
    ASSERT_OK(result);
}

inline void CreatePartitions(fluss::Admin& admin, const fluss::TablePath& path,
                             const std::string& partition_column,
                             const std::vector<std::string>& values) {
    for (const auto& value : values) {
        std::unordered_map<std::string, std::string> spec;
        spec[partition_column] = value;
        auto result = admin.CreatePartition(path, spec, true);
        ASSERT_OK(result);
    }
}

template <typename T, typename ExtractFn>
void PollRecords(fluss::LogScanner& scanner, size_t expected_count, ExtractFn extract_fn,
                 std::vector<T>& out) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (out.size() < expected_count && std::chrono::steady_clock::now() < deadline) {
        fluss::ScanRecords records;
        ASSERT_OK(scanner.Poll(1000, records));
        for (auto rec : records) {
            out.push_back(extract_fn(rec));
        }
    }
}

template <typename T, typename ExtractFn>
void PollRecordBatches(fluss::LogScanner& scanner, size_t expected_count, ExtractFn extract_fn,
                       std::vector<T>& out) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (out.size() < expected_count && std::chrono::steady_clock::now() < deadline) {
        fluss::ArrowRecordBatches batches;
        ASSERT_OK(scanner.PollRecordBatch(1000, batches));
        auto items = extract_fn(batches);
        out.insert(out.end(), items.begin(), items.end());
    }
}

}  // namespace fluss_test
