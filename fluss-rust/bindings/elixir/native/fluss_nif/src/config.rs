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

use fluss::config::{Config, NoKeyAssigner};
use rustler::{NifStruct, NifUnitEnum};

/// Bucket-assigner strategy for tables without bucket keys.
/// Maps to fluss::config::NoKeyAssigner.
#[derive(NifUnitEnum)]
pub enum NifNoKeyAssigner {
    Sticky,
    RoundRobin,
}

/// Decoded from `%Fluss.Config{}` Elixir struct.
#[derive(NifStruct)]
#[module = "Fluss.Config"]
pub struct NifConfig {
    pub bootstrap_servers: String,
    pub connect_timeout_ms: Option<u64>,
    pub remote_file_download_thread_num: Option<u64>,
    pub scanner_log_fetch_max_bytes: Option<i32>,
    pub scanner_log_fetch_max_bytes_for_bucket: Option<i32>,
    pub scanner_log_fetch_min_bytes: Option<i32>,
    pub scanner_log_fetch_wait_max_time_ms: Option<i32>,
    pub scanner_log_max_poll_records: Option<u64>,
    pub scanner_remote_log_prefetch_num: Option<u64>,
    pub scanner_remote_log_read_concurrency: Option<u64>,
    pub security_protocol: Option<String>,
    pub security_sasl_mechanism: Option<String>,
    pub security_sasl_password: Option<String>,
    pub security_sasl_username: Option<String>,
    pub writer_acks: Option<String>,
    pub writer_batch_size: Option<i32>,
    pub writer_batch_timeout_ms: Option<i64>,
    pub writer_bucket_no_key_assigner: Option<NifNoKeyAssigner>,
    pub writer_buffer_memory_size: Option<u64>,
    pub writer_buffer_wait_timeout_ms: Option<u64>,
    pub writer_dynamic_batch_size_enabled: Option<bool>,
    pub writer_dynamic_batch_size_min: Option<i32>,
    pub writer_enable_idempotence: Option<bool>,
    pub writer_max_inflight_requests_per_bucket: Option<u64>,
    pub writer_request_max_size: Option<i32>,
    pub writer_retries: Option<i32>,
}

impl NifConfig {
    pub fn into_core(self) -> Config {
        let mut config = Config {
            bootstrap_servers: self.bootstrap_servers,
            ..Config::default()
        };
        if let Some(timeout) = self.connect_timeout_ms {
            config.connect_timeout_ms = timeout;
        }
        if let Some(n) = self.remote_file_download_thread_num {
            config.remote_file_download_thread_num = n as usize;
        }
        if let Some(size) = self.scanner_log_fetch_max_bytes {
            config.scanner_log_fetch_max_bytes = size;
        }
        if let Some(size) = self.scanner_log_fetch_max_bytes_for_bucket {
            config.scanner_log_fetch_max_bytes_for_bucket = size;
        }
        if let Some(size) = self.scanner_log_fetch_min_bytes {
            config.scanner_log_fetch_min_bytes = size;
        }
        if let Some(ms) = self.scanner_log_fetch_wait_max_time_ms {
            config.scanner_log_fetch_wait_max_time_ms = ms;
        }
        if let Some(n) = self.scanner_log_max_poll_records {
            config.scanner_log_max_poll_records = n as usize;
        }
        if let Some(n) = self.scanner_remote_log_prefetch_num {
            config.scanner_remote_log_prefetch_num = n as usize;
        }
        if let Some(n) = self.scanner_remote_log_read_concurrency {
            config.scanner_remote_log_read_concurrency = n as usize;
        }
        if let Some(protocol) = self.security_protocol {
            config.security_protocol = protocol;
        }
        if let Some(mechanism) = self.security_sasl_mechanism {
            config.security_sasl_mechanism = mechanism;
        }
        if let Some(password) = self.security_sasl_password {
            config.security_sasl_password = password;
        }
        if let Some(username) = self.security_sasl_username {
            config.security_sasl_username = username;
        }
        if let Some(size) = self.writer_batch_size {
            config.writer_batch_size = size;
        }
        if let Some(ms) = self.writer_batch_timeout_ms {
            config.writer_batch_timeout_ms = ms;
        }
        if let Some(enabled) = self.writer_dynamic_batch_size_enabled {
            config.writer_dynamic_batch_size_enabled = enabled;
        }
        if let Some(size) = self.writer_dynamic_batch_size_min {
            config.writer_dynamic_batch_size_min = size;
        }
        if let Some(acks) = self.writer_acks {
            config.writer_acks = acks;
        }
        if let Some(assigner) = self.writer_bucket_no_key_assigner {
            config.writer_bucket_no_key_assigner = match assigner {
                NifNoKeyAssigner::Sticky => NoKeyAssigner::Sticky,
                NifNoKeyAssigner::RoundRobin => NoKeyAssigner::RoundRobin,
            };
        }
        if let Some(memory_size) = self.writer_buffer_memory_size {
            config.writer_buffer_memory_size = memory_size as usize;
        }
        if let Some(timeout_ms) = self.writer_buffer_wait_timeout_ms {
            config.writer_buffer_wait_timeout_ms = timeout_ms;
        }
        if let Some(enabled) = self.writer_enable_idempotence {
            config.writer_enable_idempotence = enabled;
        }
        if let Some(requests_limit) = self.writer_max_inflight_requests_per_bucket {
            config.writer_max_inflight_requests_per_bucket = requests_limit as usize;
        }
        if let Some(max_size) = self.writer_request_max_size {
            config.writer_request_max_size = max_size;
        }
        if let Some(retries) = self.writer_retries {
            config.writer_retries = retries;
        }
        config
    }
}
