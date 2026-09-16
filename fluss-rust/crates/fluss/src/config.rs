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

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use strum_macros::{Display, EnumString};

const DEFAULT_BOOTSTRAP_SERVER: &str = "127.0.0.1:9123";
const DEFAULT_REQUEST_MAX_SIZE: i32 = 10 * 1024 * 1024;
const DEFAULT_WRITER_BATCH_SIZE: i32 = 2 * 1024 * 1024;
// Mirrors Java's `2 * pageSize` floor with default pageSize = 128 KB.
const DEFAULT_WRITER_DYNAMIC_BATCH_SIZE_MIN: i32 = 256 * 1024;
const DEFAULT_WRITER_DYNAMIC_BATCH_SIZE_ENABLED: bool = true;
const DEFAULT_RETRIES: i32 = i32::MAX;
const DEFAULT_PREFETCH_NUM: usize = 4;
const DEFAULT_DOWNLOAD_THREADS: usize = 3;
const DEFAULT_SCANNER_REMOTE_LOG_READ_CONCURRENCY: usize = 4;
const DEFAULT_MAX_POLL_RECORDS: usize = 500;
const DEFAULT_SCANNER_LOG_FETCH_MAX_BYTES: i32 = 16 * 1024 * 1024;
const DEFAULT_SCANNER_LOG_FETCH_MIN_BYTES: i32 = 1;
const DEFAULT_SCANNER_LOG_FETCH_WAIT_MAX_TIME_MS: i32 = 500;
const DEFAULT_WRITER_BATCH_TIMEOUT_MS: i64 = 100;
const DEFAULT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET: i32 = 1024 * 1024;
const DEFAULT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET: usize = 5;
const DEFAULT_WRITER_BUFFER_MEMORY_SIZE: usize = 64 * 1024 * 1024; // 64MB, matching Java
const DEFAULT_WRITER_BUFFER_WAIT_TIMEOUT_MS: u64 = u64::MAX;
const DEFAULT_WRITER_KV_BACKPRESSURE_MAX_THROTTLE_MS: u64 = 3000;

const MAX_IN_FLIGHT_REQUESTS_PER_BUCKET_FOR_IDEMPOTENCE: usize = 5;
const DEFAULT_ACKS: &str = "all";
const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_SECURITY_PROTOCOL: &str = "PLAINTEXT";
const DEFAULT_SASL_MECHANISM: &str = "PLAIN";

/// Bucket assigner strategy for tables without bucket keys.
/// Matches Java `client.writer.bucket.no-key-assigner`.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Deserialize, Serialize, EnumString, Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(ascii_case_insensitive)]
pub enum NoKeyAssigner {
    /// Sticks to one bucket until the batch is full, then switches.
    #[strum(serialize = "sticky")]
    Sticky,
    /// Assigns each record to the next bucket in a rotating sequence.
    #[strum(serialize = "round_robin")]
    RoundRobin,
}

#[derive(Parser, Clone, Deserialize, Serialize)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long, default_value_t = String::from(DEFAULT_BOOTSTRAP_SERVER))]
    pub bootstrap_servers: String,

    #[arg(long, default_value_t = DEFAULT_REQUEST_MAX_SIZE)]
    pub writer_request_max_size: i32,

    #[arg(long, default_value_t = String::from(DEFAULT_ACKS))]
    pub writer_acks: String,

    #[arg(long, default_value_t = DEFAULT_RETRIES)]
    pub writer_retries: i32,

    #[arg(long, default_value_t = DEFAULT_WRITER_BATCH_SIZE)]
    pub writer_batch_size: i32,

    /// Tune the per-table writer batch size from observed fill ratios.
    /// Default: true (matching Java `client.writer.dynamic-batch-size.enabled`).
    #[arg(long, default_value_t = DEFAULT_WRITER_DYNAMIC_BATCH_SIZE_ENABLED)]
    pub writer_dynamic_batch_size_enabled: bool,

    /// Lower bound for the dynamic batch size estimator.
    /// Default: 262144 (256 KB), matching Java's `2 * pageSize` floor.
    /// Ignored when `writer_dynamic_batch_size_enabled` is false.
    #[arg(long, default_value_t = DEFAULT_WRITER_DYNAMIC_BATCH_SIZE_MIN)]
    pub writer_dynamic_batch_size_min: i32,

    #[arg(long, value_enum, default_value_t = NoKeyAssigner::Sticky)]
    pub writer_bucket_no_key_assigner: NoKeyAssigner,

    /// Maximum number of remote log segments to prefetch
    /// Default: 4 (matching Java CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM)
    #[arg(long, default_value_t = DEFAULT_PREFETCH_NUM)]
    pub scanner_remote_log_prefetch_num: usize,

    /// Maximum concurrent remote log downloads
    /// Default: 3 (matching Java REMOTE_FILE_DOWNLOAD_THREAD_NUM)
    #[arg(long, default_value_t = DEFAULT_DOWNLOAD_THREADS)]
    pub remote_file_download_thread_num: usize,

    /// Intra-file remote log read concurrency for each remote segment download.
    /// Download path always uses streaming reader.
    #[arg(long, default_value_t = DEFAULT_SCANNER_REMOTE_LOG_READ_CONCURRENCY)]
    pub scanner_remote_log_read_concurrency: usize,

    /// Maximum number of records returned in a single call to poll() for LogScanner.
    /// Default: 500 (matching Java CLIENT_SCANNER_LOG_MAX_POLL_RECORDS)
    #[arg(long, default_value_t = DEFAULT_MAX_POLL_RECORDS)]
    pub scanner_log_max_poll_records: usize,

    /// Maximum bytes per fetch response for LogScanner.
    /// Default: 16777216 (16MB)
    #[arg(long, default_value_t = DEFAULT_SCANNER_LOG_FETCH_MAX_BYTES)]
    pub scanner_log_fetch_max_bytes: i32,

    /// Minimum bytes to accumulate before returning a fetch response.
    /// Default: 1
    #[arg(long, default_value_t = DEFAULT_SCANNER_LOG_FETCH_MIN_BYTES)]
    pub scanner_log_fetch_min_bytes: i32,

    /// Maximum time the server may wait (ms) to satisfy min-bytes.
    /// Default: 500
    #[arg(long, default_value_t = DEFAULT_SCANNER_LOG_FETCH_WAIT_MAX_TIME_MS)]
    pub scanner_log_fetch_wait_max_time_ms: i32,

    /// The maximum time to wait for a batch to be completed in milliseconds.
    /// Default: 100 (matching Java CLIENT_WRITER_BATCH_TIMEOUT)
    #[arg(long, default_value_t = DEFAULT_WRITER_BATCH_TIMEOUT_MS)]
    pub writer_batch_timeout_ms: i64,

    /// Maximum bytes per fetch response **per bucket** for LogScanner.
    /// Default: 1048576 (1MB)
    #[arg(long, default_value_t = DEFAULT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET)]
    pub scanner_log_fetch_max_bytes_for_bucket: i32,

    /// Whether to enable idempotent writes. When enabled, each batch is tagged with
    /// a server-allocated writer ID and per-bucket sequence number so the server can
    /// detect and deduplicate retried batches.
    /// Default: true (matching Java CLIENT_WRITER_ENABLE_IDEMPOTENCE)
    #[arg(long, default_value_t = true)]
    pub writer_enable_idempotence: bool,

    /// Maximum number of in-flight requests per bucket for idempotent writes.
    /// Default: 5 (matching Java client.writer.max-inflight-requests-per-bucket)
    #[arg(long, default_value_t = DEFAULT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET)]
    pub writer_max_inflight_requests_per_bucket: usize,

    /// Total memory available for buffering write batches across all buckets.
    /// When this limit is reached, `upsert()`/`append()` will block until
    /// in-flight batches complete and free memory.
    /// Default: 64MB (matching Java's LazyMemorySegmentPool: 512 pages x 128KB)
    #[arg(long, default_value_t = DEFAULT_WRITER_BUFFER_MEMORY_SIZE)]
    pub writer_buffer_memory_size: usize,

    /// Maximum time in milliseconds to block waiting for buffer memory.
    /// If the timeout is exceeded, the write call returns an error.
    #[arg(long, default_value_t = DEFAULT_WRITER_BUFFER_WAIT_TIMEOUT_MS)]
    pub writer_buffer_wait_timeout_ms: u64,

    /// Maximum KV backpressure throttle in milliseconds. A pressure `p` delays the bucket by
    /// `max_throttle * p²`; a hard rejection uses the full window.
    /// Default: 3000 (matching Java `client.writer.kv-backpressure.max-throttle`)
    #[arg(long, default_value_t = DEFAULT_WRITER_KV_BACKPRESSURE_MAX_THROTTLE_MS)]
    pub writer_kv_backpressure_max_throttle_ms: u64,

    /// Connect timeout in milliseconds for TCP transport connect.
    /// Default: 120000 (120 seconds).
    #[arg(long, default_value_t = DEFAULT_CONNECT_TIMEOUT_MS)]
    pub connect_timeout_ms: u64,

    #[arg(long, default_value_t = String::from(DEFAULT_SECURITY_PROTOCOL))]
    pub security_protocol: String,

    #[arg(long, default_value_t = String::from(DEFAULT_SASL_MECHANISM))]
    pub security_sasl_mechanism: String,

    #[arg(long, default_value_t = String::new())]
    pub security_sasl_username: String,

    #[arg(long, default_value_t = String::new())]
    #[serde(skip_serializing)]
    pub security_sasl_password: String,
    /// Maximum number of pending lookup operations
    /// Default: 25600 (matching Java CLIENT_LOOKUP_QUEUE_SIZE)
    #[arg(long, default_value_t = 25600)]
    pub lookup_queue_size: usize,

    /// Maximum batch size of merging lookup operations to one lookup request
    /// Default: 128 (matching Java CLIENT_LOOKUP_MAX_BATCH_SIZE)
    #[arg(long, default_value_t = 128)]
    pub lookup_max_batch_size: usize,

    /// Maximum time to wait for the lookup batch to fill (in milliseconds)
    /// Default: 100 (matching Java CLIENT_LOOKUP_BATCH_TIMEOUT)
    #[arg(long, default_value_t = 100)]
    pub lookup_batch_timeout_ms: u64,

    /// Maximum number of unacknowledged lookup requests
    /// Default: 128 (matching Java CLIENT_LOOKUP_MAX_INFLIGHT_SIZE)
    #[arg(long, default_value_t = 128)]
    pub lookup_max_inflight_requests: usize,

    /// Maximum number of lookup retries
    /// Default: i32::MAX (matching Java CLIENT_LOOKUP_MAX_RETRIES)
    #[arg(long, default_value_t = i32::MAX)]
    pub lookup_max_retries: i32,
}

type ConfigPropertyApplier = fn(&mut Config, &str) -> Result<(), String>;

struct ConfigPropertySpec {
    key: &'static str,
    sensitive: bool,
    apply: ConfigPropertyApplier,
}

const CONFIG_PROPERTY_SPECS: &[ConfigPropertySpec] = &[
    ConfigPropertySpec {
        key: "security.protocol",
        sensitive: false,
        apply: |config, value| {
            config.security_protocol = value.to_string();
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "security.sasl.mechanism",
        sensitive: false,
        apply: |config, value| {
            config.security_sasl_mechanism = value.to_string();
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "security.sasl.username",
        sensitive: false,
        apply: |config, value| {
            config.security_sasl_username = value.to_string();
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "security.sasl.password",
        sensitive: true,
        apply: |config, value| {
            config.security_sasl_password = value.to_string();
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "connect-timeout",
        sensitive: false,
        apply: |config, value| {
            config.connect_timeout_ms = parse_duration_ms(value)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "writer.batch-size",
        sensitive: false,
        apply: |config, value| {
            config.writer_batch_size = parse_i32_bytes(value)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "writer.request-max-size",
        sensitive: false,
        apply: |config, value| {
            config.writer_request_max_size = parse_i32_bytes(value)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "writer.buffer.memory-size",
        sensitive: false,
        apply: |config, value| {
            config.writer_buffer_memory_size = parse_usize_bytes(value)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "writer.buffer.wait-timeout",
        sensitive: false,
        apply: |config, value| {
            config.writer_buffer_wait_timeout_ms = parse_duration_ms(value)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "writer.batch-timeout",
        sensitive: false,
        apply: |config, value| {
            config.writer_batch_timeout_ms = i64::try_from(parse_duration_ms(value)?)
                .map_err(|_| "duration exceeds the supported range".to_string())?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "writer.dynamic-batch-size.enabled",
        sensitive: false,
        apply: |config, value| {
            config.writer_dynamic_batch_size_enabled = parse_bool(value)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "writer.dynamic-batch-size.min",
        sensitive: false,
        apply: |config, value| {
            config.writer_dynamic_batch_size_min = parse_i32_bytes(value)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "writer.kv-backpressure.max-throttle",
        sensitive: false,
        apply: |config, value| {
            config.writer_kv_backpressure_max_throttle_ms = parse_duration_ms(value)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "lookup.queue-size",
        sensitive: false,
        apply: |config, value| {
            config.lookup_queue_size = parse_usize_count(value, 1)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "lookup.max-batch-size",
        sensitive: false,
        apply: |config, value| {
            config.lookup_max_batch_size = parse_usize_count(value, 1)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "lookup.max-inflight-requests",
        sensitive: false,
        apply: |config, value| {
            config.lookup_max_inflight_requests = parse_usize_count(value, 1)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "lookup.max-retries",
        sensitive: false,
        apply: |config, value| {
            config.lookup_max_retries = parse_i32_count(value, 0)?;
            Ok(())
        },
    },
    ConfigPropertySpec {
        key: "lookup.batch-timeout",
        sensitive: false,
        apply: |config, value| {
            config.lookup_batch_timeout_ms = parse_duration_ms(value)?;
            Ok(())
        },
    },
];

fn config_property_spec(key: &str) -> Option<&'static ConfigPropertySpec> {
    CONFIG_PROPERTY_SPECS.iter().find(|spec| spec.key == key)
}

fn parse_bool(value: &str) -> Result<bool, String> {
    value
        .parse::<bool>()
        .map_err(|_| "expected 'true' or 'false'".to_string())
}

fn parse_i32_count(value: &str, min: i32) -> Result<i32, String> {
    let parsed = value
        .parse::<i32>()
        .map_err(|_| "expected a 32-bit integer".to_string())?;
    if parsed < min {
        return Err(format!("must be at least {min}"));
    }
    Ok(parsed)
}

fn parse_usize_count(value: &str, min: usize) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| "expected a non-negative integer".to_string())?;
    if parsed < min {
        return Err(format!("must be at least {min}"));
    }
    Ok(parsed)
}

fn parse_i32_bytes(value: &str) -> Result<i32, String> {
    i32::try_from(parse_byte_size(value)?)
        .map_err(|_| "byte size exceeds the supported range".to_string())
}

fn parse_usize_bytes(value: &str) -> Result<usize, String> {
    usize::try_from(parse_byte_size(value)?)
        .map_err(|_| "byte size exceeds the supported range".to_string())
}

fn parse_byte_size(value: &str) -> Result<u64, String> {
    let (number, unit) = split_number_and_unit(value)?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "" | "b" | "bytes" => 1,
        "k" | "kb" | "kib" | "kibibyte" | "kibibytes" => 1_024,
        "m" | "mb" | "mib" | "mebibyte" | "mebibytes" => 1_024 * 1_024,
        "g" | "gb" | "gib" | "gibibyte" | "gibibytes" => 1_024 * 1_024 * 1_024,
        "t" | "tb" | "tib" | "tebibyte" | "tebibytes" => 1_024_u64 * 1_024 * 1_024 * 1_024,
        _ => return Err("unsupported byte-size unit".to_string()),
    };
    let bytes = number
        .checked_mul(multiplier)
        .ok_or_else(|| "byte size exceeds the supported range".to_string())?;
    if bytes == 0 {
        return Err("byte size must be greater than zero".to_string());
    }
    Ok(bytes)
}

fn parse_duration_ms(value: &str) -> Result<u64, String> {
    let (number, unit) = split_number_and_unit(value)?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return Err("expected a duration ending in ms, s, m, h, or d".to_string()),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| "duration exceeds the supported range".to_string())
}

fn split_number_and_unit(value: &str) -> Result<(u64, &str), String> {
    let value = value.trim();
    let split_at = value
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(value.len());
    if split_at == 0 {
        return Err("expected an unsigned integer followed by a unit".to_string());
    }
    let number = value[..split_at]
        .parse::<u64>()
        .map_err(|_| "number exceeds the supported range".to_string())?;
    Ok((number, value[split_at..].trim()))
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("writer_request_max_size", &self.writer_request_max_size)
            .field("writer_acks", &self.writer_acks)
            .field("writer_retries", &self.writer_retries)
            .field("writer_batch_size", &self.writer_batch_size)
            .field(
                "writer_dynamic_batch_size_enabled",
                &self.writer_dynamic_batch_size_enabled,
            )
            .field(
                "writer_dynamic_batch_size_min",
                &self.writer_dynamic_batch_size_min,
            )
            .field(
                "writer_bucket_no_key_assigner",
                &self.writer_bucket_no_key_assigner,
            )
            .field(
                "scanner_remote_log_prefetch_num",
                &self.scanner_remote_log_prefetch_num,
            )
            .field(
                "remote_file_download_thread_num",
                &self.remote_file_download_thread_num,
            )
            .field(
                "scanner_log_max_poll_records",
                &self.scanner_log_max_poll_records,
            )
            .field(
                "scanner_log_fetch_max_bytes",
                &self.scanner_log_fetch_max_bytes,
            )
            .field(
                "scanner_log_fetch_min_bytes",
                &self.scanner_log_fetch_min_bytes,
            )
            .field(
                "scanner_log_fetch_max_bytes_for_bucket",
                &self.scanner_log_fetch_max_bytes_for_bucket,
            )
            .field(
                "scanner_log_fetch_wait_max_time_ms",
                &self.scanner_log_fetch_wait_max_time_ms,
            )
            .field("writer_batch_timeout_ms", &self.writer_batch_timeout_ms)
            .field("writer_enable_idempotence", &self.writer_enable_idempotence)
            .field(
                "writer_max_inflight_requests_per_bucket",
                &self.writer_max_inflight_requests_per_bucket,
            )
            .field("writer_buffer_memory_size", &self.writer_buffer_memory_size)
            .field(
                "writer_buffer_wait_timeout_ms",
                &self.writer_buffer_wait_timeout_ms,
            )
            .field(
                "writer_kv_backpressure_max_throttle_ms",
                &self.writer_kv_backpressure_max_throttle_ms,
            )
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("security_protocol", &self.security_protocol)
            .field("security_sasl_mechanism", &self.security_sasl_mechanism)
            .field("security_sasl_username", &self.security_sasl_username)
            .field("security_sasl_password", &"[REDACTED]")
            .field("lookup_queue_size", &self.lookup_queue_size)
            .field("lookup_max_batch_size", &self.lookup_max_batch_size)
            .field("lookup_batch_timeout_ms", &self.lookup_batch_timeout_ms)
            .field(
                "lookup_max_inflight_requests",
                &self.lookup_max_inflight_requests,
            )
            .field("lookup_max_retries", &self.lookup_max_retries)
            .finish()
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::from(DEFAULT_BOOTSTRAP_SERVER),
            writer_request_max_size: DEFAULT_REQUEST_MAX_SIZE,
            writer_acks: String::from(DEFAULT_ACKS),
            writer_retries: i32::MAX,
            writer_batch_size: DEFAULT_WRITER_BATCH_SIZE,
            writer_dynamic_batch_size_enabled: DEFAULT_WRITER_DYNAMIC_BATCH_SIZE_ENABLED,
            writer_dynamic_batch_size_min: DEFAULT_WRITER_DYNAMIC_BATCH_SIZE_MIN,
            writer_bucket_no_key_assigner: NoKeyAssigner::Sticky,
            scanner_remote_log_prefetch_num: DEFAULT_PREFETCH_NUM,
            remote_file_download_thread_num: DEFAULT_DOWNLOAD_THREADS,
            scanner_remote_log_read_concurrency: DEFAULT_SCANNER_REMOTE_LOG_READ_CONCURRENCY,
            scanner_log_max_poll_records: DEFAULT_MAX_POLL_RECORDS,
            scanner_log_fetch_max_bytes: DEFAULT_SCANNER_LOG_FETCH_MAX_BYTES,
            scanner_log_fetch_min_bytes: DEFAULT_SCANNER_LOG_FETCH_MIN_BYTES,
            scanner_log_fetch_wait_max_time_ms: DEFAULT_SCANNER_LOG_FETCH_WAIT_MAX_TIME_MS,
            scanner_log_fetch_max_bytes_for_bucket: DEFAULT_SCANNER_LOG_FETCH_MAX_BYTES_FOR_BUCKET,
            writer_batch_timeout_ms: DEFAULT_WRITER_BATCH_TIMEOUT_MS,
            writer_enable_idempotence: true,
            writer_max_inflight_requests_per_bucket:
                DEFAULT_WRITER_MAX_INFLIGHT_REQUESTS_PER_BUCKET,
            writer_buffer_memory_size: DEFAULT_WRITER_BUFFER_MEMORY_SIZE,
            writer_buffer_wait_timeout_ms: DEFAULT_WRITER_BUFFER_WAIT_TIMEOUT_MS,
            writer_kv_backpressure_max_throttle_ms: DEFAULT_WRITER_KV_BACKPRESSURE_MAX_THROTTLE_MS,
            connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
            security_protocol: String::from(DEFAULT_SECURITY_PROTOCOL),
            security_sasl_mechanism: String::from(DEFAULT_SASL_MECHANISM),
            security_sasl_username: String::new(),
            security_sasl_password: String::new(),
            lookup_queue_size: 25600,
            lookup_max_batch_size: 128,
            lookup_batch_timeout_ms: 100,
            lookup_max_inflight_requests: 128,
            lookup_max_retries: i32::MAX,
        }
    }
}

impl Config {
    /// Applies canonical string properties to a cloned config. Cross-field
    /// validation is deferred to [`Config::validate`].
    pub fn try_with_properties(
        &self,
        properties: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let mut candidate = self.clone();
        for (key, value) in properties {
            let spec = config_property_spec(key)
                .ok_or_else(|| format!("unknown config property '{key}'"))?;
            (spec.apply)(&mut candidate, value)
                .map_err(|reason| format!("invalid value for config property '{key}': {reason}"))?;
        }
        Ok(candidate)
    }

    /// Returns whether a canonical property contains sensitive data. Unknown
    /// properties are treated as sensitive to keep logging fail-safe.
    pub fn is_sensitive_property(key: &str) -> bool {
        config_property_spec(key).is_none_or(|spec| spec.sensitive)
    }

    /// Validates all configuration groups.
    pub fn validate(&self) -> Result<(), String> {
        self.validate_security()?;
        self.validate_scanner()?;
        self.validate_writer()?;
        self.validate_lookup()
    }

    /// Returns true when the security protocol indicates SASL authentication
    /// should be performed. Matches Java's `SaslAuthenticationPlugin` which
    /// registers as `"sasl"` (case-insensitive).
    pub fn is_sasl_enabled(&self) -> bool {
        self.security_protocol.eq_ignore_ascii_case("sasl")
    }
    /// Validates security configuration. Returns `Ok(())` when the config is
    /// consistent, or an error message when SASL is enabled but the config is
    /// incomplete or uses an unsupported mechanism.
    pub fn validate_security(&self) -> Result<(), String> {
        if !self.is_sasl_enabled() {
            return Ok(());
        }
        if !self.security_sasl_mechanism.eq_ignore_ascii_case("PLAIN") {
            return Err(format!(
                "Unsupported SASL mechanism: '{}'. Only 'PLAIN' is supported.",
                self.security_sasl_mechanism
            ));
        }
        if self.security_sasl_username.is_empty() {
            return Err(
                "security_sasl_username must be set when security_protocol is 'sasl'".to_string(),
            );
        }
        if self.security_sasl_password.is_empty() {
            return Err(
                "security_sasl_password must be set when security_protocol is 'sasl'".to_string(),
            );
        }
        Ok(())
    }
    pub fn validate_scanner(&self) -> Result<(), String> {
        if self.scanner_remote_log_prefetch_num == 0 {
            return Err("scanner_remote_log_prefetch_num must be > 0".to_string());
        }
        if self.scanner_remote_log_read_concurrency == 0 {
            return Err("scanner_remote_log_read_concurrency must be > 0".to_string());
        }
        if self.remote_file_download_thread_num == 0 {
            return Err("remote_file_download_thread_num must be > 0".to_string());
        }
        // scanner_log_max_poll_records: validation intentionally omitted to match Java behavior.
        // Java allows 0 — tracked in https://github.com/apache/fluss/issues/3068
        if self.scanner_log_fetch_min_bytes <= 0 {
            return Err("scanner_log_fetch_min_bytes must be > 0".to_string());
        }
        if self.scanner_log_fetch_max_bytes <= 0 {
            return Err("scanner_log_fetch_max_bytes must be > 0".to_string());
        }
        if self.scanner_log_fetch_max_bytes < self.scanner_log_fetch_min_bytes {
            return Err(
                "scanner_log_fetch_max_bytes must be >= scanner_log_fetch_min_bytes".to_string(),
            );
        }
        if self.scanner_log_fetch_wait_max_time_ms < 0 {
            return Err("scanner_log_fetch_wait_max_time_ms must be >= 0".to_string());
        }
        if self.scanner_log_fetch_max_bytes_for_bucket <= 0 {
            return Err("scanner_log_fetch_max_bytes_for_bucket must be > 0".to_string());
        }
        if self.scanner_log_fetch_max_bytes_for_bucket > self.scanner_log_fetch_max_bytes {
            return Err(
                "scanner_log_fetch_max_bytes_for_bucket must be <= scanner_log_fetch_max_bytes"
                    .to_string(),
            );
        }
        Ok(())
    }

    pub fn validate_writer(&self) -> Result<(), String> {
        if self.writer_request_max_size <= 0 {
            return Err("writer_request_max_size must be > 0".to_string());
        }
        if self.writer_batch_size <= 0 {
            return Err("writer_batch_size must be > 0".to_string());
        }
        if self.writer_batch_timeout_ms < 0 {
            return Err("writer_batch_timeout_ms must be >= 0".to_string());
        }
        if self.writer_max_inflight_requests_per_bucket == 0 {
            return Err("writer_max_inflight_requests_per_bucket must be > 0".to_string());
        }
        if self.writer_buffer_memory_size == 0 {
            return Err("writer_buffer_memory_size must be > 0".to_string());
        }
        if self.writer_batch_size > self.writer_request_max_size {
            return Err("writer_batch_size must be <= writer_request_max_size".to_string());
        }
        if self.writer_batch_size as usize > self.writer_buffer_memory_size {
            return Err("writer_batch_size must be <= writer_buffer_memory_size".to_string());
        }
        if self.writer_dynamic_batch_size_min <= 0 {
            return Err("writer_dynamic_batch_size_min must be > 0".to_string());
        }
        if self.writer_dynamic_batch_size_min > self.writer_batch_size {
            return Err("writer_dynamic_batch_size_min must be <= writer_batch_size".to_string());
        }
        // idempotence checks
        if !self.writer_enable_idempotence {
            return Ok(());
        }
        let acks_is_all = self.writer_acks.eq_ignore_ascii_case("all") || self.writer_acks == "-1";
        if !acks_is_all {
            return Err(format!(
                "Idempotent writes require acks='all' (-1), but got acks='{}'",
                self.writer_acks
            ));
        }
        if self.writer_retries <= 0 {
            return Err(format!(
                "Idempotent writes require retries > 0, but got retries={}",
                self.writer_retries
            ));
        }
        if self.writer_max_inflight_requests_per_bucket
            > MAX_IN_FLIGHT_REQUESTS_PER_BUCKET_FOR_IDEMPOTENCE
        {
            return Err(format!(
                "Idempotent writes require max-inflight-requests-per-bucket <= {}, but got {}",
                MAX_IN_FLIGHT_REQUESTS_PER_BUCKET_FOR_IDEMPOTENCE,
                self.writer_max_inflight_requests_per_bucket
            ));
        }
        Ok(())
    }

    pub fn validate_lookup(&self) -> Result<(), String> {
        if self.lookup_queue_size == 0 {
            return Err("lookup_queue_size must be > 0".to_string());
        }
        if self.lookup_max_batch_size == 0 {
            return Err("lookup_max_batch_size must be > 0".to_string());
        }
        if self.lookup_max_inflight_requests == 0 {
            return Err("lookup_max_inflight_requests must be > 0".to_string());
        }
        if self.lookup_max_retries < 0 {
            return Err("lookup_max_retries must be >= 0".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn test_parse_byte_size_matches_fluss_units() {
        for (value, expected) in [
            ("512", 512),
            ("512 bytes", 512),
            ("1KB", 1_024),
            ("1 KiB", 1_024),
            ("1mebibyte", 1_048_576),
            ("1GB", 1_073_741_824),
            ("1 TB", 1_099_511_627_776),
        ] {
            assert_eq!(parse_byte_size(value).unwrap(), expected);
        }
        assert!(parse_byte_size("0B").is_err());
        assert!(parse_byte_size("1PB").is_err());
    }

    #[test]
    fn test_parse_duration_units() {
        assert_eq!(parse_duration_ms("1d").unwrap(), 86_400_000);
        assert_eq!(parse_duration_ms(" 2 D ").unwrap(), 172_800_000);
        assert!(parse_duration_ms("1w").is_err());
    }

    #[test]
    fn test_try_with_properties_applies_canonical_properties() {
        let config = Config::default();
        let overlay = properties(&[
            ("connect-timeout", "2s"),
            ("lookup.batch-timeout", "3s"),
            ("lookup.max-batch-size", "64"),
            ("lookup.max-inflight-requests", "32"),
            ("lookup.max-retries", "10"),
            ("lookup.queue-size", "1024"),
            ("security.protocol", "sasl"),
            ("security.sasl.mechanism", "PLAIN"),
            ("security.sasl.password", "secret"),
            ("security.sasl.username", "admin"),
            ("writer.batch-size", "4MiB"),
            ("writer.batch-timeout", "5s"),
            ("writer.buffer.memory-size", "16MiB"),
            ("writer.buffer.wait-timeout", "6s"),
            ("writer.dynamic-batch-size.enabled", "false"),
            ("writer.dynamic-batch-size.min", "1MiB"),
            ("writer.kv-backpressure.max-throttle", "1m"),
            ("writer.request-max-size", "8MiB"),
        ]);

        let overlaid = config.try_with_properties(&overlay).unwrap();

        assert_eq!(overlaid.connect_timeout_ms, 2_000);
        assert_eq!(overlaid.lookup_batch_timeout_ms, 3_000);
        assert_eq!(overlaid.lookup_max_batch_size, 64);
        assert_eq!(overlaid.lookup_max_inflight_requests, 32);
        assert_eq!(overlaid.lookup_max_retries, 10);
        assert_eq!(overlaid.lookup_queue_size, 1_024);
        assert_eq!(overlaid.security_protocol, "sasl");
        assert_eq!(overlaid.security_sasl_mechanism, "PLAIN");
        assert_eq!(overlaid.security_sasl_username, "admin");
        assert_eq!(overlaid.security_sasl_password, "secret");
        assert_eq!(overlaid.writer_batch_size, 4 * 1_048_576);
        assert_eq!(overlaid.writer_batch_timeout_ms, 5_000);
        assert_eq!(overlaid.writer_buffer_memory_size, 16 * 1_048_576);
        assert_eq!(overlaid.writer_buffer_wait_timeout_ms, 6_000);
        assert!(!overlaid.writer_dynamic_batch_size_enabled);
        assert_eq!(overlaid.writer_dynamic_batch_size_min, 1_048_576);
        assert_eq!(overlaid.writer_kv_backpressure_max_throttle_ms, 60_000);
        assert_eq!(overlaid.writer_request_max_size, 8 * 1_048_576);
    }

    #[test]
    fn test_try_with_properties_applies_related_properties() {
        let config = Config::default();
        let overlay = properties(&[
            ("writer.batch-size", "32MiB"),
            ("writer.buffer.memory-size", "128MiB"),
            ("writer.request-max-size", "64MiB"),
        ]);

        let overlaid = config.try_with_properties(&overlay).unwrap();

        assert_eq!(overlaid.writer_batch_size, 32 * 1_048_576);
        assert_eq!(overlaid.writer_request_max_size, 64 * 1_048_576);
        assert_eq!(overlaid.writer_buffer_memory_size, 128 * 1_048_576);
        assert!(overlaid.validate().is_ok());
    }

    #[test]
    fn test_try_with_properties_defers_configuration_validation() {
        let config = Config::default();
        let overlay = properties(&[
            ("writer.batch-size", "32MiB"),
            ("writer.request-max-size", "16MiB"),
        ]);

        let overlaid = config.try_with_properties(&overlay).unwrap();

        assert!(overlaid.validate().is_err());
        assert_eq!(config.writer_batch_size, DEFAULT_WRITER_BATCH_SIZE);
        assert_eq!(config.writer_request_max_size, DEFAULT_REQUEST_MAX_SIZE);
    }

    #[test]
    fn test_try_with_properties_allows_staged_security_configuration() {
        let overlay = properties(&[("security.protocol", "sasl")]);

        let mut config = Config::default().try_with_properties(&overlay).unwrap();

        assert!(config.validate().is_err());
        config.security_sasl_username = "admin".to_string();
        config.security_sasl_password = "secret".to_string();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_try_with_properties_rejects_invalid_value_atomically() {
        let config = Config::default();
        let overlay = properties(&[
            ("writer.batch-size", "4MiB"),
            ("writer.batch-timeout", "later"),
        ]);

        let error = config.try_with_properties(&overlay).unwrap_err();

        assert!(error.contains("writer.batch-timeout"));
        assert_eq!(config.writer_batch_size, DEFAULT_WRITER_BATCH_SIZE);
        assert_eq!(
            config.writer_batch_timeout_ms,
            DEFAULT_WRITER_BATCH_TIMEOUT_MS
        );
    }

    #[test]
    fn test_try_with_properties_rejects_unknown_key_without_aliases() {
        let config = Config::default();
        let overlay = properties(&[("writer.batch-timeout-ms", "100")]);

        let error = config.try_with_properties(&overlay).unwrap_err();

        assert_eq!(error, "unknown config property 'writer.batch-timeout-ms'");
    }

    #[test]
    fn test_try_with_properties_rejects_overflow() {
        let config = Config::default();
        let overlay = properties(&[("writer.request-max-size", "3GiB")]);

        let error = config.try_with_properties(&overlay).unwrap_err();

        assert!(error.contains("writer.request-max-size"));
    }

    #[test]
    fn test_property_errors_do_not_expose_sensitive_values() {
        let config = Config::default();
        let password = "do-not-leak";
        let overlay = properties(&[
            ("security.sasl.password", password),
            ("writer.batch-timeout", "later"),
        ]);

        let error = config.try_with_properties(&overlay).unwrap_err();

        assert!(!error.contains(password));
        assert!(Config::is_sensitive_property("security.sasl.password"));
        assert!(!Config::is_sensitive_property("writer.batch-size"));
        assert!(Config::is_sensitive_property("unknown.property"));
    }

    #[test]
    fn test_config_property_keys_are_unique() {
        let keys = CONFIG_PROPERTY_SPECS
            .iter()
            .map(|spec| spec.key)
            .collect::<std::collections::BTreeSet<_>>();

        assert_eq!(keys.len(), CONFIG_PROPERTY_SPECS.len());
    }

    #[test]
    fn test_default_is_not_sasl() {
        let config = Config::default();
        assert!(!config.is_sasl_enabled());
        assert!(config.validate_security().is_ok());
    }

    #[test]
    fn test_sasl_enabled_valid() {
        let config = Config {
            security_protocol: "sasl".to_string(),
            security_sasl_mechanism: "PLAIN".to_string(),
            security_sasl_username: "admin".to_string(),
            security_sasl_password: "secret".to_string(),
            ..Config::default()
        };
        assert!(config.is_sasl_enabled());
        assert!(config.validate_security().is_ok());
    }

    #[test]
    fn test_sasl_enabled_case_insensitive() {
        let config = Config {
            security_protocol: "SASL".to_string(),
            security_sasl_username: "admin".to_string(),
            security_sasl_password: "secret".to_string(),
            ..Config::default()
        };
        assert!(config.is_sasl_enabled());
        assert!(config.validate_security().is_ok());
    }

    #[test]
    fn test_sasl_missing_username() {
        let config = Config {
            security_protocol: "sasl".to_string(),
            security_sasl_password: "secret".to_string(),
            ..Config::default()
        };
        assert!(config.validate_security().is_err());
    }

    #[test]
    fn test_sasl_missing_password() {
        let config = Config {
            security_protocol: "sasl".to_string(),
            security_sasl_username: "admin".to_string(),
            ..Config::default()
        };
        assert!(config.validate_security().is_err());
    }

    #[test]
    fn test_sasl_unsupported_mechanism() {
        let config = Config {
            security_protocol: "sasl".to_string(),
            security_sasl_mechanism: "SCRAM-SHA-256".to_string(),
            security_sasl_username: "admin".to_string(),
            security_sasl_password: "secret".to_string(),
            ..Config::default()
        };
        assert!(config.validate_security().is_err());
    }

    #[test]
    fn test_scanner_defaults_valid() {
        let config = Config::default();
        assert!(config.validate_scanner().is_ok());
    }

    #[test]
    fn test_scanner_remote_log_prefetch_num_zero() {
        let config = Config {
            scanner_remote_log_prefetch_num: 0,
            ..Config::default()
        };
        assert!(config.validate_scanner().is_err());
    }

    #[test]
    fn test_scanner_remote_log_read_concurrency_zero() {
        let config = Config {
            scanner_remote_log_read_concurrency: 0,
            ..Config::default()
        };
        assert!(config.validate_scanner().is_err());
    }

    #[test]
    fn test_remote_file_download_thread_num_zero() {
        let config = Config {
            remote_file_download_thread_num: 0,
            ..Config::default()
        };
        assert!(config.validate_scanner().is_err());
    }

    #[test]
    fn test_scanner_fetch_invalid_ranges() {
        let config = Config {
            scanner_log_fetch_min_bytes: 2,
            scanner_log_fetch_max_bytes: 1,
            ..Config::default()
        };
        assert!(config.validate_scanner().is_err());
    }

    #[test]
    fn test_scanner_fetch_negative_wait() {
        let config = Config {
            scanner_log_fetch_wait_max_time_ms: -1,
            ..Config::default()
        };
        assert!(config.validate_scanner().is_err());
    }

    #[test]
    fn test_lookup_defaults_valid() {
        let config = Config::default();
        assert!(config.validate_lookup().is_ok());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_lookup_invalid_values() {
        let invalid_configs = [
            Config {
                lookup_queue_size: 0,
                ..Config::default()
            },
            Config {
                lookup_max_batch_size: 0,
                ..Config::default()
            },
            Config {
                lookup_max_inflight_requests: 0,
                ..Config::default()
            },
            Config {
                lookup_max_retries: -1,
                ..Config::default()
            },
        ];

        for config in invalid_configs {
            assert!(config.validate_lookup().is_err());
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn test_writer_defaults_valid() {
        let config = Config::default();
        assert!(config.validate_writer().is_ok());
    }

    #[test]
    fn test_writer_request_max_size_zero() {
        let config = Config {
            writer_request_max_size: 0,
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }

    #[test]
    fn test_writer_batch_size_zero() {
        let config = Config {
            writer_batch_size: 0,
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }

    #[test]
    fn test_writer_batch_timeout_negative() {
        let config = Config {
            writer_batch_timeout_ms: -1,
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }

    #[test]
    fn test_writer_max_inflight_requests_per_bucket_zero() {
        let config = Config {
            writer_max_inflight_requests_per_bucket: 0,
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }

    #[test]
    fn test_writer_buffer_memory_size_zero() {
        let config = Config {
            writer_buffer_memory_size: 0,
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }

    #[test]
    fn test_writer_batch_size_exceeds_request_max_size() {
        let config = Config {
            writer_batch_size: 20 * 1024 * 1024,
            writer_request_max_size: 10 * 1024 * 1024,
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }

    #[test]
    fn test_writer_batch_size_exceeds_buffer_memory_size() {
        let config = Config {
            writer_batch_size: 128 * 1024 * 1024,
            writer_buffer_memory_size: 64 * 1024 * 1024,
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }

    #[test]
    fn test_idempotence_disabled_skips_validation() {
        let config = Config {
            writer_enable_idempotence: false,
            writer_acks: "0".to_string(),
            writer_retries: 0,
            writer_max_inflight_requests_per_bucket: 100,
            ..Config::default()
        };
        assert!(config.validate_writer().is_ok());
    }

    #[test]
    fn test_idempotence_requires_acks_all() {
        let config = Config {
            writer_enable_idempotence: true,
            writer_acks: "1".to_string(),
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }

    #[test]
    fn test_idempotence_requires_retries() {
        let config = Config {
            writer_enable_idempotence: true,
            writer_retries: 0,
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }

    #[test]
    fn test_idempotence_requires_bounded_inflight() {
        let config = Config {
            writer_enable_idempotence: true,
            writer_max_inflight_requests_per_bucket: 10,
            ..Config::default()
        };
        assert!(config.validate_writer().is_err());
    }
}
