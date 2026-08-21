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

//! Gateway configuration loaded with precedence CLI > environment > YAML file > defaults.
//!
//! YAML uses the flat dotted keys documented by FIP-49:
//!
//! ```yaml
//! gateway.rest.listen: 0.0.0.0:8080
//! gateway.rest.write.max-request-bytes: 32MiB
//! ```
//!
//! Environment variable names are derived from these public keys; for example,
//! `gateway.rest.listen` becomes `FLUSS_GATEWAY__REST__LISTEN`.

use serde::Deserialize;
use serde::de::{self, Deserializer};
use serde_yaml_ng::{Mapping, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

/// Environment variable prefix for overrides.
pub const ENV_PREFIX: &str = "FLUSS_GATEWAY__";

/// A duration written as `<integer><ms|s|m|h>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigDuration(Duration);

/// Maximum configured duration, bounded to keep deadline arithmetic safe.
pub const MAX_CONFIG_DURATION: Duration = Duration::from_secs(365 * 24 * 60 * 60);

impl ConfigDuration {
    /// Builds a duration directly, bypassing the string syntax used by configuration sources.
    pub const fn from_secs(secs: u64) -> Self {
        Self(Duration::from_secs(secs))
    }

    /// Builds a sub-second duration without going through the string syntax.
    pub const fn from_millis(millis: u64) -> Self {
        Self(Duration::from_millis(millis))
    }

    /// Hands out the value for use with timers and deadlines.
    pub fn get(self) -> Duration {
        self.0
    }

    /// Parses the strict integer-plus-unit syntax and rejects a zero or out-of-range result.
    pub(crate) fn parse(s: &str) -> Result<Self, String> {
        let (digits, unit) = split_number_and_unit(s);
        if digits.is_empty() {
            return Err(format!(
                "invalid duration {s:?}: expected <integer><ms|s|m|h>"
            ));
        }
        let value: u64 = digits
            .parse()
            .map_err(|e| format!("invalid duration {s:?}: {e}"))?;
        let too_large = || {
            format!(
                "invalid duration {s:?}: must not exceed {} seconds",
                MAX_CONFIG_DURATION.as_secs()
            )
        };
        let duration = match unit {
            "ms" => Duration::from_millis(value),
            "s" => Duration::from_secs(value),
            "m" => Duration::from_secs(value.checked_mul(60).ok_or_else(too_large)?),
            "h" => Duration::from_secs(value.checked_mul(3600).ok_or_else(too_large)?),
            _ => {
                return Err(format!(
                    "invalid duration {s:?}: unit must be one of ms, s, m, h"
                ));
            }
        };
        if duration.is_zero() {
            return Err(format!("invalid duration {s:?}: must be greater than zero"));
        }
        if duration > MAX_CONFIG_DURATION {
            return Err(too_large());
        }
        Ok(Self(duration))
    }
}

impl<'de> Deserialize<'de> for ConfigDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(de::Error::custom)
    }
}

/// A positive byte size with an optional decimal or binary unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteSize(u64);

impl ByteSize {
    /// Builds a size directly, bypassing the syntax and non-zero rule applied to configuration sources.
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    /// Hands out the value for use in size comparisons and buffer budgets.
    pub fn bytes(self) -> u64 {
        self.0
    }

    /// Parses an integer size with an optional supported suffix and rejects a zero result.
    pub(crate) fn parse(s: &str) -> Result<Self, String> {
        let (digits, unit) = split_number_and_unit(s);
        if digits.is_empty() {
            return Err(format!("invalid byte size {s:?}: expected <integer>[unit]"));
        }
        let value: u64 = digits
            .parse()
            .map_err(|e| format!("invalid byte size {s:?}: {e}"))?;
        let multiplier: u64 = match unit {
            "" | "B" => 1,
            "KB" => 1000,
            "KiB" => 1024,
            "MB" => 1_000_000,
            "MiB" => 1024 * 1024,
            "GB" => 1_000_000_000,
            "GiB" => 1024 * 1024 * 1024,
            _ => {
                return Err(format!(
                    "invalid byte size {s:?}: unit must be one of B, KB, KiB, MB, MiB, GB, GiB"
                ));
            }
        };
        let bytes = value
            .checked_mul(multiplier)
            .ok_or_else(|| format!("invalid byte size {s:?}: overflows u64"))?;
        Self::checked(bytes).ok_or_else(|| format!("invalid byte size {s:?}: must be non-zero"))
    }

    fn checked(bytes: u64) -> Option<Self> {
        (bytes != 0).then_some(Self(bytes))
    }
}

fn split_number_and_unit(value: &str) -> (&str, &str) {
    let split = value
        .char_indices()
        .find(|(_, character)| !character.is_ascii_digit())
        .map_or(value.len(), |(index, _)| index);
    value.split_at(split)
}

impl<'de> Deserialize<'de> for ByteSize {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = ByteSize;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a positive integer or a string like \"4MiB\"")
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<ByteSize, E> {
                let bytes = u64::try_from(v)
                    .map_err(|_| E::custom(format!("byte size must be non-negative, got {v}")))?;
                self.visit_u64(bytes)
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<ByteSize, E> {
                ByteSize::checked(v).ok_or_else(|| E::custom("byte size must be non-zero"))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<ByteSize, E> {
                ByteSize::parse(v).map_err(E::custom)
            }
        }
        deserializer.deserialize_any(Visitor)
    }
}

const INSTANCE_ID_KEY: &str = "gateway.instance-id";
const REST_LISTEN_KEY: &str = "gateway.rest.listen";
const REST_HEADER_READ_TIMEOUT_KEY: &str = "gateway.rest.header-read-timeout";
const REST_REQUEST_TIMEOUT_KEY: &str = "gateway.rest.write.request-timeout";
const REST_MAX_REQUEST_BYTES_KEY: &str = "gateway.rest.write.max-request-bytes";
const METRICS_ENABLED_KEY: &str = "gateway.metrics.enabled";
const METRICS_LISTEN_KEY: &str = "gateway.metrics.exporter.prometheus.listen";
const SHUTDOWN_DRAIN_TIMEOUT_KEY: &str = "gateway.shutdown.drain-timeout";

const DEFAULT_REST_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
const DEFAULT_REST_HEADER_READ_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(10);
const DEFAULT_REST_REQUEST_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(30);
const DEFAULT_REST_MAX_REQUEST_BYTES: ByteSize = ByteSize::new(32 * 1024 * 1024);
const DEFAULT_METRICS_ENABLED: bool = true;
const DEFAULT_METRICS_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9095);
const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConfigEntry {
    key: &'static str,
    internal_path: &'static str,
}

const CONFIG_ENTRIES: &[ConfigEntry] = &[
    ConfigEntry {
        key: INSTANCE_ID_KEY,
        internal_path: "server.instance_id",
    },
    ConfigEntry {
        key: REST_LISTEN_KEY,
        internal_path: "server.rest.bind_address",
    },
    ConfigEntry {
        key: REST_HEADER_READ_TIMEOUT_KEY,
        internal_path: "server.rest.header_read_timeout",
    },
    ConfigEntry {
        key: REST_REQUEST_TIMEOUT_KEY,
        internal_path: "server.rest.request_timeout",
    },
    ConfigEntry {
        key: REST_MAX_REQUEST_BYTES_KEY,
        internal_path: "server.rest.max_body_bytes",
    },
    ConfigEntry {
        key: METRICS_ENABLED_KEY,
        internal_path: "server.metrics.enabled",
    },
    ConfigEntry {
        key: METRICS_LISTEN_KEY,
        internal_path: "server.metrics.bind_address",
    },
    ConfigEntry {
        key: SHUTDOWN_DRAIN_TIMEOUT_KEY,
        internal_path: "shutdown.drain_timeout",
    },
];

/// Gateway listeners and instance identity.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Optional operator-chosen identity used in logs and diagnostics only.
    ///
    /// Nothing in the gateway depends on it: the process is stateless, so no response, token, or handle is ever
    /// scoped to an instance. It is never required.
    pub instance_id: Option<String>,
    pub rest: RestServerConfig,
    pub metrics: MetricsServerConfig,
}

/// REST listener and request limits.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RestServerConfig {
    /// Loopback by default because the gateway has no transport security.
    pub bind_address: SocketAddr,
    /// Closes a connection whose request head is not complete within this budget, counted from
    /// connection establishment; the per-request deadline cannot defend here, as it runs only after
    /// a complete head.
    pub header_read_timeout: ConfigDuration,
    /// Per-request server-side deadline. Exceeding it yields 504.
    pub request_timeout: ConfigDuration,
    /// Maximum accepted request body size. Exceeding it yields 413.
    pub max_body_bytes: ByteSize,
}

impl Default for RestServerConfig {
    fn default() -> Self {
        Self {
            bind_address: DEFAULT_REST_LISTEN,
            header_read_timeout: DEFAULT_REST_HEADER_READ_TIMEOUT,
            request_timeout: DEFAULT_REST_REQUEST_TIMEOUT,
            max_body_bytes: DEFAULT_REST_MAX_REQUEST_BYTES,
        }
    }
}

impl RestServerConfig {
    fn validate(&self, problems: &mut Vec<String>) {
        validate_duration(
            REST_HEADER_READ_TIMEOUT_KEY,
            self.header_read_timeout.get(),
            problems,
        );
        validate_duration(
            REST_REQUEST_TIMEOUT_KEY,
            self.request_timeout.get(),
            problems,
        );
        if self.max_body_bytes.bytes() == 0 {
            problems.push(format!(
                "{REST_MAX_REQUEST_BYTES_KEY} must be greater than zero"
            ));
        }
    }
}

/// Prometheus listener configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsServerConfig {
    pub enabled: bool,
    pub bind_address: SocketAddr,
}

impl Default for MetricsServerConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_METRICS_ENABLED,
            bind_address: DEFAULT_METRICS_LISTEN,
        }
    }
}

/// Graceful-shutdown configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ShutdownConfig {
    /// Budget for the whole shutdown, not for connection draining alone: the drain runs inside it
    /// and leaves a tail for the cleanup that follows.
    pub drain_timeout: ConfigDuration,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            drain_timeout: DEFAULT_SHUTDOWN_DRAIN_TIMEOUT,
        }
    }
}

impl ShutdownConfig {
    fn validate(&self, problems: &mut Vec<String>) {
        validate_duration(
            SHUTDOWN_DRAIN_TIMEOUT_KEY,
            self.drain_timeout.get(),
            problems,
        );
    }
}

/// The validated gateway configuration: everything the process needs before it binds a listener.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct GatewayConfig {
    pub server: ServerConfig,
    pub shutdown: ShutdownConfig,
}

impl GatewayConfig {
    /// Checks invariants, including values supplied programmatically.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut problems = Vec::new();
        self.server.rest.validate(&mut problems);
        self.shutdown.validate(&mut problems);
        self.validate_identity(&mut problems);
        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(problems))
        }
    }

    /// Rejects an unusable instance identity or a port clash between the two listeners.
    ///
    /// A non-loopback listener does **not** require an instance ID. Nothing the gateway returns is scoped to an
    /// instance, so there is no identity to pin.
    fn validate_identity(&self, problems: &mut Vec<String>) {
        let server = &self.server;
        let rest_address = server.rest.bind_address;
        if let Some(instance_id) = server.instance_id.as_deref() {
            let valid = !instance_id.is_empty()
                && instance_id.len() <= 128
                && instance_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
            if !valid {
                problems.push(format!(
                    "{} must be 1-128 ASCII letters, digits, dots, underscores, or hyphens",
                    INSTANCE_ID_KEY
                ));
            }
        }
        if server.metrics.enabled && addresses_overlap(rest_address, server.metrics.bind_address) {
            problems.push(format!(
                "{} ({}) must differ from {} ({})",
                METRICS_LISTEN_KEY, server.metrics.bind_address, REST_LISTEN_KEY, rest_address
            ));
        }
    }

    /// Returns non-fatal configuration advisories that should be logged at startup.
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if !self.server.rest.bind_address.ip().is_loopback() {
            warnings.push(format!(
                "{} {} is not loopback. The REST listener accepts \
                 unauthenticated requests and has no TLS",
                REST_LISTEN_KEY, self.server.rest.bind_address
            ));
        }
        warnings
    }
}

/// True when two listeners cannot both bind: the addresses are equal, or either is a wildcard
/// (`0.0.0.0`, `::`) claiming the port for its family — `::` for both families, being dual-stack.
/// Port 0 asks the OS for a free port and never collides.
fn addresses_overlap(rest: SocketAddr, metrics: SocketAddr) -> bool {
    if rest.port() == 0 || metrics.port() == 0 || rest.port() != metrics.port() {
        return false;
    }
    match (rest.ip(), metrics.ip()) {
        (IpAddr::V4(a), IpAddr::V4(b)) => a == b || a.is_unspecified() || b.is_unspecified(),
        (IpAddr::V6(a), IpAddr::V6(b)) => a == b || a.is_unspecified() || b.is_unspecified(),
        (IpAddr::V6(a), IpAddr::V4(_)) | (IpAddr::V4(_), IpAddr::V6(a)) => a.is_unspecified(),
    }
}

fn validate_duration(key: &str, duration: Duration, problems: &mut Vec<String>) {
    if duration.is_zero() {
        problems.push(format!("{key} must be greater than zero"));
    } else if duration > MAX_CONFIG_DURATION {
        problems.push(format!(
            "{} must not exceed {} seconds",
            key,
            MAX_CONFIG_DURATION.as_secs()
        ));
    }
}

/// Targeted CLI overrides (highest precedence).
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    /// Overrides `gateway.rest.listen`.
    pub bind_address: Option<String>,
}

/// Configuration loading/validation failure.
#[derive(Debug)]
pub enum ConfigError {
    /// The config file could not be read.
    Io(String),
    /// The config file or an override value could not be parsed.
    Parse(String),
    /// A `FLUSS_GATEWAY__*` variable does not name a known section/key.
    UnknownEnvKey(String),
    /// One or more invariants failed validation.
    Invalid(Vec<String>),
}

impl fmt::Display for ConfigError {
    /// Renders a concise operator-facing configuration error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(msg) => write!(f, "cannot read configuration: {msg}"),
            ConfigError::Parse(msg) => write!(f, "invalid configuration: {msg}"),
            ConfigError::UnknownEnvKey(key) => {
                write!(f, "unknown configuration environment variable: {key}")
            }
            ConfigError::Invalid(problems) => {
                write!(f, "invalid configuration: {}", problems.join(", "))
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Writes `value` at a dotted path, creating mappings along the way and replacing whatever sat there before.
fn insert_path(table: &mut Mapping, path: &str, value: Value) {
    let mut current = table;
    let mut segments = path.split('.').peekable();
    while let Some(segment) = segments.next() {
        let key = Value::String(segment.to_string());
        if segments.peek().is_none() {
            current.insert(key, value);
            return;
        }
        let entry = current
            .entry(key)
            .or_insert_with(|| Value::Mapping(Mapping::new()));
        if !entry.is_mapping() {
            *entry = Value::Mapping(Mapping::new());
        }
        current = entry.as_mapping_mut().expect("mapping inserted above");
    }
}

/// Attributes a typed error to the override that supplied the failing option.
fn attribute(
    message: String,
    overrides: &[(&'static str, &'static str, String, Value)],
) -> ConfigError {
    for (_, key, origin, _) in overrides.iter().rev() {
        if message.starts_with(key) {
            return ConfigError::Parse(format!("{origin}: {message}"));
        }
    }
    ConfigError::Parse(message)
}

/// Deserializes the merged YAML value while retaining the nested field path in any error.
fn deserialize_config(value: Value) -> Result<GatewayConfig, ConfigError> {
    serde_path_to_error::deserialize(value)
        .map_err(|error| ConfigError::Parse(publicize_error_path(error.to_string())))
}

/// Rewrites Serde's internal typed path to the stable public option name used by operators.
fn publicize_error_path(message: String) -> String {
    for entry in CONFIG_ENTRIES {
        if let Some(reason) = message.strip_prefix(entry.internal_path) {
            return format!("{}{reason}", entry.key);
        }
    }
    message
}

fn config_entry(key: &str) -> Option<&'static ConfigEntry> {
    CONFIG_ENTRIES.iter().find(|entry| entry.key == key)
}

fn environment_variable(key: &str) -> String {
    let suffix = key
        .strip_prefix("gateway.")
        .expect("configuration keys use the gateway prefix")
        .split('.')
        .map(|segment| segment.replace('-', "_").to_ascii_uppercase())
        .collect::<Vec<_>>()
        .join("__");
    format!("{ENV_PREFIX}{suffix}")
}

fn environment_entry(variable: &str) -> Option<&'static ConfigEntry> {
    CONFIG_ENTRIES
        .iter()
        .find(|entry| environment_variable(entry.key) == variable)
}

fn convert_environment_value(entry: &ConfigEntry, raw: &str) -> Result<Value, String> {
    match entry.key {
        METRICS_ENABLED_KEY => raw
            .parse::<bool>()
            .map(Value::Bool)
            .map_err(|_| "expected true or false".to_string()),
        _ => Ok(Value::String(raw.to_string())),
    }
}

fn convert_file_value(entry: &ConfigEntry, value: &Value) -> Result<Value, String> {
    match entry.key {
        METRICS_ENABLED_KEY => scalar(value).cloned(),
        REST_MAX_REQUEST_BYTES_KEY => match scalar(value)? {
            Value::Number(_) | Value::String(_) => Ok(value.clone()),
            _ => Err("expected an integer or byte-size string".to_string()),
        },
        _ => scalar_text(value).map(Value::String),
    }
}

fn scalar(value: &Value) -> Result<&Value, String> {
    match value {
        Value::Bool(_) | Value::Number(_) | Value::String(_) => Ok(value),
        Value::Null => Err("value is missing".to_string()),
        Value::Sequence(_) | Value::Mapping(_) | Value::Tagged(_) => {
            Err("expected a scalar value".to_string())
        }
    }
}

fn scalar_text(value: &Value) -> Result<String, String> {
    match scalar(value)? {
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => Ok(value.clone()),
        _ => unreachable!("scalar rejects compound values"),
    }
}

/// Parses the flat-key YAML file into the nested mapping deserialized by [`GatewayConfig`].
fn read_config_file(contents: &str) -> Result<Mapping, ConfigError> {
    let document: Value =
        serde_yaml_ng::from_str(contents).map_err(|e| ConfigError::Parse(e.to_string()))?;
    let mut table = Mapping::new();
    if document.is_null() {
        return Ok(table);
    }
    let mapping = document.as_mapping().ok_or_else(|| {
        ConfigError::Parse(
            "configuration must be a mapping of flat dotted keys (gateway.…: value)".to_string(),
        )
    })?;

    for (key, value) in mapping {
        let key = key
            .as_str()
            .ok_or_else(|| ConfigError::Parse("configuration keys must be strings".to_string()))?;
        let entry = config_entry(key)
            .ok_or_else(|| ConfigError::Parse(format!("unknown configuration key: {key}")))?;
        let value = convert_file_value(entry, value)
            .map_err(|reason| ConfigError::Parse(format!("{key}: {reason}")))?;
        insert_path(&mut table, entry.internal_path, value);
    }
    Ok(table)
}

/// Loads configuration from all sources with precedence CLI > env > file > defaults.
///
/// `env` is explicit so loading remains deterministic and testable.
pub fn load(
    path: Option<&Path>,
    env: &BTreeMap<String, String>,
    cli: &CliOverrides,
) -> Result<GatewayConfig, ConfigError> {
    let mut table = Mapping::new();
    if let Some(path) = path {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(format!("{}: {e}", path.display())))?;
        table = read_config_file(&contents)?;
    }

    // Each override is kept with the source that wrote it, so a failure names what the operator wrote.
    let mut overrides: Vec<(&'static str, &'static str, String, Value)> = Vec::new();
    for (key, raw) in env {
        if !key.starts_with(ENV_PREFIX) {
            continue;
        }
        let entry =
            environment_entry(key).ok_or_else(|| ConfigError::UnknownEnvKey(key.clone()))?;
        overrides.push((
            entry.internal_path,
            entry.key,
            key.clone(),
            convert_environment_value(entry, raw)
                .map_err(|reason| ConfigError::Parse(format!("{key}: {}: {reason}", entry.key)))?,
        ));
    }

    if let Some(value) = &cli.bind_address {
        let entry = config_entry(REST_LISTEN_KEY).expect("REST listen option is registered");
        overrides.push((
            entry.internal_path,
            entry.key,
            "--bind-address".to_string(),
            Value::String(value.clone()),
        ));
    }

    for (path, _, _, value) in &overrides {
        insert_path(&mut table, path, value.clone());
    }

    let config = deserialize_config(Value::Mapping(table)).map_err(|error| {
        let ConfigError::Parse(message) = error else {
            unreachable!("deserialization only creates parse errors")
        };
        attribute(message, &overrides)
    })?;

    config.validate()?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn no_env() -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    fn write_temp_config(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("temp file");
        file.write_all(contents.as_bytes()).expect("write");
        file
    }

    fn load_file(contents: &str) -> Result<GatewayConfig, ConfigError> {
        let file = write_temp_config(contents);
        load(Some(file.path()), &no_env(), &CliOverrides::default())
    }

    fn problems(error: ConfigError) -> Vec<String> {
        match error {
            ConfigError::Invalid(problems) => problems,
            other => panic!("expected Invalid, got: {other:?}"),
        }
    }

    #[test]
    fn defaults_when_no_sources() {
        let config = load(None, &no_env(), &CliOverrides::default()).unwrap();
        assert_eq!(
            config.server.rest.bind_address,
            "127.0.0.1:8080".parse().unwrap()
        );
        assert_eq!(config.server.rest.max_body_bytes.bytes(), 32 * 1024 * 1024);
        assert_eq!(
            config.server.rest.request_timeout.get(),
            Duration::from_secs(30)
        );
        assert!(config.server.metrics.enabled);
        assert_eq!(
            config.server.metrics.bind_address,
            "127.0.0.1:9095".parse().unwrap()
        );
        assert_eq!(config.shutdown.drain_timeout.get(), Duration::from_secs(30));
        assert!(config.warnings().is_empty());
    }

    #[test]
    fn public_yaml_options_are_loaded() {
        let config = load_file(
            r#"
    gateway.instance-id: gateway-1
    gateway.rest.listen: 0.0.0.0:8080
    gateway.rest.write.max-request-bytes: 32MiB
    gateway.rest.write.request-timeout: 30s
    gateway.metrics.enabled: true
    gateway.metrics.exporter.prometheus.listen: 0.0.0.0:9095
    gateway.shutdown.drain-timeout: 10s
    "#,
        )
        .unwrap();
        assert_eq!(config.server.instance_id.as_deref(), Some("gateway-1"));
        assert_eq!(
            config.server.rest.bind_address,
            "0.0.0.0:8080".parse().unwrap()
        );
        assert_eq!(config.server.rest.max_body_bytes.bytes(), 32 * 1024 * 1024);
        assert_eq!(
            config.server.rest.request_timeout.get(),
            Duration::from_secs(30)
        );
        assert!(config.server.metrics.enabled);
        assert_eq!(
            config.server.metrics.bind_address,
            "0.0.0.0:9095".parse().unwrap()
        );
        assert_eq!(config.shutdown.drain_timeout.get(), Duration::from_secs(10));
    }

    #[test]
    fn unknown_file_keys_name_the_original_key() {
        for contents in [
            "gateway.rest.listenn: 0.0.0.0:8080\n",
            "rest.listen: 0.0.0.0:8080\n",
            "gateway.rest.lookup.max-keyz: 5\n",
            "gateway.scan.cursor-ttl: 1m\n",
            "gateway.tls.cert: /etc/tls.pem\n",
        ] {
            let error = load_file(contents).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
            let key = contents.split(':').next().unwrap();
            assert!(error.to_string().contains(key), "{key}: {error}");
        }
    }

    #[test]
    fn source_precedence_is_cli_then_env_then_file_then_defaults() {
        let file = write_temp_config(
            r#"
    gateway.rest.listen: 127.0.0.1:18080
    gateway.metrics.enabled: true
    "#,
        );
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__REST__LISTEN".to_string(),
            "127.0.0.1:28080".to_string(),
        );
        env.insert(
            "FLUSS_GATEWAY__METRICS__ENABLED".to_string(),
            "false".to_string(),
        );
        env.insert("PATH".to_string(), "/usr/bin".to_string());

        let config = load(
            Some(file.path()),
            &env,
            &CliOverrides {
                bind_address: Some("127.0.0.1:38080".to_string()),
            },
        )
        .unwrap();
        assert_eq!(
            config.server.rest.bind_address,
            "127.0.0.1:38080".parse().unwrap()
        );
        assert!(!config.server.metrics.enabled);
    }

    #[test]
    fn missing_file_reported() {
        let error = load(
            Some(Path::new("/nonexistent/gateway.yaml")),
            &no_env(),
            &CliOverrides::default(),
        )
        .unwrap_err();
        assert!(matches!(error, ConfigError::Io(_)), "got: {error:?}");
    }

    #[test]
    fn malformed_file_reports_position() {
        let error = load_file("gateway.rest.listen: [1\n").unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("line"), "got: {error}");
    }

    #[test]
    fn duplicate_flat_key_rejected() {
        let error =
            load_file("gateway.rest.listen: 127.0.0.1:8080\ngateway.rest.listen: 127.0.0.1:8081\n")
                .unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(error.to_string().contains("duplicate"), "got: {error}");
    }

    #[test]
    fn unknown_environment_variables_are_rejected() {
        for key in [
            "FLUSS_GATEWAY__REST__LISTENN",
            "FLUSS_GATEWAY__QUERY__ENABLED",
            "FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS",
        ] {
            let mut env = no_env();
            env.insert(key.to_string(), "value".to_string());
            let error = load(None, &env, &CliOverrides::default()).unwrap_err();
            assert!(
                matches!(error, ConfigError::UnknownEnvKey(_)),
                "{key}: {error:?}"
            );
            assert!(error.to_string().contains(key), "{key}: {error}");
        }
    }

    #[test]
    fn file_error_under_a_section_with_an_env_override_names_the_file() {
        let file = write_temp_config("gateway.shutdown.drain-timeout: 0s\n");
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__REST__LISTEN".to_string(),
            "127.0.0.1:28080".to_string(),
        );
        let error = load(Some(file.path()), &env, &CliOverrides::default()).unwrap_err();
        assert!(matches!(error, ConfigError::Parse(_)), "got: {error:?}");
        assert!(
            error.to_string().contains("gateway.shutdown.drain-timeout"),
            "got: {error}"
        );
        assert!(
            !error.to_string().contains("FLUSS_GATEWAY__"),
            "file problem misattributed to the env override: {error}"
        );
    }

    #[test]
    fn public_environment_options_are_loaded_by_type() {
        let env = BTreeMap::from([
            ("FLUSS_GATEWAY__INSTANCE_ID".to_string(), "123".to_string()),
            (
                "FLUSS_GATEWAY__REST__LISTEN".to_string(),
                "127.0.0.1:18080".to_string(),
            ),
            (
                "FLUSS_GATEWAY__REST__WRITE__REQUEST_TIMEOUT".to_string(),
                "5s".to_string(),
            ),
            (
                "FLUSS_GATEWAY__REST__WRITE__MAX_REQUEST_BYTES".to_string(),
                "2MiB".to_string(),
            ),
            (
                "FLUSS_GATEWAY__METRICS__ENABLED".to_string(),
                "false".to_string(),
            ),
            (
                "FLUSS_GATEWAY__METRICS__EXPORTER__PROMETHEUS__LISTEN".to_string(),
                "127.0.0.1:19095".to_string(),
            ),
            (
                "FLUSS_GATEWAY__SHUTDOWN__DRAIN_TIMEOUT".to_string(),
                "10s".to_string(),
            ),
        ]);

        let config = load(None, &env, &CliOverrides::default()).unwrap();
        assert_eq!(config.server.instance_id.as_deref(), Some("123"));
        assert_eq!(
            config.server.rest.bind_address,
            "127.0.0.1:18080".parse().unwrap()
        );
        assert_eq!(
            config.server.rest.request_timeout.get(),
            Duration::from_secs(5)
        );
        assert_eq!(config.server.rest.max_body_bytes.bytes(), 2 * 1024 * 1024);
        assert!(!config.server.metrics.enabled);
        assert_eq!(
            config.server.metrics.bind_address,
            "127.0.0.1:19095".parse().unwrap()
        );
        assert_eq!(config.shutdown.drain_timeout.get(), Duration::from_secs(10));
    }

    #[test]
    fn invalid_env_value_names_the_variable() {
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__REST__WRITE__MAX_REQUEST_BYTES".to_string(),
            "many".to_string(),
        );
        let error = load(None, &env, &CliOverrides::default()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("FLUSS_GATEWAY__REST__WRITE__MAX_REQUEST_BYTES"),
            "got: {error}"
        );
    }

    #[test]
    fn invalid_cli_value_names_the_flag() {
        let cli = CliOverrides {
            bind_address: Some("not-an-address".to_string()),
        };
        let error = load(None, &no_env(), &cli).unwrap_err();
        assert!(error.to_string().contains("--bind-address"), "got: {error}");
    }

    #[test]
    fn invalid_duration_rejected() {
        for bad in ["0ms", "60", "60 s", "6.5s", "s", "60d", "-1s"] {
            let error =
                load_file(&format!("gateway.shutdown.drain-timeout: \"{bad}\"\n")).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{bad}: {error:?}");
            assert!(
                error.to_string().contains("gateway.shutdown.drain-timeout"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn overflowing_duration_is_rejected_rather_than_saturated() {
        for bad in [
            "18446744073709551615ms",
            "18446744073709551615s",
            "18446744073709551615m",
            "18446744073709551615h",
        ] {
            let error = ConfigDuration::parse(bad).unwrap_err();
            assert!(error.contains("must not exceed"), "{bad}: {error}");
        }
        assert_eq!(
            ConfigDuration::parse("31536000s").unwrap().get(),
            MAX_CONFIG_DURATION
        );
        assert!(ConfigDuration::parse("31536001s").is_err());
    }

    #[test]
    fn programmatically_constructed_durations_are_validated() {
        let mut config = GatewayConfig::default();
        config.server.rest.request_timeout = ConfigDuration::from_millis(0);
        config.shutdown.drain_timeout =
            ConfigDuration::from_secs(MAX_CONFIG_DURATION.as_secs() + 1);

        let errors = problems(config.validate().unwrap_err());
        assert!(
            errors.iter().any(|error| {
                error == "gateway.rest.write.request-timeout must be greater than zero"
            }),
            "got: {errors:?}"
        );
        assert!(
            errors.iter().any(|error| {
                error == "gateway.shutdown.drain-timeout must not exceed 31536000 seconds"
            }),
            "got: {errors:?}"
        );
    }

    #[test]
    fn programmatically_constructed_zero_byte_limit_is_validated() {
        let mut config = GatewayConfig::default();
        config.server.rest.max_body_bytes = ByteSize::new(0);

        let errors = problems(config.validate().unwrap_err());
        assert_eq!(
            errors,
            vec!["gateway.rest.write.max-request-bytes must be greater than zero"]
        );
    }

    #[test]
    fn invalid_byte_size_rejected() {
        for bad in ["0", "\"4Mb\"", "\"MiB\"", "-1", "\"1.5MiB\""] {
            let error =
                load_file(&format!("gateway.rest.write.max-request-bytes: {bad}\n")).unwrap_err();
            assert!(matches!(error, ConfigError::Parse(_)), "{bad}: {error:?}");
            assert!(
                error
                    .to_string()
                    .contains("gateway.rest.write.max-request-bytes"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn metrics_address_must_differ_from_rest_address() {
        let error = load_file(
            "gateway.rest.listen: 127.0.0.1:9095\ngateway.metrics.exporter.prometheus.listen: 127.0.0.1:9095\n",
        )
        .unwrap_err();
        assert!(problems(error).iter().any(|problem| {
            problem.contains(
                "gateway.metrics.exporter.prometheus.listen (127.0.0.1:9095) must differ from \
                 gateway.rest.listen (127.0.0.1:9095)",
            )
        }));
    }

    /// Overlap detection covers the wildcard and dual-stack pairs that differ textually but cannot
    /// both bind, plus the pairs that coexist.
    #[test]
    fn listener_overlap_covers_wildcards_and_dual_stack() {
        let clashes = [
            ("0.0.0.0:8080", "127.0.0.1:8080"),
            ("127.0.0.1:8080", "0.0.0.0:8080"),
            ("0.0.0.0:8080", "0.0.0.0:8080"),
            ("[::]:8080", "[::1]:8080"),
            ("[::]:8080", "0.0.0.0:8080"),
            ("127.0.0.1:8080", "[::]:8080"),
        ];
        for (rest, metrics) in clashes {
            let rest: SocketAddr = rest.parse().unwrap();
            let metrics: SocketAddr = metrics.parse().unwrap();
            assert!(
                addresses_overlap(rest, metrics),
                "{rest} and {metrics} cannot both bind"
            );
        }

        let coexist = [
            ("127.0.0.1:8080", "192.168.1.2:8080"),
            ("127.0.0.1:8080", "[::1]:8080"),
            ("0.0.0.0:8080", "127.0.0.1:9095"),
            ("0.0.0.0:0", "0.0.0.0:0"),
            ("127.0.0.1:0", "0.0.0.0:8080"),
        ];
        for (rest, metrics) in coexist {
            let rest: SocketAddr = rest.parse().unwrap();
            let metrics: SocketAddr = metrics.parse().unwrap();
            assert!(
                !addresses_overlap(rest, metrics),
                "{rest} and {metrics} can coexist"
            );
        }
    }

    /// Two ephemeral listeners are not a clash: the OS hands out a different port to each.
    #[test]
    fn both_listeners_may_ask_for_an_ephemeral_port() {
        let config = load_file(
            "gateway.rest.listen: 127.0.0.1:0\ngateway.metrics.exporter.prometheus.listen: 127.0.0.1:0\n",
        )
        .unwrap();
        assert_eq!(config.server.rest.bind_address.port(), 0);
    }

    #[test]
    fn non_loopback_bind_is_accepted_without_an_instance_id_but_warns() {
        let config = load_file("gateway.rest.listen: 0.0.0.0:8080\n").unwrap();
        assert!(config.server.instance_id.is_none());
        assert_eq!(config.warnings().len(), 1);
        assert!(config.warnings()[0].contains("not loopback"));
        assert!(
            config.warnings()[0].contains("accepts unauthenticated requests"),
            "{:?}",
            config.warnings()
        );
    }

    #[test]
    fn malformed_instance_id_rejected() {
        let error = load_file("gateway.instance-id: has space\n").unwrap_err();
        assert!(
            problems(error)
                .iter()
                .any(|problem| problem.contains("gateway.instance-id must be 1-128 ASCII"))
        );
    }

    #[test]
    fn duration_units() {
        assert_eq!(
            ConfigDuration::parse("250ms").unwrap().get(),
            Duration::from_millis(250)
        );
        assert_eq!(
            ConfigDuration::parse("15m").unwrap().get(),
            Duration::from_secs(900)
        );
        assert_eq!(
            ConfigDuration::parse("2h").unwrap().get(),
            Duration::from_secs(7200)
        );
        assert!(ConfigDuration::parse("0s").is_err());
    }

    #[test]
    fn byte_size_units() {
        assert_eq!(ByteSize::parse("512").unwrap().bytes(), 512);
        assert_eq!(ByteSize::parse("512B").unwrap().bytes(), 512);
        assert_eq!(ByteSize::parse("4KB").unwrap().bytes(), 4000);
        assert_eq!(ByteSize::parse("4KiB").unwrap().bytes(), 4096);
        assert_eq!(ByteSize::parse("1GiB").unwrap().bytes(), 1024 * 1024 * 1024);
        assert!(ByteSize::parse("4TB").is_err());
        assert!(ByteSize::parse("0").is_err());
    }

    #[test]
    fn options_are_complete_and_unambiguous() {
        let mut public_keys = std::collections::BTreeSet::new();
        let mut internal_paths = std::collections::BTreeSet::new();
        let mut environment_variables = std::collections::BTreeSet::new();

        for entry in CONFIG_ENTRIES {
            assert!(entry.key.starts_with("gateway."), "{entry:?}");
            assert!(
                public_keys.insert(entry.key),
                "duplicate key: {}",
                entry.key
            );
            assert!(
                internal_paths.insert(entry.internal_path),
                "duplicate path: {}",
                entry.internal_path
            );
            assert!(
                environment_variables.insert(environment_variable(entry.key)),
                "duplicate environment variable for {}",
                entry.key
            );
        }

        assert_eq!(CONFIG_ENTRIES.len(), 8);
    }
}
