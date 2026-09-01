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

//! Gateway configuration with precedence CLI > environment > YAML > defaults.
//!
//! `gateway.clusters` is authoritative, `gateway.cluster.<id>.client.*` is reserved but unsupported,
//! and credentials are redacted through [`Secret`].

use axum::http::uri::Authority;
use fluss::config::Config as NativeClientConfig;
use serde::Deserialize;
use serde::de::{self, Deserializer};
use serde_yaml_ng::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::time::Duration;

/// Environment variable prefix for overrides.
pub const ENV_PREFIX: &str = "FLUSS_GATEWAY__";

/// Matches the Java-side credential placeholder.
const REDACTED: &str = "******";

/// A credential redacted by [`Debug`].
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// A duration written as `<integer><ms|s|m|h|d>`, allowing whitespace around or before the unit.
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
                "invalid duration {s:?}: expected <integer><ms|s|m|h|d>"
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
        let duration = match unit.to_ascii_lowercase().as_str() {
            "ms" => Duration::from_millis(value),
            "s" => Duration::from_secs(value),
            "m" => Duration::from_secs(value.checked_mul(60).ok_or_else(too_large)?),
            "h" => Duration::from_secs(value.checked_mul(3600).ok_or_else(too_large)?),
            "d" => Duration::from_secs(value.checked_mul(86400).ok_or_else(too_large)?),
            _ => {
                return Err(format!(
                    "invalid duration {s:?}: unit must be one of ms, s, m, h, d"
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

/// A positive byte size with an optional binary unit using Fluss's 1024-based multipliers.
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
        let multiplier: u64 = match unit.to_ascii_lowercase().as_str() {
            "" | "b" | "bytes" => 1,
            "k" | "kb" | "kib" | "kibibyte" | "kibibytes" => 1024,
            "m" | "mb" | "mib" | "mebibyte" | "mebibytes" => 1024 * 1024,
            "g" | "gb" | "gib" | "gibibyte" | "gibibytes" => 1024 * 1024 * 1024,
            "t" | "tb" | "tib" | "tebibyte" | "tebibytes" => 1024_u64 * 1024 * 1024 * 1024,
            _ => {
                return Err(format!(
                    "invalid byte size {s:?}: unit must be one of B, KB/KiB, MB/MiB, GB/GiB, TB/TiB"
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
    let value = value.trim();
    let split = value
        .char_indices()
        .find(|(_, character)| !character.is_ascii_digit())
        .map_or(value.len(), |(index, _)| index);
    let (number, unit) = value.split_at(split);
    (number, unit.trim())
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
const REST_REQUEST_TIMEOUT_KEY: &str = "gateway.rest.request-timeout";
const REST_MAX_REQUEST_BYTES_KEY: &str = "gateway.rest.write.max-request-bytes";
const REST_METADATA_MAX_CONCURRENT_REQUESTS_KEY: &str =
    "gateway.rest.metadata.max-concurrent-requests";
const METRICS_ENABLED_KEY: &str = "gateway.metrics.enabled";
const METRICS_EXPORTERS_KEY: &str = "gateway.metrics.exporters";
const METRICS_LISTEN_KEY: &str = "gateway.metrics.exporter.prometheus.listen";
const SHUTDOWN_DRAIN_TIMEOUT_KEY: &str = "gateway.shutdown.drain-timeout";
const CLUSTERS_KEY: &str = "gateway.clusters";
const CLUSTER_KEY_PREFIX: &str = "gateway.cluster.";
const CLIENT_OPTION_PREFIX: &str = "client.";
const SECURITY_AUTHENTICATION_KEY: &str = "gateway.security.authentication";
const SECURITY_USERS_KEY: &str = "gateway.security.users";
const SECURITY_TOKENS_KEY: &str = "gateway.security.tokens";
const SECURITY_TRUSTED_HEADER_NAME_KEY: &str = "gateway.security.trusted-header.name";
const REST_WRITE_MAX_ROWS_KEY: &str = "gateway.rest.write.max-rows";
const REST_WRITE_MAX_CONCURRENT_REQUESTS_KEY: &str = "gateway.rest.write.max-concurrent-requests";
const REST_WRITE_RATE_LIMIT_ENABLED_KEY: &str = "gateway.rest.write.rate-limit.enabled";
const REST_WRITE_RATE_LIMIT_REQUESTS_PER_SECOND_KEY: &str =
    "gateway.rest.write.rate-limit.requests-per-second";
const REST_WRITE_RATE_LIMIT_BYTES_PER_SECOND_KEY: &str =
    "gateway.rest.write.rate-limit.bytes-per-second";
const REST_LOOKUP_MAX_KEYS_KEY: &str = "gateway.rest.lookup.max-keys";
const REST_LOOKUP_MAX_KEY_BYTES_KEY: &str = "gateway.rest.lookup.max-key-bytes";
const REST_LOOKUP_MAX_CONCURRENT_REQUESTS_KEY: &str = "gateway.rest.lookup.max-concurrent-requests";
const REST_PREFIX_LOOKUP_MAX_PREFIXES_KEY: &str = "gateway.rest.prefix-lookup.max-prefixes";
const REST_PREFIX_LOOKUP_MAX_ROWS_PER_PREFIX_KEY: &str =
    "gateway.rest.prefix-lookup.max-rows-per-prefix";
const REST_PREFIX_LOOKUP_MAX_CONCURRENT_REQUESTS_KEY: &str =
    "gateway.rest.prefix-lookup.max-concurrent-requests";

const DEFAULT_REST_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080);
const DEFAULT_REST_HEADER_READ_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(10);
const DEFAULT_REST_REQUEST_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(30);
const DEFAULT_REST_METADATA_MAX_CONCURRENT_REQUESTS: u32 = 16;

/// Time reserved for encoding and sending an HTTP response after the handler deadline.
pub(crate) const REST_RESPONSE_GRACE: Duration = Duration::from_secs(1);

/// Applies the response grace exactly as the REST deadline middleware does.
pub(crate) fn rest_handler_timeout(request_timeout: Duration) -> Duration {
    if request_timeout > REST_RESPONSE_GRACE {
        request_timeout - REST_RESPONSE_GRACE
    } else {
        request_timeout
    }
}

const DEFAULT_REST_MAX_REQUEST_BYTES: ByteSize = ByteSize::new(32 * 1024 * 1024);
const DEFAULT_METRICS_ENABLED: bool = true;
const DEFAULT_METRICS_LISTEN: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9095);
const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(30);

const DEFAULT_CLUSTER_ID: &str = "default";
const DEFAULT_BOOTSTRAP_SERVERS: &str = "127.0.0.1:9123";
const DEFAULT_CONNECTION_IDLE_TIMEOUT: ConfigDuration = ConfigDuration::from_secs(10 * 60);
const DEFAULT_TRUSTED_HEADER_NAME: &str = "x-forwarded-user";

const CLUSTER_BOOTSTRAP_SERVERS_KEY: &str = "bootstrap.servers";
const CLUSTER_CONNECT_TIMEOUT_KEY: &str = "connect-timeout";
const CLUSTER_CONNECTION_IDLE_TIMEOUT_KEY: &str = "connection.idle-timeout";
const CLUSTER_SECURITY_PROTOCOL_KEY: &str = "connection.security.protocol";
const CLUSTER_SERVICE_ACCOUNT_KEY: &str = "connection.service.account";
const CLUSTER_SERVICE_SECRET_KEY: &str = "connection.service.secret";
const CLUSTER_IDENTITY_MODE_KEY: &str = "connection.identity-mode";

type ApplyConfigValue<C> = fn(&mut C, &Value) -> Result<(), String>;

#[derive(Debug, Clone, Copy)]
struct ConfigEntry<C> {
    key: &'static str,
    apply: ApplyConfigValue<C>,
}

type GatewayConfigEntry = ConfigEntry<GatewayConfig>;
type ClusterConfigEntry = ConfigEntry<ClusterConfig>;

trait FromConfigValue: Sized {
    fn from_config_value(value: &Value) -> Result<Self, String>;
}

fn parse_config_value<T: FromConfigValue>(value: &Value) -> Result<T, String> {
    T::from_config_value(value)
}

impl FromConfigValue for String {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        scalar_text(value)
    }
}

impl FromConfigValue for bool {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        match scalar(value)? {
            Value::Bool(value) => Ok(*value),
            Value::String(value) => value
                .parse()
                .map_err(|_| "expected true or false".to_string()),
            _ => Err("expected true or false".to_string()),
        }
    }
}

impl FromConfigValue for u32 {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        let value = match scalar(value)? {
            Value::Number(value) => value
                .as_u64()
                .ok_or_else(|| "expected a non-negative integer".to_string())?,
            Value::String(value) => value
                .parse::<u64>()
                .map_err(|_| "expected a non-negative integer".to_string())?,
            _ => return Err("expected a non-negative integer".to_string()),
        };
        Self::try_from(value).map_err(|_| format!("must not exceed {}", Self::MAX))
    }
}

impl FromConfigValue for SocketAddr {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        String::from_config_value(value)?
            .parse()
            .map_err(|error| format!("expected an IP socket address: {error}"))
    }
}

impl FromConfigValue for ConfigDuration {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        Self::parse(&String::from_config_value(value)?)
    }
}

impl FromConfigValue for ByteSize {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        Self::parse(&String::from_config_value(value)?)
    }
}

impl FromConfigValue for AuthenticationMode {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        match String::from_config_value(value)?.as_str() {
            "trust" => Ok(Self::Trust),
            "password" => Ok(Self::Password),
            "token" => Ok(Self::Token),
            "trusted-header" => Ok(Self::TrustedHeader),
            _ => Err("expected trust, password, token, or trusted-header".to_string()),
        }
    }
}

impl FromConfigValue for IdentityMode {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        match String::from_config_value(value)?.as_str() {
            "service" => Ok(Self::Service),
            "user" => Ok(Self::User),
            _ => Err("expected service or user".to_string()),
        }
    }
}

impl FromConfigValue for ConnectionSecurityProtocol {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        match String::from_config_value(value)?.as_str() {
            "plaintext" => Ok(Self::Plaintext),
            "sasl" => Ok(Self::Sasl),
            _ => Err("expected plaintext or sasl".to_string()),
        }
    }
}

impl FromConfigValue for Secret {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        String::from_config_value(value).map(Self::new)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum MetricsExporter {
    Prometheus,
}

impl FromConfigValue for MetricsExporter {
    fn from_config_value(value: &Value) -> Result<Self, String> {
        match String::from_config_value(value)?.trim() {
            "prometheus" => Ok(Self::Prometheus),
            _ => Err("expected prometheus".to_string()),
        }
    }
}

macro_rules! typed_entry {
    ($entry:ident, $key:expr, optional $($field:ident).+) => {
        $entry {
            key: $key,
            apply: |config, value| {
                config.$($field).+ = Some(parse_config_value(value)?);
                Ok(())
            },
        }
    };
    ($entry:ident, $key:expr, $($field:ident).+) => {
        $entry {
            key: $key,
            apply: |config, value| {
                config.$($field).+ = parse_config_value(value)?;
                Ok(())
            },
        }
    };
}

const CONFIG_ENTRIES: &[GatewayConfigEntry] = &[
    typed_entry!(GatewayConfigEntry, INSTANCE_ID_KEY, optional server.instance_id),
    typed_entry!(
        GatewayConfigEntry,
        REST_LISTEN_KEY,
        server.rest.bind_address
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_HEADER_READ_TIMEOUT_KEY,
        server.rest.header_read_timeout
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_REQUEST_TIMEOUT_KEY,
        server.rest.request_timeout
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_MAX_REQUEST_BYTES_KEY,
        server.rest.max_body_bytes
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_METADATA_MAX_CONCURRENT_REQUESTS_KEY,
        server.rest.metadata_max_concurrent_requests
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_WRITE_MAX_ROWS_KEY,
        request_limits.write_max_rows
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_WRITE_MAX_CONCURRENT_REQUESTS_KEY,
        request_limits.write_max_concurrent_requests
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_WRITE_RATE_LIMIT_ENABLED_KEY,
        request_limits.write_rate_limit_enabled
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_WRITE_RATE_LIMIT_REQUESTS_PER_SECOND_KEY,
        request_limits.write_rate_limit_requests_per_second
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_WRITE_RATE_LIMIT_BYTES_PER_SECOND_KEY,
        request_limits.write_rate_limit_bytes_per_second
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_LOOKUP_MAX_KEYS_KEY,
        request_limits.lookup_max_keys
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_LOOKUP_MAX_KEY_BYTES_KEY,
        request_limits.lookup_max_key_bytes
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_LOOKUP_MAX_CONCURRENT_REQUESTS_KEY,
        request_limits.lookup_max_concurrent_requests
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_PREFIX_LOOKUP_MAX_PREFIXES_KEY,
        request_limits.prefix_lookup_max_prefixes
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_PREFIX_LOOKUP_MAX_ROWS_PER_PREFIX_KEY,
        request_limits.prefix_lookup_max_rows_per_prefix
    ),
    typed_entry!(
        GatewayConfigEntry,
        REST_PREFIX_LOOKUP_MAX_CONCURRENT_REQUESTS_KEY,
        request_limits.prefix_lookup_max_concurrent_requests
    ),
    typed_entry!(
        GatewayConfigEntry,
        METRICS_ENABLED_KEY,
        server.metrics.enabled
    ),
    typed_entry!(
        GatewayConfigEntry,
        METRICS_EXPORTERS_KEY,
        server.metrics.exporter
    ),
    typed_entry!(
        GatewayConfigEntry,
        METRICS_LISTEN_KEY,
        server.metrics.bind_address
    ),
    typed_entry!(
        GatewayConfigEntry,
        SHUTDOWN_DRAIN_TIMEOUT_KEY,
        shutdown.drain_timeout
    ),
    typed_entry!(
        GatewayConfigEntry,
        SECURITY_AUTHENTICATION_KEY,
        security.authentication
    ),
    typed_entry!(GatewayConfigEntry, SECURITY_USERS_KEY, optional security.users),
    typed_entry!(GatewayConfigEntry, SECURITY_TOKENS_KEY, optional security.tokens),
    typed_entry!(GatewayConfigEntry, SECURITY_TRUSTED_HEADER_NAME_KEY, optional security.trusted_header_name),
];

// TODO: Expose `request-timeout` after fluss-rust independently bounds a complete bootstrap
// attempt (TCP connect, API version negotiation, SASL, and metadata), matching the Java client,
// and provides a general per-RPC timeout. `connect-timeout` must remain TCP-connect-only.
const CLUSTER_ENTRIES: &[ClusterConfigEntry] = &[
    typed_entry!(
        ClusterConfigEntry,
        CLUSTER_BOOTSTRAP_SERVERS_KEY,
        bootstrap_servers
    ),
    typed_entry!(
        ClusterConfigEntry,
        CLUSTER_CONNECT_TIMEOUT_KEY,
        connect_timeout
    ),
    typed_entry!(
        ClusterConfigEntry,
        CLUSTER_CONNECTION_IDLE_TIMEOUT_KEY,
        connection_idle_timeout
    ),
    typed_entry!(
        ClusterConfigEntry,
        CLUSTER_SECURITY_PROTOCOL_KEY,
        security_protocol
    ),
    typed_entry!(ClusterConfigEntry, CLUSTER_SERVICE_ACCOUNT_KEY, optional service_account),
    typed_entry!(ClusterConfigEntry, CLUSTER_SERVICE_SECRET_KEY, optional service_secret),
    typed_entry!(ClusterConfigEntry, CLUSTER_IDENTITY_MODE_KEY, identity_mode),
];

/// Gateway listeners and instance identity.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    /// Optional identity for logs and diagnostics.
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
    /// Deadline for receiving a complete request head.
    pub header_read_timeout: ConfigDuration,
    /// Per-request server-side deadline. Exceeding it yields 504.
    pub request_timeout: ConfigDuration,
    /// Maximum accepted request body size. Exceeding it yields 413.
    pub max_body_bytes: ByteSize,
    /// Maximum metadata requests concurrently accessing Fluss. Exceeding it yields 429.
    pub metadata_max_concurrent_requests: u32,
}

impl Default for RestServerConfig {
    fn default() -> Self {
        Self {
            bind_address: DEFAULT_REST_LISTEN,
            header_read_timeout: DEFAULT_REST_HEADER_READ_TIMEOUT,
            request_timeout: DEFAULT_REST_REQUEST_TIMEOUT,
            max_body_bytes: DEFAULT_REST_MAX_REQUEST_BYTES,
            metadata_max_concurrent_requests: DEFAULT_REST_METADATA_MAX_CONCURRENT_REQUESTS,
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
        if self.metadata_max_concurrent_requests == 0 {
            problems.push(format!(
                "{REST_METADATA_MAX_CONCURRENT_REQUESTS_KEY} must be greater than zero"
            ));
        }
    }
}

/// Prometheus listener configuration.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsServerConfig {
    pub enabled: bool,
    pub exporter: MetricsExporter,
    pub bind_address: SocketAddr,
}

impl Default for MetricsServerConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_METRICS_ENABLED,
            exporter: MetricsExporter::Prometheus,
            bind_address: DEFAULT_METRICS_LISTEN,
        }
    }
}

/// How the gateway derives the effective Fluss principal for one cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum IdentityMode {
    /// One shared connection, authenticated as the service account when the protocol supports it.
    #[default]
    Service,
    /// Authenticate as the service account and carry the request's principal as the authorization ID.
    User,
}

/// Transport authentication used by the native Fluss client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionSecurityProtocol {
    /// Connect without authenticating a Fluss user.
    #[default]
    Plaintext,
    /// Authenticate the configured service account with SASL/PLAIN.
    Sasl,
}

/// Connection settings for one Fluss cluster.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClusterConfig {
    /// Comma-separated bootstrap addresses passed to the native Fluss client.
    pub bootstrap_servers: String,
    pub connect_timeout: ConfigDuration,
    /// How long a connection can go without being acquired before the cleaner releases it.
    pub connection_idle_timeout: ConfigDuration,
    pub security_protocol: ConnectionSecurityProtocol,
    /// Account used to authenticate to Fluss.
    pub service_account: Option<String>,
    pub service_secret: Option<Secret>,
    pub identity_mode: IdentityMode,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: DEFAULT_BOOTSTRAP_SERVERS.to_string(),
            connect_timeout: ConfigDuration::from_secs(10),
            connection_idle_timeout: DEFAULT_CONNECTION_IDLE_TIMEOUT,
            security_protocol: ConnectionSecurityProtocol::Plaintext,
            service_account: None,
            service_secret: None,
            identity_mode: IdentityMode::Service,
        }
    }
}

impl ClusterConfig {
    pub fn service_account(&self) -> Option<&str> {
        self.service_account.as_deref()
    }

    pub fn service_secret(&self) -> Option<&str> {
        self.service_secret.as_ref().map(Secret::expose)
    }

    /// Builds native settings owned by the Gateway.
    pub fn native_client_config(&self) -> NativeClientConfig {
        let mut native = NativeClientConfig {
            bootstrap_servers: self.bootstrap_servers.clone(),
            connect_timeout_ms: u64::try_from(self.connect_timeout.get().as_millis())
                .expect("bounded configuration durations fit u64 milliseconds"),
            ..NativeClientConfig::default()
        };
        match self.security_protocol {
            ConnectionSecurityProtocol::Plaintext => {
                native.security_protocol = "PLAINTEXT".to_string();
            }
            ConnectionSecurityProtocol::Sasl => {
                native.security_protocol = "sasl".to_string();
                native.security_sasl_mechanism = "PLAIN".to_string();
                if let Some(account) = &self.service_account {
                    native.security_sasl_username = account.clone();
                }
                if let Some(secret) = &self.service_secret {
                    native.security_sasl_password = secret.expose().to_string();
                }
            }
        }
        native
    }
}

/// HTTP caller authentication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum AuthenticationMode {
    /// Every caller is accepted and reported as an anonymous principal.
    #[default]
    Trust,
    Password,
    Token,
    TrustedHeader,
}

/// Client-to-gateway authentication settings. Every credential-bearing field is a [`Secret`].
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct SecurityConfig {
    pub authentication: AuthenticationMode,
    /// Password-mode user table; the entries embed password material.
    pub users: Option<Secret>,
    /// Token-mode table; the entries are bearer tokens.
    pub tokens: Option<Secret>,
    /// Trusted-header-mode header name, defaulting to `x-forwarded-user`.
    pub trusted_header_name: Option<String>,
}

impl SecurityConfig {
    /// Returns the header the trusted-header mode reads the principal from.
    pub fn trusted_header_name(&self) -> &str {
        self.trusted_header_name
            .as_deref()
            .unwrap_or(DEFAULT_TRUSTED_HEADER_NAME)
    }

    fn parsed_user_count(&self) -> Result<usize, String> {
        parse_user_table(self.users.as_ref().map(Secret::expose).unwrap_or(""))
    }

    fn parsed_token_count(&self) -> Result<usize, String> {
        parse_token_table(self.tokens.as_ref().map(Secret::expose).unwrap_or(""))
    }
}

/// Admission limits for the data-plane APIs, whose handlers arrive in later tasks.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct RequestLimitsConfig {
    pub write_max_rows: u32,
    pub write_max_concurrent_requests: u32,
    pub write_rate_limit_enabled: bool,
    pub write_rate_limit_requests_per_second: u32,
    pub write_rate_limit_bytes_per_second: ByteSize,
    pub lookup_max_keys: u32,
    pub lookup_max_key_bytes: ByteSize,
    pub lookup_max_concurrent_requests: u32,
    pub prefix_lookup_max_prefixes: u32,
    pub prefix_lookup_max_rows_per_prefix: u32,
    pub prefix_lookup_max_concurrent_requests: u32,
}

impl Default for RequestLimitsConfig {
    fn default() -> Self {
        Self {
            write_max_rows: 10_000,
            write_max_concurrent_requests: 64,
            write_rate_limit_enabled: false,
            write_rate_limit_requests_per_second: 1000,
            write_rate_limit_bytes_per_second: ByteSize::new(64 * 1024 * 1024),
            lookup_max_keys: 128,
            lookup_max_key_bytes: ByteSize::new(1024 * 1024),
            lookup_max_concurrent_requests: 64,
            prefix_lookup_max_prefixes: 16,
            prefix_lookup_max_rows_per_prefix: 1000,
            prefix_lookup_max_concurrent_requests: 32,
        }
    }
}

impl RequestLimitsConfig {
    fn validate(&self, problems: &mut Vec<String>) {
        for (key, value) in [
            (REST_WRITE_MAX_ROWS_KEY, self.write_max_rows),
            (
                REST_WRITE_MAX_CONCURRENT_REQUESTS_KEY,
                self.write_max_concurrent_requests,
            ),
            (
                REST_WRITE_RATE_LIMIT_REQUESTS_PER_SECOND_KEY,
                self.write_rate_limit_requests_per_second,
            ),
            (REST_LOOKUP_MAX_KEYS_KEY, self.lookup_max_keys),
            (
                REST_LOOKUP_MAX_CONCURRENT_REQUESTS_KEY,
                self.lookup_max_concurrent_requests,
            ),
            (
                REST_PREFIX_LOOKUP_MAX_PREFIXES_KEY,
                self.prefix_lookup_max_prefixes,
            ),
            (
                REST_PREFIX_LOOKUP_MAX_ROWS_PER_PREFIX_KEY,
                self.prefix_lookup_max_rows_per_prefix,
            ),
            (
                REST_PREFIX_LOOKUP_MAX_CONCURRENT_REQUESTS_KEY,
                self.prefix_lookup_max_concurrent_requests,
            ),
        ] {
            if value == 0 {
                problems.push(format!("{key} must be greater than zero"));
            }
        }
        if self.lookup_max_key_bytes.bytes() == 0 {
            problems.push(format!(
                "{REST_LOOKUP_MAX_KEY_BYTES_KEY} must be greater than zero"
            ));
        }
        if self.write_rate_limit_bytes_per_second.bytes() == 0 {
            problems.push(format!(
                "{REST_WRITE_RATE_LIMIT_BYTES_PER_SECOND_KEY} must be greater than zero"
            ));
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
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct GatewayConfig {
    pub server: ServerConfig,
    /// Every declared Fluss cluster, keyed by the ID used in REST paths.
    pub clusters: BTreeMap<String, ClusterConfig>,
    pub security: SecurityConfig,
    pub request_limits: RequestLimitsConfig,
    pub shutdown: ShutdownConfig,
}

impl Default for GatewayConfig {
    /// Defaults to the single `default` cluster, so a local deployment needs no cluster list.
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            clusters: BTreeMap::from([(DEFAULT_CLUSTER_ID.to_string(), ClusterConfig::default())]),
            security: SecurityConfig::default(),
            request_limits: RequestLimitsConfig::default(),
            shutdown: ShutdownConfig::default(),
        }
    }
}

impl GatewayConfig {
    /// Checks invariants, including values supplied programmatically.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut problems = Vec::new();
        self.server.rest.validate(&mut problems);
        self.shutdown.validate(&mut problems);
        self.validate_identity(&mut problems);
        self.validate_clusters(&mut problems);
        self.validate_security(&mut problems);
        self.request_limits.validate(&mut problems);
        if problems.is_empty() {
            Ok(())
        } else {
            Err(ConfigError::Invalid(problems))
        }
    }

    fn validate_clusters(&self, problems: &mut Vec<String>) {
        if self.clusters.is_empty() {
            problems.push(format!("{CLUSTERS_KEY} must declare at least one cluster"));
        }
        let handler_timeout = rest_handler_timeout(self.server.rest.request_timeout.get());
        for (id, cluster) in &self.clusters {
            if !valid_cluster_id(id) {
                problems.push(format!(
                    "cluster ID {id:?} must be at most 63 characters, start with a lowercase letter, and contain only lowercase letters, digits, or underscores"
                ));
            }
            if let Err(problem) = validate_bootstrap_servers(&cluster.bootstrap_servers) {
                problems.push(format!(
                    "{} {problem}",
                    cluster_key(id, CLUSTER_BOOTSTRAP_SERVERS_KEY)
                ));
            }
            let connect_timeout = cluster.connect_timeout.get();
            validate_duration(
                &cluster_key(id, CLUSTER_CONNECT_TIMEOUT_KEY),
                connect_timeout,
                problems,
            );
            let idle_timeout = cluster.connection_idle_timeout.get();
            validate_duration(
                &cluster_key(id, CLUSTER_CONNECTION_IDLE_TIMEOUT_KEY),
                idle_timeout,
                problems,
            );
            if !idle_timeout.is_zero()
                && idle_timeout <= MAX_CONFIG_DURATION
                && !handler_timeout.is_zero()
                && handler_timeout <= MAX_CONFIG_DURATION
                && idle_timeout <= handler_timeout
            {
                problems.push(format!(
                    "{} must be greater than the REST handler timeout {handler_timeout:?}",
                    cluster_key(id, CLUSTER_CONNECTION_IDLE_TIMEOUT_KEY)
                ));
            }
            self.validate_connection_security(id, cluster, problems);
        }
    }

    fn validate_connection_security(
        &self,
        id: &str,
        cluster: &ClusterConfig,
        problems: &mut Vec<String>,
    ) {
        let account = cluster.service_account();
        let secret = cluster.service_secret();
        for (key, value) in [
            (CLUSTER_SERVICE_ACCOUNT_KEY, account),
            (CLUSTER_SERVICE_SECRET_KEY, secret),
        ] {
            if value.is_some_and(|value| value.trim().is_empty()) {
                problems.push(format!("{} must not be blank", cluster_key(id, key)));
            }
        }

        match cluster.security_protocol {
            ConnectionSecurityProtocol::Plaintext => {
                if account.is_some() || secret.is_some() {
                    problems.push(format!(
                        "{} and {} must be omitted when {} is plaintext",
                        cluster_key(id, CLUSTER_SERVICE_ACCOUNT_KEY),
                        cluster_key(id, CLUSTER_SERVICE_SECRET_KEY),
                        cluster_key(id, CLUSTER_SECURITY_PROTOCOL_KEY)
                    ));
                }
            }
            ConnectionSecurityProtocol::Sasl => {
                if account.is_none() && secret.is_none() {
                    problems.push(format!(
                        "{} and {} must be configured when {} is sasl",
                        cluster_key(id, CLUSTER_SERVICE_ACCOUNT_KEY),
                        cluster_key(id, CLUSTER_SERVICE_SECRET_KEY),
                        cluster_key(id, CLUSTER_SECURITY_PROTOCOL_KEY)
                    ));
                } else if account.is_some() != secret.is_some() {
                    problems.push(format!(
                        "{} and {} must be set together when {} is sasl",
                        cluster_key(id, CLUSTER_SERVICE_ACCOUNT_KEY),
                        cluster_key(id, CLUSTER_SERVICE_SECRET_KEY),
                        cluster_key(id, CLUSTER_SECURITY_PROTOCOL_KEY)
                    ));
                }
            }
        }

        if cluster.identity_mode == IdentityMode::User {
            // Reject user mode until per-caller identities are supported; never fall back to
            // the shared service account.
            problems.push(format!(
                "{} user is not supported yet: fluss-rust cannot send a SASL authorization ID, and \
                 client authentication is not implemented",
                cluster_key(id, CLUSTER_IDENTITY_MODE_KEY)
            ));
        }
    }

    fn validate_security(&self, problems: &mut Vec<String>) {
        match self.security.authentication {
            AuthenticationMode::Password => match self.security.parsed_user_count() {
                Ok(0) => problems.push(format!(
                    "{SECURITY_USERS_KEY} must configure at least one user when \
                     {SECURITY_AUTHENTICATION_KEY} is password"
                )),
                Ok(_) => {}
                Err(problem) => problems.push(format!("{SECURITY_USERS_KEY}: {problem}")),
            },
            AuthenticationMode::Token => match self.security.parsed_token_count() {
                Ok(0) => problems.push(format!(
                    "{SECURITY_TOKENS_KEY} must configure at least one token when \
                     {SECURITY_AUTHENTICATION_KEY} is token"
                )),
                Ok(_) => {}
                Err(problem) => problems.push(format!("{SECURITY_TOKENS_KEY}: {problem}")),
            },
            AuthenticationMode::TrustedHeader => {
                let name = self.security.trusted_header_name();
                if name.is_empty()
                    || !name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
                {
                    problems.push(format!(
                        "{SECURITY_TRUSTED_HEADER_NAME_KEY} must be a legal HTTP header name"
                    ));
                }
            }
            _ => {}
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
            let risk = if self.security.authentication == AuthenticationMode::Trust {
                "accepts unauthenticated requests and has no TLS"
            } else {
                "has no TLS"
            };
            warnings.push(format!(
                "{} {} is not loopback. The REST listener {risk}",
                REST_LISTEN_KEY, self.server.rest.bind_address
            ));
        }
        if self.server.metrics.enabled && !self.server.metrics.bind_address.ip().is_loopback() {
            warnings.push(format!(
                "{} {} is not loopback. Metrics are exposed without authentication or TLS",
                METRICS_LISTEN_KEY, self.server.metrics.bind_address
            ));
        }
        if self.security.authentication == AuthenticationMode::TrustedHeader
            && !self.server.rest.bind_address.ip().is_loopback()
        {
            warnings.push(format!(
                "{SECURITY_AUTHENTICATION_KEY} trusted-header on non-loopback {REST_LISTEN_KEY} \
                 trusts the {} header. Expose it only behind a trusted proxy",
                self.security.trusted_header_name()
            ));
        }
        warnings
    }

    /// Renders configuration with credentials redacted.
    pub fn redacted_debug(&self) -> String {
        format!("{self:?}")
    }
}

pub(crate) fn valid_cluster_id(id: &str) -> bool {
    if id.len() > 63 {
        return false;
    }
    let mut bytes = id.bytes();
    bytes.next().is_some_and(|first| first.is_ascii_lowercase())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn validate_bootstrap_servers(bootstrap_servers: &str) -> Result<(), String> {
    let mut configured = false;
    for server in bootstrap_servers.split(',') {
        let server = server.trim();
        if server.is_empty() {
            continue;
        }
        configured = true;
        if !valid_bootstrap_server(server) {
            return Err(format!(
                "contains invalid server {server:?}; expected host:port with a port from 1 to 65535"
            ));
        }
    }
    if configured {
        Ok(())
    } else {
        Err("must configure at least one server".to_string())
    }
}

fn valid_bootstrap_server(server: &str) -> bool {
    let Ok(authority) = server.parse::<Authority>() else {
        return false;
    };
    !server.contains('@')
        && !authority.host().is_empty()
        && authority.port_u16().is_some_and(|port| port != 0)
        && (!server.starts_with('[') || server.parse::<SocketAddr>().is_ok())
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

fn config_entry(key: &str) -> Option<&'static GatewayConfigEntry> {
    CONFIG_ENTRIES.iter().find(|entry| entry.key == key)
}

/// Derives the environment variable name from a public key: drop the `gateway.` prefix, uppercase each
/// dotted segment with `-` folded to `_`, and join the segments with `__`.
fn environment_variable(key: &str) -> String {
    let suffix = environment_suffix(
        key.strip_prefix("gateway.")
            .expect("configuration keys use the gateway prefix"),
    );
    format!("{ENV_PREFIX}{suffix}")
}

fn environment_suffix(dotted: &str) -> String {
    dotted
        .split('.')
        .map(|segment| segment.replace('-', "_").to_ascii_uppercase())
        .collect::<Vec<_>>()
        .join("__")
}

fn environment_suffix_to_option(suffix: &str) -> String {
    suffix
        .split("__")
        .map(|segment| segment.to_ascii_lowercase().replace('_', "-"))
        .collect::<Vec<_>>()
        .join(".")
}

fn environment_entry(variable: &str) -> Option<&'static GatewayConfigEntry> {
    CONFIG_ENTRIES
        .iter()
        .find(|entry| environment_variable(entry.key) == variable)
}

enum ResolvedKey {
    ClusterDeclaration,
    Fixed(&'static GatewayConfigEntry),
    Cluster {
        id: String,
        entry: &'static ClusterConfigEntry,
    },
    UnsupportedClientOption {
        id: String,
        option: String,
    },
}

fn resolve_key(key: &str) -> Result<ResolvedKey, ConfigError> {
    let unknown = || ConfigError::Parse(format!("unknown configuration key: {key}"));
    if key == CLUSTERS_KEY {
        return Ok(ResolvedKey::ClusterDeclaration);
    }
    if let Some(entry) = config_entry(key) {
        return Ok(ResolvedKey::Fixed(entry));
    }
    let Some((id, suffix)) = key
        .strip_prefix(CLUSTER_KEY_PREFIX)
        .and_then(|rest| rest.split_once('.'))
    else {
        return Err(unknown());
    };
    if !valid_cluster_id(id) {
        return Err(ConfigError::Parse(format!(
            "invalid cluster ID in configuration key: {key}"
        )));
    }
    if let Some(option) = suffix.strip_prefix(CLIENT_OPTION_PREFIX) {
        if option.is_empty() {
            return Err(unknown());
        }
        return Ok(ResolvedKey::UnsupportedClientOption {
            id: id.to_string(),
            option: option.to_string(),
        });
    }
    CLUSTER_ENTRIES
        .iter()
        .find(|entry| entry.key == suffix)
        .map(|entry| ResolvedKey::Cluster {
            id: id.to_string(),
            entry,
        })
        .ok_or_else(unknown)
}

fn resolve_environment_variable(variable: &str) -> Result<ResolvedKey, ConfigError> {
    let unknown = || ConfigError::UnknownEnvKey(variable.to_string());
    let Some(suffix) = variable.strip_prefix(ENV_PREFIX) else {
        return Err(unknown());
    };
    if suffix == environment_suffix("clusters") {
        return Ok(ResolvedKey::ClusterDeclaration);
    }
    if let Some(entry) = environment_entry(variable) {
        return Ok(ResolvedKey::Fixed(entry));
    }
    let Some((id, rest)) = suffix
        .strip_prefix("CLUSTER__")
        .and_then(|rest| rest.split_once("__"))
    else {
        return Err(unknown());
    };
    let id = id.to_ascii_lowercase();
    if !valid_cluster_id(&id) {
        return Err(unknown());
    }
    if let Some(raw_option) = rest.strip_prefix("CLIENT__") {
        let option = environment_suffix_to_option(raw_option);
        if option.is_empty() || environment_suffix(&option) != raw_option {
            return Err(unknown());
        }
        return Ok(ResolvedKey::UnsupportedClientOption { id, option });
    }
    CLUSTER_ENTRIES
        .iter()
        .find(|entry| environment_suffix(entry.key) == rest)
        .map(|entry| ResolvedKey::Cluster { id, entry })
        .ok_or_else(unknown)
}

fn cluster_key(id: &str, key: &str) -> String {
    format!("{CLUSTER_KEY_PREFIX}{id}.{key}")
}

fn parse_user_table(raw: &str) -> Result<usize, String> {
    let mut principals = std::collections::BTreeSet::new();
    for (position, entry) in raw.split(',').enumerate() {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((principal, secret)) = entry.split_once(':') else {
            return Err(format!(
                "user entry {position} must be `principal:secret` or `principal:bcrypt:<hash>`"
            ));
        };
        let principal = principal.trim();
        if principal.is_empty() {
            return Err(format!("user entry {position} has an empty principal"));
        }
        if secret.is_empty() {
            return Err(format!("user entry {position} has an empty secret"));
        }
        if let Some(hash) = secret.strip_prefix("bcrypt:")
            && (!hash.starts_with("$2") || hash.split('$').count() != 4)
        {
            return Err(format!(
                "user entry {position} (principal {principal:?}) has a malformed bcrypt hash"
            ));
        }
        if !principals.insert(principal) {
            return Err(format!(
                "user entry {position} duplicates principal {principal:?}"
            ));
        }
    }
    Ok(principals.len())
}

fn parse_token_table(raw: &str) -> Result<usize, String> {
    let mut tokens = std::collections::BTreeSet::new();
    for (position, entry) in raw.split(',').enumerate() {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((token, principal)) = entry.rsplit_once(':') else {
            return Err(format!(
                "token entry {position} must be `<token>:<principal>` or `sha256:<hex>:<principal>`"
            ));
        };
        let principal = principal.trim();
        if principal.is_empty() {
            return Err(format!("token entry {position} has an empty principal"));
        }
        if token.is_empty() {
            return Err(format!("token entry {position} has an empty token"));
        }
        if let Some(digest) = token.strip_prefix("sha256:")
            && (digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
        {
            return Err(format!(
                "token entry {position} (principal {principal:?}) has a malformed sha256 digest"
            ));
        }
        if !tokens.insert(token) {
            return Err(format!(
                "token entry {position} (principal {principal:?}) duplicates an earlier token"
            ));
        }
    }
    Ok(tokens.len())
}

fn unsupported_client_option(id: &str, option: &str, origin: Option<&str>) -> ConfigError {
    let key = cluster_key(id, &format!("{CLIENT_OPTION_PREFIX}{option}"));
    let message = format!("{key}: native Fluss client option overrides are not supported yet");
    ConfigError::Parse(match origin {
        Some(origin) => format!("{origin}: {message}"),
        None => message,
    })
}

/// Reads the cluster IDs the file may configure, from a comma-separated string or a YAML sequence.
fn declared_cluster_ids(value: &Value) -> Result<Vec<String>, ConfigError> {
    let ids: Vec<String> = match value {
        Value::String(csv) => csv.split(',').map(|id| id.trim().to_string()).collect(),
        Value::Sequence(items) => items
            .iter()
            .map(|item| {
                item.as_str().map(str::to_string).ok_or_else(|| {
                    ConfigError::Parse(format!("{CLUSTERS_KEY}: entries must be strings"))
                })
            })
            .collect::<Result<_, _>>()?,
        _ => {
            return Err(ConfigError::Parse(format!(
                "{CLUSTERS_KEY}: expected a comma-separated string or a list"
            )));
        }
    };
    let mut unique = std::collections::BTreeSet::new();
    for id in &ids {
        if !valid_cluster_id(id) {
            return Err(ConfigError::Parse(format!(
                "invalid cluster ID in {CLUSTERS_KEY}: {id:?}"
            )));
        }
        if !unique.insert(id) {
            return Err(ConfigError::Parse(format!(
                "duplicate cluster ID in {CLUSTERS_KEY}: {id:?}"
            )));
        }
    }
    Ok(ids)
}

#[derive(Clone)]
struct Assignment {
    value: Value,
    origin: Option<String>,
}

#[derive(Default)]
struct PendingConfig {
    fixed: BTreeMap<&'static str, (&'static GatewayConfigEntry, Assignment)>,
    clusters: BTreeMap<(String, &'static str), (&'static ClusterConfigEntry, Assignment)>,
}

fn assignment_error(key: &str, assignment: &Assignment, reason: String) -> ConfigError {
    let message = format!("{key}: {reason}");
    ConfigError::Parse(match &assignment.origin {
        Some(origin) => format!("{origin}: {message}"),
        None => message,
    })
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

fn read_config_file(
    contents: &str,
    pending: &mut PendingConfig,
) -> Result<Option<Vec<String>>, ConfigError> {
    let document: Value =
        serde_yaml_ng::from_str(contents).map_err(|e| ConfigError::Parse(e.to_string()))?;
    if document.is_null() {
        return Ok(None);
    }
    let mapping = document.as_mapping().ok_or_else(|| {
        ConfigError::Parse(
            "configuration must be a mapping of flat dotted keys (gateway.…: value)".to_string(),
        )
    })?;

    let mut declared = None;
    for (key, value) in mapping {
        let key = key
            .as_str()
            .ok_or_else(|| ConfigError::Parse("configuration keys must be strings".to_string()))?;
        let reason = |reason: String| ConfigError::Parse(format!("{key}: {reason}"));
        match resolve_key(key)? {
            ResolvedKey::ClusterDeclaration => declared = Some(declared_cluster_ids(value)?),
            ResolvedKey::Fixed(entry) => {
                scalar(value).map_err(reason)?;
                pending.fixed.insert(
                    entry.key,
                    (
                        entry,
                        Assignment {
                            value: value.clone(),
                            origin: None,
                        },
                    ),
                );
            }
            ResolvedKey::Cluster { id, entry } => {
                scalar(value).map_err(reason)?;
                pending.clusters.insert(
                    (id, entry.key),
                    (
                        entry,
                        Assignment {
                            value: value.clone(),
                            origin: None,
                        },
                    ),
                );
            }
            ResolvedKey::UnsupportedClientOption { id, option } => {
                return Err(unsupported_client_option(&id, &option, None));
            }
        }
    }
    Ok(declared)
}

fn declared_clusters(declared: Option<Vec<String>>) -> Vec<String> {
    declared.unwrap_or_else(|| vec![DEFAULT_CLUSTER_ID.to_string()])
}

fn apply_pending(
    pending: PendingConfig,
    declared: Vec<String>,
) -> Result<GatewayConfig, ConfigError> {
    let mut config = GatewayConfig {
        clusters: declared
            .iter()
            .map(|id| (id.clone(), ClusterConfig::default()))
            .collect(),
        ..GatewayConfig::default()
    };

    for (_, (entry, assignment)) in pending.fixed {
        (entry.apply)(&mut config, &assignment.value)
            .map_err(|reason| assignment_error(entry.key, &assignment, reason))?;
    }
    for ((id, _), (entry, assignment)) in pending.clusters {
        let public_key = cluster_key(&id, entry.key);
        let cluster = config.clusters.get_mut(&id).ok_or_else(|| {
            ConfigError::Parse(format!(
                "{CLUSTER_KEY_PREFIX}{id}.* is configured but {id} is not declared in {CLUSTERS_KEY}"
            ))
        })?;
        (entry.apply)(cluster, &assignment.value)
            .map_err(|reason| assignment_error(&public_key, &assignment, reason))?;
    }
    Ok(config)
}

/// Loads configuration with precedence CLI > env > file > defaults.
pub fn load(
    path: Option<&Path>,
    env: &BTreeMap<String, String>,
    cli: &CliOverrides,
) -> Result<GatewayConfig, ConfigError> {
    let mut pending = PendingConfig::default();
    let mut declared = None;
    if let Some(path) = path {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| ConfigError::Io(format!("{}: {e}", path.display())))?;
        declared = read_config_file(&contents, &mut pending)?;
    }

    for (variable, raw) in env {
        if !variable.starts_with(ENV_PREFIX) {
            continue;
        }
        let value = Value::String(raw.clone());
        match resolve_environment_variable(variable)? {
            ResolvedKey::ClusterDeclaration => {
                declared = Some(declared_cluster_ids(&value)?);
            }
            ResolvedKey::Fixed(entry) => {
                pending.fixed.insert(
                    entry.key,
                    (
                        entry,
                        Assignment {
                            value,
                            origin: Some(variable.clone()),
                        },
                    ),
                );
            }
            ResolvedKey::Cluster { id, entry } => {
                pending.clusters.insert(
                    (id, entry.key),
                    (
                        entry,
                        Assignment {
                            value,
                            origin: Some(variable.clone()),
                        },
                    ),
                );
            }
            ResolvedKey::UnsupportedClientOption { id, option } => {
                return Err(unsupported_client_option(&id, &option, Some(variable)));
            }
        }
    }

    if let Some(value) = &cli.bind_address {
        let entry = config_entry(REST_LISTEN_KEY).expect("REST listen option is registered");
        pending.fixed.insert(
            entry.key,
            (
                entry,
                Assignment {
                    value: Value::String(value.clone()),
                    origin: Some("--bind-address".to_string()),
                },
            ),
        );
    }

    let config = apply_pending(pending, declared_clusters(declared))?;
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
        assert_eq!(config.server.rest.metadata_max_concurrent_requests, 16);
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
        assert_eq!(config.clusters.len(), 1);
        assert_eq!(
            cluster(&config, DEFAULT_CLUSTER_ID).bootstrap_servers,
            DEFAULT_BOOTSTRAP_SERVERS
        );
        assert_eq!(
            cluster(&config, DEFAULT_CLUSTER_ID)
                .connection_idle_timeout
                .get(),
            Duration::from_secs(10 * 60)
        );
        assert_eq!(
            cluster(&config, DEFAULT_CLUSTER_ID).identity_mode,
            IdentityMode::Service
        );
        assert_eq!(
            cluster(&config, DEFAULT_CLUSTER_ID).security_protocol,
            ConnectionSecurityProtocol::Plaintext
        );
        assert_eq!(
            cluster(&config, DEFAULT_CLUSTER_ID)
                .native_client_config()
                .security_protocol,
            "PLAINTEXT"
        );
        assert_eq!(config.security.authentication, AuthenticationMode::Trust);
        assert_eq!(config.request_limits, RequestLimitsConfig::default());
        assert!(config.warnings().is_empty());
    }

    // conf/gateway.yaml ships in the binary distribution and the container image, so it is
    // a release artifact rather than a sample: it has to parse, it has to document every
    // option, and its values must not drift away from the compiled defaults.
    #[test]
    fn distribution_configuration_is_loadable_and_complete() {
        let template = include_str!("../conf/gateway.yaml");
        let config = load_file(template).unwrap();
        let defaults = load(None, &no_env(), &CliOverrides::default()).unwrap();

        assert_eq!(config, defaults);

        // Cluster IDs are resolved before the typed entries, so this key is not in
        // CONFIG_ENTRIES and would otherwise go undocumented unnoticed.
        assert!(template.contains(&format!("{CLUSTERS_KEY}:")));
        for entry in CONFIG_ENTRIES {
            assert!(
                template.contains(&format!("{}:", entry.key)),
                "distribution configuration does not document {}",
                entry.key
            );
        }
        for entry in CLUSTER_ENTRIES {
            let key = cluster_key(DEFAULT_CLUSTER_ID, entry.key);
            assert!(
                template.contains(&format!("{key}:")),
                "distribution configuration does not document {key}"
            );
        }
    }

    #[test]
    fn public_yaml_options_are_loaded() {
        let config = load_file(
            r#"
    gateway.instance-id: gateway-1
    gateway.rest.listen: 0.0.0.0:8080
    gateway.rest.write.max-request-bytes: 32MiB
    gateway.rest.request-timeout: 30s
    gateway.rest.metadata.max-concurrent-requests: 16
    gateway.metrics.enabled: true
    gateway.metrics.exporters: prometheus
    gateway.metrics.exporter.prometheus.listen: 0.0.0.0:9095
    gateway.rest.write.rate-limit.enabled: true
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
        assert_eq!(config.server.rest.metadata_max_concurrent_requests, 16);
        assert_eq!(
            config.server.rest.request_timeout.get(),
            Duration::from_secs(30)
        );
        assert!(config.server.metrics.enabled);
        assert_eq!(config.server.metrics.exporter, MetricsExporter::Prometheus);
        assert_eq!(
            config.server.metrics.bind_address,
            "0.0.0.0:9095".parse().unwrap()
        );
        assert!(config.request_limits.write_rate_limit_enabled);
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
            "gateway.cluster.default.bootstrap.serverz: fluss:9123\n",
            "gateway.cluster.default.request-timeout: 30s\n",
            "gateway.cluster.default.connection.identity: user\n",
            "gateway.cluster.default.connection.max: 512\n",
            "gateway.cluster.default.client.: 1\n",
            "gateway.cluster.default: fluss:9123\n",
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
    gateway.rest.listen: not-an-address
    gateway.metrics.enabled: true
    "#,
        );
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__REST__LISTEN".to_string(),
            "also-not-an-address".to_string(),
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

        // Semantic errors in losing sources are masked, but the winning value is still parsed normally.
        let file = write_temp_config("gateway.cluster.default.connect-timeout: not-a-duration\n");
        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__CLUSTER__DEFAULT__CONNECT_TIMEOUT".to_string(),
            "10s".to_string(),
        );
        let config = load(Some(file.path()), &env, &CliOverrides::default()).unwrap();
        assert_eq!(
            cluster(&config, DEFAULT_CLUSTER_ID).connect_timeout.get(),
            Duration::from_secs(10)
        );
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
    fn compound_file_values_are_rejected_even_when_overridden() {
        let file = write_temp_config("gateway.rest.listen: [127.0.0.1:8080]\n");
        let env = BTreeMap::from([(
            "FLUSS_GATEWAY__REST__LISTEN".to_string(),
            "127.0.0.1:28080".to_string(),
        )]);
        let error = load(Some(file.path()), &env, &CliOverrides::default()).unwrap_err();
        assert!(error.to_string().contains(REST_LISTEN_KEY), "{error}");
        assert!(error.to_string().contains("scalar"), "{error}");

        let file =
            write_temp_config("gateway.cluster.default.connect-timeout:\n  unexpected: mapping\n");
        let env = BTreeMap::from([(
            "FLUSS_GATEWAY__CLUSTER__DEFAULT__CONNECT_TIMEOUT".to_string(),
            "10s".to_string(),
        )]);
        let error = load(Some(file.path()), &env, &CliOverrides::default()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("gateway.cluster.default.connect-timeout"),
            "{error}"
        );
        assert!(error.to_string().contains("scalar"), "{error}");
    }

    #[test]
    fn unknown_environment_variables_are_rejected() {
        for key in [
            "FLUSS_GATEWAY__REST__LISTENN",
            "FLUSS_GATEWAY__QUERY__ENABLED",
            "FLUSS_GATEWAY__SERVER_REST__BIND_ADDRESS",
            "FLUSS_GATEWAY__CLUSTER__DEFAULT__BOOTSTRAP__SERVERZ",
            "FLUSS_GATEWAY__CLUSTER__DEFAULT",
            "FLUSS_GATEWAY__CLUSTER__1ST__BOOTSTRAP__SERVERS",
            "FLUSS_GATEWAY__CLUSTER__DEFAULT__CLIENT__",
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
        let mut env = no_env();
        env.extend([
            ("FLUSS_GATEWAY__INSTANCE_ID".to_string(), "123".to_string()),
            (
                "FLUSS_GATEWAY__REST__LISTEN".to_string(),
                "127.0.0.1:18080".to_string(),
            ),
            (
                "FLUSS_GATEWAY__REST__REQUEST_TIMEOUT".to_string(),
                "5s".to_string(),
            ),
            (
                "FLUSS_GATEWAY__CLUSTER__DEFAULT__CONNECT_TIMEOUT".to_string(),
                "3s".to_string(),
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
            (
                "FLUSS_GATEWAY__REST__LOOKUP__MAX_KEYS".to_string(),
                "16".to_string(),
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
        assert_eq!(
            cluster(&config, DEFAULT_CLUSTER_ID).connect_timeout.get(),
            Duration::from_secs(3)
        );
        assert_eq!(config.server.rest.max_body_bytes.bytes(), 2 * 1024 * 1024);
        assert!(!config.server.metrics.enabled);
        assert_eq!(
            config.server.metrics.bind_address,
            "127.0.0.1:19095".parse().unwrap()
        );
        assert_eq!(config.shutdown.drain_timeout.get(), Duration::from_secs(10));
        assert_eq!(config.request_limits.lookup_max_keys, 16);
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

        let env = BTreeMap::from([(
            "FLUSS_GATEWAY__CLUSTER__DEFAULT__CONNECT_TIMEOUT".to_string(),
            "soon".to_string(),
        )]);
        let rendered = load(None, &env, &CliOverrides::default())
            .unwrap_err()
            .to_string();
        assert!(
            rendered.contains("FLUSS_GATEWAY__CLUSTER__DEFAULT__CONNECT_TIMEOUT"),
            "{rendered}"
        );
        assert!(
            rendered.contains("gateway.cluster.default.connect-timeout"),
            "{rendered}"
        );

        let env = BTreeMap::from([(
            "FLUSS_GATEWAY__CLUSTER__DEFAULT__CLIENT__WRITER__BATCH_SIZE".to_string(),
            "many".to_string(),
        )]);
        let rendered = load(None, &env, &CliOverrides::default())
            .unwrap_err()
            .to_string();
        assert!(
            rendered.contains("FLUSS_GATEWAY__CLUSTER__DEFAULT__CLIENT__WRITER__BATCH_SIZE"),
            "{rendered}"
        );
        assert!(rendered.contains("writer.batch-size"), "{rendered}");
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
        for bad in ["0ms", "60", "6.5s", "s", "366d", "-1s"] {
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
            "18446744073709551615d",
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
            errors
                .iter()
                .any(|error| { error == "gateway.rest.request-timeout must be greater than zero" }),
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
    fn metadata_concurrency_limit_must_be_positive() {
        let mut config = GatewayConfig::default();
        config.server.rest.metadata_max_concurrent_requests = 0;

        let errors = problems(config.validate().unwrap_err());
        assert!(
            errors.iter().any(|error| {
                error == "gateway.rest.metadata.max-concurrent-requests must be greater than zero"
            }),
            "got: {errors:?}"
        );
    }

    /// `user` identity mode is refused at startup instead of running as the shared service account,
    /// and the message names the two upstream gaps that have to close first.
    #[test]
    fn user_identity_mode_is_refused_at_startup() {
        let mut config = GatewayConfig::default();
        let cluster = config.clusters.get_mut("default").expect("default cluster");
        cluster.identity_mode = IdentityMode::User;

        let errors = problems(config.validate().unwrap_err());
        let refusal = errors
            .iter()
            .find(|error| error.contains("connection.identity-mode"))
            .unwrap_or_else(|| panic!("{errors:?}"));
        assert!(refusal.contains("authorization ID"), "{refusal}");
        assert!(refusal.contains("client authentication"), "{refusal}");

        assert!(GatewayConfig::default().validate().is_ok());
    }

    #[test]
    fn connection_security_protocol_validates_credentials() {
        assert!(GatewayConfig::default().validate().is_ok());
        assert!(
            load_file(
                "gateway.cluster.default.connection.security.protocol: sasl\n\
                 gateway.cluster.default.connection.service.account: gateway_svc\n\
                 gateway.cluster.default.connection.service.secret: secret\n"
            )
            .is_ok()
        );

        for contents in [
            "gateway.cluster.default.connection.security.protocol: sasl\n",
            "gateway.cluster.default.connection.security.protocol: sasl\n\
             gateway.cluster.default.connection.service.account: gateway_svc\n",
            "gateway.cluster.default.connection.security.protocol: plaintext\n\
             gateway.cluster.default.connection.service.account: gateway_svc\n\
             gateway.cluster.default.connection.service.secret: secret\n",
        ] {
            let error = load_file(contents).unwrap_err().to_string();
            assert!(error.contains("connection.security.protocol"), "{error}");
        }
    }

    #[test]
    fn backend_connect_timeout_is_independent_of_a_request_deadline() {
        let mut config = GatewayConfig::default();
        config.server.rest.request_timeout = ConfigDuration::from_secs(1);
        config
            .clusters
            .get_mut(DEFAULT_CLUSTER_ID)
            .unwrap()
            .connect_timeout = ConfigDuration::from_secs(10);

        assert!(config.validate().is_ok());
    }

    #[test]
    fn connection_idle_timeout_must_outlive_the_handler_budget() {
        let mut config = GatewayConfig::default();
        config.server.rest.request_timeout = ConfigDuration::from_secs(5);
        config
            .clusters
            .get_mut(DEFAULT_CLUSTER_ID)
            .unwrap()
            .connection_idle_timeout = ConfigDuration::from_secs(4);

        let errors = problems(config.validate().unwrap_err());
        assert!(errors.iter().any(|error| {
            error.contains("gateway.cluster.default.connection.idle-timeout")
                && error.contains("REST handler timeout 4s")
        }));

        config
            .clusters
            .get_mut(DEFAULT_CLUSTER_ID)
            .unwrap()
            .connection_idle_timeout = ConfigDuration::from_millis(4001);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn programmatically_constructed_zero_byte_limits_are_validated() {
        let mut config = GatewayConfig::default();
        config.server.rest.max_body_bytes = ByteSize::new(0);
        config.request_limits.write_rate_limit_bytes_per_second = ByteSize::new(0);

        let errors = problems(config.validate().unwrap_err());
        assert!(
            errors
                .iter()
                .any(|error| error
                    == "gateway.rest.write.max-request-bytes must be greater than zero"),
            "{errors:?}"
        );
        assert!(
            errors.iter().any(|error| {
                error == "gateway.rest.write.rate-limit.bytes-per-second must be greater than zero"
            }),
            "{errors:?}"
        );
    }

    #[test]
    fn invalid_byte_size_rejected() {
        for bad in ["0", "\"4XB\"", "\"MiB\"", "-1", "\"1.5MiB\""] {
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
    fn authenticated_non_loopback_rest_does_not_warn_about_unauthenticated_requests() {
        for contents in [
            "gateway.rest.listen: 0.0.0.0:8080\n\
             gateway.security.authentication: password\n\
             gateway.security.users: alice:secret\n",
            "gateway.rest.listen: 0.0.0.0:8080\n\
             gateway.security.authentication: token\n\
             gateway.security.tokens: token:alice\n",
        ] {
            let warnings = load_file(contents).unwrap().warnings();
            assert!(
                warnings
                    .iter()
                    .any(|warning| warning.contains("has no TLS"))
            );
            assert!(
                warnings
                    .iter()
                    .all(|warning| !warning.contains("accepts unauthenticated requests")),
                "{warnings:?}"
            );
        }
    }

    #[test]
    fn exposed_metrics_and_trusted_headers_warn() {
        let config =
            load_file("gateway.metrics.exporter.prometheus.listen: 0.0.0.0:9095\n").unwrap();
        assert!(config.warnings().iter().any(|warning| {
            warning.contains(METRICS_LISTEN_KEY)
                && warning.contains("without authentication or TLS")
        }));

        let config = load_file(
            "gateway.rest.listen: 0.0.0.0:8080\n\
             gateway.security.authentication: trusted-header\n",
        )
        .unwrap();
        assert!(config.warnings().iter().any(|warning| {
            warning.contains(SECURITY_AUTHENTICATION_KEY)
                && warning.contains(DEFAULT_TRUSTED_HEADER_NAME)
                && warning.contains("trusted proxy")
        }));
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
        assert_eq!(
            ConfigDuration::parse("1 d").unwrap().get(),
            Duration::from_secs(24 * 60 * 60)
        );
        assert_eq!(
            ConfigDuration::parse(" 1 S ").unwrap().get(),
            Duration::from_secs(1)
        );
        assert!(ConfigDuration::parse("0s").is_err());
    }

    #[test]
    fn byte_size_units() {
        assert_eq!(ByteSize::parse("512").unwrap().bytes(), 512);
        assert_eq!(ByteSize::parse("512B").unwrap().bytes(), 512);
        assert_eq!(ByteSize::parse("4KB").unwrap().bytes(), 4096);
        assert_eq!(ByteSize::parse("4KiB").unwrap().bytes(), 4096);
        assert_eq!(ByteSize::parse(" 4 kb ").unwrap().bytes(), 4096);
        assert_eq!(ByteSize::parse("1GiB").unwrap().bytes(), 1024 * 1024 * 1024);
        assert_eq!(
            ByteSize::parse("1TB").unwrap().bytes(),
            1024_u64 * 1024 * 1024 * 1024
        );
        assert!(ByteSize::parse("0").is_err());
    }

    fn cluster<'a>(config: &'a GatewayConfig, id: &str) -> &'a ClusterConfig {
        config.clusters.get(id).expect("configured cluster")
    }

    #[test]
    fn typed_cluster_security_and_request_limit_options_are_loaded() {
        let config = load_file(
            "gateway.clusters: default, analytics\n\
             gateway.cluster.default.bootstrap.servers: fluss-1:9123,fluss-2:9123\n\
             gateway.cluster.default.connection.identity-mode: service\n\
             gateway.cluster.default.connection.security.protocol: sasl\n\
             gateway.cluster.default.connection.service.account: gateway_svc\n\
             gateway.cluster.default.connection.service.secret: gw-pass\n\
             gateway.cluster.default.connection.idle-timeout: 12m\n\
             gateway.cluster.analytics.bootstrap.servers: analytics:9123,analytics-2:9123\n\
             gateway.cluster.analytics.connect-timeout: 5s\n\
             gateway.security.authentication: password\n\
             gateway.security.users: alice:secret\n\
             gateway.rest.write.max-rows: 500\n\
             gateway.rest.write.rate-limit.enabled: true\n\
             gateway.rest.write.rate-limit.requests-per-second: 100\n\
             gateway.rest.write.rate-limit.bytes-per-second: 4MiB\n\
             gateway.rest.lookup.max-keys: 32\n\
             gateway.rest.lookup.max-key-bytes: 2MiB\n",
        )
        .unwrap();

        let default = cluster(&config, "default");
        assert_eq!(default.bootstrap_servers, "fluss-1:9123,fluss-2:9123");
        assert_eq!(default.identity_mode, IdentityMode::Service);
        assert_eq!(default.security_protocol, ConnectionSecurityProtocol::Sasl);
        assert_eq!(default.service_account(), Some("gateway_svc"));
        assert_eq!(default.service_secret(), Some("gw-pass"));
        assert_eq!(
            default.connection_idle_timeout.get(),
            Duration::from_secs(12 * 60)
        );
        let native = default.native_client_config();
        assert_eq!(native.bootstrap_servers, "fluss-1:9123,fluss-2:9123");
        assert_eq!(native.connect_timeout_ms, 10_000);
        assert_eq!(native.security_protocol, "sasl");
        assert_eq!(native.security_sasl_mechanism, "PLAIN");
        assert_eq!(native.security_sasl_username, "gateway_svc");
        assert_eq!(native.security_sasl_password, "gw-pass");
        assert_eq!(
            cluster(&config, "analytics").bootstrap_servers,
            "analytics:9123,analytics-2:9123"
        );
        assert_eq!(
            cluster(&config, "analytics").connect_timeout.get(),
            Duration::from_secs(5)
        );
        assert_eq!(config.security.authentication, AuthenticationMode::Password);
        let analytics_native = cluster(&config, "analytics").native_client_config();
        assert_eq!(analytics_native.connect_timeout_ms, 5_000);
        assert_eq!(analytics_native.security_protocol, "PLAINTEXT");
        assert!(analytics_native.security_sasl_username.is_empty());
        assert_eq!(config.request_limits.write_max_rows, 500);
        assert!(config.request_limits.write_rate_limit_enabled);
        assert_eq!(
            config.request_limits.write_rate_limit_requests_per_second,
            100
        );
        assert_eq!(
            config
                .request_limits
                .write_rate_limit_bytes_per_second
                .bytes(),
            4 * 1024 * 1024
        );
        assert_eq!(config.request_limits.lookup_max_keys, 32);
        assert_eq!(
            config.request_limits.lookup_max_key_bytes.bytes(),
            2 * 1024 * 1024
        );
    }

    #[test]
    fn bootstrap_servers_are_scalar_and_require_at_least_one_server() {
        let csv =
            load_file("gateway.cluster.default.bootstrap.servers: a:9123, b:9123, [::1]:9123\n")
                .unwrap();
        assert_eq!(
            cluster(&csv, "default").bootstrap_servers,
            "a:9123, b:9123, [::1]:9123"
        );

        let mut env = no_env();
        env.insert(
            "FLUSS_GATEWAY__CLUSTER__DEFAULT__BOOTSTRAP__SERVERS".to_string(),
            "env-a:9123, env-b:9123".to_string(),
        );
        let from_env = load(None, &env, &CliOverrides::default()).unwrap();
        assert_eq!(
            cluster(&from_env, "default").bootstrap_servers,
            "env-a:9123, env-b:9123"
        );

        for contents in [
            "gateway.cluster.default.bootstrap.servers: \" , \"\n",
            "gateway.cluster.default.bootstrap.servers: []\n",
            "gateway.cluster.default.bootstrap.servers: [a:9123, b:9123]\n",
            "gateway.cluster.default.bootstrap.servers: host\n",
            "gateway.cluster.default.bootstrap.servers: host:99999\n",
            "gateway.cluster.default.bootstrap.servers: http://host:9123\n",
            "gateway.cluster.default.bootstrap.servers: user@host:9123\n",
            "gateway.cluster.default.bootstrap.servers: '[not-ip]:9123'\n",
            "gateway.cluster.default.bootstrap.servers: '[::1]suffix:9123'\n",
            "gateway.cluster.default.bootstrap.servers: host:0\n",
        ] {
            let error = load_file(contents).unwrap_err().to_string();
            assert!(error.contains(CLUSTER_BOOTSTRAP_SERVERS_KEY), "{error}");
        }
    }

    #[test]
    fn cluster_declarations_are_authoritative_and_validated() {
        for contents in [
            "gateway.clusters: default\n\
             gateway.cluster.analytics.bootstrap.servers: analytics:9123\n",
            "gateway.cluster.analytics.bootstrap.servers: analytics:9123\n",
        ] {
            let error = load_file(contents).unwrap_err();
            assert!(
                error.to_string().contains("not declared"),
                "{contents}: {error}"
            );
        }

        let config = load_file("gateway.clusters: default,analytics\n").unwrap();
        assert_eq!(config.clusters.len(), 2);
        assert_eq!(
            cluster(&config, "analytics").bootstrap_servers,
            DEFAULT_BOOTSTRAP_SERVERS
        );
        for declaration in [
            "gateway.clusters: default,default\n",
            "gateway.clusters: [default, default]\n",
        ] {
            assert!(
                load_file(declaration)
                    .unwrap_err()
                    .to_string()
                    .contains("duplicate cluster ID"),
                "{declaration}"
            );
        }

        let config = load_file("gateway.cluster.default.bootstrap.servers: only:9123\n").unwrap();
        assert_eq!(config.clusters.keys().collect::<Vec<_>>(), ["default"]);

        {
            for contents in [
                "gateway.clusters: Default\n",
                "gateway.clusters: 1st\n",
                "gateway.clusters: eu-west\n",
                "gateway.cluster.EU.bootstrap.servers: eu:9123\n",
            ] {
                let error = load_file(contents).unwrap_err();
                assert!(
                    error.to_string().contains("cluster ID"),
                    "{contents}: {error}"
                );
            }
        }

        let config = load_file("gateway.clusters: analytics\n").unwrap();
        assert_eq!(config.clusters.keys().collect::<Vec<_>>(), ["analytics"]);

        let duplicate_env = BTreeMap::from([(
            "FLUSS_GATEWAY__CLUSTERS".to_string(),
            "default,default".to_string(),
        )]);
        assert!(
            load(None, &duplicate_env, &CliOverrides::default())
                .unwrap_err()
                .to_string()
                .contains("duplicate cluster ID")
        );

        {
            let file = write_temp_config("gateway.cluster.analytics.bootstrap.servers: eu:9123\n");
            let mut env = no_env();
            env.insert(
                "FLUSS_GATEWAY__CLUSTERS".to_string(),
                "analytics".to_string(),
            );
            let config = load(Some(file.path()), &env, &CliOverrides::default()).unwrap();
            assert_eq!(config.clusters.keys().collect::<Vec<_>>(), ["analytics"]);

            env.insert("FLUSS_GATEWAY__CLUSTERS".to_string(), "default".to_string());
            let error = load(Some(file.path()), &env, &CliOverrides::default()).unwrap_err();
            assert!(error.to_string().contains("not declared"), "got: {error}");
        }
    }

    #[test]
    fn same_options_remain_independent_across_clusters() {
        let config = load_file(
            "gateway.clusters: default,analytics\n\
             gateway.cluster.default.connect-timeout: 1s\n\
             gateway.cluster.analytics.connect-timeout: 2s\n",
        )
        .unwrap();
        assert_eq!(
            cluster(&config, "default").connect_timeout.get(),
            Duration::from_secs(1)
        );
        assert_eq!(
            cluster(&config, "analytics").connect_timeout.get(),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn native_client_options_are_reserved_but_not_supported() {
        let file_key = "gateway.cluster.default.client.writer.batch-size";
        let error = load_file(&format!("{file_key}: do-not-leak\n"))
            .unwrap_err()
            .to_string();
        assert!(error.contains(file_key), "{error}");
        assert!(error.contains("not supported yet"), "{error}");
        assert!(!error.contains("do-not-leak"), "{error}");

        let variable = "FLUSS_GATEWAY__CLUSTER__DEFAULT__CLIENT__WRITER__BATCH_SIZE";
        let env = BTreeMap::from([(variable.to_string(), "do-not-leak".to_string())]);
        let error = load(None, &env, &CliOverrides::default())
            .unwrap_err()
            .to_string();
        assert!(error.contains(variable), "{error}");
        assert!(error.contains(file_key), "{error}");
        assert!(error.contains("not supported yet"), "{error}");
        assert!(!error.contains("do-not-leak"), "{error}");
    }

    #[test]
    fn metrics_exporters_accept_only_prometheus() {
        assert!(load_file("gateway.metrics.exporters: prometheus\n").is_ok());
        let error = load_file("gateway.metrics.exporters: otlp\n")
            .unwrap_err()
            .to_string();
        assert!(error.contains(METRICS_EXPORTERS_KEY), "{error}");
        assert!(error.contains("expected prometheus"), "{error}");
    }

    #[test]
    fn authentication_tables_are_structurally_validated_before_startup() {
        for contents in [
            "gateway.security.authentication: password\n\
             gateway.security.users: alice\n",
            "gateway.security.authentication: password\n\
             gateway.security.users: :secret\n",
            "gateway.security.authentication: password\n\
             gateway.security.users: alice:first,alice:second\n",
            "gateway.security.authentication: password\n\
             gateway.security.users: alice:bcrypt:not-a-hash\n",
            "gateway.security.authentication: token\n\
             gateway.security.tokens: token-only\n",
            "gateway.security.authentication: token\n\
             gateway.security.tokens: token:\n",
            "gateway.security.authentication: token\n\
             gateway.security.tokens: token:alice,token:bob\n",
            "gateway.security.authentication: token\n\
             gateway.security.tokens: sha256:not-a-digest:alice\n",
        ] {
            assert!(load_file(contents).is_err(), "accepted: {contents}");
        }

        assert!(
            load_file(
                "gateway.security.authentication: password\n\
                 gateway.security.users: alice:plain-secret,bob:bcrypt:$2b$12$abcdefghijklmnopqrstuuuuuuuuuuuuuuuuuuuuuuuuuuu\n"
            )
            .is_ok()
        );
        assert!(
            load_file(
                "gateway.security.authentication: token\n\
                 gateway.security.tokens: token:with:colons:alice,sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef:bob\n"
            )
            .is_ok()
        );
    }

    #[test]
    fn cluster_ids_must_not_exceed_63_characters() {
        let valid = format!("a{}", "b".repeat(62));
        assert!(load_file(&format!("gateway.clusters: {valid}\n")).is_ok());

        let invalid = format!("a{}", "b".repeat(63));
        let error = load_file(&format!("gateway.clusters: {invalid}\n"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("invalid cluster ID"), "{error}");
    }

    #[test]
    fn cross_field_cluster_and_security_constraints_fail_before_startup() {
        for contents in [
            // An account without its secret, and the reverse.
            "gateway.cluster.default.connection.service.account: gateway_svc\n",
            "gateway.cluster.default.connection.service.secret: gw-pass\n",
            "gateway.cluster.default.bootstrap.servers: \" \"\n",
            "gateway.rest.write.rate-limit.requests-per-second: 0\n",
            // The mode's credential table is missing.
            "gateway.security.authentication: password\n",
            "gateway.security.authentication: token\n",
            "gateway.security.authentication: trusted-header\n\
             gateway.security.trusted-header.name: \"bad header\"\n",
            "gateway.rest.lookup.max-keys: 0\n",
            "gateway.rest.prefix-lookup.max-prefixes: 0\n",
            // User identity mode, however it is spelled out, cannot be served yet.
            "gateway.security.authentication: password\n\
             gateway.security.users: alice:secret\n\
             gateway.cluster.default.connection.identity-mode: user\n",
        ] {
            assert!(load_file(contents).is_err(), "accepted: {contents}");
        }

        // Directly constructed configurations are subject to the same validation.
        let mut config = GatewayConfig::default();
        config.clusters.clear();
        assert!(
            problems(config.validate().unwrap_err())
                .iter()
                .any(|error| error == "gateway.clusters must declare at least one cluster")
        );
    }

    #[test]
    fn diagnostics_redact_every_credential_and_keep_the_identities() {
        let config = load_file(
            "gateway.cluster.default.connection.security.protocol: sasl\n\
             gateway.cluster.default.connection.service.account: canonical-user\n\
             gateway.cluster.default.connection.service.secret: canonical-secret\n\
             gateway.security.authentication: password\n\
             gateway.security.users: alice:user-secret\n\
             gateway.security.tokens: token-secret:alice\n",
        )
        .unwrap();

        for diagnostic in [config.redacted_debug(), format!("{config:?}")] {
            for credential in ["canonical-secret", "user-secret", "token-secret"] {
                assert!(
                    !diagnostic.contains(credential),
                    "leaked {credential}: {diagnostic}"
                );
            }
            // The service identity stays readable: it is not credential material.
            assert!(diagnostic.contains("canonical-user"), "{diagnostic}");
            assert!(diagnostic.contains(REDACTED), "{diagnostic}");
        }

        // The credentials are still reachable by the components that authenticate with them.
        assert_eq!(
            config.security.users.as_ref().map(Secret::expose),
            Some("alice:user-secret")
        );
        assert_eq!(
            cluster(&config, "default").service_secret(),
            Some("canonical-secret")
        );

        // Startup errors name an unsupported client option without quoting credentials from the same input.
        let error = load_file(
            "gateway.security.authentication: token\n\
             gateway.security.tokens: do-not-leak\n\
             gateway.cluster.default.connection.service.secret: also-secret\n\
             gateway.cluster.default.client.writer.unknown-knob: 0\n",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("writer.unknown-knob"), "{error}");
        assert!(!error.contains("do-not-leak"), "{error}");
        assert!(!error.contains("also-secret"), "{error}");
    }

    #[test]
    fn option_registries_are_safe_and_environment_compatible() {
        let mut keys = std::collections::BTreeSet::new();
        let mut suffixes = std::collections::BTreeSet::new();

        for entry in CLUSTER_ENTRIES {
            assert!(!entry.key.starts_with("gateway."), "{entry:?}");
            assert!(
                !entry.key.starts_with(CLIENT_OPTION_PREFIX),
                "{} collides with the reserved client namespace",
                entry.key
            );
            assert!(keys.insert(entry.key), "duplicate key: {}", entry.key);
            assert!(
                suffixes.insert(environment_suffix(entry.key)),
                "duplicate environment suffix for {}",
                entry.key
            );
        }
    }

    #[test]
    fn the_environment_overrides_the_file_for_every_option() {
        for entry in CONFIG_ENTRIES {
            if entry.key == METRICS_EXPORTERS_KEY {
                let file = write_temp_config(&format!("{}: otlp\n", entry.key));
                let mut env = no_env();
                env.insert(environment_variable(entry.key), "prometheus".to_string());
                let from_env = load(Some(file.path()), &env, &CliOverrides::default())
                    .unwrap_or_else(|error| panic!("{}: {error}", entry.key));
                let only_env = load(None, &env, &CliOverrides::default())
                    .unwrap_or_else(|error| panic!("{}: {error}", entry.key));
                assert_eq!(from_env, only_env);
                continue;
            }

            let (file_value, env_value) = match entry.key {
                METRICS_ENABLED_KEY | REST_WRITE_RATE_LIMIT_ENABLED_KEY => ("true", "false"),
                REST_WRITE_MAX_ROWS_KEY
                | REST_METADATA_MAX_CONCURRENT_REQUESTS_KEY
                | REST_WRITE_MAX_CONCURRENT_REQUESTS_KEY
                | REST_WRITE_RATE_LIMIT_REQUESTS_PER_SECOND_KEY
                | REST_LOOKUP_MAX_KEYS_KEY
                | REST_LOOKUP_MAX_CONCURRENT_REQUESTS_KEY
                | REST_PREFIX_LOOKUP_MAX_PREFIXES_KEY
                | REST_PREFIX_LOOKUP_MAX_ROWS_PER_PREFIX_KEY
                | REST_PREFIX_LOOKUP_MAX_CONCURRENT_REQUESTS_KEY => ("11", "22"),
                REST_MAX_REQUEST_BYTES_KEY
                | REST_WRITE_RATE_LIMIT_BYTES_PER_SECOND_KEY
                | REST_LOOKUP_MAX_KEY_BYTES_KEY => ("1MiB", "2MiB"),
                REST_LISTEN_KEY => ("127.0.0.1:11111", "127.0.0.1:22222"),
                METRICS_LISTEN_KEY => ("127.0.0.1:11112", "127.0.0.1:22223"),
                REST_REQUEST_TIMEOUT_KEY => ("12s", "22s"),
                REST_HEADER_READ_TIMEOUT_KEY | SHUTDOWN_DRAIN_TIMEOUT_KEY => ("11s", "22s"),
                // Both modes must be valid on their own: the file value is loaded without the
                // environment override, and password and token modes need a credential table.
                SECURITY_AUTHENTICATION_KEY => ("trusted-header", "trust"),
                _ => ("file-value", "env-value"),
            };

            let file = write_temp_config(&format!("{}: \"{file_value}\"\n", entry.key));
            let from_file = load(Some(file.path()), &no_env(), &CliOverrides::default())
                .unwrap_or_else(|error| panic!("{}: {error}", entry.key));
            let mut env = no_env();
            env.insert(environment_variable(entry.key), env_value.to_string());
            let from_env = load(Some(file.path()), &env, &CliOverrides::default())
                .unwrap_or_else(|error| panic!("{}: {error}", entry.key));

            assert_ne!(
                from_file, from_env,
                "{} ignores its environment variable",
                entry.key
            );
            let only_env = load(None, &env, &CliOverrides::default())
                .unwrap_or_else(|error| panic!("{}: {error}", entry.key));
            assert_eq!(
                from_env, only_env,
                "{} lets the file value survive the environment override",
                entry.key
            );
        }
    }

    #[test]
    fn options_are_complete_and_unambiguous() {
        let mut public_keys = std::collections::BTreeSet::new();
        let mut environment_variables = std::collections::BTreeSet::new();

        for entry in CONFIG_ENTRIES {
            assert!(entry.key.starts_with("gateway."), "{entry:?}");
            assert!(
                public_keys.insert(entry.key),
                "duplicate key: {}",
                entry.key
            );
            assert!(
                environment_variables.insert(environment_variable(entry.key)),
                "duplicate environment variable for {}",
                entry.key
            );
        }
    }
}
