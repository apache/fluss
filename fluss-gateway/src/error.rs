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

//! Gateway error taxonomy and the REST error envelope.
//!
//! [`ErrorKind`] represents client-visible failure conditions. Its variants, HTTP statuses, wire codes, and
//! `Retry-After` rule are declared once by the `error_kinds!` table below — the table-driven form
//! `http::StatusCode` uses for the same problem — so a new condition cannot be added to one mapping and
//! forgotten in another.
//!
//! The wire vocabulary has a single source too: [`wire_codes`] enumerates every code the gateway can emit,
//! and the OpenAPI `ErrorCode` schema is generated from it, so the published contract cannot drift from the
//! taxonomy.
//!
//! `database_not_empty` (409) uses [`ErrorKind::FailedPrecondition`]. The `*_not_found` and
//! `*_already_exists` families use [`ErrorKind::NotFound`] and [`ErrorKind::AlreadyExists`]
//! qualified by a [`Resource`].

use serde::Serialize;
use std::any::Any;
use std::fmt;
use utoipa::openapi::schema::Type;
use utoipa::openapi::{ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

/// Declares the error taxonomy: one row per condition, carrying its HTTP status, its stable wire code, and
/// whether a response advertises `Retry-After`.
///
/// Generating [`ErrorKind`] and its mappings from the same rows keeps them in step by construction: the
/// generated matches are exhaustive, so a row is the only way to add a variant.
macro_rules! error_kinds {
    (
        $(
            $(#[$docs:meta])*
            $variant:ident => $status:literal, $code:literal, retry_after: $retry_after:literal;
        )+
    ) => {
        /// Client-visible condition kinds, ordered by HTTP status.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum ErrorKind {
            $(
                $(#[$docs])*
                $variant,
            )+
        }

        impl ErrorKind {
            /// Every kind, in declaration order.
            pub const ALL: &'static [ErrorKind] = &[$( ErrorKind::$variant, )+];

            /// Stable machine-readable code carried in the error envelope, for example `not_found`.
            pub fn code(self) -> &'static str {
                match self {
                    $( Self::$variant => $code, )+
                }
            }

            /// The REST HTTP mapping.
            ///
            /// Kept as a plain `u16` so this module stays free of HTTP framework types. The REST adapter
            /// converts to its own status type.
            pub fn http_status(self) -> u16 {
                match self {
                    $( Self::$variant => $status, )+
                }
            }

            /// Whether responses advertise `Retry-After`: all 429s and transient backend outages.
            pub fn retry_after(self) -> bool {
                match self {
                    $( Self::$variant => $retry_after, )+
                }
            }
        }
    };
}

error_kinds! {
    /// The request contains malformed input, an invalid identifier, or a type mismatch.
    InvalidArgument => 400, "invalid_argument", retry_after: false;
    /// The request carries no usable credential, or the credential failed verification.
    Unauthenticated => 401, "unauthenticated", retry_after: false;
    /// The authenticated principal is not allowed to perform the operation.
    Unauthorized => 403, "unauthorized", retry_after: false;
    /// The requested cluster, database, table, or partition does not exist.
    NotFound => 404, "not_found", retry_after: false;
    /// The route exists but not for the request method.
    MethodNotAllowed => 405, "method_not_allowed", retry_after: false;
    /// The `Accept` header does not allow a supported response type.
    NotAcceptable => 406, "not_acceptable", retry_after: false;
    /// A create operation conflicts with an existing resource.
    AlreadyExists => 409, "already_exists", retry_after: false;
    /// Current resource state prevents the requested operation.
    FailedPrecondition => 409, "failed_precondition", retry_after: false;
    /// The request exceeds a configured input-validation size limit.
    LimitExceeded => 413, "limit_exceeded", retry_after: false;
    /// The request media type is not supported.
    UnsupportedMediaType => 415, "unsupported_media_type", retry_after: false;
    /// A bounded resource (per-user act-as connections) is at capacity.
    ResourceExhausted => 429, "resource_exhausted", retry_after: true;
    /// A KV write the store refused under backpressure after client retries.
    StorageBackpressure => 429, "storage_backpressure", retry_after: true;
    /// Work was cancelled by the caller or by shutdown.
    Cancelled => 499, "cancelled", retry_after: false;
    /// The Fluss backend failed in a way the gateway cannot classify further, distinguishable from a
    /// gateway-internal failure.
    Backend => 500, "backend", retry_after: false;
    /// An unexpected internal failure occurred.
    Internal => 500, "internal", retry_after: false;
    /// The operation or table format is not supported.
    Unsupported => 501, "unsupported", retry_after: false;
    /// The backend is unavailable or the gateway is not ready.
    Unavailable => 503, "unavailable", retry_after: true;
    /// The request exceeded its deadline.
    DeadlineExceeded => 504, "timeout", retry_after: false;
}

/// A resource a failure can name, selecting a resource-specific wire code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Resource {
    Cluster,
    Database,
    Table,
    Partition,
}

/// Resource-specific codes; unlisted pairs keep the kind's generic code.
const RESOURCE_CODES: &[(ErrorKind, Resource, &str)] = &[
    (ErrorKind::NotFound, Resource::Cluster, "cluster_not_found"),
    (
        ErrorKind::NotFound,
        Resource::Database,
        "database_not_found",
    ),
    (ErrorKind::NotFound, Resource::Table, "table_not_found"),
    (
        ErrorKind::NotFound,
        Resource::Partition,
        "partition_not_found",
    ),
    (
        ErrorKind::AlreadyExists,
        Resource::Cluster,
        "cluster_already_exists",
    ),
    (
        ErrorKind::AlreadyExists,
        Resource::Database,
        "database_already_exists",
    ),
    (
        ErrorKind::AlreadyExists,
        Resource::Table,
        "table_already_exists",
    ),
    (
        ErrorKind::AlreadyExists,
        Resource::Partition,
        "partition_already_exists",
    ),
    // Dropping a non-empty database has its own precondition code.
    (
        ErrorKind::FailedPrecondition,
        Resource::Database,
        "database_not_empty",
    ),
];

fn resource_code(kind: ErrorKind, resource: Resource) -> Option<&'static str> {
    RESOURCE_CODES
        .iter()
        .find(|(row_kind, row_resource, _)| *row_kind == kind && *row_resource == resource)
        .map(|(_, _, code)| *code)
}

/// Every code the gateway can put on the wire, sorted and deduplicated.
///
/// The OpenAPI `ErrorCode` schema is built from this, so the published vocabulary is the taxonomy itself
/// rather than a hand-maintained copy of it.
pub fn wire_codes() -> Vec<&'static str> {
    let mut codes: Vec<&'static str> = ErrorKind::ALL.iter().map(|kind| kind.code()).collect();
    codes.extend(RESOURCE_CODES.iter().map(|(_, _, code)| *code));
    codes.sort_unstable();
    codes.dedup();
    codes
}

/// Schema handle for the `code` field: a string restricted to [`wire_codes`].
///
/// A marker type rather than a mirrored enum, so the documented vocabulary is generated from the taxonomy
/// and cannot fall behind it.
#[derive(Debug)]
pub struct ErrorCode;

impl PartialSchema for ErrorCode {
    fn schema() -> RefOr<Schema> {
        ObjectBuilder::new()
            .schema_type(Type::String)
            .description(Some(
                "Stable error code, resource-specific where the error names a resource.",
            ))
            .enum_values(Some(wire_codes()))
            .into()
    }
}

impl ToSchema for ErrorCode {}

/// The result of any gateway operation that can fail with a client-visible condition.
pub type GatewayResult<T> = Result<T, GatewayError>;

/// Gateway-internal error: a condition kind plus a client-safe message.
///
/// Messages must never contain stack traces, internal addresses, or wire payloads. Operational detail belongs
/// in the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayError {
    kind: ErrorKind,
    message: String,
    /// Selects a resource-specific wire code without adding a field to the error envelope.
    resource: Option<Resource>,
}

impl GatewayError {
    /// The message reaches the client verbatim, so keep it free of internal detail.
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            resource: None,
        }
    }

    /// A malformed or rejected request argument. Answered with HTTP 400.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, message)
    }

    /// A request without a usable credential, or whose credential failed verification. Answered with HTTP 401.
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unauthenticated, message)
    }

    /// An operation the authenticated principal is not allowed to perform. Answered with HTTP 403.
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unauthorized, message)
    }

    /// A named database, table, or partition that does not exist. Answered with HTTP 404.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, message)
    }

    /// A create operation targeting a resource that already exists.
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::AlreadyExists, message)
    }

    /// An operation rejected because the current resource state does not permit it.
    pub fn failed_precondition(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::FailedPrecondition, message)
    }

    /// An operation or table format the gateway does not implement. Answered with HTTP 501.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unsupported, message)
    }

    /// A request-size or configured input-validation limit was exceeded. Answered with HTTP 413.
    pub fn limit_exceeded(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::LimitExceeded, message)
    }

    /// A bounded resource, such as the per-user act-as connection pool, is at capacity.
    /// Answered with HTTP 429 and a `Retry-After` header.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::ResourceExhausted, message)
    }

    /// The request ran past its deadline. Answered with HTTP 504.
    pub fn deadline_exceeded(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::DeadlineExceeded, message)
    }

    /// Work cancelled by its caller or by gateway shutdown.
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Cancelled, message)
    }

    /// Creates a transient backend-unavailable error.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unavailable, message)
    }

    /// An unclassified Fluss failure, returned as HTTP 500 with the `backend` code.
    pub fn backend(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Backend, message)
    }

    /// An unexpected failure with no better classification. Answered with HTTP 500 and logged.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Internal, message)
    }

    /// The condition this error represents, which decides the HTTP status and the envelope code.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the safe client-facing message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Stable code carried in the error envelope.
    ///
    /// Unlisted kind/resource pairs fall back to the kind's generic code.
    pub fn code(&self) -> &'static str {
        self.resource
            .and_then(|resource| resource_code(self.kind, resource))
            .unwrap_or_else(|| self.kind.code())
    }

    pub(crate) fn resource(&self) -> Option<Resource> {
        self.resource
    }

    /// Names the resource this error is about, selecting the resource-specific code.
    ///
    /// The metadata and DDL capabilities are the first emitters; the vocabulary ships with the wire contract.
    pub fn with_resource(mut self, resource: Resource) -> Self {
        self.resource = Some(resource);
        self
    }
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code(), self.message)
    }
}

impl std::error::Error for GatewayError {}

/// Renders a caught panic payload as a log message, for the callers that report a panic as a named
/// failure instead of losing it.
pub(crate) fn panic_message(payload: Box<dyn Any + Send>) -> String {
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(payload) => match payload.downcast::<&'static str>() {
            Ok(message) => (*message).to_string(),
            Err(_) => "non-string panic payload".to_string(),
        },
    }
}

/// REST error envelope: `{"error": {"code", "message", "request_id"}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
#[schema(examples(json!({
    "error": {
        "code": "table_not_found",
        "message": "table `mydb.orders` does not exist",
        "request_id": "8f6c7f4a-f9b8-4c71-91ec-6e5578d7a913"
    }
})))]
pub struct ErrorEnvelope {
    pub error: ErrorBody,
}

/// Body of the REST error envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, ToSchema)]
pub struct ErrorBody {
    #[schema(value_type = ErrorCode)]
    pub code: String,
    pub message: String,
    /// Correlates the response with the `x-request-id` header and the access log.
    #[schema(value_type = String, format = "uuid")]
    pub request_id: String,
}

impl ErrorEnvelope {
    /// Builds a public error envelope with the correlated request ID.
    pub fn new(error: &GatewayError, request_id: impl Into<String>) -> Self {
        Self {
            error: ErrorBody {
                code: error.code().to_string(),
                message: error.message().to_string(),
                request_id: request_id.into(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keeps published status and error-code mappings stable.
    #[test]
    fn http_status_and_error_code_mappings_are_stable() {
        for (kind, status, code) in [
            (ErrorKind::InvalidArgument, 400, "invalid_argument"),
            (ErrorKind::Unauthenticated, 401, "unauthenticated"),
            (ErrorKind::Unauthorized, 403, "unauthorized"),
            (ErrorKind::NotFound, 404, "not_found"),
            (ErrorKind::MethodNotAllowed, 405, "method_not_allowed"),
            (ErrorKind::LimitExceeded, 413, "limit_exceeded"),
            (ErrorKind::ResourceExhausted, 429, "resource_exhausted"),
            (ErrorKind::Cancelled, 499, "cancelled"),
            (ErrorKind::Backend, 500, "backend"),
            (ErrorKind::Internal, 500, "internal"),
            (ErrorKind::Unsupported, 501, "unsupported"),
            (ErrorKind::Unavailable, 503, "unavailable"),
            (ErrorKind::DeadlineExceeded, 504, "timeout"),
        ] {
            assert_eq!(kind.http_status(), status, "status for {code}");
            assert_eq!(kind.code(), code);
        }
    }

    #[test]
    fn the_taxonomy_table_is_well_formed() {
        let mut codes: Vec<&str> = Vec::new();
        for kind in ErrorKind::ALL.iter().copied() {
            let code = kind.code();
            assert!(
                (400..=599).contains(&kind.http_status()),
                "{code} maps to {}",
                kind.http_status()
            );
            assert!(
                code.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "{code} is not snake_case"
            );
            // `Retry-After` is meaningful only where the caller is meant to come back.
            assert_eq!(
                kind.retry_after(),
                matches!(
                    kind,
                    ErrorKind::ResourceExhausted
                        | ErrorKind::StorageBackpressure
                        | ErrorKind::Unavailable
                ),
                "retry_after for {code}"
            );
            codes.push(code);
        }
        let total = codes.len();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), total, "duplicate wire code declared");
    }

    #[test]
    fn envelope_shape() {
        let error = GatewayError::not_found("table `db.missing` does not exist");
        let envelope = ErrorEnvelope::new(&error, "req-123");
        assert_eq!(
            serde_json::to_value(&envelope).unwrap(),
            serde_json::json!({
                "error": {
                    "code": "not_found",
                    "message": "table `db.missing` does not exist",
                    "request_id": "req-123",
                }
            })
        );
    }

    /// An error naming a resource uses its specific code, falling back to the generic code otherwise.
    #[test]
    fn resource_context_specialises_the_wire_code() {
        let cases: [(GatewayError, Resource, &str); 5] = [
            (
                GatewayError::not_found("x"),
                Resource::Cluster,
                "cluster_not_found",
            ),
            (
                GatewayError::not_found("x"),
                Resource::Database,
                "database_not_found",
            ),
            (
                GatewayError::not_found("x"),
                Resource::Table,
                "table_not_found",
            ),
            (
                GatewayError::already_exists("x"),
                Resource::Partition,
                "partition_already_exists",
            ),
            (
                GatewayError::failed_precondition("x"),
                Resource::Database,
                "database_not_empty",
            ),
        ];
        for (error, resource, expected) in cases {
            let named = error.with_resource(resource);
            assert_eq!(named.code(), expected);
            let envelope = serde_json::to_value(ErrorEnvelope::new(&named, "r")).unwrap();
            assert_eq!(envelope["error"]["code"], expected);
        }

        // Without a resource the generic codes hold — the gateway never guesses.
        assert_eq!(GatewayError::not_found("x").code(), "not_found");
        assert_eq!(GatewayError::already_exists("x").code(), "already_exists");
        // A precondition on a table (e.g. it changed during preflight) is not "not empty".
        assert_eq!(
            GatewayError::failed_precondition("x")
                .with_resource(Resource::Table)
                .code(),
            "failed_precondition"
        );
        // Backend and internal failures stay distinguishable.
        assert_eq!(GatewayError::backend("x").code(), "backend");
        assert_eq!(GatewayError::internal("x").code(), "internal");
    }

    /// Everything the taxonomy can emit is publishable, and nothing else is.
    #[test]
    fn wire_codes_cover_the_kinds_and_the_resource_forms() {
        let codes = wire_codes();
        for kind in ErrorKind::ALL.iter().copied() {
            assert!(codes.contains(&kind.code()), "missing {}", kind.code());
        }
        for (_, _, code) in RESOURCE_CODES {
            assert!(codes.contains(code), "missing {code}");
        }
        assert_eq!(
            codes.len(),
            ErrorKind::ALL.len() + RESOURCE_CODES.len(),
            "wire codes are unique across kinds and resource forms"
        );
        assert!(codes.windows(2).all(|pair| pair[0] < pair[1]), "sorted");
    }
}
