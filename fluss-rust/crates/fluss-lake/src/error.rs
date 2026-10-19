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

//! Errors returned by bounded UnionRead planning and execution.

use fluss::error::Error as ClientError;
use thiserror::Error;

/// Result type returned by UnionRead planning and execution APIs.
pub type Result<T> = std::result::Result<T, FlussLakeError>;

/// Errors surfaced by the UnionRead planning and execution contract.
///
/// Match variants to classify failures; diagnostic messages are not a stable
/// protocol. Boundary conversions include available cause messages, but do not
/// retain the original error objects.
#[derive(Debug, Error)]
pub enum FlussLakeError {
    /// Requested table is not configured for lake reads.
    #[error("table is not lake-readable: {0}")]
    NotLakeReadable(String),

    /// Invalid read configuration or failure to construct the default read plan.
    #[error("UnionRead planning failed: {0}")]
    PlanningFailed(String),

    /// A frozen snapshot, partition or required log range is missing or unreadable.
    #[error("UnionRead data unavailable: {0}")]
    DataUnavailable(String),

    /// Table identity, schema or layout is incompatible with the frozen read inputs.
    #[error("UnionRead schema incompatible: {0}")]
    SchemaIncompatible(String),

    /// Access to Fluss RPC or the lake storage/catalog failed.
    #[error("UnionRead connection error: {0}")]
    ConnectionError(String),

    /// Merge engine is not supported by the default PK tail reconciliation.
    #[error("unsupported merge engine: {0}")]
    UnsupportedMergeEngine(String),

    /// Split or backend payload version is not supported by this reader.
    #[error("incompatible UnionRead split version: {0}")]
    IncompatibleSplitVersion(String),

    /// Public engine-integration context is malformed or incomplete.
    #[error("invalid UnionRead context: {0}")]
    InvalidReadContext(String),

    /// Public context transport version is not supported.
    #[error("incompatible UnionRead context version: {0}")]
    IncompatibleReadContextVersion(String),

    /// Backend or execution failure outside the explicitly classified categories,
    /// including malformed task payloads. Returned to callers like other read errors.
    #[error("UnionRead internal error: {0}")]
    Internal(String),
}

/// Classifies metadata failures consistently for table opening and planning.
pub(crate) fn planning_client_error(action: &str, error: ClientError) -> FlussLakeError {
    let message = error_message(action, &error);
    match error {
        ClientError::RpcError { .. } => FlussLakeError::ConnectionError(message),
        _ => FlussLakeError::PlanningFailed(message),
    }
}

/// Retains cause messages in diagnostics without changing the public error variants.
pub(crate) fn error_message(action: &str, error: &dyn std::error::Error) -> String {
    let mut message = format!("failed to {action}: {error}");
    let mut source = error.source();
    while let Some(cause) = source {
        let detail = cause.to_string();
        if !detail.is_empty() && !message.contains(&detail) {
            message.push_str(": ");
            message.push_str(&detail);
        }
        source = cause.source();
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Error)]
    enum ReadError {
        #[error("reader failed")]
        Hidden(#[source] std::io::Error),
        #[error("reader failed: {0}")]
        Included(#[source] std::io::Error),
    }

    #[test]
    fn diagnostic_preserves_causes_without_repeating_displayed_messages() {
        assert_eq!(
            error_message("read frozen task", &std::io::Error::other("reader failed")),
            "failed to read frozen task: reader failed"
        );
        for error in [
            ReadError::Hidden(std::io::Error::other("underlying failure")),
            ReadError::Included(std::io::Error::other("underlying failure")),
        ] {
            assert_eq!(
                error_message("read frozen task", &error),
                "failed to read frozen task: reader failed: underlying failure"
            );
        }
    }
}
