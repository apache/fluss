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

//! Errors returned by UnionRead source preparation and context validation.

use fluss::error::Error as ClientError;
use thiserror::Error;

/// Result type returned by UnionRead APIs.
pub type Result<T> = std::result::Result<T, FlussLakeError>;

/// Errors surfaced by source preparation and context validation.
///
/// Match variants to classify failures; diagnostic messages are not a stable
/// protocol. Client failures retain their original error as the source.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FlussLakeError {
    /// Requested table configuration is unsupported by UnionRead.
    #[error("table is not lake-readable: {0}")]
    NotLakeReadable(String),

    /// Source metadata cannot form a valid read boundary.
    #[error("UnionRead planning failed: {0}")]
    PlanningFailed(String),

    /// A frozen snapshot, partition or required log range is missing or unreadable.
    #[error("UnionRead data unavailable: {0}")]
    DataUnavailable(String),

    /// Table identity, schema or layout is incompatible with the frozen read inputs.
    #[error("UnionRead schema incompatible: {0}")]
    SchemaIncompatible(String),

    /// A core client operation failed while preparing source boundaries.
    #[error("failed to {action}: {source}")]
    Client {
        action: String,
        #[source]
        source: Box<ClientError>,
    },

    /// Merge engine is unsupported by default PK tail reconciliation.
    #[error("unsupported merge engine: {0}")]
    UnsupportedMergeEngine(String),

    /// Backend or execution failure outside the classified categories.
    #[error("UnionRead internal error: {0}")]
    Internal(String),

    /// Access to the lake storage or catalog failed.
    #[error("UnionRead connection error: {0}")]
    ConnectionError(String),

    /// Lake backend payload version is unsupported by its reader.
    #[error("incompatible UnionRead split version: {0}")]
    IncompatibleSplitVersion(String),

    /// Public engine-integration context is malformed or incomplete.
    #[error("invalid UnionRead context: {0}")]
    InvalidReadContext(String),
}

pub(crate) fn planning_client_error(action: &str, error: ClientError) -> FlussLakeError {
    FlussLakeError::Client {
        action: action.to_string(),
        source: Box::new(error),
    }
}

/// Missing frozen inputs invalidate the attempt; other failures retain their category.
pub(crate) fn execution_client_error(action: &str, error: ClientError) -> FlussLakeError {
    let message = error_message(action, &error);
    if matches!(
        error.api_error(),
        Some(
            fluss::error::FlussError::TableNotExist
                | fluss::error::FlussError::UnknownTableOrBucketException
                | fluss::error::FlussError::PartitionNotExists
                | fluss::error::FlussError::LakeSnapshotNotExist
                | fluss::error::FlussError::KvSnapshotNotExist
                | fluss::error::FlussError::LogOffsetOutOfRangeException
        )
    ) {
        return FlussLakeError::DataUnavailable(message);
    }
    match error {
        ClientError::UnexpectedError {
            source: Some(source),
            ..
        } if source.downcast_ref::<ClientError>().is_some_and(|cause| {
            cause.api_error() == Some(fluss::error::FlussError::LogOffsetOutOfRangeException)
        }) =>
        {
            FlussLakeError::DataUnavailable(message)
        }
        error => planning_client_error(action, error),
    }
}
