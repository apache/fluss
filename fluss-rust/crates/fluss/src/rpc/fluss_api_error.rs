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

use crate::proto::ErrorResponse;
use std::fmt::{Debug, Display, Formatter};

/// API error response from Fluss server
pub struct ApiError {
    pub code: i32,
    pub message: String,
}

impl Debug for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiError")
            .field("code", &self.code)
            .field("message", &self.message)
            .finish()
    }
}

impl Display for ApiError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl ApiError {
    /// Returns `true` if retrying the request may succeed. Delegates to [`FlussError::is_retriable`].
    pub fn is_retriable(&self) -> bool {
        FlussError::for_code(self.code).is_retriable()
    }
}

/// Fluss protocol errors. These errors are part of the client-server protocol.
/// The error codes cannot be changed, but the names can be.
///
/// Do not add exceptions that occur only on the client or only on the server here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum FlussError {
    /// The server experienced an unexpected error when processing the request.
    UnknownServerError = -1,
    /// No error occurred.
    None = 0,
    /// The server disconnected before a response was received.
    NetworkException = 1,
    /// The version of API is not supported.
    UnsupportedVersion = 2,
    /// This message has failed its CRC checksum, exceeds the valid size, has a null key for a primary key table, or is otherwise corrupt.
    CorruptMessage = 3,
    /// The database does not exist.
    DatabaseNotExist = 4,
    /// The database is not empty.
    DatabaseNotEmpty = 5,
    /// The database already exists.
    DatabaseAlreadyExist = 6,
    /// The table does not exist.
    TableNotExist = 7,
    /// The table already exists.
    TableAlreadyExist = 8,
    /// The schema does not exist.
    SchemaNotExist = 9,
    /// Exception occur while storage data for log in server.
    LogStorageException = 10,
    /// Exception occur while storage data for kv in server.
    KvStorageException = 11,
    /// Not leader or follower.
    NotLeaderOrFollower = 12,
    /// The record is too large.
    RecordTooLargeException = 13,
    /// The record is corrupt.
    CorruptRecordException = 14,
    /// The client has attempted to perform an operation on an invalid table.
    InvalidTableException = 15,
    /// The client has attempted to perform an operation on an invalid database.
    InvalidDatabaseException = 16,
    /// The replication factor is larger then the number of available tablet servers.
    InvalidReplicationFactor = 17,
    /// Produce request specified an invalid value for required acks.
    InvalidRequiredAcks = 18,
    /// The log offset is out of range.
    LogOffsetOutOfRangeException = 19,
    /// The table is not primary key table.
    NonPrimaryKeyTableException = 20,
    /// The table or bucket does not exist.
    UnknownTableOrBucketException = 21,
    /// The update version is invalid.
    InvalidUpdateVersionException = 22,
    /// The coordinator is invalid.
    InvalidCoordinatorException = 23,
    /// The leader epoch is invalid.
    FencedLeaderEpochException = 24,
    /// The request time out.
    RequestTimeOut = 25,
    /// The general storage exception.
    StorageException = 26,
    /// The server did not attempt to execute this operation.
    OperationNotAttemptedException = 27,
    /// Records are written to the server already, but to fewer in-sync replicas than required.
    NotEnoughReplicasAfterAppendException = 28,
    /// Messages are rejected since there are fewer in-sync replicas than required.
    NotEnoughReplicasException = 29,
    /// Get file access security token exception.
    SecurityTokenException = 30,
    /// The tablet server received an out of order sequence batch.
    OutOfOrderSequenceException = 31,
    /// The tablet server received a duplicate sequence batch.
    DuplicateSequenceException = 32,
    /// This exception is raised by the tablet server if it could not locate the writer metadata.
    UnknownWriterIdException = 33,
    /// The requested column projection is invalid.
    InvalidColumnProjection = 34,
    /// The requested target column to write is invalid.
    InvalidTargetColumn = 35,
    /// The partition does not exist.
    PartitionNotExists = 36,
    /// The table is not partitioned.
    TableNotPartitionedException = 37,
    /// The timestamp is invalid.
    InvalidTimestampException = 38,
    /// The config is invalid.
    InvalidConfigException = 39,
    /// The lake storage is not configured.
    LakeStorageNotConfiguredException = 40,
    /// The kv snapshot is not exist.
    KvSnapshotNotExist = 41,
    /// The partition already exists.
    PartitionAlreadyExists = 42,
    /// The partition spec is invalid.
    PartitionSpecInvalidException = 43,
    /// There is no currently available leader for the given partition.
    LeaderNotAvailableException = 44,
    /// Exceed the maximum number of partitions.
    PartitionMaxNumException = 45,
    /// Authentication failed.
    AuthenticateException = 46,
    /// Security is disabled.
    SecurityDisabledException = 47,
    /// Authorization failed.
    AuthorizationException = 48,
    /// Exceed the maximum number of buckets.
    BucketMaxNumException = 49,
    /// The tiering epoch is invalid.
    FencedTieringEpochException = 50,
    /// Authentication failed with retriable exception.
    RetriableAuthenticateException = 51,
    /// The server rack info is invalid.
    InvalidServerRackInfoException = 52,
    /// The lake snapshot is not exist.
    LakeSnapshotNotExist = 53,
    /// The lake table already exists.
    LakeTableAlreadyExist = 54,
    /// The new ISR contains at least one ineligible replica.
    IneligibleReplicaException = 55,
    /// The alter table is invalid.
    InvalidAlterTableException = 56,
    /// Deletion operations are disabled on this table.
    DeletionDisabledException = 57,
}

impl FlussError {
    /// Returns the error code for this error.
    pub fn code(&self) -> i32 {
        *self as i32
    }

    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            FlussError::NetworkException
                | FlussError::CorruptMessage
                | FlussError::SchemaNotExist
                | FlussError::LogStorageException
                | FlussError::KvStorageException
                | FlussError::NotLeaderOrFollower
                | FlussError::CorruptRecordException
                | FlussError::UnknownTableOrBucketException
                | FlussError::RequestTimeOut
                | FlussError::StorageException
                | FlussError::NotEnoughReplicasAfterAppendException
                | FlussError::NotEnoughReplicasException
                | FlussError::LeaderNotAvailableException
        )
    }

    /// Returns a friendly description of the error.
    pub fn message(&self) -> &'static str {
        match self {
            FlussError::UnknownServerError => {
                "The server experienced an unexpected error when processing the request."
            }
            FlussError::None => "No error",
            FlussError::NetworkException => {
                "The server disconnected before a response was received."
            }
            FlussError::UnsupportedVersion => "The version of API is not supported.",
            FlussError::CorruptMessage => {
                "This message has failed its CRC checksum, exceeds the valid size, has a null key for a primary key table, or is otherwise corrupt."
            }
            FlussError::DatabaseNotExist => "The database does not exist.",
            FlussError::DatabaseNotEmpty => "The database is not empty.",
            FlussError::DatabaseAlreadyExist => "The database already exists.",
            FlussError::TableNotExist => "The table does not exist.",
            FlussError::TableAlreadyExist => "The table already exists.",
            FlussError::SchemaNotExist => "The schema does not exist.",
            FlussError::LogStorageException => {
                "Exception occur while storage data for log in server."
            }
            FlussError::KvStorageException => {
                "Exception occur while storage data for kv in server."
            }
            FlussError::NotLeaderOrFollower => "Not leader or follower.",
            FlussError::RecordTooLargeException => "The record is too large.",
            FlussError::CorruptRecordException => "The record is corrupt.",
            FlussError::InvalidTableException => {
                "The client has attempted to perform an operation on an invalid table."
            }
            FlussError::InvalidDatabaseException => {
                "The client has attempted to perform an operation on an invalid database."
            }
            FlussError::InvalidReplicationFactor => {
                "The replication factor is larger then the number of available tablet servers."
            }
            FlussError::InvalidRequiredAcks => {
                "Produce request specified an invalid value for required acks."
            }
            FlussError::LogOffsetOutOfRangeException => "The log offset is out of range.",
            FlussError::NonPrimaryKeyTableException => "The table is not primary key table.",
            FlussError::UnknownTableOrBucketException => "The table or bucket does not exist.",
            FlussError::InvalidUpdateVersionException => "The update version is invalid.",
            FlussError::InvalidCoordinatorException => "The coordinator is invalid.",
            FlussError::FencedLeaderEpochException => "The leader epoch is invalid.",
            FlussError::RequestTimeOut => "The request time out.",
            FlussError::StorageException => "The general storage exception.",
            FlussError::OperationNotAttemptedException => {
                "The server did not attempt to execute this operation."
            }
            FlussError::NotEnoughReplicasAfterAppendException => {
                "Records are written to the server already, but to fewer in-sync replicas than required."
            }
            FlussError::NotEnoughReplicasException => {
                "Messages are rejected since there are fewer in-sync replicas than required."
            }
            FlussError::SecurityTokenException => "Get file access security token exception.",
            FlussError::OutOfOrderSequenceException => {
                "The tablet server received an out of order sequence batch."
            }
            FlussError::DuplicateSequenceException => {
                "The tablet server received a duplicate sequence batch."
            }
            FlussError::UnknownWriterIdException => {
                "This exception is raised by the tablet server if it could not locate the writer metadata."
            }
            FlussError::InvalidColumnProjection => "The requested column projection is invalid.",
            FlussError::InvalidTargetColumn => "The requested target column to write is invalid.",
            FlussError::PartitionNotExists => "The partition does not exist.",
            FlussError::TableNotPartitionedException => "The table is not partitioned.",
            FlussError::InvalidTimestampException => "The timestamp is invalid.",
            FlussError::InvalidConfigException => "The config is invalid.",
            FlussError::LakeStorageNotConfiguredException => "The lake storage is not configured.",
            FlussError::KvSnapshotNotExist => "The kv snapshot does not exist.",
            FlussError::PartitionAlreadyExists => "The partition already exists.",
            FlussError::PartitionSpecInvalidException => "The partition spec is invalid.",
            FlussError::LeaderNotAvailableException => {
                "There is no currently available leader for the given partition."
            }
            FlussError::PartitionMaxNumException => "Exceed the maximum number of partitions.",
            FlussError::AuthenticateException => "Authentication failed.",
            FlussError::SecurityDisabledException => "Security is disabled.",
            FlussError::AuthorizationException => "Authorization failed.",
            FlussError::BucketMaxNumException => "Exceed the maximum number of buckets.",
            FlussError::FencedTieringEpochException => "The tiering epoch is invalid.",
            FlussError::RetriableAuthenticateException => {
                "Authentication failed with retriable exception."
            }
            FlussError::InvalidServerRackInfoException => "The server rack info is invalid.",
            FlussError::LakeSnapshotNotExist => "The lake snapshot does not exist.",
            FlussError::LakeTableAlreadyExist => "The lake table already exists.",
            FlussError::IneligibleReplicaException => {
                "The new ISR contains at least one ineligible replica."
            }
            FlussError::InvalidAlterTableException => "The alter table is invalid.",
            FlussError::DeletionDisabledException => {
                "Deletion operations are disabled on this table."
            }
        }
    }

    /// Create an ApiError from this error with the default message.
    pub fn to_api_error(&self, message: Option<String>) -> ApiError {
        ApiError {
            code: self.code(),
            message: message.unwrap_or(self.message().to_string()),
        }
    }

    /// Get the FlussError for the given error code.
    /// Returns `UnknownServerError` if the code is not recognized.
    pub fn for_code(code: i32) -> Self {
        match code {
            -1 => FlussError::UnknownServerError,
            0 => FlussError::None,
            1 => FlussError::NetworkException,
            2 => FlussError::UnsupportedVersion,
            3 => FlussError::CorruptMessage,
            4 => FlussError::DatabaseNotExist,
            5 => FlussError::DatabaseNotEmpty,
            6 => FlussError::DatabaseAlreadyExist,
            7 => FlussError::TableNotExist,
            8 => FlussError::TableAlreadyExist,
            9 => FlussError::SchemaNotExist,
            10 => FlussError::LogStorageException,
            11 => FlussError::KvStorageException,
            12 => FlussError::NotLeaderOrFollower,
            13 => FlussError::RecordTooLargeException,
            14 => FlussError::CorruptRecordException,
            15 => FlussError::InvalidTableException,
            16 => FlussError::InvalidDatabaseException,
            17 => FlussError::InvalidReplicationFactor,
            18 => FlussError::InvalidRequiredAcks,
            19 => FlussError::LogOffsetOutOfRangeException,
            20 => FlussError::NonPrimaryKeyTableException,
            21 => FlussError::UnknownTableOrBucketException,
            22 => FlussError::InvalidUpdateVersionException,
            23 => FlussError::InvalidCoordinatorException,
            24 => FlussError::FencedLeaderEpochException,
            25 => FlussError::RequestTimeOut,
            26 => FlussError::StorageException,
            27 => FlussError::OperationNotAttemptedException,
            28 => FlussError::NotEnoughReplicasAfterAppendException,
            29 => FlussError::NotEnoughReplicasException,
            30 => FlussError::SecurityTokenException,
            31 => FlussError::OutOfOrderSequenceException,
            32 => FlussError::DuplicateSequenceException,
            33 => FlussError::UnknownWriterIdException,
            34 => FlussError::InvalidColumnProjection,
            35 => FlussError::InvalidTargetColumn,
            36 => FlussError::PartitionNotExists,
            37 => FlussError::TableNotPartitionedException,
            38 => FlussError::InvalidTimestampException,
            39 => FlussError::InvalidConfigException,
            40 => FlussError::LakeStorageNotConfiguredException,
            41 => FlussError::KvSnapshotNotExist,
            42 => FlussError::PartitionAlreadyExists,
            43 => FlussError::PartitionSpecInvalidException,
            44 => FlussError::LeaderNotAvailableException,
            45 => FlussError::PartitionMaxNumException,
            46 => FlussError::AuthenticateException,
            47 => FlussError::SecurityDisabledException,
            48 => FlussError::AuthorizationException,
            49 => FlussError::BucketMaxNumException,
            50 => FlussError::FencedTieringEpochException,
            51 => FlussError::RetriableAuthenticateException,
            52 => FlussError::InvalidServerRackInfoException,
            53 => FlussError::LakeSnapshotNotExist,
            54 => FlussError::LakeTableAlreadyExist,
            55 => FlussError::IneligibleReplicaException,
            56 => FlussError::InvalidAlterTableException,
            57 => FlussError::DeletionDisabledException,
            _ => FlussError::UnknownServerError,
        }
    }
}

impl Display for FlussError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl From<ErrorResponse> for ApiError {
    fn from(error_response: ErrorResponse) -> Self {
        let code = error_response.error_code;
        let message = error_response
            .error_message
            .unwrap_or_else(|| FlussError::for_code(code).message().to_string());
        ApiError { code, message }
    }
}

impl From<ApiError> for FlussError {
    fn from(api_error: ApiError) -> Self {
        FlussError::for_code(api_error.code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_code_maps_known_and_unknown() {
        assert_eq!(FlussError::for_code(0), FlussError::None);
        assert_eq!(
            FlussError::for_code(FlussError::AuthorizationException.code()),
            FlussError::AuthorizationException
        );
        assert_eq!(FlussError::for_code(9999), FlussError::UnknownServerError);
    }

    #[test]
    fn to_api_error_uses_message() {
        let err = FlussError::InvalidTableException.to_api_error(None);
        assert_eq!(err.code, FlussError::InvalidTableException.code());
        assert!(err.message.contains("invalid table"));
    }

    #[test]
    fn error_response_conversion_round_trip() {
        let response = ErrorResponse {
            error_code: FlussError::TableNotExist.code(),
            error_message: Some("missing".to_string()),
        };
        let api_error = ApiError::from(response);
        assert_eq!(api_error.code, FlussError::TableNotExist.code());
        assert_eq!(api_error.message, "missing");
        let fluss_error = FlussError::from(api_error);
        assert_eq!(fluss_error, FlussError::TableNotExist);
    }

    #[test]
    fn error_response_preserves_unknown_wire_code() {
        let response = ErrorResponse {
            error_code: 9999,
            error_message: Some("NewException: forward compat".to_string()),
        };
        let api_error = ApiError::from(response);
        assert_eq!(api_error.code, 9999);
        assert_eq!(api_error.message, "NewException: forward compat");
        assert_eq!(FlussError::from(api_error), FlussError::UnknownServerError);
    }

    #[test]
    fn error_response_falls_back_to_default_message_for_unknown_code() {
        let response = ErrorResponse {
            error_code: 9999,
            error_message: None,
        };
        let api_error = ApiError::from(response);
        assert_eq!(api_error.code, 9999);
        assert_eq!(api_error.message, FlussError::UnknownServerError.message());
        assert!(!api_error.is_retriable());
    }

    #[test]
    fn is_retriable_known_retriable_errors() {
        let retriable = [
            FlussError::NetworkException,
            FlussError::CorruptMessage,
            FlussError::SchemaNotExist,
            FlussError::LogStorageException,
            FlussError::KvStorageException,
            FlussError::NotLeaderOrFollower,
            FlussError::CorruptRecordException,
            FlussError::UnknownTableOrBucketException,
            FlussError::RequestTimeOut,
            FlussError::StorageException,
            FlussError::NotEnoughReplicasAfterAppendException,
            FlussError::NotEnoughReplicasException,
            FlussError::LeaderNotAvailableException,
        ];
        for err in &retriable {
            assert!(err.is_retriable(), "{err:?} should be retriable");
        }
    }

    #[test]
    fn is_retriable_known_non_retriable_errors() {
        let non_retriable = [
            FlussError::UnknownServerError,
            FlussError::None,
            FlussError::TableNotExist,
            FlussError::AuthenticateException,
            FlussError::AuthorizationException,
            FlussError::RecordTooLargeException,
            FlussError::DeletionDisabledException,
            FlussError::InvalidCoordinatorException,
            FlussError::FencedLeaderEpochException,
            FlussError::FencedTieringEpochException,
            FlussError::RetriableAuthenticateException,
        ];
        for err in &non_retriable {
            assert!(!err.is_retriable(), "{err:?} should not be retriable");
        }
    }

    #[test]
    fn api_error_is_retriable_delegates_to_fluss_error() {
        let retriable_api = FlussError::RequestTimeOut.to_api_error(None);
        assert!(retriable_api.is_retriable());

        let permanent_api = FlussError::TableNotExist.to_api_error(None);
        assert!(!permanent_api.is_retriable());
    }
}
