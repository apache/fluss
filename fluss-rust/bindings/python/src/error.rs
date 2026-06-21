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

use fluss::error::Error;
use fluss::rpc::FlussError as CoreFlussError;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

/// Error code for client-side errors that did not originate from the server API protocol.
/// The value -2 is outside the server API error code range (-1 .. 57+), so it will never
/// collide with current or future API codes. Consistent with the CPP binding.
const CLIENT_ERROR_CODE: i32 = -2;

/// Fluss errors
#[pyclass(extends=PyException)]
#[derive(Debug, Clone)]
pub struct FlussError {
    #[pyo3(get)]
    pub message: String,
    #[pyo3(get)]
    pub error_code: i32,
}

#[pymethods]
impl FlussError {
    #[new]
    #[pyo3(signature = (message, error_code=-2))]
    fn new(message: String, error_code: i32) -> Self {
        Self {
            message,
            error_code,
        }
    }

    fn __str__(&self) -> String {
        if self.error_code != CLIENT_ERROR_CODE {
            format!("FlussError(code={}): {}", self.error_code, self.message)
        } else {
            format!("FlussError: {}", self.message)
        }
    }

    /// Returns ``True`` if retrying the request may succeed. Client-side errors always return ``False``.
    #[getter]
    fn is_retriable(&self) -> bool {
        if self.error_code == CLIENT_ERROR_CODE {
            return false;
        }
        CoreFlussError::for_code(self.error_code).is_retriable()
    }
}

impl FlussError {
    pub fn new_err(message: impl ToString) -> PyErr {
        PyErr::new::<FlussError, _>((message.to_string(), CLIENT_ERROR_CODE))
    }

    pub fn from_core_error(error: &Error) -> PyErr {
        // Transport failures map to `NetworkException` (Java parity,
        // retriable).
        let (msg, code) = match error {
            Error::FlussAPIError { api_error } => (api_error.message.clone(), api_error.code),
            Error::RpcError { .. } => (error.to_string(), CoreFlussError::NetworkException.code()),
            _ => (error.to_string(), CLIENT_ERROR_CODE),
        };
        PyErr::new::<FlussError, _>((msg, code))
    }
}

/// Named constants for Fluss API error codes.
///
/// Server API errors have error_code > 0 or == -1.
/// Client-side errors have error_code == CLIENT_ERROR (-2).
/// These constants match the Rust core FlussError enum and are stable across protocol versions.
/// New server error codes work automatically (error_code is a raw int, not a closed enum) —
/// these constants are convenience names, not an exhaustive list.
#[pyclass]
pub struct ErrorCode;

#[pymethods]
impl ErrorCode {
    /// Client-side error (not from server API protocol). Check the error message for details.
    #[classattr]
    const CLIENT_ERROR: i32 = -2;
    /// No error.
    #[classattr]
    const NONE: i32 = 0;
    /// The server experienced an unexpected error when processing the request.
    #[classattr]
    const UNKNOWN_SERVER_ERROR: i32 = -1;
    /// The server disconnected before a response was received.
    #[classattr]
    const NETWORK_EXCEPTION: i32 = 1;
    /// The version of API is not supported.
    #[classattr]
    const UNSUPPORTED_VERSION: i32 = 2;
    /// This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.
    #[classattr]
    const CORRUPT_MESSAGE: i32 = 3;
    /// The database does not exist.
    #[classattr]
    const DATABASE_NOT_EXIST: i32 = 4;
    /// The database is not empty.
    #[classattr]
    const DATABASE_NOT_EMPTY: i32 = 5;
    /// The database already exists.
    #[classattr]
    const DATABASE_ALREADY_EXIST: i32 = 6;
    /// The table does not exist.
    #[classattr]
    const TABLE_NOT_EXIST: i32 = 7;
    /// The table already exists.
    #[classattr]
    const TABLE_ALREADY_EXIST: i32 = 8;
    /// The schema does not exist.
    #[classattr]
    const SCHEMA_NOT_EXIST: i32 = 9;
    /// Exception occurred while storing data for log in server.
    #[classattr]
    const LOG_STORAGE_EXCEPTION: i32 = 10;
    /// Exception occurred while storing data for kv in server.
    #[classattr]
    const KV_STORAGE_EXCEPTION: i32 = 11;
    /// Not leader or follower.
    #[classattr]
    const NOT_LEADER_OR_FOLLOWER: i32 = 12;
    /// The record is too large.
    #[classattr]
    const RECORD_TOO_LARGE_EXCEPTION: i32 = 13;
    /// The record is corrupt.
    #[classattr]
    const CORRUPT_RECORD_EXCEPTION: i32 = 14;
    /// The client has attempted to perform an operation on an invalid table.
    #[classattr]
    const INVALID_TABLE_EXCEPTION: i32 = 15;
    /// The client has attempted to perform an operation on an invalid database.
    #[classattr]
    const INVALID_DATABASE_EXCEPTION: i32 = 16;
    /// The replication factor is larger than the number of available tablet servers.
    #[classattr]
    const INVALID_REPLICATION_FACTOR: i32 = 17;
    /// Produce request specified an invalid value for required acks.
    #[classattr]
    const INVALID_REQUIRED_ACKS: i32 = 18;
    /// The log offset is out of range.
    #[classattr]
    const LOG_OFFSET_OUT_OF_RANGE_EXCEPTION: i32 = 19;
    /// The table is not a primary key table.
    #[classattr]
    const NON_PRIMARY_KEY_TABLE_EXCEPTION: i32 = 20;
    /// The table or bucket does not exist.
    #[classattr]
    const UNKNOWN_TABLE_OR_BUCKET_EXCEPTION: i32 = 21;
    /// The update version is invalid.
    #[classattr]
    const INVALID_UPDATE_VERSION_EXCEPTION: i32 = 22;
    /// The coordinator is invalid.
    #[classattr]
    const INVALID_COORDINATOR_EXCEPTION: i32 = 23;
    /// The leader epoch is invalid.
    #[classattr]
    const FENCED_LEADER_EPOCH_EXCEPTION: i32 = 24;
    /// The request timed out.
    #[classattr]
    const REQUEST_TIME_OUT: i32 = 25;
    /// The general storage exception.
    #[classattr]
    const STORAGE_EXCEPTION: i32 = 26;
    /// The server did not attempt to execute this operation.
    #[classattr]
    const OPERATION_NOT_ATTEMPTED_EXCEPTION: i32 = 27;
    /// Records are written to the server already, but to fewer in-sync replicas than required.
    #[classattr]
    const NOT_ENOUGH_REPLICAS_AFTER_APPEND_EXCEPTION: i32 = 28;
    /// Messages are rejected since there are fewer in-sync replicas than required.
    #[classattr]
    const NOT_ENOUGH_REPLICAS_EXCEPTION: i32 = 29;
    /// Get file access security token exception.
    #[classattr]
    const SECURITY_TOKEN_EXCEPTION: i32 = 30;
    /// The tablet server received an out of order sequence batch.
    #[classattr]
    const OUT_OF_ORDER_SEQUENCE_EXCEPTION: i32 = 31;
    /// The tablet server received a duplicate sequence batch.
    #[classattr]
    const DUPLICATE_SEQUENCE_EXCEPTION: i32 = 32;
    /// The tablet server could not locate the writer metadata.
    #[classattr]
    const UNKNOWN_WRITER_ID_EXCEPTION: i32 = 33;
    /// The requested column projection is invalid.
    #[classattr]
    const INVALID_COLUMN_PROJECTION: i32 = 34;
    /// The requested target column to write is invalid.
    #[classattr]
    const INVALID_TARGET_COLUMN: i32 = 35;
    /// The partition does not exist.
    #[classattr]
    const PARTITION_NOT_EXISTS: i32 = 36;
    /// The table is not partitioned.
    #[classattr]
    const TABLE_NOT_PARTITIONED_EXCEPTION: i32 = 37;
    /// The timestamp is invalid.
    #[classattr]
    const INVALID_TIMESTAMP_EXCEPTION: i32 = 38;
    /// The config is invalid.
    #[classattr]
    const INVALID_CONFIG_EXCEPTION: i32 = 39;
    /// The lake storage is not configured.
    #[classattr]
    const LAKE_STORAGE_NOT_CONFIGURED_EXCEPTION: i32 = 40;
    /// The kv snapshot does not exist.
    #[classattr]
    const KV_SNAPSHOT_NOT_EXIST: i32 = 41;
    /// The partition already exists.
    #[classattr]
    const PARTITION_ALREADY_EXISTS: i32 = 42;
    /// The partition spec is invalid.
    #[classattr]
    const PARTITION_SPEC_INVALID_EXCEPTION: i32 = 43;
    /// There is no currently available leader for the given partition.
    #[classattr]
    const LEADER_NOT_AVAILABLE_EXCEPTION: i32 = 44;
    /// Exceed the maximum number of partitions.
    #[classattr]
    const PARTITION_MAX_NUM_EXCEPTION: i32 = 45;
    /// Authentication failed.
    #[classattr]
    const AUTHENTICATE_EXCEPTION: i32 = 46;
    /// Security is disabled.
    #[classattr]
    const SECURITY_DISABLED_EXCEPTION: i32 = 47;
    /// Authorization failed.
    #[classattr]
    const AUTHORIZATION_EXCEPTION: i32 = 48;
    /// Exceed the maximum number of buckets.
    #[classattr]
    const BUCKET_MAX_NUM_EXCEPTION: i32 = 49;
    /// The tiering epoch is invalid.
    #[classattr]
    const FENCED_TIERING_EPOCH_EXCEPTION: i32 = 50;
    /// Authentication failed with retriable exception.
    #[classattr]
    const RETRIABLE_AUTHENTICATE_EXCEPTION: i32 = 51;
    /// The server rack info is invalid.
    #[classattr]
    const INVALID_SERVER_RACK_INFO_EXCEPTION: i32 = 52;
    /// The lake snapshot does not exist.
    #[classattr]
    const LAKE_SNAPSHOT_NOT_EXIST: i32 = 53;
    /// The lake table already exists.
    #[classattr]
    const LAKE_TABLE_ALREADY_EXIST: i32 = 54;
    /// The new ISR contains at least one ineligible replica.
    #[classattr]
    const INELIGIBLE_REPLICA_EXCEPTION: i32 = 55;
    /// The alter table is invalid.
    #[classattr]
    const INVALID_ALTER_TABLE_EXCEPTION: i32 = 56;
    /// Deletion operations are disabled on this table.
    #[classattr]
    const DELETION_DISABLED_EXCEPTION: i32 = 57;
}
