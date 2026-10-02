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

pub use crate::rpc::RpcError;
pub use crate::rpc::{ApiError, FlussError};

use arrow_schema::ArrowError;
use snafu::Snafu;
use std::{io, result};
use strum::ParseError;

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(
        whatever,
        display("Fluss hitting unexpected error {}: {:?}", message, source)
    )]
    UnexpectedError {
        message: String,
        /// see <https://github.com/shepmaster/snafu/issues/446>
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync + 'static>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
    },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting unexpected io error {}: {:?}", message, source)
    )]
    IoUnexpectedError { message: String, source: io::Error },

    #[snafu(
        visibility(pub(crate)),
        display(
            "Fluss hitting remote storage unexpected error {}: {:?}",
            message,
            source
        )
    )]
    RemoteStorageUnexpectedError {
        message: String,
        source: opendal::Error,
    },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting json serde error {}.", message)
    )]
    JsonSerdeError { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting unexpected rpc error {}: {:?}", message, source)
    )]
    RpcError { message: String, source: RpcError },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting row convert error {}.", message)
    )]
    RowConvertError { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting Arrow error {}: {:?}.", message, source)
    )]
    ArrowError { message: String, source: ArrowError },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting illegal argument error {}.", message)
    )]
    IllegalArgument { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting IO not supported error {}.", message)
    )]
    IoUnsupported { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting wakeup error {}.", message)
    )]
    WakeupError { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Fluss hitting unsupported operation error {}.", message)
    )]
    UnsupportedOperation { message: String },

    #[snafu(visibility(pub(crate)), display("Fluss writer closed: {}.", message))]
    WriterClosed { message: String },

    #[snafu(
        visibility(pub(crate)),
        display("Fluss buffer exhausted: {}.", message)
    )]
    BufferExhausted { message: String },

    #[snafu(visibility(pub(crate)), display("Fluss API Error: {}.", api_error))]
    FlussAPIError { api_error: ApiError },

    #[snafu(
        visibility(pub(crate)),
        display("Unsupported API version: {}.", message)
    )]
    UnsupportedVersion { message: String },

    /// The server advertised a `server_type` that does not match the one expected
    /// for the target `ServerNode` (e.g. connecting to a coordinator on a tablet
    /// server address).
    #[snafu(visibility(pub(crate)), display("Invalid server type: {}.", message))]
    InvalidServerType { message: String },
}

/// Convenience constructors for API errors that may be raised client-side.
/// These create `FlussAPIError` with the correct protocol error code,
/// consistent with Java where e.g. `InvalidTableException` always carries code 15.
impl Error {
    pub fn table_not_exist(message: impl Into<String>) -> Self {
        Error::FlussAPIError {
            api_error: ApiError {
                code: FlussError::TableNotExist.code(),
                message: message.into(),
            },
        }
    }

    pub fn invalid_table(message: impl Into<String>) -> Self {
        Error::FlussAPIError {
            api_error: ApiError {
                code: FlussError::InvalidTableException.code(),
                message: message.into(),
            },
        }
    }

    pub fn partition_not_exist(message: impl Into<String>) -> Self {
        Error::FlussAPIError {
            api_error: ApiError {
                code: FlussError::PartitionNotExists.code(),
                message: message.into(),
            },
        }
    }

    pub fn invalid_partition(message: impl Into<String>) -> Self {
        Error::FlussAPIError {
            api_error: ApiError {
                code: FlussError::PartitionSpecInvalidException.code(),
                message: message.into(),
            },
        }
    }

    pub fn leader_not_available(message: impl Into<String>) -> Self {
        Error::FlussAPIError {
            api_error: ApiError {
                code: FlussError::LeaderNotAvailableException.code(),
                message: message.into(),
            },
        }
    }

    /// Returns the API error kind if this is an API error, for ergonomic pattern matching.
    pub fn api_error(&self) -> Option<FlussError> {
        if let Error::FlussAPIError { api_error } = self {
            Some(FlussError::for_code(api_error.code))
        } else {
            None
        }
    }

    /// Returns `true` if retrying the request may succeed.
    /// [`Error::RpcError`] is always retriable; [`Error::FlussAPIError`] delegates to
    /// [`ApiError::is_retriable`]; all other variants are not.
    pub fn is_retriable(&self) -> bool {
        match self {
            Error::RpcError { .. } => true,
            Error::FlussAPIError { api_error } => api_error.is_retriable(),
            _ => false,
        }
    }
}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        Error::ArrowError {
            message: format!("{value}"),
            source: value,
        }
    }
}

impl From<RpcError> for Error {
    fn from(value: RpcError) -> Self {
        Error::RpcError {
            message: format!("{value}"),
            source: value,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::IoUnexpectedError {
            message: format!("{value}"),
            source: value,
        }
    }
}

impl From<opendal::Error> for Error {
    fn from(value: opendal::Error) -> Self {
        Error::RemoteStorageUnexpectedError {
            message: format!("{value}"),
            source: value,
        }
    }
}

impl From<ApiError> for Error {
    fn from(value: ApiError) -> Self {
        Error::FlussAPIError { api_error: value }
    }
}

impl From<ParseError> for Error {
    fn from(value: ParseError) -> Self {
        Error::IllegalArgument {
            message: value.to_string(),
        }
    }
}
