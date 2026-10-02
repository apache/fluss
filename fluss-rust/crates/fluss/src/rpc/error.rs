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

use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use prost::DecodeError;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum RpcError {
    #[error("Cannot write message: {0}")]
    WriteMessageError(#[from] crate::rpc::frame::WriteError),

    #[error("Cannot read framed message: {0}")]
    ReadMessageError(#[from] crate::rpc::frame::ReadError),

    #[error("Rpc Decode Error: {0}")]
    RpcDecodeError(#[from] DecodeError),

    #[error("connection error")]
    ConnectionError(String),

    #[error("IO Error: {0}")]
    IO(#[from] std::io::Error),

    #[error("Connection is poisoned: {0}")]
    Poisoned(Arc<RpcError>),

    #[error(
        "Data left at the end of the message. Got {message_size} bytes but only read {read} bytes. api_key={api_key:?} api_version={api_version}"
    )]
    TooMuchData {
        message_size: u64,
        read: u64,
        api_key: ApiKey,
        api_version: ApiVersion,
    },
}
