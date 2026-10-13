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

use crate::metadata::AlterConfig;
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct AlterDatabaseRequest {
    pub(crate) inner_request: proto::AlterDatabaseRequest,
}

impl AlterDatabaseRequest {
    pub fn new(
        database_name: &str,
        ignore_if_not_exists: bool,
        config_changes: Vec<AlterConfig>,
        comment: Option<&str>,
    ) -> Self {
        AlterDatabaseRequest {
            inner_request: proto::AlterDatabaseRequest {
                database_name: database_name.to_string(),
                ignore_if_not_exists,
                config_changes: config_changes.iter().map(AlterConfig::to_pb).collect(),
                comment: comment.map(str::to_string),
            },
        }
    }
}

impl RequestBody for AlterDatabaseRequest {
    type ResponseBody = proto::AlterDatabaseResponse;
    const API_KEY: ApiKey = ApiKey::AlterDatabase;
}

impl_write_type!(AlterDatabaseRequest);
impl_read_type!(proto::AlterDatabaseResponse);
