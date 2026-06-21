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

use crate::rpc::frame::ReadError;

use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct GetDatabaseInfoRequest {
    pub inner_request: proto::GetDatabaseInfoRequest,
}

impl GetDatabaseInfoRequest {
    pub fn new(database_name: &str) -> Self {
        GetDatabaseInfoRequest {
            inner_request: proto::GetDatabaseInfoRequest {
                database_name: database_name.to_string(),
            },
        }
    }
}

impl RequestBody for GetDatabaseInfoRequest {
    type ResponseBody = proto::GetDatabaseInfoResponse;

    const API_KEY: ApiKey = ApiKey::GetDatabaseInfo;
}

impl_write_type!(GetDatabaseInfoRequest);
impl_read_type!(proto::GetDatabaseInfoResponse);
