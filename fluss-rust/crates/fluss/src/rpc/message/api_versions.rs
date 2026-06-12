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

use crate::proto::{
    ApiVersionsRequest as ProtoApiVersionsRequest, ApiVersionsResponse as ProtoApiVersionsResponse,
};
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug, Clone)]
pub struct ApiVersionsRequest {
    pub inner_request: ProtoApiVersionsRequest,
}

impl ApiVersionsRequest {
    pub fn new(client_name: &str, client_version: &str) -> Self {
        Self {
            inner_request: ProtoApiVersionsRequest {
                client_software_name: client_name.to_string(),
                client_software_version: client_version.to_string(),
            },
        }
    }
}

impl RequestBody for ApiVersionsRequest {
    type ResponseBody = ProtoApiVersionsResponse;
    const API_KEY: ApiKey = ApiKey::ApiVersion;
}

impl_write_type!(ApiVersionsRequest);
impl_read_type!(ProtoApiVersionsResponse);
