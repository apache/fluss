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

use crate::proto::FetchLogResponse;
use crate::rpc::frame::ReadError;

use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use prost::Message;

use bytes::{Buf, BufMut};

#[allow(dead_code)]
const LOG_FETCH_MAX_BYTES: i32 = 16 * 1024 * 1024;
#[allow(dead_code)]
const LOG_FETCH_MIN_BYTES: i32 = 1;
#[allow(dead_code)]
const LOG_FETCH_WAIT_MAX_TIME: i32 = 500;

pub struct FetchLogRequest {
    pub inner_request: proto::FetchLogRequest,
}

impl FetchLogRequest {
    pub fn new(fetch_log_request: proto::FetchLogRequest) -> Self {
        Self {
            inner_request: fetch_log_request,
        }
    }
}

impl RequestBody for FetchLogRequest {
    type ResponseBody = FetchLogResponse;

    const API_KEY: ApiKey = ApiKey::FetchLog;
}

impl_write_type!(FetchLogRequest);
impl_read_type!(FetchLogResponse);
