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

use crate::metadata::ServerTag;
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct RemoveServerTagRequest {
    pub(crate) inner_request: proto::RemoveServerTagRequest,
}

impl RemoveServerTagRequest {
    pub fn new(server_ids: Vec<i32>, server_tag: ServerTag) -> Self {
        RemoveServerTagRequest {
            inner_request: proto::RemoveServerTagRequest {
                server_ids,
                server_tag: server_tag.to_i32(),
            },
        }
    }
}

impl RequestBody for RemoveServerTagRequest {
    type ResponseBody = proto::RemoveServerTagResponse;
    const API_KEY: ApiKey = ApiKey::RemoveServerTag;
}

impl_write_type!(RemoveServerTagRequest);
impl_read_type!(proto::RemoveServerTagResponse);
