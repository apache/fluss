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

use crate::proto;
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub(crate) struct FindConsumerGroupOwnerRequest {
    pub(crate) inner_request: proto::FindConsumerGroupOwnerRequest,
}
impl RequestBody for FindConsumerGroupOwnerRequest {
    type ResponseBody = proto::FindConsumerGroupOwnerResponse;
    const API_KEY: ApiKey = ApiKey::FindConsumerGroupOwner;
}
impl_write_type!(FindConsumerGroupOwnerRequest);
impl_read_type!(proto::FindConsumerGroupOwnerResponse);

#[derive(Debug)]
pub(crate) struct CommitConsumerOffsetsRequest {
    pub(crate) inner_request: proto::CommitConsumerOffsetsRequest,
}
impl RequestBody for CommitConsumerOffsetsRequest {
    type ResponseBody = proto::CommitConsumerOffsetsResponse;
    const API_KEY: ApiKey = ApiKey::CommitConsumerOffsets;
}
impl_write_type!(CommitConsumerOffsetsRequest);
impl_read_type!(proto::CommitConsumerOffsetsResponse);

#[derive(Debug)]
pub(crate) struct GetConsumerOffsetsRequest {
    pub(crate) inner_request: proto::GetConsumerOffsetsRequest,
}
impl RequestBody for GetConsumerOffsetsRequest {
    type ResponseBody = proto::GetConsumerOffsetsResponse;
    const API_KEY: ApiKey = ApiKey::GetConsumerOffsets;
}
impl_write_type!(GetConsumerOffsetsRequest);
impl_read_type!(proto::GetConsumerOffsetsResponse);
