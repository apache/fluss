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
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct DropKvSnapshotLeaseRequest {
    pub(crate) inner_request: proto::DropKvSnapshotLeaseRequest,
}

impl DropKvSnapshotLeaseRequest {
    pub fn new(lease_id: &str) -> Self {
        DropKvSnapshotLeaseRequest {
            inner_request: proto::DropKvSnapshotLeaseRequest {
                lease_id: lease_id.to_string(),
            },
        }
    }
}

impl RequestBody for DropKvSnapshotLeaseRequest {
    type ResponseBody = proto::DropKvSnapshotLeaseResponse;
    const API_KEY: ApiKey = ApiKey::DropKvSnapshotLease;
}

impl_write_type!(DropKvSnapshotLeaseRequest);
impl_read_type!(proto::DropKvSnapshotLeaseResponse);
