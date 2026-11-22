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

use crate::metadata::TablePath;
use crate::rpc::api_key::ApiKey;
use crate::rpc::convert::to_table_path;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{SnapshotId, impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct GetLakeSnapshotRequest {
    pub(crate) inner_request: proto::GetLakeSnapshotRequest,
}

impl GetLakeSnapshotRequest {
    pub fn new(
        table_path: &TablePath,
        snapshot_id: Option<SnapshotId>,
        readable: Option<bool>,
    ) -> Self {
        GetLakeSnapshotRequest {
            inner_request: proto::GetLakeSnapshotRequest {
                table_path: to_table_path(table_path),
                snapshot_id,
                readable,
            },
        }
    }
}

impl RequestBody for GetLakeSnapshotRequest {
    type ResponseBody = proto::GetLakeSnapshotResponse;
    const API_KEY: ApiKey = ApiKey::GetLakeSnapshot;
}

impl_write_type!(GetLakeSnapshotRequest);
impl_read_type!(proto::GetLakeSnapshotResponse);
