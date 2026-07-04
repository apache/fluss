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
use crate::proto::PbTablePath;
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};

use crate::metadata::TablePath;
use crate::rpc::frame::ReadError;

use crate::{impl_read_type, impl_write_type};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct GetLatestLakeSnapshotRequest {
    pub(crate) inner_request: proto::GetLatestLakeSnapshotRequest,
}

impl GetLatestLakeSnapshotRequest {
    pub fn new(table_path: &TablePath) -> Self {
        let inner_request = proto::GetLatestLakeSnapshotRequest {
            table_path: PbTablePath {
                database_name: table_path.database().to_string(),
                table_name: table_path.table().to_string(),
            },
        };

        Self { inner_request }
    }
}

impl RequestBody for GetLatestLakeSnapshotRequest {
    type ResponseBody = proto::GetLatestLakeSnapshotResponse;
    const API_KEY: ApiKey = ApiKey::GetLakeSnapshot;
}

impl_write_type!(GetLatestLakeSnapshotRequest);
impl_read_type!(proto::GetLatestLakeSnapshotResponse);
