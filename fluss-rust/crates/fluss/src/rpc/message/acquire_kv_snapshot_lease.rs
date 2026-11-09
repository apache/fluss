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

use crate::metadata::KvSnapshotLeaseForTable;
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct AcquireKvSnapshotLeaseRequest {
    pub(crate) inner_request: proto::AcquireKvSnapshotLeaseRequest,
}

impl AcquireKvSnapshotLeaseRequest {
    pub fn new(
        lease_id: &str,
        lease_duration_ms: i64,
        snapshots_to_lease: Vec<KvSnapshotLeaseForTable>,
    ) -> Self {
        AcquireKvSnapshotLeaseRequest {
            inner_request: proto::AcquireKvSnapshotLeaseRequest {
                lease_id: lease_id.to_string(),
                lease_duration_ms,
                snapshots_to_lease: snapshots_to_lease
                    .iter()
                    .map(KvSnapshotLeaseForTable::to_pb)
                    .collect(),
            },
        }
    }
}

impl RequestBody for AcquireKvSnapshotLeaseRequest {
    type ResponseBody = proto::AcquireKvSnapshotLeaseResponse;
    const API_KEY: ApiKey = ApiKey::AcquireKvSnapshotLease;
}

impl_write_type!(AcquireKvSnapshotLeaseRequest);
impl_read_type!(proto::AcquireKvSnapshotLeaseResponse);
