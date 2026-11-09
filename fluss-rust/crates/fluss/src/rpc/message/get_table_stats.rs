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

use crate::metadata::BucketStatsRequest;
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{PartitionId, TableId, impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug)]
pub struct GetTableStatsRequest {
    pub(crate) inner_request: proto::GetTableStatsRequest,
}

impl GetTableStatsRequest {
    pub fn new(
        table_id: TableId,
        buckets_req: Vec<BucketStatsRequest>,
        target_columns: Vec<i32>,
    ) -> Self {
        GetTableStatsRequest {
            inner_request: proto::GetTableStatsRequest {
                table_id,
                buckets_req: buckets_req.iter().map(BucketStatsRequest::to_pb).collect(),
                target_columns,
            },
        }
    }

    pub fn with_routing_bucket_counts(
        mut self,
        bucket_count_of: impl Fn(Option<PartitionId>) -> Option<i32>,
    ) -> Self {
        for bucket in &mut self.inner_request.buckets_req {
            bucket.routing_bucket_count = bucket_count_of(bucket.partition_id);
        }
        self
    }
}

impl RequestBody for GetTableStatsRequest {
    type ResponseBody = proto::GetTableStatsResponse;
    const API_KEY: ApiKey = ApiKey::GetTableStats;
}

impl_write_type!(GetTableStatsRequest);
impl_read_type!(proto::GetTableStatsResponse);
