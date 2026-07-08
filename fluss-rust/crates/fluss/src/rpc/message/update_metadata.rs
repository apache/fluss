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

use crate::metadata::{PhysicalTablePath, TablePath};
use crate::proto::{MetadataResponse, PbPhysicalTablePath, PbTablePath};
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::ReadError;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use std::collections::HashSet;
use std::sync::Arc;

use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

pub struct UpdateMetadataRequest {
    pub(crate) inner_request: proto::MetadataRequest,
}

impl UpdateMetadataRequest {
    pub fn new(
        table_paths: &HashSet<&TablePath>,
        physical_table_paths: &HashSet<&Arc<PhysicalTablePath>>,
        partition_ids: Vec<i64>,
    ) -> Self {
        UpdateMetadataRequest {
            inner_request: proto::MetadataRequest {
                table_path: table_paths
                    .iter()
                    .map(|path| PbTablePath {
                        database_name: path.database().to_string(),
                        table_name: path.table().to_string(),
                    })
                    .collect(),
                partitions_path: physical_table_paths
                    .iter()
                    .map(|path| PbPhysicalTablePath {
                        database_name: path.get_database_name().to_string(),
                        table_name: path.get_table_name().to_string(),
                        partition_name: path.get_partition_name().map(|pn| pn.to_string()),
                    })
                    .collect(),
                partitions_id: partition_ids,
            },
        }
    }
}

impl RequestBody for UpdateMetadataRequest {
    type ResponseBody = MetadataResponse;

    const API_KEY: ApiKey = ApiKey::MetaData;
}

impl_write_type!(UpdateMetadataRequest);
impl_read_type!(MetadataResponse);
