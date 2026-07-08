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

use crate::{BucketId, PartitionId, TableId, impl_read_type, impl_write_type, proto};

use crate::error::Result as FlussResult;
use crate::error::{Error, FlussError};
use crate::proto::{ErrorResponse, ListOffsetsResponse};
use crate::rpc::frame::ReadError;

use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use std::collections::HashMap;

use bytes::{Buf, BufMut};
use prost::Message;

/// Offset type constants as per proto comments
pub const LIST_EARLIEST_OFFSET: i32 = 0;
pub const LIST_LATEST_OFFSET: i32 = 1;
pub const LIST_OFFSET_FROM_TIMESTAMP: i32 = 2;

/// Client follower server id constant
pub const CLIENT_FOLLOWER_SERVER_ID: i32 = -1;

/// Offset specification for list offsets request
#[derive(Debug, Clone)]
pub enum OffsetSpec {
    /// Earliest offset spec
    Earliest,
    /// Latest offset spec  
    Latest,
    /// Timestamp offset spec
    Timestamp(i64),
}

impl OffsetSpec {
    pub fn offset_type(&self) -> i32 {
        match self {
            OffsetSpec::Earliest => LIST_EARLIEST_OFFSET,
            OffsetSpec::Latest => LIST_LATEST_OFFSET,
            OffsetSpec::Timestamp(_) => LIST_OFFSET_FROM_TIMESTAMP,
        }
    }

    pub fn start_timestamp(&self) -> Option<i64> {
        match self {
            OffsetSpec::Timestamp(ts) => Some(*ts),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct ListOffsetsRequest {
    pub(crate) inner_request: proto::ListOffsetsRequest,
}

impl ListOffsetsRequest {
    pub fn new(
        table_id: TableId,
        partition_id: Option<PartitionId>,
        bucket_ids: Vec<BucketId>,
        offset_spec: OffsetSpec,
    ) -> Self {
        ListOffsetsRequest {
            inner_request: proto::ListOffsetsRequest {
                follower_server_id: CLIENT_FOLLOWER_SERVER_ID,
                offset_type: offset_spec.offset_type(),
                table_id,
                partition_id,
                bucket_id: bucket_ids,
                start_timestamp: offset_spec.start_timestamp(),
            },
        }
    }
}

impl RequestBody for ListOffsetsRequest {
    type ResponseBody = ListOffsetsResponse;

    const API_KEY: ApiKey = ApiKey::ListOffsets;
}

impl_write_type!(ListOffsetsRequest);
impl_read_type!(ListOffsetsResponse);

impl ListOffsetsResponse {
    pub fn offsets(&self) -> FlussResult<HashMap<i32, i64>> {
        self.buckets_resp
            .iter()
            .map(|resp| {
                if let Some(error_code) = resp.error_code
                    && error_code != FlussError::None.code()
                {
                    let api_error = ErrorResponse {
                        error_code,
                        error_message: resp.error_message.clone(),
                    }
                    .into();
                    return Err(Error::FlussAPIError { api_error });
                }
                // if no error msg, offset must exists
                resp.offset
                    .map(|offset| (resp.bucket_id, offset))
                    .ok_or_else(|| Error::UnexpectedError {
                        message: format!(
                            "Missing offset for bucket {} without error code.",
                            resp.bucket_id
                        ),
                        source: None,
                    })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{ListOffsetsResponse, PbListOffsetsRespForBucket};

    #[test]
    fn offsets_returns_api_error_on_error_code() {
        let response = ListOffsetsResponse {
            buckets_resp: vec![PbListOffsetsRespForBucket {
                bucket_id: 1,
                error_code: Some(FlussError::TableNotExist.code()),
                error_message: Some("missing".to_string()),
                offset: None,
            }],
        };

        let result = response.offsets();
        assert!(matches!(result, Err(Error::FlussAPIError { .. })));
    }
}
