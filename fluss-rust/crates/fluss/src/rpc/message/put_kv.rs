/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::client::ReadyWriteBatch;
use crate::proto::{PbPutKvReqForBucket, PutKvResponse};
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::ReadError;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

#[allow(dead_code)]
pub struct PutKvRequest {
    pub inner_request: proto::PutKvRequest,
}

#[allow(dead_code)]
impl PutKvRequest {
    pub fn new(
        table_id: i64,
        ack: i16,
        max_request_timeout_ms: i32,
        target_columns: Vec<i32>,
        ready_batches: &mut [ReadyWriteBatch],
    ) -> crate::error::Result<Self> {
        let mut request = proto::PutKvRequest {
            table_id,
            acks: ack as i32,
            timeout_ms: max_request_timeout_ms,
            target_columns,
            ..Default::default()
        };
        for ready_batch in ready_batches {
            request.buckets_req.push(PbPutKvReqForBucket {
                partition_id: ready_batch.table_bucket.partition_id(),
                bucket_id: ready_batch.table_bucket.bucket_id(),
                records: ready_batch.write_batch.build()?,
            })
        }

        Ok(PutKvRequest {
            inner_request: request,
        })
    }
}

impl RequestBody for PutKvRequest {
    type ResponseBody = PutKvResponse;

    const API_KEY: ApiKey = ApiKey::PutKv;
}

impl_write_type!(PutKvRequest);
impl_read_type!(PutKvResponse);
