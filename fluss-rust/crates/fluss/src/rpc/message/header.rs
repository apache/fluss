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

use crate::proto::ErrorResponse;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, WriteType};
use bytes::{Buf, BufMut};
use prost::Message;

pub(crate) const REQUEST_HEADER_LENGTH: usize = 8;
const SUCCESS_RESPONSE: u8 = 0;
#[allow(dead_code)]
const ERROR_RESPONSE: u8 = 1;
#[allow(dead_code)]
const SERVER_FAILURE: u8 = 2;

#[derive(Debug, PartialEq, Eq)]
pub struct RequestHeader {
    /// The API key of this request.
    pub request_api_key: ApiKey,

    pub request_api_version: ApiVersion,

    pub request_id: i32,

    pub client_id: Option<String>,
}

impl<W> WriteType<W> for RequestHeader
where
    W: BufMut,
{
    fn write(&self, writer: &mut W) -> Result<(), WriteError> {
        writer.put_i16(self.request_api_key.into());
        writer.put_i16(self.request_api_version.0);
        writer.put_i32(self.request_id);
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct ResponseHeader {
    pub request_id: i32,
    pub error_response: Option<ErrorResponse>,
}

impl<R> ReadType<R> for ResponseHeader
where
    R: Buf,
{
    fn read(reader: &mut R) -> Result<Self, ReadError> {
        let resp_type = reader.get_u8();
        let request_id = reader.get_i32();
        if resp_type != SUCCESS_RESPONSE {
            let error_response = ErrorResponse::decode(reader)?;
            return Ok(ResponseHeader {
                request_id,
                error_response: Some(error_response),
            });
        }
        Ok(ResponseHeader {
            request_id,
            error_response: None,
        })
    }
}
