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

use crate::proto::{AuthenticateRequest as ProtoAuthenticateRequest, AuthenticateResponse};
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type};
use bytes::{Buf, BufMut};
use prost::Message;

#[derive(Debug, Clone)]
pub struct AuthenticateRequest {
    pub(crate) inner_request: ProtoAuthenticateRequest,
}

impl AuthenticateRequest {
    /// Build a SASL/PLAIN authenticate request.
    /// Token format: `\0<username>\0<password>` (NUL-separated UTF-8).
    pub fn new_plain(username: &str, password: &str) -> Self {
        let mut token = Vec::with_capacity(1 + username.len() + 1 + password.len());
        token.push(0u8);
        token.extend_from_slice(username.as_bytes());
        token.push(0u8);
        token.extend_from_slice(password.as_bytes());

        Self {
            inner_request: ProtoAuthenticateRequest {
                protocol: "PLAIN".to_string(),
                token,
            },
        }
    }

    /// Build an authenticate request from a server challenge (for multi-round auth).
    pub fn from_challenge(protocol: &str, challenge: Vec<u8>) -> Self {
        Self {
            inner_request: ProtoAuthenticateRequest {
                protocol: protocol.to_string(),
                token: challenge,
            },
        }
    }
}

impl RequestBody for AuthenticateRequest {
    type ResponseBody = AuthenticateResponse;
    const API_KEY: ApiKey = ApiKey::Authenticate;
}

impl_write_type!(AuthenticateRequest);
impl_read_type!(AuthenticateResponse);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_plain_token_format() {
        let req = AuthenticateRequest::new_plain("admin", "secret");
        assert_eq!(req.inner_request.protocol, "PLAIN");
        assert_eq!(req.inner_request.token, b"\0admin\0secret");
    }

    #[test]
    fn test_new_plain_empty_credentials() {
        let req = AuthenticateRequest::new_plain("", "");
        assert_eq!(req.inner_request.token, b"\0\0");
    }
}
