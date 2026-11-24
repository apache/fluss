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

#[derive(Clone)]
pub struct AuthenticateRequest {
    pub(crate) inner_request: ProtoAuthenticateRequest,
}

impl std::fmt::Debug for AuthenticateRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthenticateRequest")
            .field("protocol", &self.inner_request.protocol)
            .field("token", &"[REDACTED]")
            .finish()
    }
}

impl AuthenticateRequest {
    /// Build a SASL/PLAIN request that acts as the authenticated user.
    pub fn new_plain(username: &str, password: &str) -> Self {
        Self::new_plain_with_authorization_id(username, password, None)
    }

    /// Build a SASL/PLAIN token: `[authorization_id]\0<username>\0<password>`.
    pub fn new_plain_with_authorization_id(
        username: &str,
        password: &str,
        authorization_id: Option<&str>,
    ) -> Self {
        let authorization_id = authorization_id.unwrap_or_default();
        let mut token =
            Vec::with_capacity(authorization_id.len() + 1 + username.len() + 1 + password.len());
        token.extend_from_slice(authorization_id.as_bytes());
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
        for (authorization_id, expected) in [
            (None, "\0gateway\0secret"),
            (Some(""), "\0gateway\0secret"),
            (Some("alice"), "alice\0gateway\0secret"),
            (Some("用户"), "用户\0gateway\0secret"),
        ] {
            let req = AuthenticateRequest::new_plain_with_authorization_id(
                "gateway",
                "secret",
                authorization_id,
            );
            assert_eq!(req.inner_request.protocol, "PLAIN");
            assert_eq!(req.inner_request.token, expected.as_bytes());
            let mut encoded = Vec::new();
            req.write(&mut encoded).unwrap();
            let decoded = ProtoAuthenticateRequest::decode(encoded.as_slice()).unwrap();
            assert_eq!(decoded.token, expected.as_bytes());
        }
    }

    #[test]
    fn test_debug_redacts_token() {
        let req = AuthenticateRequest::new_plain_with_authorization_id(
            "gateway-private",
            "password-private",
            Some("user-private"),
        );
        let challenge =
            AuthenticateRequest::from_challenge("PLAIN", req.inner_request.token.clone());
        for request in [req, challenge] {
            let debug = format!("{request:?}");
            for secret in ["gateway-private", "password-private", "user-private"] {
                assert!(!debug.contains(secret));
            }
            assert!(!debug.contains(&format!("{:?}", request.inner_request.token)));
        }
    }

    #[test]
    fn test_new_plain_empty_credentials() {
        let req = AuthenticateRequest::new_plain("", "");
        assert_eq!(req.inner_request.token, b"\0\0");
    }
}
