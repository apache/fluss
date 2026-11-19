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

use crate::error::Error;
use crate::rpc::api_key::ApiKey;
use crate::rpc::api_version::ApiVersion;
use crate::rpc::frame::{ReadError, WriteError};
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

const INCLUDE_HISTORY_MIN_VERSION: ApiVersion = ApiVersion(1);

#[derive(Debug)]
pub struct ListRebalanceProgressRequest {
    pub(crate) inner_request: proto::ListRebalanceProgressRequest,
}

impl ListRebalanceProgressRequest {
    pub fn new(rebalance_id: Option<&str>) -> Self {
        ListRebalanceProgressRequest {
            inner_request: proto::ListRebalanceProgressRequest {
                rebalance_id: rebalance_id.map(|s| s.to_string()),
                include_history: None,
            },
        }
    }

    /// Requests the summaries of the current rebalance and the retained history.
    pub fn with_history() -> Self {
        ListRebalanceProgressRequest {
            inner_request: proto::ListRebalanceProgressRequest {
                rebalance_id: None,
                include_history: Some(true),
            },
        }
    }
}

impl RequestBody for ListRebalanceProgressRequest {
    type ResponseBody = proto::ListRebalanceProgressResponse;
    const API_KEY: ApiKey = ApiKey::ListRebalanceProgress;

    // An older server ignores include_history and answers with the current rebalance only,
    // which the client could not tell apart from an empty history.
    fn check_version(&self, version: ApiVersion) -> Result<(), Error> {
        if self.inner_request.include_history == Some(true) && version < INCLUDE_HISTORY_MIN_VERSION
        {
            return Err(Error::UnsupportedVersion {
                message: format!(
                    "Listing the rebalance history requires LIST_REBALANCE_PROGRESS version {} or newer, but the server negotiated version {}.",
                    INCLUDE_HISTORY_MIN_VERSION.0, version.0
                ),
            });
        }
        Ok(())
    }
}

impl_write_type!(ListRebalanceProgressRequest);
impl_read_type!(proto::ListRebalanceProgressResponse);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn history_needs_list_rebalance_progress_v1() {
        let request = ListRebalanceProgressRequest::with_history();
        assert!(matches!(
            request.check_version(ApiVersion(0)),
            Err(Error::UnsupportedVersion { .. })
        ));
        assert!(request.check_version(ApiVersion(1)).is_ok());
    }

    #[test]
    fn progress_still_works_on_list_rebalance_progress_v0() {
        assert!(
            ListRebalanceProgressRequest::new(Some("id"))
                .check_version(ApiVersion(0))
                .is_ok()
        );
    }
}
