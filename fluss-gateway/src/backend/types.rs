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

//! Protocol-neutral identifiers of the backend component.

use crate::error::GatewayError;
use std::fmt;
use std::sync::Arc;

/// Validated cluster identifier, shared by configuration, the registry, and the protocol adapters.
///
/// Parsing a caller-supplied ID here means an unconfigured or malformed cluster reaches the registry
/// as a value it can look up, never as a raw string that could be interpolated somewhere else. The
/// accepted shape is the one configuration validation enforces, so a request can only name a cluster
/// that an operator could have configured.
///
/// Backed by an `Arc<str>` because every request clones the ID into its context and its pool key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClusterId(Arc<str>);

impl ClusterId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for ClusterId {
    type Error = GatewayError;

    /// Rejects an ID no operator could have configured.
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if !crate::config::valid_cluster_id(value) {
            return Err(GatewayError::invalid_argument(
                "cluster ID must be at most 63 characters, start with a lowercase letter, and \
                 contain only lowercase letters, digits, or underscores",
            ));
        }
        Ok(Self(Arc::from(value)))
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn accepts_only_ids_configuration_would_accept() {
        for id in ["default", "a", "east_1", &"c".repeat(63)] {
            assert_eq!(ClusterId::try_from(id).unwrap().as_str(), id);
        }
        for id in ["", "Default", "1east", "east-1", "east.1", &"c".repeat(64)] {
            let error = ClusterId::try_from(id).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidArgument, "{id:?}");
        }
    }
}
