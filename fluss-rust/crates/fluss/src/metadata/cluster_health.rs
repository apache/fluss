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

use crate::error::{Error, Result};
use crate::proto::GetClusterHealthResponse;

/// Mirrors Java `org.apache.fluss.client.admin.ClusterHealthStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterHealthStatus {
    Green,
    Yellow,
    Red,
    Unknown,
}

impl ClusterHealthStatus {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::Green => 0,
            Self::Yellow => 1,
            Self::Red => 2,
            Self::Unknown => 3,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::Green),
            1 => Ok(Self::Yellow),
            2 => Ok(Self::Red),
            3 => Ok(Self::Unknown),
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported ClusterHealthStatus: {value}"),
            }),
        }
    }
}

/// Result of `get_cluster_health`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterHealth {
    pub num_replicas: i32,
    pub in_sync_replicas: i32,
    pub num_leader_replicas: i32,
    pub active_leader_replicas: i32,
    pub status: ClusterHealthStatus,
}

impl ClusterHealth {
    pub fn from_pb(pb: &GetClusterHealthResponse) -> Result<Self> {
        Ok(Self {
            num_replicas: pb.num_replicas,
            in_sync_replicas: pb.in_sync_replicas,
            num_leader_replicas: pb.num_leader_replicas,
            active_leader_replicas: pb.active_leader_replicas,
            status: ClusterHealthStatus::try_from_i32(pb.status)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_health_status_roundtrip() {
        for s in [
            ClusterHealthStatus::Green,
            ClusterHealthStatus::Yellow,
            ClusterHealthStatus::Red,
            ClusterHealthStatus::Unknown,
        ] {
            assert_eq!(ClusterHealthStatus::try_from_i32(s.to_i32()).unwrap(), s);
        }
    }

    #[test]
    fn test_cluster_health_status_unknown_value() {
        assert!(ClusterHealthStatus::try_from_i32(99).is_err());
    }

    #[test]
    fn test_cluster_health_from_pb() {
        let pb = GetClusterHealthResponse {
            num_replicas: 5,
            in_sync_replicas: 4,
            num_leader_replicas: 3,
            active_leader_replicas: 3,
            status: 1,
        };
        let h = ClusterHealth::from_pb(&pb).unwrap();
        assert_eq!(h.num_replicas, 5);
        assert_eq!(h.status, ClusterHealthStatus::Yellow);
    }
}
