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
use crate::proto::{
    ListRebalanceProgressResponse, PbRebalanceInfo, PbRebalancePlanForBucket,
    PbRebalanceProgressForBucket, PbRebalanceProgressForTable,
};
use crate::{BucketId, PartitionId, TableId};

/// Mirrors Java `org.apache.fluss.cluster.rebalance.RebalanceStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceStatus {
    NotStarted,
    Rebalancing,
    Failed,
    Completed,
    Canceled,
    Timeout,
}

impl RebalanceStatus {
    pub fn to_i32(self) -> i32 {
        match self {
            Self::NotStarted => 0,
            Self::Rebalancing => 1,
            Self::Failed => 2,
            Self::Completed => 3,
            Self::Canceled => 4,
            Self::Timeout => 5,
        }
    }

    pub fn try_from_i32(value: i32) -> Result<Self> {
        match value {
            0 => Ok(Self::NotStarted),
            1 => Ok(Self::Rebalancing),
            2 => Ok(Self::Failed),
            3 => Ok(Self::Completed),
            4 => Ok(Self::Canceled),
            5 => Ok(Self::Timeout),
            _ => Err(Error::IllegalArgument {
                message: format!("Unsupported RebalanceStatus: {value}"),
            }),
        }
    }
}

/// Per-bucket plan in a rebalance: who the leader was and who it will be, who
/// the replicas were and who they will be.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketRebalancePlan {
    pub partition_id: Option<PartitionId>,
    pub bucket_id: BucketId,
    pub original_leader: Option<i32>,
    pub new_leader: Option<i32>,
    pub original_replicas: Vec<i32>,
    pub new_replicas: Vec<i32>,
}

impl BucketRebalancePlan {
    pub fn from_pb(pb: &PbRebalancePlanForBucket) -> Self {
        Self {
            partition_id: pb.partition_id,
            bucket_id: pb.bucket_id,
            original_leader: pb.original_leader,
            new_leader: pb.new_leader,
            original_replicas: pb.original_replicas.clone(),
            new_replicas: pb.new_replicas.clone(),
        }
    }
}

/// Per-bucket rebalance progress: the planned move and its current status code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketRebalanceProgress {
    pub rebalance_plan: BucketRebalancePlan,
    pub rebalance_status: RebalanceStatus,
}

impl BucketRebalanceProgress {
    pub fn from_pb(pb: &PbRebalanceProgressForBucket) -> Result<Self> {
        Ok(Self {
            rebalance_plan: BucketRebalancePlan::from_pb(&pb.rebalance_plan),
            rebalance_status: RebalanceStatus::try_from_i32(pb.rebalance_status)?,
        })
    }
}

/// All bucket progress for one table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRebalanceProgress {
    pub table_id: TableId,
    pub buckets_progress: Vec<BucketRebalanceProgress>,
}

impl TableRebalanceProgress {
    pub fn from_pb(pb: &PbRebalanceProgressForTable) -> Result<Self> {
        Ok(Self {
            table_id: pb.table_id,
            buckets_progress: pb
                .buckets_progress
                .iter()
                .map(BucketRebalanceProgress::from_pb)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

/// Result of `list_rebalance_progress`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceProgress {
    pub rebalance_id: Option<String>,
    pub rebalance_status: Option<RebalanceStatus>,
    pub table_progress: Vec<TableRebalanceProgress>,
    /// Epoch millis when the rebalance started; `None` from servers that predate the field.
    pub started_at_ms: Option<i64>,
    /// Epoch millis when the rebalance reached a final status; `None` while it is running.
    pub completed_at_ms: Option<i64>,
}

impl RebalanceProgress {
    pub fn from_pb(pb: &ListRebalanceProgressResponse) -> Result<Self> {
        Ok(Self {
            rebalance_id: pb.rebalance_id.clone(),
            rebalance_status: pb
                .rebalance_status
                .map(RebalanceStatus::try_from_i32)
                .transpose()?,
            table_progress: pb
                .table_progress
                .iter()
                .map(TableRebalanceProgress::from_pb)
                .collect::<Result<Vec<_>>>()?,
            started_at_ms: pb.started_at_ms,
            completed_at_ms: pb.completed_at_ms,
        })
    }
}

/// One entry of `list_rebalances`. Mirrors Java `org.apache.fluss.cluster.rebalance.RebalanceInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebalanceInfo {
    pub rebalance_id: String,
    pub rebalance_status: RebalanceStatus,
    pub started_at_ms: Option<i64>,
    pub completed_at_ms: Option<i64>,
}

impl RebalanceInfo {
    pub fn from_pb(pb: &PbRebalanceInfo) -> Result<Self> {
        Ok(Self {
            rebalance_id: pb.rebalance_id.clone(),
            rebalance_status: RebalanceStatus::try_from_i32(pb.rebalance_status)?,
            started_at_ms: pb.started_at_ms,
            completed_at_ms: pb.completed_at_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalance_status_roundtrip() {
        for s in [
            RebalanceStatus::NotStarted,
            RebalanceStatus::Rebalancing,
            RebalanceStatus::Failed,
            RebalanceStatus::Completed,
            RebalanceStatus::Canceled,
            RebalanceStatus::Timeout,
        ] {
            assert_eq!(RebalanceStatus::try_from_i32(s.to_i32()).unwrap(), s);
        }
    }

    #[test]
    fn test_rebalance_status_unknown() {
        assert!(RebalanceStatus::try_from_i32(99).is_err());
    }

    #[test]
    fn test_bucket_rebalance_plan_from_pb() {
        let pb = PbRebalancePlanForBucket {
            partition_id: Some(1),
            bucket_id: 2,
            original_leader: Some(3),
            new_leader: Some(4),
            original_replicas: vec![3, 5, 6],
            new_replicas: vec![4, 5, 6],
        };
        let p = BucketRebalancePlan::from_pb(&pb);
        assert_eq!(p.bucket_id, 2);
        assert_eq!(p.new_leader, Some(4));
        assert_eq!(p.new_replicas, vec![4, 5, 6]);
    }

    #[test]
    fn test_rebalance_progress_from_pb_keeps_timestamps() {
        let pb = ListRebalanceProgressResponse {
            rebalance_id: Some("r1".to_string()),
            rebalance_status: Some(RebalanceStatus::Rebalancing.to_i32()),
            table_progress: vec![],
            started_at_ms: Some(5),
            completed_at_ms: None,
        };
        let p = RebalanceProgress::from_pb(&pb).unwrap();
        assert_eq!(p.started_at_ms, Some(5));
        assert_eq!(p.completed_at_ms, None);
    }

    #[test]
    fn test_rebalance_info_from_pb() {
        let pb = PbRebalanceInfo {
            rebalance_id: "r1".to_string(),
            rebalance_status: RebalanceStatus::Completed.to_i32(),
            started_at_ms: Some(1),
            completed_at_ms: Some(2),
        };
        let info = RebalanceInfo::from_pb(&pb).unwrap();
        assert_eq!(
            info,
            RebalanceInfo {
                rebalance_id: "r1".to_string(),
                rebalance_status: RebalanceStatus::Completed,
                started_at_ms: Some(1),
                completed_at_ms: Some(2),
            }
        );
        assert!(
            RebalanceInfo::from_pb(&PbRebalanceInfo {
                rebalance_status: 99,
                ..pb
            })
            .is_err()
        );
    }
}
