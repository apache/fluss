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

use crate::proto::{GetLakeSnapshotResponse, PbLakeSnapshotForBucket};
use crate::{BucketId, PartitionId, TableId};

/// One bucket's slice of a lake snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeBucketSnapshot {
    pub partition_id: Option<PartitionId>,
    pub bucket_id: BucketId,
    pub log_offset: Option<i64>,
    pub partition_name: Option<String>,
}

impl LakeBucketSnapshot {
    pub fn from_pb(pb: &PbLakeSnapshotForBucket) -> Self {
        Self {
            partition_id: pb.partition_id,
            bucket_id: pb.bucket_id,
            log_offset: pb.log_offset,
            partition_name: pb.partition_name.clone(),
        }
    }
}

/// Result of `get_lake_snapshot` — a specific snapshot's bucket layout.
/// (Distinct from [`LakeSnapshot`](super::LakeSnapshot), which represents the
/// "latest" snapshot summary returned by `get_latest_lake_snapshot`.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LakeSnapshotInfo {
    pub table_id: TableId,
    pub snapshot_id: i64,
    pub bucket_snapshots: Vec<LakeBucketSnapshot>,
}

impl LakeSnapshotInfo {
    pub fn from_pb(pb: &GetLakeSnapshotResponse) -> Self {
        Self {
            table_id: pb.table_id,
            snapshot_id: pb.snapshot_id,
            bucket_snapshots: pb
                .bucket_snapshots
                .iter()
                .map(LakeBucketSnapshot::from_pb)
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lake_bucket_snapshot_from_pb() {
        let pb = PbLakeSnapshotForBucket {
            partition_id: Some(1),
            bucket_id: 2,
            log_offset: Some(3),
            partition_name: Some("date=2024-01-01".to_string()),
        };
        let s = LakeBucketSnapshot::from_pb(&pb);
        assert_eq!(s.bucket_id, 2);
        assert_eq!(s.partition_id, Some(1));
        assert_eq!(s.log_offset, Some(3));
        assert_eq!(s.partition_name.as_deref(), Some("date=2024-01-01"));
    }
}
