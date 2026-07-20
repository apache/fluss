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

use crate::proto::{
    AcquireKvSnapshotLeaseResponse, GetKvSnapshotMetadataResponse, GetLatestKvSnapshotsResponse,
    ListKvSnapshotsResponse, PbKvSnapshot, PbRemotePathAndLocalFile,
};
use crate::{BucketId, PartitionId, SnapshotId, TableId};

use crate::metadata::KvSnapshotLeaseForTable;

/// Per-bucket KV snapshot info.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvSnapshot {
    pub bucket_id: BucketId,
    pub snapshot_id: Option<SnapshotId>,
    pub log_offset: Option<i64>,
}

impl KvSnapshot {
    pub fn from_pb(pb: &PbKvSnapshot) -> Self {
        Self {
            bucket_id: pb.bucket_id,
            snapshot_id: pb.snapshot_id,
            log_offset: pb.log_offset,
        }
    }
}

/// Result of `get_latest_kv_snapshots`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LatestKvSnapshots {
    pub table_id: TableId,
    pub partition_id: Option<PartitionId>,
    pub latest_snapshots: Vec<KvSnapshot>,
}

impl LatestKvSnapshots {
    pub fn from_pb(pb: &GetLatestKvSnapshotsResponse) -> Self {
        Self {
            table_id: pb.table_id,
            partition_id: pb.partition_id,
            latest_snapshots: pb
                .latest_snapshots
                .iter()
                .map(KvSnapshot::from_pb)
                .collect(),
        }
    }
}

/// One file in a KV snapshot manifest: its remote path and the local filename
/// the server expects clients to materialize it as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemotePathAndLocalFile {
    pub remote_path: String,
    pub local_file_name: String,
}

impl RemotePathAndLocalFile {
    pub fn from_pb(pb: &PbRemotePathAndLocalFile) -> Self {
        Self {
            remote_path: pb.remote_path.clone(),
            local_file_name: pb.local_file_name.clone(),
        }
    }
}

/// Result of `get_kv_snapshot_metadata`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvSnapshotMetadata {
    pub log_offset: i64,
    pub snapshot_files: Vec<RemotePathAndLocalFile>,
}

impl KvSnapshotMetadata {
    pub fn from_pb(pb: &GetKvSnapshotMetadataResponse) -> Self {
        Self {
            log_offset: pb.log_offset,
            snapshot_files: pb
                .snapshot_files
                .iter()
                .map(RemotePathAndLocalFile::from_pb)
                .collect(),
        }
    }
}

/// Result of `acquire_kv_snapshot_lease` — any snapshots the server could not
/// lease (typically because they were evicted concurrently).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquireKvSnapshotLeaseResult {
    pub unavailable_snapshots: Vec<KvSnapshotLeaseForTable>,
}

impl AcquireKvSnapshotLeaseResult {
    pub fn from_pb(pb: &AcquireKvSnapshotLeaseResponse) -> Self {
        Self {
            unavailable_snapshots: pb
                .unavailable_snapshots
                .iter()
                .map(KvSnapshotLeaseForTable::from_pb)
                .collect(),
        }
    }
}

/// Result of `list_kv_snapshots`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveKvSnapshots {
    pub table_id: TableId,
    pub partition_id: Option<PartitionId>,
    pub active_snapshots: Vec<KvSnapshot>,
}

impl ActiveKvSnapshots {
    pub fn from_pb(pb: &ListKvSnapshotsResponse) -> Self {
        Self {
            table_id: pb.table_id,
            partition_id: pb.partition_id,
            active_snapshots: pb
                .active_snapshots
                .iter()
                .map(KvSnapshot::from_pb)
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_snapshot_from_pb() {
        let pb = PbKvSnapshot {
            bucket_id: 3,
            snapshot_id: Some(7),
            log_offset: Some(42),
        };
        let s = KvSnapshot::from_pb(&pb);
        assert_eq!(s.bucket_id, 3);
        assert_eq!(s.snapshot_id, Some(7));
        assert_eq!(s.log_offset, Some(42));
    }

    #[test]
    fn test_remote_path_and_local_file_from_pb() {
        let pb = PbRemotePathAndLocalFile {
            remote_path: "s3://bucket/snap/1.sst".to_string(),
            local_file_name: "1.sst".to_string(),
        };
        let f = RemotePathAndLocalFile::from_pb(&pb);
        assert_eq!(f.remote_path, "s3://bucket/snap/1.sst");
        assert_eq!(f.local_file_name, "1.sst");
    }
}
