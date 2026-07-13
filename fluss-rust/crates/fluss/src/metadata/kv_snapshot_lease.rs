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

use crate::proto::{PbKvSnapshotLeaseForBucket, PbKvSnapshotLeaseForTable};
use crate::{BucketId, PartitionId, TableId};

/// One bucket's slot in a KV-snapshot lease request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvSnapshotLeaseForBucket {
    pub partition_id: Option<PartitionId>,
    pub bucket_id: BucketId,
    pub snapshot_id: i64,
}

impl KvSnapshotLeaseForBucket {
    pub fn to_pb(&self) -> PbKvSnapshotLeaseForBucket {
        PbKvSnapshotLeaseForBucket {
            partition_id: self.partition_id,
            bucket_id: self.bucket_id,
            snapshot_id: self.snapshot_id,
        }
    }

    pub fn from_pb(pb: &PbKvSnapshotLeaseForBucket) -> Self {
        Self {
            partition_id: pb.partition_id,
            bucket_id: pb.bucket_id,
            snapshot_id: pb.snapshot_id,
        }
    }
}

/// All the buckets of a single table that should be leased together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvSnapshotLeaseForTable {
    pub table_id: TableId,
    pub bucket_snapshots: Vec<KvSnapshotLeaseForBucket>,
}

impl KvSnapshotLeaseForTable {
    pub fn to_pb(&self) -> PbKvSnapshotLeaseForTable {
        PbKvSnapshotLeaseForTable {
            table_id: self.table_id,
            bucket_snapshots: self
                .bucket_snapshots
                .iter()
                .map(KvSnapshotLeaseForBucket::to_pb)
                .collect(),
        }
    }

    pub fn from_pb(pb: &PbKvSnapshotLeaseForTable) -> Self {
        Self {
            table_id: pb.table_id,
            bucket_snapshots: pb
                .bucket_snapshots
                .iter()
                .map(KvSnapshotLeaseForBucket::from_pb)
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_snapshot_lease_for_bucket_roundtrip() {
        for b in [
            KvSnapshotLeaseForBucket {
                partition_id: None,
                bucket_id: 0,
                snapshot_id: 10,
            },
            KvSnapshotLeaseForBucket {
                partition_id: Some(42),
                bucket_id: 3,
                snapshot_id: 99,
            },
        ] {
            assert_eq!(KvSnapshotLeaseForBucket::from_pb(&b.to_pb()), b);
        }
    }

    #[test]
    fn test_kv_snapshot_lease_for_table_roundtrip() {
        let t = KvSnapshotLeaseForTable {
            table_id: 7,
            bucket_snapshots: vec![
                KvSnapshotLeaseForBucket {
                    partition_id: None,
                    bucket_id: 0,
                    snapshot_id: 10,
                },
                KvSnapshotLeaseForBucket {
                    partition_id: Some(42),
                    bucket_id: 1,
                    snapshot_id: 11,
                },
            ],
        };
        assert_eq!(KvSnapshotLeaseForTable::from_pb(&t.to_pb()), t);
    }
}
