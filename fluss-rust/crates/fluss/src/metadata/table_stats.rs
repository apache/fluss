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

use crate::proto::{GetTableStatsResponse, PbTableStatsReqForBucket, PbTableStatsRespForBucket};
use crate::{BucketId, PartitionId};

/// Per-bucket request item for `GetTableStats`.
/// Mirrors the bucket-stats request shape used by the Java client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketStatsRequest {
    pub partition_id: Option<PartitionId>,
    pub bucket_id: BucketId,
}

impl BucketStatsRequest {
    pub fn new(partition_id: Option<PartitionId>, bucket_id: BucketId) -> Self {
        Self {
            partition_id,
            bucket_id,
        }
    }

    pub fn to_pb(&self) -> PbTableStatsReqForBucket {
        PbTableStatsReqForBucket {
            partition_id: self.partition_id,
            bucket_id: self.bucket_id,
        }
    }

    pub fn from_pb(pb: &PbTableStatsReqForBucket) -> Self {
        Self {
            partition_id: pb.partition_id,
            bucket_id: pb.bucket_id,
        }
    }
}

/// Per-bucket stats result returned by `GetTableStats`. `row_count` is `None`
/// when the server returned an error for the bucket; check `error` in that case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketStats {
    pub bucket_id: BucketId,
    pub partition_id: Option<PartitionId>,
    pub row_count: Option<i64>,
    pub error: Option<BucketStatsError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketStatsError {
    pub code: i32,
    pub message: Option<String>,
}

impl BucketStats {
    pub fn from_pb(pb: &PbTableStatsRespForBucket) -> Self {
        let error = pb.error_code.map(|code| BucketStatsError {
            code,
            message: pb.error_message.clone(),
        });
        Self {
            bucket_id: pb.bucket_id,
            partition_id: pb.partition_id,
            row_count: pb.row_count,
            error,
        }
    }
}

/// Full result of `GetTableStats` — one entry per requested bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableStats {
    pub buckets: Vec<BucketStats>,
}

impl TableStats {
    pub fn from_pb(pb: &GetTableStatsResponse) -> Self {
        Self {
            buckets: pb.buckets_resp.iter().map(BucketStats::from_pb).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_stats_request_pb_roundtrip() {
        for req in [
            BucketStatsRequest::new(None, 0),
            BucketStatsRequest::new(Some(42), 7),
        ] {
            let pb = req.to_pb();
            assert_eq!(BucketStatsRequest::from_pb(&pb), req);
        }
    }

    #[test]
    fn test_bucket_stats_from_pb_ok() {
        let pb = PbTableStatsRespForBucket {
            error_code: None,
            error_message: None,
            partition_id: Some(1),
            bucket_id: 7,
            row_count: Some(123),
        };
        let s = BucketStats::from_pb(&pb);
        assert_eq!(s.bucket_id, 7);
        assert_eq!(s.row_count, Some(123));
        assert!(s.error.is_none());
    }

    #[test]
    fn test_bucket_stats_from_pb_err() {
        let pb = PbTableStatsRespForBucket {
            error_code: Some(7),
            error_message: Some("nope".to_string()),
            partition_id: None,
            bucket_id: 2,
            row_count: None,
        };
        let s = BucketStats::from_pb(&pb);
        assert_eq!(s.error.as_ref().unwrap().code, 7);
        assert_eq!(s.error.as_ref().unwrap().message.as_deref(), Some("nope"));
    }
}
