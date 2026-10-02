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

use crate::proto::{GetProducerOffsetsResponse, PbBucketOffset, PbProducerTableOffsets};
use crate::{BucketId, PartitionId, TableId};

/// Per-bucket producer log-end offset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketOffset {
    pub partition_id: Option<PartitionId>,
    pub bucket_id: BucketId,
    pub log_end_offset: Option<i64>,
}

impl BucketOffset {
    pub fn to_pb(&self) -> PbBucketOffset {
        PbBucketOffset {
            partition_id: self.partition_id,
            bucket_id: self.bucket_id,
            log_end_offset: self.log_end_offset,
        }
    }

    pub fn from_pb(pb: &PbBucketOffset) -> Self {
        Self {
            partition_id: pb.partition_id,
            bucket_id: pb.bucket_id,
            log_end_offset: pb.log_end_offset,
        }
    }
}

/// All bucket offsets of a single table belonging to one producer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerTableOffsets {
    pub table_id: TableId,
    pub bucket_offsets: Vec<BucketOffset>,
}

impl ProducerTableOffsets {
    pub fn to_pb(&self) -> PbProducerTableOffsets {
        PbProducerTableOffsets {
            table_id: self.table_id,
            bucket_offsets: self
                .bucket_offsets
                .iter()
                .map(BucketOffset::to_pb)
                .collect(),
        }
    }

    pub fn from_pb(pb: &PbProducerTableOffsets) -> Self {
        Self {
            table_id: pb.table_id,
            bucket_offsets: pb
                .bucket_offsets
                .iter()
                .map(BucketOffset::from_pb)
                .collect(),
        }
    }
}

/// Result of `get_producer_offsets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerOffsets {
    pub producer_id: Option<String>,
    pub expiration_time: Option<i64>,
    pub table_offsets: Vec<ProducerTableOffsets>,
}

impl ProducerOffsets {
    pub fn from_pb(pb: &GetProducerOffsetsResponse) -> Self {
        Self {
            producer_id: pb.producer_id.clone(),
            expiration_time: pb.expiration_time,
            table_offsets: pb
                .table_offsets
                .iter()
                .map(ProducerTableOffsets::from_pb)
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_offset_roundtrip() {
        for b in [
            BucketOffset {
                partition_id: None,
                bucket_id: 0,
                log_end_offset: None,
            },
            BucketOffset {
                partition_id: Some(42),
                bucket_id: 3,
                log_end_offset: Some(1234),
            },
        ] {
            assert_eq!(BucketOffset::from_pb(&b.to_pb()), b);
        }
    }

    #[test]
    fn test_producer_table_offsets_roundtrip() {
        let t = ProducerTableOffsets {
            table_id: 5,
            bucket_offsets: vec![
                BucketOffset {
                    partition_id: None,
                    bucket_id: 0,
                    log_end_offset: Some(100),
                },
                BucketOffset {
                    partition_id: Some(7),
                    bucket_id: 1,
                    log_end_offset: None,
                },
            ],
        };
        assert_eq!(ProducerTableOffsets::from_pb(&t.to_pb()), t);
    }
}
