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

//! Serializable logical read split for bounded UnionRead.

use crate::split_descriptor::SplitDescriptor;
use crate::{FlussLakeError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Exact split descriptor version supported by this reader.
pub(crate) const CURRENT_FLUSS_LAKE_SPLIT_VERSION: u32 = 5;

/// Estimated work attached to one logical split during planning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct SplitStatistics {
    pub(crate) estimated_rows: Option<usize>,
    pub(crate) estimated_size: Option<usize>,
}

impl SplitStatistics {
    pub(crate) fn new(estimated_rows: Option<usize>, estimated_size: Option<usize>) -> Self {
        Self {
            estimated_rows,
            estimated_size,
        }
    }
}

pub use crate::partition::FlussLakePartitionIdentity;

/// One bounded read task with a frozen partition and source bucket identity.
///
/// Append plans have separate tasks for each lake split and each nonempty log
/// tail. Primary-key plans keep a bucket's lake baseline and tail together.
/// Task payloads remain private inside `execution_descriptor`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlussLakeReadSplit {
    /// Opaque identifier unique within the owning plan, not across replanning.
    pub split_id: String,
    /// Fluss log bucket, or the lake-native bucket for an append lake task.
    /// Bucket-unaware append lake tasks use -1 and never carry a log range.
    pub bucket_id: i32,
    /// Partition represented by this logical split.
    pub partition: FlussLakePartitionIdentity,
    /// Best-effort input row estimate; `None` means unknown.
    /// Not an exact PK result cardinality.
    pub estimated_rows: Option<usize>,
    /// Best-effort input size in bytes; `None` means unknown.
    pub estimated_size: Option<usize>,
    /// Version of the private execution descriptor.
    pub descriptor_version: u32,
    execution_descriptor: SplitDescriptor,
}

impl FlussLakeReadSplit {
    pub(crate) fn try_new(
        split_id: String,
        bucket_id: i32,
        partition: FlussLakePartitionIdentity,
        descriptor_version: u32,
        execution_descriptor: SplitDescriptor,
        statistics: SplitStatistics,
    ) -> Result<Self> {
        if descriptor_version != CURRENT_FLUSS_LAKE_SPLIT_VERSION {
            return Err(incompatible_split_version(descriptor_version));
        }
        let split = Self {
            split_id,
            bucket_id,
            partition,
            estimated_rows: statistics.estimated_rows,
            estimated_size: statistics.estimated_size,
            descriptor_version,
            execution_descriptor,
        };
        split.validated_execution_descriptor()?;
        Ok(split)
    }

    /// Validates transported fields and borrows the already decoded descriptor.
    pub(crate) fn validated_execution_descriptor(&self) -> Result<&SplitDescriptor> {
        if self.descriptor_version != CURRENT_FLUSS_LAKE_SPLIT_VERSION {
            return Err(incompatible_split_version(self.descriptor_version));
        }
        if self.split_id.is_empty() {
            return Err(FlussLakeError::Internal(
                "split id must not be empty".to_string(),
            ));
        }
        let descriptor = &self.execution_descriptor;
        descriptor.validate()?;
        if self.bucket_id < 0 && !(descriptor.is_append_lake() && self.bucket_id == -1) {
            return Err(FlussLakeError::Internal(format!(
                "split bucket id must be non-negative, got {}",
                self.bucket_id
            )));
        }
        validate_partition_identity(&self.partition)?;

        if descriptor.table_bucket().bucket_id() != self.bucket_id {
            return Err(FlussLakeError::Internal(format!(
                "public split bucket id {} does not match execution descriptor bucket id {}",
                self.bucket_id,
                descriptor.table_bucket().bucket_id()
            )));
        }
        let descriptor_is_partitioned = descriptor.is_partitioned();
        let public_is_partitioned =
            matches!(self.partition, FlussLakePartitionIdentity::KeyValues(_));
        if descriptor_is_partitioned != public_is_partitioned {
            return Err(FlussLakeError::Internal(
                "public split partition identity does not match the execution descriptor"
                    .to_string(),
            ));
        }
        if descriptor
            .lake_splits()
            .iter()
            .any(|task| task.partition != self.partition)
        {
            return Err(FlussLakeError::Internal(
                "lake task partition does not match the logical split".to_string(),
            ));
        }
        Ok(descriptor)
    }
}

fn validate_partition_identity(partition: &FlussLakePartitionIdentity) -> Result<()> {
    let FlussLakePartitionIdentity::KeyValues(key_values) = partition else {
        return Ok(());
    };
    if key_values.is_empty() {
        return Err(FlussLakeError::Internal(
            "partitioned split identity must contain at least one key/value pair".to_string(),
        ));
    }
    let mut keys = HashSet::with_capacity(key_values.len());
    for (key, _) in key_values {
        if key.is_empty() {
            return Err(FlussLakeError::Internal(
                "partition key name must not be empty".to_string(),
            ));
        }
        if !keys.insert(key) {
            return Err(FlussLakeError::Internal(format!(
                "partition identity contains duplicate key '{key}'"
            )));
        }
    }
    Ok(())
}

fn incompatible_split_version(split_version: u32) -> FlussLakeError {
    FlussLakeError::IncompatibleSplitVersion(format!(
        "split descriptor version {split_version} is incompatible with reader version {CURRENT_FLUSS_LAKE_SPLIT_VERSION}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::split_descriptor::SplitDescriptor;
    use fluss::metadata::{TableBucket, TablePath};

    fn split_with_version(version: u32) -> Result<FlussLakeReadSplit> {
        FlussLakeReadSplit::try_new(
            "orders/root/0".into(),
            0,
            FlussLakePartitionIdentity::Unpartitioned,
            version,
            SplitDescriptor::try_new(
                &crate::split_descriptor::testing_table(
                    TablePath::new("fluss", "orders"),
                    1,
                    false,
                    (TableBucket::new(5, 0)).table_id(),
                ),
                TableBucket::new(5, 0),
                0,
                10,
                None,
                vec![],
                vec![],
            )
            .unwrap(),
            SplitStatistics::new(Some(42), Some(1024)),
        )
    }

    fn split() -> FlussLakeReadSplit {
        split_with_version(CURRENT_FLUSS_LAKE_SPLIT_VERSION).unwrap()
    }

    fn assert_version_error(error: FlussLakeError, version: u32) {
        match error {
            FlussLakeError::IncompatibleSplitVersion(message) => {
                assert!(message.contains(&format!("version {version}")));
                assert!(message.contains(&format!("version {CURRENT_FLUSS_LAKE_SPLIT_VERSION}")));
            }
            other => panic!("expected incompatible split version, got {other}"),
        }
    }

    #[test]
    fn serde_round_trip_preserves_split_and_statistics() {
        let split = split();
        let encoded = serde_json::to_vec(&split).unwrap();
        let decoded: FlussLakeReadSplit = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(decoded, split);
        assert_eq!(decoded.estimated_rows, Some(42));
        assert_eq!(decoded.estimated_size, Some(1024));
    }

    #[test]
    fn incompatible_versions_report_split_and_reader_versions() {
        for version in
            (1..CURRENT_FLUSS_LAKE_SPLIT_VERSION).chain([CURRENT_FLUSS_LAKE_SPLIT_VERSION + 1])
        {
            assert_version_error(split_with_version(version).unwrap_err(), version);
        }
    }

    #[test]
    fn lake_task_partition_must_match_the_logical_split() {
        let mut task = crate::source::testing_split();
        task.partition =
            FlussLakePartitionIdentity::KeyValues(vec![("region".into(), "US".into())]);
        let descriptor = SplitDescriptor::try_new(
            &crate::split_descriptor::testing_table(
                TablePath::new("fluss", "orders"),
                1,
                true,
                (TableBucket::new_with_partition(5, Some(1), 0)).table_id(),
            ),
            TableBucket::new_with_partition(5, Some(1), 0),
            0,
            0,
            Some(42),
            vec![task],
            Vec::new(),
        )
        .unwrap();
        assert!(
            FlussLakeReadSplit::try_new(
                "orders/EU/0".into(),
                0,
                FlussLakePartitionIdentity::KeyValues(vec![("region".into(), "EU".into())]),
                CURRENT_FLUSS_LAKE_SPLIT_VERSION,
                descriptor,
                SplitStatistics::default(),
            )
            .is_err()
        );
    }

    #[test]
    fn reader_validation_catches_a_public_version_field_mutation() {
        let mut split = split();
        split.descriptor_version = CURRENT_FLUSS_LAKE_SPLIT_VERSION + 1;
        assert_version_error(
            split.validated_execution_descriptor().unwrap_err(),
            split.descriptor_version,
        );
    }

    #[test]
    fn reader_validation_catches_public_bucket_mutation_after_deserialization() {
        let mut value = serde_json::to_value(split()).unwrap();
        value["bucket_id"] = serde_json::json!(1);
        let mutated: FlussLakeReadSplit = serde_json::from_value(value).unwrap();

        assert!(matches!(
            mutated.validated_execution_descriptor(),
            Err(FlussLakeError::Internal(_))
        ));
    }

    #[test]
    fn reader_validation_rejects_invalid_partition_identity() {
        let mut value = serde_json::to_value(split()).unwrap();
        value["partition"] = serde_json::json!({"key_values": []});
        let mutated: FlussLakeReadSplit = serde_json::from_value(value).unwrap();

        assert!(matches!(
            mutated.validated_execution_descriptor(),
            Err(FlussLakeError::Internal(_))
        ));
    }
}
