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

//! Frozen local tasks and their source metadata.

pub use crate::partition::FlussLakePartitionIdentity;
use crate::{FlussLakeError, FlussLakeSplitDescriptor, Result};
#[cfg(test)]
use fluss::metadata::TableBucket;
use fluss::metadata::{TableInfo, TablePath};
use std::collections::HashSet;
use std::sync::Arc;

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

/// One executable task bound to frozen source metadata.
/// Append files and log ranges are independent tasks; PK tasks own a bucket's
/// complete baseline and tail. Clones share the descriptor and table metadata.
#[derive(Clone)]
pub struct FlussLakeReadSplit {
    descriptor: Arc<FlussLakeSplitDescriptor>,
    table_info: Arc<TableInfo>,
    primary_key_indexes: Arc<[usize]>,
}

impl std::fmt::Debug for FlussLakeReadSplit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussLakeReadSplit")
            .field("table_path", self.table_path())
            .field("schema_id", &self.schema_id())
            .field("descriptor", self.descriptor())
            .finish_non_exhaustive()
    }
}

impl FlussLakeReadSplit {
    /// Source-owned execution inputs. Adapters may transmit them without runtime credentials.
    pub fn descriptor(&self) -> &FlussLakeSplitDescriptor {
        &self.descriptor
    }
    /// Identifier unique within the original plan, not across replanning.
    pub fn split_id(&self) -> &str {
        &self.descriptor.split_id
    }
    /// Fluss bucket or the native lake bucket; -1 denotes a bucket-unaware append task.
    pub fn bucket_id(&self) -> i32 {
        self.descriptor.table_bucket.bucket_id()
    }
    /// Business partition identity, including lake-only historical partitions.
    pub fn partition(&self) -> &FlussLakePartitionIdentity {
        &self.descriptor.partition
    }
    /// Best-effort input row count; not the reconciled PK result cardinality.
    pub fn estimated_rows(&self) -> Option<usize> {
        self.descriptor.estimated_rows
    }
    /// Best-effort input bytes, or None when unknown.
    pub fn estimated_size(&self) -> Option<usize> {
        self.descriptor.estimated_size
    }

    pub(crate) fn try_new(
        table_info: Arc<TableInfo>,
        descriptor: FlussLakeSplitDescriptor,
        primary_key_indexes: Vec<usize>,
    ) -> Result<Self> {
        let split = Self {
            table_info,
            descriptor: Arc::new(descriptor),
            primary_key_indexes: primary_key_indexes.into(),
        };
        split.validate()?;
        Ok(split)
    }

    pub(crate) fn append_id(&mut self, suffix: &str) {
        Arc::make_mut(&mut self.descriptor)
            .split_id
            .push_str(suffix);
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let crate::FlussLakeSplitDescriptor {
            snapshot_id,
            table_bucket,
            start_offset,
            stop_offset,
            lake_splits,
            ..
        } = self.descriptor();
        let table_info = &self.table_info;
        let primary_key_indexes = &self.primary_key_indexes;
        if let Some(snapshot_id) = self.descriptor.kv_snapshot_id
            && (snapshot_id < 0
                || !table_info.has_primary_key()
                || primary_key_indexes.is_empty()
                || !lake_splits.is_empty()
                || (table_info.is_partitioned() && table_bucket.partition_id().is_none()))
        {
            return Err(invalid_descriptor(
                "KV baseline requires a live PK bucket and cannot include lake tasks",
            ));
        }
        let partitioned = !table_info.partition_keys.is_empty();
        let task = self.descriptor();
        if task.split_id.is_empty()
            || !task.partition.matches_keys(&table_info.partition_keys)
            || task
                .lake_splits
                .iter()
                .any(|lake| lake.partition != task.partition)
        {
            return Err(invalid_descriptor(
                "split identity or partition does not match its source",
            ));
        }

        if table_info.table_id != table_bucket.table_id() {
            return Err(invalid_descriptor(
                "frozen source does not match task table",
            ));
        }
        if let Some(partition_id) = table_bucket.partition_id() {
            if !partitioned {
                return Err(invalid_descriptor(
                    "an unpartitioned split must not carry a live partition id",
                ));
            }
            if partition_id < 0 {
                return Err(invalid_descriptor(format!(
                    "partition id must be non-negative, got {partition_id}"
                )));
            }
        }
        if table_bucket.bucket_id() < 0
            && !(self.is_append_lake() && table_bucket.bucket_id() == -1)
        {
            return Err(invalid_descriptor(format!(
                "bucket id must be non-negative, got {}",
                table_bucket.bucket_id()
            )));
        }
        if *start_offset < 0 || *stop_offset < 0 || start_offset > stop_offset {
            return Err(invalid_descriptor(format!(
                "logical changelog range is invalid: [{start_offset}, {stop_offset})"
            )));
        }
        if partitioned && table_bucket.partition_id().is_none() && start_offset != stop_offset {
            return Err(invalid_descriptor(
                "a partition that no longer exists in Fluss cannot carry a log range",
            ));
        }
        if !lake_splits.is_empty() && snapshot_id.is_none() {
            return Err(invalid_descriptor(
                "lake splits require a pinned snapshot id",
            ));
        }
        for task in lake_splits {
            if task.format.is_empty()
                || Some(task.snapshot_id) != *snapshot_id
                || task.bucket_id != table_bucket.bucket_id()
                || !task.has_valid_bucket()
                || (self.is_primary_key() && task.bucket_id < 0)
                || task.bucket_count != lake_splits[0].bucket_count
                || task.payload_version == 0
                || task.payload.is_empty()
            {
                return Err(invalid_descriptor(
                    "lake task identity or payload does not match the logical split",
                ));
            }
        }
        let mut seen = HashSet::with_capacity(primary_key_indexes.len());
        if primary_key_indexes.iter().any(|index| !seen.insert(*index)) {
            return Err(invalid_descriptor(
                "primary-key indexes must not contain duplicates",
            ));
        }

        if !self.is_primary_key() && !lake_splits.is_empty() && !self.is_append_lake() {
            return Err(invalid_descriptor(
                "append lake tasks must be independent of log ranges",
            ));
        }
        Ok(())
    }

    pub(crate) fn table_path(&self) -> &TablePath {
        &self.table_info.table_path
    }

    pub(crate) fn schema_id(&self) -> i32 {
        self.table_info.schema_id
    }

    pub(crate) fn is_primary_key(&self) -> bool {
        !self.primary_key_indexes.is_empty()
    }

    pub(crate) fn is_append_lake(&self) -> bool {
        !self.is_primary_key()
            && self.descriptor.lake_splits.len() == 1
            && self.descriptor.start_offset == self.descriptor.stop_offset
    }
}
fn invalid_descriptor(message: impl Into<String>) -> FlussLakeError {
    FlussLakeError::Internal(message.into())
}

#[cfg(test)]
pub(crate) fn testing_table(
    path: TablePath,
    schema_id: i32,
    partitioned: bool,
    table_id: i64,
) -> TableInfo {
    use fluss::metadata::{DataTypes, Schema};
    use std::collections::HashMap;
    let schema = Schema::builder()
        .column("id", DataTypes::int())
        .column("region", DataTypes::string())
        .build()
        .unwrap();
    TableInfo::new(
        path,
        table_id,
        schema_id,
        schema,
        vec![],
        if partitioned {
            vec!["region".into()]
        } else {
            vec![]
        }
        .into(),
        4,
        HashMap::from([
            ("table.datalake.enabled".into(), "true".into()),
            ("table.datalake.format".into(), "paimon".into()),
        ]),
        HashMap::new(),
        None,
        0,
        0,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lake_tasks_without_a_live_bucket_cannot_carry_a_tail() {
        for (partitioned, bucket_id) in [(true, 0), (false, -1)] {
            let info = Arc::new(testing_table(
                TablePath::new("fluss", "orders"),
                1,
                partitioned,
                7,
            ));
            let partition = if partitioned {
                FlussLakePartitionIdentity::KeyValues(vec![("region".into(), "US".into())])
            } else {
                FlussLakePartitionIdentity::Unpartitioned
            };
            let mut lake = crate::source::testing_split();
            lake.partition = partition.clone();
            lake.bucket_id = bucket_id;
            if bucket_id == -1 {
                lake.bucket_count = -1;
            }
            let mut task = crate::FlussLakeSplitDescriptor {
                split_id: "test".into(),
                table_bucket: TableBucket::new(7, bucket_id),
                partition,
                start_offset: 0,
                stop_offset: 0,
                snapshot_id: Some(42),
                lake_splits: vec![lake],
                estimated_rows: None,
                estimated_size: None,
                kv_snapshot_id: None,
            };
            FlussLakeReadSplit::try_new(info.clone(), task.clone(), vec![]).unwrap();
            task.stop_offset = 1;
            assert!(FlussLakeReadSplit::try_new(info, task, vec![]).is_err());
        }
    }
}
