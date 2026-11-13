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

//! Source-owned execution inputs; adapters choose transport and task assignment.

use crate::partition::PartitionScope;
use crate::{FlussLakeError, FlussLakePartitionIdentity, FlussLakeReadContext, LakeSplit, Result};
use fluss::metadata::TableBucket;
use serde::{Deserialize, Serialize};

/// A frozen logical task without connections, credentials or scan configuration.
///
/// Export with `FlussLakeReadSplit::descriptor` and restore through
/// `FlussLakeScan::import_splits`. Adapters must preserve these inputs and the
/// planning scan settings. Lake payloads are interpreted only by their source.
/// Deserialization does not validate the task; import does.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlussLakeSplitDescriptor {
    pub(crate) split_id: String,
    pub(crate) table_bucket: TableBucket,
    pub(crate) partition: FlussLakePartitionIdentity,
    pub(crate) start_offset: i64,
    pub(crate) stop_offset: i64,
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) lake_splits: Vec<LakeSplit>,
    pub(crate) estimated_rows: Option<usize>,
    pub(crate) estimated_size: Option<usize>,
    #[serde(default)]
    pub(crate) kv_snapshot_id: Option<i64>,
}

impl FlussLakeSplitDescriptor {
    /// Identifier unique within the original plan.
    pub fn split_id(&self) -> &str {
        &self.split_id
    }

    /// Source bucket identity; lake-only tasks may lack a live partition ID.
    pub fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    /// Business partition represented by this task.
    pub fn partition(&self) -> &FlussLakePartitionIdentity {
        &self.partition
    }

    /// Inclusive fixed start of the log range.
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Exclusive fixed end of the log range; equal to start for lake-only tasks.
    pub fn stop_offset(&self) -> i64 {
        self.stop_offset
    }

    /// Pinned lake snapshot, when present.
    pub fn snapshot_id(&self) -> Option<i64> {
        self.snapshot_id
    }

    /// Persisted snapshot of `table_bucket`; `start_offset` is its matching log seam.
    /// Execution must open this exact snapshot, aligned to the context schema.
    pub fn kv_snapshot_id(&self) -> Option<i64> {
        self.kv_snapshot_id
    }

    /// Source-owned lake tasks. Payloads must be interpreted by their backend.
    pub fn lake_splits(&self) -> &[LakeSplit] {
        &self.lake_splits
    }

    /// Best-effort input row count, not PK output cardinality.
    pub fn estimated_rows(&self) -> Option<usize> {
        self.estimated_rows
    }

    /// Best-effort input bytes.
    pub fn estimated_size(&self) -> Option<usize> {
        self.estimated_size
    }

    pub(crate) fn validate(
        &self,
        context: &FlussLakeReadContext,
        range: Option<&crate::FlussLakeLogRange>,
        scope: &PartitionScope,
    ) -> Result<()> {
        if self.split_id.is_empty()
            || self.table_bucket.table_id() != context.table_id()
            || self.start_offset < 0
            || self.stop_offset < self.start_offset
        {
            return Err(FlussLakeError::InvalidReadContext(
                "invalid frozen UnionRead task identity or range".into(),
            ));
        }
        if !scope.matches(&self.partition)? {
            return Err(FlussLakeError::InvalidReadContext(
                "task is outside the context partition scope".into(),
            ));
        }
        if context.is_bucket_scoped() {
            let selected = &context.log_ranges()[0];
            if self.table_bucket != *selected.table_bucket()
                || selected.partition()
                    != &crate::FlussLakeLogPartition::Ordinary(self.partition.clone())
            {
                return Err(FlussLakeError::InvalidReadContext(
                    "task is outside the selected bucket".into(),
                ));
            }
        }
        if !self.lake_splits.is_empty() && self.snapshot_id != context.lake_snapshot_id() {
            return Err(FlussLakeError::InvalidReadContext(
                "task lake snapshot does not match the frozen context".into(),
            ));
        }
        if !self.partition.matches_keys(context.partition_keys()) {
            return Err(FlussLakeError::InvalidReadContext(
                "task partition layout mismatch".into(),
            ));
        }
        let expected_kv = range.and_then(|range| match range.baseline() {
            crate::FlussLakeBaseline::KvSnapshot { snapshot_id } => Some(snapshot_id),
            _ => None,
        });
        if self.kv_snapshot_id != expected_kv
            && (self.kv_snapshot_id.is_some() || self.lake_splits.is_empty())
        {
            return Err(FlussLakeError::InvalidReadContext(
                "task KV snapshot does not match the frozen context".into(),
            ));
        }
        if self.start_offset != self.stop_offset || self.kv_snapshot_id.is_some() {
            let range = range.ok_or_else(|| {
                FlussLakeError::InvalidReadContext("task log range absent from boundary".into())
            })?;
            if self.start_offset != range.start_offset()
                || self.stop_offset != range.stop_offset()
                || range.partition()
                    != &crate::FlussLakeLogPartition::Ordinary(self.partition.clone())
            {
                return Err(FlussLakeError::InvalidReadContext(
                    "task log range differs from frozen boundary".into(),
                ));
            }
            range.validate_available()?;
        }
        Ok(())
    }
}
