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

//! Lake-format extension points for bounded UnionRead.
//!
//! A source owns lake planning, payload encoding and lake reading, not Fluss
//! log boundaries or scheduling. Each call receives an immutable request.
//! This lets concurrent plans share a source without leaking projection or
//! filter state between queries.

use crate::{FlussLakeError, FlussLakePartitionIdentity, RecordBatchStream, Result};
use arrow::datatypes::SchemaRef;
use fluss::metadata::TableInfo;
use fluss::predicate::{BoundPredicate, CompoundFunction};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};

/// The baseline required by the UnionRead executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LakeReadSemantics {
    /// Every selected append record, without introducing duplicates.
    /// Each planned task must be independently readable; the executor may
    /// schedule tasks from the same bucket concurrently.
    Append,
    /// A snapshot current view: at most one live row per full primary key.
    ///
    /// The source must apply lake-specific version, merge and deletion rules
    /// across the entire supplied split group. Output need not be sorted.
    PrimaryKey,
}

/// One lake-format task. This is not a Fluss log range or an engine thread.
///
/// The backend owns `payload_version` and `payload`, just as a Java LakeSource
/// owns its split serializer. Payloads must not contain credentials. Another
/// reader may consume these tasks only if it implements the same payload
/// contract; the envelope is not a universal file-list format.
///
/// Primary-key reconciliation requires Fluss-aligned partition/bucket tasks.
/// Append lake tasks are independent of Fluss log buckets and may use their
/// own fixed layout or the bucket-unaware pair `(-1, -1)`.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LakeSplit {
    /// Lake format identifier, matching [`LakeSource::format`].
    pub format: String,
    /// Frozen lake snapshot from which this task was planned.
    pub snapshot_id: i64,
    /// Logical Fluss partition identity, including lake-only expired partitions.
    pub partition: FlussLakePartitionIdentity,
    /// Lake bucket identity, or -1 for a bucket-unaware append task.
    pub bucket_id: i32,
    /// Number of lake buckets in this partition, or -1 for a bucket-unaware task.
    pub bucket_count: i32,
    /// Estimated input rows for this task; `None` means unknown.
    /// This is not necessarily the number of rows in a PK current view.
    pub estimated_rows: Option<usize>,
    /// Estimated input size in bytes; `None` means unknown, not zero.
    pub estimated_size: Option<usize>,
    /// Nonzero, format-owned payload version interpreted by the matching LakeSource.
    pub payload_version: u32,
    /// Nonempty task description decoded only by a compatible lake source.
    /// Preserve snapshot-specific deletion files and their data-file associations;
    /// readers applying position-based deletion vectors must preserve row positions.
    /// Must not contain storage credentials or other runtime secrets.
    pub payload: Vec<u8>,
}

impl LakeSplit {
    pub(crate) fn validate(&self, format: &str, snapshot_id: i64) -> Result<()> {
        if self.format != format
            || self.snapshot_id != snapshot_id
            || !self.has_valid_bucket()
            || self.payload_version == 0
            || self.payload.is_empty()
        {
            return Err(FlussLakeError::PlanningFailed(
                "lake split has incompatible format, snapshot, bucket or payload".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn has_valid_bucket(&self) -> bool {
        (self.bucket_id == -1 && self.bucket_count == -1)
            || (self.bucket_id >= 0 && self.bucket_count > self.bucket_id)
    }
}

impl Debug for LakeSplit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LakeSplit")
            .field("format", &self.format)
            .field("snapshot_id", &self.snapshot_id)
            .field("partition", &self.partition)
            .field("bucket_id", &self.bucket_id)
            .field("bucket_count", &self.bucket_count)
            .field("payload_version", &self.payload_version)
            .field("payload_size", &self.payload.len())
            .finish_non_exhaustive()
    }
}

/// Immutable input to lake planning. The snapshot must never be refreshed.
pub struct LakePlannerContext<'a> {
    /// Frozen Fluss schema and layout, with runtime lake properties supplied locally.
    pub table_info: &'a TableInfo,
    /// Lake snapshot to plan; the source must not substitute a newer snapshot.
    pub snapshot_id: i64,
    /// Required baseline semantics, independent of execution parallelism.
    pub semantics: LakeReadSemantics,
    /// Whether the baseline must also support the default deduplicate tail
    /// overlay. False for append and lake-only reads.
    pub reconcile_primary_key: bool,
    /// Only predicates safe before UnionRead reconciliation are supplied.
    /// Backends may ignore them; the executor always evaluates the full filter.
    pub filter: &'a BoundPredicate,
}

/// Immutable lake-reader input from frozen tasks and the execution-side scan settings.
pub struct LakeReaderContext<'a> {
    /// Frozen Fluss schema and layout, with runtime lake properties supplied locally.
    pub table_info: &'a TableInfo,
    /// Frozen lake snapshot shared by every supplied task.
    pub snapshot_id: i64,
    /// Baseline semantics the source must preserve across the supplied tasks.
    pub semantics: LakeReadSemantics,
    /// True only when the default executor reconciles a Fluss PK tail.
    /// A lake-only PK baseline need not use the deduplicate merge engine.
    pub reconcile_primary_key: bool,
    /// Tasks for one partition/bucket. Append reads receive one independently
    /// readable task; primary-key reads receive the entire selected group.
    pub splits: &'a [LakeSplit],
    /// Zero-based column indexes in the full Fluss schema, not schema field IDs.
    /// Includes hidden filter and primary-key columns. Return columns in this order.
    pub projection: &'a [usize],
    /// Physical Arrow schema in `projection` order, to use for returned batches.
    /// Includes hidden columns and may differ from the final scan output schema.
    pub schema: SchemaRef,
    /// A safe optional optimization, not permission to change the result.
    pub filter: &'a BoundPredicate,
}

/// A lake backend that owns task planning and baseline reading.
///
/// `plan` and `read` are asynchronous rather than returning stateless factory
/// wrappers. Implementations must not store request-specific mutable state.
/// A read returns owned batches and must release I/O resources when its future
/// or stream is dropped. Missing frozen inputs must fail, never fall back to
/// a newer snapshot or skip files. Unknown payload versions must be rejected.
///
/// This is a Rust extension API, not a stable binary ABI. Engines owning their
/// complete execution can instead use source metadata/log APIs and need not
/// implement this trait or adopt the default logical splits.
pub trait LakeSource: Send + Sync {
    /// Lake format identifier matching the Fluss table configuration.
    fn format(&self) -> &str;

    /// Plan every selected lake partition, including lake-only partitions.
    fn plan<'a>(&'a self, context: LakePlannerContext<'a>)
    -> BoxFuture<'a, Result<Vec<LakeSplit>>>;

    /// Read the specified group under its required baseline semantics.
    fn read<'a>(
        &'a self,
        context: LakeReaderContext<'a>,
    ) -> BoxFuture<'a, Result<RecordBatchStream>>;
}

/// Selects the part of the exact core predicate that is safe to evaluate
/// inside a lake baseline reader.
///
/// Append and lake-only reads may push the complete predicate. During a
/// primary-key UnionRead, only primary-key predicates are immutable across
/// the lake baseline and changelog tail. Mixed top-level `AND` expressions
/// therefore contribute only their safe key conjuncts; `OR` is pushed only
/// when every branch is key-only.
pub(crate) fn lake_pushdown_filter(
    predicate: &BoundPredicate,
    table_info: &TableInfo,
    reconcile_primary_key: bool,
) -> Option<BoundPredicate> {
    if matches!(predicate, BoundPredicate::AlwaysTrue) {
        return None;
    }
    if !reconcile_primary_key {
        return Some(predicate.clone());
    }

    let primary_key_indexes: HashSet<usize> = table_info
        .primary_keys
        .iter()
        .filter_map(|key| table_info.row_type().get_field_index(key))
        .collect();
    if primary_key_indexes.len() != table_info.primary_keys.len() {
        return None;
    }
    project_predicate_to_fields(predicate, &primary_key_indexes)
}

fn project_predicate_to_fields(
    predicate: &BoundPredicate,
    allowed_fields: &HashSet<usize>,
) -> Option<BoundPredicate> {
    match predicate {
        BoundPredicate::AlwaysTrue => None,
        BoundPredicate::Leaf { field_index, .. } => allowed_fields
            .contains(field_index)
            .then(|| predicate.clone()),
        BoundPredicate::Compound {
            function: CompoundFunction::And,
            children,
        } => {
            let children: Vec<_> = children
                .iter()
                .filter_map(|child| project_predicate_to_fields(child, allowed_fields))
                .collect();
            (!children.is_empty()).then_some(BoundPredicate::Compound {
                function: CompoundFunction::And,
                children,
            })
        }
        BoundPredicate::Compound {
            function: CompoundFunction::Or,
            children,
        } => {
            let children: Option<Vec<_>> = children
                .iter()
                .map(|child| project_predicate_to_fields(child, allowed_fields))
                .collect();
            Some(BoundPredicate::Compound {
                function: CompoundFunction::Or,
                children: children?,
            })
        }
    }
}

#[cfg(test)]
pub(crate) fn testing_split() -> LakeSplit {
    LakeSplit {
        format: "paimon".into(),
        snapshot_id: 42,
        partition: FlussLakePartitionIdentity::Unpartitioned,
        bucket_id: 0,
        bucket_count: 4,
        estimated_rows: Some(3),
        estimated_size: None,
        payload_version: 1,
        payload: b"testing-task".to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataTypes, Schema, TablePath};
    use fluss::predicate::col;
    use std::collections::HashMap;

    #[test]
    fn safe_pushdown_does_not_expose_mutable_pk_filters() {
        let info = TableInfo::new(
            TablePath::new("db", "table"),
            7,
            1,
            Schema::builder()
                .column("id", DataTypes::int())
                .column("value", DataTypes::int())
                .primary_key(["id"])
                .unwrap()
                .build()
                .unwrap(),
            vec!["id".into()],
            vec![].into(),
            1,
            HashMap::new(),
            HashMap::new(),
            None,
            0,
            0,
        );
        let filter = BoundPredicate::bind(
            Some(&col("id").eq(1).and(col("value").gt(10))),
            info.row_type(),
        )
        .unwrap();
        assert_eq!(
            lake_pushdown_filter(&filter, &info, true)
                .unwrap()
                .referenced_field_indexes(),
            vec![0]
        );
        assert_eq!(
            lake_pushdown_filter(&filter, &info, false)
                .unwrap()
                .referenced_field_indexes(),
            vec![0, 1]
        );
        let filter = BoundPredicate::bind(
            Some(&col("id").eq(1).or(col("value").gt(10))),
            info.row_type(),
        )
        .unwrap();
        assert!(lake_pushdown_filter(&filter, &info, true).is_none());
    }
}
