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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Fixed-scope snapshot bootstrap followed by the logical Fluss changelog.

use crate::error::execution_client_error;
use crate::executor::{PhysicalProjection, predicate_evaluation_error};
use crate::table::validate_pk_union_merge_engine;
use crate::{
    FlussLakeError, FlussLakeReadContext, FlussLakeScan, FlussLakeTable, LakeSource, Result,
};
use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use fluss::client::{FlussTable, ReadMemoryBudget};
use fluss::metadata::{RowType, TableBucket};
use fluss::predicate::{BoundPredicate, Predicate, PredicateEvaluator};
use fluss::record::{ChangeType, ScanRecord, ScanRecords};
use fluss::row::ColumnarRow;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Initial import and subsequent changes for the captured buckets.
pub enum UnionReadEvent {
    /// Rows in the bounded view. Stage these until the bootstrap is complete.
    Snapshot(RecordBatch),
    /// All snapshot rows have been emitted. Commit them with these next offsets.
    SnapshotFinished(HashMap<TableBucket, i64>),
    /// Ordered within each bucket. Commit each poll's [`ScanRecords::consumed_up_to_offset`] with
    /// its output, including progress for buckets with no visible rows.
    Changes(ScanRecords),
}

/// Dropping the stream releases its scanner and pending bootstrap work.
pub type UnionReadStream = BoxStream<'static, Result<UnionReadEvent>>;

/// A fixed-schema, fixed-scope snapshot followed by changes to the same view.
pub struct FlussLakeStreamScan {
    scan: FlussLakeScan,
    memory_budget: Option<Arc<dyn ReadMemoryBudget>>,
}

impl FlussLakeTable {
    /// Start or resume a fixed-scope streaming import.
    pub fn new_stream_scan(&self) -> FlussLakeStreamScan {
        FlussLakeStreamScan {
            scan: self.new_scan(),
            memory_budget: None,
        }
    }
}

impl FlussLakeStreamScan {
    /// Use the same lake source as the corresponding bounded scan.
    pub fn with_lake_source(mut self, source: Arc<dyn LakeSource>) -> Self {
        self.scan = self.scan.with_lake_source(source);
        self
    }

    /// Project output columns in order. Missing primary-key columns follow the
    /// requested columns so both snapshot rows and deletes identify their key.
    pub fn with_projection(mut self, fields: Vec<usize>) -> Self {
        self.scan = self.scan.with_projection(fields);
        self
    }

    /// Select output columns by name, with the same key retention as `with_projection`.
    pub fn with_projection_by_names(mut self, names: Vec<String>) -> Self {
        self.scan = self.scan.with_projection_by_names(names);
        self
    }

    /// Filter the snapshot and subsequent changes. With a mutable-field filter on
    /// a WAL PK table, apply positives as upserts and deletes by key, even if the
    /// key is absent. A delete does not carry a previous row image.
    pub fn with_filter(mut self, predicate: Predicate) -> Self {
        self.scan = self.scan.with_filter(predicate);
        self
    }

    fn keyed_scan(&self, context: &FlussLakeReadContext) -> Result<FlussLakeScan> {
        let schema = context.table_schema();
        let scan = self.scan.clone();
        Ok(match scan.resolve_projection(schema.row_type())? {
            Some(mut projection) => {
                for key in schema.primary_key_indexes() {
                    if !projection.contains(&key) {
                        projection.push(key);
                    }
                }
                scan.with_projection(projection)
            }
            None => scan,
        })
    }

    /// Limit retained PK state during bootstrap, as on the bounded reader.
    pub fn with_memory_budget(mut self, budget: Arc<dyn ReadMemoryBudget>) -> Self {
        self.memory_budget = Some(budget);
        self
    }

    /// Bootstrap from a captured boundary; then follow from each exclusive stop.
    pub async fn start(&self, context: &FlussLakeReadContext) -> Result<UnionReadStream> {
        let scan = self.keyed_scan(context)?;
        let plan = scan.plan_with_context(context).await?;
        let mut reader = scan.new_reader();
        if let Some(budget) = &self.memory_budget {
            reader = reader.with_memory_budget(budget.clone());
        }
        let snapshot = reader.read_splits(plan.splits())?;
        let offsets: HashMap<_, _> = context
            .log_ranges()
            .iter()
            .map(|r| (r.table_bucket().clone(), r.stop_offset()))
            .collect();
        let changes = Self::follow(&scan, context, offsets.clone()).await?;
        let marker = stream::iter([Ok(UnionReadEvent::SnapshotFinished(offsets))]);
        Ok(crate::reader::stop_after_first_error(
            snapshot
                .map_ok(UnionReadEvent::Snapshot)
                .chain(marker)
                .chain(changes)
                .boxed(),
        ))
    }

    /// Resume only after the bootstrap was committed. Offsets are the next unread
    /// positions for exactly the buckets in the original context. Restore the original
    /// projection and predicate; changing the view requires a fresh bootstrap.
    pub async fn resume(
        &self,
        context: &FlussLakeReadContext,
        offsets: HashMap<TableBucket, i64>,
    ) -> Result<UnionReadStream> {
        Self::follow(&self.keyed_scan(context)?, context, offsets).await
    }

    async fn follow(
        scan: &FlussLakeScan,
        context: &FlussLakeReadContext,
        offsets: HashMap<TableBucket, i64>,
    ) -> Result<UnionReadStream> {
        let ranges = context.log_ranges();
        if context.is_lake_only()
            || offsets.len() != ranges.len()
            || ranges.iter().any(|r| {
                offsets
                    .get(r.table_bucket())
                    .is_none_or(|o| *o < r.stop_offset())
            })
        {
            return Err(FlussLakeError::InvalidReadContext(
                "follow offsets must cover the captured buckets at or after their stops".into(),
            ));
        }
        let connection = scan.connection();
        let live = connection
            .get_table(scan.table_path())
            .await
            .map_err(|e| execution_client_error("open follow table", e))?;
        let info = context.table_info(live.get_table_info())?;
        if info.has_primary_key() {
            validate_pk_union_merge_engine(&info)?;
        }
        let row_type = info.row_type();
        let bound = scan.bind(row_type)?;
        let physical =
            PhysicalProjection::resolve(row_type, bound.projection.as_deref(), &bound.filter, &[])?;
        let is_pk = info.has_primary_key();
        let wal = is_pk
            && info
                .properties
                .get("table.changelog.image")
                .is_some_and(|s| s.eq_ignore_ascii_case("wal"));
        let keys = info.schema.primary_key_indexes();
        let mutable_wal_filter = wal
            && bound
                .filter
                .referenced_field_indexes()
                .iter()
                .any(|i| !keys.contains(i));
        let pushdown = if is_pk {
            None
        } else {
            bound.filter.to_pushdown_predicate()
        };
        let output = bound
            .projection
            .as_ref()
            .filter(|p| p.len() < physical.field_indexes.len())
            .map(|p| {
                row_type
                    .project(p)
                    .map(|r| (Arc::new(r), (0..p.len()).collect::<Vec<_>>()))
            })
            .transpose()
            .map_err(predicate_evaluation_error)?;
        let filter = match bound.filter {
            BoundPredicate::AlwaysTrue => None,
            f => Some(f.into_evaluator().map_err(predicate_evaluation_error)?),
        };
        if offsets.is_empty() {
            return Ok(stream::empty().boxed());
        }
        let table = FlussTable::new(connection, connection.get_metadata(), info);
        let mut log = table.new_scan();
        if bound.projection.is_some() {
            log = log
                .project(&physical.field_indexes)
                .map_err(predicate_evaluation_error)?;
        }
        if let Some(prune) = pushdown {
            log = log.filter(prune).map_err(predicate_evaluation_error)?;
        }
        let transform = FollowRows {
            filter,
            output,
            mutable_wal_filter,
        };
        let scanner = log
            .create_log_scanner()
            .map_err(|e| execution_client_error("create follow scanner", e))?;
        if live.get_table_info().is_partitioned() {
            let assignments = offsets
                .iter()
                .map(|(b, o)| {
                    let id = b.partition_id().ok_or_else(|| {
                        FlussLakeError::InvalidReadContext("missing follow partition ID".into())
                    })?;
                    Ok(((id, b.bucket_id()), *o))
                })
                .collect::<Result<_>>()?;
            scanner.subscribe_partition_buckets(&assignments).await
        } else {
            scanner
                .subscribe_buckets(&offsets.iter().map(|(b, o)| (b.bucket_id(), *o)).collect())
                .await
        }
        .map_err(|e| execution_client_error("subscribe follow buckets", e))?;
        Ok(
            stream::try_unfold((scanner, transform), |(scanner, transform)| async move {
                loop {
                    let changes = scanner
                        .poll_frozen(Duration::from_millis(500))
                        .await
                        .map_err(|e| execution_client_error("read live changelog", e))?;
                    if !changes.is_empty() {
                        let changes = if transform.filter.is_some() || transform.output.is_some() {
                            changes.try_map_bucket_records(|rows| transform.apply(rows))?
                        } else {
                            changes
                        };
                        return Ok(Some((
                            UnionReadEvent::Changes(changes),
                            (scanner, transform),
                        )));
                    }
                }
            })
            .boxed(),
        )
    }
}

struct FollowRows {
    filter: Option<PredicateEvaluator>,
    output: Option<(Arc<RowType>, Vec<usize>)>,
    mutable_wal_filter: bool,
}

impl FollowRows {
    fn apply(&self, rows: &[ScanRecord]) -> Result<Vec<ScanRecord>> {
        let mut result = Vec::with_capacity(rows.len());
        // Rows from one decoded Arrow batch share its predicate mask and projected cursor.
        for group in rows.chunk_by(|a, b| {
            a.row()
                .get_record_batch()
                .zip(b.row().get_record_batch())
                .is_some_and(|(a, b)| std::ptr::eq(a, b))
        }) {
            let batch = group[0].row().get_record_batch().ok_or_else(|| {
                FlussLakeError::Internal("follow record has no backing batch".into())
            })?;
            let mask = self
                .filter
                .as_ref()
                .map(|f| f.evaluate_batch(batch))
                .transpose()
                .map_err(predicate_evaluation_error)?;
            let cursor = self
                .output
                .as_ref()
                .map(|(row_type, columns)| {
                    let batch = batch
                        .project(columns)
                        .map_err(|e| FlussLakeError::SchemaIncompatible(e.to_string()))?;
                    ColumnarRow::new(Arc::new(batch), row_type.clone(), 0, None)
                        .map_err(predicate_evaluation_error)
                })
                .transpose()?;
            for record in group {
                let index = record.row().get_row_id();
                let matches = mask
                    .as_ref()
                    .is_none_or(|m| m.is_valid(index) && m.value(index));
                let kind = if self.mutable_wal_filter {
                    match record.change_type() {
                        ChangeType::Insert | ChangeType::UpdateAfter if matches => {
                            ChangeType::UpdateAfter
                        }
                        ChangeType::Insert | ChangeType::UpdateAfter | ChangeType::Delete => {
                            ChangeType::Delete
                        }
                        other => {
                            return Err(FlussLakeError::SchemaIncompatible(format!(
                                "unexpected WAL change type: {other}"
                            )));
                        }
                    }
                } else if matches {
                    *record.change_type()
                } else {
                    continue;
                };
                let mut row = cursor.as_ref().unwrap_or(record.row()).clone();
                row.set_row_id(index);
                result.push(ScanRecord::new(
                    row,
                    record.offset(),
                    record.timestamp(),
                    kind,
                ));
            }
        }
        Ok(result)
    }
}
