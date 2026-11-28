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
use crate::table::full_changelog;
use crate::{FlussLakeError, FlussLakeReadContext, FlussLakeScan, Result};
use arrow::record_batch::RecordBatch;
use fluss::client::{FlussTable, FollowLogEvent, ReadMemoryBudget};
use fluss::metadata::TableBucket;
use fluss::predicate::{BoundPredicate, PredicateEvaluator};
use fluss::record::{ChangeType, ScanBatch};
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;

/// Progress after a committed bootstrap. Persist both fields with the sink output.
#[derive(Clone, Debug)]
pub struct UnionReadCheckpoint {
    /// Identity of the frozen boundary, exact filter, and ordered output columns.
    pub view_digest: [u8; 32],
    /// Next unread offset per bucket, including advances from empty output batches.
    pub next_offsets: HashMap<TableBucket, i64>,
}

/// Initial import and subsequent changes for the captured buckets.
pub enum UnionReadEvent {
    /// Rows in the bounded view. Stage these until the bootstrap is complete.
    Snapshot(RecordBatch),
    /// All snapshot rows have been emitted. Commit them with these next offsets.
    SnapshotFinished(UnionReadCheckpoint),
    /// Commit all batches and their exclusive source offsets together. Progress can
    /// advance even when no row passes the predicate.
    Changes {
        batches: Vec<UnionReadChangeBatch>,
        next_offsets: HashMap<TableBucket, i64>,
    },
    /// Remove these buckets from the checkpoint. The positions are the next unread
    /// offsets, not proof that the dropped partitions were fully consumed.
    PartitionsDropped(HashMap<TableBucket, i64>),
}

/// A physically filtered and projected Arrow batch from one bucket.
pub struct UnionReadChangeBatch {
    pub bucket: TableBucket,
    pub batch: RecordBatch,
    /// One kind per output row for PK tables; absent for append tables.
    pub change_types: Option<Vec<ChangeType>>,
}

/// Dropping the stream releases its scanner and pending bootstrap work.
pub type UnionReadStream = BoxStream<'static, Result<UnionReadEvent>>;

/// A fixed-schema, fixed-scope snapshot followed by changes to the same view.
pub struct FlussLakeStreamReader {
    scan: FlussLakeScan,
    memory_budget: Option<Arc<dyn ReadMemoryBudget>>,
}

impl FlussLakeScan {
    /// Open a snapshot-and-follow reader using this scan's configuration.
    pub fn new_stream(&self) -> FlussLakeStreamReader {
        FlussLakeStreamReader {
            scan: self.clone(),
            memory_budget: None,
        }
    }
}

impl FlussLakeStreamReader {
    fn view_digest(&self, context: &FlussLakeReadContext) -> Result<[u8; 32]> {
        let mut boundary = context.to_protobuf()?;
        boundary
            .log_ranges
            .sort_by_key(|r| (r.partition_id, r.bucket_id));
        let schema = context.table_schema();
        let bound = self.scan.bind(schema.row_type())?;
        let mut digest = Sha256::new();
        digest.update(Sha256::digest(boundary.encode_to_vec()));
        digest.update(bound.filter_digest);
        for index in bound
            .projection
            .unwrap_or_else(|| (0..schema.columns().len()).collect())
        {
            digest.update(schema.columns()[index].id().to_be_bytes());
            digest.update((index as u64).to_be_bytes());
        }
        Ok(digest.finalize().into())
    }

    /// Limit retained PK state during bootstrap, as on the bounded reader.
    pub fn with_memory_budget(mut self, budget: Arc<dyn ReadMemoryBudget>) -> Self {
        self.memory_budget = Some(budget);
        self
    }

    /// Bootstrap from a captured boundary; then follow from each exclusive stop.
    pub async fn start(&self, context: &FlussLakeReadContext) -> Result<UnionReadStream> {
        let scan = &self.scan;
        let (changes, offsets) = self.follow(context, None).await?;
        let plan = scan.plan_with_context(context).await?;
        let mut reader = scan.new_reader();
        if let Some(budget) = &self.memory_budget {
            reader = reader.with_memory_budget(budget.clone());
        }
        let snapshot = reader.read_splits(plan.splits())?;
        let checkpoint = UnionReadCheckpoint {
            view_digest: self.view_digest(context)?,
            next_offsets: offsets,
        };
        let marker = stream::iter([Ok(UnionReadEvent::SnapshotFinished(checkpoint))]);
        Ok(crate::reader::stop_after_first_error(
            snapshot
                .map_ok(UnionReadEvent::Snapshot)
                .chain(marker)
                .chain(changes)
                .boxed(),
        ))
    }

    /// Resume only after the bootstrap was committed. Offsets are the next unread
    /// positions for the selected buckets, excluding confirmed partition drops. Restore the original
    /// projection and predicate; changing the view requires a fresh bootstrap.
    pub async fn resume(
        &self,
        context: &FlussLakeReadContext,
        checkpoint: UnionReadCheckpoint,
    ) -> Result<UnionReadStream> {
        if self.view_digest(context)? != checkpoint.view_digest {
            return Err(FlussLakeError::InvalidReadContext(
                "stream checkpoint belongs to a different boundary, filter or projection".into(),
            ));
        }
        Ok(self.follow(context, Some(checkpoint.next_offsets)).await?.0)
    }

    async fn follow(
        &self,
        context: &FlussLakeReadContext,
        resume: Option<HashMap<TableBucket, i64>>,
    ) -> Result<(UnionReadStream, HashMap<TableBucket, i64>)> {
        let scan = &self.scan;
        scan.validate_configuration()?;
        if scan.lake_only() || context.is_lake_only() || scan.table_path() != context.table_path() {
            return Err(FlussLakeError::InvalidReadContext(
                "follow requires a union boundary for the same table".into(),
            ));
        }
        let connection = scan.connection();
        let admin = connection
            .get_admin()
            .map_err(|e| execution_client_error("open admin", e))?;
        let info = context.table_info(
            &admin
                .get_table_info(scan.table_path())
                .await
                .map_err(|e| execution_client_error("open follow table", e))?,
        )?;
        if info.has_primary_key() && !full_changelog(&info) {
            return Err(FlussLakeError::PlanningFailed(
                "streaming UnionRead on a primary-key table requires table.changelog.image=FULL"
                    .into(),
            ));
        }
        let row_type = info.row_type();
        let bound = scan.bind(row_type)?;
        let offsets: HashMap<_, _> = {
            let mut select = crate::planner::bucket_selector(context, &info, &bound.filter)?;
            let partitions = crate::pruning::PartitionPruner::new(
                row_type,
                context.partition_keys(),
                &bound.filter,
            );
            context
                .log_ranges()
                .iter()
                .filter(|r| {
                    let crate::FlussLakeLogPartition::Ordinary(p) = r.partition();
                    select(r.table_bucket().bucket_id(), r.bucket_count())
                        && partitions.partition_identity_may_match(p)
                })
                .map(|r| (r.table_bucket().clone(), r.stop_offset()))
                .collect()
        };
        let offsets = if let Some(restored) = resume {
            if restored
                .iter()
                .any(|(b, o)| offsets.get(b).is_none_or(|min| o < min))
            {
                return Err(FlussLakeError::InvalidReadContext(
                    "invalid follow checkpoint bucket or offset".into(),
                ));
            }
            if restored.len() != offsets.len() {
                if !info.is_partitioned() {
                    return Err(FlussLakeError::InvalidReadContext(
                        "follow checkpoint omits a live bucket".into(),
                    ));
                }
                // An omitted bucket is valid only if its original partition ID is gone.
                let live = admin
                    .list_partition_infos(scan.table_path())
                    .await
                    .map_err(|e| execution_client_error("check retired partitions", e))?;
                context.validate_table(
                    &admin
                        .get_table_info(scan.table_path())
                        .await
                        .map_err(|e| execution_client_error("recheck follow table", e))?,
                )?;
                let ids: std::collections::HashSet<_> =
                    live.iter().map(|p| p.get_partition_id()).collect();
                if offsets.keys().any(|b| {
                    !restored.contains_key(b) && b.partition_id().is_none_or(|id| ids.contains(&id))
                }) {
                    return Err(FlussLakeError::InvalidReadContext(
                        "follow checkpoint omits a live bucket".into(),
                    ));
                }
            }
            restored
        } else {
            offsets
        };
        let physical =
            PhysicalProjection::resolve(row_type, bound.projection.as_deref(), &bound.filter, &[])?;
        let projection = bound
            .projection
            .as_ref()
            .filter(|p| p.len() != physical.field_indexes.len())
            .map(|p| (0..p.len()).collect());
        let table = FlussTable::new(connection, connection.get_metadata(), info);
        let mut log = table.new_scan();
        if bound.projection.is_some() {
            log = log
                .project(&physical.field_indexes)
                .map_err(predicate_evaluation_error)?;
        }
        if !table.get_table_info().has_primary_key()
            && let Some(filter) = bound.filter.to_pushdown_predicate()
        {
            log = log.filter(filter).map_err(predicate_evaluation_error)?;
        }
        let reader = log
            .create_follow_log_reader(offsets.clone())
            .map_err(|e| execution_client_error("open follow reader", e))?;
        let transform = FollowBatches {
            filter: match bound.filter {
                BoundPredicate::AlwaysTrue => None,
                f => Some(f.into_evaluator().map_err(predicate_evaluation_error)?),
            },
            projection,
            max_size: scan.max_batch_size(),
        };
        let changes =
            stream::try_unfold((reader, transform), |(mut reader, transform)| async move {
                let event = match reader
                    .next_event()
                    .await
                    .map_err(|e| execution_client_error("read live changelog", e))?
                {
                    None => return Ok(None),
                    Some(FollowLogEvent::PartitionsDropped(d)) => {
                        UnionReadEvent::PartitionsDropped(d)
                    }
                    Some(FollowLogEvent::Data {
                        batches,
                        next_offsets,
                    }) => {
                        let mut output = Vec::new();
                        for batch in batches {
                            transform.apply(batch, &mut output)?;
                        }
                        UnionReadEvent::Changes {
                            batches: output,
                            next_offsets,
                        }
                    }
                };
                Ok(Some((event, (reader, transform))))
            })
            .boxed();
        Ok((changes, offsets))
    }
}

struct FollowBatches {
    filter: Option<PredicateEvaluator>,
    projection: Option<Vec<usize>>,
    max_size: Option<usize>,
}

impl FollowBatches {
    fn apply(&self, source: ScanBatch, out: &mut Vec<UnionReadChangeBatch>) -> Result<()> {
        let bucket = source.bucket().clone();
        let mask = self
            .filter
            .as_ref()
            .map(|f| f.evaluate_batch(source.batch()))
            .transpose()
            .map_err(predicate_evaluation_error)?;
        let types = source.change_types().map(|kinds| match &mask {
            Some(m) => kinds
                .iter()
                .zip(m.iter())
                .filter_map(|(k, pass)| (pass == Some(true)).then_some(*k))
                .collect::<Vec<_>>(),
            None => kinds.to_vec(),
        });
        let Some(batch) = crate::executor::select_output_rows(
            source.into_batch(),
            mask.as_ref(),
            self.projection.as_deref(),
        )?
        else {
            return Ok(());
        };
        let limit = self.max_size.unwrap_or(usize::MAX);
        if batch.num_rows() <= limit {
            if batch.num_rows() > 0 {
                out.push(UnionReadChangeBatch {
                    bucket,
                    batch,
                    change_types: types,
                });
            }
        } else {
            for i in (0..batch.num_rows()).step_by(limit) {
                let n = limit.min(batch.num_rows() - i);
                out.push(UnionReadChangeBatch {
                    bucket: bucket.clone(),
                    batch: batch.slice(i, n),
                    change_types: types.as_ref().map(|v| v[i..i + n].to_vec()),
                });
            }
        }
        Ok(())
    }
}
