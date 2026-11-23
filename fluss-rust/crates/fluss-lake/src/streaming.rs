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
use crate::table::validate_pk_union_merge_engine;
use crate::{
    FlussLakeError, FlussLakeReadContext, FlussLakeScan, FlussLakeTable, LakeSource, Result,
};
use arrow::record_batch::RecordBatch;
use fluss::client::{FlussTable, ReadMemoryBudget};
use fluss::metadata::TableBucket;
use fluss::record::ScanRecords;
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

/// Full rows, fixed schema and fixed partition scope. Query filters over mutable
/// values need retractions and are deliberately not exposed on this reader.
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

    /// Limit retained PK state during bootstrap, as on the bounded reader.
    pub fn with_memory_budget(mut self, budget: Arc<dyn ReadMemoryBudget>) -> Self {
        self.memory_budget = Some(budget);
        self
    }

    /// Bootstrap from a captured boundary; then follow from each exclusive stop.
    pub async fn start(&self, context: &FlussLakeReadContext) -> Result<UnionReadStream> {
        let plan = self.scan.plan_with_context(context).await?;
        let mut reader = self.scan.new_reader();
        if let Some(budget) = &self.memory_budget {
            reader = reader.with_memory_budget(budget.clone());
        }
        let snapshot = reader.read_splits(plan.splits())?;
        let offsets: HashMap<_, _> = context
            .log_ranges()
            .iter()
            .map(|r| (r.table_bucket().clone(), r.stop_offset()))
            .collect();
        let changes = self.resume(context, offsets.clone()).await?;
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
    /// positions for exactly the buckets in the original context.
    pub async fn resume(
        &self,
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
        let connection = self.scan.connection();
        let live = connection
            .get_table(self.scan.table_path())
            .await
            .map_err(|e| execution_client_error("open follow table", e))?;
        let info = context.table_info(live.get_table_info())?;
        if info.has_primary_key() {
            validate_pk_union_merge_engine(&info)?;
        }
        if offsets.is_empty() {
            return Ok(stream::empty().boxed());
        }
        let table = FlussTable::new(connection, connection.get_metadata(), info);
        let scanner = table
            .new_scan()
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
        Ok(stream::try_unfold(scanner, |scanner| async move {
            loop {
                let changes = scanner
                    .poll_frozen(Duration::from_millis(500))
                    .await
                    .map_err(|e| execution_client_error("read live changelog", e))?;
                if !changes.is_empty() {
                    return Ok(Some((UnionReadEvent::Changes(changes), scanner)));
                }
            }
        })
        .boxed())
    }
}
