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

use super::consumer_group::AutoCommit;
use super::*;
use crate::client::consumer_offsets::invalid;
use crate::client::{
    ConsumerGroupOptions, OffsetCommitError, OffsetCommitHandle, OffsetCommitResult,
};
use std::sync::atomic::AtomicUsize;

/// Configures manually assigned scans over several tables using one commit queue.
/// Each table retains its own schema. No membership or automatic assignment is performed.
pub struct MultiTableScan<'a> {
    connection: &'a FlussConnection,
    tables: Vec<TableInfo>,
    group: ConsumerGroupOptions,
    auto_commit: Option<Duration>,
}

impl FlussConnection {
    /// Resolves the selected tables before constructing a multi-table scanner.
    pub async fn new_multi_table_scan(
        &self,
        tables: &[TablePath],
        group: ConsumerGroupOptions,
    ) -> Result<MultiTableScan<'_>> {
        group.validate()?;
        if tables.is_empty() || tables.iter().collect::<HashSet<_>>().len() != tables.len() {
            return Err(invalid("Multi-table scan needs distinct, nonempty tables"));
        }
        let admin = self.get_admin()?;
        let mut infos = Vec::new();
        for path in tables {
            infos.push(admin.get_table_info(path).await?);
        }
        Ok(MultiTableScan {
            connection: self,
            tables: infos,
            group,
            auto_commit: None,
        })
    }
}

impl MultiTableScan<'_> {
    /// Table metadata captured for this scan. Use these schemas to decode returned records;
    /// a later Admin lookup may observe a newer schema than the scanner uses.
    pub fn table_infos(&self) -> &[TableInfo] {
        &self.tables
    }

    /// Enables poll-triggered commits of the previous completed polls across all tables.
    /// Process all returned records before polling again. Drop does not perform a final commit.
    pub fn with_auto_commit(mut self, interval: Duration) -> Result<Self> {
        AutoCommit::new(interval)?;
        self.auto_commit = Some(interval);
        Ok(self)
    }

    fn build(self, batch: bool) -> Result<MultiTableInner> {
        let group = Arc::new(ConsumerGroup::new(
            self.group,
            self.connection.get_connections(),
            self.connection.get_metadata(),
        ));
        let mut scanners = Vec::new();
        for info in self.tables {
            let scan = TableScan::new(self.connection, info, self.connection.get_metadata());
            let mut inner = if batch {
                scan.create_record_batch_log_scanner()?.inner
            } else {
                scan.create_log_scanner()?.inner
            };
            Arc::get_mut(&mut inner)
                .expect("new scanner is unshared")
                .consumer_group = Some(group.clone());
            scanners.push(inner);
        }
        Ok(MultiTableInner {
            scanners,
            group,
            auto_commit: self.auto_commit.map(AutoCommit::new).transpose()?,
            next: AtomicUsize::new(0),
        })
    }

    /// Creates a record scanner; record buckets identify their source table.
    pub fn create_log_scanner(self) -> Result<MultiTableLogScanner> {
        Ok(MultiTableLogScanner {
            inner: self.build(false)?,
        })
    }

    /// Creates an Arrow scanner. Every batch carries its table bucket and schema;
    /// tables must support Arrow log scans.
    pub fn create_record_batch_log_scanner(self) -> Result<MultiTableRecordBatchLogScanner> {
        Ok(MultiTableRecordBatchLogScanner {
            inner: self.build(true)?,
        })
    }
}

struct MultiTableInner {
    scanners: Vec<Arc<LogScannerInner>>,
    group: Arc<ConsumerGroup>,
    auto_commit: Option<AutoCommit>,
    next: AtomicUsize,
}

impl MultiTableInner {
    fn scanner(&self, id: i64) -> Result<&LogScannerInner> {
        self.scanners
            .iter()
            .find(|scanner| scanner.table_id == id)
            .map(AsRef::as_ref)
            .ok_or_else(|| invalid("Bucket table is not part of this multi-table scanner"))
    }

    async fn subscribe(
        &self,
        buckets: &[TableBucket],
        initializer: &OffsetsInitializer,
    ) -> Result<()> {
        if buckets.is_empty() || buckets.iter().collect::<HashSet<_>>().len() != buckets.len() {
            return Err(invalid("Assignment needs distinct, nonempty buckets"));
        }
        let mut tables: HashMap<i64, Vec<TableBucket>> = HashMap::new();
        for bucket in buckets {
            self.scanner(bucket.table_id())?
                .validate_group_buckets(std::iter::once(bucket))?;
            tables
                .entry(bucket.table_id())
                .or_default()
                .push(bucket.clone());
        }
        let mut resolved = Vec::new();
        for (id, buckets) in tables {
            let scanner = self.scanner(id)?;
            let offsets = scanner.resolve_initializer(&buckets, initializer).await?;
            scanner.prepare_bucket_metadata(&offsets).await?;
            resolved.push((scanner, offsets));
        }
        // No await between assignment changes. All fallbacks and metadata have resolved.
        for (scanner, offsets) in resolved {
            scanner.assign_buckets(offsets);
        }
        Ok(())
    }
}

/// Single-consumer record scanner across tables. Poll, assignment, and commits must be
/// driven by one task. Poll returns data from a ready table with round-robin fairness.
pub struct MultiTableLogScanner {
    inner: MultiTableInner,
}
/// Single-consumer Arrow scanner across tables, retaining each table's batch schema.
pub struct MultiTableRecordBatchLogScanner {
    inner: MultiTableInner,
}

macro_rules! multi_methods {
    ($type:ty, $result:ty, $poll:ident, $empty:expr, $output:expr) => {
        impl $type {
            /// Polls fairly across tables. Process the previous result before polling again
            /// when auto-commit is enabled. An error never discards another table's returned data.
            pub async fn poll(&self, timeout: Duration) -> Result<$result> {
                if let Some(auto) = &self.inner.auto_commit {
                    auto.before_poll(&self.inner.group).await?;
                }
                let deadline = tokio::time::Instant::now() + timeout;
                loop {
                    for _ in 0..self.inner.scanners.len() {
                        let index = self.inner.next.fetch_add(1, Ordering::Relaxed)
                            % self.inner.scanners.len();
                        let result = self.inner.scanners[index].$poll(Duration::ZERO).await?;
                        if !result.is_empty() {
                            return Ok(($output)(result));
                        }
                    }
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Ok($empty);
                    }
                    tokio::time::sleep((deadline - now).min(Duration::from_millis(10))).await;
                }
            }

            /// Restores starting positions across tables before changing any subscriptions.
            /// Other assigned buckets are retained. Use this method again to seek.
            pub async fn subscribe_with_initializer(
                &self,
                buckets: &[TableBucket],
                initializer: &OffsetsInitializer,
            ) -> Result<()> {
                self.inner.subscribe(buckets, initializer).await
            }

            /// Removes one bucket from the manual assignment without committing it.
            pub async fn unsubscribe(&self, bucket: &TableBucket) -> Result<()> {
                let scanner = self.inner.scanner(bucket.table_id())?;
                match bucket.partition_id() {
                    Some(partition) => {
                        scanner
                            .unsubscribe_partition(partition, bucket.bucket_id())
                            .await
                    }
                    None => scanner.unsubscribe(bucket.bucket_id()).await,
                }
            }

            /// Enqueues all delivered next offsets across assigned tables, in scanner call order.
            pub fn commit_async(&self) -> Result<OffsetCommitHandle> {
                self.commit_offsets_async(self.inner.group.delivered.lock().clone())
            }

            /// Enqueues explicit offsets across tables, preserving partial failures and unknown
            /// outcomes. No fencing is provided against other scanners or processes.
            pub fn commit_offsets_async(
                &self,
                offsets: HashMap<TableBucket, OffsetAndMetadata>,
            ) -> Result<OffsetCommitHandle> {
                for bucket in offsets.keys() {
                    self.inner
                        .scanner(bucket.table_id())?
                        .validate_group_buckets(std::iter::once(bucket))?;
                }
                self.inner.group.enqueue(offsets)
            }

            /// Commits delivered positions. Also use after processing the last poll and before
            /// shutdown: dropping a scanner never commits implicitly. Inspect every bucket result.
            pub async fn commit_sync(
                &self,
            ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
                self.commit_async()
                    .map_err(OffsetCommitError::Rejected)?
                    .await?
            }

            /// Commits explicit next offsets. After an unknown outcome, unsafe rewinds are rejected for the affected buckets.
            pub async fn commit_offsets_sync(
                &self,
                offsets: HashMap<TableBucket, OffsetAndMetadata>,
            ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
                self.commit_offsets_async(offsets)
                    .map_err(OffsetCommitError::Rejected)?
                    .await?
            }
        }
    };
}
multi_methods!(
    MultiTableLogScanner,
    ScanRecords,
    poll_records,
    ScanRecords::empty(),
    std::convert::identity
);
multi_methods!(
    MultiTableRecordBatchLogScanner,
    Vec<ScanBatch>,
    poll_batches,
    Vec::new(),
    |p: PolledBatches| p.batches
);
