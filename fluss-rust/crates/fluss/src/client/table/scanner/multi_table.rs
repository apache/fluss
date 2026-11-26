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
        if tables.iter().collect::<HashSet<_>>().len() != tables.len() {
            return Err(invalid("Multi-table scan needs distinct tables"));
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
            auto_commit: Some(DEFAULT_AUTO_COMMIT_INTERVAL),
        })
    }
}

impl MultiTableScan<'_> {
    /// Enables poll-triggered commits of the previous completed polls across all tables.
    /// Process all returned records before polling again. Drop does not perform a final commit.
    pub fn with_auto_commit(mut self, interval: Duration) -> Result<Self> {
        AutoCommit::new(interval)?;
        self.auto_commit = Some(interval);
        Ok(self)
    }

    /// Disables automatic commits.
    pub fn without_auto_commit(mut self) -> Self {
        self.auto_commit = None;
        self
    }

    fn build(self, batch: bool) -> Result<MultiTableInner> {
        let group = Arc::new(ConsumerGroup::new(
            self.group,
            self.connection.get_connections(),
            self.connection.get_metadata(),
        ));
        let mut inner = MultiTableInner {
            scanners: RwLock::new(Arc::default()),
            initial_tables: self.tables.iter().map(|t| t.table_path.clone()).collect(),
            group,
            batch,
            config: self.connection.config().clone(),
            metadata: self.connection.get_metadata(),
            connections: self.connection.get_connections(),
            admin: self.connection.get_admin()?,
            auto_commit: self.auto_commit.map(AutoCommit::new).transpose()?,
            next: AtomicUsize::new(0),
        };
        for info in self.tables {
            let scanner = inner.build_table(&info)?;
            Arc::make_mut(inner.scanners.get_mut()).push(scanner);
        }
        Ok(inner)
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
    batch: bool,
    config: Config,
    metadata: Arc<Metadata>,
    connections: Arc<RpcClient>,
    admin: Arc<crate::client::FlussAdmin>,
    scanners: RwLock<Arc<Vec<Arc<LogScannerInner>>>>,
    initial_tables: HashSet<TablePath>,
    group: Arc<ConsumerGroup>,
    auto_commit: Option<AutoCommit>,
    next: AtomicUsize,
}

impl MultiTableInner {
    fn build_table(&self, info: &TableInfo) -> Result<Arc<LogScannerInner>> {
        validate_scan_support_inner(&info.table_path, info, !self.batch)?;
        let mut scanner = LogScannerInner::new(
            info,
            self.metadata.clone(),
            self.connections.clone(),
            &self.config,
            None,
            true,
            None,
            self.admin.clone(),
        )?;
        scanner.consumer_group = Some(self.group.clone());
        Ok(Arc::new(scanner))
    }

    async fn register(&mut self, path: &TablePath) -> Result<&TableInfo> {
        if let Some(index) = self
            .scanners
            .get_mut()
            .iter()
            .position(|s| &s.table_path == path)
        {
            let s = self.scanners.get_mut()[index].clone();
            if s.has_subscriptions() {
                self.check_identity(&s)?;
                return Ok(&self.scanners.get_mut()[index].table_info);
            }
            let info = self.admin.get_table_info(path).await?;
            if info.table_id != s.table_id || info.get_schema_id() != s.table_info.get_schema_id() {
                let scanner = self.build_table(&info)?;
                Arc::make_mut(self.scanners.get_mut())[index] = scanner;
            }
            return Ok(&self.scanners.get_mut()[index].table_info);
        }
        let info = self.admin.get_table_info(path).await?;
        let scanner = self.build_table(&info)?;
        let scanners = Arc::make_mut(self.scanners.get_mut());
        scanners.push(scanner);
        Ok(&scanners.last().unwrap().table_info)
    }

    fn check_identity(&self, scanner: &LogScannerInner) -> Result<()> {
        if self
            .metadata
            .get_cluster()
            .get_table_id(&scanner.table_path)
            .is_some_and(|id| id != scanner.table_id)
        {
            return Err(invalid(&format!(
                "Table {} was recreated; unsubscribe its old buckets and register the table again",
                scanner.table_path
            )));
        }
        Ok(())
    }

    fn scanner(&self, id: i64) -> Result<Arc<LogScannerInner>> {
        if let Some(s) = self.scanners.read().iter().find(|s| s.table_id == id) {
            self.check_identity(s)?;
            return Ok(s.clone());
        }
        // Initial tables may be subscribed again by ID after their last bucket was released.
        let cluster = self.metadata.get_cluster();
        let info = cluster
            .get_table_path_by_id(id)
            .filter(|p| self.initial_tables.contains(*p))
            .and_then(|p| cluster.opt_get_table(p))
            .filter(|t| t.table_id == id)
            .ok_or_else(|| {
                invalid("Table is not registered; register its path before subscribing")
            })?;
        if self
            .scanners
            .read()
            .iter()
            .any(|s| s.table_path == info.table_path)
        {
            return Err(invalid(
                "Table was recreated; unsubscribe its old buckets first",
            ));
        }
        let scanner = self.build_table(info)?;
        Arc::make_mut(&mut self.scanners.write()).push(scanner.clone());
        Ok(scanner)
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

impl MultiTableLogScanner {
    /// Returns the schema used by this scanner for a registered table.
    pub fn registered_table_info(&self, id: i64) -> Option<Arc<TableInfo>> {
        self.inner
            .scanners
            .read()
            .iter()
            .find(|s| s.table_id == id)
            .map(|s| s.table_info.clone())
    }
}

macro_rules! multi_methods {
    ($type:ty, $result:ty, $poll:ident, $empty:expr, $output:expr) => {
        impl $type {
            /// Each nonempty poll returns buckets from one ready table. Process the previous result before polling again
            /// when auto-commit is enabled. An error never discards another table's returned data.
            pub async fn poll(&self, timeout: Duration) -> Result<$result> {
                let deadline = tokio::time::Instant::now() + timeout;
                let scanners = self.inner.scanners.read().clone();
                for scanner in scanners.iter() {
                    if !scanner
                        .restore_committed_offsets(
                            deadline.saturating_duration_since(tokio::time::Instant::now()),
                        )
                        .await?
                    {
                        return Ok($empty);
                    }
                }
                if let Some(auto) = &self.inner.auto_commit {
                    auto.before_poll(&self.inner.group);
                }
                loop {
                    for _ in 0..scanners.len() {
                        let index =
                            self.inner.next.fetch_add(1, Ordering::Relaxed) % scanners.len();
                        let result = scanners[index].$poll(Duration::ZERO).await?;
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

            /// Registers a table by path on demand, returning the schema used to decode its rows.
            pub async fn register_table(&mut self, path: &TablePath) -> Result<&TableInfo> {
                self.inner.register(path).await
            }

            /// Registers buckets across tables. Poll restores their committed offsets before reading.
            pub fn subscribe_from_committed(&self, buckets: &[TableBucket]) -> Result<()> {
                for bucket in buckets {
                    self.inner
                        .scanner(bucket.table_id())?
                        .validate_group_buckets(std::slice::from_ref(bucket))?;
                }
                for bucket in buckets {
                    self.inner
                        .scanner(bucket.table_id())?
                        .subscribe_from_committed(std::slice::from_ref(bucket))?;
                }
                Ok(())
            }

            /// Subscribes or seeks one bucket of a registered table.
            pub async fn subscribe(&self, bucket: &TableBucket, offset: i64) -> Result<()> {
                let scanner = self.inner.scanner(bucket.table_id())?;
                scanner.validate_group_buckets(std::iter::once(bucket))?;
                match bucket.partition_id() {
                    Some(id) => {
                        scanner
                            .subscribe_partition(id, bucket.bucket_id(), offset)
                            .await
                    }
                    None => scanner.subscribe(bucket.bucket_id(), offset).await,
                }
            }

            /// Removes one bucket from the manual assignment without committing it.
            pub async fn unsubscribe(&self, bucket: &TableBucket) -> Result<()> {
                let scanner = self
                    .inner
                    .scanners
                    .read()
                    .iter()
                    .find(|s| s.table_id == bucket.table_id())
                    .cloned();
                let Some(scanner) = scanner else {
                    return Ok(());
                };
                match bucket.partition_id() {
                    Some(partition) => {
                        scanner
                            .unsubscribe_partition(partition, bucket.bucket_id())
                            .await
                    }
                    None => scanner.unsubscribe(bucket.bucket_id()).await,
                }?;
                if !scanner.has_subscriptions() {
                    Arc::make_mut(&mut self.inner.scanners.write())
                        .retain(|s| !Arc::ptr_eq(s, &scanner));
                }
                Ok(())
            }

            /// Enqueues all delivered next offsets across assigned tables, in scanner call order.
            pub fn commit_async(&self) -> Result<OffsetCommitHandle> {
                self.commit_offsets_async(self.inner.group.delivered.lock().clone())
            }

            /// Enqueues explicit offsets from a Tokio runtime, preserving partial failures and unknown
            /// outcomes. No fencing is provided against other scanners or processes.
            pub fn commit_offsets_async(
                &self,
                offsets: HashMap<TableBucket, OffsetAndMetadata>,
            ) -> Result<OffsetCommitHandle> {
                self.inner.group.enqueue(offsets)
            }

            /// Commits delivered positions. Also use after processing the last poll and before
            /// shutdown: dropping a scanner never commits implicitly. Inspect every bucket result.
            pub async fn commit_sync(
                &self,
            ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
                self.commit_async()
                    .map_err(OffsetCommitError::Rejected)?
                    .await
            }

            /// Submits an automatic commit if due, then waits up to the group timeout for pending commits.
            pub async fn close(self) {
                let scanners = self.inner.scanners.read().clone();
                for scanner in scanners.iter() {
                    scanner.cancel_committed_offset_lookup().await;
                }
                AutoCommit::finish(self.inner.auto_commit.as_ref(), &self.inner.group).await;
            }

            /// Commits explicit next offsets. After an unknown outcome, unsafe rewinds are rejected for the affected buckets.
            pub async fn commit_offsets_sync(
                &self,
                offsets: HashMap<TableBucket, OffsetAndMetadata>,
            ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
                self.commit_offsets_async(offsets)
                    .map_err(OffsetCommitError::Rejected)?
                    .await
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{build_cluster_with_tables, build_table_info};

    #[tokio::test]
    async fn close_releases_scanners_during_committed_offset_lookup() -> Result<()> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let path = TablePath::new("db", "tbl");
        let cluster = build_cluster_with_tables(
            &[(&path, 1, 1)],
            listener.local_addr().unwrap().port().into(),
        );
        let metadata = Arc::new(Metadata::new_for_test(Arc::new(cluster)));
        let conn = FlussConnection::new_for_test(metadata.clone());
        let info = build_table_info(path, 1, 1);
        let group = ConsumerGroupOptions::new("closing");
        let single = TableScan::new(&conn, info.clone(), metadata)
            .with_consumer_group(group.clone())?
            .create_log_scanner()?;
        let multi = MultiTableScan {
            connection: &conn,
            tables: vec![info],
            group,
            auto_commit: None,
        }
        .create_log_scanner()?;
        let refs = [
            Arc::downgrade(&single.inner),
            Arc::downgrade(&multi.inner.scanners.read()[0]),
        ];

        let buckets = [TableBucket::new(1, 0)];
        single.subscribe_from_committed(&buckets)?;
        multi.subscribe_from_committed(&buckets)?;
        single.poll(Duration::ZERO).await?;
        multi.poll(Duration::ZERO).await?;
        // Keep the peer connected without answering discovery.
        let _peer = tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .unwrap()
            .unwrap();
        single.close().await;
        multi.close().await;
        assert!(refs.iter().all(|scanner| scanner.upgrade().is_none()));
        Ok(())
    }
}
