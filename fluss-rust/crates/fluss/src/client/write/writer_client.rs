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

use crate::TableId;
use crate::bucketing::BucketingFunction;
use crate::client::metadata::Metadata;
use crate::client::write::IdempotenceManager;
use crate::client::write::broadcast;
use crate::client::write::bucket_assigner::{
    BucketAssigner, HashBucketAssigner, RoundRobinBucketAssigner, StickyBucketAssigner,
};
use crate::client::write::sender::Sender;
use crate::client::{RecordAccumulator, ResultHandle, WriteRecord};
use crate::cluster::Cluster;
use crate::config::Config;
use crate::config::NoKeyAssigner;
use crate::error::{Error, FlussError, Result};
use crate::metadata::{PhysicalTablePath, TableInfo, TablePath};
use crate::metrics::WriterMetrics;
use dashmap::DashMap;
use log::warn;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[allow(dead_code)]
pub struct WriterClient {
    config: Config,
    max_request_size: i32,
    accumulate: Arc<RecordAccumulator>,
    shutdown_tx: Mutex<Option<mpsc::Sender<()>>>,
    sender_join_handle: Mutex<Option<JoinHandle<()>>>,
    metadata: Arc<Metadata>,
    /// Each path's assigner, with the table it was built for.
    bucket_assigners: DashMap<Arc<PhysicalTablePath>, (TableId, Arc<dyn BucketAssigner>)>,
    idempotence_manager: Arc<IdempotenceManager>,
}

impl WriterClient {
    pub fn new(config: Config, metadata: Arc<Metadata>) -> Result<Self> {
        let ack = Self::get_ack(&config)?;

        let idempotence_manager = Arc::new(IdempotenceManager::new(
            config.writer_enable_idempotence,
            config.writer_max_inflight_requests_per_bucket,
        ));

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let accumulator = Arc::new(RecordAccumulator::new(
            config.clone(),
            Arc::clone(&idempotence_manager),
        ));

        // Writer metrics are emitted unlabeled (global per process). Resolve the
        // recorder once and share the cached handles with the sender.
        let metrics = Arc::new(WriterMetrics::new());

        let sender = Arc::new(Sender::new(
            metadata.clone(),
            accumulator.clone(),
            config.writer_request_max_size,
            30_000,
            ack,
            config.writer_retries,
            config.writer_retry_backoff_ms,
            config.writer_retry_max_backoff_ms,
            Arc::clone(&idempotence_manager),
            Arc::clone(&metrics),
        ));

        let join_handle = tokio::spawn(async move {
            if let Err(e) = sender.run_with_shutdown(shutdown_rx).await {
                warn!("Sender loop exited with error: {e}");
            }
        });

        Ok(Self {
            max_request_size: config.writer_request_max_size,
            config,
            shutdown_tx: Mutex::new(Some(shutdown_tx)),
            sender_join_handle: Mutex::new(Some(join_handle)),
            accumulate: accumulator,
            metadata,
            bucket_assigners: Default::default(),
            idempotence_manager,
        })
    }

    fn get_ack(config: &Config) -> Result<i16> {
        let acks = config.writer_acks.as_str();
        if acks.eq_ignore_ascii_case("all") {
            Ok(-1)
        } else {
            acks.parse::<i16>().map_err(|e| Error::IllegalArgument {
                message: format!("invalid writer ack '{acks}': {e}"),
            })
        }
    }

    pub fn send(&self, record: &WriteRecord<'_>) -> Result<ResultHandle> {
        self.send_routed(record, None)
    }

    pub(crate) fn routing_bucket_count(
        &self,
        physical_table_path: &Arc<PhysicalTablePath>,
        table_info: &TableInfo,
    ) -> Result<i32> {
        let cluster = self.metadata.get_cluster();
        Self::check_table_cached(&cluster, physical_table_path.get_table_path())?;
        self.accumulate
            .routing_bucket_count(physical_table_path, table_info, &cluster)
    }

    /// Fails with `TableNotExist` once a dropped table's metadata is evicted, the error its
    /// pending writes got.
    fn check_table_cached(cluster: &Cluster, table_path: &TablePath) -> Result<()> {
        cluster.get_table(table_path).map(|_| ()).map_err(|e| {
            if e.api_error() == Some(FlussError::InvalidTableException) {
                Error::table_not_exist(format!("Table not found: {table_path}"))
            } else {
                e
            }
        })
    }

    /// Fails with `InvalidBucketRouting` when the routing count is no longer `bucket_count`.
    pub(crate) fn send_with_bucket_count(
        &self,
        record: &WriteRecord<'_>,
        bucket_count: i32,
    ) -> Result<ResultHandle> {
        self.send_routed(record, Some(bucket_count))
    }

    fn send_routed(
        &self,
        record: &WriteRecord<'_>,
        expected_bucket_count: Option<i32>,
    ) -> Result<ResultHandle> {
        if self.accumulate.is_closed() {
            return Err(Error::WriterClosed {
                message: "Cannot send: writer is closed".to_string(),
            });
        }
        let physical_table_path = &record.physical_table_path;
        let cluster = self.metadata.get_cluster();
        // Before the bucket assigner panics on a missing table.
        Self::check_table_cached(&cluster, physical_table_path.get_table_path())?;
        let bucket_key = record.bucket_key.as_ref();
        let bucket_assigner = self.bucket_assigner(&record.table_info, physical_table_path)?;

        // A provisional routing count resolves once, so this settles after a retry at most.
        loop {
            let bucket_count = self.accumulate.routing_bucket_count(
                physical_table_path,
                &record.table_info,
                &cluster,
            )?;
            if let Some(expected) = expected_bucket_count {
                if expected != bucket_count {
                    return Err(Error::invalid_bucket_routing(format!(
                        "{} is now routed with {bucket_count} buckets instead of {expected}; retry the write.",
                        physical_table_path.as_ref()
                    )));
                }
            }

            let bucket_id = bucket_assigner.assign_bucket(bucket_key, &cluster, bucket_count)?;
            let mut result = self.accumulate.append_with_bucket_count(
                record,
                bucket_id,
                bucket_count,
                &cluster,
                bucket_assigner.abort_if_batch_full(),
            )?;

            if result.abort_record_for_new_batch {
                bucket_assigner.on_new_batch(&cluster, bucket_count, bucket_id);
                let bucket_id =
                    bucket_assigner.assign_bucket(bucket_key, &cluster, bucket_count)?;
                result = self.accumulate.append_with_bucket_count(
                    record,
                    bucket_id,
                    bucket_count,
                    &cluster,
                    false,
                )?;
            }

            if result.routing_changed {
                continue;
            }

            if result.batch_is_full || result.new_batch_created {
                self.accumulate.wakeup_sender();
            }

            return Ok(result.result_handle.expect("result_handle should exist"));
        }
    }

    fn bucket_assigner(
        &self,
        table_info: &Arc<TableInfo>,
        table_path: &Arc<PhysicalTablePath>,
    ) -> Result<Arc<dyn BucketAssigner>> {
        if let Some(entry) = self.bucket_assigners.get(table_path) {
            let (table_id, assigner) = entry.value();
            // A table recreated under the same path can change its bucket key.
            if table_info.table_id <= *table_id {
                return Ok(Arc::clone(assigner));
            }
        }
        let assigner =
            Self::create_bucket_assigner(table_info, Arc::clone(table_path), &self.config)?;
        self.bucket_assigners.insert(
            Arc::clone(table_path),
            (table_info.table_id, Arc::clone(&assigner)),
        );
        Ok(assigner)
    }

    /// Close the writer with a timeout. Matches Java's two-phase shutdown:
    ///
    /// 1. **Graceful**: Signal the sender to drain all remaining batches.
    ///    `accumulator.close()` makes all batches immediately ready (no need
    ///    to wait for `batch_timeout_ms`).
    /// 2. **Force** (if timeout exceeded): Abort the sender task and fail
    ///    all remaining batches with an error.
    ///
    /// Idempotent: calling `close` a second time returns `Ok(())` immediately.
    pub async fn close(&self, timeout: Duration) -> Result<()> {
        // Take shutdown_tx and join_handle out of their Mutexes.
        // Second call sees None and returns early.
        let shutdown_tx = self.shutdown_tx.lock().take();
        let join_handle = self.sender_join_handle.lock().take();

        let Some(mut join_handle) = join_handle else {
            return Ok(());
        };

        // Phase 1: Signal graceful shutdown.
        // Mark accumulator closed so all batches become immediately sendable.
        self.accumulate.close();
        // Drop the shutdown sender — recv() returns None, breaking the sender loop.
        drop(shutdown_tx);

        // Phase 2: Wait for graceful drain, bounded by timeout.
        tokio::select! {
            result = &mut join_handle => {
                if let Err(e) = result {
                    warn!("Sender task panicked during shutdown: {e}");
                }
            }
            _ = tokio::time::sleep(timeout) => {
                // Phase 3: Force close — timeout exceeded.
                warn!("Graceful shutdown timed out after {timeout:?}, force closing");
                join_handle.abort();
                let _ = join_handle.await; // Wait for cancellation to complete
                self.accumulate.abort_batches(broadcast::Error::Client {
                    message: "Writer force closed (shutdown timeout exceeded)".to_string(),
                });
            }
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<()> {
        self.accumulate.begin_flush();
        self.accumulate.await_flush_completion().await?;
        Ok(())
    }

    #[cfg(feature = "integration_tests")]
    pub fn estimated_batch_size_for_table(
        &self,
        table_path: &Arc<PhysicalTablePath>,
    ) -> Option<usize> {
        self.accumulate.estimated_batch_size(table_path)
    }

    pub fn create_bucket_assigner(
        table_info: &Arc<TableInfo>,
        table_path: Arc<PhysicalTablePath>,
        config: &Config,
    ) -> Result<Arc<dyn BucketAssigner>> {
        // Decide from the table's bucket key, not an individual record's key.
        if table_info.has_bucket_key() {
            let datalake_format = table_info.get_table_config().get_datalake_format()?;
            let function = <dyn BucketingFunction>::of(datalake_format.as_ref());
            Ok(Arc::new(HashBucketAssigner::new(function)))
        } else {
            match config.writer_bucket_no_key_assigner {
                NoKeyAssigner::Sticky => Ok(Arc::new(StickyBucketAssigner::new(table_path))),
                NoKeyAssigner::RoundRobin => {
                    Ok(Arc::new(RoundRobinBucketAssigner::new(table_path)))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypes, Schema, TableDescriptor};
    use crate::test_utils::{build_cluster, build_table_info};

    #[tokio::test]
    async fn a_recreated_table_gets_an_assigner_for_its_bucket_key() -> Result<()> {
        let table_path = TablePath::new("db", "tbl");
        let cluster = Arc::new(build_cluster(&table_path, 1, 2));
        let writer =
            WriterClient::new(Config::default(), Arc::new(Metadata::new_for_test(cluster)))?;
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let keyless = Arc::new(build_table_info(table_path.clone(), 1, 2));
        let keyed = {
            let descriptor = TableDescriptor::builder()
                .schema(
                    Schema::builder()
                        .column("id", DataTypes::int())
                        .build()
                        .expect("schema"),
                )
                .distributed_by(Some(2), vec!["id".to_string()])
                .build()
                .expect("descriptor");
            Arc::new(TableInfo::of(table_path, 2, 1, descriptor, 0, 0))
        };

        // Only the sticky assigner of a keyless table aborts full batches.
        assert!(
            writer
                .bucket_assigner(&keyless, &physical_table_path)?
                .abort_if_batch_full()
        );
        assert!(
            !writer
                .bucket_assigner(&keyed, &physical_table_path)?
                .abort_if_batch_full()
        );
        Ok(())
    }

    #[tokio::test]
    async fn routing_a_dropped_table_reports_table_not_exist() -> Result<()> {
        let table_path = TablePath::new("db", "dropped");
        let cluster = Arc::new(build_cluster(&TablePath::new("db", "other"), 1, 2));
        let writer =
            WriterClient::new(Config::default(), Arc::new(Metadata::new_for_test(cluster)))?;
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone())));
        let table_info = build_table_info(table_path, 2, 2);

        let error = writer
            .routing_bucket_count(&physical_table_path, &table_info)
            .expect_err("dropped table");

        assert_eq!(error.api_error(), Some(FlussError::TableNotExist));
        Ok(())
    }
}
