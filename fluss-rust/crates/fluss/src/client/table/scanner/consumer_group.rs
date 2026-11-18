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

use super::{LogScanner, LogScannerInner, RecordBatchLogScanner};
use crate::client::consumer_offsets::{ConsumerGroup, invalid};
use crate::client::{OffsetAndMetadata, OffsetCommitError, OffsetCommitHandle, OffsetCommitResult};
use crate::error::Result;
use crate::metadata::TableBucket;
use std::collections::HashMap;
use std::sync::Arc;

/// Starting-position strategy for manually assigned log buckets.
/// Fallbacks apply only to missing commits, never to authorization or RPC errors.
#[derive(Debug, Clone)]
pub struct OffsetsInitializer(Initializer);

#[derive(Debug, Clone)]
enum Initializer {
    Earliest,
    Offset(i64),
    Lookup(crate::rpc::message::OffsetSpec),
    Fail,
    Committed(String, Box<OffsetsInitializer>),
}

impl OffsetsInitializer {
    /// Starts at the earliest available record.
    pub fn earliest() -> Self {
        Self(Initializer::Earliest)
    }
    /// Starts at the current end of each bucket.
    pub fn latest() -> Self {
        Self(Initializer::Lookup(crate::rpc::message::OffsetSpec::Latest))
    }
    /// Starts at the first offset at or after a Unix timestamp in milliseconds.
    pub fn timestamp(timestamp_ms: i64) -> Self {
        Self(Initializer::Lookup(
            crate::rpc::message::OffsetSpec::Timestamp(timestamp_ms),
        ))
    }
    /// Starts every selected bucket at this explicit nonnegative offset.
    pub fn offset(offset: i64) -> Self {
        Self(Initializer::Offset(offset))
    }
    /// Fails initialization if a preceding strategy could not resolve every bucket.
    pub fn fail() -> Self {
        Self(Initializer::Fail)
    }
    /// Restores saved positions, starting missing buckets at earliest.
    pub fn committed(group_id: impl Into<String>) -> Self {
        Self::committed_with_fallback(group_id, Self::earliest())
    }
    /// Restores saved positions and resolves only missing buckets with `fallback`.
    /// The source group is independent of the scanner's configured commit group.
    pub fn committed_with_fallback(group_id: impl Into<String>, fallback: Self) -> Self {
        Self(Initializer::Committed(group_id.into(), Box::new(fallback)))
    }
}

/// Failure from a poll-triggered automatic commit. Poll returns no new records when this
/// occurs. The source of `Error::UnexpectedError` retains these per-bucket outcomes.
#[derive(Debug, thiserror::Error)]
pub enum AutoCommitError {
    /// Request failure. A possibly late write restricts rewinds on its buckets.
    #[error("Automatic commit failed: {0}")]
    Request(#[from] OffsetCommitError),
    /// At least one bucket was rejected or has an unknown outcome.
    #[error("Automatic commit contains unsuccessful buckets: {0:?}")]
    Buckets(OffsetCommitResult),
}

pub(super) struct AutoCommit {
    interval: std::time::Duration,
    next: parking_lot::Mutex<tokio::time::Instant>,
}

impl AutoCommit {
    pub(super) fn new(interval: std::time::Duration) -> Result<Self> {
        if interval.is_zero() {
            return Err(invalid("Auto-commit interval must be positive"));
        }
        Ok(Self {
            interval,
            next: parking_lot::Mutex::new(tokio::time::Instant::now() + interval),
        })
    }

    pub(super) async fn before_poll(&self, group: &Arc<ConsumerGroup>) -> Result<()> {
        if tokio::time::Instant::now() < *self.next.lock() {
            return Ok(());
        }
        let offsets = group.delivered.lock().clone();
        let result = async {
            if !offsets.is_empty() {
                let result = group
                    .enqueue(offsets)
                    .map_err(OffsetCommitError::Rejected)?
                    .await
                    .map_err(OffsetCommitError::from)??;
                if result.values().any(|outcome| {
                    !matches!(outcome, crate::client::OffsetCommitOutcome::Committed)
                }) {
                    return Err(AutoCommitError::Buckets(result));
                }
            }
            Ok(())
        }
        .await;
        result.map_err(
            |error: AutoCommitError| crate::error::Error::UnexpectedError {
                message: error.to_string(),
                source: Some(Box::new(error)),
            },
        )?;
        *self.next.lock() = tokio::time::Instant::now() + self.interval;
        Ok(())
    }
}

impl LogScannerInner {
    pub(super) async fn maybe_auto_commit(&self) -> Result<()> {
        if let Some(auto) = &self.auto_commit {
            auto.before_poll(self.group()?).await?;
        }
        Ok(())
    }

    fn group(&self) -> Result<&Arc<ConsumerGroup>> {
        self.consumer_group
            .as_ref()
            .ok_or_else(|| invalid("Configure a consumer group before creating the scanner"))
    }

    pub(super) fn validate_group_buckets<'a>(
        &self,
        buckets: impl IntoIterator<Item = &'a TableBucket>,
    ) -> Result<()> {
        for bucket in buckets {
            if bucket.bucket_id() < 0
                || bucket.partition_id().is_some_and(|id| id < 0)
                || bucket.table_id() != self.table_id
                || bucket.partition_id().is_some() != self.is_partitioned_table
            {
                return Err(invalid(
                    "Consumer offset bucket does not belong to this scanner's table",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn delivered_offsets(&self) -> Result<HashMap<TableBucket, OffsetAndMetadata>> {
        self.check_no_active_reader()?;
        Ok(self.group()?.delivered.lock().clone())
    }

    async fn subscribe_with_initializer(
        &self,
        buckets: &[TableBucket],
        initializer: &OffsetsInitializer,
    ) -> Result<()> {
        let offsets = self.resolve_initializer(buckets, initializer).await?;
        self.do_subscribe_buckets(offsets, false).await
    }

    pub(super) async fn resolve_initializer(
        &self,
        buckets: &[TableBucket],
        initializer: &OffsetsInitializer,
    ) -> Result<HashMap<TableBucket, i64>> {
        self.check_no_active_reader()?;
        self.validate_group_buckets(buckets)?;
        let mut pending = buckets.to_vec();
        let mut offsets = HashMap::new();
        let admin =
            crate::client::FlussAdmin::new(self.log_fetcher.conns.clone(), self.metadata.clone());
        let mut strategy = initializer;
        while !pending.is_empty() {
            match &strategy.0 {
                Initializer::Committed(group_id, fallback) => {
                    let saved = match self
                        .consumer_group
                        .as_ref()
                        .and_then(|g| g.client_for(group_id))
                    {
                        Some(client) => client.fetch(&pending).await?,
                        None => admin.get_consumer_offsets(group_id, &pending).await?,
                    };
                    pending.retain(|bucket| {
                        if let Some(value) = &saved[bucket] {
                            offsets.insert(bucket.clone(), value.offset);
                            false
                        } else {
                            true
                        }
                    });
                    strategy = fallback;
                }
                Initializer::Fail => {
                    return Err(invalid("No committed offset and no fallback provided"));
                }
                Initializer::Earliest => {
                    offsets.extend(
                        pending
                            .drain(..)
                            .map(|bucket| (bucket, crate::client::EARLIEST_OFFSET)),
                    );
                }
                Initializer::Offset(offset) => {
                    if *offset < 0 {
                        return Err(invalid("Starting offset must be nonnegative"));
                    }
                    offsets.extend(pending.drain(..).map(|bucket| (bucket, *offset)));
                }
                Initializer::Lookup(spec) => {
                    let mut partitions: HashMap<Option<i64>, Vec<i32>> = HashMap::new();
                    for bucket in &pending {
                        partitions
                            .entry(bucket.partition_id())
                            .or_default()
                            .push(bucket.bucket_id());
                    }
                    for (partition, ids) in partitions {
                        let values = if let Some(id) = partition {
                            self.metadata
                                .check_and_update_partition_metadata_by_ids(&self.table_path, &[id])
                                .await?;
                            let name = self
                                .metadata
                                .get_cluster()
                                .get_partition_name(id)
                                .cloned()
                                .ok_or_else(|| {
                                    invalid("Partition not found for offset initialization")
                                })?;
                            admin
                                .list_partition_offsets(&self.table_path, &name, &ids, spec.clone())
                                .await?
                        } else {
                            admin
                                .list_offsets(&self.table_path, &ids, spec.clone())
                                .await?
                        };
                        for id in ids {
                            let offset = *values
                                .get(&id)
                                .filter(|offset| **offset >= 0)
                                .ok_or_else(|| invalid("No offset returned for initializer"))?;
                            offsets.insert(
                                TableBucket::new_with_partition(self.table_id, partition, id),
                                offset,
                            );
                        }
                    }
                    pending.clear();
                }
            }
        }
        // Resolve every bucket before changing subscriptions, including fallback failures.
        Ok(offsets)
    }
}

macro_rules! group_methods {
    ($scanner:ty) => {
        impl $scanner {
            /// Resolves starting positions, then subscribes the selected buckets together.
            /// Other subscriptions are retained. No subscription changes on resolution failure.
            /// Assignment is manual; drive subscriptions and polls from one consumer task.
            pub async fn subscribe_with_initializer(
                &self,
                buckets: &[TableBucket],
                initializer: &OffsetsInitializer,
            ) -> Result<()> {
                self.inner
                    .subscribe_with_initializer(buckets, initializer)
                    .await
            }

            /// Commits progress from completed polls, including ranges skipped by the filter.
            /// Process all returned records first; use `commit_offsets_async` for partial batches.
            /// Subscriptions, seeks and polls must be driven by one consumer task.
            pub fn commit_async(&self) -> Result<OffsetCommitHandle> {
                self.commit_offsets_async(self.inner.delivered_offsets()?)
            }

            /// Enqueues explicit next offsets. The scanner submits commits in call order.
            /// Dropping or aborting the handle does not cancel the commit. No ordering or fencing is
            /// provided across scanners/processes using the same group and bucket.
            /// At most 64 commits may be pending per scanner. A full queue returns
            /// `Error::BufferExhausted` before enqueueing; retry after a pending commit finishes.
            /// After a possibly late write, the scanner accepts a higher offset or an identical
            /// offset and metadata for that bucket. An unsafe rewind, even after a seek, returns
            /// `NeedsRecovery`. A late write can overwrite a higher commit and cause replay.
            pub fn commit_offsets_async(
                &self,
                offsets: HashMap<TableBucket, OffsetAndMetadata>,
            ) -> Result<OffsetCommitHandle> {
                self.inner.check_no_active_reader()?;
                self.inner.validate_group_buckets(offsets.keys())?;
                self.inner.group()?.enqueue(offsets)
            }

            /// Awaits persistence of the positions delivered by completed polls.
            /// Inspect every bucket result even when the request succeeds.
            pub async fn commit_sync(
                &self,
            ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
                await_commit(self.commit_async().map_err(OffsetCommitError::Rejected)?).await
            }

            /// Awaits persistence of explicit next offsets and optional metadata.
            /// Inspect every bucket outcome. The rewind rules of `commit_offsets_async` apply.
            pub async fn commit_offsets_sync(
                &self,
                offsets: HashMap<TableBucket, OffsetAndMetadata>,
            ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
                await_commit(
                    self.commit_offsets_async(offsets)
                        .map_err(OffsetCommitError::Rejected)?,
                )
                .await
            }
        }
    };
}
group_methods!(LogScanner);
group_methods!(RecordBatchLogScanner);

async fn await_commit(
    handle: OffsetCommitHandle,
) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
    handle.await?
}
