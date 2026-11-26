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
use crate::client::{
    OffsetAndMetadata, OffsetCommitError, OffsetCommitHandle, OffsetCommitOutcome,
    OffsetCommitResult,
};
use crate::error::Result;
use crate::metadata::TableBucket;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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

    pub(super) fn before_poll(&self, group: &Arc<ConsumerGroup>) {
        let now = tokio::time::Instant::now();
        let mut next = self.next.lock();
        if now < *next {
            return;
        }
        *next = now + self.interval;
        let offsets = group.delivered.lock();
        if offsets.is_empty() {
            return;
        }
        match group.enqueue(offsets.clone()) {
            Ok(handle) => {
                tokio::spawn(async move {
                    match handle.await {
                        Err(error) => log::warn!("Periodic offset commit failed: {error}"),
                        Ok(result)
                            if result
                                .values()
                                .any(|v| !matches!(v, OffsetCommitOutcome::Committed)) =>
                        {
                            log::warn!(
                                "Periodic offset commit contains unsuccessful buckets: {result:?}"
                            )
                        }
                        Ok(_) => {}
                    }
                });
            }
            Err(error) => log::warn!("Periodic offset commit not queued: {error}"),
        }
    }

    pub(super) async fn finish(auto: Option<&Self>, group: &Arc<ConsumerGroup>) {
        if let Some(auto) = auto {
            auto.before_poll(group);
        }
        group.await_pending_commits().await;
    }
}

/// In-flight lookup survives short poll deadlines. A seek or unsubscribe invalidates
/// its snapshot before it can change the scanner position.
#[derive(Default)]
pub(super) struct PendingStarts {
    pub(super) buckets: HashSet<TableBucket>,
    lookup: Option<tokio::task::JoinHandle<Result<HashMap<TableBucket, i64>>>>,
}

impl PendingStarts {
    pub(super) fn forget(&mut self, bucket: &TableBucket) {
        if self.buckets.remove(bucket)
            && let Some(task) = self.lookup.take()
        {
            task.abort();
        }
    }
}

impl LogScannerInner {
    pub(super) fn maybe_auto_commit(&self) {
        if let (Some(auto), Some(group)) = (&self.auto_commit, &self.consumer_group) {
            auto.before_poll(group);
        }
    }

    pub(super) fn defer_committed(&self, buckets: &[TableBucket]) -> Result<()> {
        self.check_no_active_reader()?;
        self.validate_group_buckets(buckets)?;
        self.group()?;
        for bucket in buckets {
            self.unassign_bucket(bucket);
            self.pending_starts.lock().buckets.insert(bucket.clone());
        }
        // Previously pending buckets may share a lookup that did not include the additions.
        if let Some(task) = self.pending_starts.lock().lookup.take() {
            task.abort();
        }
        Ok(())
    }

    pub(super) async fn cancel_position_lookup(&self) {
        let lookup = self.pending_starts.lock().lookup.take();
        if let Some(task) = lookup {
            task.abort();
            // Wait until the task releases its scanner reference.
            let _ = task.await;
        }
    }

    pub(super) async fn prepare_positions(
        self: &Arc<Self>,
        budget: std::time::Duration,
    ) -> Result<bool> {
        if self.consumer_group.is_none() || self.pending_starts.lock().buckets.is_empty() {
            return Ok(true);
        }
        let wait = std::future::poll_fn(|cx| {
            use std::task::{Poll, ready};
            let mut pending = self.pending_starts.lock();
            if pending.lookup.is_none() {
                let ids: Vec<_> = pending.buckets.iter().cloned().collect();
                let group = self.consumer_group.as_ref().unwrap().clone();
                let scanner = Arc::downgrade(self);
                pending.lookup = Some(tokio::spawn(async move {
                    let offsets = group
                        .fetch_positions(&ids)
                        .await?
                        .into_iter()
                        .map(|(bucket, saved)| {
                            (
                                bucket,
                                saved.map_or(crate::client::EARLIEST_OFFSET, |v| v.offset),
                            )
                        })
                        .collect();
                    let scanner = scanner.upgrade().ok_or_else(|| invalid("Scanner closed"))?;
                    scanner.prepare_bucket_metadata(&offsets).await?;
                    Ok(offsets)
                }));
            }
            let result = ready!(std::pin::Pin::new(pending.lookup.as_mut().unwrap()).poll(cx));
            pending.lookup = None;
            Poll::Ready(result)
        });
        let Ok(result) = tokio::time::timeout(budget, wait).await else {
            return Ok(false);
        };
        let offsets = result.map_err(|e| invalid(&e.to_string()))??;
        self.assign_buckets(offsets);
        Ok(true)
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
}

macro_rules! group_methods {
    ($scanner:ty) => {
        impl $scanner {
            /// Restores this scanner's group on the next poll. Buckets without a saved
            /// offset start at the earliest available record. Lookup errors propagate from poll.
            pub fn subscribe_from_committed(&self, buckets: &[TableBucket]) -> Result<()> {
                self.inner.defer_committed(buckets)
            }

            /// Commits current positions, including seeks and ranges skipped by the filter.
            /// Process all returned records first; use `commit_offsets_async` for partial batches.
            /// Call from a Tokio runtime. Drive subscriptions, seeks and polls from one consumer task.
            pub fn commit_async(&self) -> Result<OffsetCommitHandle> {
                self.commit_offsets_async(self.inner.delivered_offsets()?)
            }

            /// Enqueues explicit next offsets from a Tokio runtime, in call order.
            /// Dropping the handle does not cancel the commit. No ordering or fencing is
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
                self.inner.group()?.enqueue(offsets)
            }

            /// Submits an automatic commit if due, then waits up to the group timeout for pending commits.
            /// To confirm the final position, call `commit_sync` and inspect its result before close.
            /// Dropping the scanner without closing it does not commit.
            pub async fn close(self) {
                self.inner.cancel_position_lookup().await;
                if let Some(group) = &self.inner.consumer_group {
                    AutoCommit::finish(self.inner.auto_commit.as_ref(), group).await;
                }
            }

            /// Awaits persistence of the current scanner positions.
            /// Inspect every bucket result even when the request succeeds.
            pub async fn commit_sync(
                &self,
            ) -> std::result::Result<OffsetCommitResult, OffsetCommitError> {
                self.commit_async()
                    .map_err(OffsetCommitError::Rejected)?
                    .await
            }

            /// Awaits persistence of explicit next offsets and optional metadata.
            /// Inspect every bucket outcome. The rewind rules of `commit_offsets_async` apply.
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
group_methods!(LogScanner);
group_methods!(RecordBatchLogScanner);
