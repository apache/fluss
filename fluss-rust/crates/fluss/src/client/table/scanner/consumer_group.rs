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
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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

    fn validate_group_buckets<'a>(
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

    fn delivered_offsets(&self) -> Result<HashMap<TableBucket, OffsetAndMetadata>> {
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

            /// Commits progress from completed polls, including ranges skipped by the filter.
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
                self.inner.validate_group_buckets(offsets.keys())?;
                self.inner.group()?.enqueue(offsets)
            }

            /// Awaits persistence of the positions delivered by completed polls.
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
