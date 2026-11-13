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

use super::QueuedLookup;
use crate::cluster::Cluster;
use crate::error::Error;
use crate::metadata::{TableBucket, TablePath};
use crate::util::retry_backoff_ms;
use crate::{PartitionId, TableId};
use log::{Level, debug, log, warn};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tokio::time::Instant;

/// Lookups waiting for the leaders of their buckets, and when each bucket is refreshed next.
///
/// Only the outcome of a refresh moves a bucket's next refresh. A lookup that arrives while its
/// bucket backs off or is being refreshed waits for that refresh and leaves the schedule as it is,
/// so a steady flow of lookups neither puts off the refresh nor adds to it.
///
/// At most `bucket_capacity` lookups wait for each bucket, so a bucket that stays without a
/// leader can't make the lookups to other buckets fail. Lookups whose caller stopped waiting are
/// dropped before more are refused, and when their bucket is due for a refresh.
pub(super) struct LeaderWaits {
    buckets: HashMap<TableBucket, BucketWait>,
    bucket_capacity: usize,
    /// Whether a task refreshes the metadata of the buckets here. That task takes on every bucket
    /// added while it runs, so no second one is started.
    refresher_running: bool,
    retry_backoff_ms: u64,
    retry_max_backoff_ms: u64,
}

struct BucketWait {
    table_path: TablePath,
    lookups: Vec<QueuedLookup>,
    /// Refreshes in a row that found no leader, which set the backoff before the next one.
    failed_refreshes: i32,
    /// Not refreshed before this time. A bucket that starts to wait is due at once.
    next_refresh_at: Instant,
}

impl BucketWait {
    /// Drops the lookups whose caller stopped waiting.
    fn drop_abandoned(&mut self) {
        self.lookups.retain(|lookup| !lookup.is_done());
    }
}

#[derive(Default)]
pub(super) struct LeaderRefresh {
    pub table_buckets: Vec<TableBucket>,
    pub table_paths: HashSet<TablePath>,
    /// The table IDs of `table_buckets`. A failure reported for one of `table_paths` applies only
    /// to these, not to a table recreated under the path while the refresh ran.
    pub table_ids: HashSet<TableId>,
    /// The partitions of `table_buckets`, with their tables.
    pub partitions: HashMap<PartitionId, TablePath>,
}

impl LeaderWaits {
    pub fn new(bucket_capacity: usize, retry_backoff_ms: u64, retry_max_backoff_ms: u64) -> Self {
        Self {
            buckets: HashMap::new(),
            bucket_capacity,
            refresher_running: false,
            retry_backoff_ms,
            retry_max_backoff_ms,
        }
    }

    /// Adds lookups whose bucket has no cached leader, and fails those beyond the capacity of
    /// their bucket. Returns whether a bucket starts to wait, and so is due for a refresh.
    pub fn add(&mut self, lookups: Vec<QueuedLookup>, now: Instant) -> bool {
        let mut has_new_buckets = false;
        // A sweep visits every lookup waiting for the bucket, so it runs at most once per batch.
        let mut swept = HashSet::new();
        let mut refused: HashMap<TableBucket, usize> = HashMap::new();
        for mut lookup in lookups {
            let wait = match self.buckets.entry(lookup.table_bucket().clone()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    debug!(
                        "No leader cached for table bucket {} of table {}, waiting for a refresh",
                        entry.key(),
                        lookup.table_path()
                    );
                    has_new_buckets = true;
                    entry.insert(BucketWait {
                        table_path: lookup.table_path().clone(),
                        lookups: Vec::new(),
                        failed_refreshes: 0,
                        next_refresh_at: now,
                    })
                }
            };
            if wait.lookups.len() >= self.bucket_capacity
                && swept.insert(lookup.table_bucket().clone())
            {
                wait.drop_abandoned();
            }
            if wait.lookups.len() >= self.bucket_capacity {
                lookup.complete_with_error(Error::BufferExhausted {
                    message: format!(
                        "{} lookups already wait for the leader of table bucket {} of table {}",
                        self.bucket_capacity,
                        lookup.table_bucket(),
                        lookup.table_path()
                    ),
                });
                *refused.entry(lookup.table_bucket().clone()).or_default() += 1;
                continue;
            }
            wait.lookups.push(lookup);
        }
        for (table_bucket, refused) in refused {
            warn!(
                "Failed {refused} lookups to table bucket {table_bucket}, as {} lookups already \
                 wait for its leader",
                self.bucket_capacity
            );
        }
        has_new_buckets
    }

    /// Marks the refresher running, and returns whether it wasn't, so the caller must start it.
    pub fn claim_refresher(&mut self) -> bool {
        !std::mem::replace(&mut self.refresher_running, true)
    }

    /// If no lookup waits, marks the refresher stopped and returns true.
    pub fn release_refresher_if_idle(&mut self) -> bool {
        let idle = self.buckets.is_empty();
        if idle {
            self.refresher_running = false;
        }
        idle
    }

    /// Removes the buckets whose leader `cluster` knows, and returns their lookups to send.
    /// Fails the lookups whose table ID was replaced by a newer one, so they neither wait forever
    /// nor read the new table. An older cached ID may come from a stale metadata response.
    pub fn take_found(&mut self, cluster: &Cluster) -> Vec<QueuedLookup> {
        let mut found = Vec::new();
        self.buckets.retain(|table_bucket, wait| {
            if let Some(table_id) = cluster
                .get_table_id(&wait.table_path)
                .filter(|&table_id| table_id > table_bucket.table_id())
            {
                let message = format!(
                    "Table {} with id {} no longer exists; its current id is {table_id}",
                    wait.table_path,
                    table_bucket.table_id()
                );
                warn!(
                    "{message}, failing {} lookups of bucket {}",
                    wait.lookups.len(),
                    table_bucket.bucket_id()
                );
                for lookup in &mut wait.lookups {
                    lookup.complete_with_error(Error::table_not_exist(message.clone()));
                }
                return false;
            }
            let Some(leader) = cluster.leader_for(table_bucket) else {
                return true;
            };
            // Pairs with the warning of the first refresh that found no leader.
            let level = if wait.failed_refreshes > 0 {
                Level::Info
            } else {
                Level::Debug
            };
            log!(
                level,
                "Found leader {} of table bucket {table_bucket} of table {} after {} refreshes \
                 without one, sending the {} lookups that waited for it",
                leader.id(),
                wait.table_path,
                wait.failed_refreshes,
                wait.lookups.len()
            );
            found.append(&mut wait.lookups);
            false
        });
        found
    }

    /// The refresh of the buckets due by `now`, or of every bucket with `force`; `None` if none
    /// is. A due bucket whose callers all stopped waiting is dropped instead.
    pub fn due_refresh(&mut self, now: Instant, force: bool) -> Option<LeaderRefresh> {
        let mut refresh = LeaderRefresh::default();
        self.buckets.retain(|table_bucket, wait| {
            if !force && wait.next_refresh_at > now {
                return true;
            }
            wait.drop_abandoned();
            if wait.lookups.is_empty() {
                return false;
            }
            refresh.table_buckets.push(table_bucket.clone());
            refresh.table_paths.insert(wait.table_path.clone());
            refresh.table_ids.insert(table_bucket.table_id());
            if let Some(partition_id) = table_bucket.partition_id() {
                refresh
                    .partitions
                    .insert(partition_id, wait.table_path.clone());
            }
            true
        });
        (!refresh.table_buckets.is_empty()).then_some(refresh)
    }

    /// When the next bucket is due for a refresh; `None` if no lookup waits.
    pub fn next_refresh_at(&self) -> Option<Instant> {
        self.buckets.values().map(|wait| wait.next_refresh_at).min()
    }

    /// Backs off the next refresh of those of `table_buckets` that still wait, which the refresh
    /// that just ended found no leader for. Call after [`Self::take_found`].
    pub fn refresh_failed(&mut self, table_buckets: &[TableBucket], now: Instant) {
        for table_bucket in table_buckets {
            let Some(wait) = self.buckets.get_mut(table_bucket) else {
                continue;
            };
            let delay_ms = retry_backoff_ms(
                self.retry_backoff_ms,
                self.retry_max_backoff_ms,
                wait.failed_refreshes,
            );
            wait.failed_refreshes = wait.failed_refreshes.saturating_add(1);
            wait.next_refresh_at = now + Duration::from_millis(delay_ms);
            // Only the first refresh of an outage warns, so a long one with many buckets doesn't
            // flood the log.
            let level = if wait.failed_refreshes == 1 {
                Level::Warn
            } else {
                Level::Debug
            };
            log!(
                level,
                "No leader found for table bucket {table_bucket} of table {} after {} metadata \
                 refreshes, {} lookups wait for the next refresh in {delay_ms} ms",
                wait.table_path,
                wait.failed_refreshes,
                wait.lookups.len()
            );
        }
    }

    /// Fails the lookups of those of `table_buckets` that still wait, as the client closes before
    /// their leader is found. Call after [`Self::take_found`].
    pub fn fail_on_close(&mut self, table_buckets: &[TableBucket]) {
        for table_bucket in table_buckets {
            let Some(mut wait) = self.buckets.remove(table_bucket) else {
                continue;
            };
            let message = format!(
                "no leader found for table bucket {table_bucket} of table {}",
                wait.table_path
            );
            warn!(
                "Lookup client closed with {message}, failing {} lookups",
                wait.lookups.len()
            );
            for lookup in &mut wait.lookups {
                lookup.complete_with_error(Error::LookupClosed {
                    message: message.clone(),
                });
            }
        }
    }

    /// Fails the lookups of those of `table_buckets` that still wait with the errors of
    /// `make_error`. Call after [`Self::take_found`].
    pub fn fail(&mut self, table_buckets: &[TableBucket], make_error: impl Fn() -> Error) {
        for table_bucket in table_buckets {
            let Some(mut wait) = self.buckets.remove(table_bucket) else {
                continue;
            };
            for lookup in &mut wait.lookups {
                lookup.complete_with_error(make_error());
            }
        }
    }

    /// Fails the lookups of every bucket of `partition_id` that still waits with the errors of
    /// `make_error`, as the partition no longer exists. Returns how many lookups failed.
    pub fn fail_partition(
        &mut self,
        partition_id: PartitionId,
        make_error: impl Fn() -> Error,
    ) -> usize {
        self.fail_matching(
            |table_bucket, _| table_bucket.partition_id() == Some(partition_id),
            make_error,
        )
    }

    /// Fails the lookups waiting for the buckets of `table_path` whose table ID is one of
    /// `table_ids`, and returns how many failed.
    pub fn fail_table(
        &mut self,
        table_path: &TablePath,
        table_ids: &HashSet<TableId>,
        make_error: impl Fn() -> Error,
    ) -> usize {
        self.fail_matching(
            |table_bucket, path| path == table_path && table_ids.contains(&table_bucket.table_id()),
            make_error,
        )
    }

    fn fail_matching(
        &mut self,
        matches: impl Fn(&TableBucket, &TablePath) -> bool,
        make_error: impl Fn() -> Error,
    ) -> usize {
        let mut failed = 0;
        self.buckets.retain(|table_bucket, wait| {
            if !matches(table_bucket, &wait.table_path) {
                return true;
            }
            for lookup in &mut wait.lookups {
                lookup.complete_with_error(make_error());
            }
            failed += wait.lookups.len();
            false
        });
        failed
    }

    #[cfg(test)]
    pub fn waiting_lookups(&self) -> usize {
        self.buckets.values().map(|wait| wait.lookups.len()).sum()
    }

    #[cfg(test)]
    pub fn failed_refreshes(&self, table_bucket: &TableBucket) -> Option<i32> {
        self.buckets
            .get(table_bucket)
            .map(|wait| wait.failed_refreshes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::lookup::{PrefixLookupQuery, PrimaryLookupQuery};
    use crate::error::Result;
    use crate::test_utils::build_cluster_arc;
    use bytes::Bytes;
    use tokio::sync::oneshot;

    const TABLE_ID: i64 = 1;
    const CAPACITY: usize = 1024;

    fn table_path() -> TablePath {
        TablePath::new("db", "tbl")
    }

    fn lookup(
        table_bucket: TableBucket,
    ) -> (QueuedLookup, oneshot::Receiver<Result<Option<Vec<u8>>>>) {
        let (result_tx, result_rx) = oneshot::channel();
        let query = PrimaryLookupQuery::new(
            table_path(),
            table_bucket,
            1,
            Bytes::from_static(b"key"),
            result_tx,
        );
        (query.into(), result_rx)
    }

    fn leaderless_bucket() -> TableBucket {
        TableBucket::new(TABLE_ID, 1)
    }

    #[tokio::test(start_paused = true)]
    async fn only_a_failed_refresh_puts_off_the_next_refresh_of_a_bucket() {
        let cluster = build_cluster_arc(&table_path(), TABLE_ID, 1);
        let mut waits = LeaderWaits::new(CAPACITY, 100, 1000);
        let table_bucket = leaderless_bucket();
        let (query, first_rx) = lookup(table_bucket.clone());
        assert!(waits.add(vec![query], Instant::now()));
        let mut results = vec![first_rx];

        for (min_ms, max_ms) in [(80, 120), (160, 240), (320, 480), (640, 960), (800, 1200)] {
            let now = Instant::now();
            let refresh = waits.due_refresh(now, false).expect("the bucket is due");
            assert_eq!(refresh.table_buckets, std::slice::from_ref(&table_bucket));
            assert!(waits.take_found(&cluster).is_empty());
            waits.refresh_failed(&refresh.table_buckets, now);

            let next_refresh_at = waits.next_refresh_at().unwrap();
            let backoff = next_refresh_at - now;
            assert!(
                (Duration::from_millis(min_ms)..=Duration::from_millis(max_ms)).contains(&backoff),
                "should back off {min_ms}..={max_ms}ms, got {backoff:?}"
            );
            while Instant::now() < next_refresh_at {
                assert!(waits.due_refresh(Instant::now(), false).is_none());
                let (query, result_rx) = lookup(table_bucket.clone());
                assert!(!waits.add(vec![query], Instant::now()));
                results.push(result_rx);
                tokio::time::advance(Duration::from_millis(10)).await;
            }
            assert_eq!(waits.next_refresh_at(), Some(next_refresh_at));
        }
        assert_eq!(waits.failed_refreshes(&table_bucket), Some(5));
        assert_eq!(waits.waiting_lookups(), results.len());
    }

    #[test]
    fn a_recreated_table_fails_lookups_waiting_for_its_old_id() {
        let mut waits = LeaderWaits::new(CAPACITY, 100, 1000);
        let (old, mut old_rx) = lookup(TableBucket::new(TABLE_ID, 0));
        let (prefix_tx, mut prefix_rx) = oneshot::channel();
        let prefix = PrefixLookupQuery::new(
            table_path(),
            TableBucket::new(TABLE_ID, 0),
            1,
            Bytes::from_static(b"key"),
            prefix_tx,
        );
        let (new, _new_rx) = lookup(TableBucket::new(TABLE_ID + 1, 0));
        waits.add(vec![old, prefix.into(), new], Instant::now());

        let found = waits.take_found(&build_cluster_arc(&table_path(), TABLE_ID + 1, 1));

        for error in [
            old_rx
                .try_recv()
                .expect("the old table no longer exists")
                .unwrap_err(),
            prefix_rx
                .try_recv()
                .expect("the old table no longer exists")
                .unwrap_err(),
        ] {
            assert_eq!(
                error.api_error(),
                Some(crate::error::FlussError::TableNotExist)
            );
        }
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].table_bucket().table_id(), TABLE_ID + 1);
        assert_eq!(waits.waiting_lookups(), 0);
    }

    #[test]
    fn a_bucket_stops_waiting_once_its_leader_is_found_or_no_caller_waits() {
        let mut waits = LeaderWaits::new(CAPACITY, 100, 1000);
        let (first, _first_rx) = lookup(TableBucket::new(TABLE_ID, 0));
        let (second, _second_rx) = lookup(TableBucket::new(TABLE_ID, 0));
        let (abandoned, abandoned_rx) = lookup(leaderless_bucket());
        waits.add(vec![first, second, abandoned], Instant::now());
        assert!(waits.claim_refresher());
        assert!(!waits.claim_refresher(), "the refresher already runs");
        assert!(!waits.release_refresher_if_idle(), "lookups still wait");

        drop(abandoned_rx);
        let found = waits.take_found(&build_cluster_arc(&table_path(), TABLE_ID, 1));

        assert_eq!(found.len(), 2);
        assert!(
            found
                .iter()
                .all(|lookup| *lookup.table_bucket() == TableBucket::new(TABLE_ID, 0))
        );
        assert!(waits.due_refresh(Instant::now(), false).is_none());
        assert_eq!(waits.waiting_lookups(), 0);
        assert!(waits.release_refresher_if_idle());
        assert!(waits.claim_refresher(), "the refresher stopped");
    }
}
