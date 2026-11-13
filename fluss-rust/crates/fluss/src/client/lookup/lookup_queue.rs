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

//! Lookup queue for buffering pending lookup operations.
//!
//! This queue buffers lookup operations and provides batched draining
//! to improve throughput by reducing network round trips.

use super::QueuedLookup;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

/// A queue that buffers pending lookup operations and provides batched draining.
///
/// The queue supports two types of entries:
/// - New lookups from client calls
/// - Re-enqueued lookups from retry logic
///
/// Re-enqueued lookups are held until their retry backoff has passed, then
/// prioritized over new lookups to ensure fair processing.
pub struct LookupQueue {
    /// Channel for receiving lookup requests
    lookup_rx: mpsc::Receiver<QueuedLookup>,
    /// Channel for receiving re-enqueued lookups
    re_enqueue_rx: mpsc::UnboundedReceiver<QueuedLookup>,
    /// Re-enqueued lookups, earliest retry first.
    retries: BinaryHeap<DelayedLookup>,
    /// Retains a partially collected batch if `drain` is cancelled by shutdown.
    pending_batch: Vec<QueuedLookup>,
    /// Keeps retries with the same deadline in the order they were re-enqueued.
    next_retry_seq: u64,
    /// Maximum batch size for draining
    max_batch_size: usize,
    /// Timeout for batch collection
    batch_timeout: Duration,
    /// Wakes `drain()` early when the cluster changes.
    cluster_rx: watch::Receiver<u64>,
}

impl LookupQueue {
    pub fn new(
        queue_size: usize,
        max_batch_size: usize,
        batch_timeout_ms: u64,
        cluster_rx: watch::Receiver<u64>,
    ) -> (
        Self,
        mpsc::Sender<QueuedLookup>,
        mpsc::UnboundedSender<QueuedLookup>,
    ) {
        let (lookup_tx, lookup_rx) = mpsc::channel(queue_size);
        let (re_enqueue_tx, re_enqueue_rx) = mpsc::unbounded_channel();

        let queue = Self {
            lookup_rx,
            re_enqueue_rx,
            retries: BinaryHeap::new(),
            pending_batch: Vec::with_capacity(max_batch_size),
            next_retry_seq: 0,
            max_batch_size,
            batch_timeout: Duration::from_millis(batch_timeout_ms),
            cluster_rx,
        };

        (queue, lookup_tx, re_enqueue_tx)
    }

    /// Drains a batch of lookup queries from the queue. Cancellation keeps the partial batch
    /// available to the next drain, including [`Self::drain_all`] during shutdown.
    pub async fn drain(&mut self) -> Vec<QueuedLookup> {
        let deadline = Instant::now() + self.batch_timeout;

        loop {
            let now = Instant::now();
            let remaining = deadline.saturating_duration_since(now);

            // Re-enqueued lookups whose backoff has passed go first. A batch they fill still
            // takes one new lookup, so retries, e.g. the lookups of a bucket whose leader was just
            // found, can't hold back the other lookups.
            self.take_due_retries(now);
            if self.pending_batch.len() >= self.max_batch_size {
                if let Ok(lookup) = self.lookup_rx.try_recv() {
                    self.pending_batch.push(lookup);
                }
                break;
            }
            let next_retry_at = self.retries.peek().map(|retry| retry.retry_at);

            let sleep = tokio::time::sleep(remaining);
            tokio::select! {
                biased;
                maybe = self.lookup_rx.recv() => {
                    match maybe {
                        Some(lookup) => {
                            self.pending_batch.push(lookup);
                            while self.pending_batch.len() < self.max_batch_size {
                                match self.lookup_rx.try_recv() {
                                    Ok(lookup) => self.pending_batch.push(lookup),
                                    Err(_) => break,
                                }
                            }
                        }
                        None => break,
                    }
                }
                // A retry enqueued meanwhile may become due before the batch is sent. Without a
                // batch delay, this also lets an empty queue wait for work instead of spinning.
                maybe = self.re_enqueue_rx.recv() => {
                    match maybe {
                        Some(lookup) => self.push_retry(lookup, now),
                        None => break,
                    }
                    continue;
                }
                _ = self.cluster_rx.changed() => {
                    if !self.pending_batch.is_empty() {
                        break;
                    }
                }
                // A retry whose backoff passes before the batch is sent joins it.
                _ = tokio::time::sleep_until(next_retry_at.unwrap_or(now)), if next_retry_at.is_some() => {
                    continue;
                }
                _ = sleep, if !self.batch_timeout.is_zero() || !self.pending_batch.is_empty() => break,
            }

            // Even a zero timeout must poll the queues once before ending the batch.
            if self.pending_batch.len() >= self.max_batch_size || Instant::now() >= deadline {
                break;
            }
        }

        std::mem::replace(
            &mut self.pending_batch,
            Vec::with_capacity(self.max_batch_size),
        )
    }

    /// Moves re-enqueued lookups due by `now` into the pending batch, up to the batch size.
    fn take_due_retries(&mut self, now: Instant) {
        while let Ok(lookup) = self.re_enqueue_rx.try_recv() {
            self.push_retry(lookup, now);
        }
        while self.pending_batch.len() < self.max_batch_size {
            match self.retries.peek_mut() {
                Some(retry) if retry.retry_at <= now => {
                    self.pending_batch.push(PeekMut::pop(retry).lookup)
                }
                _ => break,
            }
        }
    }

    fn push_retry(&mut self, lookup: QueuedLookup, now: Instant) {
        // A passed deadline counts from now, so a lookup re-enqueued without a new backoff, e.g.
        // once the leader of its bucket is found, queues behind the retries already due.
        let retry_at = lookup.retry_at().map_or(now, |retry_at| retry_at.max(now));
        self.retries.push(DelayedLookup {
            retry_at,
            seq: self.next_retry_seq,
            lookup,
        });
        self.next_retry_seq += 1;
    }

    /// Waits until a lookup is re-enqueued, and keeps it for the next drain.
    pub async fn wait_for_re_enqueued(&mut self) {
        match self.re_enqueue_rx.recv().await {
            Some(lookup) => self.push_retry(lookup, Instant::now()),
            // The sender holds a handle to re-enqueue lookups while it runs.
            None => std::future::pending().await,
        }
    }

    /// Drains all remaining lookups from the queue.
    pub fn drain_all(&mut self) -> Vec<QueuedLookup> {
        let mut lookups = std::mem::take(&mut self.pending_batch);

        // Drain re-enqueued lookups, whether or not their backoff has passed
        while let Some(retry) = self.retries.pop() {
            lookups.push(retry.lookup);
        }
        while let Ok(lookup) = self.re_enqueue_rx.try_recv() {
            lookups.push(lookup);
        }

        // Drain main queue
        while let Ok(lookup) = self.lookup_rx.try_recv() {
            lookups.push(lookup);
        }

        lookups
    }

    /// Returns true if there are undrained lookups in the queue.
    pub fn has_undrained(&self) -> bool {
        !self.pending_batch.is_empty()
            || !self.lookup_rx.is_empty()
            || !self.re_enqueue_rx.is_empty()
            || !self.retries.is_empty()
    }
}

/// A re-enqueued lookup waiting for its retry backoff.
struct DelayedLookup {
    retry_at: Instant,
    seq: u64,
    lookup: QueuedLookup,
}

impl DelayedLookup {
    fn key(&self) -> (Instant, u64) {
        (self.retry_at, self.seq)
    }
}

// Reversed, so that `BinaryHeap` pops the earliest retry first.
impl Ord for DelayedLookup {
    fn cmp(&self, other: &Self) -> Ordering {
        other.key().cmp(&self.key())
    }
}

impl PartialOrd for DelayedLookup {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for DelayedLookup {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for DelayedLookup {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::lookup::PrimaryLookupQuery;
    use crate::metadata::{TableBucket, TablePath};
    use bytes::Bytes;
    use tokio::sync::oneshot;

    fn lookup(key: &'static [u8]) -> QueuedLookup {
        let (result_tx, _result_rx) = oneshot::channel();
        QueuedLookup::Primary(PrimaryLookupQuery::new(
            TablePath::new("db", "tbl"),
            TableBucket::new(1, 0),
            1,
            Bytes::from_static(key),
            result_tx,
        ))
    }

    fn retry(key: &'static [u8], backoff: Duration) -> QueuedLookup {
        let QueuedLookup::Primary(mut query) = lookup(key) else {
            unreachable!()
        };
        query.prepare_retry(Instant::now() + backoff);
        QueuedLookup::Primary(query)
    }

    #[tokio::test(start_paused = true)]
    async fn cancelling_a_drain_keeps_the_partial_batch_for_close() {
        let (_cluster_tx, cluster_rx) = watch::channel(0);
        let (mut queue, lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 8, 100, cluster_rx);
        lookup_tx.send(lookup(b"new")).await.unwrap();
        re_enqueue_tx.send(retry(b"retry", Duration::ZERO)).unwrap();
        {
            let drain = queue.drain();
            tokio::pin!(drain);
            assert!(futures::poll!(&mut drain).is_pending());
        }

        let mut keys: Vec<_> = queue
            .drain_all()
            .into_iter()
            .map(|query| query.key().clone())
            .collect();
        keys.sort();
        assert_eq!(
            keys,
            [Bytes::from_static(b"new"), Bytes::from_static(b"retry")]
        );
        assert!(!queue.has_undrained());
    }

    #[tokio::test(start_paused = true)]
    async fn retries_wait_for_their_backoff_without_delaying_new_lookups() {
        let (_cluster_tx, cluster_rx) = watch::channel(0);
        let (mut queue, lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 8, 0, cluster_rx);
        re_enqueue_tx
            .send(retry(b"later", Duration::from_millis(200)))
            .unwrap();
        re_enqueue_tx
            .send(retry(b"sooner", Duration::from_millis(100)))
            .unwrap();
        lookup_tx.send(lookup(b"new")).await.unwrap();
        let start = Instant::now();

        for (key, elapsed_ms) in [(b"new".as_slice(), 0), (b"sooner", 100), (b"later", 200)] {
            let lookups = queue.drain().await;
            assert_eq!(lookups.len(), 1);
            assert_eq!(lookups[0].key().as_ref(), key);
            assert_eq!(start.elapsed(), Duration::from_millis(elapsed_ms));
        }
        assert!(!queue.has_undrained());
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_queue_waits_for_a_retry_to_become_due() {
        let (_cluster_tx, cluster_rx) = watch::channel(0);
        let (mut queue, _lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 8, 0, cluster_rx);
        let start = Instant::now();
        let drain = queue.drain();
        tokio::pin!(drain);

        assert!(futures::poll!(&mut drain).is_pending());
        re_enqueue_tx
            .send(retry(b"retry", Duration::from_millis(100)))
            .unwrap();
        assert!(futures::poll!(&mut drain).is_pending());

        let lookups = drain.await;
        assert_eq!(lookups.len(), 1);
        assert_eq!(lookups[0].key().as_ref(), b"retry");
        assert_eq!(start.elapsed(), Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn a_retry_due_during_the_batch_timeout_joins_the_batch() {
        let (_cluster_tx, cluster_rx) = watch::channel(0);
        let (mut queue, lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 8, 100, cluster_rx);
        lookup_tx.send(lookup(b"new")).await.unwrap();
        let start = Instant::now();
        let drain = queue.drain();
        tokio::pin!(drain);

        assert!(futures::poll!(&mut drain).is_pending());
        tokio::time::advance(Duration::from_millis(10)).await;
        re_enqueue_tx
            .send(retry(b"retry", Duration::from_millis(40)))
            .unwrap();

        let lookups = drain.await;
        let keys: Vec<&[u8]> = lookups.iter().map(|lookup| lookup.key().as_ref()).collect();
        assert_eq!(keys, [b"new".as_slice(), b"retry"]);
        assert_eq!(start.elapsed(), Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn a_batch_filled_by_a_due_retry_does_not_wait_for_the_timeout() {
        let (_cluster_tx, cluster_rx) = watch::channel(0);
        let (mut queue, lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 2, 100, cluster_rx);
        lookup_tx.send(lookup(b"new")).await.unwrap();
        re_enqueue_tx
            .send(retry(b"retry", Duration::from_millis(50)))
            .unwrap();
        let start = Instant::now();

        let lookups = queue.drain().await;
        let keys: Vec<&[u8]> = lookups.iter().map(|lookup| lookup.key().as_ref()).collect();
        assert_eq!(keys, [b"new".as_slice(), b"retry"]);
        assert_eq!(start.elapsed(), Duration::from_millis(50));
    }

    #[tokio::test(start_paused = true)]
    async fn a_lookup_requeued_past_its_deadline_does_not_overtake_due_retries() {
        let (_cluster_tx, cluster_rx) = watch::channel(0);
        let (mut queue, _lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 1, 0, cluster_rx);
        re_enqueue_tx
            .send(retry(b"no leader", Duration::ZERO))
            .unwrap();
        re_enqueue_tx
            .send(retry(b"healthy", Duration::from_millis(10)))
            .unwrap();
        tokio::time::advance(Duration::from_millis(20)).await;

        let mut no_leader = queue.drain().await;
        assert_eq!(no_leader[0].key().as_ref(), b"no leader");
        re_enqueue_tx.send(no_leader.pop().unwrap()).unwrap();

        for key in [b"healthy".as_slice(), b"no leader"] {
            let lookups = queue.drain().await;
            assert_eq!(lookups.len(), 1);
            assert_eq!(lookups[0].key().as_ref(), key);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn drain_all_takes_retries_still_backing_off() {
        let (_cluster_tx, cluster_rx) = watch::channel(0);
        let (mut queue, _lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 8, 100, cluster_rx);
        re_enqueue_tx
            .send(retry(b"retry", Duration::from_secs(60)))
            .unwrap();

        assert!(queue.drain().await.is_empty());
        assert!(queue.has_undrained());

        let lookups = queue.drain_all();
        assert_eq!(lookups.len(), 1);
        assert_eq!(lookups[0].key().as_ref(), b"retry");
        assert!(!queue.has_undrained());
    }

    #[tokio::test(start_paused = true)]
    async fn zero_timeout_drains_lookups_up_to_the_batch_size() {
        for re_enqueued in [false, true] {
            let (_cluster_tx, cluster_rx) = watch::channel(0);
            let (mut queue, lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 2, 0, cluster_rx);
            for key in [b"first", b"second".as_slice(), b"third"] {
                if re_enqueued {
                    re_enqueue_tx.send(lookup(key)).unwrap();
                } else {
                    lookup_tx.send(lookup(key)).await.unwrap();
                }
            }

            let lookups = queue.drain().await;
            let keys: Vec<&[u8]> = lookups.iter().map(|lookup| lookup.key().as_ref()).collect();
            assert_eq!(keys, [b"first".as_slice(), b"second"]);
            assert!(queue.has_undrained());

            let lookups = queue.drain().await;
            assert_eq!(lookups.len(), 1);
            assert_eq!(lookups[0].key().as_ref(), b"third");
            assert!(!queue.has_undrained());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn zero_timeout_waits_for_work_when_the_queue_is_empty() {
        for re_enqueued in [false, true] {
            let (_cluster_tx, cluster_rx) = watch::channel(0);
            let (mut queue, lookup_tx, re_enqueue_tx) = LookupQueue::new(8, 2, 0, cluster_rx);
            let drain = queue.drain();
            tokio::pin!(drain);

            assert!(futures::poll!(&mut drain).is_pending());
            if re_enqueued {
                re_enqueue_tx.send(lookup(b"lookup")).unwrap();
            } else {
                lookup_tx.send(lookup(b"lookup")).await.unwrap();
            }

            let lookups = drain.await;
            assert_eq!(lookups.len(), 1);
            assert_eq!(lookups[0].key().as_ref(), b"lookup");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_partial_batch_waits_for_a_positive_timeout() {
        let (_cluster_tx, cluster_rx) = watch::channel(0);
        let (mut queue, lookup_tx, _re_enqueue_tx) = LookupQueue::new(8, 2, 100, cluster_rx);
        lookup_tx.send(lookup(b"new")).await.unwrap();
        let start = tokio::time::Instant::now();

        let lookups = queue.drain().await;
        assert_eq!(lookups.len(), 1);
        assert_eq!(lookups[0].key().as_ref(), b"new");
        assert_eq!(start.elapsed(), Duration::from_millis(100));
    }

    #[tokio::test(start_paused = true)]
    async fn a_batch_full_of_retries_still_takes_a_new_lookup() {
        for batch_timeout_ms in [0, 60_000] {
            let (_cluster_tx, cluster_rx) = watch::channel(0);
            let (mut queue, lookup_tx, re_enqueue_tx) =
                LookupQueue::new(8, 2, batch_timeout_ms, cluster_rx);
            re_enqueue_tx.send(lookup(b"retry")).unwrap();
            re_enqueue_tx.send(lookup(b"retry")).unwrap();
            lookup_tx.send(lookup(b"new")).await.unwrap();

            let lookups = tokio::time::timeout(Duration::from_secs(10), queue.drain())
                .await
                .expect("a full batch does not wait for the batch timeout");

            let keys: Vec<&[u8]> = lookups.iter().map(|lookup| lookup.key().as_ref()).collect();
            assert_eq!(keys, [b"retry".as_slice(), b"retry", b"new"]);
            assert!(!queue.has_undrained());
        }
    }
}
