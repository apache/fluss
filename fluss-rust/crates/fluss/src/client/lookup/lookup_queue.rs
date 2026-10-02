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
use std::time::Duration;
use tokio::sync::{mpsc, watch};

/// A queue that buffers pending lookup operations and provides batched draining.
///
/// The queue supports two types of entries:
/// - New lookups from client calls
/// - Re-enqueued lookups from retry logic
///
/// Re-enqueued lookups are prioritized over new lookups to ensure fair processing.
pub struct LookupQueue {
    /// Channel for receiving lookup requests
    lookup_rx: mpsc::Receiver<QueuedLookup>,
    /// Channel for receiving re-enqueued lookups
    re_enqueue_rx: mpsc::UnboundedReceiver<QueuedLookup>,
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
            max_batch_size,
            batch_timeout: Duration::from_millis(batch_timeout_ms),
            cluster_rx,
        };

        (queue, lookup_tx, re_enqueue_tx)
    }

    /// Drains a batch of lookup queries from the queue.
    pub async fn drain(&mut self) -> Vec<QueuedLookup> {
        let mut lookups = Vec::with_capacity(self.max_batch_size);
        let deadline = tokio::time::Instant::now() + self.batch_timeout;

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }

            // Prioritize re-enqueued lookups.
            while lookups.len() < self.max_batch_size {
                match self.re_enqueue_rx.try_recv() {
                    Ok(lookup) => lookups.push(lookup),
                    Err(_) => break,
                }
            }
            if lookups.len() >= self.max_batch_size {
                break;
            }

            let sleep = tokio::time::sleep(remaining);
            tokio::select! {
                biased;
                maybe = self.lookup_rx.recv() => {
                    match maybe {
                        Some(lookup) => {
                            lookups.push(lookup);
                            while lookups.len() < self.max_batch_size {
                                match self.lookup_rx.try_recv() {
                                    Ok(lookup) => lookups.push(lookup),
                                    Err(_) => break,
                                }
                            }
                        }
                        None => break,
                    }
                }
                _ = self.cluster_rx.changed() => {
                    if !lookups.is_empty() {
                        break;
                    }
                }
                _ = sleep => break,
            }

            if lookups.len() >= self.max_batch_size {
                break;
            }
        }

        lookups
    }

    /// Drains all remaining lookups from the queue.
    pub fn drain_all(&mut self) -> Vec<QueuedLookup> {
        let mut lookups = Vec::new();

        // Drain re-enqueued lookups
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
        !self.lookup_rx.is_empty() || !self.re_enqueue_rx.is_empty()
    }
}
