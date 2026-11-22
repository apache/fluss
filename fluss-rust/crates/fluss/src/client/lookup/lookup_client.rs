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

//! Lookup client that batches multiple lookups together for improved throughput.
//!
//! This client achieves parity with the Java client by:
//! - Queuing lookup operations instead of sending them immediately
//! - Batching multiple lookups to the same server/bucket
//! - Running a background sender task to process batches

use super::{LookupQueue, PrefixLookupQuery, PrimaryLookupQuery, QueuedLookup};
use crate::client::lookup::lookup_sender::LookupSender;
use crate::client::metadata::Metadata;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::metadata::{TableBucket, TablePath};
use bytes::Bytes;
use log::{debug, error};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

/// A client that lookups values from the server with batching support.
///
/// The lookup client uses a queue and background sender to batch multiple
/// lookup operations together, reducing network round trips and improving
/// throughput.
///
/// # Example
///
/// ```ignore
/// let lookup_client = LookupClient::new(config, metadata);
/// let result = lookup_client.lookup(table_path, table_bucket, key_bytes).await?;
/// ```
pub struct LookupClient {
    /// Channel to send lookup requests to the queue
    lookup_tx: mpsc::Sender<QueuedLookup>,
    /// Handle to the sender task
    sender_handle: Option<JoinHandle<()>>,
    /// Watch channel for internal shutdown handling
    shutdown_tx: watch::Sender<bool>,
    /// Whether the client is closed
    closed: AtomicBool,
}

impl LookupClient {
    /// Creates a new lookup client.
    pub fn new(config: &Config, metadata: Arc<Metadata>) -> Self {
        // Extract configuration values
        let queue_size = config.lookup_queue_size;
        let max_batch_size = config.lookup_max_batch_size;
        let batch_timeout_ms = config.lookup_batch_timeout_ms;
        let max_inflight = config.lookup_max_inflight_requests;
        let max_retries = config.lookup_max_retries;
        let retry_backoff_ms = config.lookup_retry_backoff_ms;
        let retry_max_backoff_ms = config.lookup_retry_max_backoff_ms;

        // Create queue and channels
        let cluster_rx = metadata.subscribe_cluster_changes();
        let (queue, lookup_tx, re_enqueue_tx) =
            LookupQueue::new(queue_size, max_batch_size, batch_timeout_ms, cluster_rx);

        // Create shutdown channel
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Create sender with shutdown receiver
        let mut sender = LookupSender::new(
            metadata,
            queue,
            re_enqueue_tx,
            max_inflight,
            queue_size,
            max_retries,
            retry_backoff_ms,
            retry_max_backoff_ms,
            shutdown_rx,
        );

        // Spawn sender task - sender handles shutdown internally
        let sender_handle = tokio::spawn(async move {
            sender.run().await;
            debug!("Lookup sender completed");
        });

        Self {
            lookup_tx,
            sender_handle: Some(sender_handle),
            shutdown_tx,
            closed: AtomicBool::new(false),
        }
    }

    /// Looks up a value by its primary key.
    ///
    /// This method queues the lookup operation and returns a future that will
    /// complete when the server responds. Multiple lookups may be batched together
    /// for improved throughput.
    ///
    /// # Arguments
    /// * `table_path` - The table path
    /// * `table_bucket` - The table bucket
    /// * `routing_bucket_count` - The bucket count `table_bucket` was computed with
    /// * `key_bytes` - The encoded primary key bytes
    ///
    /// # Returns
    /// * `Ok(Some(bytes))` - The value bytes if found
    /// * `Ok(None)` - If the key was not found
    /// * `Err(Error)` - If the lookup fails
    pub async fn lookup(
        &self,
        table_path: TablePath,
        table_bucket: TableBucket,
        routing_bucket_count: i32,
        key_bytes: Bytes,
    ) -> Result<Option<Vec<u8>>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::LookupClosed {
                message: "no new lookups are accepted".to_string(),
            });
        }

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let query = QueuedLookup::Primary(PrimaryLookupQuery::new(
            table_path,
            table_bucket,
            routing_bucket_count,
            key_bytes,
            result_tx,
        ));

        self.enqueue(query).await?;

        result_rx.await.map_err(|_| Error::LookupClosed {
            message: "the lookup sender stopped before the lookup completed".to_string(),
        })?
    }

    /// Looks up all values matching a prefix key.
    ///
    /// The prefix key must be a prefix subset of the table's primary key
    /// (specifically, the bucket keys). Returns every row whose primary key
    /// starts with the supplied prefix. Queries are batched together with
    /// other lookups going to the same server for improved throughput.
    ///
    /// # Arguments
    /// * `table_path` - The table path
    /// * `table_bucket` - The table bucket computed from the bucket key part of the prefix
    /// * `routing_bucket_count` - The bucket count `table_bucket` was computed with
    /// * `key_bytes` - The encoded prefix key bytes
    ///
    /// # Returns
    /// * `Ok(rows)` - Every row matching the prefix (possibly empty)
    /// * `Err(Error)` - If the lookup fails
    pub async fn prefix_lookup(
        &self,
        table_path: TablePath,
        table_bucket: TableBucket,
        routing_bucket_count: i32,
        key_bytes: Bytes,
    ) -> Result<Vec<Vec<u8>>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::LookupClosed {
                message: "no new lookups are accepted".to_string(),
            });
        }

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let query = QueuedLookup::Prefix(PrefixLookupQuery::new(
            table_path,
            table_bucket,
            routing_bucket_count,
            key_bytes,
            result_tx,
        ));

        self.enqueue(query).await?;

        result_rx.await.map_err(|_| Error::LookupClosed {
            message: "the lookup sender stopped before the lookup completed".to_string(),
        })?
    }

    async fn enqueue(&self, query: QueuedLookup) -> Result<()> {
        self.lookup_tx.send(query).await.map_err(|e| {
            let failed_query = e.0;
            error!(
                "Failed to queue lookup: channel closed. table_path: {}, table_bucket: {:?}, key_len: {}",
                failed_query.table_path(),
                failed_query.table_bucket(),
                failed_query.key().len()
            );
            Error::LookupClosed {
                message: "the lookup sender has stopped".to_string(),
            }
        })
    }

    /// Closes the lookup client gracefully.
    pub async fn close(mut self, timeout: Duration) {
        debug!("Closing lookup client");

        // Mark as closed to reject new lookups
        self.closed.store(true, Ordering::Release);

        // Send shutdown signal via watch channel
        let _ = self.shutdown_tx.send(true);

        // Wait for sender to complete with timeout
        if let Some(handle) = self.sender_handle.take() {
            debug!("Waiting for sender task to complete...");
            let abort_handle = handle.abort_handle();

            match tokio::time::timeout(timeout, handle).await {
                Ok(Ok(())) => {
                    debug!("Lookup sender task completed gracefully.");
                }
                Ok(Err(join_error)) => {
                    error!("Lookup sender task panicked: {:?}", join_error);
                }
                Err(_elapsed) => {
                    error!("Lookup sender task did not complete within timeout. Forcing shutdown.");
                    abort_handle.abort();
                }
            }
        } else {
            debug!("Lookup client was already closed or never initialized properly.");
        }

        debug!("Lookup client closed");
    }
}

impl Drop for LookupClient {
    fn drop(&mut self) {
        // Abort the sender task on drop if it wasn't already consumed by close()
        if let Some(handle) = self.sender_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::build_cluster_arc_with_port;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn a_pending_lookup_fails_with_lookup_closed_once_the_sender_stops() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = u32::from(listener.local_addr().unwrap().port());
        let table_path = TablePath::new("db", "tbl");
        let metadata = Metadata::new_for_test(build_cluster_arc_with_port(&table_path, 1, 1, port));
        let config = Config {
            lookup_batch_timeout_ms: 0,
            ..Default::default()
        };
        let client = LookupClient::new(&config, Arc::new(metadata));
        let lookup = client.lookup(
            table_path,
            TableBucket::new(1, 0),
            1,
            Bytes::from_static(b"key"),
        );
        tokio::pin!(lookup);
        let _connection = tokio::select! {
            result = &mut lookup => panic!("the lookup completed without an answer: {result:?}"),
            accepted = listener.accept() => accepted.unwrap(),
        };

        client.sender_handle.as_ref().unwrap().abort();

        let error = tokio::time::timeout(Duration::from_secs(10), lookup)
            .await
            .expect("the lookup completed")
            .unwrap_err();
        assert!(matches!(error, Error::LookupClosed { .. }), "{error}");
    }
}
