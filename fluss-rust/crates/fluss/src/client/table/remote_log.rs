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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::client::credentials::CredentialsReceiver;
use crate::error::{Error, Result};
use crate::io::{FileIO, Storage};
use crate::metadata::TableBucket;
use crate::metrics::ScannerMetrics;
use crate::proto::{PbRemoteLogFetchInfo, PbRemoteLogSegment};
use futures::TryStreamExt;
use parking_lot::Mutex;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap},
    future::Future,
    io, mem,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

#[cfg(test)]
use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;

/// Default maximum number of remote log segments to prefetch
/// Matches Java's CLIENT_SCANNER_REMOTE_LOG_PREFETCH_NUM (default: 4)
pub const DEFAULT_SCANNER_REMOTE_LOG_PREFETCH_NUM: usize = 4;

/// Default maximum concurrent remote log downloads
/// Matches Java's REMOTE_FILE_DOWNLOAD_THREAD_NUM (default: 3)
pub const DEFAULT_REMOTE_FILE_DOWNLOAD_THREAD_NUM: usize = 3;

/// Initial retry backoff delay (milliseconds)
/// Prevents hot-spin retry loops on persistent failures
const RETRY_BACKOFF_BASE_MS: u64 = 100;

/// Maximum retry backoff delay (milliseconds)
/// Caps exponential backoff to avoid excessive delays
const RETRY_BACKOFF_MAX_MS: u64 = 5_000;

/// Maximum number of retries before giving up
/// After this many retries, the download will fail permanently
const MAX_RETRY_COUNT: u32 = 10;

/// Calculate exponential backoff delay with jitter for retries
fn calculate_backoff_delay(retry_count: u32) -> tokio::time::Duration {
    use rand::Rng;

    // Exponential backoff: base * 2^retry_count
    let exponential_ms = RETRY_BACKOFF_BASE_MS.saturating_mul(1 << retry_count.min(10)); // Cap exponent to prevent overflow

    // Cap at maximum
    let capped_ms = exponential_ms.min(RETRY_BACKOFF_MAX_MS);

    // Add jitter (±25% randomness) to avoid thundering herd
    let mut rng = rand::rng();
    let jitter = rng.random_range(0.75..=1.25);
    let final_ms = ((capped_ms as f64) * jitter) as u64;

    tokio::time::Duration::from_millis(final_ms)
}

/// Result of a fetch operation containing file path and size
#[derive(Debug)]
pub struct FetchResult {
    pub file_path: PathBuf,
    pub file_size: usize,
}

/// Trait for fetching remote log segments (allows dependency injection for testing)
pub trait RemoteLogFetcher: Send + Sync {
    fn fetch(
        &self,
        request: &RemoteLogDownloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send>>;
}

/// Represents a remote log segment that needs to be downloaded
#[derive(Debug, Clone)]
pub struct RemoteLogSegment {
    pub segment_id: String,
    pub start_offset: i64,
    #[allow(dead_code)]
    pub end_offset: i64,
    #[allow(dead_code)]
    pub size_in_bytes: i32,
    pub table_bucket: TableBucket,
    pub max_timestamp: i64,
}

impl RemoteLogSegment {
    pub fn from_proto(segment: &PbRemoteLogSegment, table_bucket: TableBucket) -> Self {
        Self {
            segment_id: segment.remote_log_segment_id.clone(),
            start_offset: segment.remote_log_start_offset,
            end_offset: segment.remote_log_end_offset,
            size_in_bytes: segment.segment_size_in_bytes,
            table_bucket,
            // Match Java's behavior: use -1 for missing timestamp
            // (Java: CommonRpcMessageUtils.java:171-174)
            max_timestamp: segment.max_timestamp.unwrap_or(-1),
        }
    }

    /// Get the local file name for this remote log segment
    pub fn local_file_name(&self) -> String {
        // Format: ${remote_segment_id}_${offset_prefix}.log
        let offset_prefix = format!("{:020}", self.start_offset);
        format!("{}_{}.log", self.segment_id, offset_prefix)
    }
}

/// Represents remote log fetch information
#[derive(Debug, Clone)]
pub struct RemoteLogFetchInfo {
    pub remote_log_tablet_dir: String,
    #[allow(dead_code)]
    pub partition_name: Option<String>,
    pub remote_log_segments: Vec<RemoteLogSegment>,
    pub first_start_pos: i32,
}

impl RemoteLogFetchInfo {
    pub fn from_proto(info: &PbRemoteLogFetchInfo, table_bucket: TableBucket) -> Self {
        let segments = info
            .remote_log_segments
            .iter()
            .map(|s| RemoteLogSegment::from_proto(s, table_bucket.clone()))
            .collect();

        Self {
            remote_log_tablet_dir: info.remote_log_tablet_dir.clone(),
            partition_name: info.partition_name.clone(),
            remote_log_segments: segments,
            first_start_pos: info.first_start_pos.unwrap_or(0),
        }
    }
}

/// RAII guard for prefetch permit that notifies coordinator on drop
///
/// NOTE: File deletion is now handled by FileSource::drop(), not here.
/// This ensures the file is closed before deletion
#[derive(Debug)]
pub struct PrefetchPermit {
    permit: Option<OwnedSemaphorePermit>,
    recycle_notify: Arc<Notify>,
}

impl PrefetchPermit {
    fn new(permit: OwnedSemaphorePermit, recycle_notify: Arc<Notify>) -> Self {
        Self {
            permit: Some(permit),
            recycle_notify,
        }
    }
}

impl Drop for PrefetchPermit {
    fn drop(&mut self) {
        // Release capacity (critical: permit must be dropped before notify)
        let _ = self.permit.take(); // drops permit here

        // Then wake coordinator so it can acquire the now-available permit
        self.recycle_notify.notify_one();
    }
}

/// Downloaded remote log file with prefetch permit
/// File remains on disk for memory efficiency; file deletion is handled by FileCleanupGuard in FileSource
#[derive(Debug)]
pub struct RemoteLogFile {
    /// Path to the downloaded file on local disk
    pub file_path: PathBuf,
    /// Size of the file in bytes
    /// Currently unused but kept for potential future use (logging, metrics, etc.)
    #[allow(dead_code)]
    pub file_size: usize,
    /// RAII permit that releases prefetch semaphore slot and notifies coordinator when dropped
    pub permit: PrefetchPermit,
}

/// Represents a request to download a remote log segment with priority ordering
#[derive(Debug)]
pub struct RemoteLogDownloadRequest {
    segment: RemoteLogSegment,
    remote_log_tablet_dir: String,
    result_sender: oneshot::Sender<Result<RemoteLogFile>>,
    retry_count: u32,
    next_retry_at: Option<tokio::time::Instant>,
}

impl RemoteLogDownloadRequest {
    /// Get the segment (used by test fetcher implementations)
    #[cfg(test)]
    pub fn segment(&self) -> &RemoteLogSegment {
        &self.segment
    }
}

// Total ordering for priority queue (Rust requirement: cmp==Equal implies Eq)
// Primary: Java semantics (timestamp cross-bucket, offset within-bucket)
// Tie-breakers: table_bucket fields (table_id, partition_id, bucket_id), then segment_id
impl Ord for RemoteLogDownloadRequest {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.segment.table_bucket == other.segment.table_bucket {
            // Same bucket: order by start_offset (ascending - earlier segments first)
            self.segment
                .start_offset
                .cmp(&other.segment.start_offset)
                .then_with(|| self.segment.segment_id.cmp(&other.segment.segment_id))
        } else {
            // Different buckets: order by max_timestamp (ascending - older segments first)
            // Then by table_bucket fields for true total ordering
            self.segment
                .max_timestamp
                .cmp(&other.segment.max_timestamp)
                .then_with(|| {
                    self.segment
                        .table_bucket
                        .table_id()
                        .cmp(&other.segment.table_bucket.table_id())
                })
                .then_with(|| {
                    self.segment
                        .table_bucket
                        .partition_id()
                        .cmp(&other.segment.table_bucket.partition_id())
                })
                .then_with(|| {
                    self.segment
                        .table_bucket
                        .bucket_id()
                        .cmp(&other.segment.table_bucket.bucket_id())
                })
                .then_with(|| self.segment.segment_id.cmp(&other.segment.segment_id))
        }
    }
}

impl PartialOrd for RemoteLogDownloadRequest {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RemoteLogDownloadRequest {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RemoteLogDownloadRequest {}

/// Result of a download task
enum DownloadResult {
    /// Successful download - deliver result to future
    Success {
        result: RemoteLogFile,
        result_sender: oneshot::Sender<Result<RemoteLogFile>>,
    },
    /// Download failed - re-queue request for retry (Java pattern)
    FailedRetry { request: RemoteLogDownloadRequest },
    /// Download failed permanently after max retries - fail the future
    FailedPermanently {
        error: Error,
        result_sender: oneshot::Sender<Result<RemoteLogFile>>,
    },
    /// Cancelled - don't deliver, don't re-queue
    Cancelled,
}

/// Production implementation of RemoteLogFetcher that downloads from actual storage
struct ProductionFetcher {
    credentials_rx: CredentialsReceiver,
    local_log_dir: Arc<TempDir>,
    remote_log_read_concurrency: usize,
}

impl RemoteLogFetcher for ProductionFetcher {
    fn fetch(
        &self,
        request: &RemoteLogDownloadRequest,
    ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send>> {
        let mut credentials_rx = self.credentials_rx.clone();
        let local_log_dir = self.local_log_dir.clone();
        let remote_log_read_concurrency = self.remote_log_read_concurrency;

        // Clone data needed for async operation to avoid lifetime issues
        let segment = request.segment.clone();
        let remote_log_tablet_dir = request.remote_log_tablet_dir.to_string();

        Box::pin(async move {
            let local_file_name = segment.local_file_name();
            let local_file_path = local_log_dir.path().join(&local_file_name);

            // Build remote path
            let offset_prefix = format!("{:020}", segment.start_offset);
            let remote_path = format!(
                "{}/{}/{}.log",
                remote_log_tablet_dir, segment.segment_id, offset_prefix
            );

            // Get credentials from watch channel, waiting if not yet fetched
            // - None = not yet fetched, wait
            // - Some(props) = fetched (may be empty if no auth needed)
            let remote_fs_props = {
                let maybe_props = credentials_rx.borrow().clone();
                match maybe_props {
                    Some(props) => props,
                    None => {
                        // Credentials not yet fetched, wait for first update
                        log::info!("Waiting for credentials to be available...");
                        // If the sender side has been dropped (e.g. during shutdown),
                        // this will return an error. Surface that as a proper error
                        // instead of silently falling back to empty credentials.
                        if let Err(e) = credentials_rx.changed().await {
                            let io_err = io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                format!(
                                    "credentials manager shut down before credentials were obtained: {e}"
                                ),
                            );
                            return Err(io_err.into());
                        }
                        // After a successful change notification, credentials should be set.
                        // If they are still missing, treat this as an error instead of
                        // defaulting to an empty map (which could break auth flows).
                        credentials_rx
                            .borrow()
                            .clone()
                            .ok_or_else(|| Error::UnexpectedError {
                                message: "credentials not available after watch notification"
                                    .to_string(),
                                source: None,
                            })?
                    }
                }
            };

            // Download file to disk (streaming, no memory spike)
            let file_path = RemoteLogDownloader::download_file(
                &remote_log_tablet_dir,
                &remote_path,
                &local_file_path,
                &remote_fs_props,
                remote_log_read_concurrency,
            )
            .await?;

            // Get file size
            let metadata = tokio::fs::metadata(&file_path).await?;
            let file_size = metadata.len() as usize;

            // Return file path - file stays on disk until PrefetchPermit is dropped
            Ok(FetchResult {
                file_path,
                file_size,
            })
        })
    }
}

/// Coordinator that owns all download state and orchestrates downloads
struct DownloadCoordinator {
    download_queue: BinaryHeap<Reverse<RemoteLogDownloadRequest>>,
    active_downloads: JoinSet<DownloadResult>,
    in_flight: usize,
    prefetch_semaphore: Arc<Semaphore>,
    max_concurrent_downloads: usize,
    recycle_notify: Arc<Notify>,
    fetcher: Arc<dyn RemoteLogFetcher>,
    /// Per-table scanner metric handles cloned by every spawned download
    /// task to attribute remote-fetch metrics to the owning scanner's
    /// `(database, table)`.
    metrics: Arc<ScannerMetrics>,
}

impl DownloadCoordinator {
    /// Check if we should wait for recycle notification
    /// Only wait if we're blocked on permits AND have pending work
    fn should_wait_for_recycle(&self) -> bool {
        !self.download_queue.is_empty()
            && self.in_flight < self.max_concurrent_downloads
            && self.prefetch_semaphore.available_permits() == 0
    }

    /// Find the earliest retry deadline among pending requests
    fn next_retry_deadline(&self) -> Option<tokio::time::Instant> {
        self.download_queue
            .iter()
            .filter_map(|Reverse(req)| req.next_retry_at)
            .min()
    }
}

impl DownloadCoordinator {
    /// Try to start as many downloads as possible (event-driven drain)
    fn drain(&mut self) {
        // Collect deferred requests (backoff not ready) to push back later
        let mut deferred = Vec::new();
        // Scan entire queue once to find ready requests (prevents head-of-line blocking)
        // Bound to reasonable max to avoid excessive work if queue is huge
        let max_scan = self.download_queue.len().min(100);
        let mut scanned = 0;

        while !self.download_queue.is_empty()
            && self.in_flight < self.max_concurrent_downloads
            && scanned < max_scan
        {
            // Try acquire prefetch permit (non-blocking)
            let permit = match self.prefetch_semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => break, // No permits available
            };

            // Pop highest priority request
            let Some(Reverse(request)) = self.download_queue.pop() else {
                drop(permit);
                break;
            };

            scanned += 1;

            // Retry backoff check: defer if retry time hasn't arrived yet
            if let Some(next_retry_at) = request.next_retry_at {
                let now = tokio::time::Instant::now();
                if next_retry_at > now {
                    // Not ready for retry yet - defer and continue looking for ready requests
                    drop(permit);
                    deferred.push(request);
                    continue; // Don't block - keep looking for ready requests
                }
            }

            // Cancellation check: skip if sender closed
            if request.result_sender.is_closed() {
                drop(permit);
                continue; // Try next request
            }

            // Clone data for the spawned task
            let fetcher = self.fetcher.clone();
            let recycle_notify = self.recycle_notify.clone();
            let metrics = Arc::clone(&self.metrics);

            // Spawn download task
            self.active_downloads.spawn(async move {
                spawn_download_task(request, permit, fetcher, recycle_notify, metrics).await
            });
            self.in_flight += 1;
        }

        // Push deferred requests back to queue (maintains priority order)
        if !deferred.is_empty() {
            for req in deferred {
                self.download_queue.push(Reverse(req));
            }
        }
    }
}

/// Spawn a download task that attempts download once
/// Matches Java's RemoteLogDownloader.java
///
/// Benefits over infinite in-place retry:
/// - Failed downloads don't block prefetch slots
/// - Other segments can make progress while one is failing
/// - Natural retry through coordinator re-picking from queue
async fn spawn_download_task(
    request: RemoteLogDownloadRequest,
    permit: tokio::sync::OwnedSemaphorePermit,
    fetcher: Arc<dyn RemoteLogFetcher>,
    recycle_notify: Arc<Notify>,
    metrics: Arc<ScannerMetrics>,
) -> DownloadResult {
    // Check if receiver still alive (early cancellation check)
    if request.result_sender.is_closed() {
        drop(permit);
        return DownloadResult::Cancelled;
    }

    // Java reference: RemoteLogDownloader.java increments `remoteFetchRequestCount`
    // immediately before initiating the download. Each retry of the same segment
    // counts as a separate request (matches Java behavior).
    metrics.record_remote_fetch_request();

    // Try download ONCE
    let download_result = fetcher.fetch(&request).await;

    match download_result {
        Ok(fetch_result) => {
            // Success - permit will be released on drop (FileSource handles file deletion)
            metrics.record_remote_fetch_bytes(fetch_result.file_size as u64);
            DownloadResult::Success {
                result: RemoteLogFile {
                    file_path: fetch_result.file_path,
                    file_size: fetch_result.file_size,
                    permit: PrefetchPermit::new(permit, recycle_notify.clone()),
                },
                result_sender: request.result_sender,
            }
        }
        Err(_e) if request.result_sender.is_closed() => {
            // Receiver dropped (cancelled) - release permit, don't re-queue
            drop(permit);
            DownloadResult::Cancelled
        }
        Err(e) => {
            // Download failed - check if we should retry or give up
            // Counted per attempt, so retries each contribute one error.
            metrics.record_remote_fetch_error();
            let retry_count = request.retry_count + 1;

            if retry_count > MAX_RETRY_COUNT {
                // Too many retries - give up and fail the future
                log::error!(
                    "Failed to download remote log segment {} after {} retries: {}. Giving up.",
                    request.segment.segment_id,
                    retry_count,
                    e
                );
                drop(permit); // Release immediately

                DownloadResult::FailedPermanently {
                    error: Error::UnexpectedError {
                        message: format!(
                            "Failed to download remote log segment after {retry_count} retries: {e}"
                        ),
                        source: Some(Box::new(e)),
                    },
                    result_sender: request.result_sender,
                }
            } else {
                // Retry with exponential backoff
                let backoff_delay = calculate_backoff_delay(retry_count);
                let next_retry_at = tokio::time::Instant::now() + backoff_delay;

                log::warn!(
                    "Failed to download remote log segment {}: {}. Retry {}/{} after {:?}",
                    request.segment.segment_id,
                    e,
                    retry_count,
                    MAX_RETRY_COUNT,
                    backoff_delay
                );
                drop(permit); // Release immediately - critical!

                // Update retry state
                let mut retry_request = request;
                retry_request.retry_count = retry_count;
                retry_request.next_retry_at = Some(next_retry_at);

                // Re-queue request to same priority queue
                // Future stays with request, NOT completed - will complete on successful retry
                DownloadResult::FailedRetry {
                    request: retry_request,
                }
            }
        }
    }
}

/// Coordinator event loop - owns all download state and reacts to events
async fn coordinator_loop(
    mut coordinator: DownloadCoordinator,
    mut request_receiver: mpsc::UnboundedReceiver<RemoteLogDownloadRequest>,
) {
    loop {
        // Drain once at start of iteration to process ready work
        coordinator.drain();

        // Calculate sleep duration until next retry (if any deferred requests)
        let next_retry_sleep = coordinator.next_retry_deadline().map(|deadline| {
            let now = tokio::time::Instant::now();
            if deadline > now {
                deadline - now
            } else {
                tokio::time::Duration::from_millis(0) // Ready now
            }
        });

        tokio::select! {
            // Event 1: NewRequest
            Some(request) = request_receiver.recv() => {
                coordinator.download_queue.push(Reverse(request));
                // Immediately try to start this download
                continue;
            }

            // Event 2: DownloadFinished
            Some(result) = coordinator.active_downloads.join_next() => {
                coordinator.in_flight -= 1;

                match result {
                    Ok(DownloadResult::Success { result, result_sender }) => {
                        // Success - deliver result to future
                        if !result_sender.is_closed() {
                            let _ = result_sender.send(Ok(result));
                        }
                        // Permit held in RemoteLogFile until consumed
                    }
                    Ok(DownloadResult::FailedRetry { request }) => {
                        // Re-queue immediately (don't block coordinator with sleep)
                        // The retry time will be checked in drain() before processing
                        // (Java line 177: segmentsToFetch.add(request))
                        // Permit already released (Java line 174)
                        coordinator.download_queue.push(Reverse(request));
                    }
                    Ok(DownloadResult::FailedPermanently { error, result_sender }) => {
                        // Permanent failure - deliver error to future
                        if !result_sender.is_closed() {
                            let _ = result_sender.send(Err(error));
                        }
                        // Permit already released
                    }
                    Ok(DownloadResult::Cancelled) => {
                        // Cancelled - permit already released, nothing to do
                    }
                    Err(e) => {
                        log::error!("Download task panicked: {e:?}");
                        // Permit already released via RAII
                    }
                }
                // Immediately try to start another download
                continue;
            }

            // Event 3: Recycled (only wait when blocked on permits with pending work)
            _ = coordinator.recycle_notify.notified(),
                if coordinator.should_wait_for_recycle() => {
                // Wake up to try draining
                continue;
            }

            // Event 4: Retry timer - wake up when next retry is ready
            _ = tokio::time::sleep(next_retry_sleep.unwrap_or(tokio::time::Duration::from_secs(3600))),
                if next_retry_sleep.is_some() => {
                // Wake up to retry deferred requests
                continue;
            }

            else => break,  // All channels closed AND no work pending
        }
    }
}

type CompletionCallback = Box<dyn Fn() + Send + Sync>;

/// Future for a remote log download request
pub struct RemoteLogDownloadFuture {
    result: Arc<Mutex<Option<Result<RemoteLogFile>>>>,
    completion_callbacks: Arc<Mutex<Vec<CompletionCallback>>>,
}

impl RemoteLogDownloadFuture {
    pub fn new(receiver: oneshot::Receiver<Result<RemoteLogFile>>) -> Self {
        let result = Arc::new(Mutex::new(None));
        let result_clone = Arc::clone(&result);
        let completion_callbacks: Arc<Mutex<Vec<CompletionCallback>>> =
            Arc::new(Mutex::new(Vec::new()));
        let callbacks_clone = Arc::clone(&completion_callbacks);

        // Spawn a task to wait for the download and update result, then call callbacks
        tokio::spawn(async move {
            let download_result = match receiver.await {
                Ok(Ok(path)) => Ok(path),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(Error::UnexpectedError {
                    message: format!("Download & Read future cancelled: {e:?}"),
                    source: None,
                }),
            };

            *result_clone.lock() = Some(download_result);

            // Call all registered callbacks
            // We need to take the callbacks to avoid holding the lock while calling them
            // This also ensures that any callbacks registered after this point will be called immediately
            let callbacks: Vec<CompletionCallback> = {
                let mut callbacks_guard = callbacks_clone.lock();
                mem::take(&mut *callbacks_guard)
            };
            for callback in callbacks {
                callback();
            }

            // After calling callbacks, any new callbacks registered will see is_done() == true
            // and will be called immediately in on_complete()
        });

        Self {
            result,
            completion_callbacks,
        }
    }

    /// Register a callback to be called when download completes (similar to Java's onComplete)
    pub fn on_complete<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        // Acquire callbacks lock first to ensure atomicity of the check-and-register operation
        let mut callbacks_guard = self.completion_callbacks.lock();

        // Check completion status while holding the callbacks lock.
        // This ensures that:
        // 1. If the task completes between checking is_done() and registering the callback,
        //    we'll see the completion state correctly
        // 2. The background task cannot clear the callbacks list while we're checking/registering
        let is_done = self.result.lock().is_some();

        if is_done {
            // If already completed, call immediately (drop lock first to avoid deadlock)
            drop(callbacks_guard);
            callback();
        } else {
            // Register the callback while holding the callbacks lock.
            // This ensures that even if the background task completes right after we check
            // is_done(), it will wait for us to release the lock before taking callbacks.
            // When it does take callbacks, it will see our callback in the list and execute it.
            callbacks_guard.push(Box::new(callback));
            // Lock is automatically released here
        }
    }

    pub fn is_done(&self) -> bool {
        self.result.lock().is_some()
    }

    /// Take the RemoteLogFile (including the permit) from this future
    /// This should only be called when the download is complete
    /// This is the correct way to consume the download - it transfers permit ownership
    pub fn take_remote_log_file(&self) -> Result<RemoteLogFile> {
        let mut guard = self.result.lock();
        match guard.take() {
            Some(Ok(remote_log_file)) => Ok(remote_log_file),
            Some(Err(e)) => {
                let error_msg = format!("{e}");
                Err(Error::IoUnexpectedError {
                    message: format!("Fail to get remote log file: {error_msg}"),
                    source: io::Error::other(error_msg),
                })
            }
            None => Err(Error::IoUnexpectedError {
                message: "Remote log file already taken or not ready".to_string(),
                source: io::Error::other("Remote log file already taken or not ready"),
            }),
        }
    }
}

/// Downloader for remote log segment files.
///
/// # Shutdown behavior
///
/// When the downloader is dropped, the request channel closes, signaling the coordinator
/// to stop accepting new work. The coordinator will finish any in-flight downloads but
/// won't wait for completion. Pending futures will fail.
pub struct RemoteLogDownloader {
    request_sender: Option<mpsc::UnboundedSender<RemoteLogDownloadRequest>>,
}

impl RemoteLogDownloader {
    pub(crate) fn new(
        local_log_dir: TempDir,
        max_prefetch_segments: usize,
        max_concurrent_downloads: usize,
        remote_log_read_concurrency: usize,
        credentials_rx: CredentialsReceiver,
        metrics: Arc<ScannerMetrics>,
    ) -> Result<Self> {
        let fetcher = Arc::new(ProductionFetcher {
            credentials_rx,
            local_log_dir: Arc::new(local_log_dir),
            remote_log_read_concurrency,
        });

        Self::new_with_fetcher(
            fetcher,
            max_prefetch_segments,
            max_concurrent_downloads,
            metrics,
        )
    }

    /// Create a RemoteLogDownloader with a custom fetcher (for testing).
    pub(crate) fn new_with_fetcher(
        fetcher: Arc<dyn RemoteLogFetcher>,
        max_prefetch_segments: usize,
        max_concurrent_downloads: usize,
        metrics: Arc<ScannerMetrics>,
    ) -> Result<Self> {
        let (request_sender, request_receiver) = mpsc::unbounded_channel();

        let coordinator = DownloadCoordinator {
            download_queue: BinaryHeap::new(),
            active_downloads: JoinSet::new(),
            in_flight: 0,
            prefetch_semaphore: Arc::new(Semaphore::new(max_prefetch_segments)),
            max_concurrent_downloads,
            recycle_notify: Arc::new(Notify::new()),
            fetcher,
            metrics,
        };

        // Spawn coordinator task - it will exit when request_sender is dropped
        tokio::spawn(coordinator_loop(coordinator, request_receiver));

        Ok(Self {
            request_sender: Some(request_sender),
        })
    }

    /// Request to fetch a remote log segment to local. This method is non-blocking.
    pub fn request_remote_log(
        &self,
        remote_log_tablet_dir: &str,
        segment: &RemoteLogSegment,
    ) -> RemoteLogDownloadFuture {
        let (result_sender, result_receiver) = oneshot::channel();

        let request = RemoteLogDownloadRequest {
            segment: segment.clone(),
            remote_log_tablet_dir: remote_log_tablet_dir.to_string(),
            result_sender,
            retry_count: 0,
            next_retry_at: None,
        };

        // Send to coordinator (non-blocking)
        if let Some(ref sender) = self.request_sender {
            if sender.send(request).is_err() {
                // Coordinator is gone - immediately fail the future
                let (error_sender, error_receiver) = oneshot::channel();
                let _ = error_sender.send(Err(Error::UnexpectedError {
                    message: "RemoteLogDownloader coordinator has shut down".to_string(),
                    source: None,
                }));
                return RemoteLogDownloadFuture::new(error_receiver);
            }
        }

        RemoteLogDownloadFuture::new(result_receiver)
    }
}

impl Drop for RemoteLogDownloader {
    fn drop(&mut self) {
        // Drop the request sender to signal coordinator shutdown.
        // This causes request_receiver.recv() to return None, allowing the
        // coordinator to exit gracefully after processing pending work.
        // The coordinator task will finish on its own when it sees the channel closed.
        drop(self.request_sender.take());
    }
}

impl RemoteLogDownloader {
    /// Download a file from remote storage to local using streaming read/write.
    async fn download_file(
        remote_log_tablet_dir: &str,
        remote_path: &str,
        local_path: &Path,
        remote_fs_props: &HashMap<String, String>,
        remote_log_read_concurrency: usize,
    ) -> Result<PathBuf> {
        // Handle both URL (e.g., "s3://bucket/path") and local file paths
        // If the path doesn't contain "://", treat it as a local file path
        let remote_log_tablet_dir_url = if remote_log_tablet_dir.contains("://") {
            remote_log_tablet_dir.to_string()
        } else {
            format!("file://{remote_log_tablet_dir}")
        };

        // Create FileIO from the remote log tablet dir URL to get the storage
        let file_io_builder = FileIO::from_url(&remote_log_tablet_dir_url)?;

        // For S3/S3A URLs, inject S3 credentials from props
        let file_io_builder = if remote_log_tablet_dir.starts_with("s3://")
            || remote_log_tablet_dir.starts_with("s3a://")
            || remote_log_tablet_dir.starts_with("oss://")
        {
            file_io_builder.with_props(
                remote_fs_props
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.as_str())),
            )
        } else {
            file_io_builder
        };

        // Build storage and create operator directly
        let storage = Storage::build(file_io_builder)?;
        let (op, relative_path) = storage.create(remote_path)?;

        // Timeout for remote storage operations (30 seconds)
        const REMOTE_OP_TIMEOUT: Duration = Duration::from_secs(30);
        const CHUNK_SIZE: usize = 8 * 1024 * 1024; // 8MiB

        Self::download_file_streaming(
            &op,
            relative_path,
            remote_path,
            local_path,
            CHUNK_SIZE,
            remote_log_read_concurrency,
            REMOTE_OP_TIMEOUT,
        )
        .await?;

        Ok(local_path.to_path_buf())
    }

    async fn download_file_streaming(
        op: &opendal::Operator,
        relative_path: &str,
        remote_path: &str,
        local_path: &Path,
        chunk_size: usize,
        streaming_read_concurrency: usize,
        remote_op_timeout: Duration,
    ) -> Result<()> {
        let mut local_file = tokio::fs::File::create(local_path).await?;

        let reader_future = op
            .reader_with(relative_path)
            .chunk(chunk_size)
            .concurrent(streaming_read_concurrency);
        let reader = tokio::time::timeout(remote_op_timeout, reader_future)
            .await
            .map_err(|e| Error::IoUnexpectedError {
                message: format!("Timeout creating streaming reader for {remote_path}: {e}."),
                source: io::ErrorKind::TimedOut.into(),
            })??;

        let mut stream = tokio::time::timeout(remote_op_timeout, reader.into_bytes_stream(..))
            .await
            .map_err(|e| Error::IoUnexpectedError {
                message: format!("Timeout creating streaming bytes stream for {remote_path}: {e}."),
                source: io::ErrorKind::TimedOut.into(),
            })??;

        let mut chunk_count = 0u64;
        while let Some(chunk) = tokio::time::timeout(remote_op_timeout, stream.try_next())
            .await
            .map_err(|e| Error::IoUnexpectedError {
                message: format!(
                    "Timeout streaming chunk from remote storage: {remote_path}, exception: {e}."
                ),
                source: io::ErrorKind::TimedOut.into(),
            })??
        {
            chunk_count += 1;
            if chunk_count <= 3 || chunk_count % 10 == 0 {
                log::debug!("Remote log streaming download: chunk #{chunk_count} ({remote_path})");
            }
            local_file.write_all(&chunk).await?;
        }

        local_file.sync_all().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::TablePath;
    use crate::test_utils::test_scanner_metrics;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Helper function to create a TableBucket for testing
    fn create_table_bucket(table_id: i64, bucket_id: i32) -> TableBucket {
        TableBucket::new(table_id, bucket_id)
    }

    /// `ScannerMetrics` instance shared across the local test fixtures. The
    /// labels are arbitrary because none of the tests in this module install
    /// a metrics recorder; the metrics just need to exist for the API
    /// surface.
    fn metrics() -> Arc<ScannerMetrics> {
        test_scanner_metrics(&TablePath::new("db", "tbl"))
    }

    /// Simplified fake fetcher for testing
    struct FakeFetcher {
        completion_gate: Arc<Notify>,
        in_flight: Arc<AtomicUsize>,
        max_seen_in_flight: Arc<AtomicUsize>,
        fail_count: Arc<Mutex<usize>>,
        auto_complete: bool,
    }

    impl FakeFetcher {
        fn new(fail_count: usize, auto_complete: bool) -> Self {
            Self {
                completion_gate: Arc::new(Notify::new()),
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_seen_in_flight: Arc::new(AtomicUsize::new(0)),
                fail_count: Arc::new(Mutex::new(fail_count)),
                auto_complete,
            }
        }

        fn max_seen_in_flight(&self) -> usize {
            self.max_seen_in_flight.load(Ordering::SeqCst)
        }

        fn in_flight(&self) -> usize {
            self.in_flight.load(Ordering::SeqCst)
        }

        fn release_one(&self) {
            self.completion_gate.notify_one();
        }

        fn release_all(&self) {
            self.completion_gate.notify_waiters();
        }
    }

    impl RemoteLogFetcher for FakeFetcher {
        fn fetch(
            &self,
            request: &RemoteLogDownloadRequest,
        ) -> Pin<Box<dyn Future<Output = Result<FetchResult>> + Send>> {
            let gate = self.completion_gate.clone();
            let in_flight = self.in_flight.clone();
            let max_seen = self.max_seen_in_flight.clone();
            let fail_count = self.fail_count.clone();
            let segment_id = request.segment().segment_id.clone();
            let auto_complete = self.auto_complete;

            Box::pin(async move {
                // Track in-flight
                let current = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_seen.fetch_max(current, Ordering::SeqCst);

                // Wait for gate (or auto-complete)
                if !auto_complete {
                    gate.notified().await;
                } else {
                    tokio::task::yield_now().await;
                }

                // Check if should fail
                let should_fail = {
                    let mut count = fail_count.lock();
                    if *count > 0 {
                        *count -= 1;
                        true
                    } else {
                        false
                    }
                };

                in_flight.fetch_sub(1, Ordering::SeqCst);

                if should_fail {
                    Err(Error::UnexpectedError {
                        message: format!("Fake fetch failed for {segment_id}"),
                        source: None,
                    })
                } else {
                    let fake_data = vec![1, 2, 3, 4];
                    let temp_dir = env::temp_dir();
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos();
                    let file_path =
                        temp_dir.join(format!("fake_segment_{segment_id}_{timestamp}.log"));
                    tokio::fs::write(&file_path, &fake_data).await?;

                    Ok(FetchResult {
                        file_path,
                        file_size: fake_data.len(),
                    })
                }
            })
        }
    }

    /// Helper function to create a RemoteLogSegment for testing
    fn create_segment(
        segment_id: &str,
        start_offset: i64,
        max_timestamp: i64,
        table_bucket: TableBucket,
    ) -> RemoteLogSegment {
        RemoteLogSegment {
            segment_id: segment_id.to_string(),
            start_offset,
            end_offset: start_offset + 1000,
            size_in_bytes: 1024,
            table_bucket,
            max_timestamp,
        }
    }

    /// Helper function to create a RemoteLogDownloadRequest for testing
    fn create_request(segment: RemoteLogSegment) -> RemoteLogDownloadRequest {
        let (result_sender, _) = oneshot::channel();
        RemoteLogDownloadRequest {
            remote_log_tablet_dir: "test_dir".to_string(),
            segment,
            result_sender,
            retry_count: 0,
            next_retry_at: None,
        }
    }

    #[test]
    fn test_priority_ordering_matching_java_test_case() {
        // Test priority ordering: timestamp across buckets, offset within bucket
        // Does NOT test tie-breakers (segment_id) - those are implementation details

        let bucket1 = create_table_bucket(1, 0);
        let bucket2 = create_table_bucket(1, 1);
        let bucket3 = create_table_bucket(1, 2);
        let bucket4 = create_table_bucket(1, 3);

        // Create segments with distinct timestamps/offsets (no ties)
        let seg_negative = create_segment("seg_neg", 0, -1, bucket1.clone());
        let seg_zero = create_segment("seg_zero", 0, 0, bucket2.clone());
        let seg_1000 = create_segment("seg_1000", 0, 1000, bucket3.clone());
        let seg_2000 = create_segment("seg_2000", 0, 2000, bucket4.clone());
        let seg_same_bucket_100 = create_segment("seg_sb_100", 100, 5000, bucket1.clone());
        let seg_same_bucket_50 = create_segment("seg_sb_50", 50, 5000, bucket1.clone());

        let mut heap = BinaryHeap::new();
        heap.push(Reverse(create_request(seg_2000)));
        heap.push(Reverse(create_request(seg_same_bucket_100)));
        heap.push(Reverse(create_request(seg_1000)));
        heap.push(Reverse(create_request(seg_zero)));
        heap.push(Reverse(create_request(seg_negative)));
        heap.push(Reverse(create_request(seg_same_bucket_50)));

        // Verify ordering by timestamp/offset, not segment_id
        let first = heap.pop().unwrap().0;
        assert_eq!(first.segment.max_timestamp, -1, "Lowest timestamp first");

        let second = heap.pop().unwrap().0;
        assert_eq!(second.segment.max_timestamp, 0);

        let third = heap.pop().unwrap().0;
        assert_eq!(third.segment.max_timestamp, 1000);

        let fourth = heap.pop().unwrap().0;
        assert_eq!(fourth.segment.max_timestamp, 2000);

        // Last two are same bucket (ts=5000), ordered by offset
        let fifth = heap.pop().unwrap().0;
        assert_eq!(fifth.segment.max_timestamp, 5000);
        assert_eq!(
            fifth.segment.start_offset, 50,
            "Lower offset first within bucket"
        );

        let sixth = heap.pop().unwrap().0;
        assert_eq!(sixth.segment.max_timestamp, 5000);
        assert_eq!(sixth.segment.start_offset, 100);
    }

    #[tokio::test]
    async fn test_concurrency_and_priority() {
        // Test concurrency limiting and priority-based scheduling together
        let fake_fetcher = Arc::new(FakeFetcher::new(0, false)); // Manual control

        let downloader = RemoteLogDownloader::new_with_fetcher(
            fake_fetcher.clone(),
            10, // High prefetch limit
            2,  // Max concurrent downloads = 2
            metrics(),
        )
        .unwrap();

        let bucket = create_table_bucket(1, 0);

        // Request 4 segments with same priority (to isolate concurrency limiting from priority)
        let segs: Vec<_> = (0..4)
            .map(|i| create_segment(&format!("seg{i}"), i * 100, 1000, bucket.clone()))
            .collect();

        let _futures: Vec<_> = segs
            .iter()
            .map(|seg| downloader.request_remote_log("dir", seg))
            .collect();

        // Wait for exactly 2 to start
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            fake_fetcher.in_flight(),
            2,
            "Concurrency limit: exactly 2 should be in-flight"
        );

        // Release one
        fake_fetcher.release_one();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Max should never exceed 2
        assert_eq!(
            fake_fetcher.max_seen_in_flight(),
            2,
            "Max concurrent should not exceed 2"
        );

        // Release all
        fake_fetcher.release_all();
    }

    #[tokio::test]
    async fn test_prefetch_limit() {
        // Test that prefetch semaphore limits outstanding downloads
        let fake_fetcher = Arc::new(FakeFetcher::new(0, true)); // Auto-complete

        let downloader = RemoteLogDownloader::new_with_fetcher(
            fake_fetcher,
            2,  // Max prefetch = 2
            10, // High concurrent limit
            metrics(),
        )
        .unwrap();

        let bucket = create_table_bucket(1, 0);

        // Request 4 downloads
        let segs: Vec<_> = (0..4)
            .map(|i| create_segment(&format!("seg{i}"), i * 100, 1000, bucket.clone()))
            .collect();

        let mut futures: Vec<_> = segs
            .iter()
            .map(|seg| downloader.request_remote_log("dir", seg))
            .collect();

        // Wait for first 2 to complete
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if futures.iter().filter(|f| f.is_done()).count() >= 2 {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("Timeout waiting for first 2 downloads");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Verify 3rd and 4th are blocked (prefetch limit)
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            futures.iter().filter(|f| f.is_done()).count(),
            2,
            "Prefetch limit: only 2 should complete"
        );

        // Drop first 2 (releases permits)
        let f4 = futures.pop().unwrap();
        let f3 = futures.pop().unwrap();
        drop(futures);

        // 3rd and 4th should now complete
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if f3.is_done() && f4.is_done() {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("Timeout after permit release");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn test_retry_and_cancellation() {
        // Test retry with exponential backoff
        let fake_fetcher = Arc::new(FakeFetcher::new(2, true)); // Fail twice, succeed third time

        let downloader =
            RemoteLogDownloader::new_with_fetcher(fake_fetcher.clone(), 10, 1, metrics()).unwrap();

        let bucket = create_table_bucket(1, 0);
        let seg = create_segment("seg1", 0, 1000, bucket);

        let future = downloader.request_remote_log("dir", &seg);

        // Should succeed after retries
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if future.is_done() {
                break;
            }
            if tokio::time::Instant::now() > deadline {
                panic!("Timeout waiting for retry to succeed");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        assert!(future.is_done(), "Should succeed after retries");

        // Test cancellation
        let seg2 = create_segment("seg2", 100, 1000, create_table_bucket(1, 0));
        let fake_fetcher2 = Arc::new(FakeFetcher::new(100, true)); // Fail forever
        let downloader2 =
            RemoteLogDownloader::new_with_fetcher(fake_fetcher2.clone(), 10, 1, metrics()).unwrap();

        let future2 = downloader2.request_remote_log("dir", &seg2);
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Drop to cancel
        drop(future2);
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(
            fake_fetcher2.in_flight(),
            0,
            "Cancellation should release resources"
        );
    }
}
