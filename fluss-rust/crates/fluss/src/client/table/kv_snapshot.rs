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

use super::FlussTable;
use super::batch_scanner::{build_kv_decoders, read_schema_id};
use super::remote_log::RemoteLogDownloader;
use crate::SnapshotId;
use crate::client::{ClientSchemaGetter, CredentialsReceiver, FlussAdmin, SecurityTokenManager};
use crate::error::{Error, Result};
use crate::metadata::{
    KvSnapshotLeaseForBucket, KvSnapshotLeaseForTable, KvSnapshotMetadata, RemotePathAndLocalFile,
    Schema, SchemaInfo, TableBucket, TableInfo,
};
use crate::record::RowAppendRecordBatchBuilder;
use crate::row::FixedSchemaDecoder;
use arrow::array::RecordBatch;
use futures::Stream;
use rocksdb::{DB, Options, ReadOptions};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tempfile::TempDir;
use tokio::sync::{Semaphore, oneshot, watch};
use tokio::task::JoinHandle;

const DEFAULT_SNAPSHOT_LEASE_DURATION: Duration = Duration::from_secs(300);

/// Ownership of the lease protecting a snapshot while its files are downloaded.
#[derive(Debug, Clone)]
pub enum KvSnapshotLeaseMode {
    /// The reader acquires, renews, and releases its own lease for this duration.
    Managed(Duration),
    /// The caller already holds a lease covering this exact table, partition, bucket and snapshot.
    ///
    /// The caller must renew it until reader creation finishes, including cancellation cleanup.
    /// The reader never renews or releases the caller's lease. After successful creation all
    /// snapshot files are local, so the lease is no longer required by this reader.
    CallerManaged,
}

impl Default for KvSnapshotLeaseMode {
    fn default() -> Self {
        Self::Managed(DEFAULT_SNAPSHOT_LEASE_DURATION)
    }
}

/// Options for reading a fixed snapshot into a caller-selected schema and column order.
#[derive(Debug, Clone, Default)]
pub struct KvSnapshotReaderOptions {
    /// Schema to align stored rows to. Defaults to the schema captured by the table handle.
    pub target_schema_id: Option<i32>,
    /// Column indices in the target schema, in output order. `None` returns all columns.
    pub projected_fields: Option<Vec<usize>>,
    /// Defaults to a reader-managed five-minute download lease.
    pub lease_mode: KvSnapshotLeaseMode,
}

fn snapshot_error(message: impl Into<String>) -> Error {
    Error::UnexpectedError {
        message: message.into(),
        source: None,
    }
}

/// Reads one persisted bucket snapshot, aligned to a fixed target schema.
///
/// Created with [`FlussTable::create_kv_snapshot_reader`]. The snapshot is fully downloaded
/// before this reader is returned. Source schemas are fetched and decoders cached on first use;
/// keep the connection alive while reading. No remote snapshot lease is needed after creation.
/// Batches contain current rows, without changelog offsets or change types. TTL tags are
/// stripped without applying a new wall-clock expiration policy to the persisted view.
///
/// EOF, errors, [`close`](Self::close), and drop release the database and local directory.
/// Cancelling `next_batch` spends the reader; its blocking read finishes and releases its
/// resources in the background. Restart from the same snapshot to retry a cancelled read.
pub struct KvSnapshotReader {
    bucket: TableBucket,
    snapshot_id: SnapshotId,
    log_offset: i64,
    state: Option<ReadState>,
}

// Declaration order closes RocksDB before removing the downloaded files.
struct LocalSnapshot {
    db: DB,
    _directory: TempDir,
}

struct ReadState {
    local: LocalSnapshot,
    table_info: TableInfo,
    target_schema: Arc<Schema>,
    target_schema_id: i16,
    schema_getter: Arc<ClientSchemaGetter>,
    admin: Arc<FlussAdmin>,
    projected_fields: Option<Vec<usize>>,
    decoders: HashMap<i16, FixedSchemaDecoder>,
    value_offset: usize,
    last_key: Option<Vec<u8>>,
}

struct SnapshotBatch {
    batch: Option<RecordBatch>,
    finished: bool,
    missing_schema: Option<i16>,
}

impl FlussTable<'_> {
    /// Downloads and opens exactly `snapshot_id` for `bucket`, never substituting a newer snapshot.
    ///
    /// Uses `scanner_io_tmp_dir` (or the OS temporary directory plus `fluss`) and the existing
    /// remote-storage credentials provider. A renewable lease protects the download and is
    /// released before returning. Missing snapshots/files and unsupported formats fail creation;
    /// missing source schemas fail when first read. Cancelling creation completes any in-flight
    /// file download before cleaning up;
    /// the connection/runtime must remain alive for asynchronous lease release. If the runtime
    /// is shut down, the server lease expires after five minutes without renewal.
    ///
    /// `reader.log_offset()` is the exclusive end of the snapshot's log prefix and the start of
    /// a subsequent `[S, E)` tail. Capture `E` only after selecting this snapshot and `S`.
    pub async fn create_kv_snapshot_reader(
        &self,
        bucket: TableBucket,
        snapshot_id: SnapshotId,
    ) -> Result<KvSnapshotReader> {
        self.create_kv_snapshot_reader_with_lease_duration(
            bucket,
            snapshot_id,
            DEFAULT_SNAPSHOT_LEASE_DURATION,
        )
        .await
    }

    /// Creates a reader with a caller-selected download lease duration.
    ///
    /// The duration is rounded down to milliseconds and must be at least one millisecond.
    /// Successful renewals are followed by a wait of one third of this duration. Transient
    /// failures are retried with backoff within the last confirmed lease lifetime; permanent
    /// errors or unavailable snapshots fail creation. Choose a duration that tolerates expected
    /// RPC delays and process pauses; this is not a download timeout.
    /// The reader manages renewal and release. If the runtime stops, the lease expires after
    /// the selected duration without renewal. Other behavior matches
    /// [`create_kv_snapshot_reader`](Self::create_kv_snapshot_reader).
    pub async fn create_kv_snapshot_reader_with_lease_duration(
        &self,
        bucket: TableBucket,
        snapshot_id: SnapshotId,
        lease_duration: Duration,
    ) -> Result<KvSnapshotReader> {
        self.create_kv_snapshot_reader_with_options(
            bucket,
            snapshot_id,
            KvSnapshotReaderOptions {
                lease_mode: KvSnapshotLeaseMode::Managed(lease_duration),
                ..Default::default()
            },
        )
        .await
    }

    /// Opens a snapshot with an explicit target schema, projection and lease ownership.
    ///
    /// Projection indices refer to the target schema, after historical rows have been aligned.
    /// Invalid schema IDs or column indices fail creation. With a caller-managed lease, planning
    /// must acquire the lease before handing the snapshot to execution; a missing snapshot fails
    /// instead of being replaced by a newer one. Other lifecycle behavior matches
    /// [`create_kv_snapshot_reader`](Self::create_kv_snapshot_reader).
    pub async fn create_kv_snapshot_reader_with_options(
        &self,
        bucket: TableBucket,
        snapshot_id: SnapshotId,
        options: KvSnapshotReaderOptions,
    ) -> Result<KvSnapshotReader> {
        let lease_duration_ms = match options.lease_mode {
            KvSnapshotLeaseMode::Managed(duration) => Some(validate_lease_duration(duration)?),
            KvSnapshotLeaseMode::CallerManaged => None,
        };
        let target_id = options
            .target_schema_id
            .unwrap_or(self.table_info.get_schema_id());
        let target_id = i16::try_from(target_id)
            .ok()
            .filter(|id| *id >= 0)
            .ok_or_else(|| Error::IllegalArgument {
                message: "Target schema ID must be nonnegative and fit a persisted schema ID"
                    .into(),
            })?;
        if !self.has_primary_key
            || bucket.table_id() != self.table_info.table_id
            || bucket.bucket_id() < 0
            || snapshot_id < 0
            || bucket.partition_id().is_some() != self.table_info.is_partitioned()
        {
            return Err(Error::IllegalArgument {
                message: "A KV snapshot requires a primary-key table, matching table/partition identity, and nonnegative bucket/snapshot IDs".into(),
            });
        }
        let value_offset = value_body_offset(&self.table_info)?;
        if self.table_info.table_config.get_kv_format()? != crate::metadata::KvFormat::COMPACTED {
            return Err(Error::UnsupportedOperation {
                message: "INDEXED format is not yet supported".into(),
            });
        }
        let admin = self.conn.get_admin()?;
        let schema_getter = Arc::new(ClientSchemaGetter::new(
            self.table_path.clone(),
            admin.clone(),
            SchemaInfo::new(
                self.table_info.get_schema().clone(),
                self.table_info.get_schema_id(),
            ),
        ));
        let table_info = self.table_info.clone();
        let config = self.conn.config().clone();
        let download_slots = self.conn.snapshot_download_slots();
        let tokens = SecurityTokenManager::new(self.conn.get_connections(), self.metadata.clone());
        let (mut tx, rx) = oneshot::channel();
        // This task owns acquisition as well as cleanup: dropping the caller cannot race a
        // successful acquire response or remove a directory underneath an in-flight file write.
        tokio::spawn(async move {
            let result = async {
                let lease = match lease_duration_ms {
                    Some(duration) => Some(
                        DownloadLease::acquire(admin.clone(), &bucket, snapshot_id, duration).await?,
                    ),
                    None => None,
                };
                let result = async {
                    let target_schema = schema_getter.get_schema(i32::from(target_id)).await?;
                    if let Some(fields) = &options.projected_fields {
                        for &index in fields {
                            if index >= target_schema.row_type().fields().len() {
                                return Err(Error::IllegalArgument {
                                    message: format!("Projected field {index} is outside target schema {target_id}"),
                                });
                            }
                        }
                    }
                    let metadata = admin
                        .get_kv_snapshot_metadata(
                            bucket.table_id(),
                            bucket.partition_id(),
                            bucket.bucket_id(),
                            snapshot_id,
                        )
                        .await?;
                    validate_files(&metadata)?;
                    let parent = config
                        .scanner_io_tmp_dir
                        .as_ref()
                        .map(std::path::PathBuf::from)
                        .unwrap_or_else(|| std::env::temp_dir().join("fluss"));
                    std::fs::create_dir_all(&parent)?;
                    let directory = tempfile::Builder::new()
                        .prefix("kv-snapshot-")
                        .tempdir_in(parent)?;
                    let mut credentials = tokens.subscribe();
                    tokens.start();
                    for file in &metadata.snapshot_files {
                        if tx.is_closed() {
                            return Err(snapshot_error("Snapshot creation cancelled"));
                        }
                        download_snapshot_file(
                            file,
                            directory.path(),
                            &mut credentials,
                            &download_slots,
                            &mut tx,
                            config.scanner_remote_log_read_concurrency.max(1),
                        )
                        .await?;
                        if let Some(lease) = &lease {
                            lease.check()?;
                        }
                    }
                    if tx.is_closed() {
                        return Err(snapshot_error("Snapshot creation cancelled"));
                    }
                    let local =
                        tokio::task::spawn_blocking(move || open_local(directory))
                            .await
                            .map_err(|e| {
                                snapshot_error(format!("Snapshot open task failed: {e}"))
                            })??;
                    if admin.get_table_info(&table_info.table_path).await?.table_id
                        != bucket.table_id()
                    {
                        return Err(snapshot_error(
                            "Table was replaced while resolving snapshot schemas",
                        ));
                    }
                    if let Some(lease) = &lease {
                        lease.check()?;
                    }
                    Ok(KvSnapshotReader {
                        bucket,
                        snapshot_id,
                        log_offset: metadata.log_offset,
                        state: Some(ReadState {
                            local,
                            table_info,
                            target_schema,
                            target_schema_id: target_id,
                            schema_getter,
                            admin: admin.clone(),
                            projected_fields: options.projected_fields,
                            decoders: HashMap::new(),
                            value_offset,
                            last_key: None,
                        }),
                    })
                }
                .await;
                let released = match lease {
                    Some(lease) => lease.release().await,
                    None => Ok(()),
                };
                // Report the original read/download failure if cleanup also fails.
                match result {
                    Ok(reader) => {
                        released?;
                        Ok(reader)
                    }
                    Err(error) => {
                        if let Err(cleanup_error) = released {
                            log::warn!(
                                "Failed to release KV snapshot download lease: {cleanup_error}"
                            );
                        }
                        Err(error)
                    }
                }
            }
            .await;
            let _ = tx.send(result);
        });
        rx.await
            .map_err(|e| snapshot_error(format!("Snapshot initialization task failed: {e}")))?
    }
}

impl KvSnapshotReader {
    /// The fixed table/partition/bucket identity.
    pub fn bucket(&self) -> &TableBucket {
        &self.bucket
    }

    /// The requested persisted snapshot ID.
    pub fn snapshot_id(&self) -> SnapshotId {
        self.snapshot_id
    }

    /// Exclusive end of the snapshot's log prefix; start of a subsequent log tail.
    pub fn log_offset(&self) -> i64 {
        self.log_offset
    }

    /// Returns at most `max_rows` rows, or `None` at EOF/after close. A read error spends the reader.
    /// A cancelled call also spends the reader, so retry by opening the same fixed snapshot again.
    pub async fn next_batch(&mut self, max_rows: usize) -> Result<Option<RecordBatch>> {
        if max_rows == 0 {
            return Err(Error::IllegalArgument {
                message: "Snapshot batch size must be positive".into(),
            });
        }
        let Some(mut state) = self.state.take() else {
            return Ok(None);
        };
        loop {
            let (next_state, result) = tokio::task::spawn_blocking(move || {
                let result = state.read_batch(max_rows)?;
                Ok::<_, Error>((state, result))
            })
            .await
            .map_err(|e| snapshot_error(format!("Snapshot read task failed: {e}")))??;
            state = next_state;
            if let Some(id) = result.missing_schema {
                state.resolve_decoder(id).await?;
            }
            if result.finished || result.batch.is_some() {
                self.state = if result.finished { None } else { Some(state) };
                return Ok(result.batch);
            }
        }
    }

    /// Releases local resources. Idempotent; subsequent calls to `next_batch` return `None`.
    pub fn close(&mut self) {
        self.state = None;
    }

    /// Consumes the reader into bounded Arrow batches. Dropping the stream closes the reader.
    pub fn into_stream(self, max_rows: usize) -> impl Stream<Item = Result<RecordBatch>> + Send {
        futures::stream::try_unfold(self, move |mut reader| async move {
            Ok(reader
                .next_batch(max_rows)
                .await?
                .map(|batch| (batch, reader)))
        })
    }
}

impl ReadState {
    fn read_batch(&mut self, max_rows: usize) -> Result<SnapshotBatch> {
        let mut options = ReadOptions::default();
        options.set_total_order_seek(true);
        let mut iter = self.local.db.raw_iterator_opt(options);
        if let Some(last) = &self.last_key {
            iter.seek(last);
            if iter.valid() {
                iter.next();
            }
        } else {
            iter.seek_to_first();
        }
        let mut builder = RowAppendRecordBatchBuilder::new(self.target_schema.row_type())?;
        let mut count = 0;
        let mut missing_schema = None;
        while iter.valid() && count < max_rows {
            let payload = value_body(iter.value().unwrap(), self.value_offset)?;
            let id = read_schema_id(payload)?;
            let Some(decoder) = self.decoders.get(&id) else {
                missing_schema = Some(id);
                // Resume at this unconsumed row after resolving its schema asynchronously.
                // Only the cursor key is copied, not the values of the whole batch.
                if count > 0 {
                    iter.prev();
                    self.last_key = Some(
                        iter.key()
                            .ok_or_else(|| {
                                snapshot_error("Snapshot iterator could not retain its resume key")
                            })?
                            .to_vec(),
                    );
                }
                break;
            };
            builder.append(&decoder.decode(payload)?)?;
            count += 1;
            if count == max_rows {
                self.last_key = Some(iter.key().unwrap().to_vec());
            }
            iter.next();
        }
        iter.status()
            .map_err(|e| snapshot_error(format!("Snapshot iteration failed: {e}")))?;
        let finished = !iter.valid();
        let batch = if count == 0 {
            None
        } else {
            let batch = Arc::unwrap_or_clone(builder.build_arrow_record_batch()?);
            Some(match &self.projected_fields {
                Some(fields) => batch.project(fields)?,
                None => batch,
            })
        };
        Ok(SnapshotBatch {
            batch,
            finished,
            missing_schema,
        })
    }

    async fn resolve_decoder(&mut self, id: i16) -> Result<()> {
        let decoders = build_kv_decoders(
            &self.schema_getter,
            &self.target_schema,
            self.target_schema_id,
            self.table_info.table_config.get_kv_format()?,
            &[id],
        )
        .await?;
        // Schema lookup is by table path. Reject a replacement table's schema before decoding.
        if id != self.target_schema_id
            && self
                .admin
                .get_table_info(&self.table_info.table_path)
                .await?
                .table_id
                != self.table_info.table_id
        {
            return Err(snapshot_error(
                "Table was replaced while resolving snapshot schemas",
            ));
        }
        self.decoders.extend(decoders);
        Ok(())
    }
}

async fn download_snapshot_file(
    file: &RemotePathAndLocalFile,
    directory: &Path,
    credentials: &mut CredentialsReceiver,
    slots: &Semaphore,
    caller: &mut oneshot::Sender<Result<KvSnapshotReader>>,
    read_concurrency: usize,
) -> Result<()> {
    // Initial token acquisition must not hold a connection-wide download slot.
    tokio::select! {
        biased;
        _ = caller.closed() => return Err(snapshot_error("Snapshot creation cancelled")),
        result = credentials.wait_for(|value| value.is_some()) => {
            drop(result.map_err(|e| snapshot_error(format!("Snapshot credentials unavailable: {e}")))?);
        }
    }
    let _permit = tokio::select! {
        biased;
        _ = caller.closed() => return Err(snapshot_error("Snapshot creation cancelled")),
        permit = slots.acquire() => permit
            .map_err(|e| snapshot_error(format!("Snapshot download limiter closed: {e}")))?,
    };
    // Read credentials after waiting for a slot so queued downloads see refreshed tokens.
    let props = tokio::select! {
        biased;
        _ = caller.closed() => return Err(snapshot_error("Snapshot creation cancelled")),
        result = credentials.wait_for(|value| value.is_some()) => result
            .map_err(|e| snapshot_error(format!("Snapshot credentials unavailable: {e}")))?
            .clone().unwrap(),
    };
    RemoteLogDownloader::download_file(
        &file.remote_path,
        &file.remote_path,
        &directory.join(&file.local_file_name),
        &props,
        read_concurrency,
    )
    .await?;
    Ok(())
}

fn open_local(directory: TempDir) -> Result<LocalSnapshot> {
    let db = DB::open_for_read_only(&Options::default(), directory.path(), true)
        .map_err(|e| snapshot_error(format!("Cannot open persisted KV snapshot: {e}")))?;
    Ok(LocalSnapshot {
        db,
        _directory: directory,
    })
}

fn value_body_offset(info: &TableInfo) -> Result<usize> {
    match info
        .table_config
        .properties
        .get("table.kv.value-layout-version")
        .map(String::as_str)
        .unwrap_or("1")
    {
        "1" => Ok(0),
        "2" => Ok(8),
        value => Err(Error::UnsupportedOperation {
            message: format!("Unsupported persisted KV value layout {value}"),
        }),
    }
}

fn value_body(value: &[u8], offset: usize) -> Result<&[u8]> {
    value
        .get(offset..)
        .ok_or_else(|| snapshot_error("Persisted KV value is shorter than its layout prefix"))
}

fn validate_files(metadata: &KvSnapshotMetadata) -> Result<()> {
    let mut names = HashSet::new();
    for file in &metadata.snapshot_files {
        let mut components = Path::new(&file.local_file_name).components();
        if !matches!(components.next(), Some(Component::Normal(_)))
            || components.next().is_some()
            || !names.insert(file.local_file_name.as_str())
        {
            return Err(snapshot_error(format!(
                "Invalid or duplicate snapshot filename: {}",
                file.local_file_name
            )));
        }
    }
    if metadata.log_offset < 0
        || !names.contains("CURRENT")
        || !names.iter().any(|name| name.starts_with("MANIFEST-"))
    {
        return Err(snapshot_error(
            "Snapshot requires a nonnegative log offset, CURRENT and MANIFEST",
        ));
    }
    Ok(())
}

fn validate_lease_duration(duration: Duration) -> Result<i64> {
    let invalid = || Error::IllegalArgument {
        message: "Snapshot lease duration must be at least 1 ms and fit its expiration timestamp"
            .into(),
    };
    let millis = i64::try_from(duration.as_millis()).map_err(|_| invalid())?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| snapshot_error(format!("Cannot determine lease expiration time: {e}")))?
        .as_millis();
    if millis == 0
        || now + millis as u128 > i64::MAX as u128
        || tokio::time::Instant::now().checked_add(duration).is_none()
    {
        return Err(invalid());
    }
    Ok(millis)
}

struct DownloadLease {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<()>>,
    failure: watch::Receiver<(tokio::time::Instant, Option<String>)>,
}

impl DownloadLease {
    async fn acquire(
        admin: Arc<FlussAdmin>,
        bucket: &TableBucket,
        snapshot_id: SnapshotId,
        lease_duration_ms: i64,
    ) -> Result<Self> {
        let id = format!("rust-kv-snapshot-{}", uuid::Uuid::new_v4());
        let snapshots = vec![KvSnapshotLeaseForTable {
            table_id: bucket.table_id(),
            bucket_snapshots: vec![KvSnapshotLeaseForBucket {
                partition_id: bucket.partition_id(),
                bucket_id: bucket.bucket_id(),
                snapshot_id,
            }],
        }];
        let lease_duration = Duration::from_millis(lease_duration_ms as u64);
        let valid_until = tokio::time::Instant::now() + lease_duration;
        let acquired = admin
            .create_kv_snapshot_lease(&id, lease_duration_ms, snapshots.clone())
            .await;
        match acquired {
            Ok(result) if result.unavailable_snapshots.is_empty() => {}
            result => {
                let _ = admin.drop_kv_snapshot_lease(&id).await;
                result?;
                return Err(snapshot_error(
                    "Requested KV snapshot is unavailable for leasing",
                ));
            }
        }
        let (stop, stopped) = oneshot::channel();
        let (failed, failure) = watch::channel((valid_until, None));
        let task = tokio::spawn(async move {
            renew_download_lease(lease_duration, valid_until, stopped, failed, || async {
                admin
                    .create_kv_snapshot_lease(&id, lease_duration_ms, snapshots.clone())
                    .await
                    .map(|result| result.unavailable_snapshots.is_empty())
            })
            .await;
            // Complete an in-flight acquire/renew before releasing, so a late RPC cannot
            // recreate a lease that cancellation already released.
            admin.drop_kv_snapshot_lease(&id).await
        });
        Ok(Self {
            stop,
            task,
            failure,
        })
    }

    fn check(&self) -> Result<()> {
        let status = self.failure.borrow();
        if tokio::time::Instant::now() >= status.0 {
            return Err(snapshot_error("KV snapshot download lease expired"));
        }
        match status.1.as_ref() {
            Some(error) => Err(snapshot_error(error.clone())),
            None => Ok(()),
        }
    }

    async fn release(self) -> Result<()> {
        let _ = self.stop.send(());
        self.task
            .await
            .map_err(|e| snapshot_error(format!("Snapshot lease task failed: {e}")))?
    }
}

async fn renew_download_lease<F, Fut>(
    duration: Duration,
    mut valid_until: tokio::time::Instant,
    mut stopped: oneshot::Receiver<()>,
    status: watch::Sender<(tokio::time::Instant, Option<String>)>,
    mut renew: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let interval = duration / 3;
    let min_backoff = Duration::from_secs(1).min(interval);
    let max_backoff = Duration::from_secs(10).min(interval);
    let mut backoff = min_backoff;
    let mut next_attempt = tokio::time::Instant::now() + interval;
    let error = loop {
        tokio::select! {
            biased;
            _ = &mut stopped => return,
            _ = tokio::time::sleep_until(next_attempt.min(valid_until)) => {}
        }
        let started = tokio::time::Instant::now();
        if started >= valid_until {
            break "KV snapshot download lease expired".to_string();
        }
        let renewed = renew().await;
        if tokio::time::Instant::now() >= valid_until {
            break "KV snapshot lease renewal missed its expiration deadline".to_string();
        }
        let delay = match renewed {
            Ok(true) => {
                // Response latency must not extend the locally confirmed protection period.
                valid_until = started + duration;
                let _ = status.send((valid_until, None));
                backoff = min_backoff;
                interval
            }
            Ok(false) => {
                break "KV snapshot became unavailable during lease renewal".to_string();
            }
            Err(error) if error.is_retriable() => {
                log::warn!("KV snapshot lease renewal failed; retrying in {backoff:?}: {error}");
                let delay = backoff;
                backoff = (backoff * 2).min(max_backoff);
                delay
            }
            Err(error) => break format!("KV snapshot lease renewal failed: {error}"),
        };
        next_attempt = tokio::time::Instant::now() + delay;
    };
    let _ = status.send((valid_until, Some(error)));
    // The download owner stops file writes before allowing the lease to be released.
    let _ = stopped.await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Metadata;
    use crate::cluster::Cluster;
    use crate::metadata::{DataField, DataTypes, RemotePathAndLocalFile, TablePath};
    use crate::row::binary::BinaryWriter;
    use crate::row::compacted::CompactedRowWriter;
    use crate::rpc::RpcClient;
    use crate::test_utils::build_table_info_with_columns;

    fn transient_lease_error() -> Error {
        crate::rpc::RpcError::ConnectionError("temporary disconnect".into()).into()
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_lease_retries_without_extending_unconfirmed_deadline() {
        let duration = DEFAULT_SNAPSHOT_LEASE_DURATION;
        let initial_deadline = tokio::time::Instant::now() + duration;
        let (stop, stopped) = oneshot::channel();
        let (status, observed) = watch::channel((initial_deadline, None));
        let calls = std::cell::Cell::new(0);
        let mut task = Box::pin(renew_download_lease(
            duration,
            initial_deadline,
            stopped,
            status,
            || {
                calls.set(calls.get() + 1);
                std::future::ready(if calls.get() <= 2 {
                    Err(transient_lease_error())
                } else {
                    Ok(true)
                })
            },
        ));
        assert!(futures::poll!(task.as_mut()).is_pending());
        for (delay, expected_calls) in [(100, 1), (1, 2)] {
            tokio::time::advance(Duration::from_secs(delay)).await;
            assert!(futures::poll!(task.as_mut()).is_pending());
            assert_eq!(calls.get(), expected_calls);
            assert_eq!(*observed.borrow(), (initial_deadline, None));
        }
        tokio::time::advance(Duration::from_secs(2)).await;
        assert!(futures::poll!(task.as_mut()).is_pending());
        assert_eq!(calls.get(), 3);
        assert_eq!(
            *observed.borrow(),
            (tokio::time::Instant::now() + duration, None)
        );
        tokio::time::advance(Duration::from_secs(100)).await;
        assert!(futures::poll!(task.as_mut()).is_pending());
        assert_eq!(calls.get(), 4);
        stop.send(()).unwrap();
        task.await;
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_lease_stops_retrying_at_expiration() {
        let duration = Duration::from_secs(3);
        let deadline = tokio::time::Instant::now() + duration;
        let (stop, stopped) = oneshot::channel();
        let (status, observed) = watch::channel((deadline, None));
        let calls = std::cell::Cell::new(0);
        let mut task = Box::pin(renew_download_lease(
            duration,
            deadline,
            stopped,
            status,
            || {
                calls.set(calls.get() + 1);
                std::future::ready(Err(transient_lease_error()))
            },
        ));
        assert!(futures::poll!(task.as_mut()).is_pending());
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(futures::poll!(task.as_mut()).is_pending());
        }
        assert_eq!(calls.get(), 2);
        assert_eq!(observed.borrow().0, deadline);
        assert!(observed.borrow().1.as_ref().unwrap().contains("expired"));
        stop.send(()).unwrap();
        task.await;
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_lease_does_not_retry_unavailable_or_permanent_errors() {
        for result in [
            Ok(false),
            Err(Error::IllegalArgument {
                message: "invalid lease".into(),
            }),
        ] {
            let duration = Duration::from_secs(30);
            let deadline = tokio::time::Instant::now() + duration;
            let (stop, stopped) = oneshot::channel();
            let (status, observed) = watch::channel((deadline, None));
            let mut result = Some(result);
            let mut task = Box::pin(renew_download_lease(
                duration,
                deadline,
                stopped,
                status,
                || std::future::ready(result.take().expect("must not retry")),
            ));
            assert!(futures::poll!(task.as_mut()).is_pending());
            tokio::time::advance(Duration::from_secs(10)).await;
            assert!(futures::poll!(task.as_mut()).is_pending());
            assert!(observed.borrow().1.is_some());
            tokio::time::advance(duration).await;
            assert!(futures::poll!(task.as_mut()).is_pending());
            stop.send(()).unwrap();
            task.await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_lease_finishes_in_flight_renewal_before_stopping() {
        for expires in [false, true] {
            let duration = Duration::from_secs(3);
            let deadline = tokio::time::Instant::now() + duration;
            let (stop, stopped) = oneshot::channel();
            let (status, observed) = watch::channel((deadline, None));
            let (reply, response) = oneshot::channel();
            let mut response = Some(response);
            let mut task = Box::pin(renew_download_lease(
                duration,
                deadline,
                stopped,
                status,
                || {
                    let response = response.take().expect("must not start another renewal");
                    async move { response.await.unwrap() }
                },
            ));
            assert!(futures::poll!(task.as_mut()).is_pending());
            tokio::time::advance(Duration::from_secs(1)).await;
            assert!(futures::poll!(task.as_mut()).is_pending());
            if expires {
                tokio::time::advance(Duration::from_secs(2)).await;
            }
            stop.send(()).unwrap();
            assert!(futures::poll!(task.as_mut()).is_pending());
            reply.send(Ok(true)).unwrap();
            task.await;
            if expires {
                assert_eq!(observed.borrow().0, deadline);
                assert!(observed.borrow().1.as_ref().unwrap().contains("deadline"));
            } else {
                assert!(observed.borrow().1.is_none());
            }
        }
    }

    #[test]
    fn snapshot_lease_duration_rejects_unrepresentable_expiration() {
        assert_eq!(
            validate_lease_duration(Duration::from_micros(1_999)).unwrap(),
            1
        );
        for duration in [
            Duration::ZERO,
            Duration::from_nanos(999_999),
            Duration::MAX,
            Duration::from_millis(i64::MAX as u64),
        ] {
            assert!(matches!(
                validate_lease_duration(duration),
                Err(Error::IllegalArgument { .. })
            ));
        }
    }

    #[test]
    fn snapshot_downloads_share_slots_and_cancel_while_queued() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source");
        std::fs::write(&source, b"snapshot-file").unwrap();
        let file = RemotePathAndLocalFile {
            remote_path: url::Url::from_file_path(source).unwrap().to_string(),
            local_file_name: "CURRENT".into(),
        };
        let first_path = directory.path().join("first");
        let second_path = directory.path().join("second");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (release, wait) = std::sync::mpsc::channel();
            let (started, ready) = oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            });
            ready.await.unwrap();
            let slots = Semaphore::new(1);
            let (_credentials_tx, mut first_credentials) = watch::channel(Some(HashMap::new()));
            let mut second_credentials = first_credentials.clone();
            let (mut first_tx, _first_rx) = oneshot::channel();
            let (mut second_tx, second_rx) = oneshot::channel();
            let mut first = Box::pin(download_snapshot_file(
                &file,
                &first_path,
                &mut first_credentials,
                &slots,
                &mut first_tx,
                1,
            ));
            assert!(futures::poll!(first.as_mut()).is_pending());
            let slots_while_downloading = slots.available_permits();
            let mut second = Box::pin(download_snapshot_file(
                &file,
                &second_path,
                &mut second_credentials,
                &slots,
                &mut second_tx,
                1,
            ));
            assert!(futures::poll!(second.as_mut()).is_pending());
            drop(second_rx);
            // Release the worker before assertions so a failure cannot hang runtime shutdown.
            release.send(()).unwrap();
            blocker.await.unwrap();
            assert_eq!(slots_while_downloading, 0);
            assert!(second.await.unwrap_err().to_string().contains("cancelled"));
            first.await.unwrap();
            assert!(!second_path.exists());
            assert_eq!(slots.available_permits(), 1);
        });
    }

    #[tokio::test]
    async fn snapshot_waiting_for_credentials_keeps_download_slot_available() {
        let directory = tempfile::tempdir().unwrap();
        let file = RemotePathAndLocalFile {
            remote_path: url::Url::from_file_path(directory.path().join("source"))
                .unwrap()
                .to_string(),
            local_file_name: "CURRENT".into(),
        };
        let slots = Semaphore::new(1);
        let (_credentials_tx, mut credentials) = watch::channel(None);
        let (mut caller, receiver) = oneshot::channel();
        let mut download = Box::pin(download_snapshot_file(
            &file,
            directory.path(),
            &mut credentials,
            &slots,
            &mut caller,
            1,
        ));
        assert!(futures::poll!(download.as_mut()).is_pending());
        assert!(slots.try_acquire().is_ok());
        drop(receiver);
        assert!(
            download
                .await
                .unwrap_err()
                .to_string()
                .contains("cancelled")
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    fn reader() -> (KvSnapshotReader, std::path::PathBuf) {
        reader_with_schema(1)
    }

    fn reader_with_schema(schema_id: i16) -> (KvSnapshotReader, std::path::PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_owned();
        let info = build_table_info_with_columns(
            TablePath::new("db", "table"),
            1,
            1,
            vec![DataField::new("id", DataTypes::int(), None)],
        );
        {
            let db = DB::open_default(&path).unwrap();
            let mut row = CompactedRowWriter::new(1);
            row.write_int(42);
            let mut value = schema_id.to_le_bytes().to_vec();
            value.extend_from_slice(&row.to_bytes());
            db.put(b"key", value).unwrap();
            db.flush().unwrap();
        }
        let local = open_local(directory).unwrap();
        let admin = Arc::new(FlussAdmin::new(
            Arc::new(RpcClient::new()),
            Arc::new(Metadata::new_for_test(Arc::new(Cluster::default()))),
        ));
        let schema_getter = Arc::new(ClientSchemaGetter::new(
            info.table_path.clone(),
            admin.clone(),
            SchemaInfo::new(info.get_schema().clone(), 1),
        ));
        (
            KvSnapshotReader {
                bucket: TableBucket::new(1, 0),
                snapshot_id: 7,
                log_offset: 12,
                state: Some(ReadState {
                    local,
                    target_schema: Arc::new(info.get_schema().clone()),
                    target_schema_id: 1,
                    schema_getter,
                    admin,
                    projected_fields: None,
                    table_info: info,
                    decoders: HashMap::new(),
                    value_offset: 0,
                    last_key: None,
                }),
            },
            path,
        )
    }

    #[tokio::test]
    async fn snapshot_read_error_releases_database_and_directory() {
        let (mut reader, path) = reader();
        reader.state.as_mut().unwrap().value_offset = usize::MAX;
        assert!(
            reader
                .next_batch(1)
                .await
                .unwrap_err()
                .to_string()
                .contains("shorter than its layout prefix")
        );
        assert!(!path.exists());
        assert!(reader.next_batch(1).await.unwrap().is_none());
    }

    #[test]
    fn snapshot_cancelled_batch_releases_resources_after_blocking_work() {
        let (mut reader, path) = reader();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let (release, wait) = std::sync::mpsc::channel();
            let (started, ready) = oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            });
            ready.await.unwrap();
            let mut batch = Box::pin(reader.next_batch(1));
            assert!(futures::poll!(batch.as_mut()).is_pending());
            drop(batch);
            assert!(reader.next_batch(1).await.unwrap().is_none());
            release.send(()).unwrap();
            blocker.await.unwrap();
            // The single blocking worker runs the queued read before this marker.
            tokio::task::spawn_blocking(|| {}).await.unwrap();
            assert!(!path.exists());
        });
    }

    #[tokio::test]
    async fn snapshot_empty_projection_preserves_row_count() {
        let (mut reader, _) = reader();
        reader.state.as_mut().unwrap().projected_fields = Some(Vec::new());
        let batch = reader.next_batch(10).await.unwrap().unwrap();
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 1);
    }

    #[tokio::test]
    async fn snapshot_schema_lookup_failure_releases_local_resources() {
        let (mut reader, path) = reader_with_schema(2);
        // Opening local files does not resolve source schemas. The first read needs the
        // unavailable coordinator for schema 2, then must close the database on failure.
        assert!(
            reader
                .next_batch(10)
                .await
                .unwrap_err()
                .to_string()
                .contains("Coordinator server not found")
        );
        assert!(!path.exists());
        assert!(reader.next_batch(10).await.unwrap().is_none());
    }

    #[test]
    fn snapshot_manifest_rejects_missing_and_unsafe_files() {
        let metadata = |names: &[&str]| KvSnapshotMetadata {
            log_offset: 12,
            snapshot_files: names
                .iter()
                .map(|name| RemotePathAndLocalFile {
                    remote_path: "file:///snapshot/file".into(),
                    local_file_name: name.to_string(),
                })
                .collect(),
        };
        assert!(validate_files(&metadata(&["CURRENT", "MANIFEST-000001", "000002.sst"])).is_ok());
        for names in [
            vec!["000002.sst"],
            vec!["CURRENT", "MANIFEST-000001", "../outside"],
            vec!["CURRENT", "MANIFEST-000001", "CURRENT"],
        ] {
            assert!(validate_files(&metadata(&names)).is_err());
        }
    }
}
