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
use crate::client::{ClientSchemaGetter, FlussAdmin, SecurityTokenManager};
use crate::error::{Error, Result};
use crate::metadata::{
    KvSnapshotLeaseForBucket, KvSnapshotLeaseForTable, KvSnapshotMetadata, SchemaInfo, TableBucket,
    TableInfo,
};
use crate::record::RowAppendRecordBatchBuilder;
use crate::row::FixedSchemaDecoder;
use arrow::array::RecordBatch;
use futures::Stream;
use rocksdb::{DB, Options, ReadOptions};
use std::collections::{HashMap, HashSet};
use std::path::{Component, Path};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

fn snapshot_error(message: impl Into<String>) -> Error {
    Error::UnexpectedError {
        message: message.into(),
        source: None,
    }
}

/// Reads one persisted bucket snapshot, aligned to the schema captured by its table handle.
///
/// Created with [`FlussTable::create_kv_snapshot_reader`]. The snapshot is fully downloaded
/// before this reader is returned. No remote lease or metadata I/O is needed while reading.
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
    decoders: HashMap<i16, FixedSchemaDecoder>,
    value_offset: usize,
    last_key: Option<Vec<u8>>,
}

impl FlussTable<'_> {
    /// Downloads and opens exactly `snapshot_id` for `bucket`, never substituting a newer snapshot.
    ///
    /// Uses `scanner_io_tmp_dir` (or the OS temporary directory plus `fluss`) and the existing
    /// remote-storage credentials provider. A renewable lease protects the download and is
    /// released before returning. Missing snapshots, files, schemas, and unsupported formats fail
    /// creation. Cancelling creation completes any in-flight file download before cleaning up;
    /// the connection/runtime must remain alive for asynchronous lease release. If the runtime
    /// is shut down, the server lease expires after 60 seconds without renewal.
    ///
    /// `reader.log_offset()` is the exclusive end of the snapshot's log prefix and the start of
    /// a subsequent `[S, E)` tail. Capture `E` only after selecting this snapshot and `S`.
    pub async fn create_kv_snapshot_reader(
        &self,
        bucket: TableBucket,
        snapshot_id: SnapshotId,
    ) -> Result<KvSnapshotReader> {
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
        let schema_getter = ClientSchemaGetter::new(
            self.table_path.clone(),
            admin.clone(),
            SchemaInfo::new(
                self.table_info.get_schema().clone(),
                self.table_info.get_schema_id(),
            ),
        );
        let table_info = self.table_info.clone();
        let config = self.conn.config().clone();
        let tokens = SecurityTokenManager::new(self.conn.get_connections(), self.metadata.clone());
        let (mut tx, rx) = oneshot::channel();
        // This task owns acquisition as well as cleanup: dropping the caller cannot race a
        // successful acquire response or remove a directory underneath an in-flight file write.
        tokio::spawn(async move {
            let result = async {
                let lease = DownloadLease::acquire(admin.clone(), &bucket, snapshot_id).await?;
                let result = async {
                    let metadata = admin.get_kv_snapshot_metadata(
                        bucket.table_id(), bucket.partition_id(), bucket.bucket_id(), snapshot_id,
                    ).await?;
                    validate_files(&metadata)?;
                    let parent = config.scanner_io_tmp_dir.as_ref().map(std::path::PathBuf::from)
                        .unwrap_or_else(|| std::env::temp_dir().join("fluss"));
                    std::fs::create_dir_all(&parent)?;
                    let directory = tempfile::Builder::new().prefix("kv-snapshot-").tempdir_in(parent)?;
                    let mut credentials = tokens.subscribe();
                    tokens.start();
                    for file in &metadata.snapshot_files {
                        if tx.is_closed() { return Err(snapshot_error("Snapshot creation cancelled")); }
                        let props = tokio::select! {
                            _ = tx.closed() => return Err(snapshot_error("Snapshot creation cancelled")),
                            result = credentials.wait_for(|value| value.is_some()) => result
                                .map_err(|e| snapshot_error(format!("Snapshot credentials unavailable: {e}")))?
                                .clone().unwrap(),
                        };
                        RemoteLogDownloader::download_file(
                            &file.remote_path, &file.remote_path,
                            &directory.path().join(&file.local_file_name), &props,
                            config.scanner_remote_log_read_concurrency.max(1),
                        ).await?;
                        lease.check()?;
                    }
                    if tx.is_closed() { return Err(snapshot_error("Snapshot creation cancelled")); }
                    let (local, schema_ids) = tokio::task::spawn_blocking(move || {
                        open_local(directory, value_offset)
                    }).await.map_err(|e| snapshot_error(format!("Snapshot open task failed: {e}")))??;
                    let target_id = i16::try_from(table_info.get_schema_id())
                        .map_err(|e| snapshot_error(format!("Invalid target schema ID: {e}")))?;
                    let decoders = build_kv_decoders(
                        &schema_getter, table_info.get_schema(), target_id,
                        table_info.table_config.get_kv_format()?, &schema_ids,
                    ).await?;
                    if admin.get_table_info(&table_info.table_path).await?.table_id != bucket.table_id() {
                        return Err(snapshot_error("Table was replaced while resolving snapshot schemas"));
                    }
                    lease.check()?;
                    Ok(KvSnapshotReader {
                        bucket, snapshot_id, log_offset: metadata.log_offset,
                        state: Some(ReadState { local, table_info, decoders, value_offset, last_key: None }),
                    })
                }.await;
                let released = lease.release().await;
                // Report the original read/download failure if cleanup also fails.
                match result {
                    Ok(reader) => { released?; Ok(reader) },
                    Err(error) => {
                        if let Err(cleanup_error) = released {
                            log::warn!("Failed to release KV snapshot download lease: {cleanup_error}");
                        }
                        Err(error)
                    }
                }
            }.await;
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
        let (state, batch) = tokio::task::spawn_blocking(move || {
            let (batch, finished) = state.read_batch(max_rows)?;
            Ok::<_, Error>((if finished { None } else { Some(state) }, batch))
        })
        .await
        .map_err(|e| snapshot_error(format!("Snapshot read task failed: {e}")))??;
        self.state = state;
        Ok(batch)
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
    fn read_batch(&mut self, max_rows: usize) -> Result<(Option<RecordBatch>, bool)> {
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
        let mut builder = RowAppendRecordBatchBuilder::new(self.table_info.get_row_type())?;
        let mut count = 0;
        while iter.valid() && count < max_rows {
            let payload = value_body(iter.value().unwrap(), self.value_offset)?;
            let id = read_schema_id(payload)?;
            let decoder = self
                .decoders
                .get(&id)
                .ok_or_else(|| snapshot_error(format!("Missing decoder for schema {id}")))?;
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
            Some(Arc::unwrap_or_clone(builder.build_arrow_record_batch()?))
        };
        Ok((batch, finished))
    }
}

fn open_local(directory: TempDir, value_offset: usize) -> Result<(LocalSnapshot, Vec<i16>)> {
    let db = DB::open_for_read_only(&Options::default(), directory.path(), true)
        .map_err(|e| snapshot_error(format!("Cannot open persisted KV snapshot: {e}")))?;
    let mut ids = HashSet::new();
    {
        let mut iter = db.raw_iterator();
        iter.seek_to_first();
        while iter.valid() {
            ids.insert(read_schema_id(value_body(
                iter.value().unwrap(),
                value_offset,
            )?)?);
            iter.next();
        }
        iter.status()
            .map_err(|e| snapshot_error(format!("Snapshot schema discovery failed: {e}")))?;
    }
    Ok((
        LocalSnapshot {
            db,
            _directory: directory,
        },
        ids.into_iter().collect(),
    ))
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
        let mut valid_until = tokio::time::Instant::now() + Duration::from_secs(60);
        let acquired = admin
            .create_kv_snapshot_lease(&id, 60_000, snapshots.clone())
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
        let (stop, mut stopped) = oneshot::channel();
        let (failed, failure) = watch::channel((valid_until, None));
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    _ = tokio::time::sleep(Duration::from_secs(20)) => {}
                }
                let renewed_until = tokio::time::Instant::now() + Duration::from_secs(60);
                let renewed = admin
                    .create_kv_snapshot_lease(&id, 60_000, snapshots.clone())
                    .await;
                let error = if tokio::time::Instant::now() >= valid_until {
                    Some("KV snapshot lease renewal missed its expiration deadline".to_string())
                } else {
                    match renewed {
                        Ok(result) if result.unavailable_snapshots.is_empty() => None,
                        Ok(_) => {
                            Some("KV snapshot became unavailable during lease renewal".to_string())
                        }
                        Err(error) => Some(format!("KV snapshot lease renewal failed: {error}")),
                    }
                };
                if let Some(error) = error {
                    let _ = failed.send((valid_until, Some(error)));
                    let _ = stopped.await;
                    break;
                }
                valid_until = renewed_until;
                let _ = failed.send((valid_until, None));
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes, KvFormat, RemotePathAndLocalFile, TablePath};
    use crate::row::binary::BinaryWriter;
    use crate::row::compacted::CompactedRowWriter;
    use crate::test_utils::build_table_info_with_columns;

    fn reader() -> (KvSnapshotReader, std::path::PathBuf) {
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
            let mut value = 1i16.to_le_bytes().to_vec();
            value.extend_from_slice(&row.to_bytes());
            db.put(b"key", value).unwrap();
            db.flush().unwrap();
        }
        let (local, ids) = open_local(directory, 0).unwrap();
        assert_eq!(ids, vec![1]);
        let decoder =
            FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, info.get_schema()).unwrap();
        (
            KvSnapshotReader {
                bucket: TableBucket::new(1, 0),
                snapshot_id: 7,
                log_offset: 12,
                state: Some(ReadState {
                    local,
                    table_info: info,
                    decoders: HashMap::from([(1, decoder)]),
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
        reader.state.as_mut().unwrap().decoders.clear();
        assert!(
            reader
                .next_batch(1)
                .await
                .unwrap_err()
                .to_string()
                .contains("Missing decoder")
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

    #[test]
    fn snapshot_open_failure_removes_directory() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_owned();
        assert!(open_local(directory, 0).is_err());
        assert!(!path.exists());
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
