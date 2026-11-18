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

use crate::admin::AdminResource;
use crate::connection::ConnectionResource;
use crate::table::TableResource;
use crate::{RUNTIME, async_nif, atoms};
use fluss::client::{
    ConsumerGroupOptions, MultiTableLogScanner, OffsetAndMetadata, OffsetCommitError,
    OffsetCommitOutcome, OffsetsInitializer,
};
use fluss::metadata::{Column, TableBucket, TablePath};
use rustler::{Encoder, Env, NifMap, NifTaggedEnum, NifUnitEnum, ResourceArc, Term};
use std::collections::HashMap;
use std::time::Duration;

type Bucket = (i64, Option<i64>, i32);
type SavedOffset = (i64, Option<String>);
fn bucket(value: Bucket) -> TableBucket {
    TableBucket::new_with_partition(value.0, value.1, value.2)
}
fn bucket_value(value: TableBucket) -> Bucket {
    (value.table_id(), value.partition_id(), value.bucket_id())
}
fn invalid(message: &str) -> fluss::error::Error {
    fluss::error::Error::IllegalArgument {
        message: message.into(),
    }
}

#[derive(NifUnitEnum)]
pub enum Status {
    Success,
    Rejected,
    Unknown,
    NeedsRecovery,
}
#[derive(NifMap)]
pub struct Outcome {
    status: Status,
    error_code: Option<i32>,
    message: String,
}
impl Outcome {
    fn success() -> Self {
        Self {
            status: Status::Success,
            error_code: None,
            message: String::new(),
        }
    }
    fn error(error: OffsetCommitError) -> Self {
        let status = match &error {
            OffsetCommitError::Rejected(_) => Status::Rejected,
            OffsetCommitError::Unknown(_) => Status::Unknown,
            OffsetCommitError::NeedsRecovery => Status::NeedsRecovery,
        };
        let error_code = match &error {
            OffsetCommitError::Rejected(fluss::error::Error::FlussAPIError { api_error })
            | OffsetCommitError::Unknown(fluss::error::Error::FlussAPIError { api_error }) => {
                Some(api_error.code)
            }
            _ => None,
        };
        Self {
            status,
            error_code,
            message: error.to_string(),
        }
    }
}
#[derive(NifMap)]
pub struct CommitResult {
    request: Outcome,
    buckets: HashMap<Bucket, Outcome>,
}
fn commit_result(
    result: Result<fluss::client::OffsetCommitResult, OffsetCommitError>,
) -> CommitResult {
    match result {
        Err(error) => CommitResult {
            request: Outcome::error(error),
            buckets: HashMap::new(),
        },
        Ok(results) => CommitResult {
            request: Outcome::success(),
            buckets: results
                .into_iter()
                .map(|(bucket, result)| {
                    (
                        bucket_value(bucket),
                        match result {
                            OffsetCommitOutcome::Committed => Outcome::success(),
                            OffsetCommitOutcome::Rejected(error) => Outcome {
                                status: Status::Rejected,
                                error_code: Some(error.code),
                                message: error.message,
                            },
                            OffsetCommitOutcome::Unknown(error) => Outcome {
                                status: Status::Unknown,
                                error_code: Some(error.code),
                                message: error.message,
                            },
                        },
                    )
                })
                .collect(),
        },
    }
}
fn values(
    offsets: Vec<(Bucket, SavedOffset)>,
) -> fluss::error::Result<HashMap<TableBucket, OffsetAndMetadata>> {
    let mut values = HashMap::new();
    for (key, (offset, metadata)) in offsets {
        if values
            .insert(bucket(key), OffsetAndMetadata { offset, metadata })
            .is_some()
        {
            return Err(invalid("Duplicate consumer offset bucket"));
        }
    }
    Ok(values)
}
#[derive(NifTaggedEnum)]
pub enum Initializer {
    Earliest,
    Latest,
    Timestamp(i64),
    Offset(i64),
    Fail,
    Committed(String),
}
fn initializer(steps: Vec<Initializer>) -> OffsetsInitializer {
    let mut value = OffsetsInitializer::fail();
    for step in steps.into_iter().rev() {
        value = match step {
            Initializer::Earliest => OffsetsInitializer::earliest(),
            Initializer::Latest => OffsetsInitializer::latest(),
            Initializer::Timestamp(ms) => OffsetsInitializer::timestamp(ms),
            Initializer::Offset(offset) => OffsetsInitializer::offset(offset),
            Initializer::Fail => OffsetsInitializer::fail(),
            Initializer::Committed(group) => {
                OffsetsInitializer::committed_with_fallback(group, value)
            }
        };
    }
    value
}
pub struct ConsumerScannerResource {
    inner: MultiTableLogScanner,
    columns: HashMap<i64, Vec<Column>>,
}
impl std::panic::RefUnwindSafe for ConsumerScannerResource {}
#[rustler::resource_impl]
impl rustler::Resource for ConsumerScannerResource {}

async fn create(
    connection: &fluss::client::FlussConnection,
    tables: Vec<TablePath>,
    group_id: String,
    timeout_ms: u64,
    auto_commit_ms: Option<u64>,
) -> fluss::error::Result<ResourceArc<ConsumerScannerResource>> {
    let options = ConsumerGroupOptions {
        group_id,
        timeout: Duration::from_millis(timeout_ms),
    };
    let mut scan = connection.new_multi_table_scan(&tables, options).await?;
    if let Some(ms) = auto_commit_ms {
        scan = scan.with_auto_commit(Duration::from_millis(ms))?;
    }
    let mut columns = HashMap::new();
    for info in scan.table_infos() {
        columns.insert(info.table_id, info.schema.columns().to_vec());
    }
    Ok(ResourceArc::new(ConsumerScannerResource {
        inner: scan.create_log_scanner()?,
        columns,
    }))
}
#[rustler::nif]
fn consumer_scanner_new(
    env: Env<'_>,
    table: ResourceArc<TableResource>,
    group: String,
    timeout_ms: u64,
    auto_commit_ms: Option<u64>,
) -> Term<'_> {
    async_nif::spawn_task_with_result(env, async move {
        create(
            &table.connection,
            vec![table.table_info.table_path.clone()],
            group,
            timeout_ms,
            auto_commit_ms,
        )
        .await
    })
}
#[rustler::nif]
fn consumer_scanner_multi_new(
    env: Env<'_>,
    connection: ResourceArc<ConnectionResource>,
    tables: Vec<(String, String)>,
    group: String,
    timeout_ms: u64,
    auto_commit_ms: Option<u64>,
) -> Term<'_> {
    async_nif::spawn_task_with_result(env, async move {
        create(
            &connection.inner,
            tables
                .into_iter()
                .map(|(db, table)| TablePath::new(db, table))
                .collect(),
            group,
            timeout_ms,
            auto_commit_ms,
        )
        .await
    })
}
#[rustler::nif]
fn consumer_scanner_subscribe(
    env: Env<'_>,
    scanner: ResourceArc<ConsumerScannerResource>,
    buckets: Vec<Bucket>,
    steps: Vec<Initializer>,
) -> Term<'_> {
    async_nif::spawn_task(env, async move {
        scanner
            .inner
            .subscribe_with_initializer(
                &buckets.into_iter().map(bucket).collect::<Vec<_>>(),
                &initializer(steps),
            )
            .await
    })
}
#[rustler::nif]
fn consumer_scanner_unsubscribe(
    env: Env<'_>,
    scanner: ResourceArc<ConsumerScannerResource>,
    value: Bucket,
) -> Term<'_> {
    async_nif::spawn_task(env, async move {
        scanner.inner.unsubscribe(&bucket(value)).await
    })
}
#[rustler::nif]
fn consumer_scanner_commit(
    env: Env<'_>,
    scanner: ResourceArc<ConsumerScannerResource>,
    offsets: Option<Vec<(Bucket, SavedOffset)>>,
) -> Term<'_> {
    // Enqueue before returning to BEAM; async wait scheduling cannot reorder invocations.
    let _guard = RUNTIME.enter();
    let handle = match offsets {
        None => scanner.inner.commit_async(),
        Some(offsets) => {
            values(offsets).and_then(|values| scanner.inner.commit_offsets_async(values))
        }
    };
    async_nif::spawn_task_with_result(env, async move {
        let result = match handle {
            Ok(handle) => handle.await.unwrap_or_else(|error| Err(error.into())),
            Err(error) => Err(OffsetCommitError::Rejected(error)),
        };
        Ok(commit_result(result))
    })
}
#[rustler::nif]
fn consumer_scanner_poll(
    env: Env<'_>,
    scanner: ResourceArc<ConsumerScannerResource>,
    timeout_ms: u64,
) -> rustler::Atom {
    let pid = env.pid();
    RUNTIME.spawn(async move {
        let result = scanner.inner.poll(Duration::from_millis(timeout_ms)).await;
        let mut owned = rustler::env::OwnedEnv::new();
        let _ = owned.send_and_clear(&pid, |env| match result {
            Err(error) => (
                atoms::fluss_poll_error(),
                atoms::NifFlussError::from_core(&error),
            )
                .encode(env),
            Ok(records) => {
                let result = encode_records(env, records, &scanner.columns);
                match result {
                    Ok(records) => (atoms::fluss_records(), records).encode(env),
                    Err(message) => (
                        atoms::fluss_poll_error(),
                        atoms::NifFlussError::client(message),
                    )
                        .encode(env),
                }
            }
        });
    });
    atoms::ok()
}
fn encode_records<'a>(
    env: Env<'a>,
    records: fluss::record::ScanRecords,
    columns: &HashMap<i64, Vec<Column>>,
) -> Result<Vec<Term<'a>>, String> {
    let mut result = Vec::new();
    for (bucket, records) in records.into_records_by_buckets() {
        let schema = columns
            .get(&bucket.table_id())
            .ok_or_else(|| "Unknown source table".to_string())?;
        let key = bucket_value(bucket);
        let column_atoms = crate::row_convert::intern_column_atoms(env, schema);
        for record in records {
            let encoded = crate::log_scanner::encode_record(env, &record, schema, &column_atoms)?;
            result.push(
                encoded
                    .map_put(atoms::table_bucket(), key)
                    .map_err(|_| "Failed to encode table bucket".to_string())?,
            );
        }
    }
    Ok(result)
}

#[rustler::nif]
fn admin_get_consumer_offsets(
    env: Env<'_>,
    admin: ResourceArc<AdminResource>,
    group: String,
    buckets: Vec<Bucket>,
) -> Term<'_> {
    async_nif::spawn_task_with_result(env, async move {
        admin
            .inner
            .get_consumer_offsets(&group, &buckets.into_iter().map(bucket).collect::<Vec<_>>())
            .await
            .map(|values| {
                values
                    .into_iter()
                    .map(|(key, value)| (bucket_value(key), value.map(|v| (v.offset, v.metadata))))
                    .collect::<HashMap<_, _>>()
            })
    })
}
#[rustler::nif]
fn admin_list_consumer_offsets(
    env: Env<'_>,
    admin: ResourceArc<AdminResource>,
    group: String,
) -> Term<'_> {
    async_nif::spawn_task_with_result(env, async move {
        admin
            .inner
            .list_consumer_offsets(&group)
            .await
            .map(|values| {
                values
                    .into_iter()
                    .map(|(key, value)| (bucket_value(key), (value.offset, value.metadata)))
                    .collect::<HashMap<_, _>>()
            })
    })
}
#[rustler::nif]
fn admin_list_consumer_groups(env: Env<'_>, admin: ResourceArc<AdminResource>) -> Term<'_> {
    async_nif::spawn_task_with_result(env, async move { admin.inner.list_consumer_groups().await })
}
#[rustler::nif]
fn admin_alter_consumer_offsets(
    env: Env<'_>,
    admin: ResourceArc<AdminResource>,
    group: String,
    offsets: Vec<(Bucket, SavedOffset)>,
) -> Term<'_> {
    async_nif::spawn_task_with_result(env, async move {
        Ok(commit_result(
            admin
                .inner
                .alter_consumer_offsets(&group, values(offsets)?)
                .await,
        ))
    })
}
#[rustler::nif]
fn admin_delete_consumer_offsets(
    env: Env<'_>,
    admin: ResourceArc<AdminResource>,
    group: String,
    buckets: Vec<Bucket>,
) -> Term<'_> {
    async_nif::spawn_task_with_result(env, async move {
        let result = admin
            .inner
            .delete_consumer_offsets(&group, &buckets.into_iter().map(bucket).collect::<Vec<_>>())
            .await;
        Ok(match result {
            Err(error) => CommitResult {
                request: Outcome::error(error),
                buckets: HashMap::new(),
            },
            Ok(results) => CommitResult {
                request: Outcome::success(),
                buckets: results
                    .into_iter()
                    .map(|(key, result)| {
                        (
                            bucket_value(key),
                            result.map_or_else(Outcome::error, |_| Outcome::success()),
                        )
                    })
                    .collect(),
            },
        })
    })
}
#[rustler::nif]
fn admin_delete_consumer_groups(
    env: Env<'_>,
    admin: ResourceArc<AdminResource>,
    groups: Vec<String>,
) -> Term<'_> {
    async_nif::spawn_task_with_result(env, async move {
        admin
            .inner
            .delete_consumer_groups(&groups)
            .await
            .map(|results| {
                results
                    .into_iter()
                    .map(|(key, result)| {
                        (
                            key,
                            result.map_or_else(Outcome::error, |_| Outcome::success()),
                        )
                    })
                    .collect::<HashMap<_, _>>()
            })
    })
}
