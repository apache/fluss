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

use crate::RUNTIME;
use crate::async_nif;
use crate::atoms::{self, NifFlussError, to_nif_err};
use crate::row_convert;
use crate::table::TableResource;
use fluss::client::{EARLIEST_OFFSET, LogScanner};
use fluss::error::Error;
use fluss::metadata::Column;
use fluss::record::{ChangeType, ScanRecords};
use rustler::env::OwnedEnv;
use rustler::types::LocalPid;
use rustler::{Atom, Encoder, Env, ResourceArc, Term};
use std::collections::HashMap;
use std::time::Duration;

pub struct LogScannerResource {
    pub inner: LogScanner,
    pub columns: Vec<Column>,
}

impl std::panic::RefUnwindSafe for LogScannerResource {}

#[rustler::resource_impl]
impl rustler::Resource for LogScannerResource {}

#[rustler::nif]
fn log_scanner_new(
    table: ResourceArc<TableResource>,
) -> Result<ResourceArc<LogScannerResource>, rustler::Error> {
    let _guard = RUNTIME.enter();
    let (inner, columns) = table.with_table(|t| {
        let inner = t.new_scan().create_log_scanner().map_err(to_nif_err)?;
        Ok((inner, t.get_table_info().schema.columns().to_vec()))
    })?;
    Ok(ResourceArc::new(LogScannerResource { inner, columns }))
}

#[rustler::nif]
fn log_scanner_subscribe<'a>(
    env: Env<'a>,
    scanner: ResourceArc<LogScannerResource>,
    bucket: i32,
    offset: i64,
) -> Term<'a> {
    async_nif::spawn_task(
        env,
        async move { scanner.inner.subscribe(bucket, offset).await },
    )
}

#[rustler::nif]
fn log_scanner_subscribe_buckets<'a>(
    env: Env<'a>,
    scanner: ResourceArc<LogScannerResource>,
    bucket_offsets: Vec<(i32, i64)>,
) -> Term<'a> {
    let map: HashMap<i32, i64> = bucket_offsets.into_iter().collect();
    async_nif::spawn_task(
        env,
        async move { scanner.inner.subscribe_buckets(&map).await },
    )
}

#[rustler::nif]
fn log_scanner_unsubscribe<'a>(
    env: Env<'a>,
    scanner: ResourceArc<LogScannerResource>,
    bucket: i32,
) -> Term<'a> {
    async_nif::spawn_task(env, async move { scanner.inner.unsubscribe(bucket).await })
}

#[rustler::nif]
fn log_scanner_poll(env: Env, scanner: ResourceArc<LogScannerResource>, timeout_ms: u64) -> Atom {
    let pid = env.pid();
    let scanner = scanner.clone();

    RUNTIME.spawn(async move {
        let result = scanner.inner.poll(Duration::from_millis(timeout_ms)).await;
        send_poll_result(&pid, result, &scanner.columns);
    });

    atoms::ok()
}

fn send_poll_result(pid: &LocalPid, result: Result<ScanRecords, Error>, columns: &[Column]) {
    let mut msg_env = OwnedEnv::new();

    match result {
        Ok(scan_records) => {
            let _ = msg_env.send_and_clear(pid, |env| {
                match encode_scan_records(env, scan_records, columns) {
                    Ok(records) => (atoms::fluss_records(), records).encode(env),
                    Err(message) => {
                        (atoms::fluss_poll_error(), NifFlussError::client(message)).encode(env)
                    }
                }
            });
        }
        Err(e) => {
            let _ = msg_env.send_and_clear(pid, |env| {
                (atoms::fluss_poll_error(), NifFlussError::from_core(&e)).encode(env)
            });
        }
    }
}

fn encode_scan_records<'a>(
    env: Env<'a>,
    scan_records: ScanRecords,
    columns: &[Column],
) -> Result<rustler::Term<'a>, String> {
    let column_atoms = row_convert::intern_column_atoms(env, columns);
    let mut result = Vec::new();

    for record in scan_records {
        let row_map = row_convert::row_to_term(env, record.row(), columns, &column_atoms)
            .map_err(|e| format!("failed to convert row at offset {}: {e}", record.offset()))?;
        let change_type_atom = match record.change_type() {
            ChangeType::AppendOnly => atoms::append_only().encode(env),
            ChangeType::Insert => atoms::insert().encode(env),
            ChangeType::UpdateBefore => atoms::update_before().encode(env),
            ChangeType::UpdateAfter => atoms::update_after().encode(env),
            ChangeType::Delete => atoms::delete().encode(env),
        };

        let record_map = rustler::Term::map_from_pairs(
            env,
            &[
                (atoms::offset().encode(env), record.offset().encode(env)),
                (
                    atoms::timestamp().encode(env),
                    record.timestamp().encode(env),
                ),
                (atoms::change_type().encode(env), change_type_atom),
                (atoms::row().encode(env), row_map),
            ],
        )
        .map_err(|_| "failed to create record map".to_string())?;
        result.push(record_map);
    }

    Ok(result.encode(env))
}

#[rustler::nif]
fn earliest_offset() -> i64 {
    EARLIEST_OFFSET
}
