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

use crate::async_nif;
use crate::atoms::client_err;
use crate::row_convert;
use crate::write_handle;
use fluss::client::UpsertWriter;
use fluss::metadata::TableInfo;
use rustler::{Env, ResourceArc, Term, TermType};

/// Core's `UpsertWriter` exposes neither its schema nor its targets, and building a row
/// needs both, so they are kept here.
pub struct UpsertWriterResource {
    pub inner: UpsertWriter,
    pub table_info: TableInfo,
    /// Target column indices in the order the caller named them, or `None` for a writer
    /// that writes every column.
    pub target_indices: Option<Vec<usize>>,
}

impl std::panic::RefUnwindSafe for UpsertWriterResource {}

#[rustler::resource_impl]
impl rustler::Resource for UpsertWriterResource {}

#[rustler::nif]
fn upsert_writer_upsert<'a>(
    env: Env<'a>,
    writer: ResourceArc<UpsertWriterResource>,
    values: Term<'a>,
) -> Result<Term<'a>, rustler::Error> {
    let columns = writer.table_info.schema.columns();
    let row = if values.get_type() == TermType::Map {
        row_convert::map_to_row(env, values, columns, writer.target_indices.as_deref())
    } else {
        match writer.target_indices.as_deref() {
            None => row_convert::term_to_row(env, values, columns),
            Some(indices) => row_convert::term_to_sparse_row(env, values, columns, indices),
        }
    }
    .map_err(client_err)?;

    write_handle::queue_write(env, move |deadline| {
        writer.inner.upsert_with_deadline(&row, deadline)
    })
}

#[rustler::nif]
fn upsert_writer_delete<'a>(
    env: Env<'a>,
    writer: ResourceArc<UpsertWriterResource>,
    primary_key: Term<'a>,
) -> Result<Term<'a>, rustler::Error> {
    let schema = &writer.table_info.schema;
    let columns = schema.columns();
    // Core rejects a row that is not full width even though delete only reads the key.
    let pk_indices = schema.primary_key_indexes();
    let row = if primary_key.get_type() == TermType::Map {
        row_convert::map_to_row(env, primary_key, columns, Some(&pk_indices))
    } else {
        row_convert::term_to_sparse_row(env, primary_key, columns, &pk_indices)
    }
    .map_err(client_err)?;

    write_handle::queue_write(env, move |deadline| {
        writer.inner.delete_with_deadline(&row, deadline)
    })
}

#[rustler::nif]
fn upsert_writer_flush<'a>(env: Env<'a>, writer: ResourceArc<UpsertWriterResource>) -> Term<'a> {
    async_nif::spawn_task(env, async move { writer.inner.flush().await })
}
