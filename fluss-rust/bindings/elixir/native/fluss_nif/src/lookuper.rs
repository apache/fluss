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
use crate::atoms::{self, client_err, to_nif_err};
use crate::row_convert;
use crate::table::TableResource;
use fluss::client::{LookupResult, Lookuper, PrefixKeyLookuper};
use fluss::metadata::Column;
use fluss::row::{Datum, GenericRow, InternalRow};
use rustler::{Encoder, Env, ResourceArc, Term, TermType};

/// Key columns build the key-only lookup row; all columns decode the result.
pub struct KeySpec {
    columns: Vec<Column>,
    key_indices: Vec<usize>,
    key_columns: Vec<Column>,
}

impl KeySpec {
    fn new(columns: &[Column], key_indices: Vec<usize>) -> Self {
        let key_columns = key_indices
            .iter()
            .map(|&idx| columns[idx].clone())
            .collect();
        Self {
            columns: columns.to_vec(),
            key_indices,
            key_columns,
        }
    }

    fn row<'a>(&self, env: Env<'a>, key: Term<'a>) -> Result<GenericRow<'static>, String> {
        if key.get_type() == TermType::Map {
            // Built full width so non-key columns are "not accepted here"; nil keys fail NOT NULL.
            let mut full =
                row_convert::map_to_row(env, key, &self.columns, Some(&self.key_indices))?;
            let values: Vec<Datum<'static>> = self
                .key_indices
                .iter()
                .map(|&idx| std::mem::replace(&mut full.values[idx], Datum::Null))
                .collect();
            Ok(GenericRow::from_data(values))
        } else {
            // Key columns are NOT NULL, so this also rejects a nil key.
            row_convert::term_to_row(env, key, &self.key_columns)
        }
    }

    fn encode_rows<'a, R: InternalRow>(
        &self,
        env: Env<'a>,
        rows: &[R],
    ) -> Result<Vec<Term<'a>>, String> {
        let column_atoms = row_convert::intern_column_atoms(env, &self.columns);
        rows.iter()
            .map(|row| row_convert::row_to_term(env, row, &self.columns, &column_atoms))
            .collect()
    }
}

pub struct LookuperResource {
    inner: Lookuper,
    key: KeySpec,
}

impl std::panic::RefUnwindSafe for LookuperResource {}

#[rustler::resource_impl]
impl rustler::Resource for LookuperResource {}

pub struct PrefixLookuperResource {
    inner: PrefixKeyLookuper,
    key: KeySpec,
}

impl std::panic::RefUnwindSafe for PrefixLookuperResource {}

#[rustler::resource_impl]
impl rustler::Resource for PrefixLookuperResource {}

#[rustler::nif]
fn lookuper_new(
    table: ResourceArc<TableResource>,
) -> Result<ResourceArc<LookuperResource>, rustler::Error> {
    // LookupClient::new() calls tokio::spawn internally.
    let _guard = RUNTIME.enter();
    let inner = table
        .with_table(|t| t.new_lookup()?.create_lookuper())
        .map_err(to_nif_err)?;
    let key_indices = table.table_info.schema.primary_key_indexes();

    Ok(ResourceArc::new(LookuperResource {
        inner,
        key: KeySpec::new(table.columns(), key_indices),
    }))
}

#[rustler::nif]
fn lookuper_lookup<'a>(
    env: Env<'a>,
    lookuper: ResourceArc<LookuperResource>,
    key: Term<'a>,
) -> Result<Term<'a>, rustler::Error> {
    let row = lookuper.key.row(env, key).map_err(client_err)?;
    let reply = lookuper.clone();

    Ok(async_nif::spawn_task_encode(
        env,
        async move { lookuper.inner.lookup(&row).await },
        move |env, result: LookupResult| match result.get_single_row().map_err(|e| e.to_string())? {
            Some(row) => Ok(reply.key.encode_rows(env, &[row])?[0]),
            None => Ok(atoms::nil().encode(env)),
        },
    )
    .encode(env))
}

#[rustler::nif]
fn prefix_lookuper_new<'a>(
    table: ResourceArc<TableResource>,
    columns: Term<'a>,
) -> Result<ResourceArc<PrefixLookuperResource>, rustler::Error> {
    let names: Vec<String> = row_convert::decode_list(columns, "expected a list of column names")
        .map_err(client_err)?
        .iter()
        .map(|name| row_convert::term_to_column_name(*name))
        .collect::<Result<_, String>>()
        .map_err(client_err)?;

    // LookupClient::new() calls tokio::spawn internally. Core validates the columns first.
    let _guard = RUNTIME.enter();
    let inner = table
        .with_table(|t| t.new_lookup()?.lookup_by(names.clone()).create_lookuper())
        .map_err(to_nif_err)?;

    let row_type = table.table_info.row_type();
    let key_indices = names
        .iter()
        .map(|name| {
            row_type
                .get_field_index(name)
                .ok_or_else(|| format!("unknown column `{name}`"))
        })
        .collect::<Result<Vec<usize>, String>>()
        .map_err(client_err)?;
    // Core's validation ignores a repeated partition column; a list prefix would need it twice.
    row_convert::check_distinct_columns(table.columns(), &key_indices).map_err(client_err)?;

    Ok(ResourceArc::new(PrefixLookuperResource {
        inner,
        key: KeySpec::new(table.columns(), key_indices),
    }))
}

#[rustler::nif]
fn prefix_lookuper_lookup<'a>(
    env: Env<'a>,
    lookuper: ResourceArc<PrefixLookuperResource>,
    prefix: Term<'a>,
) -> Result<Term<'a>, rustler::Error> {
    let row = lookuper.key.row(env, prefix).map_err(client_err)?;
    let reply = lookuper.clone();

    Ok(async_nif::spawn_task_encode(
        env,
        async move { lookuper.inner.lookup(&row).await },
        move |env, result: LookupResult| {
            let rows = result.get_rows().map_err(|e| e.to_string())?;
            Ok(reply.key.encode_rows(env, &rows)?.encode(env))
        },
    )
    .encode(env))
}
