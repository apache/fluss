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
use crate::atoms::{client_err, to_nif_err};
use crate::row_convert;
use crate::table::TableResource;
use crate::upsert_writer::UpsertWriterResource;
use fluss::client::TableUpsert;
use fluss::metadata::TableInfo;
use rustler::{ResourceArc, Term};

/// Core's `TableUpsert` holds the schema and target columns but exposes neither, so both
/// are kept alongside it here and carried into the writer by `create_writer`.
pub struct TableUpsertResource {
    pub inner: TableUpsert,
    pub table_info: TableInfo,
    /// Target column indices in the order the caller named them, or `None` for a writer
    /// that writes every column.
    pub target_indices: Option<Vec<usize>>,
}

impl std::panic::RefUnwindSafe for TableUpsertResource {}

#[rustler::resource_impl]
impl rustler::Resource for TableUpsertResource {}

#[rustler::nif]
fn table_upsert_new(
    table: ResourceArc<TableResource>,
) -> Result<ResourceArc<TableUpsertResource>, rustler::Error> {
    // WriterClient::new() calls tokio::spawn internally.
    let _guard = RUNTIME.enter();
    let (inner, table_info) = table.with_table(|t| {
        let inner = t.new_upsert().map_err(to_nif_err)?;
        Ok((inner, t.get_table_info().clone()))
    })?;
    Ok(ResourceArc::new(TableUpsertResource {
        inner,
        table_info,
        target_indices: None,
    }))
}

#[rustler::nif]
fn table_upsert_partial_update_by_name<'a>(
    table_upsert: ResourceArc<TableUpsertResource>,
    column_names: Term<'a>,
) -> Result<ResourceArc<TableUpsertResource>, rustler::Error> {
    let names: Vec<String> =
        row_convert::decode_list(column_names, "expected a list of column names")
            .map_err(client_err)?
            .iter()
            .map(|name| row_convert::term_to_column_name(*name))
            .collect::<Result<_, String>>()
            .map_err(client_err)?;
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();

    // Core validates first (its errors name the table); the names are then resolved again
    // because core keeps its indices private, in caller order, which a list is matched against.
    let inner = table_upsert
        .inner
        .partial_update_with_column_names(&name_refs)
        .map_err(to_nif_err)?;

    let row_type = table_upsert.table_info.row_type();
    let target_indices = name_refs
        .iter()
        .map(|name| {
            row_type
                .get_field_index(name)
                .ok_or_else(|| format!("unknown column `{name}`"))
        })
        .collect::<Result<Vec<usize>, String>>()
        .map_err(client_err)?;
    row_convert::check_distinct_columns(table_upsert.table_info.schema.columns(), &target_indices)
        .map_err(client_err)?;

    Ok(ResourceArc::new(TableUpsertResource {
        inner,
        table_info: table_upsert.table_info.clone(),
        target_indices: Some(target_indices),
    }))
}

#[rustler::nif]
fn table_upsert_partial_update_by_index<'a>(
    table_upsert: ResourceArc<TableUpsertResource>,
    column_indices: Term<'a>,
) -> Result<ResourceArc<TableUpsertResource>, rustler::Error> {
    // Decoded here so bad input is an error result; Vec<usize> would raise.
    let column_indices =
        row_convert::decode_list(column_indices, "expected a list of column indices")
            .map_err(client_err)?
            .iter()
            .map(|idx| {
                idx.decode::<usize>()
                    .map_err(|_| format!("invalid target column index `{idx:?}`"))
            })
            .collect::<Result<Vec<usize>, String>>()
            .map_err(client_err)?;
    let inner = table_upsert
        .inner
        .partial_update(Some(column_indices.clone()))
        .map_err(to_nif_err)?;
    row_convert::check_distinct_columns(table_upsert.table_info.schema.columns(), &column_indices)
        .map_err(client_err)?;

    Ok(ResourceArc::new(TableUpsertResource {
        inner,
        table_info: table_upsert.table_info.clone(),
        target_indices: Some(column_indices),
    }))
}

#[rustler::nif]
fn table_upsert_create_writer(
    table_upsert: ResourceArc<TableUpsertResource>,
) -> Result<ResourceArc<UpsertWriterResource>, rustler::Error> {
    // Partial update constraints are checked here, not when the target columns were named.
    let inner = table_upsert.inner.create_writer().map_err(to_nif_err)?;

    Ok(ResourceArc::new(UpsertWriterResource {
        inner,
        table_info: table_upsert.table_info.clone(),
        target_indices: table_upsert.target_indices.clone(),
    }))
}
