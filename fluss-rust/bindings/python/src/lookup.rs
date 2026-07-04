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

use crate::table::{internal_row_to_dict, python_to_dense_generic_row};
use crate::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Lookuper for performing primary key lookups on a Fluss table.
///
/// The Lookuper caches key encoders and bucketing functions, making
/// repeated lookups efficient. Create once and reuse for multiple lookups.
///
/// # Example:
///     lookuper = table.new_lookup().create_lookuper()
///     result = await lookuper.lookup({"user_id": 1})
///     result2 = await lookuper.lookup({"user_id": 2})  # Reuses cached encoders
#[pyclass]
pub struct Lookuper {
    inner: Arc<Mutex<fcore::client::Lookuper>>,
    table_info: Arc<fcore::metadata::TableInfo>,
}

#[pymethods]
impl Lookuper {
    /// Lookup a row by its primary key.
    ///
    /// Args:
    ///     pk: A dict, list, or tuple containing only the primary key values.
    ///         For dict: keys are PK column names.
    ///         For list/tuple: values in PK column order.
    ///
    /// Returns:
    ///     A dict containing the row data if found, None otherwise.
    pub fn lookup<'py>(
        &self,
        py: Python<'py>,
        pk: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pk_indices = self.table_info.get_schema().primary_key_indexes();
        let generic_row = python_to_dense_generic_row(pk, &self.table_info, &pk_indices)?;
        let inner = self.inner.clone();
        let table_info = self.table_info.clone();

        future_into_py(py, async move {
            // Perform async lookup
            let result = {
                let mut lookuper = inner.lock().await;
                lookuper
                    .lookup(&generic_row)
                    .await
                    .map_err(|e| FlussError::from_core_error(&e))?
            };

            // Extract row data
            let row_opt = result
                .get_single_row()
                .map_err(|e| FlussError::from_core_error(&e))?;

            // Convert to Python with GIL
            Python::attach(|py| match row_opt {
                Some(row) => internal_row_to_dict(py, &row, &table_info),
                None => Ok(py.None()),
            })
        })
    }

    fn __repr__(&self) -> String {
        "Lookuper()".to_string()
    }
}

impl Lookuper {
    /// Create a Lookuper from connection components.
    ///
    /// This creates the core Lookuper which caches encoders and bucketing functions.
    pub fn new(
        connection: &Arc<fcore::client::FlussConnection>,
        metadata: Arc<fcore::client::Metadata>,
        table_info: fcore::metadata::TableInfo,
    ) -> PyResult<Self> {
        // Run inside tokio runtime context because new_lookup()
        // spawns a background task via tokio::spawn() in LookupClient::new().
        let lookuper = TOKIO_RUNTIME.block_on(async {
            let fluss_table =
                fcore::client::FlussTable::new(connection, metadata, table_info.clone());
            let table_lookup = fluss_table
                .new_lookup()
                .map_err(|e| FlussError::from_core_error(&e))?;
            table_lookup
                .create_lookuper()
                .map_err(|e| FlussError::from_core_error(&e))
        })?;

        Ok(Self {
            inner: Arc::new(Mutex::new(lookuper)),
            table_info: Arc::new(table_info),
        })
    }
}

/// Lookuper for performing prefix key lookups on a Fluss table.
///
/// Returns all rows whose primary key starts with the given prefix.
/// Create once via `table.new_lookup().lookup_by(columns).create_lookuper()`
/// and reuse for multiple lookups.
#[pyclass]
pub struct PrefixLookuper {
    inner: Arc<Mutex<fcore::client::PrefixKeyLookuper>>,
    table_info: Arc<fcore::metadata::TableInfo>,
    lookup_column_indices: Vec<usize>,
}

#[pymethods]
impl PrefixLookuper {
    /// Lookup all rows matching a prefix key.
    ///
    /// Args:
    ///     prefix: A dict, list, or tuple containing only the prefix key values
    ///         (the columns specified in lookup_by()).
    ///         For dict: keys are prefix column names.
    ///         For list/tuple: values in prefix column order.
    ///
    /// Returns:
    ///     A list of dicts, each containing the full row data. Empty list if no matches.
    pub fn lookup<'py>(
        &self,
        py: Python<'py>,
        prefix: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let generic_row =
            python_to_dense_generic_row(prefix, &self.table_info, &self.lookup_column_indices)?;
        let inner = self.inner.clone();
        let table_info = self.table_info.clone();

        future_into_py(py, async move {
            let result = {
                let mut lookuper = inner.lock().await;
                lookuper
                    .lookup(&generic_row)
                    .await
                    .map_err(|e| FlussError::from_core_error(&e))?
            };

            let rows = result
                .get_rows()
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let py_rows: Vec<Py<PyAny>> = rows
                    .iter()
                    .map(|row| internal_row_to_dict(py, row, &table_info))
                    .collect::<PyResult<_>>()?;
                Ok(py_rows)
            })
        })
    }

    fn __repr__(&self) -> String {
        "PrefixLookuper()".to_string()
    }
}

impl PrefixLookuper {
    pub fn new(
        connection: &Arc<fcore::client::FlussConnection>,
        metadata: Arc<fcore::client::Metadata>,
        table_info: fcore::metadata::TableInfo,
        lookup_column_names: Vec<String>,
    ) -> PyResult<Self> {
        let row_type = table_info.row_type();
        let lookup_column_indices: Vec<usize> = lookup_column_names
            .iter()
            .map(|name| {
                row_type.get_field_index(name).ok_or_else(|| {
                    FlussError::new_err(format!("Unknown column name '{name}' for prefix lookup"))
                })
            })
            .collect::<PyResult<_>>()?;

        let lookuper = TOKIO_RUNTIME.block_on(async {
            let fluss_table =
                fcore::client::FlussTable::new(connection, metadata, table_info.clone());
            let table_lookup = fluss_table
                .new_lookup()
                .map_err(|e| FlussError::from_core_error(&e))?;
            table_lookup
                .lookup_by(lookup_column_names)
                .create_lookuper()
                .map_err(|e| FlussError::from_core_error(&e))
        })?;

        Ok(Self {
            inner: Arc::new(Mutex::new(lookuper)),
            table_info: Arc::new(table_info),
            lookup_column_indices,
        })
    }
}
