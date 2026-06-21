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

use crate::table::{python_to_generic_row, python_to_sparse_generic_row};
use crate::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

/// Writer for upserting and deleting data in a Fluss primary key table.
///
/// Each upsert/delete operation synchronously queues the write. Call `flush()`
/// to ensure all queued writes are delivered to the server.
///
/// # Example:
///     writer = table.new_upsert().create_writer()
///
///     # Fire-and-forget — ignore the returned handle
///     writer.upsert(row1)
///     writer.upsert(row2)
///     await writer.flush()
///
///     # Per-record ack — call wait() on the handle
///     handle = writer.upsert(critical_row)
///     await handle.wait()
#[pyclass]
pub struct UpsertWriter {
    writer: Arc<fcore::client::UpsertWriter>,
    table_info: fcore::metadata::TableInfo,
    /// Column indices for partial updates (None = full row)
    target_columns: Option<Vec<usize>>,
}

#[pymethods]
impl UpsertWriter {
    /// Upsert a row into the table.
    ///
    /// If a row with the same primary key exists, it will be updated.
    /// Otherwise, a new row will be inserted.
    ///
    /// The write is queued synchronously. Call `flush()` to ensure delivery.
    ///
    /// Args:
    ///     row: A dict, list, or tuple containing the row data.
    ///          For dict: keys are column names, values are column values.
    ///          For list/tuple: values must be in schema order.
    pub fn upsert(&self, row: &Bound<'_, PyAny>) -> PyResult<WriteResultHandle> {
        let generic_row = if let Some(target_cols) = &self.target_columns {
            python_to_sparse_generic_row(row, &self.table_info, target_cols)?
        } else {
            python_to_generic_row(row, &self.table_info)?
        };

        let result_future = self
            .writer
            .upsert(&generic_row)
            .map_err(|e| FlussError::from_core_error(&e))?;
        Ok(WriteResultHandle::new(result_future))
    }

    /// Delete a row from the table by primary key.
    ///
    /// The delete is queued synchronously. Call `flush()` to ensure delivery.
    ///
    /// Args:
    ///     pk: A dict, list, or tuple containing only the primary key values.
    ///         For dict: keys are PK column names.
    ///         For list/tuple: values in PK column order.
    pub fn delete(&self, pk: &Bound<'_, PyAny>) -> PyResult<WriteResultHandle> {
        let pk_indices = self.table_info.get_schema().primary_key_indexes();
        let generic_row = python_to_sparse_generic_row(pk, &self.table_info, &pk_indices)?;

        let result_future = self
            .writer
            .delete(&generic_row)
            .map_err(|e| FlussError::from_core_error(&e))?;
        Ok(WriteResultHandle::new(result_future))
    }

    /// Flush all pending upsert/delete operations to the server.
    ///
    /// This method sends all buffered operations and waits until they are
    /// acknowledged according to the writer's ack configuration.
    ///
    /// Returns:
    ///     None on success
    pub fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let writer = self.writer.clone();

        future_into_py(py, async move {
            writer
                .flush()
                .await
                .map_err(|e| FlussError::from_core_error(&e))
        })
    }

    // Enter the async runtime context (for 'async with' statement)
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let py_slf = slf.into_pyobject(py)?.unbind();
        future_into_py(py, async move { Ok(py_slf) })
    }

    // Exit the async runtime context (for 'async with' statement)
    /// On exit, the writer is automatically flushed.
    #[pyo3(signature = (exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<Bound<'py, PyAny>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let writer = self.writer.clone();
        let is_exc_none = exc_type.as_ref().is_none_or(|e| e.is_none());
        future_into_py(py, async move {
            let res = writer.flush().await;
            if let Err(e) = res {
                if is_exc_none {
                    return Err(FlussError::from_core_error(&e));
                }
            }
            Ok(false)
        })
    }

    fn __repr__(&self) -> String {
        "UpsertWriter()".to_string()
    }
}

impl UpsertWriter {
    /// Create an UpsertWriter by eagerly creating the core writer from a TableUpsert.
    pub fn new(
        table_upsert: &fcore::client::TableUpsert,
        table_info: fcore::metadata::TableInfo,
        target_columns: Option<Vec<usize>>,
    ) -> PyResult<Self> {
        let writer = table_upsert
            .create_writer()
            .map_err(|e| FlussError::from_core_error(&e))?;
        Ok(Self {
            writer: Arc::new(writer),
            table_info,
            target_columns,
        })
    }
}
