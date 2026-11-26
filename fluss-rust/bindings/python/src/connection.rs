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

use crate::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use std::time::Duration;

/// Connection to a Fluss cluster
#[pyclass]
pub struct FlussConnection {
    inner: Arc<fcore::client::FlussConnection>,
}

#[pymethods]
impl FlussConnection {
    /// Create a new FlussConnection (async)
    #[staticmethod]
    fn create<'py>(py: Python<'py>, config: &Config) -> PyResult<Bound<'py, PyAny>> {
        let rust_config = config.get_core_config();

        future_into_py(py, async move {
            let connection = fcore::client::FlussConnection::new(rust_config)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            let py_connection = FlussConnection {
                inner: Arc::new(connection),
            };

            Python::attach(|py| Py::new(py, py_connection))
        })
    }

    /// Creates a manually assigned multi-table record or Arrow scanner with one commit queue.
    #[pyo3(signature=(tables, group_id, timeout_ms=30000, auto_commit_interval_ms=None, arrow=false))]
    fn create_multi_table_scanner<'py>(
        &self,
        py: Python<'py>,
        tables: Vec<TablePath>,
        group_id: String,
        timeout_ms: u64,
        auto_commit_interval_ms: Option<u64>,
        arrow: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let connection = self.inner.clone();
        let tables: Vec<_> = tables.iter().map(TablePath::to_core).collect();
        future_into_py(py, async move {
            let options = fcore::client::ConsumerGroupOptions {
                group_id,
                timeout: Duration::from_millis(timeout_ms),
            };
            let mut scan = connection
                .new_multi_table_scan(&tables, options)
                .await
                .map_err(|error| FlussError::from_core_error(&error))?;
            if auto_commit_interval_ms == Some(0) {
                scan = scan.without_auto_commit();
            } else if let Some(ms) = auto_commit_interval_ms {
                scan = scan
                    .with_auto_commit(Duration::from_millis(ms))
                    .map_err(|error| FlussError::from_core_error(&error))?;
            }
            let inner = if arrow {
                crate::consumer_offsets::MultiKind::Batch(
                    scan.create_record_batch_log_scanner()
                        .map_err(|error| FlussError::from_core_error(&error))?,
                )
            } else {
                crate::consumer_offsets::MultiKind::Record(
                    scan.create_log_scanner()
                        .map_err(|error| FlussError::from_core_error(&error))?,
                )
            };
            Ok(MultiTableLogScanner {
                delivery: crate::consumer_offsets::PollDelivery::new(true)?,
                inner: Arc::new(inner),
            })
        })
    }

    /// Get admin interface
    fn get_admin(&self, py: Python<'_>) -> PyResult<Py<FlussAdmin>> {
        let admin = self
            .inner
            .get_admin()
            .map_err(|e| FlussError::from_core_error(&e))?;

        Py::new(py, FlussAdmin::from_core(admin))
    }

    /// Get a table
    fn get_table<'py>(
        &self,
        py: Python<'py>,
        table_path: &TablePath,
    ) -> PyResult<Bound<'py, PyAny>> {
        let client = self.inner.clone();
        let core_path = table_path.to_core().clone();

        future_into_py(py, async move {
            let core_table = client
                .get_table(&core_path)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            let py_table = FlussTable::new_table(
                client.clone(),
                core_table.metadata().clone(),
                core_table.get_table_info().clone(),
                core_table.table_path().clone(),
                core_table.has_primary_key(),
            );

            Python::attach(|py| Py::new(py, py_table))
        })
    }

    /// Close the connection (async).
    ///
    /// Gracefully shuts down the connection by draining any pending write batches.
    /// This method is awaitable.
    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();

        future_into_py(py, async move {
            inner
                .close(Duration::MAX)
                .await
                .map_err(|e| FlussError::from_core_error(&e))
        })
    }

    // Enter the runtime context (for 'with' statement)
    fn __enter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    // Exit the runtime context (for 'with' statement)
    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &mut self,
        _exc_type: Option<Bound<'_, PyAny>>,
        _exc_value: Option<Bound<'_, PyAny>>,
        _traceback: Option<Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        // Sync exit cannot await the graceful drain, so it's a no-op here.
        // Users should use 'async with' for graceful shutdown.
        Ok(false)
    }

    // Enter the async runtime context (for 'async with' statement)
    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let py_slf = slf.into_pyobject(py)?.unbind();
        future_into_py(py, async move { Ok(py_slf) })
    }

    // Exit the async runtime context (for 'async with' statement)
    #[pyo3(signature = (exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<Bound<'py, PyAny>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        let is_exc_none = exc_type.as_ref().is_none_or(|e| e.is_none());
        future_into_py(py, async move {
            let res = inner.close(Duration::MAX).await;
            if let Err(e) = res {
                if is_exc_none {
                    return Err(FlussError::from_core_error(&e));
                }
            }
            Ok(false)
        })
    }

    fn __repr__(&self) -> String {
        "FlussConnection()".to_string()
    }
}
