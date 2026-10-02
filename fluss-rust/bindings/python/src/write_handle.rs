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
use std::sync::Mutex;

/// Handle for a pending write operation.
///
/// Returned by `upsert()`, `delete()`, `append()`, etc.
/// Can be safely ignored for fire-and-forget semantics,
/// or awaited via `wait()` for per-record acknowledgment.
///
/// # Example:
///     # Fire-and-forget — just ignore the handle
///     writer.upsert(row1)
///     writer.upsert(row2)
///     await writer.flush()
///
///     # Per-record ack — call wait()
///     handle = writer.upsert(critical_row)
///     await handle.wait()
#[pyclass]
pub struct WriteResultHandle {
    inner: Mutex<Option<fcore::client::WriteResultFuture>>,
}

impl WriteResultHandle {
    pub fn new(future: fcore::client::WriteResultFuture) -> Self {
        Self {
            inner: Mutex::new(Some(future)),
        }
    }
}

#[pymethods]
impl WriteResultHandle {
    /// Wait for server acknowledgment of this specific write.
    ///
    /// Returns:
    ///     None on success, raises FlussError on failure.
    pub fn wait<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let future = self
            .inner
            .lock()
            .map_err(|e| FlussError::new_err(format!("Lock poisoned: {e}")))?
            .take()
            .ok_or_else(|| FlussError::new_err("WriteResultHandle already consumed"))?;

        future_into_py(py, async move {
            future.await.map_err(|e| FlussError::from_core_error(&e))?;
            Ok(())
        })
    }

    fn __repr__(&self) -> String {
        let consumed = self.inner.lock().map(|g| g.is_none()).unwrap_or(false);
        if consumed {
            "WriteResultHandle(consumed)".to_string()
        } else {
            "WriteResultHandle(pending)".to_string()
        }
    }
}
