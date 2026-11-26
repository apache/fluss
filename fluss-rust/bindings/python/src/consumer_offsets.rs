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
use fcore::client::{OffsetCommitError as CoreCommitError, OffsetCommitOutcome};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Saved next offset and optional metadata. None differs from an empty string.
#[pyclass(from_py_object, get_all)]
#[derive(Clone)]
pub struct OffsetAndMetadata {
    pub offset: i64,
    pub metadata: Option<String>,
}
#[pymethods]
impl OffsetAndMetadata {
    #[new]
    #[pyo3(signature=(offset, metadata=None))]
    fn new(offset: i64, metadata: Option<String>) -> Self {
        Self { offset, metadata }
    }
}

/// status is success, rejected, unknown, or needs_recovery. Check every bucket as well.
#[pyclass(from_py_object, get_all)]
#[derive(Clone)]
pub struct OffsetOperationOutcome {
    pub status: String,
    pub error_code: Option<i32>,
    pub message: String,
}
impl OffsetOperationOutcome {
    pub(crate) fn success() -> Self {
        Self {
            status: "success".into(),
            error_code: None,
            message: String::new(),
        }
    }
    pub(crate) fn error(error: CoreCommitError) -> Self {
        let status = match &error {
            CoreCommitError::Rejected(_) => "rejected",
            CoreCommitError::Unknown(_) => "unknown",
            CoreCommitError::NeedsRecovery => "needs_recovery",
        };
        let error_code = match &error {
            CoreCommitError::Rejected(fcore::error::Error::FlussAPIError { api_error })
            | CoreCommitError::Unknown(fcore::error::Error::FlussAPIError { api_error }) => {
                Some(api_error.code)
            }
            _ => None,
        };
        Self {
            status: status.into(),
            error_code,
            message: error.to_string(),
        }
    }
}
#[pyclass(get_all)]
pub struct OffsetCommitResult {
    pub request: OffsetOperationOutcome,
    pub buckets: HashMap<TableBucket, OffsetOperationOutcome>,
}

pub(crate) fn values(
    offsets: Vec<(TableBucket, OffsetAndMetadata)>,
) -> PyResult<HashMap<fcore::metadata::TableBucket, fcore::client::OffsetAndMetadata>> {
    let mut result = HashMap::new();
    for (bucket, offset) in offsets {
        if result
            .insert(
                bucket.to_core(),
                fcore::client::OffsetAndMetadata {
                    offset: offset.offset,
                    metadata: offset.metadata,
                },
            )
            .is_some()
        {
            return Err(FlussError::new_err("Duplicate consumer offset bucket"));
        }
    }
    Ok(result)
}

pub(crate) fn commit_result(
    result: Result<fcore::client::OffsetCommitResult, CoreCommitError>,
) -> OffsetCommitResult {
    match result {
        Err(error) => OffsetCommitResult {
            request: OffsetOperationOutcome::error(error),
            buckets: HashMap::new(),
        },
        Ok(results) => OffsetCommitResult {
            request: OffsetOperationOutcome::success(),
            buckets: results
                .into_iter()
                .map(|(bucket, result)| {
                    (
                        TableBucket::from_core(bucket),
                        match result {
                            OffsetCommitOutcome::Committed => OffsetOperationOutcome::success(),
                            OffsetCommitOutcome::Rejected(error) => OffsetOperationOutcome {
                                status: "rejected".into(),
                                error_code: Some(error.code),
                                message: error.message,
                            },
                            OffsetCommitOutcome::Unknown(error) => OffsetOperationOutcome {
                                status: "unknown".into(),
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

pub(crate) fn delete_result(
    result: Result<fcore::client::OffsetDeleteResult, CoreCommitError>,
) -> OffsetCommitResult {
    match result {
        Err(error) => OffsetCommitResult {
            request: OffsetOperationOutcome::error(error),
            buckets: HashMap::new(),
        },
        Ok(results) => OffsetCommitResult {
            request: OffsetOperationOutcome::success(),
            buckets: results
                .into_iter()
                .map(|(bucket, result)| {
                    (
                        TableBucket::from_core(bucket),
                        result.map_or_else(OffsetOperationOutcome::error, |_| {
                            OffsetOperationOutcome::success()
                        }),
                    )
                })
                .collect(),
        },
    }
}

pub(crate) fn saved(
    offsets: HashMap<fcore::metadata::TableBucket, Option<fcore::client::OffsetAndMetadata>>,
) -> HashMap<TableBucket, Option<OffsetAndMetadata>> {
    offsets
        .into_iter()
        .map(|(bucket, value)| {
            (
                TableBucket::from_core(bucket),
                value.map(|value| OffsetAndMetadata {
                    offset: value.offset,
                    metadata: value.metadata,
                }),
            )
        })
        .collect()
}

pub(crate) fn enqueue(
    kind: &crate::table::ScannerKind,
    offsets: Option<Vec<(TableBucket, OffsetAndMetadata)>>,
) -> PyResult<fcore::client::OffsetCommitHandle> {
    let _guard = TOKIO_RUNTIME.enter();
    let offsets = offsets.map(values).transpose()?;
    let result = match kind {
        crate::table::ScannerKind::Record(scanner) => match offsets {
            Some(offsets) => scanner.commit_offsets_async(offsets),
            None => scanner.commit_async(),
        },
        crate::table::ScannerKind::Batch(scanner) => match offsets {
            Some(offsets) => scanner.commit_offsets_async(offsets),
            None => scanner.commit_async(),
        },
    };
    result.map_err(|error| FlussError::from_core_error(&error))
}

pub(crate) struct PollDelivery(Option<Py<PyAny>>);

impl PollDelivery {
    pub(crate) fn new(enabled: bool) -> PyResult<Self> {
        Python::attach(|py| {
            Ok(Self(if enabled {
                Some(
                    py.import("fluss._delivery")?
                        .getattr("PollDelivery")?
                        .call0()?
                        .unbind(),
                )
            } else {
                None
            }))
        })
    }

    pub(crate) fn check(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(state) = &self.0 {
            state.bind(py).call_method0("check")?;
        }
        Ok(())
    }

    pub(crate) fn poll<'py>(
        &self,
        py: Python<'py>,
        start: impl FnOnce() -> PyResult<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Some(state) = &self.0 else { return start() };
        let state = state.bind(py);
        state.call_method0("begin")?;
        state.call_method1("deliver", (start()?,))
    }
}

pub(crate) enum MultiKind {
    Record(fcore::client::MultiTableLogScanner),
    Batch(fcore::client::MultiTableRecordBatchLogScanner),
}
#[pyclass]
pub struct MultiTableLogScanner {
    pub(crate) delivery: PollDelivery,
    pub(crate) inner: Arc<MultiKind>,
}
#[pymethods]
impl MultiTableLogScanner {
    /// Resume this group on poll; a bucket with no saved position starts at earliest.
    fn subscribe_from_committed(&self, buckets: Vec<TableBucket>) -> PyResult<()> {
        let buckets: Vec<_> = buckets.iter().map(TableBucket::to_core).collect();
        match self.inner.as_ref() {
            MultiKind::Record(s) => s.subscribe_from_committed(&buckets),
            MultiKind::Batch(s) => s.subscribe_from_committed(&buckets),
        }
        .map_err(|error| FlussError::from_core_error(&error))
    }
    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        bucket: TableBucket,
        offset: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scanner = self.inner.clone();
        future_into_py(py, async move {
            let bucket = bucket.to_core();
            match scanner.as_ref() {
                MultiKind::Record(s) => s.subscribe(&bucket, offset).await,
                MultiKind::Batch(s) => s.subscribe(&bucket, offset).await,
            }
            .map_err(|error| FlussError::from_core_error(&error))
        })
    }
    fn unsubscribe<'py>(
        &self,
        py: Python<'py>,
        bucket: TableBucket,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scanner = self.inner.clone();
        future_into_py(py, async move {
            match scanner.as_ref() {
                MultiKind::Record(s) => s.unsubscribe(&bucket.to_core()).await,
                MultiKind::Batch(s) => s.unsubscribe(&bucket.to_core()).await,
            }
            .map_err(|error| FlussError::from_core_error(&error))
        })
    }
    /// Process the previous result completely before polling with auto-commit enabled.
    fn poll<'py>(&self, py: Python<'py>, timeout_ms: u64) -> PyResult<Bound<'py, PyAny>> {
        let scanner = self.inner.clone();
        self.delivery.poll(py, || {
            future_into_py(py, async move {
                let MultiKind::Record(scanner) = scanner.as_ref() else {
                    return Err(FlussError::new_err(
                        "Use poll_record_batch for an Arrow scanner",
                    ));
                };
                let records = scanner
                    .poll(Duration::from_millis(timeout_ms))
                    .await
                    .map_err(|error| FlussError::from_core_error(&error))?;
                Python::attach(|py| {
                    let mut records_by_bucket = indexmap::IndexMap::new();
                    let total_count = records.count();
                    let buckets = records.into_records_by_buckets();
                    if let Some(first) = buckets.keys().next() {
                        let info = scanner
                            .registered_table_info(first.table_id())
                            .ok_or_else(|| FlussError::new_err("Unknown source table"))?;
                        for (bucket, records) in buckets {
                            let values = records
                                .iter()
                                .map(|record| {
                                    ScanRecord::from_core(py, record, info.get_row_type())
                                        .and_then(|record| Py::new(py, record))
                                })
                                .collect::<PyResult<Vec<_>>>()?;
                            records_by_bucket.insert(TableBucket::from_core(bucket), values);
                        }
                    }
                    Ok(ScanRecords {
                        records_by_bucket,
                        total_count,
                    })
                })
            })
        })
    }
    /// Returns separate batches with source table buckets and individual Arrow schemas.
    fn poll_record_batch<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scanner = self.inner.clone();
        self.delivery.poll(py, || {
            future_into_py(py, async move {
                let MultiKind::Batch(scanner) = scanner.as_ref() else {
                    return Err(FlussError::new_err("Use poll for a record scanner"));
                };
                let batches = scanner
                    .poll(Duration::from_millis(timeout_ms))
                    .await
                    .map_err(|error| FlussError::from_core_error(&error))?;
                Python::attach(|py| {
                    batches
                        .into_iter()
                        .map(|batch| Py::new(py, RecordBatch::from_scan_batch(batch)))
                        .collect::<PyResult<Vec<_>>>()
                })
            })
        })
    }
    /// Enqueues immediately; cancelling the returned awaitable does not cancel the commit.
    #[pyo3(signature=(offsets=None))]
    fn commit_async<'py>(
        &self,
        py: Python<'py>,
        offsets: Option<Vec<(TableBucket, OffsetAndMetadata)>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.delivery.check(py)?;
        let handle = self.enqueue(offsets)?;
        future_into_py(py, async move { Ok(commit_result(handle.await)) })
    }
    /// Blocks with the GIL released. Inspect request and bucket outcomes before shutdown.
    #[pyo3(signature=(offsets=None))]
    fn commit_sync(
        &self,
        py: Python<'_>,
        offsets: Option<Vec<(TableBucket, OffsetAndMetadata)>>,
    ) -> PyResult<OffsetCommitResult> {
        self.delivery.check(py)?;
        let handle = self.enqueue(offsets)?;
        Ok(py.detach(|| commit_result(TOKIO_RUNTIME.block_on(handle))))
    }
}
impl MultiTableLogScanner {
    fn enqueue(
        &self,
        offsets: Option<Vec<(TableBucket, OffsetAndMetadata)>>,
    ) -> PyResult<fcore::client::OffsetCommitHandle> {
        let _guard = TOKIO_RUNTIME.enter();
        let offsets = offsets.map(values).transpose()?;
        match self.inner.as_ref() {
            MultiKind::Record(scanner) => match offsets {
                Some(values) => scanner.commit_offsets_async(values),
                None => scanner.commit_async(),
            },
            MultiKind::Batch(scanner) => match offsets {
                Some(values) => scanner.commit_offsets_async(values),
                None => scanner.commit_async(),
            },
        }
        .map_err(|error| FlussError::from_core_error(&error))
    }
}
