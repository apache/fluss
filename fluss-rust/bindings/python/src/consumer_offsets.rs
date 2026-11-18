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

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct OffsetsInitializer {
    pub(crate) inner: fcore::client::OffsetsInitializer,
}
#[pymethods]
impl OffsetsInitializer {
    #[staticmethod]
    fn earliest() -> Self {
        Self {
            inner: fcore::client::OffsetsInitializer::earliest(),
        }
    }
    #[staticmethod]
    fn latest() -> Self {
        Self {
            inner: fcore::client::OffsetsInitializer::latest(),
        }
    }
    #[staticmethod]
    fn timestamp(timestamp_ms: i64) -> Self {
        Self {
            inner: fcore::client::OffsetsInitializer::timestamp(timestamp_ms),
        }
    }
    #[staticmethod]
    fn offset(offset: i64) -> Self {
        Self {
            inner: fcore::client::OffsetsInitializer::offset(offset),
        }
    }
    #[staticmethod]
    fn fail() -> Self {
        Self {
            inner: fcore::client::OffsetsInitializer::fail(),
        }
    }
    #[staticmethod]
    #[pyo3(signature=(group_id, fallback=None))]
    fn committed(group_id: String, fallback: Option<Self>) -> Self {
        Self {
            inner: fcore::client::OffsetsInitializer::committed_with_fallback(
                group_id,
                fallback.unwrap_or_else(Self::earliest).inner,
            ),
        }
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

pub(crate) enum MultiKind {
    Record(fcore::client::MultiTableLogScanner),
    Batch(fcore::client::MultiTableRecordBatchLogScanner),
}
#[pyclass]
pub struct MultiTableLogScanner {
    pub(crate) inner: Arc<MultiKind>,
    pub(crate) schemas: Arc<HashMap<i64, fcore::metadata::RowType>>,
}
#[pymethods]
impl MultiTableLogScanner {
    /// Resolves every selected bucket before assignment. Other subscriptions are retained.
    fn subscribe_with_initializer<'py>(
        &self,
        py: Python<'py>,
        buckets: Vec<TableBucket>,
        initializer: OffsetsInitializer,
    ) -> PyResult<Bound<'py, PyAny>> {
        let scanner = self.inner.clone();
        let buckets: Vec<_> = buckets.iter().map(TableBucket::to_core).collect();
        future_into_py(py, async move {
            match scanner.as_ref() {
                MultiKind::Record(s) => {
                    s.subscribe_with_initializer(&buckets, &initializer.inner)
                        .await
                }
                MultiKind::Batch(s) => {
                    s.subscribe_with_initializer(&buckets, &initializer.inner)
                        .await
                }
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
        let schemas = self.schemas.clone();
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
                for (bucket, records) in records.into_records_by_buckets() {
                    let schema = schemas
                        .get(&bucket.table_id())
                        .ok_or_else(|| FlussError::new_err("Unknown source table"))?;
                    let values = records
                        .iter()
                        .map(|record| {
                            ScanRecord::from_core(py, record, schema)
                                .and_then(|record| Py::new(py, record))
                        })
                        .collect::<PyResult<Vec<_>>>()?;
                    records_by_bucket.insert(TableBucket::from_core(bucket), values);
                }
                Ok(ScanRecords {
                    records_by_bucket,
                    total_count,
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
    }
    /// Enqueues immediately; cancelling the returned awaitable does not cancel the commit.
    #[pyo3(signature=(offsets=None))]
    fn commit_async<'py>(
        &self,
        py: Python<'py>,
        offsets: Option<Vec<(TableBucket, OffsetAndMetadata)>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let handle = self.enqueue(offsets)?;
        future_into_py(py, async move {
            Ok(commit_result(
                handle.await.unwrap_or_else(|error| Err(error.into())),
            ))
        })
    }
    /// Blocks with the GIL released. Inspect request and bucket outcomes before shutdown.
    #[pyo3(signature=(offsets=None))]
    fn commit_sync(
        &self,
        py: Python<'_>,
        offsets: Option<Vec<(TableBucket, OffsetAndMetadata)>>,
    ) -> PyResult<OffsetCommitResult> {
        let handle = self.enqueue(offsets)?;
        Ok(py.detach(|| {
            commit_result(
                TOKIO_RUNTIME
                    .block_on(handle)
                    .unwrap_or_else(|error| Err(error.into())),
            )
        }))
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
