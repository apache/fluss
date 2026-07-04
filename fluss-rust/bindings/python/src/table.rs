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

use crate::TOKIO_RUNTIME;
use crate::*;
use arrow::array::RecordBatch as ArrowRecordBatch;
use arrow::record_batch::RecordBatchReader as _;
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use arrow_schema::SchemaRef;
use fcore::client::LimitBatchScanner;
use fcore::metadata::{DataField, DataType, MapType, RowType};
use fcore::row::binary_array::{FlussArray, FlussArrayWriter};
use fcore::row::binary_map::{FlussMap, FlussMapWriter};
use fcore::row::{Datum, F32, F64, GenericRow, InternalRow};
use fluss::record::to_arrow_schema;
use indexmap::IndexMap;
use pyo3::IntoPyObjectExt;
use pyo3::exceptions::{PyIndexError, PyRuntimeError, PyTypeError};
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    IntoPyDict, PyBool, PyByteArray, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta,
    PyDeltaAccess, PyDict, PyList, PySequence, PySlice, PyString, PyTime, PyTimeAccess, PyTuple,
    PyType, PyTzInfo,
};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

// Time conversion constants
const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60_000;
const MILLIS_PER_HOUR: i64 = 3_600_000;
const MICROS_PER_MILLI: i64 = 1_000;
const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_DAY: i64 = 86_400_000_000;
const NANOS_PER_MILLI: i64 = 1_000_000;
const NANOS_PER_MICRO: i64 = 1_000;
const DEFAULT_POLL_INTERVAL_MS: i64 = 1000;

/// Represents a single scan record with metadata.
///
/// Matches Rust/Java: offset, timestamp, change_type, row.
/// The bucket is the key in ScanRecords, not on the individual record.
#[pyclass]
pub struct ScanRecord {
    #[pyo3(get)]
    offset: i64,
    #[pyo3(get)]
    timestamp: i64,
    #[pyo3(get)]
    change_type: ChangeType,
    /// Store row as a Python dict directly
    row_dict: Py<PyDict>,
}

#[pymethods]
impl ScanRecord {
    /// Get the row data as a dictionary
    #[getter]
    pub fn row(&self, py: Python) -> Py<PyDict> {
        self.row_dict.clone_ref(py)
    }

    fn __str__(&self) -> String {
        format!(
            "ScanRecord(offset={}, timestamp={}, change_type={})",
            self.offset,
            self.timestamp,
            self.change_type.short_string()
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl ScanRecord {
    /// Create a ScanRecord from core types
    pub fn from_core(
        py: Python,
        record: &fcore::record::ScanRecord,
        row_type: &fcore::metadata::RowType,
    ) -> PyResult<Self> {
        let fields = row_type.fields();
        let row = record.row();
        let dict = PyDict::new(py);

        for (pos, field) in fields.iter().enumerate() {
            let value = datum_to_python_value(py, row, pos, field.data_type())?;
            dict.set_item(field.name(), value)?;
        }

        Ok(ScanRecord {
            offset: record.offset(),
            timestamp: record.timestamp(),
            change_type: ChangeType::from_core(*record.change_type()),
            row_dict: dict.unbind(),
        })
    }
}

/// Represents a batch of records with metadata
#[pyclass]
pub struct RecordBatch {
    batch: Arc<ArrowRecordBatch>,
    #[pyo3(get)]
    bucket: TableBucket,
    #[pyo3(get)]
    base_offset: i64,
    #[pyo3(get)]
    last_offset: i64,
}

#[pymethods]
impl RecordBatch {
    /// Get the Arrow RecordBatch as PyArrow RecordBatch
    #[getter]
    pub fn batch(&self, py: Python) -> PyResult<Py<PyAny>> {
        let pyarrow_batch = self
            .batch
            .as_ref()
            .to_pyarrow(py)
            .map_err(|e| FlussError::new_err(format!("Failed to convert batch: {e}")))?;
        Ok(pyarrow_batch.unbind())
    }

    fn __str__(&self) -> String {
        format!(
            "RecordBatch(bucket={}, base_offset={}, last_offset={}, rows={})",
            self.bucket.__str__(),
            self.base_offset,
            self.last_offset,
            self.batch.num_rows()
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }
}

impl RecordBatch {
    /// Create a RecordBatch from core ScanBatch
    pub fn from_scan_batch(scan_batch: fcore::record::ScanBatch) -> Self {
        RecordBatch {
            bucket: TableBucket::from_core(scan_batch.bucket().clone()),
            base_offset: scan_batch.base_offset(),
            last_offset: scan_batch.last_offset(),
            batch: Arc::new(scan_batch.into_batch()),
        }
    }
}

/// A collection of scan records grouped by bucket.
///
/// Returned by `LogScanner.poll()`. Records are grouped by `TableBucket`.
#[pyclass]
pub struct ScanRecords {
    records_by_bucket: IndexMap<TableBucket, Vec<Py<ScanRecord>>>,
    total_count: usize,
}

#[pymethods]
impl ScanRecords {
    /// List of distinct buckets that have records in this result.
    pub fn buckets(&self) -> Vec<TableBucket> {
        self.records_by_bucket.keys().cloned().collect()
    }

    /// Get records for a specific bucket.
    ///
    /// Returns an empty list if the bucket is not present (matches Rust/Java behavior).
    pub fn records(&self, py: Python, bucket: &TableBucket) -> Vec<Py<ScanRecord>> {
        self.records_by_bucket
            .get(bucket)
            .map(|recs| recs.iter().map(|r| r.clone_ref(py)).collect())
            .unwrap_or_default()
    }

    /// Total number of records across all buckets.
    pub fn count(&self) -> usize {
        self.total_count
    }

    /// Whether the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    fn __len__(&self) -> usize {
        self.total_count
    }

    /// Type-dispatched indexing:
    ///   records[0]       → ScanRecord (flat index)
    ///   records[-1]      → ScanRecord (negative index)
    ///   records[1:3]     → list[ScanRecord] (slice)
    ///   records[bucket]  → list[ScanRecord] (by bucket)
    fn __getitem__(&self, py: Python, key: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        // Try integer index first
        if let Ok(mut idx) = key.extract::<isize>() {
            let len = self.total_count as isize;
            if idx < 0 {
                idx += len;
            }
            if idx < 0 || idx >= len {
                return Err(PyIndexError::new_err(format!(
                    "index {idx} out of range for ScanRecords of size {len}"
                )));
            }
            let idx = idx as usize;
            let mut offset = 0;
            for recs in self.records_by_bucket.values() {
                if idx < offset + recs.len() {
                    return Ok(recs[idx - offset].clone_ref(py).into_any());
                }
                offset += recs.len();
            }
            return Err(PyRuntimeError::new_err(
                "internal error: total_count out of sync with records",
            ));
        }
        // Try slice
        if let Ok(slice) = key.downcast::<PySlice>() {
            let indices = slice.indices(self.total_count as isize)?;
            let mut result: Vec<Py<ScanRecord>> = Vec::new();
            let mut i = indices.start;
            while (indices.step > 0 && i < indices.stop) || (indices.step < 0 && i > indices.stop) {
                let idx = i as usize;
                let mut offset = 0;
                for recs in self.records_by_bucket.values() {
                    if idx < offset + recs.len() {
                        result.push(recs[idx - offset].clone_ref(py));
                        break;
                    }
                    offset += recs.len();
                }
                i += indices.step;
            }
            return Ok(result.into_pyobject(py).unwrap().into_any().unbind());
        }
        // Try TableBucket
        if let Ok(bucket) = key.extract::<TableBucket>() {
            let recs = self.records(py, &bucket);
            return Ok(recs.into_pyobject(py).unwrap().into_any().unbind());
        }
        Err(PyTypeError::new_err(
            "index must be int, slice, or TableBucket",
        ))
    }

    /// Support `bucket in records`.
    fn __contains__(&self, bucket: &TableBucket) -> bool {
        self.records_by_bucket.contains_key(bucket)
    }

    /// Mapping protocol: alias for `buckets()`.
    pub fn keys(&self) -> Vec<TableBucket> {
        self.buckets()
    }

    /// Mapping protocol: lazy iterator over record lists, one per bucket.
    pub fn values(slf: Bound<'_, Self>) -> ScanRecordsBucketIter {
        let this = slf.borrow();
        let bucket_keys: Vec<TableBucket> = this.records_by_bucket.keys().cloned().collect();
        drop(this);
        ScanRecordsBucketIter {
            owner: slf.unbind(),
            bucket_keys,
            bucket_idx: 0,
            with_keys: false,
        }
    }

    /// Mapping protocol: lazy iterator over `(TableBucket, list[ScanRecord])` pairs.
    pub fn items(slf: Bound<'_, Self>) -> ScanRecordsBucketIter {
        let this = slf.borrow();
        let bucket_keys: Vec<TableBucket> = this.records_by_bucket.keys().cloned().collect();
        drop(this);
        ScanRecordsBucketIter {
            owner: slf.unbind(),
            bucket_keys,
            bucket_idx: 0,
            with_keys: true,
        }
    }

    fn __str__(&self) -> String {
        format!(
            "ScanRecords(records={}, buckets={})",
            self.total_count,
            self.records_by_bucket.len()
        )
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }

    /// Flat iterator over all records across all buckets (matches Java/Rust).
    fn __iter__(slf: Bound<'_, Self>) -> ScanRecordsIter {
        let this = slf.borrow();
        let bucket_keys: Vec<TableBucket> = this.records_by_bucket.keys().cloned().collect();
        drop(this);
        ScanRecordsIter {
            owner: slf.unbind(),
            bucket_keys,
            bucket_idx: 0,
            rec_idx: 0,
        }
    }
}

#[pyclass]
struct ScanRecordsIter {
    owner: Py<ScanRecords>,
    bucket_keys: Vec<TableBucket>,
    bucket_idx: usize,
    rec_idx: usize,
}

#[pymethods]
impl ScanRecordsIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> Option<Py<ScanRecord>> {
        let owner = self.owner.borrow(py);
        loop {
            if self.bucket_idx >= self.bucket_keys.len() {
                return None;
            }
            let bucket = &self.bucket_keys[self.bucket_idx];
            if let Some(recs) = owner.records_by_bucket.get(bucket) {
                if self.rec_idx < recs.len() {
                    let rec = recs[self.rec_idx].clone_ref(py);
                    self.rec_idx += 1;
                    return Some(rec);
                }
            }
            self.bucket_idx += 1;
            self.rec_idx = 0;
        }
    }
}

/// Lazy iterator for `ScanRecords.items()` and `ScanRecords.values()`.
///
/// Yields one bucket at a time: `(TableBucket, list[ScanRecord])` for items,
/// or `list[ScanRecord]` for values. Only materializes records for the
/// current bucket on each `__next__` call.
#[pyclass]
pub struct ScanRecordsBucketIter {
    owner: Py<ScanRecords>,
    bucket_keys: Vec<TableBucket>,
    bucket_idx: usize,
    with_keys: bool,
}

#[pymethods]
impl ScanRecordsBucketIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> Option<Py<PyAny>> {
        if self.bucket_idx >= self.bucket_keys.len() {
            return None;
        }
        let bucket = &self.bucket_keys[self.bucket_idx];
        let owner = self.owner.borrow(py);
        let recs = owner
            .records_by_bucket
            .get(bucket)
            .map(|recs| recs.iter().map(|r| r.clone_ref(py)).collect::<Vec<_>>())
            .unwrap_or_default();
        let bucket = bucket.clone();
        self.bucket_idx += 1;

        if self.with_keys {
            Some(
                (bucket, recs)
                    .into_pyobject(py)
                    .unwrap()
                    .into_any()
                    .unbind(),
            )
        } else {
            Some(recs.into_pyobject(py).unwrap().into_any().unbind())
        }
    }
}

/// Represents a Fluss table for data operations
#[pyclass]
pub struct FlussTable {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    table_path: fcore::metadata::TablePath,
    has_primary_key: bool,
}

/// Builder for creating log scanners with flexible configuration.
///
/// Use this builder to configure projection, and in the future, filters
/// before creating a log scanner.
#[pyclass]
pub struct TableScan {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    projection: Option<ProjectionType>,
    limit: Option<i32>,
}

/// Scanner type for internal use
enum ScannerType {
    Record,
    Batch,
}

#[pymethods]
impl TableScan {
    /// Project to specific columns by their indices.
    ///
    /// Args:
    ///     indices: List of column indices (0-based) to include in the scan.
    ///
    /// Returns:
    ///     Self for method chaining.
    pub fn project(mut slf: PyRefMut<'_, Self>, indices: Vec<usize>) -> PyRefMut<'_, Self> {
        slf.projection = Some(ProjectionType::Indices(indices));
        slf
    }

    /// Project to specific columns by their names.
    ///
    /// Args:
    ///     names: List of column names to include in the scan.
    ///
    /// Returns:
    ///     Self for method chaining.
    pub fn project_by_name(mut slf: PyRefMut<'_, Self>, names: Vec<String>) -> PyRefMut<'_, Self> {
        slf.projection = Some(ProjectionType::Names(names));
        slf
    }

    /// Set a positive row limit, enabling `create_bucket_batch_scanner()`.
    ///
    /// Args:
    ///     n: Maximum number of rows to scan. Must be positive.
    ///
    /// Returns:
    ///     Self for method chaining.
    pub fn limit(mut slf: PyRefMut<'_, Self>, n: i32) -> PyResult<PyRefMut<'_, Self>> {
        if n <= 0 {
            return Err(FlussError::new_err(format!(
                "Scan limit must be positive, got {n}"
            )));
        }
        slf.limit = Some(n);
        Ok(slf)
    }

    /// Create a one-shot bounded scanner over a single bucket.
    ///
    /// Requires a limit set via `limit()`; the scan runs on the first
    /// `next_batch()`.
    ///
    /// Args:
    ///     bucket: Bucket to scan; must belong to this table.
    ///
    /// Returns:
    ///     A BatchScanner for `bucket`.
    pub fn create_bucket_batch_scanner(&self, bucket: &TableBucket) -> PyResult<BatchScanner> {
        let limit = self.limit.ok_or_else(|| {
            FlussError::new_err("create_bucket_batch_scanner requires a limit set via .limit(n)")
        })?;

        let conn = self.connection.clone();
        let _guard = TOKIO_RUNTIME.enter();
        let table =
            fcore::client::FlussTable::new(&conn, self.metadata.clone(), self.table_info.clone());

        let projection = self.projection.clone();
        let projection_indices = resolve_projection_indices(&projection, &self.table_info)?;
        let scan = apply_projection(table.new_scan(), projection)?
            .limit(limit)
            .map_err(|e| FlussError::from_core_error(&e))?;
        let scanner = scan
            .create_bucket_batch_scanner(bucket.to_core())
            .map_err(|e| FlussError::from_core_error(&e))?;

        let (projected_schema, _) =
            calculate_projected_types(&self.table_info, projection_indices)?;
        Ok(BatchScanner::new(scanner, bucket.clone(), projected_schema))
    }

    /// Create a record-based log scanner.
    ///
    /// Use this scanner with `poll()` to get individual records with metadata
    /// (offset, timestamp, change_type).
    ///
    /// Returns:
    ///     LogScanner for record-by-record scanning with `poll()`
    pub fn create_log_scanner<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.create_scanner_internal(py, ScannerType::Record)
    }

    /// Create a batch-based log scanner.
    ///
    /// Use this scanner with `poll_arrow()` to get Arrow Tables, or with
    /// `poll_record_batch()` to get individual batches with metadata.
    ///
    /// Returns:
    ///     LogScanner for batch-based scanning with `poll_arrow()` or `poll_record_batch()`
    pub fn create_record_batch_log_scanner<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.create_scanner_internal(py, ScannerType::Batch)
    }

    fn __repr__(&self) -> String {
        format!(
            "TableScan(table={}.{})",
            self.table_info.table_path.database(),
            self.table_info.table_path.table()
        )
    }
}

impl TableScan {
    fn create_scanner_internal<'py>(
        &self,
        py: Python<'py>,
        scanner_type: ScannerType,
    ) -> PyResult<Bound<'py, PyAny>> {
        if let Some(limit) = self.limit {
            return Err(FlussError::new_err(format!(
                "Log scanners don't support limit pushdown (requested limit: {limit}). \
                 Use create_bucket_batch_scanner() for a bounded scan."
            )));
        }

        let conn = self.connection.clone();
        let metadata = self.metadata.clone();
        let table_info = self.table_info.clone();
        let projection = self.projection.clone();

        future_into_py(py, async move {
            let fluss_table = fcore::client::FlussTable::new(&conn, metadata, table_info.clone());

            let projection_indices = resolve_projection_indices(&projection, &table_info)?;
            let table_scan = apply_projection(fluss_table.new_scan(), projection)?;

            let admin = conn
                .get_admin()
                .map_err(|e| FlussError::from_core_error(&e))?;

            let (projected_schema, projected_row_type) =
                calculate_projected_types(&table_info, projection_indices)?;

            let scanner_kind = match scanner_type {
                ScannerType::Record => {
                    let s = table_scan
                        .create_log_scanner()
                        .map_err(|e| FlussError::from_core_error(&e))?;
                    ScannerKind::Record(s)
                }
                ScannerType::Batch => {
                    let s = table_scan
                        .create_record_batch_log_scanner()
                        .map_err(|e| FlussError::from_core_error(&e))?;
                    ScannerKind::Batch(s)
                }
            };

            let py_scanner = LogScanner::new(
                scanner_kind,
                admin,
                table_info,
                projected_schema,
                Arc::new(projected_row_type),
            );

            Python::attach(|py| Py::new(py, py_scanner))
        })
    }
}

/// Internal enum to represent different projection types
#[derive(Clone)]
enum ProjectionType {
    Indices(Vec<usize>),
    Names(Vec<String>),
}

/// Resolve projection to column indices
fn resolve_projection_indices(
    projection: &Option<ProjectionType>,
    table_info: &fcore::metadata::TableInfo,
) -> PyResult<Option<Vec<usize>>> {
    match projection {
        Some(ProjectionType::Indices(indices)) => Ok(Some(indices.clone())),
        Some(ProjectionType::Names(names)) => {
            let schema = table_info.get_schema();
            let columns = schema.columns();
            let mut indices = Vec::with_capacity(names.len());
            for name in names {
                let idx = columns
                    .iter()
                    .position(|c| c.name() == name)
                    .ok_or_else(|| FlussError::new_err(format!("Column '{name}' not found")))?;
                indices.push(idx);
            }
            Ok(Some(indices))
        }
        None => Ok(None),
    }
}

/// Apply projection to table scan
fn apply_projection(
    table_scan: fcore::client::TableScan,
    projection: Option<ProjectionType>,
) -> PyResult<fcore::client::TableScan> {
    match projection {
        Some(ProjectionType::Indices(indices)) => table_scan
            .project(&indices)
            .map_err(|e| FlussError::from_core_error(&e)),
        Some(ProjectionType::Names(names)) => {
            let column_name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            table_scan
                .project_by_name(&column_name_refs)
                .map_err(|e| FlussError::from_core_error(&e))
        }
        None => Ok(table_scan),
    }
}

/// Calculate projected schema and row type from projection indices
fn calculate_projected_types(
    table_info: &fcore::metadata::TableInfo,
    projection_indices: Option<Vec<usize>>,
) -> PyResult<(SchemaRef, fcore::metadata::RowType)> {
    let full_schema =
        to_arrow_schema(table_info.get_row_type()).map_err(|e| FlussError::from_core_error(&e))?;
    let full_row_type = table_info.get_row_type();

    match projection_indices {
        Some(indices) => {
            let arrow_fields: Vec<_> = indices
                .iter()
                .map(|&i| full_schema.field(i).clone())
                .collect();
            let row_fields: Vec<_> = indices
                .iter()
                .map(|&i| full_row_type.fields()[i].clone())
                .collect();
            Ok((
                Arc::new(arrow_schema::Schema::new(arrow_fields)),
                fcore::metadata::RowType::new(row_fields),
            ))
        }
        None => Ok((full_schema, full_row_type.clone())),
    }
}

#[pymethods]
impl FlussTable {
    /// Create a new table scan builder for configuring and creating log scanners.
    ///
    /// Use this method to create scanners with the builder pattern:
    /// Returns:
    ///     TableScan builder for configuring the scanner.
    pub fn new_scan(&self) -> TableScan {
        TableScan {
            connection: self.connection.clone(),
            metadata: self.metadata.clone(),
            table_info: self.table_info.clone(),
            projection: None,
            limit: None,
        }
    }

    /// Create a new TableAppend builder for the table.
    ///
    /// Returns:
    ///     TableAppend builder. Call `create_writer()` to get an AppendWriter.
    fn new_append(&self) -> PyResult<TableAppend> {
        let _guard = TOKIO_RUNTIME.enter();
        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );

        let table_append = fluss_table
            .new_append()
            .map_err(|e| FlussError::from_core_error(&e))?;

        Ok(TableAppend {
            inner: table_append,
            table_info: self.table_info.clone(),
        })
    }

    /// Get table information
    pub fn get_table_info(&self) -> TableInfo {
        TableInfo::from_core(self.table_info.clone())
    }

    /// Get table path
    pub fn get_table_path(&self) -> TablePath {
        TablePath::from_core(self.table_path.clone())
    }

    /// Check if table has primary key
    pub fn has_primary_key(&self) -> bool {
        self.has_primary_key
    }

    /// Create a new TableLookup builder for primary key lookups.
    ///
    /// This is only available for tables with a primary key.
    ///
    /// Returns:
    ///     TableLookup builder. Call `create_lookuper()` to get a Lookuper.
    pub fn new_lookup(&self) -> PyResult<TableLookup> {
        if !self.has_primary_key {
            return Err(FlussError::new_err(
                "Lookup is only supported for primary key tables",
            ));
        }

        Ok(TableLookup {
            connection: self.connection.clone(),
            metadata: self.metadata.clone(),
            table_info: self.table_info.clone(),
        })
    }

    /// Create a new TableUpsert builder for the table.
    ///
    /// This is only available for tables with a primary key.
    ///
    /// Returns:
    ///     TableUpsert builder. Call `create_writer()` to get an UpsertWriter,
    ///     or use `partial_update_by_name()` / `partial_update_by_index()` first.
    pub fn new_upsert(&self) -> PyResult<TableUpsert> {
        if !self.has_primary_key {
            return Err(FlussError::new_err(
                "Upsert is only supported for primary key tables",
            ));
        }

        let _guard = TOKIO_RUNTIME.enter();
        let fluss_table = fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        );

        let table_upsert = fluss_table
            .new_upsert()
            .map_err(|e| FlussError::from_core_error(&e))?;

        Ok(TableUpsert {
            inner: table_upsert,
            table_info: self.table_info.clone(),
            target_columns: None,
        })
    }

    fn __repr__(&self) -> String {
        format!(
            "FlussTable(path={}.{})",
            self.table_path.database(),
            self.table_path.table()
        )
    }
}

impl FlussTable {
    /// Create a FlussTable
    pub fn new_table(
        connection: Arc<fcore::client::FlussConnection>,
        metadata: Arc<fcore::client::Metadata>,
        table_info: fcore::metadata::TableInfo,
        table_path: fcore::metadata::TablePath,
        has_primary_key: bool,
    ) -> Self {
        Self {
            connection,
            metadata,
            table_info,
            table_path,
            has_primary_key,
        }
    }
}

/// Builder for creating an AppendWriter.
///
/// Obtain via `FlussTable.new_append()`, then call `create_writer()`.
#[pyclass]
pub struct TableAppend {
    inner: fcore::client::TableAppend,
    table_info: fcore::metadata::TableInfo,
}

#[pymethods]
impl TableAppend {
    /// Create an AppendWriter from this builder.
    pub fn create_writer(&self) -> PyResult<AppendWriter> {
        let rust_writer = self
            .inner
            .create_writer()
            .map_err(|e| FlussError::from_core_error(&e))?;
        Ok(AppendWriter::from_core(
            rust_writer,
            self.table_info.clone(),
        ))
    }

    fn __repr__(&self) -> String {
        "TableAppend()".to_string()
    }
}

/// Builder for creating an UpsertWriter, with optional partial update configuration.
///
/// Obtain via `FlussTable.new_upsert()`, then optionally call
/// `partial_update_by_name()` or `partial_update_by_index()`,
/// then call `create_writer()`.
#[pyclass]
pub struct TableUpsert {
    inner: fcore::client::TableUpsert,
    table_info: fcore::metadata::TableInfo,
    /// Column indices for partial updates, tracked for Python's dict→GenericRow conversion.
    target_columns: Option<Vec<usize>>,
}

#[pymethods]
impl TableUpsert {
    /// Configure partial update by column names.
    ///
    /// Only the specified columns will be updated on upsert.
    ///
    /// Args:
    ///     columns: List of column names to update.
    ///
    /// Returns:
    ///     A new TableUpsert configured for partial update.
    pub fn partial_update_by_name(&self, columns: Vec<String>) -> PyResult<TableUpsert> {
        let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
        // Core validates and resolves names → indices internally
        let updated = self
            .inner
            .partial_update_with_column_names(&col_refs)
            .map_err(|e| FlussError::from_core_error(&e))?;
        // Resolve indices for Python's row conversion layer (core validated names above)
        let row_type = self.table_info.row_type();
        let indices: Vec<usize> = columns
            .iter()
            .map(|name| {
                row_type.get_field_index(name).ok_or_else(|| {
                    FlussError::new_err(format!("Unknown column name '{name}' for partial update"))
                })
            })
            .collect::<PyResult<Vec<usize>>>()?;
        Ok(TableUpsert {
            inner: updated,
            table_info: self.table_info.clone(),
            target_columns: Some(indices),
        })
    }

    /// Configure partial update by column indices.
    ///
    /// Only the specified columns will be updated on upsert.
    ///
    /// Args:
    ///     column_indices: List of column indices (0-based) to update.
    ///
    /// Returns:
    ///     A new TableUpsert configured for partial update.
    pub fn partial_update_by_index(&self, column_indices: Vec<usize>) -> PyResult<TableUpsert> {
        let target = column_indices.clone();
        // Core validates indices internally
        let updated = self
            .inner
            .partial_update(Some(column_indices))
            .map_err(|e| FlussError::from_core_error(&e))?;
        Ok(TableUpsert {
            inner: updated,
            table_info: self.table_info.clone(),
            target_columns: Some(target),
        })
    }

    /// Create an UpsertWriter from this builder.
    pub fn create_writer(&self) -> PyResult<crate::UpsertWriter> {
        crate::UpsertWriter::new(
            &self.inner,
            self.table_info.clone(),
            self.target_columns.clone(),
        )
    }

    fn __repr__(&self) -> String {
        "TableUpsert()".to_string()
    }
}

/// Builder for creating a Lookuper.
///
/// Obtain via `FlussTable.new_lookup()`, then call `create_lookuper()`.
#[pyclass]
pub struct TableLookup {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
}

#[pymethods]
impl TableLookup {
    /// Create a Lookuper from this builder.
    pub fn create_lookuper(&self) -> PyResult<crate::Lookuper> {
        crate::Lookuper::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        )
    }

    /// Switch to prefix-scan mode for the given lookup columns.
    ///
    /// The columns must be the table's partition keys (if any) plus the
    /// bucket keys, in that order.
    ///
    /// Args:
    ///     column_names: List of column names forming the prefix key.
    ///
    /// Returns:
    ///     TablePrefixLookup builder. Call `create_lookuper()` to get a PrefixLookuper.
    pub fn lookup_by(&self, column_names: Vec<String>) -> TablePrefixLookup {
        TablePrefixLookup {
            connection: self.connection.clone(),
            metadata: self.metadata.clone(),
            table_info: self.table_info.clone(),
            lookup_column_names: column_names,
        }
    }

    fn __repr__(&self) -> String {
        "TableLookup()".to_string()
    }
}

/// Builder for creating a PrefixLookuper.
///
/// Obtain via `TableLookup.lookup_by(columns)`, then call `create_lookuper()`.
#[pyclass]
pub struct TablePrefixLookup {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    lookup_column_names: Vec<String>,
}

#[pymethods]
impl TablePrefixLookup {
    /// Create a PrefixLookuper from this builder.
    pub fn create_lookuper(&self) -> PyResult<crate::PrefixLookuper> {
        crate::PrefixLookuper::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
            self.lookup_column_names.clone(),
        )
    }

    fn __repr__(&self) -> String {
        "TablePrefixLookup()".to_string()
    }
}

/// Writer for appending data to a Fluss table
#[pyclass]
pub struct AppendWriter {
    inner: Arc<fcore::client::AppendWriter>,
    table_info: fcore::metadata::TableInfo,
}

#[pymethods]
impl AppendWriter {
    /// Write Arrow table data (fire-and-forget, use flush() to ensure delivery)
    pub fn write_arrow(&self, py: Python, table: Py<PyAny>) -> PyResult<()> {
        // Convert Arrow Table to batches and write each batch
        let batches = table.call_method0(py, "to_batches")?;
        let batch_list: Vec<Py<PyAny>> = batches.extract(py)?;

        for batch in batch_list {
            // Drop the handle — fire-and-forget for bulk writes
            drop(self.write_arrow_batch(py, batch)?);
        }
        Ok(())
    }

    /// Write Arrow batch data.
    ///
    /// Returns:
    ///     WriteResultHandle that can be ignored (fire-and-forget) or
    ///     awaited via `handle.wait()` for server acknowledgment.
    pub fn write_arrow_batch(&self, py: Python, batch: Py<PyAny>) -> PyResult<WriteResultHandle> {
        // This shares the underlying Arrow buffers without copying data
        let batch_bound = batch.bind(py);
        let rust_batch: ArrowRecordBatch = FromPyArrow::from_pyarrow_bound(batch_bound)
            .map_err(|e| FlussError::new_err(format!("Failed to convert RecordBatch: {e}")))?;

        let result_future = self
            .inner
            .append_arrow_batch(rust_batch)
            .map_err(|e| FlussError::from_core_error(&e))?;
        Ok(WriteResultHandle::new(result_future))
    }

    /// Append a single row to the table.
    ///
    /// Returns:
    ///     WriteResultHandle that can be ignored (fire-and-forget) or
    ///     awaited via `handle.wait()` for server acknowledgment.
    pub fn append(&self, row: &Bound<'_, PyAny>) -> PyResult<WriteResultHandle> {
        let generic_row = python_to_generic_row(row, &self.table_info)?;

        let result_future = self
            .inner
            .append(&generic_row)
            .map_err(|e| FlussError::from_core_error(&e))?;
        Ok(WriteResultHandle::new(result_future))
    }

    /// Write Pandas DataFrame data
    pub fn write_pandas(&self, py: Python, df: Py<PyAny>) -> PyResult<()> {
        // Get the expected Arrow schema from the Fluss table
        let row_type = self.table_info.get_row_type();
        let expected_schema = fcore::record::to_arrow_schema(row_type)
            .map_err(|e| FlussError::from_core_error(&e))?;

        // Convert Arrow schema to PyArrow schema
        let py_schema = expected_schema
            .as_ref()
            .to_pyarrow(py)
            .map_err(|e| FlussError::new_err(format!("Failed to convert schema: {e}")))?;

        // Import pyarrow module
        let pyarrow = py.import("pyarrow")?;

        // Get the Table class from pyarrow module
        let table_class = pyarrow.getattr("Table")?;

        // Call Table.from_pandas(df, schema=expected_schema) to ensure proper type casting
        let pa_table = table_class.call_method(
            "from_pandas",
            (df,),
            Some(&[("schema", py_schema)].into_py_dict(py)?),
        )?;

        // Then call write_arrow with the converted table
        self.write_arrow(py, pa_table.into())
    }

    /// Flush any pending data
    pub fn flush<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            inner
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
        let inner = self.inner.clone();
        let is_exc_none = exc_type.as_ref().is_none_or(|e| e.is_none());
        future_into_py(py, async move {
            let res = inner.flush().await;
            if let Err(e) = res {
                if is_exc_none {
                    return Err(FlussError::from_core_error(&e));
                }
            }
            Ok(false)
        })
    }

    fn __repr__(&self) -> String {
        "AppendWriter()".to_string()
    }
}

impl AppendWriter {
    /// Create a AppendWriter from a core append writer
    pub fn from_core(
        append: fcore::client::AppendWriter,
        table_info: fcore::metadata::TableInfo,
    ) -> Self {
        Self {
            inner: Arc::new(append),
            table_info,
        }
    }
}

/// Represents different input shapes for a row
#[derive(FromPyObject)]
enum RowInput<'py> {
    Dict(Bound<'py, PyDict>),
    Tuple(Bound<'py, PyTuple>),
    List(Bound<'py, PyList>),
}

/// Convert Python row (dict/list/tuple) to GenericRow requiring all schema columns.
pub fn python_to_generic_row(
    row: &Bound<PyAny>,
    table_info: &fcore::metadata::TableInfo,
) -> PyResult<fcore::row::GenericRow<'static>> {
    let all_indices: Vec<usize> = (0..table_info.row_type().fields().len()).collect();
    python_to_sparse_generic_row(row, table_info, &all_indices)
}

/// Process a Python sequence (list or tuple) into datums at the target column positions.
fn process_sequence(
    seq: &Bound<PySequence>,
    target_indices: &[usize],
    fields: &[fcore::metadata::DataField],
    datums: &mut [fcore::row::Datum<'static>],
    sparse: bool,
) -> PyResult<()> {
    if seq.len()? != target_indices.len() {
        return Err(FlussError::new_err(format!(
            "Expected {} elements, got {}",
            target_indices.len(),
            seq.len()?
        )));
    }
    for (i, &col_idx) in target_indices.iter().enumerate() {
        let field = &fields[col_idx];
        let value = seq.get_item(i)?;
        let dest = if sparse { col_idx } else { i };
        datums[dest] = python_value_to_datum(&value, field.data_type())
            .map_err(|e| FlussError::new_err(format!("Field '{}': {}", field.name(), e)))?;
    }
    Ok(())
}

/// Build a full-width GenericRow filling only the specified column
/// indices from user input; all other columns are set to Null.
pub fn python_to_sparse_generic_row(
    row: &Bound<PyAny>,
    table_info: &fcore::metadata::TableInfo,
    target_indices: &[usize],
) -> PyResult<fcore::row::GenericRow<'static>> {
    python_to_generic_row_inner(row, table_info, target_indices, true)
}

/// Build a dense GenericRow with exactly `target_indices.len()` fields,
/// containing only the target column values in order.
pub fn python_to_dense_generic_row(
    row: &Bound<PyAny>,
    table_info: &fcore::metadata::TableInfo,
    target_indices: &[usize],
) -> PyResult<fcore::row::GenericRow<'static>> {
    python_to_generic_row_inner(row, table_info, target_indices, false)
}

/// Build a GenericRow from user input. When `sparse` is true, the row is full width and padded with nulls
fn python_to_generic_row_inner(
    row: &Bound<PyAny>,
    table_info: &fcore::metadata::TableInfo,
    target_indices: &[usize],
    sparse: bool,
) -> PyResult<fcore::row::GenericRow<'static>> {
    let row_type = table_info.row_type();
    let fields = row_type.fields();
    let target_names: Vec<&str> = target_indices.iter().map(|&i| fields[i].name()).collect();

    let num_fields = if sparse {
        fields.len()
    } else {
        target_indices.len()
    };
    let mut datums: Vec<fcore::row::Datum<'static>> = vec![fcore::row::Datum::Null; num_fields];

    let row_input: RowInput = row.extract().map_err(|_| {
        let type_name = row
            .get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_else(|_| "unknown".to_string());
        FlussError::new_err(format!(
            "Row must be a dict, list, or tuple; got {type_name}"
        ))
    })?;

    match row_input {
        RowInput::Dict(dict) => {
            for (k, _) in dict.iter() {
                let key_str = k.extract::<&str>().map_err(|_| {
                    let key_type = k
                        .get_type()
                        .name()
                        .map(|n| n.to_string())
                        .unwrap_or_else(|_| "unknown".to_string());
                    FlussError::new_err(format!("Dict keys must be strings; got {key_type}"))
                })?;
                if !target_names.contains(&key_str) {
                    return Err(FlussError::new_err(format!(
                        "Unknown field '{}'. Expected: {}",
                        key_str,
                        target_names.join(", ")
                    )));
                }
            }
            for (i, &col_idx) in target_indices.iter().enumerate() {
                let name = target_names[i];
                let field = &fields[col_idx];
                let dest = if sparse { col_idx } else { i };
                match dict.get_item(name)? {
                    Some(value) => {
                        datums[dest] = python_value_to_datum(&value, field.data_type())
                            .map_err(|e| FlussError::new_err(format!("Field '{name}': {e}")))?;
                    }
                    None if field.data_type().is_nullable() => {}
                    None => {
                        return Err(FlussError::new_err(format!(
                            "Missing value for non-nullable field '{name}'"
                        )));
                    }
                }
            }
        }

        RowInput::List(list) => {
            process_sequence(
                list.as_sequence(),
                target_indices,
                fields,
                &mut datums,
                sparse,
            )?;
        }

        RowInput::Tuple(tuple) => {
            process_sequence(
                tuple.as_sequence(),
                target_indices,
                fields,
                &mut datums,
                sparse,
            )?;
        }
    }

    Ok(fcore::row::GenericRow { values: datums })
}

/// Convert Python value to Datum based on data type
fn python_value_to_datum(value: &Bound<PyAny>, data_type: &DataType) -> PyResult<Datum<'static>> {
    if value.is_none() {
        return Ok(Datum::Null);
    }

    match data_type {
        DataType::Boolean(_) => {
            let v: bool = value.extract()?;
            Ok(Datum::Bool(v))
        }
        DataType::TinyInt(_) => {
            // Strict type checking: reject bool for int columns
            if value.is_instance_of::<PyBool>() {
                return Err(FlussError::new_err(
                    "Expected int for TinyInt column, got bool. Use 0 or 1 explicitly.".to_string(),
                ));
            }
            let v: i8 = value.extract()?;
            Ok(Datum::Int8(v))
        }
        DataType::SmallInt(_) => {
            if value.is_instance_of::<PyBool>() {
                return Err(FlussError::new_err(
                    "Expected int for SmallInt column, got bool. Use 0 or 1 explicitly."
                        .to_string(),
                ));
            }
            let v: i16 = value.extract()?;
            Ok(Datum::Int16(v))
        }
        DataType::Int(_) => {
            if value.is_instance_of::<PyBool>() {
                return Err(FlussError::new_err(
                    "Expected int for Int column, got bool. Use 0 or 1 explicitly.".to_string(),
                ));
            }
            let v: i32 = value.extract()?;
            Ok(Datum::Int32(v))
        }
        DataType::BigInt(_) => {
            if value.is_instance_of::<PyBool>() {
                return Err(FlussError::new_err(
                    "Expected int for BigInt column, got bool. Use 0 or 1 explicitly.".to_string(),
                ));
            }
            let v: i64 = value.extract()?;
            Ok(Datum::Int64(v))
        }
        DataType::Float(_) => {
            let v: f32 = value.extract()?;
            Ok(Datum::Float32(F32::from(v)))
        }
        DataType::Double(_) => {
            let v: f64 = value.extract()?;
            Ok(Datum::Float64(F64::from(v)))
        }
        DataType::String(_) | DataType::Char(_) => {
            let v: String = value.extract()?;
            Ok(v.into())
        }
        DataType::Bytes(_) | DataType::Binary(_) => {
            // Efficient extraction: downcast to specific type and use bulk copy.
            // PyBytes::as_bytes() and PyByteArray::to_vec() are O(n) bulk copies of the underlying data.
            if let Ok(bytes) = value.downcast::<PyBytes>() {
                Ok(bytes.as_bytes().to_vec().into())
            } else if let Ok(bytearray) = value.downcast::<PyByteArray>() {
                Ok(bytearray.to_vec().into())
            } else {
                Err(FlussError::new_err(format!(
                    "Expected bytes or bytearray, got {}",
                    value.get_type().name()?
                )))
            }
        }
        DataType::Decimal(decimal_type) => {
            python_decimal_to_datum(value, decimal_type.precision(), decimal_type.scale())
        }
        DataType::Date(_) => python_date_to_datum(value),
        DataType::Time(_) => python_time_to_datum(value),
        DataType::Timestamp(_) => python_datetime_to_timestamp_ntz(value),
        DataType::TimestampLTz(_) => python_datetime_to_timestamp_ltz(value),
        DataType::Array(array_type) => {
            let element_type = array_type.get_element_type();
            if value.is_instance_of::<PyString>() {
                return Err(FlussError::new_err(format!(
                    "Expected sequence for Array column, got {}",
                    get_type_name(value)
                )));
            }
            let seq = value.downcast::<PySequence>().map_err(|_| {
                FlussError::new_err(format!(
                    "Expected sequence for Array column, got {}",
                    get_type_name(value)
                ))
            })?;

            let len = seq.len()?;
            let mut writer = FlussArrayWriter::new(len, element_type);

            for i in 0..len {
                let item = seq.get_item(i)?;
                if item.is_none() {
                    writer.set_null_at(i);
                } else {
                    let val_datum = python_value_to_datum(&item, element_type)?;
                    match val_datum {
                        Datum::Null => writer.set_null_at(i),
                        Datum::Bool(v) => writer.write_boolean(i, v),
                        Datum::Int8(v) => writer.write_byte(i, v),
                        Datum::Int16(v) => writer.write_short(i, v),
                        Datum::Int32(v) => writer.write_int(i, v),
                        Datum::Int64(v) => writer.write_long(i, v),
                        Datum::Float32(v) => writer.write_float(i, v.into_inner()),
                        Datum::Float64(v) => writer.write_double(i, v.into_inner()),
                        Datum::String(v) => writer.write_string(i, &v),
                        Datum::Blob(v) => writer.write_binary_bytes(i, v.as_ref()),
                        Datum::Decimal(v) => {
                            if let DataType::Decimal(dt) = element_type {
                                writer.write_decimal(i, &v, dt.precision());
                            }
                        }
                        Datum::Date(v) => writer.write_date(i, v),
                        Datum::Time(v) => writer.write_time(i, v),
                        Datum::TimestampNtz(v) => {
                            if let DataType::Timestamp(dt) = element_type {
                                writer.write_timestamp_ntz(i, &v, dt.precision());
                            }
                        }
                        Datum::TimestampLtz(v) => {
                            if let DataType::TimestampLTz(dt) = element_type {
                                writer.write_timestamp_ltz(i, &v, dt.precision());
                            }
                        }
                        Datum::Array(v) => writer.write_array(i, &v),
                        Datum::Map(v) => writer.write_map(i, &v),
                        Datum::Row(v) => writer
                            .write_row(i, v.as_ref())
                            .map_err(|e| FlussError::from_core_error(&e))?,
                    }
                }
            }

            let array = writer
                .complete()
                .map_err(|e| FlussError::from_core_error(&e))?;
            Ok(Datum::Array(array))
        }
        DataType::Map(map_type) => {
            let key_type = map_type.key_type();
            let value_type = map_type.value_type();
            let pairs = python_map_pairs(value)?;
            let mut writer = FlussMapWriter::new(pairs.len(), key_type, value_type);
            for (k, v) in pairs {
                let key_datum = python_value_to_datum(&k, key_type)?;
                let value_datum = python_value_to_datum(&v, value_type)?;
                writer
                    .write_entry(key_datum, value_datum)
                    .map_err(|e| FlussError::from_core_error(&e))?;
            }
            let map = writer
                .complete()
                .map_err(|e| FlussError::from_core_error(&e))?;
            Ok(Datum::Map(map))
        }
        DataType::Row(row_type) => {
            let nested = python_value_to_row_datum(value, row_type)?;
            Ok(Datum::Row(Box::new(nested)))
        }
    }
}

/// Extract `(key, value)` pairs from a Python value representing a MAP column.
/// Accepts a `dict`, or a sequence of `(key, value)` pairs — matching the
/// shape pyarrow's `MapArray.to_pylist()` produces, so MAP values round-trip.
fn python_map_pairs<'py>(
    value: &Bound<'py, PyAny>,
) -> PyResult<Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>)>> {
    if let Ok(dict) = value.downcast::<PyDict>() {
        return Ok(dict.iter().collect());
    }
    if value.is_instance_of::<PyString>() {
        return Err(FlussError::new_err(format!(
            "Expected dict or sequence of (key, value) pairs for Map column, got {}",
            get_type_name(value)
        )));
    }
    let seq = value.downcast::<PySequence>().map_err(|_| {
        FlussError::new_err(format!(
            "Expected dict or sequence of (key, value) pairs for Map column, got {}",
            get_type_name(value)
        ))
    })?;
    let len = seq.len()?;
    let mut pairs = Vec::with_capacity(len);
    for i in 0..len {
        let entry = seq.get_item(i)?;
        let pair = entry.downcast::<PySequence>().map_err(|_| {
            FlussError::new_err("Map entries must be (key, value) pairs".to_string())
        })?;
        if pair.len()? != 2 {
            return Err(FlussError::new_err(
                "Map entries must be (key, value) pairs of length 2".to_string(),
            ));
        }
        pairs.push((pair.get_item(0)?, pair.get_item(1)?));
    }
    Ok(pairs)
}

/// Convert a Python value (`dict` by field name, or `list`/`tuple` by position)
/// into a nested `GenericRow` for a ROW column.
fn python_value_to_row_datum(
    value: &Bound<PyAny>,
    row_type: &RowType,
) -> PyResult<GenericRow<'static>> {
    let fields = row_type.fields();
    let mut datums: Vec<Datum<'static>> = vec![Datum::Null; fields.len()];

    let row_input: RowInput = value.extract().map_err(|_| {
        FlussError::new_err(format!(
            "Row column must be a dict, list, or tuple; got {}",
            get_type_name(value)
        ))
    })?;

    match row_input {
        RowInput::Dict(dict) => {
            for (k, _) in dict.iter() {
                let key_str = k.extract::<&str>().map_err(|_| {
                    FlussError::new_err(format!(
                        "Row field keys must be strings; got {}",
                        get_type_name(&k)
                    ))
                })?;
                if !fields.iter().any(|f| f.name() == key_str) {
                    return Err(FlussError::new_err(format!(
                        "Unknown row field '{}'. Expected: {}",
                        key_str,
                        fields
                            .iter()
                            .map(|f| f.name())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            }
            for (i, field) in fields.iter().enumerate() {
                match dict.get_item(field.name())? {
                    Some(v) => {
                        datums[i] = python_value_to_datum(&v, field.data_type()).map_err(|e| {
                            FlussError::new_err(format!("Row field '{}': {}", field.name(), e))
                        })?;
                    }
                    None if field.data_type().is_nullable() => {}
                    None => {
                        return Err(FlussError::new_err(format!(
                            "Missing value for non-nullable row field '{}'",
                            field.name()
                        )));
                    }
                }
            }
        }
        RowInput::List(list) => fill_row_fields(list.as_sequence(), fields, &mut datums)?,
        RowInput::Tuple(tuple) => fill_row_fields(tuple.as_sequence(), fields, &mut datums)?,
    }

    Ok(GenericRow { values: datums })
}

/// Fill ROW field datums from a positional Python sequence.
fn fill_row_fields(
    seq: &Bound<PySequence>,
    fields: &[DataField],
    datums: &mut [Datum<'static>],
) -> PyResult<()> {
    if seq.len()? != fields.len() {
        return Err(FlussError::new_err(format!(
            "Expected {} row fields, got {}",
            fields.len(),
            seq.len()?
        )));
    }
    for (i, field) in fields.iter().enumerate() {
        let v = seq.get_item(i)?;
        datums[i] = python_value_to_datum(&v, field.data_type())
            .map_err(|e| FlussError::new_err(format!("Row field '{}': {}", field.name(), e)))?;
    }
    Ok(())
}

/// Convert Rust Datum to Python value based on data type.
/// This is the reverse of python_value_to_datum.
pub fn datum_to_python_value(
    py: Python,
    row: &dyn InternalRow,
    pos: usize,
    data_type: &DataType,
) -> PyResult<Py<PyAny>> {
    // Check for null first
    if row
        .is_null_at(pos)
        .map_err(|e| FlussError::from_core_error(&e))?
    {
        return Ok(py.None());
    }

    match data_type {
        DataType::Boolean(_) => Ok(row
            .get_boolean(pos)
            .map_err(|e| FlussError::from_core_error(&e))?
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::TinyInt(_) => Ok(row
            .get_byte(pos)
            .map_err(|e| FlussError::from_core_error(&e))?
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::SmallInt(_) => Ok(row
            .get_short(pos)
            .map_err(|e| FlussError::from_core_error(&e))?
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::Int(_) => Ok(row
            .get_int(pos)
            .map_err(|e| FlussError::from_core_error(&e))?
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::BigInt(_) => Ok(row
            .get_long(pos)
            .map_err(|e| FlussError::from_core_error(&e))?
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::Float(_) => Ok(row
            .get_float(pos)
            .map_err(|e| FlussError::from_core_error(&e))?
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::Double(_) => Ok(row
            .get_double(pos)
            .map_err(|e| FlussError::from_core_error(&e))?
            .into_pyobject(py)?
            .to_owned()
            .into_any()
            .unbind()),
        DataType::String(_) => {
            let s = row
                .get_string(pos)
                .map_err(|e| FlussError::from_core_error(&e))?;
            Ok(s.into_pyobject(py)?.into_any().unbind())
        }
        DataType::Char(char_type) => {
            let s = row
                .get_char(pos, char_type.length() as usize)
                .map_err(|e| FlussError::from_core_error(&e))?;
            Ok(s.into_pyobject(py)?.into_any().unbind())
        }
        DataType::Bytes(_) => {
            let b = row
                .get_bytes(pos)
                .map_err(|e| FlussError::from_core_error(&e))?;
            Ok(PyBytes::new(py, b).into_any().unbind())
        }
        DataType::Binary(binary_type) => {
            let b = row
                .get_binary(pos, binary_type.length())
                .map_err(|e| FlussError::from_core_error(&e))?;
            Ok(PyBytes::new(py, b).into_any().unbind())
        }
        DataType::Decimal(decimal_type) => {
            let decimal = row
                .get_decimal(
                    pos,
                    decimal_type.precision() as usize,
                    decimal_type.scale() as usize,
                )
                .map_err(|e| FlussError::from_core_error(&e))?;
            rust_decimal_to_python(py, &decimal)
        }
        DataType::Date(_) => {
            let date = row
                .get_date(pos)
                .map_err(|e| FlussError::from_core_error(&e))?;
            rust_date_to_python(py, date)
        }
        DataType::Time(_) => {
            let time = row
                .get_time(pos)
                .map_err(|e| FlussError::from_core_error(&e))?;
            rust_time_to_python(py, time)
        }
        DataType::Timestamp(ts_type) => {
            let ts = row
                .get_timestamp_ntz(pos, ts_type.precision())
                .map_err(|e| FlussError::from_core_error(&e))?;
            rust_timestamp_ntz_to_python(py, ts)
        }
        DataType::TimestampLTz(ts_type) => {
            let ts = row
                .get_timestamp_ltz(pos, ts_type.precision())
                .map_err(|e| FlussError::from_core_error(&e))?;
            rust_timestamp_ltz_to_python(py, ts)
        }
        DataType::Array(array_type) => {
            let array_data = row
                .get_array(pos)
                .map_err(|e| FlussError::from_core_error(&e))?
                .try_into_binary()
                .map_err(|e| FlussError::from_core_error(&e))?;
            array_to_pylist(py, &array_data, array_type.get_element_type())
        }
        DataType::Map(map_type) => {
            let map_data = row
                .get_map(pos)
                .map_err(|e| FlussError::from_core_error(&e))?
                .try_into_binary()
                .map_err(|e| FlussError::from_core_error(&e))?;
            map_to_pylist(py, &map_data, map_type)
        }
        DataType::Row(row_type) => {
            let nested = row
                .get_row(pos)
                .map_err(|e| FlussError::from_core_error(&e))?
                .try_into_generic(row_type)
                .map_err(|e| FlussError::from_core_error(&e))?;
            row_to_pydict(py, &nested, row_type)
        }
    }
}

/// Convert a binary `FlussArray` to a Python list.
fn array_to_pylist(py: Python, arr: &FlussArray, element_type: &DataType) -> PyResult<Py<PyAny>> {
    let py_list = pyo3::types::PyList::empty(py);
    for i in 0..arr.size() {
        py_list.append(array_elem_to_python(py, arr, i, element_type)?)?;
    }
    Ok(py_list.into_any().unbind())
}

/// Convert a Fluss `MAP` to a Python list of `(key, value)` tuples — matching
/// pyarrow's default `MapArray.to_pylist()` shape (preserves duplicate keys and
/// ordering, and allows non-hashable keys).
fn map_to_pylist(py: Python, map_data: &FlussMap, map_type: &MapType) -> PyResult<Py<PyAny>> {
    let keys = map_data.key_array();
    let values = map_data.value_array();
    let py_list = pyo3::types::PyList::empty(py);
    for i in 0..map_data.size() {
        let py_key = array_elem_to_python(py, keys, i, map_type.key_type())?;
        let py_val = array_elem_to_python(py, values, i, map_type.value_type())?;
        py_list.append(pyo3::types::PyTuple::new(py, [py_key, py_val])?)?;
    }
    Ok(py_list.into_any().unbind())
}

/// Convert a Fluss `ROW` to a Python dict keyed by field name — matching
/// pyarrow's `StructArray.to_pylist()` shape.
fn row_to_pydict(py: Python, row: &dyn InternalRow, row_type: &RowType) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    for (i, field) in row_type.fields().iter().enumerate() {
        let py_val = datum_to_python_value(py, row, i, field.data_type())?;
        dict.set_item(field.name(), py_val)?;
    }
    Ok(dict.into_any().unbind())
}

/// Convert element `i` of a binary `FlussArray` to a Python value. A binary
/// array needs the explicit nested type to decode MAP/ROW elements (the generic
/// `DataGetters` trait getters cover only scalars and nested arrays), so this
/// dispatches through `FlussArray`'s inherent typed getters.
fn array_elem_to_python(
    py: Python,
    arr: &FlussArray,
    i: usize,
    dt: &DataType,
) -> PyResult<Py<PyAny>> {
    if arr.is_null_at(i) {
        return Ok(py.None());
    }
    match dt {
        DataType::Array(array_type) => {
            let nested = arr
                .get_array(i)
                .map_err(|e| FlussError::from_core_error(&e))?;
            array_to_pylist(py, &nested, array_type.get_element_type())
        }
        DataType::Map(map_type) => {
            let nested = arr
                .get_map(i, map_type.key_type(), map_type.value_type())
                .map_err(|e| FlussError::from_core_error(&e))?;
            map_to_pylist(py, &nested, map_type)
        }
        DataType::Row(row_type) => {
            let nested = arr
                .get_row(i, row_type)
                .map_err(|e| FlussError::from_core_error(&e))?;
            row_to_pydict(py, &nested, row_type)
        }
        _ => datum_to_python_value(py, arr, i, dt),
    }
}

/// Convert Rust Decimal to Python decimal.Decimal
fn rust_decimal_to_python(py: Python, decimal: &fcore::row::Decimal) -> PyResult<Py<PyAny>> {
    let decimal_ty = get_decimal_type(py)?;
    let decimal_str = decimal.to_string();
    let py_decimal = decimal_ty.call1((decimal_str,))?;
    Ok(py_decimal.into_any().unbind())
}

/// Convert Rust Date (days since epoch) to Python datetime.date
fn rust_date_to_python(py: Python, date: fcore::row::Date) -> PyResult<Py<PyAny>> {
    let days_since_epoch = date.get_inner();
    let epoch = jiff::civil::date(1970, 1, 1);
    let civil_date = epoch + jiff::Span::new().days(days_since_epoch as i64);

    let py_date = PyDate::new(
        py,
        civil_date.year() as i32,
        civil_date.month() as u8,
        civil_date.day() as u8,
    )?;
    Ok(py_date.into_any().unbind())
}

/// Convert Rust Time (millis since midnight) to Python datetime.time
fn rust_time_to_python(py: Python, time: fcore::row::Time) -> PyResult<Py<PyAny>> {
    let millis = time.get_inner() as i64;
    let hours = millis / MILLIS_PER_HOUR;
    let minutes = (millis % MILLIS_PER_HOUR) / MILLIS_PER_MINUTE;
    let seconds = (millis % MILLIS_PER_MINUTE) / MILLIS_PER_SECOND;
    let microseconds = (millis % MILLIS_PER_SECOND) * MICROS_PER_MILLI;

    let py_time = PyTime::new(
        py,
        hours as u8,
        minutes as u8,
        seconds as u8,
        microseconds as u32,
        None,
    )?;
    Ok(py_time.into_any().unbind())
}

/// Convert Rust TimestampNtz to Python naive datetime
fn rust_timestamp_ntz_to_python(py: Python, ts: fcore::row::TimestampNtz) -> PyResult<Py<PyAny>> {
    let millis = ts.get_millisecond();
    let nanos = ts.get_nano_of_millisecond();
    let total_micros = millis * MICROS_PER_MILLI + (nanos as i64 / NANOS_PER_MICRO);

    // Convert to civil datetime via jiff
    let timestamp = jiff::Timestamp::from_microsecond(total_micros)
        .map_err(|e| FlussError::new_err(format!("Invalid timestamp: {e}")))?;
    let civil_dt = timestamp.to_zoned(jiff::tz::TimeZone::UTC).datetime();

    let py_dt = PyDateTime::new(
        py,
        civil_dt.year() as i32,
        civil_dt.month() as u8,
        civil_dt.day() as u8,
        civil_dt.hour() as u8,
        civil_dt.minute() as u8,
        civil_dt.second() as u8,
        (civil_dt.subsec_nanosecond() / 1000) as u32, // microseconds
        None,
    )?;
    Ok(py_dt.into_any().unbind())
}

/// Convert Rust TimestampLtz to Python timezone-aware datetime (UTC)
fn rust_timestamp_ltz_to_python(py: Python, ts: fcore::row::TimestampLtz) -> PyResult<Py<PyAny>> {
    let millis = ts.get_epoch_millisecond();
    let nanos = ts.get_nano_of_millisecond();
    let total_micros = millis * MICROS_PER_MILLI + (nanos as i64 / NANOS_PER_MICRO);

    // Convert to civil datetime via jiff
    let timestamp = jiff::Timestamp::from_microsecond(total_micros)
        .map_err(|e| FlussError::new_err(format!("Invalid timestamp: {e}")))?;
    let civil_dt = timestamp.to_zoned(jiff::tz::TimeZone::UTC).datetime();

    let utc = get_utc_timezone(py)?;
    let py_dt = PyDateTime::new(
        py,
        civil_dt.year() as i32,
        civil_dt.month() as u8,
        civil_dt.day() as u8,
        civil_dt.hour() as u8,
        civil_dt.minute() as u8,
        civil_dt.second() as u8,
        (civil_dt.subsec_nanosecond() / 1000) as u32, // microseconds
        Some(&utc),
    )?;
    Ok(py_dt.into_any().unbind())
}

/// Convert an InternalRow to a Python dictionary
pub fn internal_row_to_dict(
    py: Python,
    row: &dyn fcore::row::InternalRow,
    table_info: &fcore::metadata::TableInfo,
) -> PyResult<Py<PyAny>> {
    let row_type = table_info.row_type();
    let fields = row_type.fields();
    let dict = PyDict::new(py);

    for (pos, field) in fields.iter().enumerate() {
        let value = datum_to_python_value(py, row, pos, field.data_type())?;
        dict.set_item(field.name(), value)?;
    }

    Ok(dict.into_any().unbind())
}

/// Cached decimal.Decimal type
/// Uses PyOnceLock for thread-safety and subinterpreter compatibility.
static DECIMAL_TYPE: PyOnceLock<Py<PyType>> = PyOnceLock::new();

/// Cached UTC timezone
static UTC_TIMEZONE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Cached UTC epoch type
static UTC_EPOCH: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Get the cached decimal.Decimal type, importing it once per interpreter.
fn get_decimal_type(py: Python) -> PyResult<Bound<PyType>> {
    let ty = DECIMAL_TYPE.get_or_try_init(py, || -> PyResult<_> {
        let decimal_mod = py.import("decimal")?;
        let decimal_ty = decimal_mod.getattr("Decimal")?.downcast_into::<PyType>()?;
        Ok(decimal_ty.unbind())
    })?;
    Ok(ty.bind(py).clone())
}

/// Get the cached UTC timezone (datetime.timezone.utc), creating it once per interpreter.
fn get_utc_timezone(py: Python) -> PyResult<Bound<PyTzInfo>> {
    let tz = UTC_TIMEZONE.get_or_try_init(py, || -> PyResult<_> {
        let datetime_mod = py.import("datetime")?;
        let timezone = datetime_mod.getattr("timezone")?;
        let utc = timezone.getattr("utc")?;
        Ok(utc.unbind())
    })?;
    // Downcast to PyTzInfo for use with PyDateTime::new()
    Ok(tz.bind(py).clone().downcast_into::<PyTzInfo>()?)
}

/// Get the cached UTC epoch datetime, creating it once per interpreter.
fn get_utc_epoch(py: Python) -> PyResult<Bound<PyAny>> {
    let epoch = UTC_EPOCH.get_or_try_init(py, || -> PyResult<_> {
        let datetime_mod = py.import("datetime")?;
        let timezone = datetime_mod.getattr("timezone")?;
        let utc = timezone.getattr("utc")?;
        let epoch = datetime_mod
            .getattr("datetime")?
            .call1((1970, 1, 1, 0, 0, 0, 0, &utc))?;
        Ok(epoch.unbind())
    })?;
    Ok(epoch.bind(py).clone())
}

/// Validate that value is a decimal.Decimal instance.
fn ensure_is_decimal(value: &Bound<PyAny>) -> PyResult<()> {
    let decimal_ty = get_decimal_type(value.py())?;
    if !value.is_instance(&decimal_ty.into_any())? {
        return Err(FlussError::new_err(format!(
            "Expected decimal.Decimal, got {}",
            get_type_name(value)
        )));
    }
    Ok(())
}

/// Convert Python decimal.Decimal to Datum::Decimal.
/// Only accepts decimal.Decimal
fn python_decimal_to_datum(
    value: &Bound<PyAny>,
    precision: u32,
    scale: u32,
) -> PyResult<fcore::row::Datum<'static>> {
    use std::str::FromStr;

    ensure_is_decimal(value)?;

    let decimal_str: String = value.str()?.extract()?;
    let bd = bigdecimal::BigDecimal::from_str(&decimal_str).map_err(|e| {
        FlussError::new_err(format!("Failed to parse decimal '{decimal_str}': {e}"))
    })?;

    let decimal = fcore::row::Decimal::from_big_decimal(bd, precision, scale).map_err(|e| {
        FlussError::new_err(format!(
            "Failed to convert decimal '{decimal_str}' to DECIMAL({precision}, {scale}): {e}"
        ))
    })?;

    Ok(fcore::row::Datum::Decimal(decimal))
}

/// Convert Python datetime.date to Datum::Date.
fn python_date_to_datum(value: &Bound<PyAny>) -> PyResult<fcore::row::Datum<'static>> {
    // Reject datetime.datetime (subclass of date) - use timestamp columns for those
    if value.downcast::<PyDateTime>().is_ok() {
        return Err(FlussError::new_err(
            "Expected datetime.date, got datetime.datetime. Use a TIMESTAMP column for datetime values.",
        ));
    }

    let date = value.downcast::<PyDate>().map_err(|_| {
        FlussError::new_err(format!(
            "Expected datetime.date, got {}",
            get_type_name(value)
        ))
    })?;

    let year = date.get_year();
    let month = date.get_month();
    let day = date.get_day();

    // Calculate days since Unix epoch (1970-01-01)
    let civil_date = jiff::civil::date(year as i16, month as i8, day as i8);
    let epoch = jiff::civil::date(1970, 1, 1);
    let days_since_epoch = (civil_date - epoch).get_days();

    Ok(fcore::row::Datum::Date(fcore::row::Date::new(
        days_since_epoch,
    )))
}

/// Convert Python datetime.time to Datum::Time.
/// Uses PyO3's native PyTime type for efficient access.
///
/// Note: Fluss TIME is always stored as milliseconds since midnight (i32) regardless
/// of the schema's precision setting. This matches the Java Fluss wire protocol.
/// Sub-millisecond precision (microseconds not divisible by 1000) will raise an error
/// to prevent silent data loss and ensure fail-fast behavior.
fn python_time_to_datum(value: &Bound<PyAny>) -> PyResult<fcore::row::Datum<'static>> {
    let time = value.downcast::<PyTime>().map_err(|_| {
        FlussError::new_err(format!(
            "Expected datetime.time, got {}",
            get_type_name(value)
        ))
    })?;

    let hour = time.get_hour() as i32;
    let minute = time.get_minute() as i32;
    let second = time.get_second() as i32;
    let microsecond = time.get_microsecond() as i32;

    // Strict validation: reject sub-millisecond precision
    if microsecond % MICROS_PER_MILLI as i32 != 0 {
        return Err(FlussError::new_err(format!(
            "TIME values with sub-millisecond precision are not supported. \
             Got time with {microsecond} microseconds (not divisible by 1000). \
             Fluss stores TIME as milliseconds since midnight. \
             Please round to milliseconds before insertion."
        )));
    }

    // Convert to milliseconds since midnight
    let millis = hour * MILLIS_PER_HOUR as i32
        + minute * MILLIS_PER_MINUTE as i32
        + second * MILLIS_PER_SECOND as i32
        + microsecond / MICROS_PER_MILLI as i32;

    Ok(fcore::row::Datum::Time(fcore::row::Time::new(millis)))
}

/// Convert Python datetime-like object to Datum::TimestampNtz.
/// Supports: datetime.datetime (naive preferred), pd.Timestamp, np.datetime64
fn python_datetime_to_timestamp_ntz(value: &Bound<PyAny>) -> PyResult<fcore::row::Datum<'static>> {
    let (epoch_millis, nano_of_milli) = extract_datetime_components_ntz(value)?;

    let ts = fcore::row::TimestampNtz::from_millis_nanos(epoch_millis, nano_of_milli)
        .map_err(|e| FlussError::new_err(format!("Failed to create TimestampNtz: {e}")))?;

    Ok(fcore::row::Datum::TimestampNtz(ts))
}

/// Convert Python datetime-like object to Datum::TimestampLtz.
/// For naive datetimes, assumes UTC. For aware datetimes, converts to UTC.
/// Supports: datetime.datetime, pd.Timestamp, np.datetime64
fn python_datetime_to_timestamp_ltz(value: &Bound<PyAny>) -> PyResult<fcore::row::Datum<'static>> {
    let (epoch_millis, nano_of_milli) = extract_datetime_components_ltz(value)?;

    let ts = fcore::row::TimestampLtz::from_millis_nanos(epoch_millis, nano_of_milli)
        .map_err(|e| FlussError::new_err(format!("Failed to create TimestampLtz: {e}")))?;

    Ok(fcore::row::Datum::TimestampLtz(ts))
}

/// Extract epoch milliseconds for TimestampNtz (wall-clock time, no timezone conversion).
/// Uses integer arithmetic to avoid float precision issues.
/// For clarity, tz-aware datetimes are rejected - use TimestampLtz for those.
fn extract_datetime_components_ntz(value: &Bound<PyAny>) -> PyResult<(i64, i32)> {
    // Try PyDateTime first
    if let Ok(dt) = value.downcast::<PyDateTime>() {
        // Reject tz-aware datetime for NTZ - it's ambiguous what the user wants
        let tzinfo = dt.getattr("tzinfo")?;
        if !tzinfo.is_none() {
            return Err(FlussError::new_err(
                "TIMESTAMP (without timezone) requires a naive datetime. \
                 Got timezone-aware datetime. Either remove tzinfo or use TIMESTAMP_LTZ column.",
            ));
        }
        return datetime_to_epoch_millis_as_utc(dt);
    }

    // Check for pandas Timestamp by verifying module name
    if is_pandas_timestamp(value) {
        // For NTZ, reject tz-aware pandas Timestamps for consistency with datetime behavior
        if let Ok(tz) = value.getattr("tz") {
            if !tz.is_none() {
                return Err(FlussError::new_err(
                    "TIMESTAMP (without timezone) requires a naive pd.Timestamp. \
                     Got timezone-aware Timestamp. Either use tz_localize(None) or use TIMESTAMP_LTZ column.",
                ));
            }
        }
        // Naive pandas Timestamp: .value is nanoseconds since epoch (wall-clock as UTC)
        let nanos: i64 = value.getattr("value")?.extract()?;
        return Ok(nanos_to_millis_and_submillis(nanos));
    }

    // Try to_pydatetime() for objects that support it
    if let Ok(py_dt) = value.call_method0("to_pydatetime") {
        if let Ok(dt) = py_dt.downcast::<PyDateTime>() {
            let tzinfo = dt.getattr("tzinfo")?;
            if !tzinfo.is_none() {
                return Err(FlussError::new_err(
                    "TIMESTAMP (without timezone) requires a naive datetime. \
                     Got timezone-aware value. Use TIMESTAMP_LTZ column instead.",
                ));
            }
            return datetime_to_epoch_millis_as_utc(dt);
        }
    }

    Err(FlussError::new_err(format!(
        "Expected naive datetime.datetime or pd.Timestamp, got {}",
        get_type_name(value)
    )))
}

/// Extract epoch milliseconds for TimestampLtz (instant in time, UTC-based).
/// For naive datetimes, assumes UTC. For aware datetimes, converts to UTC.
fn extract_datetime_components_ltz(value: &Bound<PyAny>) -> PyResult<(i64, i32)> {
    // Try PyDateTime first
    if let Ok(dt) = value.downcast::<PyDateTime>() {
        // Check if timezone-aware
        let tzinfo = dt.getattr("tzinfo")?;
        if tzinfo.is_none() {
            // Naive datetime: assume UTC (treat components as UTC time)
            return datetime_to_epoch_millis_as_utc(dt);
        } else {
            // Aware datetime: use timedelta from epoch to get correct UTC instant
            return datetime_to_epoch_millis_utc_aware(dt);
        }
    }

    // Check for pandas Timestamp
    if is_pandas_timestamp(value) {
        // pandas Timestamp.value is always nanoseconds since UTC epoch
        let nanos: i64 = value.getattr("value")?.extract()?;
        return Ok(nanos_to_millis_and_submillis(nanos));
    }

    // Try to_pydatetime()
    if let Ok(py_dt) = value.call_method0("to_pydatetime") {
        if let Ok(dt) = py_dt.downcast::<PyDateTime>() {
            let tzinfo = dt.getattr("tzinfo")?;
            if tzinfo.is_none() {
                return datetime_to_epoch_millis_as_utc(dt);
            } else {
                return datetime_to_epoch_millis_utc_aware(dt);
            }
        }
    }

    Err(FlussError::new_err(format!(
        "Expected datetime.datetime or pd.Timestamp, got {}",
        get_type_name(value)
    )))
}

/// Convert datetime components to epoch milliseconds treating them as UTC
fn datetime_to_epoch_millis_as_utc(dt: &Bound<'_, PyDateTime>) -> PyResult<(i64, i32)> {
    let year = dt.get_year();
    let month = dt.get_month();
    let day = dt.get_day();
    let hour = dt.get_hour();
    let minute = dt.get_minute();
    let second = dt.get_second();
    let microsecond = dt.get_microsecond();

    // Create jiff civil datetime and convert to UTC timestamp
    // Safe casts: hour (0-23), minute (0-59), second (0-59) all fit in i8
    let civil_dt = jiff::civil::date(year as i16, month as i8, day as i8).at(
        hour as i8,
        minute as i8,
        second as i8,
        microsecond as i32 * 1000,
    );

    let timestamp = jiff::tz::Offset::UTC
        .to_timestamp(civil_dt)
        .map_err(|e| FlussError::new_err(format!("Invalid datetime: {e}")))?;

    let millis = timestamp.as_millisecond();
    let nano_of_milli = (timestamp.subsec_nanosecond() % NANOS_PER_MILLI as i32) as i32;

    Ok((millis, nano_of_milli))
}

/// Convert timezone-aware datetime to epoch milliseconds using Python's timedelta.
/// This correctly handles timezone conversions by computing (dt - UTC_EPOCH).
/// The UTC epoch is cached for performance.
fn datetime_to_epoch_millis_utc_aware(dt: &Bound<'_, PyDateTime>) -> PyResult<(i64, i32)> {
    let py = dt.py();
    let epoch = get_utc_epoch(py)?;

    // Compute delta = dt - epoch (this handles timezone conversion correctly)
    let delta = dt.call_method1("__sub__", (epoch,))?;
    let delta = delta.downcast::<PyDelta>()?;

    // Extract components using integer arithmetic
    let days = delta.get_days() as i64;
    let seconds = delta.get_seconds() as i64;
    let microseconds = delta.get_microseconds() as i64;

    // Total milliseconds (note: days can be negative for dates before epoch)
    let total_micros = days * MICROS_PER_DAY + seconds * MICROS_PER_SECOND + microseconds;
    let millis = total_micros / MICROS_PER_MILLI;
    let nano_of_milli = ((total_micros % MICROS_PER_MILLI) * MICROS_PER_MILLI) as i32;

    // Handle negative microseconds remainder
    let (millis, nano_of_milli) = if nano_of_milli < 0 {
        (millis - 1, nano_of_milli + NANOS_PER_MILLI as i32)
    } else {
        (millis, nano_of_milli)
    };

    Ok((millis, nano_of_milli))
}

/// Convert nanoseconds to (milliseconds, nano_of_millisecond)
fn nanos_to_millis_and_submillis(nanos: i64) -> (i64, i32) {
    let millis = nanos / NANOS_PER_MILLI;
    let nano_of_milli = (nanos % NANOS_PER_MILLI) as i32;

    // Handle negative nanoseconds correctly (Euclidean remainder)
    if nano_of_milli < 0 {
        (millis - 1, nano_of_milli + NANOS_PER_MILLI as i32)
    } else {
        (millis, nano_of_milli)
    }
}

/// Check if value is a pandas Timestamp by examining its type.
fn is_pandas_timestamp(value: &Bound<PyAny>) -> bool {
    // Check module and class name to avoid importing pandas
    if let Ok(cls) = value.get_type().getattr("__module__") {
        if let Ok(module) = cls.extract::<&str>() {
            if module.starts_with("pandas") {
                if let Ok(name) = value.get_type().getattr("__name__") {
                    if let Ok(name_str) = name.extract::<&str>() {
                        return name_str == "Timestamp";
                    }
                }
            }
        }
    }
    false
}

/// Get type name
fn get_type_name(value: &Bound<PyAny>) -> String {
    value
        .get_type()
        .name()
        .map(|s| s.to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

/// Thin Python iterator over [`fcore::client::SyncRecordBatchLogReader`].
/// Used internally as the backing iterator for
/// ``pa.RecordBatchReader.from_batches()``; not registered on the module.
#[pyclass]
struct PyRecordBatchLogReader {
    sync_reader: fcore::client::SyncRecordBatchLogReader,
}

#[pymethods]
impl PyRecordBatchLogReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python) -> PyResult<Option<Py<PyAny>>> {
        let result = py.detach(|| self.sync_reader.next().transpose());

        match result {
            Ok(Some(batch)) => {
                let py_batch = batch
                    .to_pyarrow(py)
                    .map_err(|e| FlussError::new_err(format!("Failed to convert batch: {e}")))?;
                Ok(Some(py_batch.unbind()))
            }
            Ok(None) => Ok(None),
            Err(arrow_err) => Err(FlussError::new_err(format!(
                "Error reading batch: {arrow_err}"
            ))),
        }
    }
}

/// Wraps the two scanner variants so we never have an impossible state
/// (both None or both Some).
enum ScannerKind {
    Record(fcore::client::LogScanner),
    Batch(fcore::client::RecordBatchLogScanner),
}

impl ScannerKind {
    fn as_record(&self) -> PyResult<&fcore::client::LogScanner> {
        match self {
            Self::Record(s) => Ok(s),
            Self::Batch(_) => Err(FlussError::new_err(
                "poll() requires a record-based scanner. Use new_scan().create_log_scanner().",
            )),
        }
    }

    fn as_batch(&self) -> PyResult<&fcore::client::RecordBatchLogScanner> {
        match self {
            Self::Batch(s) => Ok(s),
            Self::Record(_) => Err(FlussError::new_err(
                "This method requires a batch-based scanner. Use new_scan().create_record_batch_log_scanner().",
            )),
        }
    }
}

/// Dispatch a method call to whichever scanner variant is active.
/// Both `LogScanner` and `RecordBatchLogScanner` share the same subscribe interface.
macro_rules! with_scanner {
    ($scanner:expr, $method:ident($($arg:expr),*)) => {
        match $scanner.as_ref() {
            ScannerKind::Record(s) => s.$method($($arg),*).await,
            ScannerKind::Batch(s) => s.$method($($arg),*).await,
        }
    };
}

/// Scanner for reading log data from a Fluss table.
///
/// This scanner supports two modes:
/// - Record-based scanning via `poll()` - returns individual records with metadata
/// - Batch-based scanning via `poll_arrow()` / `poll_record_batch()` - returns Arrow batches
#[pyclass]
pub struct LogScanner {
    kind: Arc<ScannerKind>,
    admin: Arc<fcore::client::FlussAdmin>,
    table_info: fcore::metadata::TableInfo,
    /// The projected Arrow schema to use for empty table creation
    projected_schema: SchemaRef,
    /// The projected row type to use for record-based scanning
    projected_row_type: Arc<fcore::metadata::RowType>,
}

#[pymethods]
impl LogScanner {
    /// Subscribe to a single bucket at a specific offset (non-partitioned tables).
    ///
    /// Args:
    ///     bucket_id: The bucket ID to subscribe to
    ///     start_offset: The offset to start reading from (use EARLIEST_OFFSET for beginning)
    fn subscribe(&self, py: Python, bucket_id: i32, start_offset: i64) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(&self.kind, subscribe(bucket_id, start_offset))
                    .map_err(|e| FlussError::from_core_error(&e))
            })
        })
    }

    /// Subscribe to multiple buckets at specified offsets (non-partitioned tables).
    ///
    /// Args:
    ///     bucket_offsets: A dict mapping bucket_id -> start_offset
    fn subscribe_buckets(&self, py: Python, bucket_offsets: HashMap<i32, i64>) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(&self.kind, subscribe_buckets(&bucket_offsets))
                    .map_err(|e| FlussError::from_core_error(&e))
            })
        })
    }

    /// Subscribe to a bucket within a specific partition (partitioned tables only).
    ///
    /// Args:
    ///     partition_id: The partition ID (from PartitionInfo.partition_id)
    ///     bucket_id: The bucket ID within the partition
    ///     start_offset: The offset to start reading from (use EARLIEST_OFFSET for beginning)
    fn subscribe_partition(
        &self,
        py: Python,
        partition_id: i64,
        bucket_id: i32,
        start_offset: i64,
    ) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(
                    &self.kind,
                    subscribe_partition(partition_id, bucket_id, start_offset)
                )
                .map_err(|e| FlussError::from_core_error(&e))
            })
        })
    }

    /// Subscribe to multiple partition+bucket combinations at once (partitioned tables only).
    ///
    /// Args:
    ///     partition_bucket_offsets: A dict mapping (partition_id, bucket_id) tuples to start_offsets
    fn subscribe_partition_buckets(
        &self,
        py: Python,
        partition_bucket_offsets: HashMap<(i64, i32), i64>,
    ) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(
                    &self.kind,
                    subscribe_partition_buckets(&partition_bucket_offsets)
                )
                .map_err(|e| FlussError::from_core_error(&e))
            })
        })
    }

    /// Unsubscribe from a specific bucket (non-partitioned tables only).
    ///
    /// Args:
    ///     bucket_id: The bucket ID to unsubscribe from
    fn unsubscribe(&self, py: Python, bucket_id: i32) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(&self.kind, unsubscribe(bucket_id))
                    .map_err(|e| FlussError::from_core_error(&e))
            })
        })
    }

    /// Unsubscribe from a specific partition bucket (partitioned tables only).
    ///
    /// Args:
    ///     partition_id: The partition ID to unsubscribe from
    ///     bucket_id: The bucket ID within the partition
    fn unsubscribe_partition(&self, py: Python, partition_id: i64, bucket_id: i32) -> PyResult<()> {
        py.detach(|| {
            TOKIO_RUNTIME.block_on(async {
                with_scanner!(&self.kind, unsubscribe_partition(partition_id, bucket_id))
                    .map_err(|e| FlussError::from_core_error(&e))
            })
        })
    }

    /// Poll for individual records with metadata.
    ///
    /// Args:
    ///     timeout_ms: Timeout in milliseconds to wait for records
    ///
    /// Returns:
    ///     ScanRecords grouped by bucket. Supports flat iteration
    ///     (`for rec in records`) and per-bucket access (`records.buckets()`,
    ///     `records.records(bucket)`, `records[bucket]`).
    ///
    /// Note:
    ///     - Requires a record-based scanner (created with new_scan().create_log_scanner())
    ///     - Returns an empty ScanRecords if no records are available
    ///     - When timeout expires, returns an empty ScanRecords (NOT an error)
    fn poll<'py>(&self, py: Python<'py>, timeout_ms: i64) -> PyResult<Bound<'py, PyAny>> {
        if timeout_ms < 0 {
            return Err(FlussError::new_err(format!(
                "timeout_ms must be non-negative, got: {timeout_ms}"
            )));
        }

        let timeout = Duration::from_millis(timeout_ms as u64);
        let scanner = Arc::clone(&self.kind);
        let projected_row_type = self.projected_row_type.clone();

        future_into_py(py, async move {
            let scan_records = scanner
                .as_record()?
                .poll(timeout)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                let mut records_by_bucket = IndexMap::new();
                let mut total_count = 0usize;

                for (bucket, records) in scan_records.into_records_by_buckets() {
                    let py_bucket = TableBucket::from_core(bucket);
                    let mut py_records = Vec::with_capacity(records.len());
                    for record in &records {
                        let scan_record = ScanRecord::from_core(py, record, &projected_row_type)?;
                        py_records.push(Py::new(py, scan_record)?);
                        total_count += 1;
                    }
                    records_by_bucket.insert(py_bucket, py_records);
                }

                Ok(ScanRecords {
                    records_by_bucket,
                    total_count,
                })
            })
        })
    }

    /// Poll for batches with metadata.
    ///
    /// Args:
    ///     timeout_ms: Timeout in milliseconds to wait for batches
    ///
    /// Returns:
    ///     List of RecordBatch objects, each containing the Arrow batch along with
    ///     bucket, base_offset, and last_offset metadata.
    ///
    /// Note:
    ///     - Requires a batch-based scanner (created with new_scan().create_record_batch_log_scanner())
    ///     - Returns an empty list if no batches are available
    ///     - When timeout expires, returns an empty list (NOT an error)
    fn poll_record_batch<'py>(
        &self,
        py: Python<'py>,
        timeout_ms: i64,
    ) -> PyResult<Bound<'py, PyAny>> {
        if timeout_ms < 0 {
            return Err(FlussError::new_err(format!(
                "timeout_ms must be non-negative, got: {timeout_ms}"
            )));
        }

        let timeout = Duration::from_millis(timeout_ms as u64);
        let scanner = Arc::clone(&self.kind);

        future_into_py(py, async move {
            let scan_batches = scanner
                .as_batch()?
                .poll(timeout)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            Python::attach(|py| {
                scan_batches
                    .into_iter()
                    .map(|sb| Py::new(py, RecordBatch::from_scan_batch(sb)))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Poll for new records as an Arrow Table.
    ///
    /// Args:
    ///     timeout_ms: Timeout in milliseconds to wait for records
    ///
    /// Returns:
    ///     PyArrow Table containing the polled records (batches merged)
    ///
    /// Note:
    ///     - Requires a batch-based scanner (created with new_scan().create_record_batch_log_scanner())
    ///     - Returns an empty table (with correct schema) if no records are available
    ///     - When timeout expires, returns an empty table (NOT an error)
    fn poll_arrow<'py>(&self, py: Python<'py>, timeout_ms: i64) -> PyResult<Bound<'py, PyAny>> {
        if timeout_ms < 0 {
            return Err(FlussError::new_err(format!(
                "timeout_ms must be non-negative, got: {timeout_ms}"
            )));
        }

        let timeout = Duration::from_millis(timeout_ms as u64);
        let scanner = Arc::clone(&self.kind);
        let projected_schema = self.projected_schema.clone();

        future_into_py(py, async move {
            let scan_batches = scanner
                .as_batch()?
                .poll(timeout)
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;

            let arrow_batches = scan_batches
                .into_iter()
                .map(|sb| Arc::new(sb.into_batch()))
                .collect();

            Python::attach(|py| Self::batches_to_arrow_table(py, arrow_batches, &projected_schema))
        })
    }

    /// Create a lazy Arrow RecordBatchReader that reads until latest offsets.
    ///
    /// This is a **blocking / synchronous** API: construction queries the
    /// server for latest offsets (via ``block_on``), and each
    /// ``RecordBatchReader.__next__()`` call blocks the calling thread until
    /// the next batch is available. It is suitable for Arrow interop
    /// (feeding into DuckDB, Polars, etc.) but should not be used
    /// from ``asyncio`` coroutines -- see issue #545 for a planned
    /// asyncio-native streaming alternative.
    /// TODO(#545): Add asyncio-native streaming counterpart.
    ///
    /// Returns a PyArrow RecordBatchReader that lazily polls batches one at a
    /// time. This is more memory-efficient than ``to_arrow()`` which loads all
    /// data into a single table.
    ///
    /// **Concurrency:** While this reader is alive, ``subscribe*`` and
    /// ``unsubscribe*`` calls on the scanner are rejected with an error.
    /// You should also avoid calling ``poll_arrow`` / ``poll_record_batch``
    /// on the same scanner — these are not blocked by the guard, but they
    /// share the underlying fetch buffer with the reader and would
    /// interleave batches between both consumers. Drop the reader before
    /// resuming any of these operations.
    ///
    /// You must call subscribe(), subscribe_buckets(), subscribe_partition(),
    /// or subscribe_partition_buckets() first.
    ///
    /// Returns:
    ///     ``pyarrow.RecordBatchReader`` yielding ``RecordBatch`` objects
    fn to_arrow_batch_reader(&self, py: Python) -> PyResult<Py<PyAny>> {
        let scanner = self.kind.as_batch()?;

        let sync_reader = py
            .detach(|| {
                TOKIO_RUNTIME.block_on(async {
                    let reader = fcore::client::RecordBatchLogReader::new_until_latest(
                        scanner.new_shared_handle(),
                        &self.admin,
                    )
                    .await?;
                    Ok::<_, fcore::error::Error>(
                        reader.to_record_batch_reader(TOKIO_RUNTIME.handle().clone()),
                    )
                })
            })
            .map_err(|e| FlussError::from_core_error(&e))?;

        let py_schema = sync_reader
            .schema()
            .to_pyarrow(py)
            .map_err(|e| FlussError::new_err(format!("Failed to convert schema: {e}")))?;

        let py_iter = Py::new(py, PyRecordBatchLogReader { sync_reader })?;

        let pyarrow = py.import("pyarrow")?;
        let batch_reader = pyarrow
            .getattr("RecordBatchReader")?
            .call_method1("from_batches", (py_schema, py_iter))?;

        Ok(batch_reader.into())
    }

    /// Convert all data to Arrow Table.
    ///
    /// Reads from currently subscribed buckets until reaching their latest offsets.
    /// Works for both partitioned and non-partitioned tables.
    ///
    /// Materializes batches in Rust (``RecordBatchLogReader::collect_all_batches``)
    /// then builds one PyArrow table, avoiding per-batch Python iteration.
    ///
    /// You must call subscribe(), subscribe_buckets(), subscribe_partition(), or subscribe_partition_buckets() first.
    ///
    /// Returns:
    ///     PyArrow Table containing all data from subscribed buckets
    fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(
            py,
            Self::scan_to_arrow_table(
                Arc::clone(&self.kind),
                Arc::clone(&self.admin),
                self.projected_schema.clone(),
            ),
        )
    }

    /// Convert all data to Pandas DataFrame.
    ///
    /// Reads from currently subscribed buckets until reaching their latest offsets.
    /// Works for both partitioned and non-partitioned tables.
    ///
    /// You must call subscribe(), subscribe_buckets(), subscribe_partition(), or subscribe_partition_buckets() first.
    ///
    /// Returns:
    ///     Pandas DataFrame containing all data from subscribed buckets
    fn to_pandas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let kind = Arc::clone(&self.kind);
        let admin = Arc::clone(&self.admin);
        let projected_schema = self.projected_schema.clone();
        future_into_py(py, async move {
            let table = Self::scan_to_arrow_table(kind, admin, projected_schema).await?;
            Python::attach(|py| table.call_method0(py, "to_pandas"))
        })
    }

    fn __aiter__<'py>(slf: PyRef<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();

        // Single lock for the generic async generator
        static ASYNC_GEN_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

        let gen_fn = ASYNC_GEN_FN.get_or_init(py, || {
            let code = pyo3::ffi::c_str!(
                r#"
async def _async_scan_generic(scanner, method_name, timeout_ms):
    poll_method = getattr(scanner, method_name)
    while True:
        for item in await poll_method(timeout_ms):
            yield item
"#
            );
            let globals = pyo3::types::PyDict::new(py);
            py.run(code, Some(&globals), None).unwrap();
            globals
                .get_item("_async_scan_generic")
                .unwrap()
                .unwrap()
                .unbind()
        });

        let method_name = match slf.kind.as_ref() {
            ScannerKind::Record(_) => "poll",
            ScannerKind::Batch(_) => "poll_record_batch",
        };

        gen_fn.bind(py).call1((
            slf.into_bound_py_any(py)?,
            method_name,
            DEFAULT_POLL_INTERVAL_MS,
        ))
    }

    fn __repr__(&self) -> String {
        format!("LogScanner(table={})", self.table_info.table_path)
    }
}

impl LogScanner {
    fn new(
        scanner: ScannerKind,
        admin: Arc<fcore::client::FlussAdmin>,
        table_info: fcore::metadata::TableInfo,
        projected_schema: SchemaRef,
        projected_row_type: Arc<fcore::metadata::RowType>,
    ) -> Self {
        Self {
            kind: Arc::new(scanner),
            admin,
            table_info,
            projected_schema,
            projected_row_type,
        }
    }

    /// Read until the latest offsets and build one PyArrow Table.
    async fn scan_to_arrow_table(
        kind: Arc<ScannerKind>,
        admin: Arc<fcore::client::FlussAdmin>,
        projected_schema: SchemaRef,
    ) -> PyResult<Py<PyAny>> {
        let scanner = kind.as_batch()?;
        let mut reader = fcore::client::RecordBatchLogReader::new_until_latest(
            scanner.new_shared_handle(),
            &admin,
        )
        .await
        .map_err(|e| FlussError::from_core_error(&e))?;
        let batches: Vec<Arc<ArrowRecordBatch>> = reader
            .collect_all_batches()
            .await
            .map_err(|e| FlussError::from_core_error(&e))?
            .into_iter()
            .map(|sb| Arc::new(sb.into_batch()))
            .collect();
        Python::attach(|py| Self::batches_to_arrow_table(py, batches, &projected_schema))
    }

    /// Convert Arrow record batches to a PyArrow Table (or empty table if no batches).
    fn batches_to_arrow_table(
        py: Python<'_>,
        batches: Vec<Arc<ArrowRecordBatch>>,
        projected_schema: &SchemaRef,
    ) -> PyResult<Py<PyAny>> {
        if batches.is_empty() {
            let py_schema = projected_schema
                .as_ref()
                .to_pyarrow(py)
                .map_err(|e| FlussError::new_err(format!("Failed to convert schema: {e}")))?;
            let pyarrow = py.import("pyarrow")?;
            let empty_table = pyarrow
                .getattr("Table")?
                .call_method1("from_batches", (vec![] as Vec<Py<PyAny>>, py_schema))?;
            Ok(empty_table.into())
        } else {
            Utils::combine_batches_to_table(py, batches)
        }
    }
}

/// One-shot bounded scanner over a single bucket.
///
/// Obtained via `table.new_scan().limit(n).create_bucket_batch_scanner(bucket)`.
/// The scan runs on the first `next_batch()` and yields its single batch once,
/// then is spent. Honors the configured limit and any projection.
#[pyclass]
pub struct BatchScanner {
    inner: Arc<Mutex<LimitBatchScanner>>,
    bucket: TableBucket,
    projected_schema: SchemaRef,
}

#[pymethods]
impl BatchScanner {
    /// The bucket scanned by this batch scanner.
    #[getter]
    fn bucket(&self) -> TableBucket {
        self.bucket.clone()
    }

    /// Run the scan and return its batch, or `None` once the scanner is spent.
    ///
    /// The scan runs on the first call and is not retried; on error, create a
    /// new scanner.
    fn next_batch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut scanner = inner.lock().await;
            let batch = scanner
                .next_batch()
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;
            Python::attach(|py| match batch {
                Some(sb) => Ok(Some(Py::new(py, RecordBatch::from_scan_batch(sb))?)),
                None => Ok(None),
            })
        })
    }

    /// Drain the scanner into all of its batches.
    fn collect_all_batches<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_py(py, async move {
            let mut scanner = inner.lock().await;
            let batches = scanner
                .collect_all_batches()
                .await
                .map_err(|e| FlussError::from_core_error(&e))?;
            Python::attach(|py| {
                batches
                    .into_iter()
                    .map(|sb| Py::new(py, RecordBatch::from_scan_batch(sb)))
                    .collect::<PyResult<Vec<_>>>()
            })
        })
    }

    /// Drain the scanner into a PyArrow Table (empty, with the projected schema,
    /// when the scan yields nothing).
    fn to_arrow<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(
            py,
            Self::scan_to_arrow_table(Arc::clone(&self.inner), self.projected_schema.clone()),
        )
    }

    /// Drain the scanner into a Pandas DataFrame.
    fn to_pandas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        let projected_schema = self.projected_schema.clone();
        future_into_py(py, async move {
            let table = Self::scan_to_arrow_table(inner, projected_schema).await?;
            Python::attach(|py| table.call_method0(py, "to_pandas"))
        })
    }

    fn __repr__(&self) -> String {
        format!("BatchScanner(bucket={})", self.bucket.__str__())
    }
}

impl BatchScanner {
    fn new(scanner: LimitBatchScanner, bucket: TableBucket, projected_schema: SchemaRef) -> Self {
        Self {
            inner: Arc::new(Mutex::new(scanner)),
            bucket,
            projected_schema,
        }
    }

    /// Drain the scanner into one PyArrow Table.
    async fn scan_to_arrow_table(
        inner: Arc<Mutex<LimitBatchScanner>>,
        projected_schema: SchemaRef,
    ) -> PyResult<Py<PyAny>> {
        let mut scanner = inner.lock().await;
        let batches = scanner
            .collect_all_batches()
            .await
            .map_err(|e| FlussError::from_core_error(&e))?
            .into_iter()
            .map(|sb| Arc::new(sb.into_batch()))
            .collect();
        Python::attach(|py| LogScanner::batches_to_arrow_table(py, batches, &projected_schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanos_to_millis_and_submillis() {
        // Simple positive case
        assert_eq!(nanos_to_millis_and_submillis(1_500_000), (1, 500_000));

        // Exact millisecond boundary
        assert_eq!(nanos_to_millis_and_submillis(2_000_000), (2, 0));

        // Zero
        assert_eq!(nanos_to_millis_and_submillis(0), (0, 0));

        // Large value
        assert_eq!(
            nanos_to_millis_and_submillis(86_400_000_000_000), // 1 day in nanos
            (86_400_000, 0)
        );

        // Negative: -1.5 milliseconds should be (-2 millis, +500_000 nanos)
        // Because -1_500_000 nanos = -2ms + 500_000ns
        assert_eq!(nanos_to_millis_and_submillis(-1_500_000), (-2, 500_000));

        // Negative exact boundary
        assert_eq!(nanos_to_millis_and_submillis(-2_000_000), (-2, 0));

        // Small negative
        assert_eq!(nanos_to_millis_and_submillis(-1), (-1, 999_999));

        // Negative with sub-millisecond part
        assert_eq!(nanos_to_millis_and_submillis(-500_000), (-1, 500_000));
    }
}
