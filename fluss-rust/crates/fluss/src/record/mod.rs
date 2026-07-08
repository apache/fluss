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

use crate::metadata::TableBucket;
use crate::row::ColumnarRow;
use ::arrow::array::RecordBatch;
use core::fmt;
use std::collections::HashMap;

mod arrow;
mod error;
pub mod kv;

pub use arrow::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeType {
    /// Append-only operation
    AppendOnly,
    /// Insert operation
    Insert,
    /// Update operation containing the previous content of the updated row
    UpdateBefore,
    /// Update operation containing the new content of the updated row
    UpdateAfter,
    /// Delete operation
    Delete,
}

impl ChangeType {
    /// Returns a short string representation of this ChangeType
    pub fn short_string(&self) -> &'static str {
        match self {
            ChangeType::AppendOnly => "+A",
            ChangeType::Insert => "+I",
            ChangeType::UpdateBefore => "-U",
            ChangeType::UpdateAfter => "+U",
            ChangeType::Delete => "-D",
        }
    }

    /// Returns the byte value representation used for serialization
    pub fn to_byte_value(&self) -> u8 {
        match self {
            ChangeType::AppendOnly => 0,
            ChangeType::Insert => 1,
            ChangeType::UpdateBefore => 2,
            ChangeType::UpdateAfter => 3,
            ChangeType::Delete => 4,
        }
    }

    /// Creates a ChangeType from its byte value representation
    ///
    /// # Errors
    /// Returns an error if the byte value doesn't correspond to any ChangeType
    pub fn from_byte_value(value: u8) -> Result<Self, String> {
        match value {
            0 => Ok(ChangeType::AppendOnly),
            1 => Ok(ChangeType::Insert),
            2 => Ok(ChangeType::UpdateBefore),
            3 => Ok(ChangeType::UpdateAfter),
            4 => Ok(ChangeType::Delete),
            _ => Err(format!("Unsupported byte value '{value}' for change type")),
        }
    }
}

impl fmt::Display for ChangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.short_string())
    }
}

#[derive(Clone)]
pub struct ScanRecord {
    pub row: ColumnarRow,
    offset: i64,
    timestamp: i64,
    change_type: ChangeType,
}

impl ScanRecord {
    const INVALID: i64 = -1;

    pub fn new_default(row: ColumnarRow) -> Self {
        ScanRecord {
            row,
            offset: Self::INVALID,
            timestamp: Self::INVALID,
            change_type: ChangeType::Insert,
        }
    }

    pub fn new(row: ColumnarRow, offset: i64, timestamp: i64, change_type: ChangeType) -> Self {
        ScanRecord {
            row,
            offset,
            timestamp,
            change_type,
        }
    }

    pub fn row(&self) -> &ColumnarRow {
        &self.row
    }

    /// Returns the position in the log
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the timestamp
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the change type
    pub fn change_type(&self) -> &ChangeType {
        &self.change_type
    }
}

pub struct ScanRecords {
    records: HashMap<TableBucket, Vec<ScanRecord>>,
}

impl ScanRecords {
    pub fn empty() -> Self {
        Self {
            records: HashMap::new(),
        }
    }

    pub fn new(records: HashMap<TableBucket, Vec<ScanRecord>>) -> Self {
        Self { records }
    }

    pub fn records(&self, scan_bucket: &TableBucket) -> &[ScanRecord] {
        self.records.get(scan_bucket).map_or(&[], |records| records)
    }

    pub fn count(&self) -> usize {
        self.records.values().map(|v| v.len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn records_by_buckets(&self) -> &HashMap<TableBucket, Vec<ScanRecord>> {
        &self.records
    }

    pub fn into_records_by_buckets(self) -> HashMap<TableBucket, Vec<ScanRecord>> {
        self.records
    }
}

/// A batch of records with metadata about bucket and offsets.
///
/// This is the batch-level equivalent of [`ScanRecord`], providing efficient
/// access to Arrow RecordBatches while preserving the bucket and offset information
/// needed for tracking consumption progress.
#[derive(Debug, Clone)]
pub struct ScanBatch {
    /// The bucket this batch belongs to
    bucket: TableBucket,
    /// The Arrow RecordBatch containing the data
    batch: RecordBatch,
    /// Offset of the first record in this batch
    base_offset: i64,
}

impl ScanBatch {
    pub fn new(bucket: TableBucket, batch: RecordBatch, base_offset: i64) -> Self {
        Self {
            bucket,
            batch,
            base_offset,
        }
    }

    pub fn bucket(&self) -> &TableBucket {
        &self.bucket
    }

    pub fn batch(&self) -> &RecordBatch {
        &self.batch
    }

    pub fn into_batch(self) -> RecordBatch {
        self.batch
    }

    pub fn base_offset(&self) -> i64 {
        self.base_offset
    }

    pub fn num_records(&self) -> usize {
        self.batch.num_rows()
    }

    /// Returns the offset of the last record in this batch.
    pub fn last_offset(&self) -> i64 {
        if self.batch.num_rows() == 0 {
            self.base_offset - 1
        } else {
            self.base_offset + self.batch.num_rows() as i64 - 1
        }
    }
}

impl IntoIterator for ScanRecords {
    type Item = ScanRecord;
    type IntoIter = std::vec::IntoIter<ScanRecord>;

    fn into_iter(self) -> Self::IntoIter {
        self.records
            .into_values()
            .flatten()
            .collect::<Vec<_>>()
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::arrow::array::{Int32Array, RecordBatch};
    use ::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_row(values: Vec<i32>, row_id: usize) -> ColumnarRow {
        use crate::metadata::RowType;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))])
            .expect("record batch");
        let row_type = Arc::new(RowType::with_data_types(vec![
            crate::metadata::DataType::Int(crate::metadata::IntType::new()),
        ]));
        ColumnarRow::new(Arc::new(batch), row_type, row_id, None).expect("ColumnarRow")
    }

    #[test]
    fn change_type_round_trip() {
        let cases = [
            (ChangeType::AppendOnly, "+A", 0),
            (ChangeType::Insert, "+I", 1),
            (ChangeType::UpdateBefore, "-U", 2),
            (ChangeType::UpdateAfter, "+U", 3),
            (ChangeType::Delete, "-D", 4),
        ];

        for (change_type, short, byte) in cases {
            assert_eq!(change_type.short_string(), short);
            assert_eq!(change_type.to_byte_value(), byte);
            assert_eq!(ChangeType::from_byte_value(byte).unwrap(), change_type);
        }

        let err = ChangeType::from_byte_value(9).unwrap_err();
        assert!(err.contains("Unsupported byte value"));
    }

    #[test]
    fn scan_records_counts_and_iterates() {
        let bucket0 = TableBucket::new(1, 0);
        let bucket1 = TableBucket::new(1, 1);
        let record0 = ScanRecord::new(make_row(vec![10, 11], 0), 5, 7, ChangeType::Insert);
        let record1 = ScanRecord::new(make_row(vec![10, 11], 1), 6, 8, ChangeType::Delete);

        let mut records = HashMap::new();
        records.insert(bucket0.clone(), vec![record0.clone(), record1.clone()]);

        let scan_records = ScanRecords::new(records);
        assert_eq!(scan_records.records(&bucket0).len(), 2);
        assert!(scan_records.records(&bucket1).is_empty());
        assert_eq!(scan_records.count(), 2);

        let collected: Vec<_> = scan_records.into_iter().collect();
        assert_eq!(collected.len(), 2);
    }

    #[test]
    fn scan_record_default_values() {
        let record = ScanRecord::new_default(make_row(vec![1], 0));
        assert_eq!(record.offset(), -1);
        assert_eq!(record.timestamp(), -1);
        assert_eq!(record.change_type(), &ChangeType::Insert);
    }

    #[test]
    fn scan_batch_last_offset() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let bucket = TableBucket::new(1, 0);

        // Batch with 3 records starting at offset 100 -> last_offset = 102
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let scan_batch = ScanBatch::new(bucket.clone(), batch, 100);
        assert_eq!(scan_batch.num_records(), 3);
        assert_eq!(scan_batch.last_offset(), 102);

        // Empty batch -> last_offset = base_offset - 1
        let empty_batch = RecordBatch::new_empty(schema);
        let empty_scan_batch = ScanBatch::new(bucket, empty_batch, 100);
        assert_eq!(empty_scan_batch.num_records(), 0);
        assert_eq!(empty_scan_batch.last_offset(), 99);
    }
}
