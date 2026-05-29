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

use crate::client::broadcast::{BatchWriteResult, BroadcastOnce};
use crate::client::{Record, ResultHandle, WriteRecord};
use crate::compression::{ArrowCompressionInfo, ArrowCompressionRatioEstimator};
use crate::error::{Error, Result};
use crate::metadata::{KvFormat, PhysicalTablePath, RowType};
use crate::record::MemoryLogRecordsArrowBuilder;
use crate::record::kv::KvRecordBatchBuilder;
use crate::record::{NO_BATCH_SEQUENCE, NO_WRITER_ID};
use bytes::Bytes;
use std::cmp::max;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

pub struct InnerWriteBatch {
    batch_id: i64,
    physical_table_path: Arc<PhysicalTablePath>,
    create_ms: i64,
    results: BroadcastOnce<BatchWriteResult>,
    completed: AtomicBool,
    attempts: AtomicI32,
    drained_ms: i64,
    batch_sequence: i32,
    writer_id: i64,
}

impl InnerWriteBatch {
    fn new(batch_id: i64, physical_table_path: Arc<PhysicalTablePath>, create_ms: i64) -> Self {
        InnerWriteBatch {
            batch_id,
            physical_table_path,
            create_ms,
            results: Default::default(),
            completed: AtomicBool::new(false),
            attempts: AtomicI32::new(0),
            drained_ms: -1,
            batch_sequence: NO_BATCH_SEQUENCE,
            writer_id: NO_WRITER_ID,
        }
    }

    pub fn batch_sequence(&self) -> i32 {
        self.batch_sequence
    }

    pub fn writer_id(&self) -> i64 {
        self.writer_id
    }

    pub fn has_batch_sequence(&self) -> bool {
        self.batch_sequence != NO_BATCH_SEQUENCE
    }

    fn waited_time_ms(&self, now: i64) -> i64 {
        max(0i64, now - self.create_ms)
    }

    fn complete(&self, write_result: BatchWriteResult) -> bool {
        if self
            .completed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return false;
        }
        self.results.broadcast(write_result);
        true
    }

    fn drained(&mut self, now_ms: i64) {
        self.drained_ms = max(self.drained_ms, now_ms);
    }

    fn physical_table_path(&self) -> &Arc<PhysicalTablePath> {
        &self.physical_table_path
    }

    fn attempts(&self) -> i32 {
        self.attempts.load(Ordering::Acquire)
    }

    fn re_enqueued(&self) {
        self.attempts.fetch_add(1, Ordering::AcqRel);
    }

    fn is_done(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }
}

pub enum WriteBatch {
    ArrowLog(ArrowLogWriteBatch),
    Kv(KvWriteBatch),
}

impl WriteBatch {
    pub fn inner_batch(&self) -> &InnerWriteBatch {
        match self {
            WriteBatch::ArrowLog(batch) => &batch.write_batch,
            WriteBatch::Kv(batch) => &batch.write_batch,
        }
    }

    pub fn inner_batch_mut(&mut self) -> &mut InnerWriteBatch {
        match self {
            WriteBatch::ArrowLog(batch) => &mut batch.write_batch,
            WriteBatch::Kv(batch) => &mut batch.write_batch,
        }
    }

    pub fn try_append(&mut self, write_record: &WriteRecord) -> Result<Option<ResultHandle>> {
        match self {
            WriteBatch::ArrowLog(batch) => batch.try_append(write_record),
            WriteBatch::Kv(batch) => batch.try_append(write_record),
        }
    }

    pub fn waited_time_ms(&self, now: i64) -> i64 {
        self.inner_batch().waited_time_ms(now)
    }

    pub fn close(&mut self) -> Result<()> {
        match self {
            WriteBatch::ArrowLog(batch) => {
                batch.close();
                Ok(())
            }
            WriteBatch::Kv(batch) => batch.close(),
        }
    }

    pub fn estimated_size_in_bytes(&self) -> usize {
        match self {
            WriteBatch::ArrowLog(batch) => batch.estimated_size_in_bytes(),
            WriteBatch::Kv(batch) => batch.estimated_size_in_bytes(),
        }
    }

    pub fn is_closed(&self) -> bool {
        match self {
            WriteBatch::ArrowLog(batch) => batch.is_closed(),
            WriteBatch::Kv(batch) => batch.is_closed(),
        }
    }

    pub fn drained(&mut self, now_ms: i64) {
        self.inner_batch_mut().drained(now_ms);
    }

    pub fn build(&mut self) -> Result<Bytes> {
        match self {
            WriteBatch::ArrowLog(batch) => batch.build(),
            WriteBatch::Kv(batch) => batch.build(),
        }
    }

    pub fn complete(&self, write_result: BatchWriteResult) -> bool {
        self.inner_batch().complete(write_result)
    }

    pub fn batch_id(&self) -> i64 {
        self.inner_batch().batch_id
    }

    pub fn physical_table_path(&self) -> &Arc<PhysicalTablePath> {
        self.inner_batch().physical_table_path()
    }

    pub fn attempts(&self) -> i32 {
        self.inner_batch().attempts()
    }

    pub fn re_enqueued(&self) {
        self.inner_batch().re_enqueued();
    }

    pub fn is_done(&self) -> bool {
        self.inner_batch().is_done()
    }

    pub fn batch_sequence(&self) -> i32 {
        self.inner_batch().batch_sequence()
    }

    pub fn writer_id(&self) -> i64 {
        self.inner_batch().writer_id()
    }

    pub fn has_batch_sequence(&self) -> bool {
        self.inner_batch().has_batch_sequence()
    }

    pub fn set_writer_state(&mut self, writer_id: i64, batch_base_sequence: i32) {
        match self {
            WriteBatch::ArrowLog(batch) => batch.set_writer_state(writer_id, batch_base_sequence),
            WriteBatch::Kv(batch) => batch.set_writer_state(writer_id, batch_base_sequence),
        }
    }
}

pub struct ArrowLogWriteBatch {
    pub write_batch: InnerWriteBatch,
    pub arrow_builder: MemoryLogRecordsArrowBuilder,
    built_records: Option<Bytes>,
}

impl ArrowLogWriteBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_id: i64,
        physical_table_path: Arc<PhysicalTablePath>,
        schema_id: i32,
        arrow_compression_info: ArrowCompressionInfo,
        row_type: &RowType,
        create_ms: i64,
        to_append_record_batch: bool,
        write_limit: usize,
        compression_ratio_estimator: Arc<ArrowCompressionRatioEstimator>,
    ) -> Result<Self> {
        let base = InnerWriteBatch::new(batch_id, physical_table_path, create_ms);
        Ok(Self {
            write_batch: base,
            arrow_builder: MemoryLogRecordsArrowBuilder::new(
                schema_id,
                row_type,
                to_append_record_batch,
                arrow_compression_info,
                write_limit,
                compression_ratio_estimator,
            )?,
            built_records: None,
        })
    }

    pub fn batch_id(&self) -> i64 {
        self.write_batch.batch_id
    }

    pub fn try_append(&mut self, write_record: &WriteRecord) -> Result<Option<ResultHandle>> {
        if self.arrow_builder.is_closed() || self.arrow_builder.is_full() {
            Ok(None)
        } else {
            // append successfully
            if self.arrow_builder.append(write_record)? {
                Ok(Some(ResultHandle::new(self.write_batch.results.receiver())))
            } else {
                // append fail
                Ok(None)
            }
        }
    }

    pub fn set_writer_state(&mut self, writer_id: i64, batch_base_sequence: i32) {
        self.arrow_builder
            .set_writer_state(writer_id, batch_base_sequence);
        self.write_batch.batch_sequence = batch_base_sequence;
        self.write_batch.writer_id = writer_id;
        self.built_records = None;
    }

    pub fn build(&mut self) -> Result<Bytes> {
        if let Some(bytes) = &self.built_records {
            return Ok(bytes.clone());
        }
        let bytes = Bytes::from(self.arrow_builder.build()?);
        self.built_records = Some(bytes.clone());
        Ok(bytes)
    }

    pub fn is_closed(&self) -> bool {
        self.arrow_builder.is_closed()
    }

    pub fn close(&mut self) {
        self.arrow_builder.close()
    }

    /// Get an estimate of the number of bytes written to the underlying buffer.
    /// The returned value is exactly correct if the batch has been built.
    pub fn estimated_size_in_bytes(&self) -> usize {
        if let Some(ref bytes) = self.built_records {
            // Return actual size if already built
            bytes.len()
        } else {
            // Delegate to arrow builder for estimated size
            self.arrow_builder.estimated_size_in_bytes()
        }
    }
}

pub struct KvWriteBatch {
    write_batch: InnerWriteBatch,
    kv_batch_builder: KvRecordBatchBuilder,
    target_columns: Option<Arc<Vec<usize>>>,
    schema_id: i32,
}

impl KvWriteBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_id: i64,
        physical_table_path: Arc<PhysicalTablePath>,
        schema_id: i32,
        write_limit: usize,
        kv_format: KvFormat,
        target_columns: Option<Arc<Vec<usize>>>,
        create_ms: i64,
    ) -> Self {
        let base = InnerWriteBatch::new(batch_id, physical_table_path, create_ms);
        Self {
            write_batch: base,
            kv_batch_builder: KvRecordBatchBuilder::new(schema_id, write_limit, kv_format),
            target_columns,
            schema_id,
        }
    }

    pub fn try_append(&mut self, write_record: &WriteRecord) -> Result<Option<ResultHandle>> {
        let kv_write_record = match &write_record.record {
            Record::Kv(record) => record,
            _ => {
                return Err(Error::UnsupportedOperation {
                    message: "Only KvRecord to append to KvWriteBatch ".to_string(),
                });
            }
        };

        let key = kv_write_record.key.as_ref();

        if self.schema_id != write_record.schema_id {
            return Err(Error::UnexpectedError {
                message: format!(
                    "schema id {} of the write record to append is not the same as the current schema id {} in the batch.",
                    write_record.schema_id, self.schema_id
                ),
                source: None,
            });
        };

        if self.target_columns != kv_write_record.target_columns {
            return Err(Error::UnexpectedError {
                message: format!(
                    "target columns {:?} of the write record to append are not the same as the current target columns {:?} in the batch.",
                    kv_write_record.target_columns,
                    self.target_columns.as_deref()
                ),
                source: None,
            });
        }

        let row_bytes = kv_write_record.row_bytes();

        if self.is_closed() || !self.kv_batch_builder.has_room_for_row(key, row_bytes) {
            Ok(None)
        } else {
            // append successfully
            self.kv_batch_builder
                .append_row(key, row_bytes)
                .map_err(|e| Error::UnexpectedError {
                    message: "Failed to append row to KvWriteBatch".to_string(),
                    source: Some(Box::new(e)),
                })?;
            Ok(Some(ResultHandle::new(self.write_batch.results.receiver())))
        }
    }

    pub fn build(&mut self) -> Result<Bytes> {
        self.kv_batch_builder.build()
    }

    pub fn is_closed(&self) -> bool {
        self.kv_batch_builder.is_closed()
    }

    pub fn close(&mut self) -> Result<()> {
        self.kv_batch_builder.close()
    }

    pub fn set_writer_state(&mut self, writer_id: i64, batch_base_sequence: i32) {
        self.kv_batch_builder
            .set_writer_state(writer_id, batch_base_sequence);
        self.write_batch.batch_sequence = batch_base_sequence;
        self.write_batch.writer_id = writer_id;
    }

    pub fn target_columns(&self) -> Option<&Arc<Vec<usize>>> {
        self.target_columns.as_ref()
    }

    /// Get an estimate of the number of bytes written to the underlying buffer.
    /// This returns the current size including header and all appended records.
    pub fn estimated_size_in_bytes(&self) -> usize {
        self.kv_batch_builder.get_size_in_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{RowBytes, WriteFormat};
    use crate::metadata::TablePath;
    use crate::test_utils::build_table_info;

    #[test]
    fn complete_only_once() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let physical_path = PhysicalTablePath::of(Arc::new(table_path));
        let batch = InnerWriteBatch::new(1, Arc::new(physical_path), 0);
        assert!(batch.complete(Ok(())));
        assert!(!batch.complete(Err(crate::client::broadcast::Error::Dropped)));
    }

    #[test]
    fn attempts_increment_on_reenqueue() {
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let physical_path = PhysicalTablePath::of(Arc::new(table_path));
        let batch = InnerWriteBatch::new(1, Arc::new(physical_path), 0);
        assert_eq!(batch.attempts(), 0);
        batch.re_enqueued();
        assert_eq!(batch.attempts(), 1);
    }

    #[test]
    fn test_arrow_log_write_batch_estimated_size() {
        use crate::client::WriteRecord;
        use crate::compression::{
            ArrowCompressionInfo, ArrowCompressionType, DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
        };
        use crate::metadata::{DataField, DataTypes, RowType};
        use crate::row::GenericRow;
        use arrow::array::{Int32Array, RecordBatch, StringArray};
        use std::sync::Arc;

        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        // Test 1: RowAppendRecordBatchBuilder (to_append_record_batch=false)
        {
            let mut batch = ArrowLogWriteBatch::new(
                1,
                Arc::clone(&physical_table_path),
                1,
                ArrowCompressionInfo {
                    compression_type: ArrowCompressionType::None,
                    compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
                },
                &row_type,
                0,
                false,
                2 * 1024 * 1024,
                Arc::new(ArrowCompressionRatioEstimator::default()),
            )
            .unwrap();

            // Append rows
            for _ in 0..200 {
                let mut row = GenericRow::new(2);
                row.set_field(0, 1_i32);
                row.set_field(1, "hello");
                let record = WriteRecord::for_append(
                    Arc::clone(&table_info),
                    Arc::clone(&physical_table_path),
                    1,
                    &row,
                );
                batch.try_append(&record).unwrap();
            }

            let estimated_size = batch.estimated_size_in_bytes();
            assert!(estimated_size > 0);

            let built_data = batch.build().unwrap();
            let actual_size = built_data.len();

            let diff = actual_size.abs_diff(estimated_size);
            let threshold = actual_size / 10; // 10% tolerance
            assert!(
                diff <= threshold,
                "RowAppend: estimated_size {estimated_size} and actual_size {actual_size} differ by more than 10%"
            );
        }

        // Test 2: PrebuiltRecordBatchBuilder (to_append_record_batch=true)
        {
            let mut batch = ArrowLogWriteBatch::new(
                1,
                physical_table_path.clone(),
                1,
                ArrowCompressionInfo {
                    compression_type: ArrowCompressionType::None,
                    compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
                },
                &row_type,
                0,
                true,
                2 * 1024 * 1024,
                Arc::new(ArrowCompressionRatioEstimator::default()),
            )
            .unwrap();

            // Create a pre-built RecordBatch
            let schema = crate::record::to_arrow_schema(&row_type).unwrap();
            let ids: Vec<i32> = (0..200).collect();
            let names: Vec<&str> = (0..200).map(|_| "hello").collect();
            let record_batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                ],
            )
            .unwrap();

            let record = WriteRecord::for_append_record_batch(
                Arc::clone(&table_info),
                Arc::clone(&physical_table_path),
                1,
                record_batch,
            );
            batch.try_append(&record).unwrap();

            let estimated_size = batch.estimated_size_in_bytes();
            assert!(estimated_size > 0);

            let built_data = batch.build().unwrap();
            let actual_size = built_data.len();

            let diff = actual_size.abs_diff(estimated_size);
            let threshold = actual_size / 10; // 10% tolerance
            assert!(
                diff <= threshold,
                "Prebuilt: estimated_size {estimated_size} and actual_size {actual_size} differ by more than 10%"
            );
        }
    }

    #[test]
    fn test_kv_write_batch_estimated_size() {
        use crate::metadata::KvFormat;

        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        let mut batch = KvWriteBatch::new(
            1,
            Arc::clone(&physical_path),
            1,
            256,
            KvFormat::COMPACTED,
            None,
            0,
        );

        for _ in 0..200 {
            let record = WriteRecord::for_upsert(
                Arc::clone(&table_info),
                Arc::clone(&physical_path),
                1,
                Bytes::from(vec![1_u8, 2_u8, 3_u8]),
                None,
                WriteFormat::CompactedKv,
                None,
                Some(RowBytes::Owned(Bytes::from(vec![1_u8, 2_u8, 3_u8]))),
            );
            batch.try_append(&record).unwrap();
        }

        let estimated_size = batch.estimated_size_in_bytes();
        let actual_size = batch.build().unwrap().len();

        assert_eq!(
            actual_size, estimated_size,
            "estimated size {estimated_size} is not equal to actual size"
        );
    }

    /// Verifies byte-size-based fullness:
    /// 1. Actual built size stays within the configured limit (no compression).
    /// 2. Old 256-record cap is gone — large batches accept >256 small rows.
    /// 3. Compression feedback loop: shared estimator updates after build(),
    ///    second batch with same estimator accepts more records.
    #[test]
    fn test_arrow_batch_byte_size_fullness() {
        use crate::client::WriteRecord;
        use crate::compression::{
            ArrowCompressionInfo, ArrowCompressionRatioEstimator, ArrowCompressionType,
            DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
        };
        use crate::metadata::{DataField, DataTypes, RowType};
        use crate::row::GenericRow;
        use std::sync::Arc;

        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        // --- Part 1: actual built size stays within limit (uncompressed) ---
        let write_limit: usize = 16 * 1024;
        let mut batch = ArrowLogWriteBatch::new(
            1,
            Arc::clone(&physical_table_path),
            1,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
            &row_type,
            0,
            false,
            write_limit,
            Arc::new(ArrowCompressionRatioEstimator::default()),
        )
        .unwrap();

        let mut appended = 0;
        for i in 0..100_000 {
            let mut row = GenericRow::new(2);
            row.set_field(0, i);
            row.set_field(1, "hello_world");
            let record = WriteRecord::for_append(
                Arc::clone(&table_info),
                Arc::clone(&physical_table_path),
                1,
                &row,
            );
            match batch.try_append(&record).unwrap() {
                Some(_) => appended += 1,
                None => break,
            }
        }

        assert!(
            appended > 0 && appended < 100_000,
            "batch should have filled, appended: {appended}"
        );
        let built = batch.build().unwrap();
        assert!(
            built.len() <= write_limit * 120 / 100,
            "actual size {} exceeds write_limit {write_limit} by more than 20%",
            built.len()
        );

        // --- Part 2: old 256-record cap is gone ---
        let row_type_small = RowType::new(vec![DataField::new(
            "id".to_string(),
            DataTypes::int(),
            None,
        )]);
        let mut batch = ArrowLogWriteBatch::new(
            2,
            Arc::clone(&physical_table_path),
            1,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
            &row_type_small,
            0,
            false,
            2 * 1024 * 1024,
            Arc::new(ArrowCompressionRatioEstimator::default()),
        )
        .unwrap();

        let mut appended = 0;
        for i in 0..1000 {
            let mut row = GenericRow::new(1);
            row.set_field(0, i);
            let record = WriteRecord::for_append(
                Arc::clone(&table_info),
                Arc::clone(&physical_table_path),
                1,
                &row,
            );
            match batch.try_append(&record).unwrap() {
                Some(_) => appended += 1,
                None => break,
            }
        }
        assert_eq!(appended, 1000, "2MB batch should fit 1000 tiny rows");

        // --- Part 3: compression feedback loop ---
        let estimator = Arc::new(ArrowCompressionRatioEstimator::default());
        assert_eq!(estimator.estimation(), 1.0);

        let write_limit = 64 * 1024;
        let compression = ArrowCompressionInfo {
            compression_type: ArrowCompressionType::Zstd,
            compression_level: 3,
        };

        // First batch: fill and build with ZSTD.
        let mut batch1 = ArrowLogWriteBatch::new(
            3,
            Arc::clone(&physical_table_path),
            1,
            compression.clone(),
            &row_type,
            0,
            false,
            write_limit,
            Arc::clone(&estimator),
        )
        .unwrap();

        for i in 0..500 {
            let mut row = GenericRow::new(2);
            row.set_field(0, i);
            row.set_field(1, "aaaaaaaaaaaaaaaa");
            let record = WriteRecord::for_append(
                Arc::clone(&table_info),
                Arc::clone(&physical_table_path),
                1,
                &row,
            );
            if batch1.try_append(&record).unwrap().is_none() {
                break;
            }
        }
        batch1.build().unwrap();

        // Estimator should have decreased (ZSTD compresses repeated data well).
        assert!(
            estimator.estimation() < 1.0,
            "ratio should decrease after compressed build, got: {}",
            estimator.estimation()
        );

        // Second batch: same estimator → knows data compresses well → accepts more rows.
        let mut batch2 = ArrowLogWriteBatch::new(
            4,
            Arc::clone(&physical_table_path),
            1,
            compression,
            &row_type,
            0,
            false,
            write_limit,
            Arc::clone(&estimator),
        )
        .unwrap();

        let mut appended2 = 0;
        for i in 0..10_000 {
            let mut row = GenericRow::new(2);
            row.set_field(0, i);
            row.set_field(1, "aaaaaaaaaaaaaaaa");
            let record = WriteRecord::for_append(
                Arc::clone(&table_info),
                Arc::clone(&physical_table_path),
                1,
                &row,
            );
            match batch2.try_append(&record).unwrap() {
                Some(_) => appended2 += 1,
                None => break,
            }
        }
        assert!(
            appended2 > 500,
            "second batch should accept more records with updated ratio, got: {appended2}"
        );
    }
}
