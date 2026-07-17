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

use crate::client::{LogWriteRecord, Record, WriteRecord};
use crate::compression::{
    ArrowCompressionInfo, ArrowCompressionRatioEstimator, ArrowCompressionType,
};
use crate::error::{Error, Result};
use crate::metadata::{DataField, DataType, RowType};
use crate::record::{ChangeType, ScanRecord};
use crate::row::column_vector::TypedBatch;
use crate::row::column_writer::{ColumnWriter, round_up_to_8};
use crate::row::{ColumnarRow, InternalRow};
use arrow::array::{ArrayBuilder, ArrayRef, new_null_array};
use arrow::{
    array::RecordBatch,
    buffer::Buffer,
    ipc::{
        CompressionType,
        reader::{StreamReader, read_record_batch},
        root_as_message,
        writer::StreamWriter,
    },
};
use arrow_schema::ArrowError::ParseError;
use arrow_schema::SchemaRef;
use arrow_schema::{DataType as ArrowDataType, Field};
use byteorder::WriteBytesExt;
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use crc32c::crc32c;
use std::{
    cell::Cell,
    collections::HashMap,
    fs::File,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Arc,
};

use crate::error::Error::IllegalArgument;
use arrow::ipc::writer::IpcWriteOptions;
/// const for record batch
pub const BASE_OFFSET_LENGTH: usize = 8;
pub const LENGTH_LENGTH: usize = 4;
pub const MAGIC_LENGTH: usize = 1;
pub const COMMIT_TIMESTAMP_LENGTH: usize = 8;
pub const CRC_LENGTH: usize = 4;
pub const SCHEMA_ID_LENGTH: usize = 2;
pub const ATTRIBUTE_LENGTH: usize = 1;
pub const LAST_OFFSET_DELTA_LENGTH: usize = 4;
pub const WRITE_CLIENT_ID_LENGTH: usize = 8;
pub const BATCH_SEQUENCE_LENGTH: usize = 4;
pub const RECORDS_COUNT_LENGTH: usize = 4;

pub const BASE_OFFSET_OFFSET: usize = 0;
pub const LENGTH_OFFSET: usize = BASE_OFFSET_OFFSET + BASE_OFFSET_LENGTH;
pub const MAGIC_OFFSET: usize = LENGTH_OFFSET + LENGTH_LENGTH;
pub const COMMIT_TIMESTAMP_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub const CRC_OFFSET: usize = COMMIT_TIMESTAMP_OFFSET + COMMIT_TIMESTAMP_LENGTH;
pub const SCHEMA_ID_OFFSET: usize = CRC_OFFSET + CRC_LENGTH;
pub const ATTRIBUTES_OFFSET: usize = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
pub const LAST_OFFSET_DELTA_OFFSET: usize = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
pub const WRITE_CLIENT_ID_OFFSET: usize = LAST_OFFSET_DELTA_OFFSET + LAST_OFFSET_DELTA_LENGTH;
pub const BATCH_SEQUENCE_OFFSET: usize = WRITE_CLIENT_ID_OFFSET + WRITE_CLIENT_ID_LENGTH;
pub const RECORDS_COUNT_OFFSET: usize = BATCH_SEQUENCE_OFFSET + BATCH_SEQUENCE_LENGTH;
pub const RECORDS_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;

pub const RECORD_BATCH_HEADER_SIZE: usize = RECORDS_OFFSET;
pub const ARROW_CHANGETYPE_OFFSET: usize = RECORD_BATCH_HEADER_SIZE;
pub const LOG_OVERHEAD: usize = LENGTH_OFFSET + LENGTH_LENGTH;

/// Bit 0 of the attributes byte. When set, the batch is append-only and carries
/// no change-type vector; when clear, a `record_count`-byte change-type vector
/// precedes the Arrow IPC payload (the changelog of a primary-key table). Shares
/// the wire layout of the Java client's `DefaultLogRecordBatch`.
pub const APPEND_ONLY_FLAG_MASK: u8 = 0x01;

/// Maximum batch size matches Java's Integer.MAX_VALUE limit.
/// Java uses int type for batch size, so max value is 2^31 - 1 = 2,147,483,647 bytes (~2GB).
/// This is the implicit limit in FileLogRecords.java and other Java components.
pub const MAX_BATCH_SIZE: usize = i32::MAX as usize; // 2,147,483,647 bytes (~2GB)

/// const for record
/// The "magic" values.
#[derive(Debug, Clone, Copy)]
pub enum LogMagicValue {
    V0 = 0,
}

/// Safely convert batch size from i32 to usize with validation.
///
/// Validates that:
/// - batch_size_bytes is non-negative
/// - batch_size_bytes + LOG_OVERHEAD doesn't overflow
/// - Result is within reasonable bounds
fn validate_batch_size(batch_size_bytes: i32) -> Result<usize> {
    // Check for negative size (corrupted data)
    if batch_size_bytes < 0 {
        return Err(Error::UnexpectedError {
            message: format!("Invalid negative batch size: {batch_size_bytes}"),
            source: None,
        });
    }

    let batch_size_u = batch_size_bytes as usize;

    // Check for overflow when adding LOG_OVERHEAD
    let total_size =
        batch_size_u
            .checked_add(LOG_OVERHEAD)
            .ok_or_else(|| Error::UnexpectedError {
                message: format!(
                    "Batch size {batch_size_u} + LOG_OVERHEAD {LOG_OVERHEAD} would overflow"
                ),
                source: None,
            })?;

    // Sanity check: reject unreasonably large batches
    if total_size > MAX_BATCH_SIZE {
        return Err(Error::UnexpectedError {
            message: format!(
                "Batch size {total_size} exceeds maximum allowed size {MAX_BATCH_SIZE}"
            ),
            source: None,
        });
    }

    Ok(total_size)
}

// NOTE: Rust layout/offsets currently match Java only for V0.
// TODO: Add V1 layout/offsets to keep parity with Java's V1 format.
pub const CURRENT_LOG_MAGIC_VALUE: u8 = LogMagicValue::V0 as u8;

/// Value used if writer ID is not available or non-idempotent.
pub const NO_WRITER_ID: i64 = -1;

/// Value used if batch sequence is not available.
pub const NO_BATCH_SEQUENCE: i32 = -1;

pub const BUILDER_DEFAULT_OFFSET: i64 = 0;

/// Initial capacity for Arrow column vectors (pre-allocation hint, not a record cap).
/// Matching Java's `ArrowWriter.INITIAL_CAPACITY`.
const INITIAL_ROW_CAPACITY: usize = 1024;

/// Fraction of the allocated buffer used as the effective write limit.
/// Matching Java's `ArrowWriter.BUFFER_USAGE_RATIO`.
const BUFFER_USAGE_RATIO: f32 = 0.95;

pub struct MemoryLogRecordsArrowBuilder {
    base_log_offset: i64,
    schema_id: i32,
    magic: u8,
    writer_id: i64,
    batch_sequence: i32,
    arrow_record_batch_builder: Box<dyn ArrowRecordBatchInnerBuilder>,
    is_closed: bool,
    arrow_compression_info: ArrowCompressionInfo,
    /// Effective write limit in bytes (after applying BUFFER_USAGE_RATIO).
    write_limit: usize,
    /// Pre-computed Arrow IPC overhead (metadata + body framing) for this schema.
    /// Constant per schema+compression combination.
    ipc_overhead: usize,
    /// Estimated record count at which the next byte-size check should occur.
    /// -1 means "unknown — check on the next append". Updated dynamically to
    /// skip expensive `estimated_size_in_bytes()` calls on every append.
    /// Matching Java's `ArrowWriter.estimatedMaxRecordsCount`.
    estimated_max_records_count: Cell<i32>,
    /// Compression ratio estimator shared across batches for the same table.
    compression_ratio_estimator: Arc<ArrowCompressionRatioEstimator>,
    /// Snapshot of the compression ratio at batch creation time.
    /// Matching Java's `ArrowWriter.estimatedCompressionRatio` which is
    /// cached per batch and only refreshed on `reset()`.
    estimated_compression_ratio: f32,
}

pub trait ArrowRecordBatchInnerBuilder: Send {
    fn build_arrow_record_batch(&mut self) -> Result<Arc<RecordBatch>>;

    fn append(&mut self, row: &dyn InternalRow) -> Result<bool>;

    fn append_batch(&mut self, record_batch: Arc<RecordBatch>) -> Result<bool>;

    fn schema(&self) -> SchemaRef;

    fn records_count(&self) -> i32;

    fn is_full(&self) -> bool;

    /// Get an estimate of the size in bytes of the arrow data.
    fn estimated_size_in_bytes(&self) -> usize;
}

#[derive(Default)]
pub struct PrebuiltRecordBatchBuilder {
    arrow_record_batch: Option<Arc<RecordBatch>>,
    records_count: i32,
}

impl ArrowRecordBatchInnerBuilder for PrebuiltRecordBatchBuilder {
    fn build_arrow_record_batch(&mut self) -> Result<Arc<RecordBatch>> {
        Ok(self.arrow_record_batch.as_ref().unwrap().clone())
    }

    fn append(&mut self, _row: &dyn InternalRow) -> Result<bool> {
        // append one single row is not supported, return false directly
        Ok(false)
    }

    fn append_batch(&mut self, record_batch: Arc<RecordBatch>) -> Result<bool> {
        if self.arrow_record_batch.is_some() {
            return Ok(false);
        }
        self.records_count = record_batch.num_rows() as i32;
        self.arrow_record_batch = Some(record_batch);
        Ok(true)
    }

    fn schema(&self) -> SchemaRef {
        self.arrow_record_batch.as_ref().unwrap().schema()
    }

    fn records_count(&self) -> i32 {
        self.records_count
    }

    fn is_full(&self) -> bool {
        // full if has one record batch
        self.arrow_record_batch.is_some()
    }

    fn estimated_size_in_bytes(&self) -> usize {
        self.arrow_record_batch
            .as_ref()
            .map(|batch| batch.get_array_memory_size())
            .unwrap_or(0)
    }
}

pub struct RowAppendRecordBatchBuilder {
    table_schema: SchemaRef,
    column_writers: Vec<ColumnWriter>,
    records_count: i32,
}

impl RowAppendRecordBatchBuilder {
    pub fn new(row_type: &RowType) -> Result<Self> {
        let capacity = INITIAL_ROW_CAPACITY;
        let schema_ref = to_arrow_schema(row_type)?;
        let writers: Result<Vec<_>> = row_type
            .fields()
            .iter()
            .enumerate()
            .map(|(pos, field)| {
                let arrow_type = schema_ref.field(pos).data_type();
                ColumnWriter::create(field.data_type(), arrow_type, pos, capacity)
            })
            .collect();
        Ok(Self {
            table_schema: schema_ref.clone(),
            column_writers: writers?,
            records_count: 0,
        })
    }
    /// Appends a row to the builder.
    pub fn append(&mut self, row: &dyn InternalRow) -> Result<bool> {
        ArrowRecordBatchInnerBuilder::append(self, row)
    }

    /// Builds the final Arrow RecordBatch.
    pub fn build_arrow_record_batch(&mut self) -> Result<Arc<RecordBatch>> {
        ArrowRecordBatchInnerBuilder::build_arrow_record_batch(self)
    }
}

impl ArrowRecordBatchInnerBuilder for RowAppendRecordBatchBuilder {
    fn build_arrow_record_batch(&mut self) -> Result<Arc<RecordBatch>> {
        let arrays: Result<Vec<ArrayRef>> = self
            .column_writers
            .iter_mut()
            .enumerate()
            .map(|(idx, writer)| {
                let array = writer.finish();
                let expected_type = self.table_schema.field(idx).data_type();

                // Validate array type matches schema
                if array.data_type() != expected_type {
                    return Err(Error::IllegalArgument {
                        message: format!(
                            "Builder type mismatch at column {}: expected {:?}, got {:?}",
                            idx,
                            expected_type,
                            array.data_type()
                        ),
                    });
                }

                Ok(array)
            })
            .collect();

        Ok(Arc::new(RecordBatch::try_new(
            self.table_schema.clone(),
            arrays?,
        )?))
    }

    fn append(&mut self, row: &dyn InternalRow) -> Result<bool> {
        for writer in &mut self.column_writers {
            writer.write_field(row)?;
        }
        self.records_count += 1;
        Ok(true)
    }

    fn append_batch(&mut self, _record_batch: Arc<RecordBatch>) -> Result<bool> {
        Ok(false)
    }

    fn schema(&self) -> SchemaRef {
        self.table_schema.clone()
    }

    fn records_count(&self) -> i32 {
        self.records_count
    }

    fn is_full(&self) -> bool {
        // Size-based fullness is handled by MemoryLogRecordsArrowBuilder,
        // which accounts for metadata length and compression ratio.
        false
    }

    fn estimated_size_in_bytes(&self) -> usize {
        // Returns the uncompressed Arrow IPC body size by reading buffer lengths
        // directly from the builders — O(num_columns), zero allocation.
        // Analogous to Java's `ArrowUtils.estimateArrowBodyLength()`.
        // Java reads exact IPC buffer sizes from vectors; we read builder
        // buffer lengths. The IPC framing overhead is accounted for
        // separately by `ipc_overhead`.
        self.column_writers.iter().map(|w| w.buffer_size()).sum()
    }
}

// TODO: Pool and reuse MemoryLogRecordsArrowBuilder instances per table/schema like
// Java's ArrowWriterPool. Reused writers can seed `estimated_max_records_count` from
// the previous batch (recordsCount / 2) for a warm start, avoiding the first-record
// size check on every new batch.
impl MemoryLogRecordsArrowBuilder {
    pub fn new(
        schema_id: i32,
        row_type: &RowType,
        to_append_record_batch: bool,
        arrow_compression_info: ArrowCompressionInfo,
        write_limit: usize,
        compression_ratio_estimator: Arc<ArrowCompressionRatioEstimator>,
    ) -> Result<Self> {
        let arrow_batch_builder: Box<dyn ArrowRecordBatchInnerBuilder> = {
            if to_append_record_batch {
                Box::new(PrebuiltRecordBatchBuilder::default())
            } else {
                Box::new(RowAppendRecordBatchBuilder::new(row_type)?)
            }
        };
        let schema = to_arrow_schema(row_type)?;
        let ipc_overhead =
            estimate_arrow_ipc_overhead(&schema, arrow_compression_info.get_compression_type())?;
        let effective_limit = (write_limit as f32 * BUFFER_USAGE_RATIO) as usize;
        let estimated_compression_ratio = compression_ratio_estimator.estimation();
        Ok(MemoryLogRecordsArrowBuilder {
            base_log_offset: BUILDER_DEFAULT_OFFSET,
            schema_id,
            magic: CURRENT_LOG_MAGIC_VALUE,
            writer_id: NO_WRITER_ID,
            batch_sequence: NO_BATCH_SEQUENCE,
            is_closed: false,
            arrow_record_batch_builder: arrow_batch_builder,
            arrow_compression_info,
            write_limit: effective_limit,
            ipc_overhead,
            estimated_max_records_count: Cell::new(-1),
            compression_ratio_estimator,
            estimated_compression_ratio,
        })
    }

    pub fn append(&mut self, record: &WriteRecord) -> Result<bool> {
        match &record.record() {
            Record::Log(log_write_record) => match log_write_record {
                LogWriteRecord::InternalRow(row) => {
                    Ok(self.arrow_record_batch_builder.append(*row)?)
                }
                LogWriteRecord::RecordBatch(record_batch) => Ok(self
                    .arrow_record_batch_builder
                    .append_batch(record_batch.clone())?),
            },
            Record::Kv(_) => Err(Error::UnsupportedOperation {
                message: "Only LogRecord is supported to append".to_string(),
            }),
        }
        // todo: consider write other change type
    }

    /// Check if the builder is full based on estimated serialized size.
    ///
    /// Uses a threshold-based optimization to skip expensive size checks:
    /// only computes the actual estimated size when the record count reaches
    /// the predicted threshold. Matching Java's `ArrowWriter.isFull()`.
    pub fn is_full(&self) -> bool {
        // Delegate to inner builder first (e.g. PrebuiltRecordBatchBuilder
        // is always full after one batch, regardless of size).
        if self.arrow_record_batch_builder.is_full() {
            return true;
        }
        let records_count = self.arrow_record_batch_builder.records_count();
        let threshold = self.estimated_max_records_count.get();
        if records_count > 0 && records_count >= threshold {
            let body_size = self.arrow_record_batch_builder.estimated_size_in_bytes();
            let estimated_body = self.estimated_compressed_size(body_size);
            let current_size = self.ipc_overhead + estimated_body;
            if current_size >= self.write_limit {
                return true;
            }
            if estimated_body == 0 {
                self.estimated_max_records_count.set(records_count + 1);
                return false;
            }
            // Matching Java: subtract fixed metadata overhead from the limit,
            // divide remaining body budget by per-record body cost.
            let body_per_record = estimated_body as f64 / records_count as f64;
            let next = ((self.write_limit.saturating_sub(self.ipc_overhead) as f64
                / body_per_record)
                .ceil() as i32)
                .max(records_count + 1);
            self.estimated_max_records_count.set(next);
        }
        false
    }

    /// Estimate the compressed body size using the ratio snapshot taken at batch creation.
    /// Matching Java's `ArrowWriter.estimatedBytesWritten()`.
    fn estimated_compressed_size(&self, uncompressed_body: usize) -> usize {
        if self.arrow_compression_info.compression_type == ArrowCompressionType::None {
            uncompressed_body
        } else {
            (uncompressed_body as f64 * self.estimated_compression_ratio as f64) as usize
        }
    }

    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    pub fn close(&mut self) {
        self.is_closed = true;
    }

    pub fn build(&mut self) -> Result<Vec<u8>> {
        // Capture uncompressed body size before serialization for compression ratio update.
        let uncompressed_body_size = self.arrow_record_batch_builder.estimated_size_in_bytes();

        // serialize arrow batch
        let mut arrow_batch_bytes = vec![];
        let table_schema = self.arrow_record_batch_builder.schema();
        let compression_type = self.arrow_compression_info.get_compression_type();
        let write_option =
            IpcWriteOptions::try_with_compression(IpcWriteOptions::default(), compression_type);
        let mut writer = StreamWriter::try_new_with_options(
            &mut arrow_batch_bytes,
            &table_schema,
            write_option?,
        )?;

        // get header len
        let header = writer.get_ref().len();
        let record_batch = self.arrow_record_batch_builder.build_arrow_record_batch()?;
        writer.write(record_batch.as_ref())?;
        // get real arrow batch bytes (metadata + body, potentially compressed)
        let real_arrow_batch_bytes = &arrow_batch_bytes[header..];

        // Update compression ratio estimator with actual ratio.
        // The serialized bytes include metadata + compressed body. Subtract
        // metadata to isolate the compressed body for an accurate ratio.
        if uncompressed_body_size > 0
            && self.arrow_compression_info.compression_type != ArrowCompressionType::None
        {
            let compressed_body_size = real_arrow_batch_bytes
                .len()
                .saturating_sub(self.ipc_overhead);
            let actual_ratio = compressed_body_size as f32 / uncompressed_body_size as f32;
            self.compression_ratio_estimator
                .update_estimation(actual_ratio);
        }

        // now, write batch header and arrow batch
        let mut batch_bytes = vec![0u8; RECORD_BATCH_HEADER_SIZE + real_arrow_batch_bytes.len()];
        // write batch header
        self.write_batch_header(&mut batch_bytes[..])?;

        // write arrow batch bytes
        let mut cursor = Cursor::new(&mut batch_bytes[..]);
        cursor.set_position(RECORD_BATCH_HEADER_SIZE as u64);
        cursor.write_all(real_arrow_batch_bytes)?;

        let calcute_crc_bytes = &cursor.get_ref()[SCHEMA_ID_OFFSET..];
        // then update crc
        let crc = crc32c(calcute_crc_bytes);
        cursor.set_position(CRC_OFFSET as u64);
        cursor.write_u32::<LittleEndian>(crc)?;

        Ok(batch_bytes.to_vec())
    }

    fn write_batch_header(&self, buffer: &mut [u8]) -> Result<()> {
        let total_len = buffer.len();
        let mut cursor = Cursor::new(buffer);
        cursor.write_i64::<LittleEndian>(self.base_log_offset)?;
        cursor
            .write_i32::<LittleEndian>((total_len - BASE_OFFSET_LENGTH - LENGTH_LENGTH) as i32)?;
        cursor.write_u8(self.magic)?;
        cursor.write_i64::<LittleEndian>(0)?; // timestamp placeholder
        cursor.write_u32::<LittleEndian>(0)?; // crc placeholder
        cursor.write_i16::<LittleEndian>(self.schema_id as i16)?;

        let record_count = self.arrow_record_batch_builder.records_count();
        // todo: curerntly, always is append only
        let append_only = true;
        cursor.write_u8(if append_only { 1 } else { 0 })?;
        cursor.write_i32::<LittleEndian>(if record_count > 0 {
            record_count - 1
        } else {
            0
        })?;

        cursor.write_i64::<LittleEndian>(self.writer_id)?;
        cursor.write_i32::<LittleEndian>(self.batch_sequence)?;
        cursor.write_i32::<LittleEndian>(record_count)?;
        Ok(())
    }

    pub fn set_writer_state(&mut self, writer_id: i64, batch_base_sequence: i32) {
        self.writer_id = writer_id;
        self.batch_sequence = batch_base_sequence;
    }

    /// Get an estimate of the number of bytes written to the underlying buffer.
    /// Includes Fluss record batch header + Arrow IPC metadata + estimated
    /// compressed body size.
    pub fn estimated_size_in_bytes(&self) -> usize {
        let body = self.arrow_record_batch_builder.estimated_size_in_bytes();
        let estimated_body = self.estimated_compressed_size(body);
        RECORD_BATCH_HEADER_SIZE + self.ipc_overhead + estimated_body
    }

    /// Number of records appended so far. Used for writer throughput metrics.
    pub(crate) fn records_count(&self) -> i32 {
        self.arrow_record_batch_builder.records_count()
    }
}

/// Estimate the Arrow IPC overhead (metadata + body framing) for a given schema.
///
/// Serializes a 1-row RecordBatch with known data sizes, then subtracts the
/// raw data contribution to isolate the fixed overhead: IPC message header,
/// RecordBatch flatbuffer, and per-buffer alignment padding within the body.
/// This overhead is constant for a given schema+compression combination.
///
/// Note: called once per batch creation. With writer pooling (see TODO above),
/// this would be computed once per pooled writer and reused across batches.
/// Analogous to Java's `ArrowUtils.estimateArrowMetadataLength()`.
fn estimate_arrow_ipc_overhead(
    schema: &SchemaRef,
    compression: Option<CompressionType>,
) -> Result<usize> {
    use arrow::array::new_null_array;

    // Create a 1-row batch of nulls. Null arrays have minimal, predictable
    // data: no validity bitmap, no variable-length data, just fixed-width
    // zero buffers. This lets us compute raw data size exactly.
    let null_arrays: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| new_null_array(field.data_type(), 1))
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), null_arrays)?;

    // Sum the raw buffer sizes — this is what buffer_size() would report.
    let raw_data: usize = batch
        .columns()
        .iter()
        .map(|col| {
            col.to_data()
                .buffers()
                .iter()
                .map(|buf| round_up_to_8(buf.len()))
                .sum::<usize>()
                // Validity buffer (null bitmap)
                + col
                    .nulls()
                    .map_or(0, |n| round_up_to_8(n.buffer().len()))
        })
        .sum();

    // Serialize the batch via IPC and measure total output.
    let mut buf = vec![];
    let write_option =
        IpcWriteOptions::try_with_compression(IpcWriteOptions::default(), compression);
    let mut writer = StreamWriter::try_new_with_options(&mut buf, schema, write_option?)?;
    let header_len = writer.get_ref().len();
    writer.write(&batch)?;
    let total_len = writer.get_ref().len();

    // IPC overhead = total message size - raw data we put in.
    let ipc_message_len = total_len - header_len;
    Ok(ipc_message_len.saturating_sub(raw_data))
}

pub trait ToArrow {
    fn append_to(&self, builder: &mut dyn ArrayBuilder) -> Result<()>;
}

/// In-memory log record source.
/// Used for local tablet server fetches (existing path).
struct MemorySource {
    data: Bytes,
}

impl MemorySource {
    fn new(data: Vec<u8>) -> Self {
        Self {
            data: Bytes::from(data),
        }
    }

    fn read_batch_header(&mut self, pos: usize) -> Result<(i64, usize)> {
        if pos + LOG_OVERHEAD > self.data.len() {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Position {} + LOG_OVERHEAD {} exceeds data size {}",
                    pos,
                    LOG_OVERHEAD,
                    self.data.len()
                ),
                source: None,
            });
        }

        let base_offset = LittleEndian::read_i64(&self.data[pos + BASE_OFFSET_OFFSET..]);
        let batch_size_bytes = LittleEndian::read_i32(&self.data[pos + LENGTH_OFFSET..]);

        // Validate batch size to prevent integer overflow and corruption
        let batch_size = validate_batch_size(batch_size_bytes)?;

        Ok((base_offset, batch_size))
    }

    fn read_batch_data(&mut self, pos: usize, size: usize) -> Result<Bytes> {
        if pos + size > self.data.len() {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Read beyond data size: {} + {} > {}",
                    pos,
                    size,
                    self.data.len()
                ),
                source: None,
            });
        }
        // Zero-copy slice (Bytes is Arc-based)
        Ok(self.data.slice(pos..pos + size))
    }

    fn total_size(&self) -> usize {
        self.data.len()
    }
}

/// RAII guard that deletes a file when dropped.
/// Used to ensure file deletion happens AFTER the file handle is closed.
struct FileCleanupGuard {
    file_path: PathBuf,
}

impl Drop for FileCleanupGuard {
    fn drop(&mut self) {
        // File handle is already closed (this guard drops after the file field)
        if let Err(e) = std::fs::remove_file(&self.file_path) {
            log::warn!(
                "Failed to delete remote log file {}: {}",
                self.file_path.display(),
                e
            );
        } else {
            log::debug!("Deleted remote log file: {}", self.file_path.display());
        }
    }
}

/// File-backed log record source.
/// Used for remote log segments downloaded to local disk.
/// Streams data on-demand instead of loading entire file into memory.
///
/// Uses seek + read_exact for cross-platform compatibility.
/// Access pattern is sequential iteration (single consumer).
struct FileSource {
    file: File,
    file_size: usize,
    base_offset: usize,
    _cleanup: Option<FileCleanupGuard>, // Drops AFTER file (field order matters!)
}

impl FileSource {
    /// Create a new FileSource.
    ///
    /// The file at `file_path` will be deleted when this FileSource is dropped.
    fn new(file: File, base_offset: usize, file_path: PathBuf) -> Result<Self> {
        let file_size = file.metadata()?.len() as usize;

        // Validate base_offset to prevent underflow in total_size()
        if base_offset > file_size {
            return Err(Error::UnexpectedError {
                message: format!("base_offset ({base_offset}) exceeds file_size ({file_size})"),
                source: None,
            });
        }

        Ok(Self {
            file,
            file_size,
            base_offset,
            _cleanup: Some(FileCleanupGuard { file_path }),
        })
    }

    /// Read data at a specific position using seek + read_exact.
    /// This is cross-platform and adequate for sequential access patterns.
    fn read_at(&mut self, pos: u64, buf: &mut [u8]) -> Result<()> {
        self.file.seek(SeekFrom::Start(pos))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    fn read_batch_header(&mut self, pos: usize) -> Result<(i64, usize)> {
        let actual_pos = self.base_offset + pos;
        if actual_pos + LOG_OVERHEAD > self.file_size {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Position {} exceeds file size {}",
                    actual_pos, self.file_size
                ),
                source: None,
            });
        }

        // Read only the header to extract base_offset and batch_size
        let mut header_buf = vec![0u8; LOG_OVERHEAD];
        self.read_at(actual_pos as u64, &mut header_buf)?;

        let base_offset = LittleEndian::read_i64(&header_buf[BASE_OFFSET_OFFSET..]);
        let batch_size_bytes = LittleEndian::read_i32(&header_buf[LENGTH_OFFSET..]);

        // Validate batch size to prevent integer overflow and corruption
        let batch_size = validate_batch_size(batch_size_bytes)?;

        Ok((base_offset, batch_size))
    }

    fn read_batch_data(&mut self, pos: usize, size: usize) -> Result<Bytes> {
        let actual_pos = self.base_offset + pos;
        if actual_pos + size > self.file_size {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Read beyond file size: {} + {} > {}",
                    actual_pos, size, self.file_size
                ),
                source: None,
            });
        }

        // Read the full batch data
        let mut batch_buf = vec![0u8; size];
        self.read_at(actual_pos as u64, &mut batch_buf)?;

        Ok(Bytes::from(batch_buf))
    }

    fn total_size(&self) -> usize {
        self.file_size - self.base_offset
    }
}

/// Enum for different log record sources.
enum LogRecordsSource {
    Memory(MemorySource),
    File(FileSource),
}

impl LogRecordsSource {
    fn read_batch_header(&mut self, pos: usize) -> Result<(i64, usize)> {
        match self {
            Self::Memory(s) => s.read_batch_header(pos),
            Self::File(s) => s.read_batch_header(pos),
        }
    }

    fn read_batch_data(&mut self, pos: usize, size: usize) -> Result<Bytes> {
        match self {
            Self::Memory(s) => s.read_batch_data(pos, size),
            Self::File(s) => s.read_batch_data(pos, size),
        }
    }

    fn total_size(&self) -> usize {
        match self {
            Self::Memory(s) => s.total_size(),
            Self::File(s) => s.total_size(),
        }
    }
}

pub struct LogRecordsBatches {
    source: LogRecordsSource,
    current_pos: usize,
    remaining_bytes: usize,
}

impl LogRecordsBatches {
    /// Create from in-memory Vec (existing path - backward compatible).
    pub fn new(data: Vec<u8>) -> Self {
        let source = LogRecordsSource::Memory(MemorySource::new(data));
        let remaining_bytes = source.total_size();
        Self {
            source,
            current_pos: 0,
            remaining_bytes,
        }
    }

    /// Create from file.
    /// Enables streaming without loading entire file into memory.
    ///
    /// The file at `file_path` will be deleted when dropped.
    /// This ensures the file is closed before deletion.
    pub fn from_file(file: File, base_offset: usize, file_path: PathBuf) -> Result<Self> {
        let source = FileSource::new(file, base_offset, file_path)?;
        let remaining_bytes = source.total_size();
        Ok(Self {
            source: LogRecordsSource::File(source),
            current_pos: 0,
            remaining_bytes,
        })
    }

    /// Try to get the size of the next batch.
    fn next_batch_size(&mut self) -> Result<Option<usize>> {
        if self.remaining_bytes < LOG_OVERHEAD {
            return Ok(None);
        }

        // Read only header to get size
        match self.source.read_batch_header(self.current_pos) {
            Ok((_base_offset, batch_size)) => {
                if batch_size > self.remaining_bytes {
                    Ok(None)
                } else {
                    Ok(Some(batch_size))
                }
            }
            Err(e) => Err(e),
        }
    }
}

impl Iterator for LogRecordsBatches {
    type Item = Result<LogRecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_batch_size() {
            Ok(Some(batch_size)) => {
                // Read full batch data on-demand
                match self.source.read_batch_data(self.current_pos, batch_size) {
                    Ok(data) => {
                        let record_batch = LogRecordBatch::new(data);
                        self.current_pos += batch_size;
                        self.remaining_bytes -= batch_size;
                        Some(Ok(record_batch))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

pub struct LogRecordBatch {
    data: Bytes,
}

#[allow(dead_code)]
impl LogRecordBatch {
    pub fn new(data: Bytes) -> Self {
        LogRecordBatch { data }
    }

    pub fn magic(&self) -> u8 {
        self.data[MAGIC_OFFSET]
    }

    pub fn commit_timestamp(&self) -> i64 {
        let offset = COMMIT_TIMESTAMP_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + COMMIT_TIMESTAMP_LENGTH])
    }

    pub fn writer_id(&self) -> i64 {
        let offset = WRITE_CLIENT_ID_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + WRITE_CLIENT_ID_LENGTH])
    }

    pub fn batch_sequence(&self) -> i32 {
        let offset = BATCH_SEQUENCE_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + BATCH_SEQUENCE_LENGTH])
    }

    pub fn ensure_valid(&self) -> Result<()> {
        // TODO enable validation once checksum handling is corrected.
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        self.size_in_bytes() >= RECORD_BATCH_HEADER_SIZE
            && self.checksum() == self.compute_checksum()
    }

    fn compute_checksum(&self) -> u32 {
        let start = SCHEMA_ID_OFFSET;
        crc32c(&self.data[start..])
    }

    fn attributes(&self) -> u8 {
        self.data[ATTRIBUTES_OFFSET]
    }

    /// Whether this batch is append-only (see [`APPEND_ONLY_FLAG_MASK`]).
    fn is_append_only(&self) -> bool {
        self.attributes() & APPEND_ONLY_FLAG_MASK != 0
    }

    pub fn next_log_offset(&self) -> i64 {
        self.last_log_offset() + 1
    }

    pub fn checksum(&self) -> u32 {
        let offset = CRC_OFFSET;
        LittleEndian::read_u32(&self.data[offset..offset + CRC_LENGTH])
    }

    pub fn schema_id(&self) -> i16 {
        let offset = SCHEMA_ID_OFFSET;
        LittleEndian::read_i16(&self.data[offset..offset + SCHEMA_ID_LENGTH])
    }

    pub fn base_log_offset(&self) -> i64 {
        let offset = BASE_OFFSET_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + BASE_OFFSET_LENGTH])
    }

    pub fn last_log_offset(&self) -> i64 {
        self.base_log_offset() + self.last_offset_delta() as i64
    }

    fn last_offset_delta(&self) -> i32 {
        let offset = LAST_OFFSET_DELTA_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + LAST_OFFSET_DELTA_LENGTH])
    }

    pub fn size_in_bytes(&self) -> usize {
        let offset = LENGTH_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + LENGTH_LENGTH]) as usize + LOG_OVERHEAD
    }

    pub fn record_count(&self) -> i32 {
        let offset = RECORDS_COUNT_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + RECORDS_COUNT_LENGTH])
    }

    /// Splits the batch body into its per-record change types and the trailing
    /// Arrow IPC payload (see [`APPEND_ONLY_FLAG_MASK`] for the layout).
    fn decode_change_types(&self) -> Result<(BatchChangeTypes, &[u8])> {
        let body = self
            .data
            .get(RECORDS_OFFSET..)
            .ok_or_else(|| Error::UnexpectedError {
                message: format!(
                    "Corrupt log record batch: data length {} is less than RECORDS_OFFSET {}",
                    self.data.len(),
                    RECORDS_OFFSET
                ),
                source: None,
            })?;

        if self.is_append_only() {
            return Ok((BatchChangeTypes::Uniform(ChangeType::AppendOnly), body));
        }

        let record_count = self.record_count();
        if record_count < 0 {
            return Err(Error::UnexpectedError {
                message: format!("Corrupt changelog batch: negative record count {record_count}"),
                source: None,
            });
        }
        let record_count = record_count as usize;
        let (change_type_bytes, arrow_data) =
            body.split_at_checked(record_count)
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!(
                        "Corrupt changelog batch: body length {} is smaller than its \
                         {record_count}-record change-type vector",
                        body.len()
                    ),
                    source: None,
                })?;

        let mut change_types = Vec::with_capacity(record_count);
        for &byte in change_type_bytes {
            let change_type =
                ChangeType::from_byte_value(byte).map_err(|message| Error::UnexpectedError {
                    message,
                    source: None,
                })?;
            change_types.push(change_type);
        }

        Ok((BatchChangeTypes::PerRecord(change_types), arrow_data))
    }

    pub fn records(&self, read_context: &ReadContext) -> Result<LogRecordIterator> {
        if self.record_count() == 0 {
            return Ok(LogRecordIterator::empty());
        }

        let (change_types, arrow_data) = self.decode_change_types()?;
        let record_batch = read_context.record_batch(arrow_data)?;
        let arrow_reader = ArrowReader::new_with_fluss_row_type(
            Arc::new(record_batch),
            read_context.row_type.clone(),
            read_context.fluss_row_type().cloned(),
        )?;
        let iterator = ArrowLogRecordIterator::new(
            arrow_reader,
            self.base_log_offset(),
            self.commit_timestamp(),
            change_types,
        )?;

        Ok(LogRecordIterator::Arrow(iterator))
    }

    pub fn records_for_remote_log(&self, read_context: &ReadContext) -> Result<LogRecordIterator> {
        if self.record_count() == 0 {
            return Ok(LogRecordIterator::empty());
        }

        let (change_types, arrow_data) = self.decode_change_types()?;
        let record_batch = read_context.record_batch_for_remote_log(arrow_data)?;
        let log_record_iterator = match record_batch {
            None => LogRecordIterator::empty(),
            Some(record_batch) => {
                let arrow_reader = ArrowReader::new_with_fluss_row_type(
                    Arc::new(record_batch),
                    read_context.row_type.clone(),
                    read_context.fluss_row_type().cloned(),
                )?;
                let iterator = ArrowLogRecordIterator::new(
                    arrow_reader,
                    self.base_log_offset(),
                    self.commit_timestamp(),
                    change_types,
                )?;
                LogRecordIterator::Arrow(iterator)
            }
        };
        Ok(log_record_iterator)
    }

    /// Returns the record batch directly without creating an iterator.
    /// This is more efficient when you need the entire batch rather than
    /// iterating row-by-row.
    pub fn record_batch(&self, read_context: &ReadContext) -> Result<RecordBatch> {
        if self.record_count() == 0 {
            // Return empty batch with correct schema
            return Ok(RecordBatch::new_empty(read_context.target_schema.clone()));
        }

        // Batch access drops the change-type vector; use `records()` for CDC.
        let (_, arrow_data) = self.decode_change_types()?;
        read_context.record_batch(arrow_data)
    }
}

/// Parse an Arrow IPC message from a byte slice.
///
/// Server returns RecordBatch message (without Schema message) in the encapsulated message format.
/// Format: [continuation: 4 bytes (0xFFFFFFFF)][metadata_size: 4 bytes][RecordBatch metadata][body]
///
/// This format is documented at:
/// https://arrow.apache.org/docs/format/Columnar.html#encapsulated-message-format
///
/// # Arguments
/// * `data` - The byte slice containing the IPC message.
///
/// # Returns
/// Returns `Ok((batch_metadata, body_buffer, version))` on success:
/// - `batch_metadata`: The RecordBatch metadata from the IPC message.
/// - `body_buffer`: The buffer containing the record batch body data.
/// - `version`: The Arrow IPC metadata version.
///
/// Returns `Err(arrow_error)` on errors
/// - `arrow_error`: Error details e.g. malformed, too short or bad continuation marker.
fn parse_ipc_message(
    data: &[u8],
) -> Result<(
    arrow::ipc::RecordBatch<'_>,
    Buffer,
    arrow::ipc::MetadataVersion,
)> {
    const CONTINUATION_MARKER: u32 = 0xFFFFFFFF;

    if data.len() < 8 {
        Err(ParseError(format!("Invalid data length: {}", data.len())))?
    }

    let continuation = LittleEndian::read_u32(&data[0..4]);
    let metadata_size = LittleEndian::read_u32(&data[4..8]) as usize;

    if continuation != CONTINUATION_MARKER {
        Err(ParseError(format!(
            "Invalid continuation marker: {continuation}"
        )))?
    }

    if data.len() < 8 + metadata_size {
        Err(ParseError(format!(
            "Invalid data length. Remaining data length {} is shorter than specified size {}",
            data.len() - 8,
            metadata_size
        )))?
    }

    let metadata_bytes = &data[8..8 + metadata_size];
    let message = root_as_message(metadata_bytes).map_err(|err| ParseError(err.to_string()))?;
    let batch_metadata = message
        .header_as_record_batch()
        .ok_or(ParseError(String::from("Not a record batch")))?;

    let metadata_padded_size = (metadata_size + 7) & !7;
    let body_start = 8 + metadata_padded_size;
    let body_data = &data[body_start..];
    let body_buffer = Buffer::from(body_data);

    Ok((batch_metadata, body_buffer, message.version()))
}

pub fn to_arrow_schema(fluss_schema: &RowType) -> Result<SchemaRef> {
    let fields: Result<Vec<Field>> = fluss_schema
        .fields()
        .iter()
        .map(|f| {
            Ok(Field::new(
                f.name(),
                to_arrow_type(f.data_type())?,
                f.data_type().is_nullable(),
            ))
        })
        .collect();

    Ok(SchemaRef::new(arrow_schema::Schema::new(fields?)))
}

pub fn to_arrow_type(fluss_type: &DataType) -> Result<ArrowDataType> {
    Ok(match fluss_type {
        DataType::Boolean(_) => ArrowDataType::Boolean,
        DataType::TinyInt(_) => ArrowDataType::Int8,
        DataType::SmallInt(_) => ArrowDataType::Int16,
        DataType::BigInt(_) => ArrowDataType::Int64,
        DataType::Int(_) => ArrowDataType::Int32,
        DataType::Float(_) => ArrowDataType::Float32,
        DataType::Double(_) => ArrowDataType::Float64,
        DataType::Char(_) => ArrowDataType::Utf8,
        DataType::String(_) => ArrowDataType::Utf8,
        DataType::Decimal(decimal_type) => {
            let precision =
                decimal_type
                    .precision()
                    .try_into()
                    .map_err(|_| Error::IllegalArgument {
                        message: format!(
                            "Decimal precision {} exceeds Arrow's maximum (u8::MAX)",
                            decimal_type.precision()
                        ),
                    })?;
            let scale = decimal_type
                .scale()
                .try_into()
                .map_err(|_| Error::IllegalArgument {
                    message: format!(
                        "Decimal scale {} exceeds Arrow's maximum (i8::MAX)",
                        decimal_type.scale()
                    ),
                })?;
            ArrowDataType::Decimal128(precision, scale)
        }
        DataType::Date(_) => ArrowDataType::Date32,
        DataType::Time(time_type) => match time_type.precision() {
            0 => ArrowDataType::Time32(arrow_schema::TimeUnit::Second),
            1..=3 => ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond),
            4..=6 => ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond),
            7..=9 => ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond),
            invalid => {
                return Err(Error::IllegalArgument {
                    message: format!("Invalid precision {invalid} for TimeType (must be 0-9)"),
                });
            }
        },
        DataType::Timestamp(timestamp_type) => match timestamp_type.precision() {
            0 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None),
            1..=3 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            4..=6 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            7..=9 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
            invalid => {
                return Err(Error::IllegalArgument {
                    message: format!("Invalid precision {invalid} for TimestampType (must be 0-9)"),
                });
            }
        },
        // TIMESTAMP_LTZ is an instant, so it carries the UTC zone. This keeps
        // `to_arrow_type` symmetric with `from_arrow_type` (which treats a
        // zoned Arrow timestamp as LTZ) so the type round-trips losslessly.
        DataType::TimestampLTz(timestamp_ltz_type) => match timestamp_ltz_type.precision() {
            0 => ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, Some("UTC".into())),
            1..=3 => {
                ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into()))
            }
            4..=6 => {
                ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
            }
            7..=9 => {
                ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into()))
            }
            invalid => {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Invalid precision {invalid} for TimestampLTzType (must be 0-9)"
                    ),
                });
            }
        },
        DataType::Bytes(_) => ArrowDataType::Binary,
        DataType::Binary(binary_type) => {
            let length = binary_type
                .length()
                .try_into()
                .map_err(|_| Error::IllegalArgument {
                    message: format!(
                        "Binary length {} exceeds Arrow's maximum (i32::MAX)",
                        binary_type.length()
                    ),
                })?;
            ArrowDataType::FixedSizeBinary(length)
        }
        DataType::Array(array_type) => ArrowDataType::List(
            Field::new_list_field(
                to_arrow_type(array_type.get_element_type())?,
                array_type.get_element_type().is_nullable(),
            )
            .into(),
        ),
        DataType::Map(map_type) => {
            let key_type = to_arrow_type(map_type.key_type())?;
            let value_type = to_arrow_type(map_type.value_type())?;
            let entry_fields = vec![
                Field::new("key", key_type, map_type.key_type().is_nullable()),
                Field::new("value", value_type, map_type.value_type().is_nullable()),
            ];
            ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowDataType::Struct(arrow_schema::Fields::from(entry_fields)),
                    false,
                )),
                false,
            )
        }
        DataType::Row(row_type) => {
            let fields: Result<Vec<Field>> = row_type
                .fields()
                .iter()
                .map(|f| {
                    Ok(Field::new(
                        f.name(),
                        to_arrow_type(f.data_type())?,
                        f.data_type().is_nullable(),
                    ))
                })
                .collect();
            ArrowDataType::Struct(arrow_schema::Fields::from(fields?))
        }
    })
}

/// Like `from_arrow_type`, but also reads the Field's nullability —
/// Arrow stores it on the Field wrapper, not the leaf data type.
pub fn from_arrow_field(field: &arrow_schema::Field) -> Result<DataType> {
    let mut dt = from_arrow_type(field.data_type())?;
    if !field.is_nullable() {
        dt = dt.as_non_nullable();
    }
    Ok(dt)
}

/// Converts an Arrow data type back to a Fluss `DataType`.
/// Used for reading array elements from Arrow ListArray back into Fluss types.
pub(crate) fn from_arrow_type(arrow_type: &ArrowDataType) -> Result<DataType> {
    use crate::metadata::DataTypes;

    Ok(match arrow_type {
        ArrowDataType::Boolean => DataTypes::boolean(),
        ArrowDataType::Int8 => DataTypes::tinyint(),
        ArrowDataType::Int16 => DataTypes::smallint(),
        ArrowDataType::Int32 => DataTypes::int(),
        ArrowDataType::Int64 => DataTypes::bigint(),
        // No unsigned types in Fluss; map to the signed type of the same width.
        ArrowDataType::UInt8 => DataTypes::tinyint(),
        ArrowDataType::UInt16 => DataTypes::smallint(),
        ArrowDataType::UInt32 => DataTypes::int(),
        ArrowDataType::UInt64 => DataTypes::bigint(),
        ArrowDataType::Float32 => DataTypes::float(),
        ArrowDataType::Float64 => DataTypes::double(),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataTypes::string(),
        ArrowDataType::Binary | ArrowDataType::LargeBinary => DataTypes::bytes(),
        ArrowDataType::Date32 | ArrowDataType::Date64 => DataTypes::date(),
        ArrowDataType::FixedSizeBinary(len) => {
            if *len < 0 {
                return Err(Error::IllegalArgument {
                    message: format!("FixedSizeBinary length must be >= 0, got {len}"),
                });
            }
            DataTypes::binary(*len as usize)
        }
        ArrowDataType::Decimal128(p, s) => {
            if *s < 0 {
                return Err(Error::IllegalArgument {
                    message: format!("Decimal scale must be >= 0, got {s}"),
                });
            }
            DataTypes::decimal(*p as u32, *s as u32)
        }
        ArrowDataType::Time32(arrow_schema::TimeUnit::Second) => DataTypes::time_with_precision(0),
        ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
            DataTypes::time_with_precision(3)
        }
        ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            DataTypes::time_with_precision(6)
        }
        ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
            DataTypes::time_with_precision(9)
        }
        ArrowDataType::Timestamp(unit, tz) => {
            let precision = match unit {
                arrow_schema::TimeUnit::Second => 0,
                arrow_schema::TimeUnit::Millisecond => 3,
                arrow_schema::TimeUnit::Microsecond => 6,
                arrow_schema::TimeUnit::Nanosecond => 9,
            };

            if tz.is_some() {
                DataTypes::timestamp_ltz_with_precision(precision)
            } else {
                DataTypes::timestamp_with_precision(precision)
            }
        }
        ArrowDataType::List(field) => DataTypes::array(from_arrow_field(field)?),
        ArrowDataType::Map(entries_field, _sorted) => {
            let fields = match entries_field.data_type() {
                ArrowDataType::Struct(f) => f,
                other => {
                    return Err(Error::IllegalArgument {
                        message: format!("Map entries must be Struct, got {other:?}"),
                    });
                }
            };
            if fields.len() != 2 {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Map entries Struct must have 2 fields (key, value), got {}",
                        fields.len()
                    ),
                });
            }
            DataTypes::map(from_arrow_field(&fields[0])?, from_arrow_field(&fields[1])?)
        }
        ArrowDataType::Struct(fields) => {
            let row_fields: Result<Vec<DataField>> = fields
                .iter()
                .map(|f| Ok(DataField::new(f.name(), from_arrow_field(f)?, None)))
                .collect();
            DataTypes::row(row_fields?)
        }
        other => {
            return Err(Error::IllegalArgument {
                message: format!("Cannot convert Arrow type to Fluss type: {other:?}"),
            });
        }
    })
}

#[derive(Clone)]
pub struct ReadContext {
    target_schema: SchemaRef,
    full_schema: SchemaRef,
    row_type: Arc<RowType>,
    projection: Option<Projection>,
    is_from_remote: bool,
    fluss_row_type: Option<Arc<RowType>>,
    align_to_target_schema: bool,
}

#[derive(Clone)]
struct Projection {
    ordered_schema: SchemaRef,
    projected_fields: Vec<usize>,
    ordered_fields: Vec<usize>,

    reordering_indexes: Vec<usize>,
    reordering_needed: bool,
}

impl ReadContext {
    pub fn new(
        arrow_schema: SchemaRef,
        row_type: Arc<RowType>,
        is_from_remote: bool,
    ) -> ReadContext {
        ReadContext {
            target_schema: arrow_schema.clone(),
            full_schema: arrow_schema,
            row_type,
            projection: None,
            is_from_remote,
            fluss_row_type: None,
            align_to_target_schema: false,
        }
    }

    pub fn with_fluss_row_type(mut self, fluss_row_type: Arc<RowType>) -> ReadContext {
        self.fluss_row_type = Some(fluss_row_type);
        self
    }

    pub fn fluss_row_type(&self) -> Option<&Arc<RowType>> {
        self.fluss_row_type.as_ref()
    }

    pub(crate) fn target_schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }

    pub(crate) fn row_type_arc(&self) -> Arc<RowType> {
        self.row_type.clone()
    }

    pub(crate) fn with_target_schema_alignment(mut self, target_schema: SchemaRef) -> ReadContext {
        debug_assert!(
            self.projection.is_none(),
            "target schema alignment is not supported with projection"
        );
        self.target_schema = target_schema;
        self.align_to_target_schema = true;
        self
    }

    pub fn with_projection_pushdown(
        arrow_schema: SchemaRef,
        row_type: Arc<RowType>,
        projected_fields: Vec<usize>,
        is_from_remote: bool,
    ) -> Result<ReadContext> {
        Self::validate_projection(&arrow_schema, projected_fields.as_slice())?;
        let target_schema =
            Self::project_schema(arrow_schema.clone(), projected_fields.as_slice())?;
        // the logic is little bit of hard to understand, to refactor it to follow
        // java side
        let (need_do_reorder, sorted_fields) = {
            // currently, for remote read, arrow log doesn't support projection pushdown,
            // so, only need to do reordering when is not from remote
            if !is_from_remote {
                let mut sorted_fields = projected_fields.clone();
                sorted_fields.sort_unstable();
                (!sorted_fields.eq(&projected_fields), sorted_fields)
            } else {
                // sorted_fields won't be used when need_do_reorder is false,
                // let's use an empty vec directly
                (false, vec![])
            }
        };

        let project = {
            if need_do_reorder {
                // reordering is required
                // Calculate reordering indexes to transform from sorted order to user-requested order
                let mut reordering_indexes = Vec::with_capacity(projected_fields.len());
                for &original_idx in &projected_fields {
                    let pos = sorted_fields.binary_search(&original_idx).map_err(|_| {
                        IllegalArgument {
                            message: format!(
                                "Projection index {original_idx} is invalid for the current schema."
                            ),
                        }
                    })?;
                    reordering_indexes.push(pos);
                }
                Projection {
                    ordered_schema: Self::project_schema(
                        arrow_schema.clone(),
                        sorted_fields.as_slice(),
                    )?,
                    projected_fields,
                    ordered_fields: sorted_fields,
                    reordering_indexes,
                    reordering_needed: true,
                }
            } else {
                Projection {
                    ordered_schema: Self::project_schema(
                        arrow_schema.clone(),
                        projected_fields.as_slice(),
                    )?,
                    ordered_fields: projected_fields.clone(),
                    projected_fields,
                    reordering_indexes: vec![],
                    reordering_needed: false,
                }
            }
        };

        Ok(ReadContext {
            target_schema,
            full_schema: arrow_schema,
            row_type,
            projection: Some(project),
            is_from_remote,
            fluss_row_type: None,
            align_to_target_schema: false,
        })
    }

    fn validate_projection(schema: &SchemaRef, projected_fields: &[usize]) -> Result<()> {
        let field_count = schema.fields().len();
        for &index in projected_fields {
            if index >= field_count {
                return Err(IllegalArgument {
                    message: format!(
                        "Projection index {index} is out of bounds for schema with {field_count} fields."
                    ),
                });
            }
        }
        Ok(())
    }

    pub fn project_schema(schema: SchemaRef, projected_fields: &[usize]) -> Result<SchemaRef> {
        Ok(SchemaRef::new(schema.project(projected_fields).map_err(
            |e| IllegalArgument {
                message: format!("Invalid projection: {e}"),
            },
        )?))
    }

    pub fn project_fields(&self) -> Option<&[usize]> {
        self.projection
            .as_ref()
            .map(|p| p.projected_fields.as_slice())
    }

    pub fn project_fields_in_order(&self) -> Option<&[usize]> {
        self.projection
            .as_ref()
            .map(|p| p.ordered_fields.as_slice())
    }

    pub fn record_batch(&self, data: &[u8]) -> Result<RecordBatch> {
        let (batch_metadata, body_buffer, version) = parse_ipc_message(data)?;

        let resolve_schema = {
            // if from remote, no projection, need to use full schema
            if self.is_from_remote || self.align_to_target_schema {
                self.full_schema.clone()
            } else {
                // the record batch from server must be ordered by field pos,
                // according to project to decide what arrow schema to use
                // to parse the record batch
                match self.projection {
                    Some(ref projection) => {
                        // projection, should use ordered schema by project field pos
                        projection.ordered_schema.clone()
                    }
                    None => {
                        // no projection, use target output schema
                        self.target_schema.clone()
                    }
                }
            }
        };

        let record_batch = read_record_batch(
            &body_buffer,
            batch_metadata,
            resolve_schema,
            &HashMap::new(),
            None,
            &version,
        )?;

        let record_batch = match &self.projection {
            Some(projection) => {
                let reordered_columns = {
                    // need to do reorder
                    if self.is_from_remote {
                        Some(&projection.projected_fields)
                    } else if projection.reordering_needed {
                        Some(&projection.reordering_indexes)
                    } else {
                        None
                    }
                };
                match reordered_columns {
                    Some(reordered_columns) => {
                        let arrow_columns = reordered_columns
                            .iter()
                            .map(|&idx| record_batch.column(idx).clone())
                            .collect();
                        RecordBatch::try_new(self.target_schema.clone(), arrow_columns)?
                    }
                    _ => record_batch,
                }
            }
            _ => record_batch,
        };
        let record_batch = if self.align_to_target_schema {
            align_record_batch_to_schema(record_batch, self.target_schema.clone())?
        } else {
            record_batch
        };
        Ok(record_batch)
    }

    pub fn record_batch_for_remote_log(&self, data: &[u8]) -> Result<Option<RecordBatch>> {
        let (batch_metadata, body_buffer, version) = parse_ipc_message(data)?;

        let record_batch = read_record_batch(
            &body_buffer,
            batch_metadata,
            self.full_schema.clone(),
            &HashMap::new(),
            None,
            &version,
        )?;

        let record_batch = match &self.projection {
            Some(projection) => {
                let projected_columns: Vec<_> = projection
                    .projected_fields
                    .iter()
                    .map(|&idx| record_batch.column(idx).clone())
                    .collect();
                RecordBatch::try_new(self.target_schema.clone(), projected_columns)?
            }
            None => record_batch,
        };
        let record_batch = if self.align_to_target_schema {
            align_record_batch_to_schema(record_batch, self.target_schema.clone())?
        } else {
            record_batch
        };
        Ok(Some(record_batch))
    }
}

fn align_record_batch_to_schema(
    record_batch: RecordBatch,
    target_schema: SchemaRef,
) -> Result<RecordBatch> {
    if record_batch.schema_ref() == &target_schema {
        return Ok(record_batch);
    }

    let row_count = record_batch.num_rows();
    let source_schema = record_batch.schema();
    let mut columns = Vec::with_capacity(target_schema.fields().len());

    for target_field in target_schema.fields() {
        match source_schema.index_of(target_field.name()) {
            Ok(source_idx) => {
                let column = record_batch.column(source_idx);
                if column.data_type() != target_field.data_type() {
                    return Err(Error::UnexpectedError {
                        message: format!(
                            "Cannot align column '{}' from type {:?} to {:?}",
                            target_field.name(),
                            column.data_type(),
                            target_field.data_type()
                        ),
                        source: None,
                    });
                }
                columns.push(column.clone());
            }
            Err(_) => columns.push(new_null_array(target_field.data_type(), row_count)),
        }
    }

    Ok(RecordBatch::try_new(target_schema, columns)?)
}

pub enum LogRecordIterator {
    Empty,
    Arrow(ArrowLogRecordIterator),
}

impl LogRecordIterator {
    pub fn empty() -> Self {
        LogRecordIterator::Empty
    }
}

impl Iterator for LogRecordIterator {
    type Item = ScanRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            LogRecordIterator::Empty => None,
            LogRecordIterator::Arrow(iter) => iter.next(),
        }
    }
}

/// Per-record change types decoded from a log batch.
///
/// Append-only batches carry no change-type vector on the wire, so a single
/// `AppendOnly` value covers every record without allocating. Changelog batches
/// (the CDC stream of a primary-key table) decode one change type per record,
/// in record order.
enum BatchChangeTypes {
    /// Every record shares this change type (append-only batches).
    Uniform(ChangeType),
    /// One change type per record, indexed by row id (changelog batches).
    PerRecord(Vec<ChangeType>),
}

impl BatchChangeTypes {
    fn get(&self, row_id: usize) -> ChangeType {
        match self {
            BatchChangeTypes::Uniform(change_type) => *change_type,
            BatchChangeTypes::PerRecord(change_types) => change_types[row_id],
        }
    }
}

pub struct ArrowLogRecordIterator {
    reader: ArrowReader,
    base_offset: i64,
    timestamp: i64,
    row_id: usize,
    change_types: BatchChangeTypes,
}

impl ArrowLogRecordIterator {
    fn new(
        reader: ArrowReader,
        base_offset: i64,
        timestamp: i64,
        change_types: BatchChangeTypes,
    ) -> Result<Self> {
        if let BatchChangeTypes::PerRecord(ref change_types) = change_types {
            if change_types.len() != reader.row_count() {
                return Err(Error::UnexpectedError {
                    message: format!(
                        "Changelog batch decode mismatch: {} change types for {} Arrow rows",
                        change_types.len(),
                        reader.row_count()
                    ),
                    source: None,
                });
            }
        }

        Ok(Self {
            reader,
            base_offset,
            timestamp,
            row_id: 0,
            change_types,
        })
    }
}

impl Iterator for ArrowLogRecordIterator {
    type Item = ScanRecord;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row_id >= self.reader.row_count() {
            return None;
        }

        let columnar_row = self.reader.read(self.row_id);
        let scan_record = ScanRecord::new(
            columnar_row,
            self.base_offset + self.row_id as i64,
            self.timestamp,
            self.change_types.get(self.row_id),
        );
        self.row_id += 1;
        Some(scan_record)
    }
}

pub struct ArrowReader {
    batch: Arc<TypedBatch>,
}

impl ArrowReader {
    pub fn new(record_batch: Arc<RecordBatch>, row_type: Arc<RowType>) -> Result<Self> {
        Self::new_with_fluss_row_type(record_batch, row_type, None)
    }

    pub fn new_with_fluss_row_type(
        record_batch: Arc<RecordBatch>,
        row_type: Arc<RowType>,
        fluss_row_type: Option<Arc<RowType>>,
    ) -> Result<Self> {
        let schema = fluss_row_type.as_deref().unwrap_or(&row_type);
        let typed = TypedBatch::build(&record_batch, schema)?;
        Ok(ArrowReader {
            batch: Arc::new(typed),
        })
    }

    pub fn row_count(&self) -> usize {
        self.batch.num_rows
    }

    pub fn read(&self, row_id: usize) -> ColumnarRow {
        ColumnarRow::from_typed_batch(Arc::clone(&self.batch), row_id)
    }
}
pub struct MyVec<T>(pub StreamReader<T>);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::WriteRecord;
    use crate::compression::{
        ArrowCompressionInfo, ArrowCompressionType, DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
    };
    use crate::metadata::{DataField, DataTypes, PhysicalTablePath, RowType, TablePath};
    use crate::row::{DataGetters, GenericRow};
    use crate::test_utils::build_table_info;

    #[test]
    fn test_to_array_type() {
        assert_eq!(
            to_arrow_type(&DataTypes::boolean()).unwrap(),
            ArrowDataType::Boolean
        );
        assert_eq!(
            to_arrow_type(&DataTypes::tinyint()).unwrap(),
            ArrowDataType::Int8
        );
        assert_eq!(
            to_arrow_type(&DataTypes::smallint()).unwrap(),
            ArrowDataType::Int16
        );
        assert_eq!(
            to_arrow_type(&DataTypes::bigint()).unwrap(),
            ArrowDataType::Int64
        );
        assert_eq!(
            to_arrow_type(&DataTypes::int()).unwrap(),
            ArrowDataType::Int32
        );
        assert_eq!(
            to_arrow_type(&DataTypes::float()).unwrap(),
            ArrowDataType::Float32
        );
        assert_eq!(
            to_arrow_type(&DataTypes::double()).unwrap(),
            ArrowDataType::Float64
        );
        assert_eq!(
            to_arrow_type(&DataTypes::char(16)).unwrap(),
            ArrowDataType::Utf8
        );
        assert_eq!(
            to_arrow_type(&DataTypes::string()).unwrap(),
            ArrowDataType::Utf8
        );
        assert_eq!(
            to_arrow_type(&DataTypes::decimal(10, 2)).unwrap(),
            ArrowDataType::Decimal128(10, 2)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::date()).unwrap(),
            ArrowDataType::Date32
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time()).unwrap(),
            ArrowDataType::Time32(arrow_schema::TimeUnit::Second)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time_with_precision(3)).unwrap(),
            ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time_with_precision(6)).unwrap(),
            ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::time_with_precision(9)).unwrap(),
            ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(0)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(3)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(6)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_with_precision(9)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(0)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, Some("UTC".into()))
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(3)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into()))
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(6)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            to_arrow_type(&DataTypes::timestamp_ltz_with_precision(9)).unwrap(),
            ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            to_arrow_type(&DataTypes::bytes()).unwrap(),
            ArrowDataType::Binary
        );
        assert_eq!(
            to_arrow_type(&DataTypes::binary(16)).unwrap(),
            ArrowDataType::FixedSizeBinary(16)
        );

        assert_eq!(
            to_arrow_type(&DataTypes::array(DataTypes::int())).unwrap(),
            ArrowDataType::List(Field::new_list_field(ArrowDataType::Int32, true).into())
        );

        assert_eq!(
            to_arrow_type(&DataTypes::map(DataTypes::string(), DataTypes::int())).unwrap(),
            ArrowDataType::Map(
                Arc::new(Field::new(
                    "entries",
                    ArrowDataType::Struct(arrow_schema::Fields::from(vec![
                        Field::new("key", ArrowDataType::Utf8, false),
                        Field::new("value", ArrowDataType::Int32, true),
                    ])),
                    false,
                )),
                false,
            )
        );

        assert_eq!(
            to_arrow_type(&DataTypes::row(vec![
                DataTypes::field("f1", DataTypes::int()),
                DataTypes::field("f2", DataTypes::string()),
            ]))
            .unwrap(),
            ArrowDataType::Struct(arrow_schema::Fields::from(vec![
                Field::new("f1", ArrowDataType::Int32, true),
                Field::new("f2", ArrowDataType::Utf8, true),
            ]))
        );
    }

    #[test]
    fn test_arrow_map_schema_strictness() {
        let map_type = DataTypes::map(DataTypes::string(), DataTypes::int());
        let arrow_type = to_arrow_type(&map_type).unwrap();

        if let ArrowDataType::Map(entries_field, _) = arrow_type {
            assert!(
                !entries_field.is_nullable(),
                "Arrow Map 'entries' field must be strictly non-nullable"
            );
        } else {
            panic!("Expected ArrowDataType::Map, got {:?}", arrow_type);
        }
    }

    #[test]
    fn test_from_arrow_type_preserves_container_field_nullability() {
        let arrow_list = ArrowDataType::List(Arc::new(arrow_schema::Field::new(
            "item",
            ArrowDataType::Int32,
            false,
        )));
        match from_arrow_type(&arrow_list).unwrap() {
            DataType::Array(at) => assert!(!at.get_element_type().is_nullable()),
            other => panic!("expected Array, got {other:?}"),
        }

        let entries_struct = ArrowDataType::Struct(arrow_schema::Fields::from(vec![
            arrow_schema::Field::new("key", ArrowDataType::Utf8, false),
            arrow_schema::Field::new("value", ArrowDataType::Int32, false),
        ]));
        let entries_field = arrow_schema::Field::new("entries", entries_struct, false);
        let arrow_map = ArrowDataType::Map(Arc::new(entries_field), false);
        match from_arrow_type(&arrow_map).unwrap() {
            DataType::Map(m) => {
                assert!(!m.key_type().is_nullable());
                assert!(!m.value_type().is_nullable());
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn test_from_arrow_type_accepts_unsigned_large_and_date64() {
        assert!(matches!(
            from_arrow_type(&ArrowDataType::UInt8).unwrap(),
            DataType::TinyInt(_)
        ));
        assert!(matches!(
            from_arrow_type(&ArrowDataType::UInt16).unwrap(),
            DataType::SmallInt(_)
        ));
        assert!(matches!(
            from_arrow_type(&ArrowDataType::UInt32).unwrap(),
            DataType::Int(_)
        ));
        assert!(matches!(
            from_arrow_type(&ArrowDataType::UInt64).unwrap(),
            DataType::BigInt(_)
        ));
        assert!(matches!(
            from_arrow_type(&ArrowDataType::LargeUtf8).unwrap(),
            DataType::String(_)
        ));
        assert!(matches!(
            from_arrow_type(&ArrowDataType::LargeBinary).unwrap(),
            DataType::Bytes(_)
        ));
        assert!(matches!(
            from_arrow_type(&ArrowDataType::Date64).unwrap(),
            DataType::Date(_)
        ));
    }

    #[test]
    fn test_parse_ipc_message() {
        let empty_body: &[u8] = &le_bytes(&[0xFFFFFFFF, 0x00000000]);
        let result = parse_ipc_message(empty_body);
        assert_eq!(
            result.unwrap_err().to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Range [0, 4) is out of bounds.\n\n: ParseError(\"Range [0, 4) is out of bounds.\\n\\n\")."
            )
        );

        let invalid_data = &[];
        assert_eq!(
            parse_ipc_message(invalid_data).unwrap_err().to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Invalid data length: 0: ParseError(\"Invalid data length: 0\")."
            )
        );

        let data_with_invalid_continuation: &[u8] = &le_bytes(&[0x00000001, 0x00000000]);
        assert_eq!(
            parse_ipc_message(data_with_invalid_continuation)
                .unwrap_err()
                .to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Invalid continuation marker: 1: ParseError(\"Invalid continuation marker: 1\")."
            )
        );

        let data_with_invalid_length: &[u8] = &le_bytes(&[0xFFFFFFFF, 0x00000001]);
        assert_eq!(
            parse_ipc_message(data_with_invalid_length)
                .unwrap_err()
                .to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Invalid data length. Remaining data length 0 is shorter than specified size 1: ParseError(\"Invalid data length. Remaining data length 0 is shorter than specified size 1\")."
            )
        );

        let data_with_invalid_length = &le_bytes(&[0xFFFFFFFF, 0x00000004, 0x00000000]);
        assert_eq!(
            parse_ipc_message(data_with_invalid_length)
                .unwrap_err()
                .to_string(),
            String::from(
                "Fluss hitting Arrow error Parser error: Not a record batch: ParseError(\"Not a record batch\")."
            )
        );
    }

    #[test]
    fn projection_rejects_out_of_bounds_index() {
        let row_type = RowType::new(vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("name", DataTypes::string(), None),
        ]);
        let schema = to_arrow_schema(&row_type).unwrap();
        let result =
            ReadContext::with_projection_pushdown(schema, Arc::new(row_type), vec![0, 2], false);

        assert!(matches!(result, Err(IllegalArgument { .. })));
    }

    #[test]
    fn checksum_and_schema_id_read_minimum_header() {
        // Header-only batches with record_count == 0 are valid; this covers the minimal bytes
        // needed for checksum/schema_id access.
        let mut data = vec![0u8; SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH];
        let crc = 0xA1B2C3D4u32;
        let schema_id = 42i16;
        LittleEndian::write_u32(&mut data[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH], crc);
        LittleEndian::write_i16(
            &mut data[SCHEMA_ID_OFFSET..SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH],
            schema_id,
        );

        let batch = LogRecordBatch::new(Bytes::from(data));
        assert_eq!(batch.checksum(), crc);
        assert_eq!(batch.schema_id(), schema_id);

        let expected = crc32c(&batch.data[SCHEMA_ID_OFFSET..]);
        assert_eq!(batch.compute_checksum(), expected);
    }

    fn le_bytes(vals: &[u32]) -> Vec<u8> {
        let mut out = Vec::with_capacity(vals.len() * 4);
        for &v in vals {
            out.extend_from_slice(&v.to_le_bytes());
        }
        out
    }

    #[test]
    fn test_temporal_and_decimal_builder_validation() {
        use crate::row::column_writer::ColumnWriter;
        use arrow::array::Array;

        // Test valid builder creation with precision=10, scale=2
        let mut writer = ColumnWriter::create(
            &DataTypes::decimal(10, 2),
            &ArrowDataType::Decimal128(10, 2),
            0,
            256,
        )
        .unwrap();
        let array = writer.finish();
        assert_eq!(array.data_type(), &ArrowDataType::Decimal128(10, 2));

        // Test error case: invalid Arrow precision/scale (exceeds Arrow's limit)
        let result = ColumnWriter::create(
            &DataTypes::decimal(10, 2),
            &ArrowDataType::Decimal128(100, 50),
            0,
            256,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_decimal_rescaling_and_validation() -> Result<()> {
        use crate::row::{Datum, Decimal, GenericRow};
        use arrow::array::Decimal128Array;
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        // Test 1: Rescaling from scale 3 to scale 2
        let row_type = RowType::new(vec![DataField::new(
            "amount",
            DataTypes::decimal(10, 2),
            None,
        )]);
        let mut builder = RowAppendRecordBatchBuilder::new(&row_type)?;
        let decimal = Decimal::from_big_decimal(BigDecimal::from_str("123.456").unwrap(), 10, 3)?;
        let row = GenericRow {
            values: vec![Datum::Decimal(decimal)],
        };
        builder.append(&row)?;
        let batch = builder.build_arrow_record_batch()?;
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(array.value(0), 12346); // 123.456 rounded to 2 decimal places
        assert_eq!(array.scale(), 2);

        // Test 2: Precision overflow (should error)
        let row_type = RowType::new(vec![DataField::new(
            "amount",
            DataTypes::decimal(5, 2),
            None,
        )]);
        let mut builder = RowAppendRecordBatchBuilder::new(&row_type)?;
        let decimal = Decimal::from_big_decimal(BigDecimal::from_str("123456.78").unwrap(), 10, 2)?;
        let row = GenericRow {
            values: vec![Datum::Decimal(decimal)],
        };
        let result = builder.append(&row);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("precision overflow")
        );

        Ok(())
    }

    // Tests for file-backed streaming

    #[test]
    fn test_file_source_streaming() -> Result<()> {
        use tempfile::NamedTempFile;

        // Test 1: Basic file reads work
        let test_data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut tmp_file = NamedTempFile::new()?;
        tmp_file.write_all(&test_data)?;
        tmp_file.flush()?;

        let file_path = tmp_file.path().to_path_buf();
        let file = File::open(&file_path)?;
        let mut source = FileSource::new(file, 0, file_path)?;

        // Read full data
        let data = source.read_batch_data(0, 10)?;
        assert_eq!(data.to_vec(), test_data);

        // Read partial data
        let partial = source.read_batch_data(2, 5)?;
        assert_eq!(partial.to_vec(), vec![3, 4, 5, 6, 7]);

        // Test 2: base_offset works (critical for remote logs with pos_in_log_segment)
        let prefix = vec![0xFF; 100];
        let actual_data = vec![1, 2, 3, 4, 5];
        let mut tmp_file2 = NamedTempFile::new()?;
        tmp_file2.write_all(&prefix)?;
        tmp_file2.write_all(&actual_data)?;
        tmp_file2.flush()?;

        let file_path2 = tmp_file2.path().to_path_buf();
        let file2 = File::open(&file_path2)?;
        let mut source2 = FileSource::new(file2, 100, file_path2)?; // Skip first 100 bytes

        assert_eq!(source2.total_size(), 5); // Only counts data after offset
        let data2 = source2.read_batch_data(0, 5)?;
        assert_eq!(data2.to_vec(), actual_data);

        Ok(())
    }

    #[test]
    fn test_all_types_end_to_end() -> Result<()> {
        use crate::row::{Date, Datum, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};
        use arrow::array::{
            Date32Array, Decimal128Array, Int32Array, Time32MillisecondArray,
            Time64NanosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
        };
        use bigdecimal::BigDecimal;
        use std::str::FromStr;

        // Schema with int, decimal, date, time (ms + ns), timestamps (μs + ns)
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("amount".to_string(), DataTypes::decimal(10, 2), None),
            DataField::new("date".to_string(), DataTypes::date(), None),
            DataField::new(
                "time_ms".to_string(),
                DataTypes::time_with_precision(3),
                None,
            ),
            DataField::new(
                "time_ns".to_string(),
                DataTypes::time_with_precision(9),
                None,
            ),
            DataField::new(
                "ts_us".to_string(),
                DataTypes::timestamp_with_precision(6),
                None,
            ),
            DataField::new(
                "ts_ltz_ns".to_string(),
                DataTypes::timestamp_ltz_with_precision(9),
                None,
            ),
        ]);

        let mut builder = RowAppendRecordBatchBuilder::new(&row_type)?;

        // Append rows with various data types
        let row = GenericRow {
            values: vec![
                Datum::Int32(1),
                Datum::Decimal(Decimal::from_big_decimal(
                    BigDecimal::from_str("123.456").unwrap(),
                    10,
                    3,
                )?),
                // 18000 days since epoch = 2019-04-14
                Datum::Date(Date::new(18000)),
                // 43200000 ms = 12:00:00.000 (noon)
                Datum::Time(Time::new(43200000)),
                // 12345 ms = 00:00:12.345
                Datum::Time(Time::new(12345)),
                // 1609459200000 ms = 2021-01-01 00:00:00 UTC, with 123456 additional nanoseconds
                Datum::TimestampNtz(TimestampNtz::from_millis_nanos(1609459200000, 123456)?),
                // 1609459200000 ms = 2021-01-01 00:00:00 UTC, with 987654 additional nanoseconds
                Datum::TimestampLtz(TimestampLtz::from_millis_nanos(1609459200000, 987654)?),
            ],
        };
        builder.append(&row)?;

        let batch = builder.build_arrow_record_batch()?;

        // Verify all conversions
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            1
        );

        let dec = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(dec.value(0), 12346); // 123.456 rounded to 2 decimal places

        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(0),
            18000
        );

        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .unwrap()
                .value(0),
            43200000
        );

        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap()
                .value(0),
            12345000000
        );

        // Timestamp with sub-millisecond nanos preserved
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(0),
            1609459200000123
        );

        assert_eq!(
            batch
                .column(6)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(0),
            1609459200000987654
        );

        Ok(())
    }

    #[test]
    fn test_log_records_batches_from_file() -> Result<()> {
        use crate::client::WriteRecord;
        use crate::compression::{
            ArrowCompressionInfo, ArrowCompressionType, DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
        };
        use crate::metadata::{PhysicalTablePath, TablePath};
        use crate::row::GenericRow;
        use tempfile::NamedTempFile;

        // Integration test: Real log record batch streamed from file
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            &row_type,
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
            usize::MAX,
            Arc::new(ArrowCompressionRatioEstimator::default()),
        )?;

        let mut row = GenericRow::new(2);
        row.set_field(0, 1_i32);
        row.set_field(1, "alice");
        let record = WriteRecord::for_append(
            Arc::clone(&table_info),
            physical_table_path.clone(),
            1,
            &row,
        );
        builder.append(&record)?;

        let mut row2 = GenericRow::new(2);
        row2.set_field(0, 2_i32);
        row2.set_field(1, "bob");
        let record2 =
            WriteRecord::for_append(Arc::clone(&table_info), physical_table_path, 2, &row2);
        builder.append(&record2)?;

        let data = builder.build()?;

        // Write to file
        let mut tmp_file = NamedTempFile::new()?;
        tmp_file.write_all(&data)?;
        tmp_file.flush()?;

        // Create file-backed LogRecordsBatches (should stream, not load all into memory)
        let file_path = tmp_file.path().to_path_buf();
        let file = File::open(&file_path)?;
        let mut batches = LogRecordsBatches::from_file(file, 0, file_path)?;

        // Iterate through batches (should work just like in-memory)
        let batch = batches.next().expect("Should have at least one batch")?;
        assert!(batch.size_in_bytes() > 0);
        assert_eq!(batch.record_count(), 2);

        Ok(())
    }

    /// Builds an append-only `(id INT, name STRING)` Arrow log batch from `rows`.
    /// The writer always emits append-only batches, so changelog tests derive
    /// their bytes from this with [`splice_change_type_vector`].
    fn build_append_only_batch(rows: &[(i32, &str)]) -> (RowType, Vec<u8>) {
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            &row_type,
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
            usize::MAX,
            Arc::new(ArrowCompressionRatioEstimator::default()),
        )
        .unwrap();

        for (id, name) in rows {
            let mut row = GenericRow::new(2);
            row.set_field(0, *id);
            row.set_field(1, *name);
            let record = WriteRecord::for_append(
                Arc::clone(&table_info),
                physical_table_path.clone(),
                1,
                &row,
            );
            builder.append(&record).unwrap();
        }

        (row_type, builder.build().unwrap())
    }

    /// Turns an append-only batch into a wire-valid changelog batch: clears the
    /// append-only flag, splices one change-type byte per record between the
    /// header and the Arrow payload, then fixes up the length field and CRC.
    fn splice_change_type_vector(append_only: &[u8], change_types: &[ChangeType]) -> Vec<u8> {
        let mut data = append_only.to_vec();
        data[ATTRIBUTES_OFFSET] &= !APPEND_ONLY_FLAG_MASK;
        let change_bytes = change_types.iter().map(|ct| ct.to_byte_value());
        data.splice(RECORDS_OFFSET..RECORDS_OFFSET, change_bytes);

        let new_length = (data.len() - LOG_OVERHEAD) as i32;
        data[LENGTH_OFFSET..LENGTH_OFFSET + LENGTH_LENGTH]
            .copy_from_slice(&new_length.to_le_bytes());

        let crc = crc32c(&data[SCHEMA_ID_OFFSET..]);
        data[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH].copy_from_slice(&crc.to_le_bytes());
        data
    }

    #[test]
    fn decode_changelog_batch_applies_per_record_change_types() -> Result<()> {
        let (row_type, append_only) =
            build_append_only_batch(&[(1, "alice"), (2, "bob"), (3, "carol")]);
        let read_context = ReadContext::new(to_arrow_schema(&row_type)?, Arc::new(row_type), false);

        // Append-only batch: every record decodes as AppendOnly (regression guard).
        let batch = LogRecordsBatches::new(append_only.clone())
            .next()
            .expect("append-only batch")?;
        assert!(batch.is_append_only());
        let records: Vec<_> = batch.records(&read_context)?.collect();
        assert_eq!(records.len(), 3);
        assert!(
            records
                .iter()
                .all(|r| *r.change_type() == ChangeType::AppendOnly)
        );

        // Changelog variant: the spliced change-type vector drives per-record types.
        let change_types = [
            ChangeType::Insert,
            ChangeType::UpdateAfter,
            ChangeType::Delete,
        ];
        let changelog = splice_change_type_vector(&append_only, &change_types);
        let batch = LogRecordsBatches::new(changelog)
            .next()
            .expect("changelog batch")?;
        assert!(!batch.is_append_only());
        assert_eq!(batch.record_count(), 3);

        let records: Vec<_> = batch.records(&read_context)?.collect();
        let got: Vec<ChangeType> = records.iter().map(|r| *r.change_type()).collect();
        assert_eq!(got, change_types.to_vec());

        // The row payload and offsets survive the splice unchanged.
        let mut ids = Vec::new();
        for record in &records {
            ids.push(record.row().get_int(0)?);
        }
        assert_eq!(ids, vec![1, 2, 3]);
        let offsets: Vec<i64> = records.iter().map(|r| r.offset()).collect();
        assert_eq!(offsets, vec![0, 1, 2]);

        // Batch-level access skips the change-type vector and still decodes rows.
        let batch = LogRecordsBatches::new(splice_change_type_vector(&append_only, &change_types))
            .next()
            .expect("changelog batch")?;
        assert_eq!(batch.record_batch(&read_context)?.num_rows(), 3);

        Ok(())
    }

    #[test]
    fn decode_changelog_batch_rejects_invalid_change_type_byte() {
        let (row_type, append_only) = build_append_only_batch(&[(1, "a"), (2, "b")]);
        let read_context = ReadContext::new(
            to_arrow_schema(&row_type).unwrap(),
            Arc::new(row_type),
            false,
        );

        let mut changelog =
            splice_change_type_vector(&append_only, &[ChangeType::Insert, ChangeType::Insert]);
        // Corrupt the second change-type byte to an out-of-range value.
        changelog[RECORDS_OFFSET + 1] = 99;

        let batch = LogRecordBatch::new(Bytes::from(changelog));
        let err = batch
            .records(&read_context)
            .err()
            .expect("expected decode to reject an invalid change-type byte");
        assert!(matches!(err, Error::UnexpectedError { .. }));
        assert!(err.to_string().contains("change type"));
    }

    #[test]
    fn decode_changelog_batch_rejects_truncated_change_type_vector() {
        let (row_type, append_only) = build_append_only_batch(&[(1, "a"), (2, "b")]);
        let read_context = ReadContext::new(
            to_arrow_schema(&row_type).unwrap(),
            Arc::new(row_type),
            false,
        );

        // Clear the append-only flag, then cut the body shorter than the
        // record_count change-type bytes the decoder now expects.
        let mut data = append_only;
        data[ATTRIBUTES_OFFSET] &= !APPEND_ONLY_FLAG_MASK;
        data.truncate(RECORDS_OFFSET + 1);

        let batch = LogRecordBatch::new(Bytes::from(data));
        assert_eq!(batch.record_count(), 2);
        let err = batch
            .records(&read_context)
            .err()
            .expect("expected decode to reject a truncated change-type vector");
        assert!(matches!(err, Error::UnexpectedError { .. }));
    }
}
