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
use crate::metadata::{DataField, DataType, RowType, UNEXIST_MAPPING};
use crate::record::ScanRecord;
use crate::row::column_vector::TypedBatch;
use crate::row::column_writer::{ColumnWriter, round_up_to_8};
use crate::row::{ColumnarRow, InternalRow};
use arrow::array::{Array, ArrayBuilder, ArrayRef, new_null_array};
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
use crc32c::crc32c;
use std::{
    cell::Cell,
    collections::HashMap,
    io::{Cursor, Write},
    sync::Arc,
};

use super::log_record_batch::*;
use crate::error::Error::IllegalArgument;
use crate::record::statistics::{estimated_serialized_size, serialize_statistics};
use arrow::ipc::writer::IpcWriteOptions;

pub const BUILDER_DEFAULT_OFFSET: i64 = 0;

/// Initial capacity for Arrow column vectors (pre-allocation hint, not a record cap).
/// Matching Java's `ArrowWriter.INITIAL_CAPACITY`.
const INITIAL_ROW_CAPACITY: usize = 1024;

/// Fraction of the allocated buffer used as the effective write limit.
/// Matching Java's `ArrowWriter.BUFFER_USAGE_RATIO`.
const BUFFER_USAGE_RATIO: f32 = 0.95;

pub(crate) struct MemoryLogRecordsArrowBuilder {
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
    /// Statistics columns with their row type; `Some` upgrades the batch to V1.
    statistics: Option<(RowType, Vec<usize>)>,
    /// Per-schema estimate of the serialized statistics size.
    estimated_statistics_size: usize,
}

/// Table-level configuration for building Arrow log batches, shared by
/// `ArrowLogWriteBatch` and `MemoryLogRecordsArrowBuilder`.
pub(crate) struct ArrowBatchConfig {
    pub schema_id: i32,
    pub row_type: RowType,
    /// Statistics column mapping; `Some` upgrades batches to V1.
    pub stats_index_mapping: Option<Vec<usize>>,
    pub compression: ArrowCompressionInfo,
    pub write_limit: usize,
    pub compression_ratio_estimator: Arc<ArrowCompressionRatioEstimator>,
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

pub(crate) struct PrebuiltRecordBatchBuilder {
    row_type: RowType,
    arrow_record_batch: Option<Arc<RecordBatch>>,
    records_count: i32,
}

impl PrebuiltRecordBatchBuilder {
    fn new(row_type: RowType) -> Self {
        Self {
            row_type,
            arrow_record_batch: None,
            records_count: 0,
        }
    }
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
        validate_append_record_batch(record_batch.as_ref(), &self.row_type)?;
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
    pub(crate) fn new(config: ArrowBatchConfig, to_append_record_batch: bool) -> Result<Self> {
        let ArrowBatchConfig {
            schema_id,
            row_type,
            stats_index_mapping,
            compression: arrow_compression_info,
            write_limit,
            compression_ratio_estimator,
        } = config;
        let arrow_batch_builder: Box<dyn ArrowRecordBatchInnerBuilder> = {
            if to_append_record_batch {
                Box::new(PrebuiltRecordBatchBuilder::new(row_type.clone()))
            } else {
                Box::new(RowAppendRecordBatchBuilder::new(&row_type)?)
            }
        };
        let schema = to_arrow_schema(&row_type)?;
        let ipc_overhead =
            estimate_arrow_ipc_overhead(&schema, arrow_compression_info.get_compression_type())?;
        let effective_limit = (write_limit as f32 * BUFFER_USAGE_RATIO) as usize;
        let estimated_compression_ratio = compression_ratio_estimator.estimation();

        // Validate the mapping as Java does when building the collector's
        // stats row type.
        let field_count = row_type.fields().len();
        if let Some(mapping) = &stats_index_mapping {
            if let Some(&index) = mapping.iter().find(|&&index| index >= field_count) {
                return Err(IllegalArgument {
                    message: format!(
                        "Statistics column index {index} is out of range for {field_count} fields"
                    ),
                });
            }
        }
        let magic = if stats_index_mapping.is_some() {
            LOG_MAGIC_VALUE_V1
        } else {
            LOG_MAGIC_VALUE_V0
        };
        let estimated_statistics_size = stats_index_mapping
            .as_ref()
            .map_or(0, |mapping| estimated_serialized_size(&row_type, mapping));
        let statistics = stats_index_mapping.map(|mapping| (row_type, mapping));

        Ok(MemoryLogRecordsArrowBuilder {
            base_log_offset: BUILDER_DEFAULT_OFFSET,
            schema_id,
            magic,
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
            statistics,
            estimated_statistics_size,
        })
    }

    fn header_size(&self) -> usize {
        if self.magic >= LOG_MAGIC_VALUE_V1 {
            V1_RECORD_BATCH_HEADER_SIZE
        } else {
            RECORD_BATCH_HEADER_SIZE
        }
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
        // The header and CRC offsets below assume the V0/V1 layout without V2's leader epoch.
        debug_assert!(self.magic < LOG_MAGIC_VALUE_V2);

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

        let statistics_bytes = match &self.statistics {
            Some((row_type, mapping)) => {
                match serialize_statistics(record_batch.as_ref(), row_type, mapping) {
                    Ok(Some(bytes)) => bytes,
                    // Unlike Java's 27-byte empty block, an empty mapping
                    // writes length 0, which both parsers read as no statistics.
                    Ok(None) => Vec::new(),
                    // A failure degrades to an empty section rather than
                    // failing the batch, matching Java's builder.
                    Err(error) => {
                        log::error!("Failed to serialize statistics for record batch: {error}");
                        Vec::new()
                    }
                }
            }
            None => Vec::new(),
        };

        // now, write batch header, statistics and arrow batch
        let header_size = self.header_size();
        let mut batch_bytes =
            vec![0u8; header_size + statistics_bytes.len() + real_arrow_batch_bytes.len()];
        // write batch header
        self.write_batch_header(&mut batch_bytes[..], statistics_bytes.len())?;

        // write statistics and arrow batch bytes
        let mut cursor = Cursor::new(&mut batch_bytes[..]);
        cursor.set_position(header_size as u64);
        cursor.write_all(&statistics_bytes)?;
        cursor.write_all(real_arrow_batch_bytes)?;

        let calcute_crc_bytes = &cursor.get_ref()[SCHEMA_ID_OFFSET..];
        // then update crc
        let crc = crc32c(calcute_crc_bytes);
        cursor.set_position(CRC_OFFSET as u64);
        cursor.write_u32::<LittleEndian>(crc)?;

        Ok(batch_bytes.to_vec())
    }

    fn write_batch_header(&self, buffer: &mut [u8], statistics_length: usize) -> Result<()> {
        write_batch_header_fields(
            buffer,
            BatchHeaderFields {
                base_log_offset: self.base_log_offset,
                magic: self.magic,
                schema_id: self.schema_id,
                writer_id: self.writer_id,
                batch_sequence: self.batch_sequence,
                record_count: self.arrow_record_batch_builder.records_count(),
                statistics_length,
            },
        )
    }

    pub fn set_writer_state(&mut self, writer_id: i64, batch_base_sequence: i32) {
        self.writer_id = writer_id;
        self.batch_sequence = batch_base_sequence;
    }

    /// Estimated bytes written: header, statistics estimate (V1), Arrow IPC
    /// metadata and estimated compressed body.
    pub fn estimated_size_in_bytes(&self) -> usize {
        let body = self.arrow_record_batch_builder.estimated_size_in_bytes();
        let estimated_body = self.estimated_compressed_size(body);
        self.header_size() + self.estimated_statistics_size + self.ipc_overhead + estimated_body
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

    let fields = schema.fields();
    let mut probe_fields = Vec::with_capacity(fields.len());
    let mut null_arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for f in fields {
        null_arrays.push(new_null_array(f.data_type(), 1));
        probe_fields.push(f.as_ref().clone().with_nullable(true));
    }
    let probe_schema: SchemaRef = Arc::new(arrow_schema::Schema::new(probe_fields));
    let batch = RecordBatch::try_new(probe_schema.clone(), null_arrays)?;

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
    let mut writer = StreamWriter::try_new_with_options(&mut buf, &probe_schema, write_option?)?;
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

pub(crate) fn validate_append_record_batch(batch: &RecordBatch, row_type: &RowType) -> Result<()> {
    let typed = TypedBatch::build(batch, row_type)?;
    typed.check_not_null(row_type)
}

/// The fixed fields of a log record batch header.
pub(crate) struct BatchHeaderFields {
    pub base_log_offset: i64,
    pub magic: u8,
    pub schema_id: i32,
    pub writer_id: i64,
    pub batch_sequence: i32,
    pub record_count: i32,
    /// Only written for V1 and above.
    pub statistics_length: usize,
}

/// `buffer` must be the whole batch. Timestamp and CRC are left for the caller.
pub(crate) fn write_batch_header_fields(
    buffer: &mut [u8],
    fields: BatchHeaderFields,
) -> Result<()> {
    let total_len = buffer.len();
    let mut cursor = Cursor::new(buffer);
    cursor.write_i64::<LittleEndian>(fields.base_log_offset)?;
    cursor.write_i32::<LittleEndian>((total_len - BASE_OFFSET_LENGTH - LENGTH_LENGTH) as i32)?;
    cursor.write_u8(fields.magic)?;
    cursor.write_i64::<LittleEndian>(0)?; // timestamp placeholder
    cursor.write_u32::<LittleEndian>(0)?; // crc placeholder
    cursor.write_i16::<LittleEndian>(fields.schema_id as i16)?;

    // todo: curerntly, always is append only
    let append_only = true;
    cursor.write_u8(if append_only { 1 } else { 0 })?;
    cursor.write_i32::<LittleEndian>(if fields.record_count > 0 {
        fields.record_count - 1
    } else {
        0
    })?;

    cursor.write_i64::<LittleEndian>(fields.writer_id)?;
    cursor.write_i32::<LittleEndian>(fields.batch_sequence)?;
    cursor.write_i32::<LittleEndian>(fields.record_count)?;

    if fields.magic >= LOG_MAGIC_VALUE_V1 {
        cursor.write_i32::<LittleEndian>(fields.statistics_length as i32)?;
    }
    Ok(())
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
    schema_alignment: Option<Arc<[i32]>>,
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
            schema_alignment: None,
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

    pub(crate) fn with_target_schema_alignment(
        mut self,
        target_schema: SchemaRef,
        schema_alignment: Arc<[i32]>,
    ) -> ReadContext {
        debug_assert!(
            self.projection.is_none(),
            "target schema alignment is not supported with projection"
        );
        debug_assert_eq!(
            schema_alignment.len(),
            target_schema.fields().len(),
            "schema alignment length must match target schema"
        );
        debug_assert!(
            schema_alignment.iter().all(|source_index| {
                *source_index == UNEXIST_MAPPING
                    || usize::try_from(*source_index)
                        .is_ok_and(|index| index < self.full_schema.fields().len())
            }),
            "schema alignment contains an invalid source index"
        );
        debug_assert!(
            target_schema
                .fields()
                .iter()
                .zip(schema_alignment.iter())
                .all(|(target_field, source_index)| {
                    *source_index == UNEXIST_MAPPING
                        || self.full_schema.field(*source_index as usize).data_type()
                            == target_field.data_type()
                }),
            "schema alignment source and target types must match"
        );
        self.target_schema = target_schema;
        self.schema_alignment = Some(schema_alignment);
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
            schema_alignment: None,
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
            if self.is_from_remote || self.schema_alignment.is_some() {
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
        let record_batch = match &self.schema_alignment {
            Some(schema_alignment) => align_record_batch_to_schema(
                record_batch,
                self.target_schema.clone(),
                schema_alignment,
            )?,
            None => record_batch,
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
        let record_batch = match &self.schema_alignment {
            Some(schema_alignment) => align_record_batch_to_schema(
                record_batch,
                self.target_schema.clone(),
                schema_alignment,
            )?,
            None => record_batch,
        };
        Ok(Some(record_batch))
    }
}

fn align_record_batch_to_schema(
    record_batch: RecordBatch,
    target_schema: SchemaRef,
    schema_alignment: &[i32],
) -> Result<RecordBatch> {
    let row_count = record_batch.num_rows();
    let mut columns = Vec::with_capacity(target_schema.fields().len());

    for (target_field, source_index) in target_schema.fields().iter().zip(schema_alignment.iter()) {
        if *source_index == UNEXIST_MAPPING {
            columns.push(new_null_array(target_field.data_type(), row_count));
        } else {
            columns.push(record_batch.column(*source_index as usize).clone());
        }
    }

    Ok(RecordBatch::try_new(target_schema, columns)?)
}

pub struct ArrowLogRecordIterator {
    reader: ArrowReader,
    base_offset: i64,
    timestamp: i64,
    row_id: usize,
    change_types: BatchChangeTypes,
}

impl ArrowLogRecordIterator {
    pub(crate) fn new(
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
    use crate::metadata::{DataField, DataTypes, PhysicalTablePath, RowType, TablePath};
    use crate::row::{DataGetters, GenericRow};
    use crate::test_utils::{
        build_append_only_batch, build_table_info, uncompressed_arrow_batch_config,
    };
    use bytes::Bytes;

    #[test]
    fn nonnullable_append_builder_roundtrips_for_both_append_modes() {
        let row_type = RowType::new(vec![DataField::new(
            "id",
            DataTypes::bigint().as_non_nullable(),
            None,
        )]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        for to_append_record_batch in [false, true] {
            let mut builder = MemoryLogRecordsArrowBuilder::new(
                uncompressed_arrow_batch_config(1, &row_type, usize::MAX),
                to_append_record_batch,
            )
            .expect("NOT NULL builder should construct");

            if to_append_record_batch {
                let batch_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
                    "id",
                    arrow_schema::DataType::Int64,
                    false,
                )]));
                let batch = RecordBatch::try_new(
                    batch_schema,
                    vec![Arc::new(arrow::array::Int64Array::from(vec![1_i64, 2_i64]))
                        as arrow::array::ArrayRef],
                )
                .expect("non-nullable record batch");
                let record = WriteRecord::for_append_record_batch(
                    Arc::clone(&table_info),
                    physical_table_path.clone(),
                    1,
                    batch,
                );
                builder
                    .append(&record)
                    .expect("append batch should succeed");
            } else {
                let mut r1 = GenericRow::new(1);
                r1.set_field(0, 1_i64);
                let mut r2 = GenericRow::new(1);
                r2.set_field(0, 2_i64);
                builder
                    .append(&WriteRecord::for_append(
                        Arc::clone(&table_info),
                        physical_table_path.clone(),
                        1,
                        &r1,
                    ))
                    .expect("append row 1 should succeed");
                builder
                    .append(&WriteRecord::for_append(
                        Arc::clone(&table_info),
                        physical_table_path.clone(),
                        1,
                        &r2,
                    ))
                    .expect("append row 2 should succeed");
            }

            assert_eq!(builder.records_count(), 2);
            let bytes = builder
                .build()
                .expect("build should succeed for NOT NULL column");
            assert!(!bytes.is_empty());
        }
    }

    #[test]
    fn validate_append_record_batch_rejects_nulls_in_not_null_column() {
        let row_type = RowType::new(vec![DataField::new(
            "id",
            DataTypes::bigint().as_non_nullable(),
            None,
        )]);

        // Caller marks the Arrow field nullable (common from pyarrow) and includes a null.
        let poison = single_col_batch(
            "id",
            Arc::new(arrow::array::Int64Array::from(vec![
                Some(1_i64),
                None,
                Some(3_i64),
            ])),
        );
        let ok_batch = single_col_batch(
            "id",
            Arc::new(arrow::array::Int64Array::from(vec![1_i64, 2_i64])),
        );
        assert_rejects_null_in_not_null(&row_type, &poison, &ok_batch);
    }

    #[test]
    fn prebuilt_builder_rejects_nulls_in_not_null_column() {
        let row_type = RowType::new(vec![DataField::new(
            "id",
            DataTypes::bigint().as_non_nullable(),
            None,
        )]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        let mut builder = MemoryLogRecordsArrowBuilder::new(
            uncompressed_arrow_batch_config(1, &row_type, usize::MAX),
            true,
        )
        .expect("NOT NULL prebuilt builder should construct");

        let batch_schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "id",
            arrow_schema::DataType::Int64,
            true,
        )]));
        let poison = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(vec![Some(1_i64), None]))
                    as arrow::array::ArrayRef,
            ],
        )
        .expect("poison batch");
        let record = WriteRecord::for_append_record_batch(
            Arc::clone(&table_info),
            physical_table_path,
            1,
            poison,
        );
        assert_not_null_violation(
            builder
                .append(&record)
                .expect_err("prebuilt append must reject null in NOT NULL"),
        );
    }

    fn assert_not_null_violation(err: impl std::fmt::Display) {
        let text = err.to_string();
        assert!(
            text.contains("declared as non-nullable but contains null values"),
            "unexpected error: {text}"
        );
    }

    fn single_col_batch(name: &str, array: ArrayRef) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![Field::new(
                name,
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .expect("single-column batch")
    }

    fn assert_rejects_null_in_not_null(row_type: &RowType, poison: &RecordBatch, ok: &RecordBatch) {
        assert_not_null_violation(
            validate_append_record_batch(poison, row_type)
                .expect_err("null in NOT NULL nested column must be rejected"),
        );
        validate_append_record_batch(ok, row_type)
            .expect("null-free nested batch must be accepted");
    }

    fn list_array(values: ArrayRef, offsets: Vec<i32>, validity: Option<Vec<bool>>) -> ArrayRef {
        use arrow::array::ListArray;
        use arrow::buffer::{NullBuffer, OffsetBuffer};

        Arc::new(ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            OffsetBuffer::new(offsets.into()),
            values,
            validity.map(NullBuffer::from),
        ))
    }

    fn list_int_array(
        values: Vec<Option<i32>>,
        offsets: Vec<i32>,
        validity: Option<Vec<bool>>,
    ) -> ArrayRef {
        list_array(
            Arc::new(arrow::array::Int32Array::from(values)),
            offsets,
            validity,
        )
    }

    fn map_string_array(
        keys: Vec<&str>,
        values: ArrayRef,
        offsets: Vec<i32>,
        validity: Option<Vec<bool>>,
    ) -> ArrayRef {
        use arrow::array::{MapArray, StringArray, StructArray};
        use arrow::buffer::{NullBuffer, OffsetBuffer};

        let key_field = Arc::new(Field::new("key", ArrowDataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", values.data_type().clone(), true));
        let entries = StructArray::from(vec![
            (key_field, Arc::new(StringArray::from(keys)) as ArrayRef),
            (value_field, values),
        ]);
        Arc::new(MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(offsets.into()),
            entries,
            validity.map(NullBuffer::from),
            false,
        ))
    }

    fn map_string_int_array(
        keys: Vec<&str>,
        values: Vec<Option<i32>>,
        offsets: Vec<i32>,
        validity: Option<Vec<bool>>,
    ) -> ArrayRef {
        map_string_array(
            keys,
            Arc::new(arrow::array::Int32Array::from(values)),
            offsets,
            validity,
        )
    }

    fn struct_with_field(name: &str, child: ArrayRef, validity: Option<Vec<bool>>) -> ArrayRef {
        use arrow::array::StructArray;
        use arrow::buffer::NullBuffer;

        let fields =
            arrow_schema::Fields::from(vec![Field::new(name, child.data_type().clone(), true)]);
        Arc::new(StructArray::new(
            fields,
            vec![child],
            validity.map(NullBuffer::from),
        ))
    }

    fn struct_int_string_array(
        seq: Vec<Option<i32>>,
        label: Vec<Option<&str>>,
        validity: Option<Vec<bool>>,
    ) -> ArrayRef {
        use arrow::array::{StringArray, StructArray};
        use arrow::buffer::NullBuffer;

        let fields = arrow_schema::Fields::from(vec![
            Field::new("seq", ArrowDataType::Int32, true),
            Field::new("label", ArrowDataType::Utf8, true),
        ]);
        Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(arrow::array::Int32Array::from(seq)) as ArrayRef,
                Arc::new(StringArray::from(label)) as ArrayRef,
            ],
            validity.map(NullBuffer::from),
        ))
    }

    fn assert_full_and_sliced_not_null(row_type: &RowType, poison: &RecordBatch, ok: &RecordBatch) {
        assert_rejects_null_in_not_null(row_type, poison, ok);
        validate_append_record_batch(&poison.slice(1, 1), row_type)
            .expect("sliced-away nested null must not reject the live rows");
        assert_not_null_violation(
            validate_append_record_batch(&poison.slice(0, 1), row_type)
                .expect_err("live slice that still contains the nested null must reject"),
        );
    }

    #[test]
    fn validate_append_record_batch_rejects_null_nested_container() {
        // Only the container is null here; the element type is nullable.
        let array_type = RowType::new(vec![DataField::new(
            "tags",
            DataTypes::array(DataTypes::int()).as_non_nullable(),
            None,
        )]);
        assert_rejects_null_in_not_null(
            &array_type,
            &single_col_batch(
                "tags",
                list_int_array(
                    vec![Some(1), Some(2)],
                    vec![0, 2, 2],
                    Some(vec![true, false]),
                ),
            ),
            &single_col_batch(
                "tags",
                list_int_array(vec![Some(1), Some(2), Some(3)], vec![0, 2, 3], None),
            ),
        );
    }

    #[test]
    fn validate_append_record_batch_allows_null_elements_in_not_null_array() {
        let row_type = RowType::new(vec![DataField::new(
            "tags",
            DataTypes::array(DataTypes::int()).as_non_nullable(),
            None,
        )]);
        let batch = single_col_batch(
            "tags",
            list_int_array(vec![Some(1), None, Some(3)], vec![0, 3], None),
        );
        validate_append_record_batch(&batch, &row_type)
            .expect("null elements in a non-null ARRAY value must be accepted");
    }

    #[test]
    fn validate_append_record_batch_rejects_nulls_inside_not_null_nested_fields() {
        let array_type = RowType::new(vec![DataField::new(
            "tags",
            DataTypes::array(DataTypes::int().as_non_nullable()).as_non_nullable(),
            None,
        )]);
        assert_rejects_null_in_not_null(
            &array_type,
            &single_col_batch(
                "tags",
                list_int_array(vec![Some(1), None, Some(3)], vec![0, 3], None),
            ),
            &single_col_batch(
                "tags",
                list_int_array(vec![Some(1), Some(2), Some(3)], vec![0, 3], None),
            ),
        );

        let map_type = RowType::new(vec![DataField::new(
            "attrs",
            DataTypes::map(DataTypes::string(), DataTypes::int().as_non_nullable())
                .as_non_nullable(),
            None,
        )]);
        assert_rejects_null_in_not_null(
            &map_type,
            &single_col_batch(
                "attrs",
                map_string_int_array(vec!["a", "b"], vec![Some(1), None], vec![0, 2], None),
            ),
            &single_col_batch(
                "attrs",
                map_string_int_array(vec!["a", "b"], vec![Some(1), Some(2)], vec![0, 2], None),
            ),
        );

        let row_col_type = RowType::new(vec![DataField::new(
            "nested",
            DataTypes::row(vec![
                DataField::new("seq", DataTypes::int().as_non_nullable(), None),
                DataField::new("label", DataTypes::string(), None),
            ])
            .as_non_nullable(),
            None,
        )]);
        assert_rejects_null_in_not_null(
            &row_col_type,
            &single_col_batch(
                "nested",
                struct_int_string_array(vec![Some(1), None], vec![Some("x"), Some("y")], None),
            ),
            &single_col_batch(
                "nested",
                struct_int_string_array(vec![Some(1), Some(2)], vec![Some("x"), Some("y")], None),
            ),
        );
    }

    #[test]
    fn validate_append_record_batch_ignores_nulls_in_sliced_away_nested_values() {
        let array_type = RowType::new(vec![DataField::new(
            "tags",
            DataTypes::array(DataTypes::int().as_non_nullable()).as_non_nullable(),
            None,
        )]);
        let lists = single_col_batch(
            "tags",
            list_int_array(
                vec![Some(1), None, Some(3), Some(4), Some(5)],
                vec![0, 3, 5],
                None,
            ),
        );
        validate_append_record_batch(&lists.slice(1, 1), &array_type)
            .expect("sliced-away ARRAY element nulls must not reject the live rows");
        assert_not_null_violation(
            validate_append_record_batch(&lists.slice(0, 1), &array_type)
                .expect_err("live slice that still contains the element null must reject"),
        );

        let map_type = RowType::new(vec![DataField::new(
            "attrs",
            DataTypes::map(DataTypes::string(), DataTypes::int().as_non_nullable())
                .as_non_nullable(),
            None,
        )]);
        let maps = single_col_batch(
            "attrs",
            map_string_int_array(
                vec!["a", "b", "c"],
                vec![Some(1), None, Some(2)],
                vec![0, 2, 3],
                None,
            ),
        );
        validate_append_record_batch(&maps.slice(1, 1), &map_type)
            .expect("sliced-away MAP value nulls must not reject the live rows");
        assert_not_null_violation(
            validate_append_record_batch(&maps.slice(0, 1), &map_type)
                .expect_err("live slice that still contains the map value null must reject"),
        );

        let row_col_type = RowType::new(vec![DataField::new(
            "nested",
            DataTypes::row(vec![
                DataField::new("seq", DataTypes::int().as_non_nullable(), None),
                DataField::new("label", DataTypes::string(), None),
            ])
            .as_non_nullable(),
            None,
        )]);
        let structs = single_col_batch(
            "nested",
            struct_int_string_array(vec![None, Some(2)], vec![Some("x"), Some("y")], None),
        );
        validate_append_record_batch(&structs.slice(1, 1), &row_col_type)
            .expect("sliced-away ROW field nulls must not reject the live rows");
        assert_not_null_violation(
            validate_append_record_batch(&structs.slice(0, 1), &row_col_type)
                .expect_err("live slice that still contains the ROW field null must reject"),
        );
    }

    #[test]
    fn validate_append_record_batch_nested_nested_nullability() {
        // One case per pair of nested containers.
        let array_of_array = RowType::new(vec![DataField::new(
            "tags",
            DataTypes::array(DataTypes::array(DataTypes::int().as_non_nullable()))
                .as_non_nullable(),
            None,
        )]);
        assert_full_and_sliced_not_null(
            &array_of_array,
            &single_col_batch(
                "tags",
                list_array(
                    list_int_array(vec![Some(1), None, Some(4), Some(5)], vec![0, 2, 4], None),
                    vec![0, 1, 2],
                    None,
                ),
            ),
            &single_col_batch(
                "tags",
                list_array(
                    list_int_array(
                        vec![Some(1), Some(2), Some(4), Some(5)],
                        vec![0, 2, 4],
                        None,
                    ),
                    vec![0, 1, 2],
                    None,
                ),
            ),
        );

        let row_of_array = RowType::new(vec![DataField::new(
            "nested",
            DataTypes::row(vec![DataField::new(
                "tags",
                DataTypes::array(DataTypes::int().as_non_nullable()),
                None,
            )])
            .as_non_nullable(),
            None,
        )]);
        assert_full_and_sliced_not_null(
            &row_of_array,
            &single_col_batch(
                "nested",
                struct_with_field(
                    "tags",
                    list_int_array(vec![Some(1), None, Some(4), Some(5)], vec![0, 2, 4], None),
                    None,
                ),
            ),
            &single_col_batch(
                "nested",
                struct_with_field(
                    "tags",
                    list_int_array(
                        vec![Some(1), Some(2), Some(4), Some(5)],
                        vec![0, 2, 4],
                        None,
                    ),
                    None,
                ),
            ),
        );

        let array_of_row = RowType::new(vec![DataField::new(
            "nested",
            DataTypes::array(DataTypes::row(vec![
                DataField::new("seq", DataTypes::int().as_non_nullable(), None),
                DataField::new("label", DataTypes::string(), None),
            ]))
            .as_non_nullable(),
            None,
        )]);
        assert_full_and_sliced_not_null(
            &array_of_row,
            &single_col_batch(
                "nested",
                list_array(
                    struct_int_string_array(vec![None, Some(2)], vec![Some("x"), Some("y")], None),
                    vec![0, 1, 2],
                    None,
                ),
            ),
            &single_col_batch(
                "nested",
                list_array(
                    struct_int_string_array(
                        vec![Some(1), Some(2)],
                        vec![Some("x"), Some("y")],
                        None,
                    ),
                    vec![0, 1, 2],
                    None,
                ),
            ),
        );

        let map_of_array = RowType::new(vec![DataField::new(
            "attrs",
            DataTypes::map(
                DataTypes::string(),
                DataTypes::array(DataTypes::int().as_non_nullable()),
            )
            .as_non_nullable(),
            None,
        )]);
        assert_full_and_sliced_not_null(
            &map_of_array,
            &single_col_batch(
                "attrs",
                map_string_array(
                    vec!["a", "b"],
                    list_int_array(vec![Some(1), None, Some(4), Some(5)], vec![0, 2, 4], None),
                    vec![0, 1, 2],
                    None,
                ),
            ),
            &single_col_batch(
                "attrs",
                map_string_array(
                    vec!["a", "b"],
                    list_int_array(
                        vec![Some(1), Some(2), Some(4), Some(5)],
                        vec![0, 2, 4],
                        None,
                    ),
                    vec![0, 1, 2],
                    None,
                ),
            ),
        );
    }

    fn single_int_read_context() -> (ReadContext, SchemaRef) {
        let row_type = Arc::new(RowType::new(vec![DataField::new(
            "id",
            DataTypes::int(),
            None,
        )]));
        let schema = to_arrow_schema(&row_type).expect("arrow schema");
        (ReadContext::new(schema.clone(), row_type, false), schema)
    }

    #[test]
    #[should_panic(expected = "schema alignment length must match target schema")]
    fn target_schema_alignment_rejects_length_mismatch() {
        let (read_context, target_schema) = single_int_read_context();
        read_context.with_target_schema_alignment(target_schema, Arc::from([]));
    }

    #[test]
    #[should_panic(expected = "schema alignment contains an invalid source index")]
    fn target_schema_alignment_rejects_invalid_source_index() {
        let (read_context, target_schema) = single_int_read_context();
        read_context.with_target_schema_alignment(target_schema, Arc::from([-2]));
    }

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

    /// An `(id INT, name STRING)` builder collecting statistics for `mapping`.
    fn builder_with_statistics(
        row_type: &RowType,
        mapping: Option<Vec<usize>>,
        to_append_record_batch: bool,
    ) -> MemoryLogRecordsArrowBuilder {
        MemoryLogRecordsArrowBuilder::new(
            ArrowBatchConfig {
                stats_index_mapping: mapping,
                ..uncompressed_arrow_batch_config(1, row_type, usize::MAX)
            },
            to_append_record_batch,
        )
        .expect("builder should construct")
    }

    #[test]
    fn statistics_upgrade_the_batch_to_v1() -> Result<()> {
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        // The block the batch must carry, derived independently of the writer.
        let expected_batch = RecordBatch::try_new(
            to_arrow_schema(&row_type)?,
            vec![
                Arc::new(arrow::array::Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(arrow::array::StringArray::from(vec!["alice", "bob"])) as ArrayRef,
            ],
        )?;
        let expected_statistics = serialize_statistics(&expected_batch, &row_type, &[0, 1])?
            .expect("two rows must produce statistics");

        for to_append_record_batch in [false, true] {
            let mut builder =
                builder_with_statistics(&row_type, Some(vec![0, 1]), to_append_record_batch);
            if to_append_record_batch {
                let record = WriteRecord::for_append_record_batch(
                    Arc::clone(&table_info),
                    physical_table_path.clone(),
                    1,
                    expected_batch.clone(),
                );
                builder.append(&record)?;
            } else {
                for (id, name) in [(1, "alice"), (2, "bob")] {
                    let mut row = GenericRow::new(2);
                    row.set_field(0, id);
                    row.set_field(1, name);
                    let record = WriteRecord::for_append(
                        Arc::clone(&table_info),
                        physical_table_path.clone(),
                        1,
                        &row,
                    );
                    builder.append(&record)?;
                }
            }

            let bytes = builder.build()?;
            let batch = LogRecordBatch::new(Bytes::from(bytes.clone()));
            assert_eq!(batch.magic(), LOG_MAGIC_VALUE_V1);
            assert!(batch.is_valid(), "the CRC must cover the statistics");

            let statistics_length = LittleEndian::read_i32(
                &bytes[V1_STATISTICS_LENGTH_OFFSET..V1_STATISTICS_DATA_OFFSET],
            ) as usize;
            assert_eq!(statistics_length, expected_statistics.len());
            assert_eq!(
                &bytes[V1_STATISTICS_DATA_OFFSET..V1_STATISTICS_DATA_OFFSET + statistics_length],
                &expected_statistics[..]
            );

            let read_context = ReadContext::new(
                to_arrow_schema(&row_type)?,
                Arc::new(row_type.clone()),
                false,
            );
            let records: Vec<_> = batch.records(&read_context)?.collect();
            let mut ids = Vec::new();
            for record in &records {
                ids.push(record.row().get_int(0)?);
            }
            assert_eq!(ids, vec![1, 2]);
        }
        Ok(())
    }

    #[test]
    fn no_statistics_mapping_keeps_the_v0_format() {
        let (_, append_only) = build_append_only_batch(&[(1, "alice")]);
        let batch = LogRecordBatch::new(Bytes::from(append_only));
        assert_eq!(batch.magic(), LOG_MAGIC_VALUE_V0);
    }

    #[test]
    fn empty_statistics_mapping_writes_a_v1_batch_with_no_statistics() -> Result<()> {
        // A table can enable statistics while no column supports them.
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let table_path = TablePath::new("db".to_string(), "tbl".to_string());
        let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
        let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

        let mut builder = builder_with_statistics(&row_type, Some(Vec::new()), false);
        let mut row = GenericRow::new(2);
        row.set_field(0, 1_i32);
        row.set_field(1, "alice");
        let record = WriteRecord::for_append(table_info, physical_table_path, 1, &row);
        builder.append(&record)?;

        let bytes = builder.build()?;
        let batch = LogRecordBatch::new(Bytes::from(bytes.clone()));
        assert_eq!(batch.magic(), LOG_MAGIC_VALUE_V1);
        let statistics_length =
            LittleEndian::read_i32(&bytes[V1_STATISTICS_LENGTH_OFFSET..V1_STATISTICS_DATA_OFFSET]);
        assert_eq!(statistics_length, 0);

        let read_context = ReadContext::new(to_arrow_schema(&row_type)?, Arc::new(row_type), false);
        assert_eq!(batch.record_batch(&read_context)?.num_rows(), 1);
        Ok(())
    }

    #[test]
    fn estimated_size_reserves_room_for_the_statistics() {
        let row_type = RowType::new(vec![
            DataField::new("id".to_string(), DataTypes::int(), None),
            DataField::new("name".to_string(), DataTypes::string(), None),
        ]);
        let with_statistics = builder_with_statistics(&row_type, Some(vec![0, 1]), false);
        let without_statistics = builder_with_statistics(&row_type, None, false);
        assert_eq!(
            with_statistics.estimated_size_in_bytes()
                - without_statistics.estimated_size_in_bytes(),
            STATISTICS_LENGTH_LENGTH + estimated_serialized_size(&row_type, &[0, 1])
        );
    }

    #[test]
    fn builder_rejects_an_out_of_range_statistics_index() {
        let row_type = RowType::new(vec![DataField::new(
            "id".to_string(),
            DataTypes::int(),
            None,
        )]);
        let err = MemoryLogRecordsArrowBuilder::new(
            ArrowBatchConfig {
                stats_index_mapping: Some(vec![5]),
                ..uncompressed_arrow_batch_config(1, &row_type, usize::MAX)
            },
            false,
        )
        .err()
        .expect("an out-of-range statistics index must be rejected");
        assert!(err.to_string().contains("out of range"));
    }

    /// Encodes `batch` without going through `MemoryLogRecordsArrowBuilder`, so a
    /// batch that validation rejects can still be handed to the reader.
    fn encode_batch_bypassing_validation(batch: &RecordBatch) -> Vec<u8> {
        use arrow::ipc::writer::StreamWriter;

        let mut ipc = Vec::new();
        let mut writer =
            StreamWriter::try_new(&mut ipc, batch.schema().as_ref()).expect("ipc writer");
        let schema_message_len = writer.get_ref().len();
        writer.write(batch).expect("write batch");
        drop(writer);
        let payload = &ipc[schema_message_len..];

        let mut bytes = vec![0u8; RECORD_BATCH_HEADER_SIZE + payload.len()];
        write_batch_header_fields(
            &mut bytes,
            BatchHeaderFields {
                base_log_offset: BUILDER_DEFAULT_OFFSET,
                magic: CURRENT_LOG_MAGIC_VALUE,
                schema_id: 1,
                writer_id: NO_WRITER_ID,
                batch_sequence: NO_BATCH_SEQUENCE,
                record_count: batch.num_rows() as i32,
                statistics_length: 0,
            },
        )
        .expect("write header");
        bytes[RECORD_BATCH_HEADER_SIZE..].copy_from_slice(payload);

        let crc = crc32c(&bytes[SCHEMA_ID_OFFSET..]);
        bytes[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    /// A batch validation accepts must decode; one that fails to decode must be
    /// rejected. Pairing the two verdicts keeps the masking rules out of the test.
    fn assert_validation_matches_reader(label: &str, batch: &RecordBatch, row_type: &RowType) {
        let accepted = validate_append_record_batch(batch, row_type).is_ok();
        let bytes = encode_batch_bypassing_validation(batch);
        let read_context = ReadContext::new(
            to_arrow_schema(row_type).expect("arrow schema"),
            Arc::new(row_type.clone()),
            false,
        );
        let decodes = LogRecordsBatches::new(bytes)
            .next()
            .expect("one batch")
            .expect("batch header")
            .records(&read_context)
            .map(|records| records.count())
            .is_ok();

        assert_eq!(
            accepted,
            decodes,
            "{label}: validation {} but the reader {}",
            if accepted { "accepted" } else { "rejected" },
            if decodes {
                "decoded it"
            } else {
                "could not decode it"
            }
        );
    }

    #[test]
    fn validation_agrees_with_reader_on_nested_nullability() {
        let array_of_not_null = RowType::new(vec![DataField::new(
            "tags",
            DataTypes::array(DataTypes::int().as_non_nullable()),
            None,
        )]);
        let row_of_not_null_array = RowType::new(vec![DataField::new(
            "nested",
            DataTypes::row(vec![DataField::new(
                "tags",
                DataTypes::array(DataTypes::int().as_non_nullable()),
                None,
            )]),
            None,
        )]);
        let row_of_not_null_field = RowType::new(vec![DataField::new(
            "nested",
            DataTypes::row(vec![DataField::new(
                "seq",
                DataTypes::int().as_non_nullable(),
                None,
            )]),
            None,
        )]);

        assert_validation_matches_reader(
            "poison on a live list row",
            &single_col_batch(
                "tags",
                list_int_array(vec![Some(1), None], vec![0, 2], None),
            ),
            &array_of_not_null,
        );
        assert_validation_matches_reader(
            "leftover under a null list row",
            &single_col_batch(
                "tags",
                list_int_array(vec![Some(1), None], vec![0, 1, 2], Some(vec![true, false])),
            ),
            &array_of_not_null,
        );
        assert_validation_matches_reader(
            "null list row owning no elements",
            &single_col_batch(
                "tags",
                list_int_array(vec![Some(1)], vec![0, 1, 1], Some(vec![true, false])),
            ),
            &array_of_not_null,
        );
        assert_validation_matches_reader(
            "null ROW row masking its own field",
            &single_col_batch(
                "nested",
                struct_with_field(
                    "seq",
                    Arc::new(arrow::array::Int32Array::from(vec![None, Some(2)])) as ArrayRef,
                    Some(vec![false, true]),
                ),
            ),
            &row_of_not_null_field,
        );
        assert_validation_matches_reader(
            "null ROW row above a live list",
            &single_col_batch(
                "nested",
                struct_with_field(
                    "tags",
                    list_int_array(vec![None, Some(7)], vec![0, 1, 2], None),
                    Some(vec![false, true]),
                ),
            ),
            &row_of_not_null_array,
        );
        assert_validation_matches_reader(
            "poison removed by a slice",
            &single_col_batch(
                "tags",
                list_int_array(vec![None, Some(7), Some(8)], vec![0, 1, 3], None),
            )
            .slice(1, 1),
            &array_of_not_null,
        );
    }

    #[test]
    fn validation_agrees_with_reader_on_maps_and_deeper_nesting() {
        let map_of_not_null_value = RowType::new(vec![DataField::new(
            "attrs",
            DataTypes::map(DataTypes::string(), DataTypes::int().as_non_nullable()),
            None,
        )]);
        assert_validation_matches_reader(
            "MAP: poison on a live entry",
            &single_col_batch(
                "attrs",
                map_string_int_array(vec!["a", "b"], vec![Some(1), None], vec![0, 2], None),
            ),
            &map_of_not_null_value,
        );
        assert_validation_matches_reader(
            "MAP: leftover under a null entry",
            &single_col_batch(
                "attrs",
                map_string_int_array(
                    vec!["a", "b"],
                    vec![Some(1), None],
                    vec![0, 1, 2],
                    Some(vec![true, false]),
                ),
            ),
            &map_of_not_null_value,
        );
        assert_validation_matches_reader(
            "MAP: null entry owning nothing",
            &single_col_batch(
                "attrs",
                map_string_int_array(
                    vec!["a"],
                    vec![Some(1)],
                    vec![0, 1, 1],
                    Some(vec![true, false]),
                ),
            ),
            &map_of_not_null_value,
        );

        let array_of_array = RowType::new(vec![DataField::new(
            "grid",
            DataTypes::array(DataTypes::array(DataTypes::int().as_non_nullable())),
            None,
        )]);
        let inner_with_poison = || list_int_array(vec![None, Some(4)], vec![0, 1, 2], None);
        assert_validation_matches_reader(
            "ARRAY<ARRAY>: poison on a live outer row",
            &single_col_batch("grid", list_array(inner_with_poison(), vec![0, 2], None)),
            &array_of_array,
        );
        assert_validation_matches_reader(
            "ARRAY<ARRAY>: poison sliced away",
            &single_col_batch("grid", list_array(inner_with_poison(), vec![0, 1, 2], None))
                .slice(1, 1),
            &array_of_array,
        );

        let array_of_row = RowType::new(vec![DataField::new(
            "items",
            DataTypes::array(DataTypes::row(vec![
                DataField::new("seq", DataTypes::int().as_non_nullable(), None),
                DataField::new("label", DataTypes::string(), None),
            ])),
            None,
        )]);
        assert_validation_matches_reader(
            "ARRAY<ROW>: poison on a live list row",
            &single_col_batch(
                "items",
                list_array(
                    struct_int_string_array(vec![None, Some(2)], vec![Some("x"), Some("y")], None),
                    vec![0, 2],
                    None,
                ),
            ),
            &array_of_row,
        );
        assert_validation_matches_reader(
            "ARRAY<ROW>: leftover under a null list row",
            &single_col_batch(
                "items",
                list_array(
                    struct_int_string_array(vec![None, Some(2)], vec![Some("x"), Some("y")], None),
                    vec![0, 1, 2],
                    Some(vec![false, true]),
                ),
            ),
            &array_of_row,
        );

        let row_of_row = RowType::new(vec![DataField::new(
            "outer",
            DataTypes::row(vec![DataField::new(
                "inner",
                DataTypes::row(vec![DataField::new(
                    "seq",
                    DataTypes::int().as_non_nullable(),
                    None,
                )]),
                None,
            )]),
            None,
        )]);
        // A null ROW row above a live MAP.
        assert_validation_matches_reader(
            "ROW<MAP>: null ROW row above a live map",
            &single_col_batch(
                "nested",
                struct_with_field(
                    "attrs",
                    map_string_int_array(vec!["a", "b"], vec![Some(1), None], vec![0, 2], None),
                    Some(vec![false]),
                ),
            ),
            &RowType::new(vec![DataField::new(
                "nested",
                DataTypes::row(vec![DataField::new(
                    "attrs",
                    DataTypes::map(DataTypes::string(), DataTypes::int().as_non_nullable()),
                    None,
                )]),
                None,
            )]),
        );

        assert_validation_matches_reader(
            "ROW<ROW>: null outer over a live inner",
            &single_col_batch(
                "outer",
                struct_with_field(
                    "inner",
                    struct_with_field(
                        "seq",
                        Arc::new(arrow::array::Int32Array::from(vec![None, Some(2)])) as ArrayRef,
                        None,
                    ),
                    Some(vec![false, true]),
                ),
            ),
            &row_of_row,
        );
    }

    #[test]
    fn validation_agrees_with_reader_across_element_types() {
        use arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};

        let cases: Vec<(&str, DataType, ArrayRef)> = vec![
            (
                "STRING",
                DataTypes::string().as_non_nullable(),
                Arc::new(StringArray::from(vec![Some("a"), None])),
            ),
            (
                "DOUBLE",
                DataTypes::double().as_non_nullable(),
                Arc::new(Float64Array::from(vec![Some(1.5), None])),
            ),
            (
                "TIMESTAMP",
                DataTypes::timestamp_with_precision(3).as_non_nullable(),
                Arc::new(TimestampMillisecondArray::from(vec![
                    Some(1_700_000_000_000),
                    None,
                ])),
            ),
            (
                "BIGINT",
                DataTypes::bigint().as_non_nullable(),
                Arc::new(arrow::array::Int64Array::from(vec![Some(7_i64), None])),
            ),
        ];

        for (name, element_type, values) in cases {
            let row_type = RowType::new(vec![DataField::new(
                "tags",
                DataTypes::array(element_type),
                None,
            )]);
            assert_validation_matches_reader(
                &format!("ARRAY<{name} NOT NULL>: poison on a live row"),
                &single_col_batch("tags", list_array(Arc::clone(&values), vec![0, 2], None)),
                &row_type,
            );
            assert_validation_matches_reader(
                &format!("ARRAY<{name} NOT NULL>: leftover under a null row"),
                &single_col_batch(
                    "tags",
                    list_array(values, vec![0, 1, 2], Some(vec![true, false])),
                ),
                &row_type,
            );
        }
    }
}
