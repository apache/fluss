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

//! Fluss' log record batch wire format, mirroring Java's
//! `LogRecordBatchFormat` / `DefaultLogRecordBatch`.
//!
//! A batch on the wire is `[header][statistics (V1 only)][change types][records
//! data]`, where the records data is an Arrow IPC payload that is encoded and
//! decoded in [`super::arrow`]. The versions here (`LOG_MAGIC_VALUE_*`) are
//! versions of this Fluss framing and are unrelated to the Arrow IPC format
//! version.

use crate::error::{Error, Result};
use crate::record::arrow::{ArrowLogRecordIterator, ArrowReader, ReadContext};
use crate::record::{ChangeType, ScanRecord};
use arrow::array::RecordBatch;
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use crc32c::crc32c;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;

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
pub const LOG_OVERHEAD: usize = LENGTH_OFFSET + LENGTH_LENGTH;

pub const STATISTICS_LENGTH_LENGTH: usize = 4;
/// V1 keeps the whole V0 header layout and appends a statistics length field,
/// so every offset up to the records count is shared between the two versions.
pub const V1_STATISTICS_LENGTH_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;
pub const V1_STATISTICS_DATA_OFFSET: usize = V1_STATISTICS_LENGTH_OFFSET + STATISTICS_LENGTH_LENGTH;
pub const V1_RECORD_BATCH_HEADER_SIZE: usize = V1_STATISTICS_DATA_OFFSET;

pub const LEADER_EPOCH_LENGTH: usize = 4;
/// Leader epoch reported for batches whose magic predates V2, mirroring Java's
/// `LogRecordBatchFormat.NO_LEADER_EPOCH`.
pub const NO_LEADER_EPOCH: i32 = -1;

/// V2 inserts a leader epoch between the commit timestamp and the CRC, so every
/// field from the CRC onward sits [`LEADER_EPOCH_LENGTH`] bytes after its V1
/// position; the statistics section and records data layout match V1.
pub const V2_LEADER_EPOCH_OFFSET: usize = COMMIT_TIMESTAMP_OFFSET + COMMIT_TIMESTAMP_LENGTH;
pub const V2_STATISTICS_LENGTH_OFFSET: usize = V1_STATISTICS_LENGTH_OFFSET + LEADER_EPOCH_LENGTH;
pub const V2_STATISTICS_DATA_OFFSET: usize = V2_STATISTICS_LENGTH_OFFSET + STATISTICS_LENGTH_LENGTH;
pub const V2_RECORD_BATCH_HEADER_SIZE: usize = V2_STATISTICS_DATA_OFFSET;

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
    /// V1 places a statistics section between the fixed header and the records
    /// so the server can prune whole batches against a pushed-down filter.
    V1 = 1,
    /// V2 adds a leader epoch field to the header.
    V2 = 2,
}

pub const LOG_MAGIC_VALUE_V0: u8 = LogMagicValue::V0 as u8;
pub const LOG_MAGIC_VALUE_V1: u8 = LogMagicValue::V1 as u8;
pub const LOG_MAGIC_VALUE_V2: u8 = LogMagicValue::V2 as u8;

/// Fixed header size for the given magic, mirroring Java's
/// `LogRecordBatchFormat.recordBatchHeaderSize`.
pub fn record_batch_header_size(magic: u8) -> Result<usize> {
    match magic {
        LOG_MAGIC_VALUE_V0 => Ok(RECORD_BATCH_HEADER_SIZE),
        LOG_MAGIC_VALUE_V1 => Ok(V1_RECORD_BATCH_HEADER_SIZE),
        LOG_MAGIC_VALUE_V2 => Ok(V2_RECORD_BATCH_HEADER_SIZE),
        _ => Err(Error::UnexpectedError {
            message: format!("Unsupported magic value {magic}"),
            source: None,
        }),
    }
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

#[allow(
    dead_code,
    reason = "mirrors Java's LogRecordBatchFormat default magic"
)]
pub const CURRENT_LOG_MAGIC_VALUE: u8 = LOG_MAGIC_VALUE_V0;

/// Value used if writer ID is not available or non-idempotent.
pub const NO_WRITER_ID: i64 = -1;

/// Value used if batch sequence is not available.
pub const NO_BATCH_SEQUENCE: i32 = -1;

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

    /// Byte shift of every header field at or after the CRC, relative to the
    /// V0/V1 layout: V2 inserts the leader epoch before the CRC. Decode entry
    /// points gate on [`record_batch_header_size`] first, so unknown magics
    /// never reach the accessors that use this.
    fn header_field_shift(&self) -> usize {
        if self.magic() >= LOG_MAGIC_VALUE_V2 {
            LEADER_EPOCH_LENGTH
        } else {
            0
        }
    }

    pub fn commit_timestamp(&self) -> i64 {
        let offset = COMMIT_TIMESTAMP_OFFSET;
        LittleEndian::read_i64(&self.data[offset..offset + COMMIT_TIMESTAMP_LENGTH])
    }

    /// The leader epoch field of a V2 batch, or [`NO_LEADER_EPOCH`] for
    /// magics before V2.
    pub fn leader_epoch(&self) -> i32 {
        if self.magic() < LOG_MAGIC_VALUE_V2 {
            return NO_LEADER_EPOCH;
        }
        let offset = V2_LEADER_EPOCH_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + LEADER_EPOCH_LENGTH])
    }

    pub fn writer_id(&self) -> i64 {
        let offset = WRITE_CLIENT_ID_OFFSET + self.header_field_shift();
        LittleEndian::read_i64(&self.data[offset..offset + WRITE_CLIENT_ID_LENGTH])
    }

    pub fn batch_sequence(&self) -> i32 {
        let offset = BATCH_SEQUENCE_OFFSET + self.header_field_shift();
        LittleEndian::read_i32(&self.data[offset..offset + BATCH_SEQUENCE_LENGTH])
    }

    pub fn ensure_valid(&self) -> Result<()> {
        // TODO enable validation once checksum handling is corrected.
        Ok(())
    }

    pub fn is_valid(&self) -> bool {
        match self.ensure_header_complete() {
            Ok(header_size) => {
                self.size_in_bytes() >= header_size && self.checksum() == self.compute_checksum()
            }
            Err(_) => false,
        }
    }

    /// Rejects unsupported magic versions and batches shorter than their
    /// magic's fixed header, so the fixed-offset accessors cannot slice past
    /// the buffer. Mirrors the guard in Java's `DefaultLogRecordBatch`.
    fn ensure_header_complete(&self) -> Result<usize> {
        if self.data.len() <= MAGIC_OFFSET {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Corrupt log record batch: data length {} does not reach the magic byte",
                    self.data.len()
                ),
                source: None,
            });
        }
        let magic = self.magic();
        let header_size = record_batch_header_size(magic)?;
        if self.data.len() < header_size {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Corrupt log record batch: data length {} is less than the V{magic} header size {header_size}",
                    self.data.len()
                ),
                source: None,
            });
        }
        Ok(header_size)
    }

    fn compute_checksum(&self) -> u32 {
        let start = SCHEMA_ID_OFFSET + self.header_field_shift();
        crc32c(&self.data[start..])
    }

    fn attributes(&self) -> u8 {
        self.data[ATTRIBUTES_OFFSET + self.header_field_shift()]
    }

    /// Whether this batch is append-only (see [`APPEND_ONLY_FLAG_MASK`]).
    fn is_append_only(&self) -> bool {
        self.attributes() & APPEND_ONLY_FLAG_MASK != 0
    }

    pub fn next_log_offset(&self) -> i64 {
        self.last_log_offset() + 1
    }

    pub fn checksum(&self) -> u32 {
        let offset = CRC_OFFSET + self.header_field_shift();
        LittleEndian::read_u32(&self.data[offset..offset + CRC_LENGTH])
    }

    pub fn schema_id(&self) -> i16 {
        let offset = SCHEMA_ID_OFFSET + self.header_field_shift();
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
        let offset = LAST_OFFSET_DELTA_OFFSET + self.header_field_shift();
        LittleEndian::read_i32(&self.data[offset..offset + LAST_OFFSET_DELTA_LENGTH])
    }

    pub fn size_in_bytes(&self) -> usize {
        let offset = LENGTH_OFFSET;
        LittleEndian::read_i32(&self.data[offset..offset + LENGTH_LENGTH]) as usize + LOG_OVERHEAD
    }

    pub fn record_count(&self) -> i32 {
        let offset = RECORDS_COUNT_OFFSET + self.header_field_shift();
        LittleEndian::read_i32(&self.data[offset..offset + RECORDS_COUNT_LENGTH])
    }

    /// Offset where the records data starts, mirroring Java's
    /// `DefaultLogRecordBatch.recordsDataOffset`.
    fn records_data_offset(&self) -> Result<usize> {
        let magic = self.magic();
        let header_size = record_batch_header_size(magic)?;
        if magic < LOG_MAGIC_VALUE_V1 {
            return Ok(header_size);
        }
        let offset = V1_STATISTICS_LENGTH_OFFSET + self.header_field_shift();
        let statistics_length = self
            .data
            .get(offset..offset + STATISTICS_LENGTH_LENGTH)
            .map(LittleEndian::read_i32)
            .ok_or_else(|| Error::UnexpectedError {
                message: format!(
                    "Corrupt log record batch: data length {} is less than the V{magic} header size {header_size}",
                    self.data.len(),
                ),
                source: None,
            })?;
        if statistics_length < 0 {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Corrupt log record batch: negative statistics length {statistics_length}"
                ),
                source: None,
            });
        }
        Ok(header_size + statistics_length as usize)
    }

    /// Splits the batch body into its per-record change types and the trailing
    /// Arrow IPC payload (see [`APPEND_ONLY_FLAG_MASK`] for the layout).
    fn decode_change_types(&self) -> Result<(BatchChangeTypes, Bytes)> {
        let records_offset = self.records_data_offset()?;
        let body = self
            .data
            .get(records_offset..)
            .ok_or_else(|| Error::UnexpectedError {
                message: format!(
                    "Corrupt log record batch: data length {} is less than the records offset {records_offset}",
                    self.data.len(),
                ),
                source: None,
            })?;

        if self.is_append_only() {
            return Ok((
                BatchChangeTypes::Uniform(ChangeType::AppendOnly),
                self.data.slice(records_offset..),
            ));
        }

        let record_count = self.record_count();
        if record_count < 0 {
            return Err(Error::UnexpectedError {
                message: format!("Corrupt changelog batch: negative record count {record_count}"),
                source: None,
            });
        }
        let record_count = record_count as usize;
        let (change_type_bytes, _) =
            body.split_at_checked(record_count)
                .ok_or_else(|| Error::UnexpectedError {
                    message: format!(
                        "Corrupt changelog batch: body length {} is smaller than its \
                         {record_count}-record change-type vector",
                        body.len()
                    ),
                    source: None,
                })?;
        let arrow_data = self.data.slice(records_offset + record_count..);

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
        // Gate on the magic and length first: header offsets of an unsupported
        // version or a truncated batch must not be interpreted with this layout.
        self.ensure_header_complete()?;
        if self.record_count() == 0 {
            return Ok(LogRecordIterator::empty());
        }

        let (change_types, arrow_data) = self.decode_change_types()?;
        let record_batch = read_context.record_batch(arrow_data)?;
        let arrow_reader = ArrowReader::new_with_fluss_row_type(
            Arc::new(record_batch),
            read_context.row_type_arc(),
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
        self.ensure_header_complete()?;
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
                    read_context.row_type_arc(),
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
        self.ensure_header_complete()?;
        if self.record_count() == 0 {
            // Return empty batch with correct schema
            return Ok(RecordBatch::new_empty(read_context.target_schema()));
        }

        // Batch access drops the change-type vector; use `records()` for CDC.
        let (_, arrow_data) = self.decode_change_types()?;
        read_context.record_batch(arrow_data)
    }
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
pub(crate) enum BatchChangeTypes {
    /// Every record shares this change type (append-only batches).
    Uniform(ChangeType),
    /// One change type per record, indexed by row id (changelog batches).
    PerRecord(Vec<ChangeType>),
}

impl BatchChangeTypes {
    pub(crate) fn get(&self, row_id: usize) -> ChangeType {
        match self {
            BatchChangeTypes::Uniform(change_type) => *change_type,
            BatchChangeTypes::PerRecord(change_types) => change_types[row_id],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::metadata::{DataField, DataTypes, RowType};
    use crate::record::{MemoryLogRecordsArrowBuilder, to_arrow_schema};
    use crate::row::DataGetters;
    use std::io::Write;

    use crate::test_utils::{
        build_append_only_batch, build_table_info, splice_change_type_vector,
        splice_statistics_section, uncompressed_arrow_batch_config,
    };
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
    fn test_log_records_batches_from_file() -> Result<()> {
        use crate::client::WriteRecord;
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
            uncompressed_arrow_batch_config(1, &row_type, usize::MAX),
            false,
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

    #[test]
    fn decode_changelog_record_batch_applies_per_record_change_types() -> Result<()> {
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
    fn decode_changelog_record_batch_rejects_invalid_change_type_byte() {
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
    fn decode_changelog_record_batch_rejects_truncated_change_type_vector() {
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

    #[test]
    fn header_size_follows_the_magic_version() {
        assert_eq!(record_batch_header_size(LOG_MAGIC_VALUE_V0).unwrap(), 48);
        assert_eq!(record_batch_header_size(LOG_MAGIC_VALUE_V1).unwrap(), 52);
        assert_eq!(record_batch_header_size(LOG_MAGIC_VALUE_V2).unwrap(), 56);
        let err = record_batch_header_size(3).expect_err("V3 is not supported");
        assert!(err.to_string().contains("Unsupported magic value 3"));
    }

    #[test]
    fn decode_v1_batch_skips_the_statistics_section() -> Result<()> {
        let (row_type, append_only) = build_append_only_batch(&[(1, "alice"), (2, "bob")]);
        let read_context = ReadContext::new(to_arrow_schema(&row_type)?, Arc::new(row_type), false);

        // The reader must step over the statistics blindly, whatever they hold.
        let statistics = vec![0xAB_u8; 37];
        let v1 = splice_statistics_section(&append_only, &statistics);
        let batch = LogRecordsBatches::new(v1).next().expect("V1 batch")?;
        assert_eq!(batch.magic(), LOG_MAGIC_VALUE_V1);
        assert!(
            batch.is_valid(),
            "the CRC must cover the spliced statistics"
        );

        let records: Vec<_> = batch.records(&read_context)?.collect();
        let mut ids = Vec::new();
        for record in &records {
            ids.push(record.row().get_int(0)?);
        }
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(batch.record_batch(&read_context)?.num_rows(), 2);

        // The server's projection path emits V1 with an empty statistics section.
        let v1_empty = splice_statistics_section(&append_only, &[]);
        let batch = LogRecordsBatches::new(v1_empty).next().expect("V1 batch")?;
        assert_eq!(batch.magic(), LOG_MAGIC_VALUE_V1);
        assert_eq!(batch.record_batch(&read_context)?.num_rows(), 2);
        Ok(())
    }

    #[test]
    fn decode_v1_changelog_record_batch_reads_change_types_after_statistics() -> Result<()> {
        let (row_type, append_only) = build_append_only_batch(&[(1, "a"), (2, "b")]);
        let read_context = ReadContext::new(to_arrow_schema(&row_type)?, Arc::new(row_type), false);

        // Change types first (a V0 changelog), then the statistics in front of
        // them, giving the V1 layout [header][statistics][changeTypes][arrow].
        let change_types = [ChangeType::Insert, ChangeType::Delete];
        let changelog = splice_change_type_vector(&append_only, &change_types);
        let v1 = splice_statistics_section(&changelog, &[0xCD_u8; 21]);

        let batch = LogRecordsBatches::new(v1).next().expect("V1 changelog")?;
        assert!(!batch.is_append_only());
        let records: Vec<_> = batch.records(&read_context)?.collect();
        let got: Vec<ChangeType> = records.iter().map(|r| *r.change_type()).collect();
        assert_eq!(got, change_types.to_vec());
        Ok(())
    }

    #[test]
    fn decode_rejects_an_unsupported_magic_version() {
        let (row_type, append_only) = build_append_only_batch(&[(1, "a")]);
        let read_context = ReadContext::new(
            to_arrow_schema(&row_type).unwrap(),
            Arc::new(row_type),
            false,
        );

        let mut data = append_only;
        data[MAGIC_OFFSET] = 3;
        // A future magic shifting the header would leave a stale value at this
        // offset, so a zero here made the batch read as empty and get silently
        // dropped before the magic gate.
        data[RECORDS_COUNT_OFFSET..RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH]
            .copy_from_slice(&0_i32.to_le_bytes());
        let batch = LogRecordBatch::new(Bytes::from(data));
        let err = batch
            .records(&read_context)
            .err()
            .expect("V3 batches must be rejected, not misparsed");
        assert!(err.to_string().contains("Unsupported magic value 3"));
        let err = batch
            .record_batch(&read_context)
            .expect_err("batch mode must reject V3 too");
        assert!(err.to_string().contains("Unsupported magic value 3"));
    }

    #[test]
    fn decode_rejects_a_batch_shorter_than_its_header() {
        let (row_type, append_only) = build_append_only_batch(&[(1, "a")]);
        let read_context = ReadContext::new(
            to_arrow_schema(&row_type).unwrap(),
            Arc::new(row_type),
            false,
        );

        // A corrupt length field can hand the iterator a batch shorter than
        // the fixed header; decoding must error instead of panicking.
        let mut truncated = append_only[..RECORD_BATCH_HEADER_SIZE - 8].to_vec();
        let declared = (truncated.len() - LOG_OVERHEAD) as i32;
        truncated[LENGTH_OFFSET..LENGTH_OFFSET + LENGTH_LENGTH]
            .copy_from_slice(&declared.to_le_bytes());
        let batch = LogRecordsBatches::new(truncated)
            .next()
            .expect("the iterator must yield the truncated batch")
            .expect("reading the truncated batch bytes must succeed");
        let err = batch
            .records(&read_context)
            .err()
            .expect("a batch shorter than its header must be rejected");
        assert!(err.to_string().contains("less than the V0 header size"));
        assert!(!batch.is_valid());

        // A V1 batch cut between the V0 and V1 header sizes.
        let v1 = splice_statistics_section(&append_only, &[]);
        let batch = LogRecordBatch::new(Bytes::from(v1[..RECORD_BATCH_HEADER_SIZE + 2].to_vec()));
        let err = batch
            .records(&read_context)
            .err()
            .expect("a truncated V1 batch must be rejected");
        assert!(err.to_string().contains("less than the V1 header size"));
        assert!(!batch.is_valid());

        // A buffer that does not even reach the magic byte.
        let batch = LogRecordBatch::new(Bytes::from(vec![0_u8; MAGIC_OFFSET]));
        assert!(!batch.is_valid());
        let err = batch
            .records(&read_context)
            .err()
            .expect("a batch without a magic byte must be rejected");
        assert!(err.to_string().contains("does not reach the magic byte"));
    }

    #[test]
    fn is_valid_is_false_for_an_unsupported_magic() {
        let (_, append_only) = build_append_only_batch(&[(1, "a")]);
        let mut data = append_only;
        data[MAGIC_OFFSET] = 3;
        assert!(!LogRecordBatch::new(Bytes::from(data)).is_valid());
    }

    /// Turns a V1 batch into a wire-valid V2 batch by inserting the leader
    /// epoch before the CRC and fixing up the magic, length field and CRC.
    fn splice_leader_epoch(v1_batch: &[u8], leader_epoch: i32) -> Vec<u8> {
        let mut data = v1_batch.to_vec();
        data[MAGIC_OFFSET] = LOG_MAGIC_VALUE_V2;
        data.splice(
            V2_LEADER_EPOCH_OFFSET..V2_LEADER_EPOCH_OFFSET,
            leader_epoch.to_le_bytes(),
        );

        let new_length = (data.len() - LOG_OVERHEAD) as i32;
        data[LENGTH_OFFSET..LENGTH_OFFSET + LENGTH_LENGTH]
            .copy_from_slice(&new_length.to_le_bytes());
        let crc_offset = CRC_OFFSET + LEADER_EPOCH_LENGTH;
        let crc = crc32c(&data[SCHEMA_ID_OFFSET + LEADER_EPOCH_LENGTH..]);
        data[crc_offset..crc_offset + CRC_LENGTH].copy_from_slice(&crc.to_le_bytes());
        data
    }

    #[test]
    fn decode_v2_batch_reads_records_after_the_leader_epoch() -> Result<()> {
        let (row_type, append_only) = build_append_only_batch(&[(1, "alice"), (2, "bob")]);
        let read_context = ReadContext::new(to_arrow_schema(&row_type)?, Arc::new(row_type), false);

        let v1 = splice_statistics_section(&append_only, &[0xAB_u8; 19]);
        let v2 = splice_leader_epoch(&v1, 7);
        let batch = LogRecordsBatches::new(v2).next().expect("V2 batch")?;
        assert_eq!(batch.magic(), LOG_MAGIC_VALUE_V2);
        assert_eq!(batch.leader_epoch(), 7);
        assert!(
            batch.is_valid(),
            "the CRC must be read from its shifted V2 offset"
        );
        // Every post-CRC header field must come from its shifted offset.
        assert_eq!(batch.record_count(), 2);
        assert_eq!(batch.schema_id(), 1);
        assert_eq!(batch.writer_id(), NO_WRITER_ID);
        assert_eq!(batch.batch_sequence(), NO_BATCH_SEQUENCE);
        assert_eq!(batch.last_log_offset(), 1);

        let records: Vec<_> = batch.records(&read_context)?.collect();
        let mut ids = Vec::new();
        for record in &records {
            ids.push(record.row().get_int(0)?);
        }
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(batch.record_batch(&read_context)?.num_rows(), 2);

        // The server can also emit V2 with an empty statistics section.
        let v2_empty = splice_leader_epoch(&splice_statistics_section(&append_only, &[]), 7);
        let batch = LogRecordsBatches::new(v2_empty).next().expect("V2 batch")?;
        assert_eq!(batch.record_batch(&read_context)?.num_rows(), 2);
        Ok(())
    }

    #[test]
    fn decode_v2_changelog_batch_reads_change_types_after_statistics() -> Result<()> {
        let (row_type, append_only) = build_append_only_batch(&[(1, "a"), (2, "b")]);
        let read_context = ReadContext::new(to_arrow_schema(&row_type)?, Arc::new(row_type), false);

        let change_types = [ChangeType::Insert, ChangeType::Delete];
        let changelog = splice_change_type_vector(&append_only, &change_types);
        let v1 = splice_statistics_section(&changelog, &[0xCD_u8; 11]);
        let v2 = splice_leader_epoch(&v1, 3);

        let batch = LogRecordsBatches::new(v2).next().expect("V2 changelog")?;
        assert!(!batch.is_append_only());
        let records: Vec<_> = batch.records(&read_context)?.collect();
        let got: Vec<ChangeType> = records.iter().map(|r| *r.change_type()).collect();
        assert_eq!(got, change_types.to_vec());
        Ok(())
    }

    #[test]
    fn pre_v2_batches_report_no_leader_epoch() {
        let (_, append_only) = build_append_only_batch(&[(1, "a")]);
        let batch = LogRecordBatch::new(Bytes::from(append_only.clone()));
        assert_eq!(batch.leader_epoch(), NO_LEADER_EPOCH);

        let v1 = splice_statistics_section(&append_only, &[]);
        let batch = LogRecordBatch::new(Bytes::from(v1));
        assert_eq!(batch.leader_epoch(), NO_LEADER_EPOCH);
    }

    #[test]
    fn decode_rejects_a_negative_statistics_length() {
        let (row_type, append_only) = build_append_only_batch(&[(1, "a")]);
        let read_context = ReadContext::new(
            to_arrow_schema(&row_type).unwrap(),
            Arc::new(row_type),
            false,
        );

        let mut v1 = splice_statistics_section(&append_only, &[]);
        v1[V1_STATISTICS_LENGTH_OFFSET..V1_STATISTICS_LENGTH_OFFSET + STATISTICS_LENGTH_LENGTH]
            .copy_from_slice(&(-1_i32).to_le_bytes());
        let batch = LogRecordBatch::new(Bytes::from(v1));
        let err = batch
            .records(&read_context)
            .err()
            .expect("a negative statistics length must be rejected");
        assert!(err.to_string().contains("negative statistics length"));
    }

    #[test]
    fn decode_rejects_a_statistics_length_past_the_batch_end() {
        let (row_type, append_only) = build_append_only_batch(&[(1, "a")]);
        let read_context = ReadContext::new(
            to_arrow_schema(&row_type).unwrap(),
            Arc::new(row_type),
            false,
        );

        let mut v1 = splice_statistics_section(&append_only, &[]);
        let past_the_end = v1.len() as i32;
        v1[V1_STATISTICS_LENGTH_OFFSET..V1_STATISTICS_LENGTH_OFFSET + STATISTICS_LENGTH_LENGTH]
            .copy_from_slice(&past_the_end.to_le_bytes());
        let batch = LogRecordBatch::new(Bytes::from(v1));
        let err = batch
            .records(&read_context)
            .err()
            .expect("a statistics length past the batch end must be rejected");
        assert!(err.to_string().contains("records offset"));
    }
    /// Changelog batches put a record_count-byte change-type vector before the
    /// Arrow payload, so the payload's offset - and its alignment - shifts with
    /// the record count. Decode every count from 1..=16 to cover each residue.
    #[test]
    fn decodes_changelog_at_every_payload_alignment() -> Result<()> {
        for n in 1usize..=16 {
            let rows: Vec<(i32, String)> = (0..n).map(|i| (i as i32, format!("v{i}"))).collect();
            let row_refs: Vec<(i32, &str)> = rows.iter().map(|(i, s)| (*i, s.as_str())).collect();
            let (row_type, append_only) = build_append_only_batch(&row_refs);
            let change_types = vec![ChangeType::Insert; n];
            let changelog = splice_change_type_vector(&append_only, &change_types);

            let batch = LogRecordsBatches::new(changelog)
                .next()
                .expect("changelog batch")?;
            let read_context =
                ReadContext::new(to_arrow_schema(&row_type)?, Arc::new(row_type), false);

            let decoded = batch.record_batch(&read_context)?;
            assert_eq!(decoded.num_rows(), n, "row count for {n} records");

            let ids: Vec<i32> = batch
                .records(&read_context)?
                .map(|r| r.row().get_int(0).expect("id"))
                .collect();
            let expected: Vec<i32> = (0..n as i32).collect();
            assert_eq!(ids, expected, "values for {n} records");
        }
        Ok(())
    }
}
