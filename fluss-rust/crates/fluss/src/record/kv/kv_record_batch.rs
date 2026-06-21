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

//! KV record batch implementation.
//!
//! The schema of a KvRecordBatch is:
//! - Length => Int32
//! - Magic => Int8
//! - CRC => Uint32
//! - SchemaId => Int16
//! - Attributes => Int8
//! - WriterId => Int64
//! - BatchSequence => Int32
//! - RecordCount => Int32
//! - Records => [Record]
//!
//! The CRC covers data from the SchemaId to the end of the batch.

use bytes::Bytes;
use std::io;
use std::sync::Arc;

use crate::error::Result;
use crate::record::kv::{KvRecord, ReadContext};
use crate::row::RowDecoder;

// Field lengths in bytes
pub const LENGTH_LENGTH: usize = 4;
pub const MAGIC_LENGTH: usize = 1;
pub const CRC_LENGTH: usize = 4;
pub const SCHEMA_ID_LENGTH: usize = 2;
pub const ATTRIBUTE_LENGTH: usize = 1;
pub const WRITE_CLIENT_ID_LENGTH: usize = 8;
pub const BATCH_SEQUENCE_LENGTH: usize = 4;
pub const RECORDS_COUNT_LENGTH: usize = 4;

// Field offsets
pub const LENGTH_OFFSET: usize = 0;
pub const MAGIC_OFFSET: usize = LENGTH_OFFSET + LENGTH_LENGTH;
pub const CRC_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
pub const SCHEMA_ID_OFFSET: usize = CRC_OFFSET + CRC_LENGTH;
pub const ATTRIBUTES_OFFSET: usize = SCHEMA_ID_OFFSET + SCHEMA_ID_LENGTH;
pub const WRITE_CLIENT_ID_OFFSET: usize = ATTRIBUTES_OFFSET + ATTRIBUTE_LENGTH;
pub const BATCH_SEQUENCE_OFFSET: usize = WRITE_CLIENT_ID_OFFSET + WRITE_CLIENT_ID_LENGTH;
pub const RECORDS_COUNT_OFFSET: usize = BATCH_SEQUENCE_OFFSET + BATCH_SEQUENCE_LENGTH;
pub const RECORDS_OFFSET: usize = RECORDS_COUNT_OFFSET + RECORDS_COUNT_LENGTH;

/// Total header size
pub const RECORD_BATCH_HEADER_SIZE: usize = RECORDS_OFFSET;

/// Overhead of the batch (length field)
pub const KV_OVERHEAD: usize = LENGTH_OFFSET + LENGTH_LENGTH;

/// A KV record batch.
///
/// This struct provides read access to a serialized KV record batch.
// Reference implementation:
// https://github.com/apache/fluss/blob/main/fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatch.java
pub struct KvRecordBatch {
    data: Bytes,
    position: usize,
}

impl KvRecordBatch {
    /// Create a new KvRecordBatch pointing to the given data at the specified position.
    pub fn new(data: Bytes, position: usize) -> Self {
        Self { data, position }
    }

    /// Get the size in bytes of this batch.
    pub fn size_in_bytes(&self) -> io::Result<usize> {
        if self.data.len() < self.position.saturating_add(LENGTH_LENGTH) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read batch length",
            ));
        }
        let length_i32 = i32::from_le_bytes([
            self.data[self.position],
            self.data[self.position + 1],
            self.data[self.position + 2],
            self.data[self.position + 3],
        ]);

        if length_i32 < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid batch length: {length_i32}"),
            ));
        }

        let length = length_i32 as usize;

        Ok(length.saturating_add(KV_OVERHEAD))
    }

    /// Check if this batch is valid by verifying the checksum.
    pub fn is_valid(&self) -> bool {
        if !matches!(self.size_in_bytes(), Ok(s) if s >= RECORD_BATCH_HEADER_SIZE) {
            return false;
        }

        match (self.checksum(), self.compute_checksum()) {
            (Ok(stored), Ok(computed)) => stored == computed,
            _ => false,
        }
    }

    /// Get the magic byte.
    pub fn magic(&self) -> io::Result<u8> {
        if self.data.len() < self.position.saturating_add(MAGIC_OFFSET).saturating_add(1) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read magic byte",
            ));
        }
        Ok(self.data[self.position + MAGIC_OFFSET])
    }

    /// Get the checksum.
    pub fn checksum(&self) -> io::Result<u32> {
        if self.data.len() < self.position.saturating_add(CRC_OFFSET).saturating_add(4) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read checksum",
            ));
        }
        Ok(u32::from_le_bytes([
            self.data[self.position + CRC_OFFSET],
            self.data[self.position + CRC_OFFSET + 1],
            self.data[self.position + CRC_OFFSET + 2],
            self.data[self.position + CRC_OFFSET + 3],
        ]))
    }

    /// Compute the checksum of this batch.
    pub fn compute_checksum(&self) -> io::Result<u32> {
        let size = self.size_in_bytes()?;
        if size < RECORD_BATCH_HEADER_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Batch size {size} is less than header size {RECORD_BATCH_HEADER_SIZE}"),
            ));
        }

        let start = self.position.saturating_add(SCHEMA_ID_OFFSET);
        let end = self.position.saturating_add(size);

        if end > self.data.len() || start >= end {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to compute checksum",
            ));
        }

        Ok(crc32c::crc32c(&self.data[start..end]))
    }

    /// Get the schema ID.
    pub fn schema_id(&self) -> io::Result<i16> {
        if self.data.len()
            < self
                .position
                .saturating_add(SCHEMA_ID_OFFSET)
                .saturating_add(2)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read schema ID",
            ));
        }
        Ok(i16::from_le_bytes([
            self.data[self.position + SCHEMA_ID_OFFSET],
            self.data[self.position + SCHEMA_ID_OFFSET + 1],
        ]))
    }

    /// Get the writer ID.
    pub fn writer_id(&self) -> io::Result<i64> {
        if self.data.len()
            < self
                .position
                .saturating_add(WRITE_CLIENT_ID_OFFSET)
                .saturating_add(8)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read writer ID",
            ));
        }
        Ok(i64::from_le_bytes([
            self.data[self.position + WRITE_CLIENT_ID_OFFSET],
            self.data[self.position + WRITE_CLIENT_ID_OFFSET + 1],
            self.data[self.position + WRITE_CLIENT_ID_OFFSET + 2],
            self.data[self.position + WRITE_CLIENT_ID_OFFSET + 3],
            self.data[self.position + WRITE_CLIENT_ID_OFFSET + 4],
            self.data[self.position + WRITE_CLIENT_ID_OFFSET + 5],
            self.data[self.position + WRITE_CLIENT_ID_OFFSET + 6],
            self.data[self.position + WRITE_CLIENT_ID_OFFSET + 7],
        ]))
    }

    /// Get the batch sequence.
    pub fn batch_sequence(&self) -> io::Result<i32> {
        if self.data.len()
            < self
                .position
                .saturating_add(BATCH_SEQUENCE_OFFSET)
                .saturating_add(4)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read batch sequence",
            ));
        }
        Ok(i32::from_le_bytes([
            self.data[self.position + BATCH_SEQUENCE_OFFSET],
            self.data[self.position + BATCH_SEQUENCE_OFFSET + 1],
            self.data[self.position + BATCH_SEQUENCE_OFFSET + 2],
            self.data[self.position + BATCH_SEQUENCE_OFFSET + 3],
        ]))
    }

    /// Get the number of records in this batch.
    pub fn record_count(&self) -> io::Result<i32> {
        if self.data.len()
            < self
                .position
                .saturating_add(RECORDS_COUNT_OFFSET)
                .saturating_add(4)
        {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read record count",
            ));
        }
        Ok(i32::from_le_bytes([
            self.data[self.position + RECORDS_COUNT_OFFSET],
            self.data[self.position + RECORDS_COUNT_OFFSET + 1],
            self.data[self.position + RECORDS_COUNT_OFFSET + 2],
            self.data[self.position + RECORDS_COUNT_OFFSET + 3],
        ]))
    }

    /// Create an iterable collection of records in this batch.
    ///
    /// This validates the batch checksum before returning the records.
    /// For trusted data paths, use `records_unchecked()` to skip validation.
    ///
    /// Mirrors: KvRecordBatch.records(ReadContext)
    pub fn records(&self, read_context: &dyn ReadContext) -> Result<KvRecords> {
        if !self.is_valid() {
            return Err(crate::error::Error::IoUnexpectedError {
                message: "Invalid batch checksum".to_string(),
                source: io::Error::new(io::ErrorKind::InvalidData, "Invalid batch checksum"),
            });
        }
        self.records_unchecked(read_context)
    }

    /// Create an iterable collection of records in this batch without validating the checksum.
    pub fn records_unchecked(&self, read_context: &dyn ReadContext) -> Result<KvRecords> {
        let size = self.size_in_bytes()?;
        let count = self.record_count()?;
        let schema_id = self.schema_id()?;

        if count < 0 {
            return Err(crate::error::Error::IoUnexpectedError {
                message: format!("Invalid record count: {count}"),
                source: io::Error::new(io::ErrorKind::InvalidData, "Invalid record count"),
            });
        }

        // Get row decoder for this schema from context (cached)
        let row_decoder = read_context.get_row_decoder(schema_id)?;

        Ok(KvRecords {
            iter: KvRecordIterator {
                data: self.data.clone(),
                position: self.position + RECORDS_OFFSET,
                end: self.position + size,
                remaining_count: count,
            },
            row_decoder,
        })
    }
}

/// Iterable collection of KV records with associated decoder.
///
/// This wrapper provides both iteration capability and access to the row decoder
/// needed to decode record values into typed rows.
pub struct KvRecords {
    iter: KvRecordIterator,
    row_decoder: Arc<dyn RowDecoder>,
}

impl KvRecords {
    /// Get a reference to the row decoder for decoding record values.
    ///
    /// Returns a reference tied to the lifetime of `&self`.
    /// Use this when iterating by reference.
    pub fn decoder(&self) -> &dyn RowDecoder {
        &*self.row_decoder
    }

    /// Get an owned Arc to the row decoder.
    ///
    /// Returns a cloned Arc that can outlive the KvRecords,
    /// allowing you to grab it before consuming the iterator.
    /// Useful if you must keep the decoder beyond the iterable’s lifetime(collect then decode style)
    pub fn decoder_arc(&self) -> Arc<dyn RowDecoder> {
        Arc::clone(&self.row_decoder)
    }
}

impl IntoIterator for KvRecords {
    type Item = io::Result<KvRecord>;
    type IntoIter = KvRecordIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

/// Iterator over records in a KV record batch.
pub struct KvRecordIterator {
    data: Bytes,
    position: usize,
    end: usize,
    remaining_count: i32,
}

impl Iterator for KvRecordIterator {
    type Item = io::Result<KvRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_count <= 0 || self.position >= self.end {
            return None;
        }

        match KvRecord::read_from(&self.data, self.position) {
            Ok((record, size)) => {
                self.position += size;
                self.remaining_count -= 1;
                Some(Ok(record))
            }
            Err(e) => {
                self.remaining_count = 0; // Stop iteration on error
                Some(Err(e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypes, KvFormat};
    use crate::record::kv::test_util::TestReadContext;
    use crate::record::kv::{CURRENT_KV_MAGIC_VALUE, KvRecordBatchBuilder};
    use crate::row::InternalRow;
    use crate::row::binary::BinaryWriter;

    use bytes::{BufMut, BytesMut};

    #[test]
    fn test_invalid_batch_lengths() {
        // Test negative length
        let mut buf = BytesMut::new();
        buf.put_i32_le(-1);
        let bytes = buf.freeze();
        let batch = KvRecordBatch::new(bytes, 0);
        assert!(batch.size_in_bytes().is_err()); // Should error for invalid
        assert!(!batch.is_valid());

        // Test overflow length
        let mut buf = BytesMut::new();
        buf.put_i32_le(i32::MAX);
        let bytes = buf.freeze();
        let batch = KvRecordBatch::new(bytes, 0);
        assert!(!batch.is_valid());

        // Test too-short buffer
        let mut buf = BytesMut::new();
        buf.put_i32_le(100); // Claims 100 bytes but buffer is tiny
        let bytes = buf.freeze();
        let batch = KvRecordBatch::new(bytes, 0);
        assert!(!batch.is_valid());
    }

    #[test]
    fn test_kv_record_batch_build_and_read() {
        use crate::row::compacted::CompactedRowWriter;

        let schema_id = 42;
        let write_limit = 4096;

        let mut builder = KvRecordBatchBuilder::new(schema_id, write_limit, KvFormat::COMPACTED);
        builder.set_writer_state(100, 5);

        let key1 = b"key1";
        let mut value1_writer = CompactedRowWriter::new(1);
        value1_writer.write_bytes(&[1, 2, 3, 4, 5]);

        let row_bytes = value1_writer.buffer();
        builder.append_row(key1, Some(row_bytes)).unwrap();

        let key2 = b"key2";
        builder.append_row(key2, None).unwrap();

        let bytes = builder.build().unwrap();

        let batch = KvRecordBatch::new(bytes.clone(), 0);
        assert!(batch.is_valid());
        assert_eq!(batch.magic().unwrap(), CURRENT_KV_MAGIC_VALUE);
        assert_eq!(batch.schema_id().unwrap(), schema_id as i16);
        assert_eq!(batch.writer_id().unwrap(), 100);
        assert_eq!(batch.batch_sequence().unwrap(), 5);
        assert_eq!(batch.record_count().unwrap(), 2);

        // Create ReadContext for reading
        let read_context = TestReadContext::compacted(vec![DataTypes::bytes()]);

        // Iterate and verify records using typed API
        let records = batch.records(&read_context).unwrap();
        let decoder = records.decoder_arc(); // Get Arc before consuming

        let mut iter = records.into_iter();
        let record1 = iter.next().unwrap().unwrap();
        assert_eq!(record1.key().as_ref(), key1);
        assert!(!record1.is_deletion());
        let row1 = record1.row(&*decoder).unwrap();
        assert_eq!(row1.get_bytes(0).unwrap(), &[1, 2, 3, 4, 5]);

        let record2 = iter.next().unwrap().unwrap();
        assert_eq!(record2.key().as_ref(), key2);
        assert!(record2.is_deletion());

        assert!(iter.next().is_none());
    }
}
