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

//! KV record batch builder implementation.
//!
//! This module provides the KvRecordBatchBuilder for building batches of KV records.

use crate::error::{Error, Result};
use crate::metadata::KvFormat;
use crate::record::kv::kv_record::KvRecord;
use crate::record::kv::kv_record_batch::{
    ATTRIBUTES_OFFSET, BATCH_SEQUENCE_OFFSET, CRC_OFFSET, LENGTH_LENGTH, LENGTH_OFFSET,
    MAGIC_OFFSET, RECORD_BATCH_HEADER_SIZE, RECORDS_COUNT_OFFSET, SCHEMA_ID_OFFSET,
    WRITE_CLIENT_ID_OFFSET,
};
use crate::record::kv::{CURRENT_KV_MAGIC_VALUE, NO_BATCH_SEQUENCE, NO_WRITER_ID};
use bytes::{Bytes, BytesMut};
use log::warn;
use std::io;

/// Builder for KvRecordBatch.
///
/// This builder accumulates KV records and produces a serialized batch with proper
/// header information and checksums.
// Reference implementation:
// https://github.com/apache/fluss/blob/main/fluss-common/src/main/java/org/apache/fluss/record/KvRecordBatchBuilder.java
pub struct KvRecordBatchBuilder {
    schema_id: i32,
    magic: u8,
    write_limit: usize,
    buffer: BytesMut,
    writer_id: i64,
    batch_sequence: i32,
    current_record_number: i32,
    size_in_bytes: usize,
    is_closed: bool,
    kv_format: KvFormat,
    aborted: bool,
    built_buffer: Option<Bytes>,
}

impl KvRecordBatchBuilder {
    /// Create a new KvRecordBatchBuilder.
    ///
    /// # Arguments
    /// * `schema_id` - The schema ID for records in this batch (must fit in i16)
    /// * `write_limit` - Maximum bytes that can be appended
    /// * `kv_format` - The KV format (Compacted, Indexed, or Aligned)
    pub fn new(schema_id: i32, write_limit: usize, kv_format: KvFormat) -> Self {
        assert!(
            schema_id <= i16::MAX as i32,
            "schema_id shouldn't be greater than the max value of i16: {}",
            i16::MAX
        );

        let mut buffer = BytesMut::with_capacity(write_limit.max(RECORD_BATCH_HEADER_SIZE));

        // Reserve space for header (we'll write it at the end)
        buffer.resize(RECORD_BATCH_HEADER_SIZE, 0);

        Self {
            schema_id,
            magic: CURRENT_KV_MAGIC_VALUE,
            write_limit,
            buffer,
            writer_id: NO_WRITER_ID,
            batch_sequence: NO_BATCH_SEQUENCE,
            current_record_number: 0,
            size_in_bytes: RECORD_BATCH_HEADER_SIZE,
            is_closed: false,
            kv_format,
            aborted: false,
            built_buffer: None,
        }
    }

    /// Check if there is room for a new record containing the given key and row bytes.
    /// If no records have been appended, this always returns true.
    pub fn has_room_for_row(&self, key: &[u8], row_bytes: Option<&[u8]>) -> bool {
        self.size_in_bytes + KvRecord::size_of(key, row_bytes) <= self.write_limit
    }

    /// Append a KV record with row bytes to the batch.
    ///
    /// Returns an error if:
    /// - The builder has been aborted
    /// - The builder is closed
    /// - Adding this record would exceed the write limit
    /// - The maximum number of records is exceeded
    /// - The KV format is not COMPACTED
    pub fn append_row(&mut self, key: &[u8], row_bytes: Option<&[u8]>) -> io::Result<()> {
        if self.kv_format != KvFormat::COMPACTED {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "append_row can only be used with KvFormat::COMPACTED",
            ));
        }

        if self.aborted {
            return Err(io::Error::other(
                "Tried to append a record, but KvRecordBatchBuilder has already been aborted",
            ));
        }

        if self.is_closed {
            return Err(io::Error::other(
                "Tried to append a record, but KvRecordBatchBuilder is closed for record appends",
            ));
        }

        // Check record count limit before mutation
        if self.current_record_number == i32::MAX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Maximum number of records per batch exceeded, max records: {}",
                    i32::MAX
                ),
            ));
        }

        let record_size = KvRecord::size_of(key, row_bytes);
        if self.size_in_bytes + record_size > self.write_limit {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                format!(
                    "Adding record would exceed write limit: {} + {} > {}",
                    self.size_in_bytes, record_size, self.write_limit
                ),
            ));
        }

        let record_byte_size = KvRecord::write_to_buf(&mut self.buffer, key, row_bytes)?;
        debug_assert_eq!(record_byte_size, record_size, "Record size mismatch");

        self.current_record_number += 1;
        self.size_in_bytes += record_byte_size;

        // Invalidate cached buffer since we modified the batch
        self.built_buffer = None;

        Ok(())
    }

    /// Set the writer state (writer ID and batch base sequence).
    ///
    /// This invalidates any cached buffer, ensuring the batch header will be rebuilt
    /// on the next call to [`build`](Self::build).
    pub fn set_writer_state(&mut self, writer_id: i64, batch_base_sequence: i32) {
        self.writer_id = writer_id;
        self.batch_sequence = batch_base_sequence;
        // Invalidate cached buffer since header fields changed
        self.built_buffer = None;
    }

    /// Build the batch and return the serialized bytes.
    ///
    /// This can be called multiple times as the batch is cached after the first build.
    ///
    /// # Caching and Mutations
    ///
    /// The builder caches the result after the first successful build. However, the cache
    /// is invalidated (and the batch rebuilt) if any of the following occur after building:
    /// - Calling [`append_row`](Self::append_row) to add records
    /// - Calling [`set_writer_state`](Self::set_writer_state) to modify writer metadata
    ///
    /// This allows the builder to be reused with different writer states or to continue
    /// appending records after an initial build, but callers should be aware that the
    /// built bytes may change if mutations occur between builds.
    ///
    /// Note: [`close`](Self::close) prevents further appends but does not prevent writer state modifications.
    pub fn build(&mut self) -> Result<Bytes> {
        if self.aborted {
            return Err(Error::UnexpectedError {
                message: "Attempting to build an aborted record batch".to_string(),
                source: None,
            });
        }

        if let Some(ref cached) = self.built_buffer {
            return Ok(cached.clone());
        }

        self.write_batch_header()?;
        let bytes = self.buffer.clone().freeze();
        self.built_buffer = Some(bytes);
        Ok(self.built_buffer.as_ref().unwrap().clone())
    }

    /// Get the writer ID.
    pub fn writer_id(&self) -> i64 {
        self.writer_id
    }

    /// Get the batch sequence.
    pub fn batch_sequence(&self) -> i32 {
        self.batch_sequence
    }

    /// Check if the builder is closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// Abort the builder.
    /// After aborting, no more records can be appended and the batch cannot be built.
    pub fn abort(&mut self) {
        self.aborted = true;
    }

    /// Close the builder.
    /// After closing, no more records can be appended, but the batch can still be built.
    pub fn close(&mut self) -> Result<()> {
        if self.aborted {
            return Err(Error::UnexpectedError {
                message: "Cannot close KvRecordBatchBuilder as it has already been aborted"
                    .to_string(),
                source: None,
            });
        }
        self.is_closed = true;
        Ok(())
    }

    /// Get the current size in bytes of the batch.
    pub fn get_size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    // ----------------------- Internal methods -------------------------------

    /// Write the batch header.
    fn write_batch_header(&mut self) -> io::Result<()> {
        let size_without_length = self.size_in_bytes - LENGTH_LENGTH;
        let total_size = i32::try_from(size_without_length).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Batch size {size_without_length} exceeds i32::MAX"),
            )
        })?;

        // Compute attributes before borrowing buffer mutably
        let attributes = self.compute_attributes();

        // Write to the beginning of the buffer
        let header = &mut self.buffer[0..RECORD_BATCH_HEADER_SIZE];

        // Write length
        header[LENGTH_OFFSET..LENGTH_OFFSET + LENGTH_LENGTH]
            .copy_from_slice(&total_size.to_le_bytes());

        // Write magic
        header[MAGIC_OFFSET] = self.magic;

        // Write empty CRC first (will update later)
        header[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&0u32.to_le_bytes());

        // Write schema ID
        header[SCHEMA_ID_OFFSET..SCHEMA_ID_OFFSET + 2]
            .copy_from_slice(&(self.schema_id as i16).to_le_bytes());

        // Write attributes
        header[ATTRIBUTES_OFFSET] = attributes;

        // Write writer ID
        header[WRITE_CLIENT_ID_OFFSET..WRITE_CLIENT_ID_OFFSET + 8]
            .copy_from_slice(&self.writer_id.to_le_bytes());

        // Write batch sequence
        header[BATCH_SEQUENCE_OFFSET..BATCH_SEQUENCE_OFFSET + 4]
            .copy_from_slice(&self.batch_sequence.to_le_bytes());

        // Write record count
        header[RECORDS_COUNT_OFFSET..RECORDS_COUNT_OFFSET + 4]
            .copy_from_slice(&self.current_record_number.to_le_bytes());

        // Compute and update CRC
        let crc = crc32c::crc32c(&self.buffer[SCHEMA_ID_OFFSET..self.size_in_bytes]);
        self.buffer[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_le_bytes());

        Ok(())
    }

    /// Compute the attributes byte.
    fn compute_attributes(&self) -> u8 {
        // Currently no attributes are used
        0
    }
}

impl Drop for KvRecordBatchBuilder {
    fn drop(&mut self) {
        // Warn if the builder has records but was never built or was aborted
        if self.current_record_number > 0 && !self.aborted && self.built_buffer.is_none() {
            warn!(
                "Warning: KvRecordBatchBuilder dropped with {} record(s) that were never built. \
                 Call build() to serialize the batch before dropping.",
                self.current_record_number
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypes, RowType};
    use crate::row::binary::BinaryWriter;
    use crate::row::compacted::{CompactedRow, CompactedRowWriter};
    use std::sync::LazyLock;
    static TEST_ROW_TYPE: LazyLock<RowType> =
        LazyLock::new(|| RowType::with_data_types(vec![DataTypes::bytes()]));

    // Helper function to create a CompactedRowWriter with a single bytes field for testing
    fn create_test_row(data: &[u8]) -> CompactedRow<'_> {
        CompactedRow::from_bytes(&TEST_ROW_TYPE, data)
    }

    #[test]
    fn test_builder_basic_operations() {
        // Test basic workflow: initial state, writer state, append, close, build
        let schema_id = 42;
        let write_limit = 4096;
        let mut builder = KvRecordBatchBuilder::new(schema_id, write_limit, KvFormat::COMPACTED);

        assert!(!builder.is_closed());
        assert_eq!(builder.writer_id(), NO_WRITER_ID);
        assert_eq!(builder.batch_sequence(), NO_BATCH_SEQUENCE);

        builder.set_writer_state(100, 5);
        assert_eq!(builder.writer_id(), 100);
        assert_eq!(builder.batch_sequence(), 5);

        let key1 = b"key1";
        let value1 = create_test_row(b"value1");
        assert!(builder.has_room_for_row(key1, Some(value1.as_bytes())));
        builder.append_row(key1, Some(value1.as_bytes())).unwrap();

        let key2 = b"key2";
        assert!(builder.has_room_for_row(key2, None));
        builder.append_row(key2, None).unwrap();

        builder.close().unwrap();
        assert!(builder.is_closed());

        let bytes = builder.build().unwrap();
        assert!(bytes.len() > RECORD_BATCH_HEADER_SIZE);

        // Building again should return cached result
        let bytes2 = builder.build().unwrap();
        assert_eq!(bytes.len(), bytes2.len());

        // Test lifecycle: abort behavior
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);
        let value = create_test_row(b"value");
        builder.append_row(b"key", Some(value.as_bytes())).unwrap();
        builder.abort();
        assert!(builder.append_row(b"key2", None).is_err());
        assert!(builder.build().is_err());
        assert!(builder.close().is_err());

        // Test lifecycle: close behavior
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);
        let value = create_test_row(b"value");
        builder.append_row(b"key", Some(value.as_bytes())).unwrap();
        builder.close().unwrap();
        assert!(builder.append_row(b"key2", None).is_err());
        assert!(builder.build().is_ok());

        // Test KvFormat validation
        let mut row_writer = CompactedRowWriter::new(1);
        row_writer.write_int(42);
        let row_bytes = row_writer.buffer();

        // INDEXED format should reject append_row
        let mut indexed_builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::INDEXED);
        let result = indexed_builder.append_row(b"key", Some(row_bytes));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);

        // COMPACTED format should accept append_row
        let mut compacted_builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);
        let result = compacted_builder.append_row(b"key", Some(row_bytes));
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_limit_enforcement() {
        let write_limit = 100; // Very small limit
        let mut builder = KvRecordBatchBuilder::new(1, write_limit, KvFormat::COMPACTED);

        // Test has_room_for_row helper
        let large_key = vec![0u8; 1000];
        let large_value = vec![1u8; 1000];
        let large_row = create_test_row(&large_value);
        assert!(!builder.has_room_for_row(&large_key, Some(large_row.as_bytes())));
        let small_value = create_test_row(b"value");
        assert!(builder.has_room_for_row(b"key", Some(small_value.as_bytes())));

        // Test append enforcement - add small record first
        builder
            .append_row(b"key", Some(small_value.as_bytes()))
            .unwrap();

        // Try to add large record that exceeds limit (reuse large_row from above)
        let result = builder.append_row(b"key2", Some(large_row.as_bytes()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WriteZero);
    }

    #[test]
    fn test_append_checks_record_count_limit() {
        let mut builder = KvRecordBatchBuilder::new(1, 100000, KvFormat::COMPACTED);
        builder.current_record_number = i32::MAX - 1;

        let value1 = create_test_row(b"value1");
        builder
            .append_row(b"key1", Some(value1.as_bytes()))
            .unwrap();

        let value2 = create_test_row(b"value2");
        let result = builder.append_row(b"key2", Some(value2.as_bytes()));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    #[should_panic(expected = "schema_id shouldn't be greater than")]
    fn test_builder_invalid_schema_id() {
        KvRecordBatchBuilder::new(i16::MAX as i32 + 1, 4096, KvFormat::COMPACTED);
    }

    #[test]
    fn test_builder_cache_invalidation() {
        use crate::record::kv::KvRecordBatch;

        // Test cache invalidation on append
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);
        builder.set_writer_state(100, 5);

        let value1 = create_test_row(b"value1");
        builder
            .append_row(b"key1", Some(value1.as_bytes()))
            .unwrap();
        let bytes1 = builder.build().unwrap();
        let len1 = bytes1.len();

        // Append another record - this should invalidate the cache
        let value2 = create_test_row(b"value2");
        builder
            .append_row(b"key2", Some(value2.as_bytes()))
            .unwrap();
        let bytes2 = builder.build().unwrap();
        let len2 = bytes2.len();

        // Verify the second build includes both records
        assert!(len2 > len1);
        let batch = KvRecordBatch::new(bytes2, 0);
        assert!(batch.is_valid());
        assert_eq!(batch.record_count().unwrap(), 2);

        // Test cache invalidation on writer state change
        let mut builder = KvRecordBatchBuilder::new(1, 4096, KvFormat::COMPACTED);
        builder.set_writer_state(100, 5);
        let value = create_test_row(b"value");
        builder.append_row(b"key", Some(value.as_bytes())).unwrap();
        let bytes1 = builder.build().unwrap();

        // Change writer state - this should invalidate the cache
        builder.set_writer_state(200, 10);
        let bytes2 = builder.build().unwrap();

        assert_ne!(bytes1, bytes2);

        let batch1 = KvRecordBatch::new(bytes1, 0);
        let batch2 = KvRecordBatch::new(bytes2, 0);

        assert_eq!(batch1.writer_id().unwrap(), 100);
        assert_eq!(batch1.batch_sequence().unwrap(), 5);
        assert_eq!(batch2.writer_id().unwrap(), 200);
        assert_eq!(batch2.batch_sequence().unwrap(), 10);
    }

    #[test]
    fn test_builder_with_compacted_row_writer() -> crate::error::Result<()> {
        use crate::record::kv::KvRecordBatch;
        use crate::row::InternalRow;

        let mut builder = KvRecordBatchBuilder::new(1, 100000, KvFormat::COMPACTED);
        builder.set_writer_state(100, 5);

        // Create and append first record with CompactedRowWriter
        let mut row_writer1 = CompactedRowWriter::new(2);
        row_writer1.write_int(42);
        row_writer1.write_string("hello");

        let row_bytes1 = row_writer1.buffer();

        let key1 = b"key1";
        assert!(builder.has_room_for_row(key1, Some(row_bytes1)));
        builder.append_row(key1, Some(row_bytes1))?;

        // Create and append second record
        let mut row_writer2 = CompactedRowWriter::new(2);
        row_writer2.write_int(100);
        row_writer2.write_string("world");

        let row_bytes2 = row_writer2.buffer();

        let key2 = b"key2";
        builder.append_row(key2, Some(row_bytes2))?;

        // Append a deletion record
        let key3 = b"key3";
        builder.append_row(key3, None)?;

        // Build and verify
        builder.close()?;
        let bytes = builder.build()?;

        let batch = KvRecordBatch::new(bytes, 0);
        assert!(batch.is_valid());
        assert_eq!(batch.record_count()?, 3);
        assert_eq!(batch.writer_id()?, 100);
        assert_eq!(batch.batch_sequence()?, 5);

        // Create ReadContext for reading typed rows
        let types = vec![DataTypes::int(), DataTypes::string()];
        let read_context = crate::record::kv::test_util::TestReadContext::compacted(types);

        // Read back and verify records using idiomatic for-loop
        let records = batch.records(&read_context)?;
        let decoder = records.decoder_arc();
        let mut record_count = 0;

        for rec in records {
            let rec = rec?;
            record_count += 1;

            match record_count {
                1 => {
                    assert_eq!(rec.key().as_ref(), key1);
                    let row = rec.row(&*decoder).unwrap();
                    assert_eq!(row.get_int(0)?, 42);
                    assert_eq!(row.get_string(1)?, "hello");
                }
                2 => {
                    assert_eq!(rec.key().as_ref(), key2);
                    let row = rec.row(&*decoder).unwrap();
                    assert_eq!(row.get_int(0)?, 100);
                    assert_eq!(row.get_string(1)?, "world");
                }
                3 => {
                    assert_eq!(rec.key().as_ref(), key3);
                    assert!(rec.is_deletion());
                }
                _ => panic!("Unexpected record count"),
            }
        }

        assert_eq!(record_count, 3);
        Ok(())
    }
}
