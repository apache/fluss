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

//! Key-Value record implementation.
//!
//! This module provides the KvRecord struct which represents an immutable key-value record.
//! The record format is:
//! - Length => Int32
//! - KeyLength => Unsigned VarInt
//! - Key => bytes
//! - Row => BinaryRow (optional, if null then this is a deletion record)

use bytes::{BufMut, Bytes, BytesMut};
use std::io;

use crate::row::RowDecoder;
use crate::row::compacted::CompactedRow;
use crate::util::varint::{
    read_unsigned_varint_bytes, size_of_unsigned_varint, write_unsigned_varint_buf,
};

/// Length field size in bytes
pub const LENGTH_LENGTH: usize = 4;

/// A key-value record containing raw key and value bytes.
///
/// The schema is:
/// - Length => Int32
/// - KeyLength => Unsigned VarInt
/// - Key => bytes
/// - Value => bytes (BinaryRow, written directly without length prefix)
///
/// When the value is None (deletion), no Value bytes are present.
///
/// This struct stores only raw bytes. To decode the value into a typed row,
/// use the `row()` method with a RowDecoder (typically obtained from the iterator).
///
/// Reference implementation:
/// <https://github.com/apache/fluss/blob/main/fluss-common/src/main/java/org/apache/fluss/record/KvRecord.java>
#[derive(Debug, Clone)]
pub struct KvRecord {
    key: Bytes,
    value_bytes: Option<Bytes>,
    size_in_bytes: usize,
}

impl KvRecord {
    /// Get the key bytes.
    pub fn key(&self) -> &Bytes {
        &self.key
    }

    /// Get the raw value bytes (for testing).
    #[cfg(test)]
    pub(crate) fn value_bytes(&self) -> Option<&Bytes> {
        self.value_bytes.as_ref()
    }

    /// Decode the value bytes into a typed row using the provided decoder.
    /// This creates a lightweight CompactedRow view over the raw bytes.
    /// Actual field parsing is lazy (on first access).
    pub fn row<'a>(&'a self, decoder: &dyn RowDecoder) -> Option<CompactedRow<'a>> {
        self.value_bytes.as_ref().map(|bytes| {
            // Decode on-demand - CompactedRow<'a> lifetime tied to &'a self
            decoder.decode(bytes.as_ref())
        })
    }

    /// Calculate the total size of the record when serialized (including length prefix).
    pub fn size_of(key: &[u8], value: Option<&[u8]>) -> usize {
        Self::size_without_length(key, value) + LENGTH_LENGTH
    }

    /// Calculate the size without the length prefix.
    fn size_without_length(key: &[u8], value: Option<&[u8]>) -> usize {
        let key_len = key.len();
        let key_len_size = size_of_unsigned_varint(key_len as u32);

        match value {
            Some(v) => key_len_size.saturating_add(key_len).saturating_add(v.len()),
            None => {
                // Deletion: no value bytes
                key_len_size.saturating_add(key_len)
            }
        }
    }

    /// Write a KV record to a buffer.
    ///
    /// Returns the number of bytes written.
    pub fn write_to_buf(buf: &mut BytesMut, key: &[u8], value: Option<&[u8]>) -> io::Result<usize> {
        let size_in_bytes = Self::size_without_length(key, value);

        let size_i32 = i32::try_from(size_in_bytes).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Record size {size_in_bytes} exceeds i32::MAX"),
            )
        })?;
        buf.put_i32_le(size_i32);
        let key_len = key.len() as u32;
        write_unsigned_varint_buf(key_len, buf);

        buf.put_slice(key);

        if let Some(v) = value {
            buf.put_slice(v);
        }
        // For None (deletion), don't write any value bytes

        Ok(size_in_bytes + LENGTH_LENGTH)
    }

    /// Read a KV record from bytes at the given position.
    ///
    /// Returns the KvRecord and the number of bytes consumed.
    /// The record contains only raw bytes; use `row()` with a RowDecoder to decode the value.
    pub fn read_from(bytes: &Bytes, position: usize) -> io::Result<(Self, usize)> {
        if bytes.len() < position.saturating_add(LENGTH_LENGTH) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read record length",
            ));
        }

        let size_in_bytes_i32 = i32::from_le_bytes([
            bytes[position],
            bytes[position + 1],
            bytes[position + 2],
            bytes[position + 3],
        ]);

        if size_in_bytes_i32 < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid record length: {size_in_bytes_i32}"),
            ));
        }

        let size_in_bytes = size_in_bytes_i32 as usize;

        let total_size = size_in_bytes.checked_add(LENGTH_LENGTH).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Record size overflow: {size_in_bytes} + {LENGTH_LENGTH}"),
            )
        })?;

        let available = bytes.len().saturating_sub(position);
        if available < total_size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "Not enough bytes to read record: expected {total_size}, available {available}"
                ),
            ));
        }

        let mut current_offset = position + LENGTH_LENGTH;
        let record_end = position + total_size;

        // Read key length as unsigned varint (bounded by record end)
        let (key_len, varint_size) =
            read_unsigned_varint_bytes(&bytes[current_offset..record_end])?;
        current_offset += varint_size;

        // Read key bytes
        let key_end = current_offset + key_len as usize;
        if key_end > position + total_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Key length exceeds record size",
            ));
        }
        let key = bytes.slice(current_offset..key_end);
        current_offset = key_end;

        // Read value bytes directly (don't decode yet - will decode on-demand)
        let value_bytes = if current_offset < record_end {
            // Value is present: all remaining bytes are the value
            Some(bytes.slice(current_offset..record_end))
        } else {
            // No remaining bytes: this is a deletion record
            None
        };

        Ok((
            Self {
                key,
                value_bytes,
                size_in_bytes: total_size,
            },
            total_size,
        ))
    }

    /// Get the total size in bytes of this record.
    pub fn get_size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    /// Check if this is a deletion record (no value).
    pub fn is_deletion(&self) -> bool {
        self.value_bytes.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_record_basic_operations() {
        let key = b"test_key";
        let value = b"test_value";

        // Test size calculation with value
        let size_with_value = KvRecord::size_of(key, Some(value));
        assert_eq!(
            size_with_value,
            LENGTH_LENGTH + size_of_unsigned_varint(key.len() as u32) + key.len() + value.len()
        );

        // Test size calculation without value (deletion)
        let size_without_value = KvRecord::size_of(key, None);
        assert_eq!(
            size_without_value,
            LENGTH_LENGTH + size_of_unsigned_varint(key.len() as u32) + key.len()
        );

        // Test write/read round trip with value
        let mut buf = BytesMut::new();
        let written = KvRecord::write_to_buf(&mut buf, key, Some(value)).unwrap();

        let bytes = buf.freeze();
        let (record, read_size) = KvRecord::read_from(&bytes, 0).unwrap();

        assert_eq!(written, read_size);
        assert_eq!(record.key().as_ref(), key);
        assert_eq!(record.value_bytes().unwrap().as_ref(), value);
        assert_eq!(record.get_size_in_bytes(), written);
        assert!(!record.is_deletion());

        // Test deletion record (no value)
        let delete_key = b"delete_me";
        let mut buf = BytesMut::new();
        let written = KvRecord::write_to_buf(&mut buf, delete_key, None).unwrap();

        let bytes = buf.freeze();
        let (record, read_size) = KvRecord::read_from(&bytes, 0).unwrap();

        assert_eq!(written, read_size);
        assert_eq!(record.key().as_ref(), delete_key);
        assert!(record.is_deletion());
        assert!(record.value_bytes().is_none());
    }

    #[test]
    fn test_kv_record_multiple_records() {
        // Test multiple regular-sized records in buffer
        let records = vec![
            (b"key1".as_slice(), Some(b"value1".as_slice())),
            (b"key2".as_slice(), None), // Deletion
            (b"key3".as_slice(), Some(b"value3".as_slice())),
        ];

        let mut buf = BytesMut::new();
        for (key, value) in &records {
            KvRecord::write_to_buf(&mut buf, key, *value).unwrap();
        }

        let bytes = buf.freeze();
        let mut offset = 0;
        for (expected_key, expected_value) in &records {
            let (record, size) = KvRecord::read_from(&bytes, offset).unwrap();
            assert_eq!(record.key().as_ref(), *expected_key);
            match expected_value {
                Some(v) => {
                    assert_eq!(record.value_bytes().unwrap().as_ref(), *v);
                    assert!(!record.is_deletion());
                }
                None => {
                    assert!(record.is_deletion());
                    assert!(record.value_bytes().is_none());
                }
            }
            offset += size;
        }
        assert_eq!(offset, bytes.len());

        // Test large keys and values
        let large_key = vec![0u8; 1024];
        let large_value = vec![1u8; 4096];

        let mut buf = BytesMut::new();
        let written = KvRecord::write_to_buf(&mut buf, &large_key, Some(&large_value)).unwrap();

        let bytes = buf.freeze();
        let (record, read_size) = KvRecord::read_from(&bytes, 0).unwrap();

        assert_eq!(written, read_size);
        assert_eq!(record.key().len(), large_key.len());
        assert_eq!(record.value_bytes().unwrap().len(), large_value.len());
    }

    #[test]
    fn test_invalid_record_lengths() {
        let mut buf = BytesMut::new();
        buf.put_i32_le(-1); // Negative length
        buf.put_u8(1); // Some dummy data
        buf.put_slice(b"key");
        let bytes = buf.freeze();
        let result = KvRecord::read_from(&bytes, 0);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), io::ErrorKind::InvalidData);
        }

        // Test overflow length
        let mut buf = BytesMut::new();
        buf.put_i32_le(i32::MAX); // Very large length
        buf.put_u8(1); // Some dummy data
        let bytes = buf.freeze();
        let result = KvRecord::read_from(&bytes, 0);
        assert!(result.is_err());

        // Test impossibly large but non-negative length
        let mut buf = BytesMut::new();
        buf.put_i32_le(1_000_000);
        let bytes = buf.freeze();
        let result = KvRecord::read_from(&bytes, 0);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), io::ErrorKind::UnexpectedEof);
        }
    }
}
