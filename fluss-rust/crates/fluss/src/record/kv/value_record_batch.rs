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

//! Reader for the value-record batch returned by a KV (primary-key) limit
//! scan. This is a distinct wire format from [`super::KvRecordBatch`]: it
//! carries value-only records (no keys, no CRC/writer-id header) and a schema
//! id *per record* rather than per batch.
//!
//! Batch layout (little-endian):
//! - Length      => Int32  (size of everything after this field)
//! - Magic       => Int8
//! - RecordCount => Int32
//! - Records     => [ValueRecord]
//!
//! Each `ValueRecord`:
//! - Length   => Int32  (size after this field: SchemaId + Value)
//! - SchemaId => Int16
//! - Value    => row bytes
//!
//! Reference: `org.apache.fluss.record.DefaultValueRecordBatch` and
//! `org.apache.fluss.record.DefaultValueRecord`.

use crate::error::{Error, Result};
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use std::ops::Range;

const LENGTH_LENGTH: usize = 4;
const MAGIC_LENGTH: usize = 1;
const RECORD_COUNT_LENGTH: usize = 4;
/// Offset of the record count within the batch header.
const RECORD_COUNT_OFFSET: usize = LENGTH_LENGTH + MAGIC_LENGTH;
/// Size of the batch header (`Length + Magic + RecordCount`).
const RECORD_BATCH_HEADER_SIZE: usize = LENGTH_LENGTH + MAGIC_LENGTH + RECORD_COUNT_LENGTH;
/// Size of a `ValueRecord`'s leading length field.
const RECORD_LENGTH_LENGTH: usize = 4;

/// Read-only view over a serialized value-record batch.
pub(crate) struct ValueRecordBatch {
    data: Bytes,
}

impl ValueRecordBatch {
    /// Wraps raw batch bytes. The batch is expected to start at offset 0.
    pub(crate) fn new(data: Bytes) -> Self {
        Self { data }
    }

    /// Number of records declared in the batch header.
    pub(crate) fn record_count(&self) -> Result<i32> {
        if self.data.len() < RECORD_BATCH_HEADER_SIZE {
            return Err(corrupt(format!(
                "value-record batch too short: {} bytes, need {} for header",
                self.data.len(),
                RECORD_BATCH_HEADER_SIZE
            )));
        }
        Ok(LittleEndian::read_i32(
            &self.data[RECORD_COUNT_OFFSET..RECORD_COUNT_OFFSET + RECORD_COUNT_LENGTH],
        ))
    }

    /// Returns one byte range per record, each spanning `[SchemaId | Value]`:
    /// the payload [`crate::row::FixedSchemaDecoder::decode`] expects. Index
    /// [`Self::data`] with a returned range to get it without copying.
    pub(crate) fn value_ranges(&self) -> Result<Vec<Range<usize>>> {
        let count = self.record_count()?;
        if count < 0 {
            return Err(corrupt(format!("invalid record count {count}")));
        }
        let mut ranges = Vec::with_capacity(count as usize);
        let mut pos = RECORD_BATCH_HEADER_SIZE;
        for i in 0..count as usize {
            if pos + RECORD_LENGTH_LENGTH > self.data.len() {
                return Err(corrupt(format!(
                    "truncated value-record batch: record {i} length field runs past end"
                )));
            }
            let rec_len = LittleEndian::read_i32(&self.data[pos..pos + RECORD_LENGTH_LENGTH]);
            if rec_len < 0 {
                return Err(corrupt(format!("record {i} has negative length {rec_len}")));
            }
            let start = pos + RECORD_LENGTH_LENGTH;
            let end = start + rec_len as usize;
            if end > self.data.len() {
                return Err(corrupt(format!(
                    "truncated value-record batch: record {i} payload runs past end"
                )));
            }
            ranges.push(start..end);
            pos = end;
        }
        Ok(ranges)
    }

    /// The underlying batch bytes.
    pub(crate) fn data(&self) -> &Bytes {
        &self.data
    }
}

fn corrupt(message: String) -> Error {
    Error::UnexpectedError {
        message,
        source: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::kv::SCHEMA_ID_LENGTH;

    /// Build a value-record batch from `(schema_id, row_bytes)` pairs, mirroring
    /// the Java `DefaultValueRecordBatch.Builder` wire layout.
    fn build_batch(records: &[(i16, &[u8])]) -> Vec<u8> {
        let mut body = Vec::new();
        for (schema_id, row) in records {
            let rec_len = (SCHEMA_ID_LENGTH + row.len()) as i32;
            body.extend_from_slice(&rec_len.to_le_bytes());
            body.extend_from_slice(&schema_id.to_le_bytes());
            body.extend_from_slice(row);
        }
        let mut out = Vec::new();
        // Length covers Magic + RecordCount + body.
        let length = (MAGIC_LENGTH + RECORD_COUNT_LENGTH + body.len()) as i32;
        out.extend_from_slice(&length.to_le_bytes());
        out.push(0); // magic
        out.extend_from_slice(&(records.len() as i32).to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn parses_record_count_and_ranges() {
        let raw = build_batch(&[(7, &[1, 2, 3]), (7, &[4, 5])]);
        let batch = ValueRecordBatch::new(Bytes::from(raw));
        assert_eq!(batch.record_count().unwrap(), 2);

        let ranges = batch.value_ranges().unwrap();
        assert_eq!(ranges.len(), 2);
        // First record payload = [schema_id(2) | row(3)] = 5 bytes.
        let r0 = &batch.data()[ranges[0].clone()];
        assert_eq!(r0.len(), 5);
        assert_eq!(LittleEndian::read_i16(&r0[..2]), 7);
        assert_eq!(&r0[2..], &[1, 2, 3]);
        // Second record payload = [schema_id(2) | row(2)] = 4 bytes.
        let r1 = &batch.data()[ranges[1].clone()];
        assert_eq!(r1.len(), 4);
        assert_eq!(&r1[2..], &[4, 5]);
    }

    #[test]
    fn empty_batch_has_no_ranges() {
        let raw = build_batch(&[]);
        let batch = ValueRecordBatch::new(Bytes::from(raw));
        assert_eq!(batch.record_count().unwrap(), 0);
        assert!(batch.value_ranges().unwrap().is_empty());
    }

    #[test]
    fn truncated_payload_errors() {
        let mut raw = build_batch(&[(7, &[1, 2, 3])]);
        raw.truncate(raw.len() - 2); // chop into the row payload
        let batch = ValueRecordBatch::new(Bytes::from(raw));
        assert!(batch.value_ranges().is_err());
    }

    #[test]
    fn short_header_errors() {
        let batch = ValueRecordBatch::new(Bytes::from(vec![0u8, 1, 2]));
        assert!(batch.record_count().is_err());
    }
}
