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

//! Row decoder for deserializing binary row formats.
//!
//! Mirrors the Java org.apache.fluss.row.decode package.

use crate::error::{Error, Result};
use crate::metadata::{KvFormat, RowType};
use crate::row::compacted::{CompactedRow, CompactedRowDeserializer};
use std::sync::Arc;

/// Decoder for creating BinaryRow from bytes.
///
/// This trait provides an abstraction for decoding different row formats
/// (COMPACTED, INDEXED, etc.) from binary data.
///
/// Reference: org.apache.fluss.row.decode.RowDecoder
pub trait RowDecoder: Send + Sync {
    /// Decode bytes into a CompactedRow.
    ///
    /// The lifetime 'a ties the returned row to the input data, ensuring
    /// the data remains valid as long as the row is used.
    fn decode<'a>(&self, data: &'a [u8]) -> CompactedRow<'a>;
}

/// Decoder for CompactedRow format.
///
/// Uses the existing CompactedRow infrastructure for decoding.
/// This is a thin wrapper that implements the RowDecoder trait.
///
/// Reference: org.apache.fluss.row.decode.CompactedRowDecoder
pub struct CompactedRowDecoder {
    field_count: usize,
    deserializer: Arc<CompactedRowDeserializer<'static>>,
}

impl CompactedRowDecoder {
    /// Create a new CompactedRowDecoder with the given row type.
    pub fn new(row_type: RowType) -> Self {
        let field_count = row_type.fields().len();
        let deserializer = Arc::new(CompactedRowDeserializer::new_from_owned(row_type));

        Self {
            field_count,
            deserializer,
        }
    }
}

impl RowDecoder for CompactedRowDecoder {
    fn decode<'a>(&self, data: &'a [u8]) -> CompactedRow<'a> {
        // Use existing CompactedRow::deserialize() infrastructure
        CompactedRow::deserialize(Arc::clone(&self.deserializer), self.field_count, data)
    }
}

/// Factory for creating RowDecoders based on KvFormat.
///
/// Reference: org.apache.fluss.row.decode.RowDecoder.create()
pub struct RowDecoderFactory;

impl RowDecoderFactory {
    /// Create a RowDecoder for the given format and row type.
    pub fn create(kv_format: KvFormat, row_type: RowType) -> Result<Arc<dyn RowDecoder>> {
        match kv_format {
            KvFormat::COMPACTED => Ok(Arc::new(CompactedRowDecoder::new(row_type))),
            KvFormat::INDEXED => Err(Error::UnsupportedOperation {
                message: "INDEXED format is not yet supported".to_string(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::row::InternalRow;
    use crate::row::binary::BinaryWriter;
    use crate::row::compacted::CompactedRowWriter;

    #[test]
    fn test_compacted_row_decoder() {
        // Write a CompactedRow
        let mut writer = CompactedRowWriter::new(2);
        writer.write_int(42);
        writer.write_string("hello");

        let data = writer.to_bytes();

        // Create decoder with RowType
        let row_type = RowType::with_data_types(vec![DataTypes::int(), DataTypes::string()]);
        let decoder = CompactedRowDecoder::new(row_type);

        // Decode
        let row = decoder.decode(&data);

        // Verify
        assert_eq!(row.get_field_count(), 2);
        assert_eq!(row.get_int(0).unwrap(), 42);
        assert_eq!(row.get_string(1).unwrap(), "hello");
    }

    #[test]
    fn test_row_decoder_factory() {
        let row_type = RowType::with_data_types(vec![DataTypes::int(), DataTypes::string()]);
        let decoder = RowDecoderFactory::create(KvFormat::COMPACTED, row_type).unwrap();

        // Write a row
        let mut writer = CompactedRowWriter::new(2);
        writer.write_int(100);
        writer.write_string("world");
        let data = writer.to_bytes();

        // Decode
        let row = decoder.decode(&data);

        // Verify
        assert_eq!(row.get_int(0).unwrap(), 100);
        assert_eq!(row.get_string(1).unwrap(), "world");
    }
}
