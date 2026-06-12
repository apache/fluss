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

//! Decode a `[schema_id (2 bytes) | row]` value into an [`InternalRow`]
//! conforming to a fixed target schema, projecting across schema
//! versions when needed.

use crate::error::{Error, Result};
use crate::metadata::{KvFormat, Schema, index_mapping};
use crate::record::kv::SCHEMA_ID_LENGTH;
use crate::row::{LookupRow, ProjectedRow, RowDecoder, RowDecoderFactory};
use std::sync::Arc;

pub(crate) struct FixedSchemaDecoder {
    row_decoder: Arc<dyn RowDecoder>,
    index_mapping: Option<Arc<[i32]>>,
}

impl FixedSchemaDecoder {
    pub fn new_no_projection(kv_format: KvFormat, schema: &Schema) -> Result<Self> {
        let row_decoder = RowDecoderFactory::create(kv_format, schema.row_type().clone())?;
        Ok(Self {
            row_decoder,
            index_mapping: None,
        })
    }

    pub fn new(
        kv_format: KvFormat,
        source_schema: &Schema,
        target_schema: &Schema,
    ) -> Result<Self> {
        let mapping = index_mapping(source_schema, target_schema)?;
        let row_decoder = RowDecoderFactory::create(kv_format, source_schema.row_type().clone())?;
        Ok(Self {
            row_decoder,
            index_mapping: Some(Arc::from(mapping.into_boxed_slice())),
        })
    }

    pub fn decode<'a>(&self, value_bytes: &'a [u8]) -> Result<LookupRow<'a>> {
        let payload =
            value_bytes
                .get(SCHEMA_ID_LENGTH..)
                .ok_or_else(|| Error::RowConvertError {
                    message: format!(
                        "Row payload too short: {} bytes, need at least {} for schema id",
                        value_bytes.len(),
                        SCHEMA_ID_LENGTH
                    ),
                })?;
        let row = self.row_decoder.decode(payload);
        match &self.index_mapping {
            None => Ok(LookupRow::raw(row)),
            Some(mapping) => Ok(LookupRow::projected(ProjectedRow::new(
                row,
                Arc::clone(mapping),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Column, DataTypes, Schema};
    use crate::record::kv::SCHEMA_ID_LENGTH;
    use crate::row::InternalRow;
    use crate::row::binary::BinaryWriter;
    use crate::row::compacted::CompactedRowWriter;

    fn schema_with_ids(columns: &[(i32, &str, crate::metadata::DataType)]) -> Schema {
        let cols: Vec<Column> = columns
            .iter()
            .map(|(id, name, dt)| Column::new(*name, dt.clone()).with_id(*id))
            .collect();
        Schema::builder().with_columns(cols).build().unwrap()
    }

    fn write_value(schema_id: i16, writer: CompactedRowWriter) -> Vec<u8> {
        let row_bytes = writer.to_bytes();
        let mut out = Vec::with_capacity(SCHEMA_ID_LENGTH + row_bytes.len());
        out.extend_from_slice(&schema_id.to_le_bytes());
        out.extend_from_slice(row_bytes.as_ref());
        out
    }

    #[test]
    fn decode_no_projection_strips_schema_id_and_returns_row() {
        let schema = schema_with_ids(&[(0, "a", DataTypes::int()), (1, "b", DataTypes::string())]);
        let decoder = FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &schema).unwrap();

        let mut writer = CompactedRowWriter::new(2);
        writer.write_int(42);
        writer.write_string("hi");
        let value = write_value(7, writer);

        let row = decoder.decode(&value).unwrap();
        assert_eq!(row.get_field_count(), 2);
        assert_eq!(row.get_int(0).unwrap(), 42);
        assert_eq!(row.get_string(1).unwrap(), "hi");
    }

    #[test]
    fn decode_with_projection_pads_missing_field_with_null() {
        // Source schema (older): [a:int, b:string]
        let source = schema_with_ids(&[(0, "a", DataTypes::int()), (1, "b", DataTypes::string())]);
        // Target schema (newer): added column c at id=2
        let target = schema_with_ids(&[
            (0, "a", DataTypes::int()),
            (1, "b", DataTypes::string()),
            (2, "c", DataTypes::bigint()),
        ]);
        let decoder = FixedSchemaDecoder::new(KvFormat::COMPACTED, &source, &target).unwrap();

        let mut writer = CompactedRowWriter::new(2);
        writer.write_int(7);
        writer.write_string("seven");
        let value = write_value(0, writer);

        let row = decoder.decode(&value).unwrap();
        assert_eq!(row.get_field_count(), 3);
        assert_eq!(row.get_int(0).unwrap(), 7);
        assert_eq!(row.get_string(1).unwrap(), "seven");
        assert!(
            row.is_null_at(2).unwrap(),
            "added-but-missing column must read as null"
        );
    }

    #[test]
    fn decode_with_projection_drops_removed_field() {
        // Source schema (older): [a, b, c]
        let source = schema_with_ids(&[
            (0, "a", DataTypes::int()),
            (1, "b", DataTypes::string()),
            (2, "c", DataTypes::bigint()),
        ]);
        // Target schema (newer): dropped b
        let target = schema_with_ids(&[(0, "a", DataTypes::int()), (2, "c", DataTypes::bigint())]);
        let decoder = FixedSchemaDecoder::new(KvFormat::COMPACTED, &source, &target).unwrap();

        let mut writer = CompactedRowWriter::new(3);
        writer.write_int(1);
        writer.write_string("dropped");
        writer.write_long(99);
        let value = write_value(0, writer);

        let row = decoder.decode(&value).unwrap();
        assert_eq!(row.get_field_count(), 2);
        assert_eq!(row.get_int(0).unwrap(), 1);
        assert_eq!(row.get_long(1).unwrap(), 99);
    }

    #[test]
    fn decode_with_projection_reorders_fields() {
        let source = schema_with_ids(&[(0, "a", DataTypes::int()), (1, "b", DataTypes::string())]);
        // Target reorders: b first, then a.
        let target = schema_with_ids(&[(1, "b", DataTypes::string()), (0, "a", DataTypes::int())]);
        let decoder = FixedSchemaDecoder::new(KvFormat::COMPACTED, &source, &target).unwrap();

        let mut writer = CompactedRowWriter::new(2);
        writer.write_int(123);
        writer.write_string("xyz");
        let value = write_value(0, writer);

        let row = decoder.decode(&value).unwrap();
        assert_eq!(row.get_string(0).unwrap(), "xyz");
        assert_eq!(row.get_int(1).unwrap(), 123);
    }

    #[test]
    fn decode_payload_too_short_errors() {
        let schema = schema_with_ids(&[(0, "a", DataTypes::int())]);
        let decoder = FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &schema).unwrap();
        // Only 1 byte — short of the schema id.
        match decoder.decode(&[0u8]) {
            Ok(_) => panic!("expected error for short payload"),
            Err(e) => assert!(e.to_string().contains("too short"), "got: {e}"),
        }
    }
}
