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

use bytes::Bytes;

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::RowType;
use crate::row::binary::ValueWriter;
use crate::row::encode::KeyEncoder;
use crate::row::field_getter::FieldGetter;
use crate::row::paimon::PaimonBinaryRowWriter;
use crate::row::{Datum, InternalRow};

/// Rust port of Java's `org.apache.fluss.row.encode.paimon.PaimonKeyEncoder`.
///
/// Encodes a set of key columns of a Fluss `InternalRow` into Paimon's
/// BinaryRow layout via [`PaimonBinaryRowWriter`]. The key's row type is a
/// projection of the original `row_type` to just the `keys` (same column order
/// as `keys`), and each key column is written into its projection position.
pub struct PaimonKeyEncoder {
    field_getters: Vec<FieldGetter>,
    field_encoders: Vec<ValueWriter>,
    writer: PaimonBinaryRowWriter,
}

impl PaimonKeyEncoder {
    /// Construct a Paimon key encoder for `keys` drawn from `row_type`.
    pub fn new(row_type: &RowType, keys: &[String]) -> Result<Self> {
        let mut field_getters: Vec<FieldGetter> = Vec::with_capacity(keys.len());
        let mut field_encoders: Vec<ValueWriter> = Vec::with_capacity(keys.len());

        for key in keys {
            let idx = row_type
                .get_field_index(key)
                .ok_or_else(|| IllegalArgument {
                    message: format!("Field {key:?} not found in input row type {row_type:?}"),
                })?;
            let data_type = row_type.fields()[idx].data_type();
            // Validate Paimon-supported field type (rejects ARRAY/MAP/ROW).
            let value_writer = PaimonBinaryRowWriter::create_value_writer(data_type)?;
            field_getters.push(FieldGetter::create(data_type, idx));
            field_encoders.push(value_writer);
        }

        Ok(Self {
            writer: PaimonBinaryRowWriter::new(keys.len()),
            field_getters,
            field_encoders,
        })
    }
}

impl KeyEncoder for PaimonKeyEncoder {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes> {
        use crate::row::binary::BinaryWriter;

        self.writer.reset();
        self.writer.write_change_type_insert();

        for (proj_pos, (getter, encoder)) in self
            .field_getters
            .iter()
            .zip(self.field_encoders.iter())
            .enumerate()
        {
            let datum: Datum = getter.get_field(row)?;
            encoder.write_value(&mut self.writer, proj_pos, &datum)?;
        }

        Ok(self.writer.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::row::{Datum, GenericRow};

    #[test]
    fn encode_single_int_key_has_insert_header() {
        let row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["pk", "other"],
        );
        let mut encoder =
            PaimonKeyEncoder::new(&row_type, &["pk".to_string()]).expect("construct encoder");

        let row = GenericRow::from_data(vec![Datum::from(42i32), Datum::from("hi")]);
        let bytes = encoder.encode_key(&row).unwrap();

        // 8 bytes null-bits + 8 bytes field slot
        assert_eq!(bytes.len(), 16);
        // change-type INSERT = 0 at byte 0
        assert_eq!(bytes[0], 0);
        // int 42 in little-endian in the field slot
        assert_eq!(&bytes[8..12], &42_i32.to_le_bytes());
    }

    #[test]
    fn encode_multi_field_key() {
        let row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::string(), DataTypes::int(), DataTypes::string()],
            vec!["other", "pk1", "pk2"],
        );
        let mut encoder = PaimonKeyEncoder::new(&row_type, &["pk1".to_string(), "pk2".to_string()])
            .expect("construct encoder");

        let row = GenericRow::from_data(vec![
            Datum::from("ignored"),
            Datum::from(7i32),
            Datum::from("hi"),
        ]);
        let bytes = encoder.encode_key(&row).unwrap();

        // 8 bytes null-bits + 2 * 8 bytes field slots = 24
        assert_eq!(bytes.len(), 24);
        // slot 0 -> int 7
        assert_eq!(&bytes[8..12], &7_i32.to_le_bytes());
        // slot 1 -> "hi" inlined; bytes[16..18] = "hi", bytes[23] = 0x82
        assert_eq!(&bytes[16..18], b"hi");
        assert_eq!(bytes[23], 0x82);
    }

    #[test]
    fn encode_reuses_buffer_across_rows() {
        let row_type = RowType::with_data_types_and_field_names(vec![DataTypes::int()], vec!["pk"]);
        let mut encoder = PaimonKeyEncoder::new(&row_type, &["pk".to_string()]).unwrap();

        let row_a = GenericRow::from_data(vec![Datum::from(1i32)]);
        let row_b = GenericRow::from_data(vec![Datum::from(1i32)]);
        let a = encoder.encode_key(&row_a).unwrap().to_vec();
        let b = encoder.encode_key(&row_b).unwrap().to_vec();
        assert_eq!(a, b);
    }

    #[test]
    fn encode_missing_key_field_errors() {
        let row_type = RowType::with_data_types_and_field_names(vec![DataTypes::int()], vec!["pk"]);
        let res = PaimonKeyEncoder::new(&row_type, &["missing".to_string()]);
        let err = match res {
            Ok(_) => panic!("expected IllegalArgument"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("not found in input row type"));
    }
}
