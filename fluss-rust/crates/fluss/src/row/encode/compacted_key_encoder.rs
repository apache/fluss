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

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::RowType;
use crate::row::binary::ValueWriter;
use crate::row::compacted::CompactedKeyWriter;
use crate::row::encode::KeyEncoder;
use crate::row::field_getter::FieldGetter;
use crate::row::{Datum, InternalRow};
use bytes::Bytes;

#[allow(dead_code)]
pub struct CompactedKeyEncoder {
    field_getters: Vec<FieldGetter>,
    field_encoders: Vec<ValueWriter>,
    compacted_encoder: CompactedKeyWriter,
}

impl CompactedKeyEncoder {
    /// Create a key encoder to encode the key of the input row.
    ///
    /// # Arguments
    /// * `row_type` - the row type of the input row
    /// * `keys` - the key fields to encode
    ///
    /// # Returns
    /// * key_encoder - the [`KeyEncoder`]
    pub fn create_key_encoder(row_type: &RowType, keys: &[String]) -> Result<CompactedKeyEncoder> {
        let mut encode_col_indexes = Vec::with_capacity(keys.len());

        for key in keys {
            match row_type.get_field_index(key) {
                Some(idx) => encode_col_indexes.push(idx),
                None => {
                    return Err(IllegalArgument {
                        message: format!("Field {key:?} not found in input row type {row_type:?}"),
                    });
                }
            }
        }

        Self::new(row_type, encode_col_indexes)
    }

    pub fn new(row_type: &RowType, encode_field_pos: Vec<usize>) -> Result<CompactedKeyEncoder> {
        let mut field_getters: Vec<FieldGetter> = Vec::with_capacity(encode_field_pos.len());
        let mut field_encoders: Vec<ValueWriter> = Vec::with_capacity(encode_field_pos.len());

        for pos in &encode_field_pos {
            let data_type = row_type.fields().get(*pos).unwrap().data_type();
            // Validate key type support first, so unsupported types return a
            // typed error instead of panicking in FieldGetter::create.
            let field_encoder = CompactedKeyWriter::create_value_writer(data_type)?;
            let field_getter = FieldGetter::create(data_type, *pos);
            field_getters.push(field_getter);
            field_encoders.push(field_encoder);
        }

        Ok(CompactedKeyEncoder {
            field_encoders,
            field_getters,
            compacted_encoder: CompactedKeyWriter::new(),
        })
    }
}

#[allow(dead_code)]
impl KeyEncoder for CompactedKeyEncoder {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes> {
        self.compacted_encoder.reset();

        // iterate all the fields of the row, and encode each field
        for (pos, (field_getter, field_encoder)) in self
            .field_getters
            .iter()
            .zip(self.field_encoders.iter())
            .enumerate()
        {
            match &field_getter.get_field(row)? {
                Datum::Null => {
                    return Err(IllegalArgument {
                        message: format!("Cannot encode key with null value at position: {pos:?}"),
                    });
                }
                value => field_encoder.write_value(&mut self.compacted_encoder, pos, value)?,
            }
        }

        Ok(self.compacted_encoder.to_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataType, DataTypes};
    use crate::row::binary_array::FlussArrayWriter;
    use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
    use crate::row::{Datum, Decimal, FlussArray, GenericRow};

    fn build_int_array(values: &[i32]) -> FlussArray {
        let mut w = FlussArrayWriter::new(values.len(), &DataTypes::int());
        for (i, v) in values.iter().enumerate() {
            w.write_int(i, *v);
        }
        w.complete().unwrap()
    }

    fn build_nullable_int_array(values: &[Option<i32>]) -> FlussArray {
        let mut w = FlussArrayWriter::new(values.len(), &DataTypes::int());
        for (i, v) in values.iter().enumerate() {
            match v {
                Some(value) => w.write_int(i, *value),
                None => w.set_null_at(i),
            }
        }
        w.complete().unwrap()
    }

    fn build_float_array(values: &[f32]) -> FlussArray {
        let mut w = FlussArrayWriter::new(values.len(), &DataTypes::float().as_non_nullable());
        for (i, v) in values.iter().enumerate() {
            w.write_float(i, *v);
        }
        w.complete().unwrap()
    }

    fn build_nested_string_array() -> FlussArray {
        let mut inner_1 = FlussArrayWriter::new(3, &DataTypes::string());
        inner_1.write_string(0, "a");
        inner_1.set_null_at(1);
        inner_1.write_string(2, "c");
        let inner_1 = inner_1.complete().unwrap();

        let mut inner_2 = FlussArrayWriter::new(2, &DataTypes::string());
        inner_2.write_string(0, "hello");
        inner_2.write_string(1, "world");
        let inner_2 = inner_2.complete().unwrap();

        let mut outer = FlussArrayWriter::new(3, &DataTypes::array(DataTypes::string()));
        outer.write_array(0, &inner_1);
        outer.set_null_at(1);
        outer.write_array(2, &inner_2);
        outer.complete().unwrap()
    }

    pub fn for_test_row_type(row_type: &RowType) -> CompactedKeyEncoder {
        CompactedKeyEncoder::new(row_type, (0..row_type.fields().len()).collect())
            .expect("CompactedKeyEncoder initialization failed")
    }

    #[test]
    fn test_encode_map_rejected() {
        let row_type =
            RowType::with_data_types(vec![DataTypes::map(DataTypes::string(), DataTypes::int())]);

        let res = CompactedKeyEncoder::new(&row_type, vec![0]);
        assert!(res.is_err());
        if let Err(e) = res {
            assert!(
                e.to_string().contains("Cannot use Map"),
                "Expected error to contain 'Cannot use Map', got '{}'",
                e
            );
        }
    }

    #[test]
    fn test_encode_key() {
        let row_type = RowType::with_data_types(vec![
            DataTypes::int(),
            DataTypes::bigint(),
            DataTypes::int(),
        ]);
        let row = GenericRow::from_data(vec![
            Datum::from(1i32),
            Datum::from(3i64),
            Datum::from(2i32),
        ]);

        let mut encoder = for_test_row_type(&row_type);

        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            [1u8, 3u8, 2u8]
        );

        let row = GenericRow::from_data(vec![
            Datum::from(2i32),
            Datum::from(5i64),
            Datum::from(6i32),
        ]);

        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            [2u8, 5u8, 6u8]
        );
    }

    #[test]
    fn test_encode_key_with_key_names() {
        let data_types = vec![
            DataTypes::string(),
            DataTypes::bigint(),
            DataTypes::string(),
        ];
        let field_names = vec!["partition", "f1", "f2"];

        let row_type = RowType::with_data_types_and_field_names(data_types, field_names);

        let primary_keys = &["f2".to_string()];

        let mut encoder = CompactedKeyEncoder::create_key_encoder(&row_type, primary_keys).unwrap();

        let row = GenericRow::from_data(vec![
            Datum::from("p1"),
            Datum::from(1i64),
            Datum::from("a2"),
        ]);

        // should only get "a2" 's ASCII representation
        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            //  2 (start of text), 97 (the letter a), 50 (the number 2)
            [2u8, 97u8, 50u8]
        );
    }

    #[test]
    #[should_panic(expected = "Cannot encode key with null value at position: 2")]
    fn test_null_primary_key() {
        let row_type = RowType::with_data_types(vec![
            DataTypes::int(),
            DataTypes::bigint(),
            DataTypes::int(),
            DataTypes::string(),
        ]);

        let primary_key_indices = vec![0, 1, 2];

        let mut encoder = CompactedKeyEncoder::new(&row_type, primary_key_indices)
            .expect("CompactedKeyEncoder initialization failed");

        let row = GenericRow::from_data(vec![
            Datum::from(1i32),
            Datum::from(3i64),
            Datum::from(2i32),
            Datum::from("a2"),
        ]);

        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            [1u8, 3u8, 2u8]
        );

        let row = GenericRow::from_data(vec![
            Datum::from(1i32),
            Datum::from(3i64),
            Datum::Null,
            Datum::from("a2"),
        ]);

        encoder.encode_key(&row).unwrap();
    }

    #[test]
    fn test_int_string_as_primary_key() {
        let row_type = RowType::with_data_types(vec![
            DataTypes::string(),
            DataTypes::int(),
            DataTypes::string(),
            DataTypes::string(),
        ]);

        let primary_key_indices = vec![1, 2];
        let mut encoder = CompactedKeyEncoder::new(&row_type, primary_key_indices)
            .expect("CompactedKeyEncoder initialization failed");

        let row = GenericRow::from_data(vec![
            Datum::from("a1"),
            Datum::from(1i32),
            Datum::from("a2"),
            Datum::from("a3"),
        ]);

        assert_eq!(
            encoder.encode_key(&row).unwrap().iter().as_slice(),
            // 1 (1i32), 2 (start of text), 97 (the letter a), 50 (the number 2)
            [1u8, 2u8, 97u8, 50u8]
        );
    }

    #[test]
    fn test_array_type_allowed_as_key() {
        // Java's CompactedKeyEncoder allows Array as a key column type
        // (the server rejects unsupported key types at table-creation time).
        let row_type =
            RowType::with_data_types(vec![DataTypes::int(), DataTypes::array(DataTypes::int())]);
        let mut encoder = CompactedKeyEncoder::new(&row_type, vec![0, 1]).unwrap();

        let row_a = GenericRow::from_data(vec![
            Datum::Int32(42),
            Datum::Array(build_int_array(&[10, 20])),
        ]);
        let row_b = GenericRow::from_data(vec![
            Datum::Int32(42),
            Datum::Array(build_int_array(&[10, 30])),
        ]);

        let encoded_a = encoder.encode_key(&row_a).unwrap();
        let encoded_b = encoder.encode_key(&row_b).unwrap();

        assert!(!encoded_a.is_empty());
        assert_ne!(
            encoded_a.iter().as_slice(),
            encoded_b.iter().as_slice(),
            "Array key payload should affect compacted key encoding"
        );
    }

    #[test]
    fn test_map_type_rejected_as_key() {
        let row_type = RowType::with_data_types(vec![
            DataTypes::int(),
            DataTypes::map(DataTypes::int(), DataTypes::string()),
        ]);
        match CompactedKeyEncoder::new(&row_type, vec![0, 1]) {
            Ok(_) => panic!("Expected error when using Map as key type"),
            Err(err) => {
                assert!(
                    err.to_string().contains("Cannot use"),
                    "Expected 'Cannot use' error, got: {err}"
                );
            }
        }
    }

    #[test]
    fn test_all_data_types_java_compatible() {
        // Test encoding compatibility with Java using reference from:
        // https://github.com/apache/fluss/blob/main/fluss-common/src/test/resources/encoding/encoded_key.hex
        use crate::metadata::{DataType, TimestampLTzType, TimestampType};

        let row_type = RowType::with_data_types(vec![
            DataTypes::boolean(),                                                 // BOOLEAN
            DataTypes::tinyint(),                                                 // TINYINT
            DataTypes::smallint(),                                                // SMALLINT
            DataTypes::int(),                                                     // INT
            DataTypes::bigint(),                                                  // BIGINT
            DataTypes::float(),                                                   // FLOAT
            DataTypes::double(),                                                  // DOUBLE
            DataTypes::date(),                                                    // DATE
            DataTypes::time(),                                                    // TIME
            DataTypes::binary(20),                                                // BINARY(20)
            DataTypes::bytes(),                                                   // BYTES
            DataTypes::char(2),                                                   // CHAR(2)
            DataTypes::string(),                                                  // STRING
            DataTypes::decimal(5, 2),                                             // DECIMAL(5,2)
            DataTypes::decimal(20, 0),                                            // DECIMAL(20,0)
            DataType::Timestamp(TimestampType::with_nullable(false, 1).unwrap()), // TIMESTAMP(1)
            DataType::Timestamp(TimestampType::with_nullable(false, 5).unwrap()), // TIMESTAMP(5)
            DataType::TimestampLTz(TimestampLTzType::with_nullable(false, 1).unwrap()), // TIMESTAMP_LTZ(1)
            DataType::TimestampLTz(TimestampLTzType::with_nullable(false, 5).unwrap()), // TIMESTAMP_LTZ(5)
            DataTypes::array(DataTypes::int()), // ARRAY<INT>
            DataTypes::array(DataTypes::float().as_non_nullable()), // ARRAY<FLOAT NOT NULL>
            DataTypes::array(DataTypes::array(DataTypes::string())), // ARRAY<ARRAY<STRING>>
                                                // Note: MAP is rejected as a key type (see test_encode_map_rejected)
                                                // TODO: Add support for ROW type
        ]);

        // Exact values from Java's IndexedRowTest.genRecordForAllTypes()
        let row = GenericRow::from_data(vec![
            Datum::from(true),                                             // BOOLEAN: true
            Datum::from(2i8),                                              // TINYINT: 2
            Datum::from(10i16),                                            // SMALLINT: 10
            Datum::from(100i32),                                           // INT: 100
            Datum::from(-6101065172474983726i64),                          // BIGINT
            Datum::from(13.2f32),                                          // FLOAT: 13.2
            Datum::from(15.21f64),                                         // DOUBLE: 15.21
            Datum::Date(Date::new(19655)), // DATE: 2023-10-25 (19655 days since epoch)
            Datum::Time(Time::new(34200000)), // TIME: 09:30:00.0
            Datum::from("1234567890".as_bytes()), // BINARY(20)
            Datum::from("20".as_bytes()),  // BYTES
            Datum::from("1"),              // CHAR(2): "1"
            Datum::from("hello"),          // STRING: "hello"
            Datum::Decimal(Decimal::from_unscaled_long(9, 5, 2).unwrap()), // DECIMAL(5,2)
            Datum::Decimal(
                Decimal::from_big_decimal(
                    bigdecimal::BigDecimal::new(bigdecimal::num_bigint::BigInt::from(10), 0),
                    20,
                    0,
                )
                .unwrap(),
            ), // DECIMAL(20,0)
            Datum::TimestampNtz(TimestampNtz::new(1698235273182)), // TIMESTAMP(1)
            Datum::TimestampNtz(TimestampNtz::new(1698235273182)), // TIMESTAMP(5)
            Datum::TimestampLtz(TimestampLtz::new(1698235273182)), // TIMESTAMP_LTZ(1)
            Datum::TimestampLtz(TimestampLtz::new(1698235273182)), // TIMESTAMP_LTZ(5)
            Datum::Array(build_nullable_int_array(&[
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(-11),
                None,
                Some(444),
                Some(102234),
            ])), // ARRAY<INT>: GenericArray.of(1, 2, 3, 4, 5, -11, null, 444, 102234)
            Datum::Array(build_float_array(&[
                0.1_f32,
                1.1_f32,
                -0.5_f32,
                6.6_f32,
                f32::MAX,
                f32::from_bits(1),
            ])), // ARRAY<FLOAT NOT NULL>: GenericArray.of(0.1f, 1.1f, -0.5f, 6.6f, MAX, MIN)
            Datum::Array(build_nested_string_array()), // ARRAY<ARRAY<STRING>>
        ]);

        // Expected bytes from Java's encoded_key.hex reference file
        #[rustfmt::skip]
        let expected: Vec<u8> = vec![
            // BOOLEAN: true
            0x01,
            // TINYINT: 2
            0x02,
            // SMALLINT: 10 (varint encoded)
            0x0A,
            // INT: 100 (varint encoded)
            0x00, 0x64,
            // BIGINT: -6101065172474983726
            0xD2, 0x95, 0xFC, 0xD8, 0xCE, 0xB1, 0xAA, 0xAA, 0xAB, 0x01,
            // FLOAT: 13.2
            0x33, 0x33, 0x53, 0x41,
            // DOUBLE: 15.21
            0xEC, 0x51, 0xB8, 0x1E, 0x85, 0x6B, 0x2E, 0x40,
            // DATE: 2023-10-25
            0xC7, 0x99, 0x01,
            // TIME: 09:30:00.0
            0xC0, 0xB3, 0xA7, 0x10,
            // BINARY(20): "1234567890"
            0x0A, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x30,
            // BYTES: "20"
            0x02, 0x32, 0x30,
            // CHAR(2): "1"
            0x01, 0x31,
            // STRING: "hello"
            0x05, 0x68, 0x65, 0x6C, 0x6C, 0x6F,
            // DECIMAL(5,2): 9
            0x09,
            // DECIMAL(20,0): 10
            0x01, 0x0A,
            // TIMESTAMP(1): 1698235273182
            0xDE, 0x9F, 0xD7, 0xB5, 0xB6, 0x31,
            // TIMESTAMP(5): 1698235273182
            0xDE, 0x9F, 0xD7, 0xB5, 0xB6, 0x31, 0x00,
            // TIMESTAMP_LTZ(1): 1698235273182
            0xDE, 0x9F, 0xD7, 0xB5, 0xB6, 0x31,
            // TIMESTAMP_LTZ(5): 1698235273182
            0xDE, 0x9F, 0xD7, 0xB5, 0xB6, 0x31, 0x00,
            // ARRAY<INT>: GenericArray.of(1, 2, 3, 4, 5, -11, null, 444, 102234)
            0x30, 0x09, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
            0x00, 0x05, 0x00, 0x00, 0x00, 0xF5, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00,
            0x00, 0xBC, 0x01, 0x00, 0x00, 0x5A, 0x8F, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00,
            // ARRAY<FLOAT NOT NULL>: GenericArray.of(0.1f, 1.1f, -0.5f, 6.6f, MAX, MIN)
            0x20, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xCD, 0xCC, 0xCC,
            0x3D, 0xCD, 0xCC, 0x8C, 0x3F, 0x00, 0x00, 0x00, 0xBF, 0x33, 0x33, 0xD3,
            0x40, 0xFF, 0xFF, 0x7F, 0x7F, 0x01, 0x00, 0x00, 0x00,
            // ARRAY<ARRAY<STRING>>
            0x58, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00,
            0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x18, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00, 0x00, 0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x81, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x81, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00, 0x00, 0x85, 0x77, 0x6F, 0x72,
            0x6C, 0x64, 0x00, 0x00, 0x85,
        ];

        let mut encoder = for_test_row_type(&row_type);
        let encoded = encoder.encode_key(&row).unwrap();

        // Assert byte-for-byte compatibility with Java's encoded_key.hex
        assert_eq!(
            encoded.iter().as_slice(),
            expected.as_slice(),
            "\n\nRust encoding does not match Java reference from encoded_key.hex\n\
             Expected: {:02X?}\n\
             Actual:   {:02X?}\n",
            expected,
            encoded.iter().as_slice()
        );
    }

    #[test]
    fn test_row_as_primary_key() {
        // ROW<INT, STRING> as a primary key column
        let inner_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["x", "label"],
        );
        let row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataType::Row(inner_row_type.clone())],
            vec!["id", "nested"],
        );

        let mut inner = GenericRow::new(2);
        inner.set_field(0, 42_i32);
        inner.set_field(1, "hello");

        let mut row = GenericRow::new(2);
        row.set_field(0, 1_i32);
        row.set_field(1, Datum::Row(Box::new(inner)));

        let mut encoder = for_test_row_type(&row_type);
        let encoded = encoder.encode_key(&row).unwrap();

        // Verify it encodes without error and produces non-empty bytes
        assert!(!encoded.is_empty());

        // Encode the same row again to verify determinism
        let encoded2 = encoder.encode_key(&row).unwrap();
        assert_eq!(encoded, encoded2);

        // Encode a different nested row and verify different output
        let mut inner2 = GenericRow::new(2);
        inner2.set_field(0, 99_i32);
        inner2.set_field(1, "world");

        let mut row2 = GenericRow::new(2);
        row2.set_field(0, 1_i32);
        row2.set_field(1, Datum::Row(Box::new(inner2)));

        let encoded3 = encoder.encode_key(&row2).unwrap();
        assert_ne!(encoded, encoded3);
    }
}
