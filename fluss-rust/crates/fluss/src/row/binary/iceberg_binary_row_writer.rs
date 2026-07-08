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

use bytes::{Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::metadata::DataType;
use crate::row::Decimal;
use crate::row::binary::{BinaryWriter, ValueWriter};
use crate::row::binary_array::FlussArray;
use crate::row::binary_map::FlussMap;

const MICROS_PER_MILLI: i64 = 1_000;

/// Iceberg-specific binary writer for encoding key columns.
///
/// Unlike [`CompactedRowWriter`] which uses varint encoding and length-prefixed
/// variable-length fields, this writer follows Iceberg's encoding conventions:
/// - Integers (int, date) are written as i64 (8 bytes, little-endian)
/// - Time values are converted from milliseconds to microseconds
/// - Timestamps are converted to microseconds
/// - Floats/doubles use fixed-width little-endian encoding
/// - Variable-length types (string, binary) are written without length prefixes
/// - Decimals are written as unscaled big-endian bytes without length prefixes
///
/// The encoded bytes feed directly into `IcebergBucketingFunction`'s MurmurHash
/// for bucket assignment and must match the Java Fluss server's encoding exactly.
///
/// [`CompactedRowWriter`]: crate::row::compacted::CompactedRowWriter
pub struct IcebergBinaryRowWriter {
    position: usize,
    buffer: BytesMut,
}

impl Default for IcebergBinaryRowWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl IcebergBinaryRowWriter {
    pub fn new() -> Self {
        let buffer = BytesMut::zeroed(64);
        Self {
            position: 0,
            buffer,
        }
    }

    // Dependency order note:
    // 1) Keep this PR scoped to writer-level Java parity.
    // 2) Wire the writer through IcebergKeyEncoder in follow-up #308.
    // TODO(#308): add end-to-end key-encoding tests via IcebergKeyEncoder
    // (similar to CompactedKeyEncoder tests for CompactedKeyWriter).
    pub fn create_value_writer(field_type: &DataType) -> Result<ValueWriter> {
        match field_type {
            // Match Java IcebergBinaryRowWriter.createFieldWriter() supported types exactly.
            DataType::Int(_)
            | DataType::Date(_)
            | DataType::Time(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::Timestamp(_)
            | DataType::Decimal(_)
            | DataType::String(_)
            | DataType::Char(_)
            | DataType::Binary(_)
            | DataType::Bytes(_) => ValueWriter::create_value_writer(field_type, None),

            // Keep Java's explicit scalar-only rejection messaging for ARRAY/MAP.
            DataType::Array(_) => Err(Error::UnsupportedOperation {
                message:
                    "Array types cannot be used as bucket keys. Bucket keys must be scalar types."
                        .to_string(),
            }),
            DataType::Map(_) => Err(Error::UnsupportedOperation {
                message:
                    "Map types cannot be used as bucket keys. Bucket keys must be scalar types."
                        .to_string(),
            }),

            // BOOLEAN, TINYINT, SMALLINT, TIMESTAMP_LTZ, ROW and any future types.
            _ => Err(Error::UnsupportedOperation {
                message: format!(
                    "Unsupported type for Iceberg binary row writer: {:?}",
                    field_type
                ),
            }),
        }
    }

    #[allow(dead_code)]
    pub fn position(&self) -> usize {
        self.position
    }

    #[allow(dead_code)]
    pub fn buffer(&self) -> &[u8] {
        &self.buffer[..self.position]
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.buffer[..self.position])
    }

    fn ensure_capacity(&mut self, need_len: usize) {
        if (self.buffer.len() - self.position) < need_len {
            let new_len = std::cmp::max(self.buffer.len() * 2, self.buffer.len() + need_len);
            self.buffer.resize(new_len, 0);
        }
    }

    fn write_raw(&mut self, src: &[u8]) {
        let end = self.position + src.len();
        self.ensure_capacity(src.len());
        self.buffer[self.position..end].copy_from_slice(src);
        self.position = end;
    }
}

impl BinaryWriter for IcebergBinaryRowWriter {
    fn reset(&mut self) {
        if self.position > 0 {
            self.buffer[..self.position].fill(0);
        }
        self.position = 0;
    }

    fn set_null_at(&mut self, _pos: usize) {
        panic!("Iceberg key columns do not support null values");
    }

    fn write_boolean(&mut self, value: bool) {
        self.write_raw(&[if value { 1u8 } else { 0u8 }]);
    }

    fn write_byte(&mut self, value: u8) {
        self.write_raw(&[value]);
    }

    fn write_bytes(&mut self, value: &[u8]) {
        // Iceberg: raw bytes, no length prefix
        self.write_raw(value);
    }

    fn write_char(&mut self, value: &str, _length: usize) {
        // Iceberg: same as string — raw UTF-8, no length prefix
        self.write_string(value);
    }

    fn write_string(&mut self, value: &str) {
        // Iceberg: raw UTF-8 bytes, no length prefix
        self.write_raw(value.as_bytes());
    }

    fn write_short(&mut self, value: i16) {
        self.write_raw(&value.to_le_bytes());
    }

    fn write_int(&mut self, value: i32) {
        // Iceberg: promote i32 to i64, write as 8 bytes little-endian
        self.write_raw(&(value as i64).to_le_bytes());
    }

    fn write_long(&mut self, value: i64) {
        self.write_raw(&value.to_le_bytes());
    }

    fn write_float(&mut self, value: f32) {
        self.write_raw(&value.to_le_bytes());
    }

    fn write_double(&mut self, value: f64) {
        self.write_raw(&value.to_le_bytes());
    }

    fn write_binary(&mut self, bytes: &[u8], length: usize) {
        // Iceberg: raw bytes, no length prefix
        self.write_raw(&bytes[..length.min(bytes.len())]);
    }

    fn write_decimal(&mut self, value: &Decimal, _precision: u32) {
        // Iceberg: unscaled big-endian bytes, no length prefix
        let unscaled_bytes = value.to_unscaled_bytes();
        self.write_raw(&unscaled_bytes);
    }

    fn write_time(&mut self, value: i32, _precision: u32) {
        // NOTE: this is the same with Java's long arithmetic wraps on overflow.
        let micros = (value as i64).wrapping_mul(MICROS_PER_MILLI);
        self.write_raw(&micros.to_le_bytes());
    }

    fn write_timestamp_ntz(&mut self, value: &crate::row::datum::TimestampNtz, _precision: u32) {
        // NOTE: this is the same with Java's long arithmetic wraps on overflow.
        let millis = value.get_millisecond();
        let nanos = value.get_nano_of_millisecond();
        let micros = millis
            .wrapping_mul(MICROS_PER_MILLI)
            .wrapping_add((nanos as i64) / MICROS_PER_MILLI);
        self.write_raw(&micros.to_le_bytes());
    }

    fn write_timestamp_ltz(&mut self, value: &crate::row::datum::TimestampLtz, _precision: u32) {
        // NOTE: this is the same with Java's long arithmetic wraps on overflow.
        let millis = value.get_epoch_millisecond();
        let nanos = value.get_nano_of_millisecond();
        let micros = millis
            .wrapping_mul(MICROS_PER_MILLI)
            .wrapping_add((nanos as i64) / MICROS_PER_MILLI);
        self.write_raw(&micros.to_le_bytes());
    }

    fn write_array(&mut self, _value: &FlussArray) {
        unreachable!("Array/Map types are rejected during value writer creation");
    }

    fn write_map(&mut self, _value: &FlussMap) {
        unreachable!("Array/Map types are rejected during value writer creation");
    }

    fn complete(&mut self) {
        // No finalization needed for Iceberg key encoding
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypes, SmallIntType, TinyIntType};
    use crate::row::datum::{TimestampLtz, TimestampNtz};
    use bigdecimal::{BigDecimal, num_bigint::BigInt};

    fn assert_unsupported_type(dt: DataType, expected_fragment: &str) {
        match IcebergBinaryRowWriter::create_value_writer(&dt) {
            Err(e) => assert!(
                e.to_string().contains(expected_fragment),
                "unexpected error for {dt:?}: {e}"
            ),
            Ok(_) => panic!("expected error for unsupported type {dt:?}, got Ok"),
        }
    }

    #[test]
    fn test_write_int_as_i64_le() {
        let mut w = IcebergBinaryRowWriter::new();
        w.write_int(42);
        assert_eq!(w.buffer(), &42i64.to_le_bytes());
    }

    #[test]
    fn test_write_int_negative() {
        let mut w = IcebergBinaryRowWriter::new();
        w.write_int(-1);
        assert_eq!(w.buffer(), &(-1i64).to_le_bytes());
    }

    #[test]
    fn test_write_long() {
        let mut w = IcebergBinaryRowWriter::new();
        w.write_long(123456789012345i64);
        assert_eq!(w.buffer(), &123456789012345i64.to_le_bytes());
    }

    #[test]
    fn test_write_float() {
        let mut w = IcebergBinaryRowWriter::new();
        let val = 1.23f32;
        w.write_float(val);
        assert_eq!(w.buffer(), &val.to_le_bytes());
    }

    #[test]
    fn test_write_double() {
        let mut w = IcebergBinaryRowWriter::new();
        let val = 9.876543210f64;
        w.write_double(val);
        assert_eq!(w.buffer(), &val.to_le_bytes());
    }

    #[test]
    fn test_write_string_no_length_prefix() {
        let mut w = IcebergBinaryRowWriter::new();
        w.write_string("hello");
        assert_eq!(w.buffer(), b"hello");
    }

    #[test]
    fn test_write_bytes_no_length_prefix() {
        let mut w = IcebergBinaryRowWriter::new();
        let data = &[0xDE, 0xAD, 0xBE, 0xEF];
        w.write_bytes(data);
        assert_eq!(w.buffer(), data);
    }

    #[test]
    fn test_write_binary_no_length_prefix() {
        let mut w = IcebergBinaryRowWriter::new();
        let data = &[1, 2, 3, 4, 5];
        w.write_binary(data, 3);
        assert_eq!(w.buffer(), &[1, 2, 3]);
    }

    #[test]
    fn test_write_time_millis_to_micros() {
        let mut w = IcebergBinaryRowWriter::new();
        // 1000 ms = 1_000_000 µs
        w.write_time(1000, 0);
        assert_eq!(w.buffer(), &1_000_000i64.to_le_bytes());
    }

    #[test]
    fn test_write_timestamp_ntz_compact() {
        let mut w = IcebergBinaryRowWriter::new();
        let ts = TimestampNtz::new(1672531200000); // 2023-01-01 00:00:00 UTC
        w.write_timestamp_ntz(&ts, 3);
        let expected_micros = 1672531200000i64 * 1000;
        assert_eq!(w.buffer(), &expected_micros.to_le_bytes());
    }

    #[test]
    fn test_write_timestamp_ntz_with_nanos() {
        let mut w = IcebergBinaryRowWriter::new();
        let ts = TimestampNtz::from_millis_nanos(1000, 500_000).unwrap();
        w.write_timestamp_ntz(&ts, 6);
        // 1000ms * 1000 + 500_000ns / 1000 = 1_000_000 + 500 = 1_000_500 µs
        assert_eq!(w.buffer(), &1_000_500i64.to_le_bytes());
    }

    #[test]
    fn test_write_timestamp_ltz() {
        let mut w = IcebergBinaryRowWriter::new();
        let ts = TimestampLtz::from_millis_nanos(2000, 300_000).unwrap();
        w.write_timestamp_ltz(&ts, 6);
        // 2000ms * 1000 + 300_000ns / 1000 = 2_000_000 + 300 = 2_000_300 µs
        assert_eq!(w.buffer(), &2_000_300i64.to_le_bytes());
    }

    #[test]
    fn test_write_timestamp_ntz_overflow_wraps_like_java() {
        let mut w = IcebergBinaryRowWriter::new();
        let ts = TimestampNtz::from_millis_nanos(i64::MAX, 999_999).unwrap();
        w.write_timestamp_ntz(&ts, 9);

        let expected = i64::MAX.wrapping_mul(MICROS_PER_MILLI).wrapping_add(999);
        assert_eq!(w.buffer(), &expected.to_le_bytes());
    }

    #[test]
    fn test_write_timestamp_ltz_overflow_wraps_like_java() {
        let mut w = IcebergBinaryRowWriter::new();
        let ts = TimestampLtz::from_millis_nanos(i64::MIN, 999_999).unwrap();
        w.write_timestamp_ltz(&ts, 9);

        let expected = i64::MIN.wrapping_mul(MICROS_PER_MILLI).wrapping_add(999);
        assert_eq!(w.buffer(), &expected.to_le_bytes());
    }

    #[test]
    fn test_write_decimal_compact() {
        let mut w = IcebergBinaryRowWriter::new();
        let bd = BigDecimal::new(BigInt::from(12345), 2); // 123.45
        let decimal = Decimal::from_big_decimal(bd, 10, 2).unwrap();
        w.write_decimal(&decimal, 10);

        let expected = BigInt::from(12345).to_signed_bytes_be();
        assert_eq!(w.buffer(), expected.as_slice());
    }

    #[test]
    fn test_write_decimal_non_compact() {
        let mut w = IcebergBinaryRowWriter::new();
        let bd = BigDecimal::new(BigInt::from(12345), 0);
        let decimal = Decimal::from_big_decimal(bd, 28, 0).unwrap();
        w.write_decimal(&decimal, 28);

        let expected = BigInt::from(12345).to_signed_bytes_be();
        assert_eq!(w.buffer(), expected.as_slice());
    }

    #[test]
    fn test_write_boolean() {
        let mut w = IcebergBinaryRowWriter::new();
        w.write_boolean(true);
        assert_eq!(w.buffer(), &[1u8]);

        w.reset();
        w.write_boolean(false);
        assert_eq!(w.buffer(), &[0u8]);
    }

    #[test]
    #[should_panic(expected = "Iceberg key columns do not support null values")]
    fn test_set_null_panics() {
        let mut w = IcebergBinaryRowWriter::new();
        w.set_null_at(0);
    }

    #[test]
    fn test_reset_clears_position() {
        let mut w = IcebergBinaryRowWriter::new();
        w.write_int(42);
        assert_eq!(w.position(), 8);
        w.reset();
        assert_eq!(w.position(), 0);
        assert_eq!(w.buffer().len(), 0);
    }

    #[test]
    fn test_to_bytes() {
        let mut w = IcebergBinaryRowWriter::new();
        w.write_string("test");
        let bytes = w.to_bytes();
        assert_eq!(bytes.as_ref(), b"test");
    }

    #[test]
    fn test_multiple_writes() {
        let mut w = IcebergBinaryRowWriter::new();
        w.write_int(1);
        w.write_string("ab");
        let buf = w.buffer().to_vec();
        // 8 bytes for int-as-i64 + 2 bytes for "ab"
        assert_eq!(buf.len(), 10);
        assert_eq!(&buf[..8], &1i64.to_le_bytes());
        assert_eq!(&buf[8..], b"ab");
    }

    #[test]
    fn test_buffer_growth() {
        let mut w = IcebergBinaryRowWriter::new();
        // Write more than 64 bytes to trigger buffer growth
        let large = vec![0xAAu8; 128];
        w.write_bytes(&large);
        assert_eq!(w.buffer(), large.as_slice());
    }

    #[test]
    fn test_create_value_writer_rejects_tinyint() {
        let dt = DataType::TinyInt(TinyIntType::new());
        match IcebergBinaryRowWriter::create_value_writer(&dt) {
            Err(e) => assert!(
                e.to_string()
                    .contains("Unsupported type for Iceberg binary row writer"),
                "unexpected error: {e}",
            ),
            Ok(_) => panic!("expected error for TinyInt, got Ok"),
        }
    }

    #[test]
    fn test_create_value_writer_rejects_smallint() {
        let dt = DataType::SmallInt(SmallIntType::new());
        match IcebergBinaryRowWriter::create_value_writer(&dt) {
            Err(e) => assert!(
                e.to_string()
                    .contains("Unsupported type for Iceberg binary row writer"),
                "unexpected error: {e}",
            ),
            Ok(_) => panic!("expected error for SmallInt, got Ok"),
        }
    }

    #[test]
    fn test_create_value_writer_rejects_boolean() {
        assert_unsupported_type(
            DataTypes::boolean(),
            "Unsupported type for Iceberg binary row writer",
        );
    }

    #[test]
    fn test_create_value_writer_rejects_timestamp_ltz() {
        assert_unsupported_type(
            DataTypes::timestamp_ltz(),
            "Unsupported type for Iceberg binary row writer",
        );
    }

    #[test]
    fn test_create_value_writer_rejects_array() {
        assert_unsupported_type(
            DataTypes::array(DataTypes::int()),
            "Array types cannot be used as bucket keys",
        );
    }

    #[test]
    fn test_create_value_writer_rejects_map() {
        assert_unsupported_type(
            DataTypes::map(DataTypes::string(), DataTypes::int()),
            "Map types cannot be used as bucket keys",
        );
    }

    #[test]
    fn test_create_value_writer_rejects_row() {
        assert_unsupported_type(
            DataTypes::row(vec![DataTypes::field("f0", DataTypes::int())]),
            "Unsupported type for Iceberg binary row writer",
        );
    }

    #[test]
    fn test_create_value_writer_accepts_java_supported_scalar_types() {
        let supported_types = vec![
            ("int", DataTypes::int()),
            ("date", DataTypes::date()),
            ("time", DataTypes::time()),
            ("bigint", DataTypes::bigint()),
            ("float", DataTypes::float()),
            ("double", DataTypes::double()),
            ("timestamp_ntz", DataTypes::timestamp()),
            ("decimal", DataTypes::decimal(10, 2)),
            ("string", DataTypes::string()),
            ("char", DataTypes::char(16)),
            ("binary", DataTypes::binary(8)),
            ("bytes", DataTypes::bytes()),
        ];

        for (name, data_type) in supported_types {
            let res = IcebergBinaryRowWriter::create_value_writer(&data_type);
            if let Err(e) = res {
                panic!("expected {name} to be supported, got error: {e}");
            }
        }
    }

    #[test]
    fn test_write_char_same_as_string() {
        let mut w1 = IcebergBinaryRowWriter::new();
        w1.write_char("hello", 10);

        let mut w2 = IcebergBinaryRowWriter::new();
        w2.write_string("hello");

        assert_eq!(w1.buffer(), w2.buffer());
    }

    #[test]
    fn test_write_date_as_int() {
        // Date encoding goes through write_int (via InnerValueWriter::Date)
        // which writes as i64 LE in Iceberg encoding
        let mut w = IcebergBinaryRowWriter::new();
        let days_since_epoch = 19000i32; // ~2022-01-06
        w.write_int(days_since_epoch);
        assert_eq!(w.buffer(), &(days_since_epoch as i64).to_le_bytes());
    }
}
