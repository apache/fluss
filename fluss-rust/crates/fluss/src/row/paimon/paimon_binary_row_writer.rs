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

use crate::error::{Error, Result};
use crate::metadata::DataType;
use crate::row::binary::encoding::{
    append_non_compact_decimal, append_non_compact_timestamp, pack_or_append_bytes,
};
use crate::row::binary::{BinaryWriter, ValueWriter};
use crate::row::datum::{TimestampLtz, TimestampNtz};
use crate::row::{Decimal, FlussArray, FlussMap};

/// Header size in bits (for the ChangeType byte at the front of the null bitset).
const HEADER_SIZE_IN_BITS: usize = 8;

/// A Rust port of Java's
/// `org.apache.fluss.row.encode.paimon.PaimonBinaryRowWriter`, encoding a Fluss
/// `InternalRow` using Paimon's BinaryRow layout.
///
/// Layout:
/// - 1-byte ChangeType header at offset 0 (always `INSERT = 0` for key encoding).
/// - Null bitset (`nullBitsSizeInBytes` bytes), where bit `pos + 8` indicates
///   field `pos` is null.
/// - Fixed-length region: 8 bytes per field after the null bitset.
/// - Variable-length tail growing on demand.
///
/// This writer implements the [`BinaryWriter`] trait so it can plug into
/// [`ValueWriter::write_value`]. Because the trait API does not pass the
/// position to the type-specific write methods, the writer keeps an internal
/// `current_pos` cursor that is advanced after every field write (including
/// `set_null_at`). The encoder is required to write fields in field order,
/// matching the iteration order used by [`crate::row::encode`].
pub struct PaimonBinaryRowWriter {
    null_bits_size_in_bytes: usize,
    fixed_size: usize,
    buffer: Vec<u8>,
    cursor: usize,
    current_pos: usize,
}

impl PaimonBinaryRowWriter {
    pub fn new(arity: usize) -> Self {
        let null_bits_size_in_bytes = calculate_bit_set_width_in_bytes(arity);
        let fixed_size = get_fixed_length_part_size(null_bits_size_in_bytes, arity);
        Self {
            null_bits_size_in_bytes,
            fixed_size,
            buffer: vec![0u8; fixed_size],
            cursor: fixed_size,
            current_pos: 0,
        }
    }

    /// Mirrors Java's `PaimonBinaryRowWriter.createFieldWriter`, returning the
    /// Fluss [`ValueWriter`] for a Paimon-supported scalar field type.
    /// ARRAY/MAP/ROW are explicitly rejected (Java's `default` branch throws).
    pub fn create_value_writer(field_type: &DataType) -> Result<ValueWriter> {
        match field_type {
            DataType::Char(_)
            | DataType::String(_)
            | DataType::Boolean(_)
            | DataType::Binary(_)
            | DataType::Bytes(_)
            | DataType::Decimal(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::Date(_)
            | DataType::Time(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::Timestamp(_)
            | DataType::TimestampLTz(_) => ValueWriter::create_value_writer(field_type, None),
            _ => Err(Error::UnsupportedOperation {
                message: format!("Unsupported type for Paimon BinaryRow writer: {field_type:?}"),
            }),
        }
    }

    /// Writes the Paimon ChangeType byte at offset 0 (always `INSERT = 0`
    /// for key encoding). Must be called immediately after [`Self::reset`].
    pub fn write_change_type_insert(&mut self) {
        self.buffer[0] = 0;
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.buffer[..self.cursor])
    }

    #[allow(dead_code)]
    pub fn buffer(&self) -> &[u8] {
        &self.buffer[..self.cursor]
    }

    #[allow(dead_code)]
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    fn field_offset(&self, pos: usize) -> usize {
        self.null_bits_size_in_bytes + 8 * pos
    }

    fn set_null_bit(&mut self, pos: usize) {
        let bit = pos + HEADER_SIZE_IN_BITS;
        let byte_index = bit / 8;
        let bit_in_byte = bit % 8;
        self.buffer[byte_index] |= 1u8 << bit_in_byte;
    }

    fn put_long_le(&mut self, offset: usize, value: i64) {
        self.buffer[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    fn put_int_le(&mut self, offset: usize, value: i32) {
        self.buffer[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn put_short_le(&mut self, offset: usize, value: i16) {
        self.buffer[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    fn write_bytes_internal(&mut self, pos: usize, bytes: &[u8]) {
        let slot = pack_or_append_bytes(&mut self.buffer, &mut self.cursor, bytes);
        let field_offset = self.field_offset(pos);
        self.put_long_le(field_offset, slot);
    }
}

/// Number of bytes occupied by Paimon's null bitset for the given arity,
/// including the 1-byte (8-bit) ChangeType header.
fn calculate_bit_set_width_in_bytes(arity: usize) -> usize {
    ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8
}

fn get_fixed_length_part_size(null_bits_size_in_bytes: usize, arity: usize) -> usize {
    null_bits_size_in_bytes + 8 * arity
}

impl BinaryWriter for PaimonBinaryRowWriter {
    fn reset(&mut self) {
        self.cursor = self.fixed_size;
        self.current_pos = 0;
        // Zero the null-bits region only (Java semantics: field slots are not
        // wiped because every field is overwritten in the next encode pass).
        for b in &mut self.buffer[..self.null_bits_size_in_bytes] {
            *b = 0;
        }
    }

    fn set_null_at(&mut self, pos: usize) {
        debug_assert_eq!(
            pos, self.current_pos,
            "Paimon writer expects in-order writes"
        );
        self.set_null_bit(pos);
        let field_offset = self.field_offset(pos);
        self.put_long_le(field_offset, 0);
        self.current_pos = pos + 1;
    }

    fn write_boolean(&mut self, value: bool) {
        let pos = self.current_pos;
        let off = self.field_offset(pos);
        self.put_long_le(off, 0);
        self.buffer[off] = if value { 1 } else { 0 };
        self.current_pos = pos + 1;
    }

    fn write_byte(&mut self, value: u8) {
        let pos = self.current_pos;
        let off = self.field_offset(pos);
        self.put_long_le(off, 0);
        self.buffer[off] = value;
        self.current_pos = pos + 1;
    }

    fn write_bytes(&mut self, value: &[u8]) {
        let pos = self.current_pos;
        self.write_bytes_internal(pos, value);
        self.current_pos = pos + 1;
    }

    fn write_char(&mut self, value: &str, _length: usize) {
        // Paimon treats CHAR identically to STRING (BinaryString in Java).
        self.write_string(value);
    }

    fn write_string(&mut self, value: &str) {
        let pos = self.current_pos;
        self.write_bytes_internal(pos, value.as_bytes());
        self.current_pos = pos + 1;
    }

    fn write_short(&mut self, value: i16) {
        let pos = self.current_pos;
        let off = self.field_offset(pos);
        // Zero the unused high bytes to keep the slot deterministic.
        self.put_long_le(off, 0);
        self.put_short_le(off, value);
        self.current_pos = pos + 1;
    }

    fn write_int(&mut self, value: i32) {
        let pos = self.current_pos;
        let off = self.field_offset(pos);
        self.put_long_le(off, 0);
        self.put_int_le(off, value);
        self.current_pos = pos + 1;
    }

    fn write_long(&mut self, value: i64) {
        let pos = self.current_pos;
        let off = self.field_offset(pos);
        self.put_long_le(off, value);
        self.current_pos = pos + 1;
    }

    fn write_float(&mut self, value: f32) {
        let pos = self.current_pos;
        let off = self.field_offset(pos);
        self.put_long_le(off, 0);
        self.buffer[off..off + 4].copy_from_slice(&value.to_le_bytes());
        self.current_pos = pos + 1;
    }

    fn write_double(&mut self, value: f64) {
        let pos = self.current_pos;
        let off = self.field_offset(pos);
        self.buffer[off..off + 8].copy_from_slice(&value.to_le_bytes());
        self.current_pos = pos + 1;
    }

    fn write_binary(&mut self, bytes: &[u8], length: usize) {
        let pos = self.current_pos;
        let slice = &bytes[..length.min(bytes.len())];
        self.write_bytes_internal(pos, slice);
        self.current_pos = pos + 1;
    }

    fn write_decimal(&mut self, value: &Decimal, precision: u32) {
        let pos = self.current_pos;
        if Decimal::is_compact_precision(precision) {
            // Compact: store unscaled long in the field slot (use write_long
            // semantics but consume `current_pos` exactly once).
            let unscaled = value.to_unscaled_long().unwrap_or(0);
            let off = self.field_offset(pos);
            self.put_long_le(off, unscaled);
        } else {
            let slot = append_non_compact_decimal(
                &mut self.buffer,
                &mut self.cursor,
                &value.to_unscaled_bytes(),
            );
            let field_offset = self.field_offset(pos);
            self.put_long_le(field_offset, slot);
        }
        self.current_pos = pos + 1;
    }

    fn write_time(&mut self, value: i32, _precision: u32) {
        // Java's Paimon writer uses writeInt for TIME.
        self.write_int(value);
    }

    fn write_timestamp_ntz(&mut self, value: &TimestampNtz, precision: u32) {
        let pos = self.current_pos;
        if TimestampNtz::is_compact(precision) {
            let off = self.field_offset(pos);
            self.put_long_le(off, value.get_millisecond());
        } else {
            let slot = append_non_compact_timestamp(
                &mut self.buffer,
                &mut self.cursor,
                value.get_millisecond(),
                value.get_nano_of_millisecond(),
            );
            let field_offset = self.field_offset(pos);
            self.put_long_le(field_offset, slot);
        }
        self.current_pos = pos + 1;
    }

    fn write_timestamp_ltz(&mut self, value: &TimestampLtz, precision: u32) {
        let pos = self.current_pos;
        if TimestampLtz::is_compact(precision) {
            let off = self.field_offset(pos);
            self.put_long_le(off, value.get_epoch_millisecond());
        } else {
            let slot = append_non_compact_timestamp(
                &mut self.buffer,
                &mut self.cursor,
                value.get_epoch_millisecond(),
                value.get_nano_of_millisecond(),
            );
            let field_offset = self.field_offset(pos);
            self.put_long_le(field_offset, slot);
        }
        self.current_pos = pos + 1;
    }

    fn write_array(&mut self, _value: &FlussArray) {
        // Java's PaimonBinaryRowWriter rejects ARRAY in its switch default.
        // This should be unreachable because `create_value_writer` rejects
        // ARRAY at encoder-construction time.
        panic!("Paimon BinaryRow writer does not support ARRAY field types");
    }

    fn write_map(&mut self, _value: &FlussMap) {
        panic!("Paimon BinaryRow writer does not support MAP field types");
    }

    fn complete(&mut self) {
        // No-op: `to_bytes` already returns the trimmed-to-cursor buffer.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;

    fn writer_for_arity(arity: usize) -> PaimonBinaryRowWriter {
        let mut w = PaimonBinaryRowWriter::new(arity);
        w.write_change_type_insert();
        w
    }

    #[test]
    fn fixed_layout_sizes() {
        // arity=4 -> nullBitsSizeInBytes = ceil((4 + 63 + 8)/64)*8 = 8
        // fixedSize = 8 + 8*4 = 40
        let w = PaimonBinaryRowWriter::new(4);
        assert_eq!(w.null_bits_size_in_bytes, 8);
        assert_eq!(w.fixed_size, 40);
        assert_eq!(w.cursor, 40);
    }

    #[test]
    fn write_int_in_slot_le() {
        let mut w = writer_for_arity(1);
        w.write_int(0x01020304);
        let bytes = w.to_bytes();
        // header byte 0 (INSERT) + null bits (zeros) total 8, then slot 8 bytes.
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[0], 0x00);
        // INT is written little-endian into bytes 8..12.
        assert_eq!(&bytes[8..12], &0x01020304_i32.to_le_bytes());
        // Remaining 4 bytes of the slot are zero.
        assert_eq!(&bytes[12..16], &[0u8; 4]);
    }

    #[test]
    fn write_short_string_inlined() {
        let mut w = writer_for_arity(1);
        w.write_string("hi"); // 2 bytes -> inline
        let bytes = w.to_bytes();
        assert_eq!(bytes.len(), 16);
        // bytes[8..10] = "hi", bytes[15] = 2 | 0x80
        assert_eq!(&bytes[8..10], b"hi");
        assert_eq!(bytes[15], 0x82);
    }

    #[test]
    fn write_long_string_in_var_part() {
        let mut w = writer_for_arity(1);
        let s = "this_is_a_long_string"; // 21 bytes -> var part, rounded to 24
        w.write_string(s);
        let bytes = w.to_bytes();
        assert_eq!(bytes.len(), 16 + 24);
        // Field slot at 8..16 is (offset=16 << 32) | 21
        let packed = i64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let offset = (packed >> 32) as usize;
        let size = (packed & 0xFFFFFFFF) as usize;
        assert_eq!(offset, 16);
        assert_eq!(size, 21);
        assert_eq!(&bytes[offset..offset + size], s.as_bytes());
    }

    #[test]
    fn set_null_at_marks_bit_and_zeroes_slot() {
        let mut w = writer_for_arity(2);
        w.set_null_at(0);
        w.write_int(7);
        let bytes = w.to_bytes();
        // bit 0+8 = 8 -> null bitset byte index 1, bit 0 -> 0x01
        assert_eq!(bytes[1], 0x01);
        assert_eq!(&bytes[8..16], &[0u8; 8]);
        assert_eq!(&bytes[16..20], &7_i32.to_le_bytes());
    }

    #[test]
    fn reset_clears_null_bits_and_reuses_buffer() {
        let mut w = PaimonBinaryRowWriter::new(2);
        w.write_change_type_insert();
        w.set_null_at(0);
        w.write_int(7);
        let first = w.to_bytes().to_vec();

        // Re-encode the same row after reset; bytes should match.
        w.reset();
        w.write_change_type_insert();
        w.set_null_at(0);
        w.write_int(7);
        let second = w.to_bytes().to_vec();

        assert_eq!(first, second);
    }

    #[test]
    fn buffer_grows_for_large_var_len() {
        let mut w = PaimonBinaryRowWriter::new(1);
        w.write_change_type_insert();
        let big: Vec<u8> = (0..200u8).collect();
        w.write_bytes(&big);
        let bytes = w.to_bytes();
        // var part rounded up: 200 -> 200 (already multiple of 8)
        assert_eq!(bytes.len(), 16 + 200);
        let packed = i64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let off = (packed >> 32) as usize;
        let size = (packed & 0xFFFFFFFF) as usize;
        assert_eq!(off, 16);
        assert_eq!(size, 200);
        assert_eq!(&bytes[off..off + size], big.as_slice());
    }

    #[test]
    fn create_value_writer_rejects_array() {
        let dt = DataTypes::array(DataTypes::int());
        let res = PaimonBinaryRowWriter::create_value_writer(&dt);
        match res {
            Err(Error::UnsupportedOperation { message }) => {
                assert!(message.contains("Unsupported type for Paimon BinaryRow writer"));
            }
            Err(other) => panic!("expected UnsupportedOperation, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    #[test]
    fn create_value_writer_accepts_scalars() {
        let cases = [
            DataTypes::int(),
            DataTypes::bigint(),
            DataTypes::string(),
            DataTypes::char(8),
            DataTypes::boolean(),
            DataTypes::float(),
            DataTypes::double(),
            DataTypes::binary(8),
            DataTypes::bytes(),
            DataTypes::date(),
            DataTypes::time(),
            DataTypes::decimal(10, 2),
            DataTypes::timestamp(),
        ];
        for dt in cases {
            PaimonBinaryRowWriter::create_value_writer(&dt)
                .unwrap_or_else(|e| panic!("expected scalar {dt:?} accepted, got {e}"));
        }
    }
}
