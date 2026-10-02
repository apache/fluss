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

//! Writer for Fluss's aligned binary row, the format carrying the min and max
//! values of a V1 log record batch's statistics.

use crate::row::binary::BinaryWriter;
use crate::row::binary::encoding::{
    append_non_compact_decimal, append_non_compact_timestamp, pack_or_append_bytes,
};
use crate::row::datum::{TimestampLtz, TimestampNtz};
use crate::row::{Decimal, FlussArray, FlussMap};
use bytes::Bytes;

/// Bits reserved ahead of the null bitset, matching Java's
/// `AlignedRow.HEADER_SIZE_IN_BITS`.
const HEADER_SIZE_IN_BITS: usize = 8;

/// Builds Java's `AlignedRow` byte for byte: a fixed part of null bits plus one
/// 8-byte slot per field, then an 8-byte-aligned variable-length part.
///
/// Laying out `(id INT = 7, name STRING = "hello world", score BIGINT = 42)`:
///
/// ```text
/// byte 0        8         16        24        32          43   48
///  |------------|---------|---------|---------|-----------|----|
///  | null bits  | slot 0  | slot 1  | slot 2  | "hello world"  |
///  | (8 bytes)  | int 7   | ptr     | long 42 | + zero padding |
///  |------------|---------|---------|---------|----------------|
///   <--------- fixed part (32) ---------->  <-- variable tail -->
///                              |                    ^
///                              +-- (offset=32, len=11) packed into 8 bytes
/// ```
///
/// The null bits reserve 8 header bits, so field `i` owns bit `i + 8`, and the
/// region is padded to whole 8-byte words. A value of 8 bytes or less lives in
/// its slot, while anything longer stores `(offset << 32) | len` there and puts
/// its payload in the tail.
///
/// Fields must be written in order; each `write_*` consumes the next position.
pub struct AlignedRowWriter {
    buffer: Vec<u8>,
    null_bits_size_in_bytes: usize,
    /// Size of the null bits plus one 8-byte slot per field, which is also
    /// where the variable-length part starts.
    fixed_size: usize,
    /// Byte offset where the next value too large for its slot gets appended.
    cursor: usize,
    /// Index of the field the next `write_*` fills, which picks its 8-byte slot.
    current_pos: usize,
}

impl AlignedRowWriter {
    pub fn new(arity: usize) -> Self {
        let null_bits_size_in_bytes = calculate_bit_set_width_in_bytes(arity);
        let fixed_size = null_bits_size_in_bytes + 8 * arity;
        Self {
            buffer: vec![0u8; fixed_size],
            null_bits_size_in_bytes,
            fixed_size,
            cursor: fixed_size,
            current_pos: 0,
        }
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.buffer[..self.cursor])
    }

    fn field_offset(&self, pos: usize) -> usize {
        self.null_bits_size_in_bytes + 8 * pos
    }

    fn set_null_bit(&mut self, pos: usize) {
        let bit = pos + HEADER_SIZE_IN_BITS;
        self.buffer[bit / 8] |= 1u8 << (bit % 8);
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

/// Null bits are padded to whole 8-byte words, after the reserved header bits.
fn calculate_bit_set_width_in_bytes(arity: usize) -> usize {
    ((arity + 63 + HEADER_SIZE_IN_BITS) / 64) * 8
}

/// Mirrors Java's `AlignedRow.calculateFixPartSizeInBytes`.
pub(crate) fn calculate_fix_part_size_in_bytes(arity: usize) -> usize {
    calculate_bit_set_width_in_bytes(arity) + 8 * arity
}

impl BinaryWriter for AlignedRowWriter {
    fn reset(&mut self) {
        self.cursor = self.fixed_size;
        self.current_pos = 0;
        for b in &mut self.buffer[..self.fixed_size] {
            *b = 0;
        }
    }

    fn set_null_at(&mut self, pos: usize) {
        self.set_null_bit(pos);
        let field_offset = self.field_offset(pos);
        self.put_long_le(field_offset, 0);
        self.current_pos = pos + 1;
    }

    fn write_boolean(&mut self, value: bool) {
        let off = self.field_offset(self.current_pos);
        self.put_long_le(off, 0);
        self.buffer[off] = u8::from(value);
        self.current_pos += 1;
    }

    fn write_byte(&mut self, value: u8) {
        let off = self.field_offset(self.current_pos);
        self.put_long_le(off, 0);
        self.buffer[off] = value;
        self.current_pos += 1;
    }

    fn write_bytes(&mut self, value: &[u8]) {
        let pos = self.current_pos;
        self.write_bytes_internal(pos, value);
        self.current_pos = pos + 1;
    }

    fn write_char(&mut self, value: &str, _length: usize) {
        self.write_string(value);
    }

    fn write_string(&mut self, value: &str) {
        let pos = self.current_pos;
        self.write_bytes_internal(pos, value.as_bytes());
        self.current_pos = pos + 1;
    }

    fn write_short(&mut self, value: i16) {
        let off = self.field_offset(self.current_pos);
        self.put_long_le(off, 0);
        self.put_short_le(off, value);
        self.current_pos += 1;
    }

    fn write_int(&mut self, value: i32) {
        let off = self.field_offset(self.current_pos);
        self.put_long_le(off, 0);
        self.put_int_le(off, value);
        self.current_pos += 1;
    }

    fn write_long(&mut self, value: i64) {
        let off = self.field_offset(self.current_pos);
        self.put_long_le(off, value);
        self.current_pos += 1;
    }

    fn write_float(&mut self, value: f32) {
        let off = self.field_offset(self.current_pos);
        self.put_long_le(off, 0);
        self.buffer[off..off + 4].copy_from_slice(&value.to_le_bytes());
        self.current_pos += 1;
    }

    fn write_double(&mut self, value: f64) {
        let off = self.field_offset(self.current_pos);
        self.buffer[off..off + 8].copy_from_slice(&value.to_le_bytes());
        self.current_pos += 1;
    }

    fn write_binary(&mut self, bytes: &[u8], length: usize) {
        let pos = self.current_pos;
        let slice = &bytes[..length.min(bytes.len())];
        self.write_bytes_internal(pos, slice);
        self.current_pos = pos + 1;
    }

    fn write_decimal(&mut self, value: &Decimal, precision: u32) {
        assert_eq!(
            value.precision(),
            precision,
            "decimal was built at a different precision than the column's"
        );
        let pos = self.current_pos;
        if Decimal::is_compact_precision(precision) {
            let unscaled = value
                .to_unscaled_long()
                .expect("a compact precision guarantees the unscaled value fits in i64");
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
        panic!("statistics are never collected for ARRAY columns");
    }

    fn write_map(&mut self, _value: &FlussMap) {
        panic!("statistics are never collected for MAP columns");
    }

    fn complete(&mut self) {
        // `to_bytes` already trims to the cursor.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    #[test]
    fn fixed_part_matches_java_sizing() {
        // arity 4 -> ceil((4 + 63 + 8) / 64) * 8 = 8 null bytes, + 8 per field.
        let writer = AlignedRowWriter::new(4);
        assert_eq!(writer.null_bits_size_in_bytes, 8);
        assert_eq!(writer.fixed_size, 40);
        assert_eq!(writer.cursor, 40);

        // 57 fields cross into a second word of null bits.
        assert_eq!(calculate_bit_set_width_in_bytes(56), 8);
        assert_eq!(calculate_bit_set_width_in_bytes(57), 16);
    }

    #[test]
    fn writes_int_little_endian_into_its_slot() {
        let mut writer = AlignedRowWriter::new(1);
        writer.write_int(0x01020304);
        let bytes = writer.to_bytes();
        assert_eq!(bytes.len(), 16);
        assert_eq!(&bytes[8..12], &0x01020304_i32.to_le_bytes());
        assert_eq!(&bytes[12..16], &[0u8; 4]);
    }

    #[test]
    fn inlines_a_short_string_with_its_length_marker() {
        let mut writer = AlignedRowWriter::new(1);
        writer.write_string("abc");
        let bytes = writer.to_bytes();
        // No variable part is needed, so the row is just the fixed part.
        assert_eq!(bytes.len(), 16);
        assert_eq!(&bytes[8..11], b"abc");
        assert_eq!(bytes[15], 0x83);
    }

    #[test]
    fn spills_a_long_string_to_the_variable_part() {
        let mut writer = AlignedRowWriter::new(1);
        writer.write_string("abcdefghij");
        let bytes = writer.to_bytes();
        // 10 bytes of payload round up to two 8-byte words.
        assert_eq!(bytes.len(), 16 + 16);
        let packed = i64::from_le_bytes(bytes[8..16].try_into().unwrap());
        assert_eq!((packed >> 32) as usize, 16);
        assert_eq!((packed & 0xFFFF_FFFF) as usize, 10);
        assert_eq!(&bytes[16..26], b"abcdefghij");
        // The padding to the word boundary is zeroed.
        assert_eq!(&bytes[26..32], &[0u8; 6]);
    }

    #[test]
    fn marks_null_without_disturbing_other_fields() {
        let mut writer = AlignedRowWriter::new(2);
        writer.set_null_at(0);
        writer.write_long(7);
        let bytes = writer.to_bytes();
        // Field 0's null bit sits at bit 8, the first bit after the header.
        assert_eq!(bytes[1], 0x01);
        assert_eq!(&bytes[8..16], &[0u8; 8]);
        assert_eq!(i64::from_le_bytes(bytes[16..24].try_into().unwrap()), 7);
    }

    #[test]
    fn reset_clears_the_fixed_part_and_rewinds() {
        let mut writer = AlignedRowWriter::new(1);
        writer.write_string("abcdefghij");
        writer.reset();
        writer.write_int(5);
        let bytes = writer.to_bytes();
        assert_eq!(bytes.len(), 16);
        assert_eq!(&bytes[8..12], &5_i32.to_le_bytes());
    }

    /// Reads the 8-byte slot of field `pos` as the packed `(offset, size)` pair
    /// used for values that live in the variable-length tail.
    fn packed_slot(bytes: &[u8], null_bits: usize, pos: usize) -> (usize, usize) {
        let at = null_bits + 8 * pos;
        let packed = i64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        ((packed >> 32) as usize, (packed & 0xFFFF_FFFF) as usize)
    }

    /// Slices out the 8-byte fixed-part slot belonging to field `pos`.
    fn slot(bytes: &[u8], null_bits: usize, pos: usize) -> &[u8] {
        let at = null_bits + 8 * pos;
        &bytes[at..at + 8]
    }

    #[test]
    fn writes_each_numeric_width_into_the_low_bytes_of_its_slot() {
        let mut writer = AlignedRowWriter::new(7);
        writer.write_boolean(true);
        writer.write_byte(0xAB);
        writer.write_short(-2);
        writer.write_int(-3);
        writer.write_long(-4);
        writer.write_float(1.5);
        writer.write_double(2.5);
        let bytes = writer.to_bytes();

        assert_eq!(slot(&bytes, 8, 0), &[1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(slot(&bytes, 8, 1), &[0xAB, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(&slot(&bytes, 8, 2)[..2], &(-2i16).to_le_bytes());
        assert_eq!(&slot(&bytes, 8, 3)[..4], &(-3i32).to_le_bytes());
        assert_eq!(slot(&bytes, 8, 4), &(-4i64).to_le_bytes());
        assert_eq!(&slot(&bytes, 8, 5)[..4], &1.5f32.to_le_bytes());
        assert_eq!(slot(&bytes, 8, 6), &2.5f64.to_le_bytes());
        // The unused high bytes of the narrower slots are zeroed.
        assert_eq!(&slot(&bytes, 8, 2)[2..], &[0u8; 6]);
        assert_eq!(&slot(&bytes, 8, 5)[4..], &[0u8; 4]);
    }

    #[test]
    fn keeps_a_compact_decimal_in_its_slot() {
        let precision = 4;
        let decimal = Decimal::from_unscaled_long(5, precision, 2).expect("decimal");
        let mut writer = AlignedRowWriter::new(2);
        writer.write_decimal(&decimal, precision);
        writer.set_null_at(1);
        let bytes = writer.to_bytes();

        // Precision 4 is compact, so the unscaled value sits inline.
        assert_eq!(bytes.len(), 24);
        assert_eq!(slot(&bytes, 8, 0), &5i64.to_le_bytes());
        // Field 1's null bit is bit 9, so byte 1 bit 1.
        assert_eq!(bytes[1] & 0x02, 0x02);
    }

    #[test]
    fn spills_a_non_compact_decimal_into_sixteen_tail_bytes() {
        let precision = 25;
        let decimal =
            Decimal::from_big_decimal(BigDecimal::from_str("5.55").unwrap(), precision, 5)
                .expect("decimal");
        let unscaled = decimal.to_unscaled_bytes();

        let mut writer = AlignedRowWriter::new(1);
        writer.write_decimal(&decimal, precision);
        let bytes = writer.to_bytes();

        // Java always reserves 16 tail bytes here, whatever the unscaled length,
        // so the stride does not depend on how many the value needed.
        assert_eq!(bytes.len(), 16 + 16);
        let (offset, size) = packed_slot(&bytes, 8, 0);
        assert_eq!(offset, 16);
        assert_eq!(size, unscaled.len());
        assert_eq!(&bytes[16..16 + unscaled.len()], &unscaled[..]);
        // The unscaled value rarely fills all 16, so the rest must be zeroed.
        assert_eq!(
            &bytes[16 + unscaled.len()..32],
            &vec![0u8; 16 - unscaled.len()][..]
        );
    }

    #[test]
    #[should_panic(expected = "assertion")]
    fn rejects_a_decimal_built_at_another_precision() {
        let decimal = Decimal::from_big_decimal(BigDecimal::from_str("5.55").unwrap(), 25, 5)
            .expect("decimal");
        let mut writer = AlignedRowWriter::new(1);
        writer.write_decimal(&decimal, 10);
    }

    #[test]
    fn keeps_a_compact_timestamp_in_its_slot() {
        let value = TimestampNtz::from_millis_nanos(123, 0).expect("timestamp");
        let mut writer = AlignedRowWriter::new(1);
        writer.write_timestamp_ntz(&value, 3);
        let bytes = writer.to_bytes();

        assert_eq!(bytes.len(), 16);
        assert_eq!(slot(&bytes, 8, 0), &123i64.to_le_bytes());
    }

    #[test]
    fn splits_a_non_compact_timestamp_between_slot_and_tail() {
        let value = TimestampNtz::from_millis_nanos(123, 456_000).expect("timestamp");
        let mut writer = AlignedRowWriter::new(1);
        writer.write_timestamp_ntz(&value, 6);
        let bytes = writer.to_bytes();

        // Millis go to the tail; the slot carries the offset and the nanos.
        // Timestamps are the one case where the packed low half is not a
        // length, since the tail is always exactly 8 bytes.
        assert_eq!(bytes.len(), 16 + 8);
        let (offset, nanos) = packed_slot(&bytes, 8, 0);
        assert_eq!(offset, 16);
        assert_eq!(nanos, 456_000);
        assert_eq!(i64::from_le_bytes(bytes[16..24].try_into().unwrap()), 123);
    }

    #[test]
    fn encodes_a_local_zoned_timestamp_the_same_way() {
        let compact = TimestampLtz::from_millis_nanos(99, 0).expect("timestamp");
        let mut writer = AlignedRowWriter::new(1);
        writer.write_timestamp_ltz(&compact, 3);
        assert_eq!(slot(&writer.to_bytes(), 8, 0), &99i64.to_le_bytes());

        let wide = TimestampLtz::from_millis_nanos(99, 1_000).expect("timestamp");
        let mut writer = AlignedRowWriter::new(1);
        writer.write_timestamp_ltz(&wide, 9);
        let bytes = writer.to_bytes();
        let (offset, nanos) = packed_slot(&bytes, 8, 0);
        assert_eq!((offset, nanos), (16, 1_000));
        assert_eq!(i64::from_le_bytes(bytes[16..24].try_into().unwrap()), 99);
    }

    #[test]
    fn inlines_short_binary_and_spills_longer_binary() {
        let mut writer = AlignedRowWriter::new(2);
        writer.write_bytes(&[1, 0xFF, 5]);
        writer.write_bytes(&[1, 0xFF, 5, 5, 1, 5, 1, 5]);
        let bytes = writer.to_bytes();

        // Three bytes fit inline with the length marker in the slot's top byte.
        assert_eq!(&slot(&bytes, 8, 0)[..3], &[1, 0xFF, 5]);
        assert_eq!(slot(&bytes, 8, 0)[7], 0x83);
        // Eight bytes exceed the seven that fit, so they move to the tail.
        let (offset, size) = packed_slot(&bytes, 8, 1);
        assert_eq!((offset, size), (24, 8));
        assert_eq!(&bytes[24..32], &[1, 0xFF, 5, 5, 1, 5, 1, 5]);
    }

    #[test]
    fn writes_char_like_a_string() {
        let mut writer = AlignedRowWriter::new(1);
        writer.write_char("ab", 2);
        let bytes = writer.to_bytes();
        assert_eq!(&slot(&bytes, 8, 0)[..2], b"ab");
        assert_eq!(slot(&bytes, 8, 0)[7], 0x82);
    }

    #[test]
    fn tracks_null_bits_across_a_second_word() {
        // 60 fields need two 8-byte words of null bits.
        let mut writer = AlignedRowWriter::new(60);
        assert_eq!(writer.null_bits_size_in_bytes, 16);
        for pos in 0..60 {
            if pos == 56 {
                writer.set_null_at(pos);
            } else {
                writer.write_int(pos as i32);
            }
        }
        let bytes = writer.to_bytes();

        // Field 56 owns bit 56 + 8 = 64, which is byte 8 bit 0, so the null
        // landed in the first byte of the second word rather than overflowing
        // the first.
        assert_eq!(bytes[8], 0x01);
        // The first word stays clear: no other field is null and the 8 reserved
        // header bits are never set.
        assert_eq!(&bytes[..8], &[0u8; 8]);
        // Slots start after both words, so the last field sits at 16 + 8 * 59
        // rather than the 8 + 8 * 59 a single-word row would use.
        assert_eq!(&slot(&bytes, 16, 59)[..4], &59i32.to_le_bytes());
    }

    #[test]
    fn grows_the_buffer_across_many_spilled_values() {
        let mut writer = AlignedRowWriter::new(8);
        let long = "0123456789abcdef";
        for _ in 0..8 {
            writer.write_string(long);
        }
        let bytes = writer.to_bytes();

        // Eight 16-byte payloads follow the 8 + 64 byte fixed part.
        assert_eq!(bytes.len(), 72 + 8 * 16);
        for pos in 0..8 {
            let (offset, size) = packed_slot(&bytes, 8, pos);
            assert_eq!(size, 16);
            assert_eq!(&bytes[offset..offset + 16], long.as_bytes());
        }
    }
}
