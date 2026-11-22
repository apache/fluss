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

use crate::row::Decimal;
use crate::row::binary::BinaryWriter;
use crate::row::binary_array::FlussArray;
use crate::row::binary_map::FlussMap;
use crate::row::compacted::compacted_row::calculate_bit_set_width_in_bytes;
use crate::util::varint::{write_unsigned_varint_to_slice, write_unsigned_varint_u64_to_slice};
use bytes::{Bytes, BytesMut};
use std::cmp;

// Writer for CompactedRow
// Reference implementation:
// https://github.com/apache/fluss/blob/d4a72fad240d4b81563aaf83fa3b09b5058674ed/fluss-common/src/main/java/org/apache/fluss/row/compacted/CompactedRowWriter.java#L71
#[allow(dead_code)]
pub struct CompactedRowWriter {
    header_size_in_bytes: usize,
    position: usize,
    buffer: BytesMut,
}

#[allow(dead_code)]
impl CompactedRowWriter {
    pub const MAX_INT_SIZE: usize = 5;
    pub const MAX_LONG_SIZE: usize = 10;

    pub fn new(field_count: usize) -> Self {
        let header_size = calculate_bit_set_width_in_bytes(field_count);
        let cap = cmp::max(64, header_size);

        let mut buffer = BytesMut::with_capacity(cap);
        buffer.resize(cap, 0);

        Self {
            header_size_in_bytes: header_size,
            position: header_size,
            buffer,
        }
    }

    pub fn position(&self) -> usize {
        self.position
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buffer[..self.position]
    }

    pub fn to_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.buffer[..self.position])
    }

    /// Flushes writer's ByteMut, resetting writer's inner state and returns Byte of flushed state
    pub fn flush_bytes(&mut self) -> Bytes {
        let used = self.buffer.split_to(self.position);
        self.position = self.header_size_in_bytes;
        if self.buffer.len() < self.header_size_in_bytes {
            self.buffer.resize(self.header_size_in_bytes.max(64), 0);
        } else {
            self.buffer[..self.header_size_in_bytes].fill(0);
        }
        used.freeze()
    }

    fn ensure_capacity(&mut self, need_len: usize) {
        if (self.buffer.len() - self.position) < need_len {
            let new_len = cmp::max(self.buffer.len() * 2, self.buffer.len() + need_len);
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

impl BinaryWriter for CompactedRowWriter {
    fn reset(&mut self) {
        self.position = self.header_size_in_bytes;
        self.buffer[..self.header_size_in_bytes].fill(0);
    }

    fn set_null_at(&mut self, pos: usize) {
        let byte_index = pos >> 3;
        let bit = pos & 7;
        debug_assert!(byte_index < self.header_size_in_bytes);
        self.buffer[byte_index] |= 1u8 << bit;
    }

    fn write_boolean(&mut self, value: bool) {
        let b = if value { 1u8 } else { 0u8 };
        self.write_raw(&[b])
    }

    fn write_byte(&mut self, value: u8) {
        self.write_raw(&[value])
    }

    fn write_bytes(&mut self, value: &[u8]) {
        let len_i32 = i32::try_from(value.len())
            .expect("Byte slice too large to encode length as i32: exceeds i32::MAX");
        self.write_int(len_i32);
        self.write_raw(value)
    }

    fn write_char(&mut self, value: &str, _length: usize) {
        // TODO: currently, we encoding CHAR(length) as the same with STRING, the length info can be
        //  omitted and the bytes length should be enforced in the future.
        self.write_string(value)
    }

    fn write_string(&mut self, value: &str) {
        self.write_bytes(value.as_ref())
    }

    fn write_short(&mut self, value: i16) {
        // Use native endianness to match Java's UnsafeUtils.putShort behavior
        // Java uses sun.misc.Unsafe which writes in native byte order (typically LE on x86/ARM)
        self.write_raw(&value.to_ne_bytes())
    }

    fn write_int(&mut self, value: i32) {
        self.ensure_capacity(Self::MAX_INT_SIZE);
        let bytes_written =
            write_unsigned_varint_to_slice(value as u32, &mut self.buffer[self.position..]);
        self.position += bytes_written;
    }

    fn write_long(&mut self, value: i64) {
        self.ensure_capacity(Self::MAX_LONG_SIZE);
        let bytes_written =
            write_unsigned_varint_u64_to_slice(value as u64, &mut self.buffer[self.position..]);
        self.position += bytes_written;
    }

    fn write_float(&mut self, value: f32) {
        // Use native endianness to match Java's UnsafeUtils.putFloat behavior
        self.write_raw(&value.to_ne_bytes())
    }

    fn write_double(&mut self, value: f64) {
        // Use native endianness to match Java's UnsafeUtils.putDouble behavior
        self.write_raw(&value.to_ne_bytes())
    }

    fn write_binary(&mut self, bytes: &[u8], length: usize) {
        // TODO: currently, we encoding BINARY(length) as the same with BYTES, the length info can
        //  be omitted and the bytes length should be enforced in the future.
        self.write_bytes(&bytes[..length.min(bytes.len())])
    }

    fn write_array(&mut self, value: &FlussArray) {
        self.write_bytes(value.as_bytes())
    }

    fn write_map(&mut self, value: &FlussMap) {
        self.write_bytes(value.as_bytes())
    }

    fn complete(&mut self) {
        // do nothing
    }

    fn write_decimal(&mut self, value: &Decimal, precision: u32) {
        // Decimal is already validated and rescaled during construction.
        // Just serialize the precomputed unscaled representation.
        if Decimal::is_compact_precision(precision) {
            self.write_long(
                value
                    .to_unscaled_long()
                    .expect("Decimal should fit in i64 for compact precision"),
            )
        } else {
            self.write_bytes(&value.to_unscaled_bytes())
        }
    }

    fn write_time(&mut self, value: i32, _precision: u32) {
        // TIME is always encoded as i32 (milliseconds since midnight) regardless of precision
        self.write_int(value)
    }

    fn write_timestamp_ntz(&mut self, value: &crate::row::datum::TimestampNtz, precision: u32) {
        if crate::row::datum::TimestampNtz::is_compact(precision) {
            // Compact: write only milliseconds
            self.write_long(value.get_millisecond());
        } else {
            // Non-compact: write milliseconds + nanoOfMillisecond
            self.write_long(value.get_millisecond());
            self.write_int(value.get_nano_of_millisecond());
        }
    }

    fn write_timestamp_ltz(&mut self, value: &crate::row::datum::TimestampLtz, precision: u32) {
        if crate::row::datum::TimestampLtz::is_compact(precision) {
            // Compact: write only epoch milliseconds
            self.write_long(value.get_epoch_millisecond());
        } else {
            // Non-compact: write epoch milliseconds + nanoOfMillisecond
            self.write_long(value.get_epoch_millisecond());
            self.write_int(value.get_nano_of_millisecond());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bigdecimal::{BigDecimal, num_bigint::BigInt};

    #[test]
    fn test_write_decimal_compact() {
        // Compact decimal (precision <= 18)
        let bd = BigDecimal::new(BigInt::from(12345), 2); // 123.45
        let decimal = Decimal::from_big_decimal(bd, 10, 2).unwrap();

        let mut w = CompactedRowWriter::new(1);
        w.write_decimal(&decimal, 10);

        let (val, _) = crate::util::varint::read_unsigned_varint_u64_at(
            w.buffer(),
            w.header_size_in_bytes,
            CompactedRowWriter::MAX_LONG_SIZE,
        )
        .unwrap();
        assert_eq!(val as i64, 12345);
    }

    #[test]
    fn test_write_decimal_rounding() {
        // Test HALF_UP rounding: 12.345 → 12.35
        let bd = BigDecimal::new(BigInt::from(12345), 3);
        let decimal = Decimal::from_big_decimal(bd, 10, 2).unwrap();

        let mut w = CompactedRowWriter::new(1);
        w.write_decimal(&decimal, 10);

        let (val, _) = crate::util::varint::read_unsigned_varint_u64_at(
            w.buffer(),
            w.header_size_in_bytes,
            CompactedRowWriter::MAX_LONG_SIZE,
        )
        .unwrap();
        assert_eq!(val as i64, 1235); // 12.35 with scale 2
    }

    #[test]
    fn test_write_decimal_non_compact() {
        // Non-compact (precision > 18): uses byte array
        let bd = BigDecimal::new(BigInt::from(12345), 0);
        let decimal = Decimal::from_big_decimal(bd, 28, 0).unwrap();

        let mut w = CompactedRowWriter::new(1);
        w.write_decimal(&decimal, 28);

        // Verify something was written (at least length varint + some bytes)
        assert!(w.position() > w.header_size_in_bytes);
    }
}
