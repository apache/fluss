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

//! Variable-length integer encoding utilities.
//!
//! This module provides utilities for encoding integers in variable-length format,
//! which can save space when encoding small integers. The encoding uses 7 bits per byte
//! with the most significant bit as a continuation flag.

use bytes::BufMut;
use std::io::{self, Read, Write};

/// Write an unsigned integer in variable-length format.
///
/// The encoding uses 7 bits per byte with the MSB set to 1 if more bytes follow.
/// This matches the encoding used in Google Protocol Buffers.
#[allow(dead_code)]
pub fn write_unsigned_varint<W: Write>(value: u32, writer: &mut W) -> io::Result<usize> {
    let mut v = value;
    let mut bytes_written = 0;

    while (v & !0x7F) != 0 {
        writer.write_all(&[((v as u8) & 0x7F) | 0x80])?;
        bytes_written += 1;
        v >>= 7;
    }
    writer.write_all(&[v as u8])?;
    bytes_written += 1;

    Ok(bytes_written)
}

/// Write an unsigned integer in variable-length format to a buffer.
pub fn write_unsigned_varint_buf(value: u32, buf: &mut impl BufMut) {
    let mut v = value;

    while (v & !0x7F) != 0 {
        buf.put_u8(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

/// Read an unsigned integer stored in variable-length format.
#[allow(dead_code)]
pub fn read_unsigned_varint<R: Read>(reader: &mut R) -> io::Result<u32> {
    let mut tmp = [0u8; 1];
    reader.read_exact(&mut tmp)?;
    let mut byte = tmp[0] as i8;

    if byte >= 0 {
        return Ok(byte as u32);
    }

    let mut result = (byte & 127) as u32;

    reader.read_exact(&mut tmp)?;
    byte = tmp[0] as i8;
    if byte >= 0 {
        result |= (byte as u32) << 7;
    } else {
        result |= ((byte & 127) as u32) << 7;

        reader.read_exact(&mut tmp)?;
        byte = tmp[0] as i8;
        if byte >= 0 {
            result |= (byte as u32) << 14;
        } else {
            result |= ((byte & 127) as u32) << 14;

            reader.read_exact(&mut tmp)?;
            byte = tmp[0] as i8;
            if byte >= 0 {
                result |= (byte as u32) << 21;
            } else {
                result |= ((byte & 127) as u32) << 21;

                reader.read_exact(&mut tmp)?;
                byte = tmp[0] as i8;
                result |= (byte as u32) << 28;

                if byte < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid u32 varint encoding: too many bytes (most significant bit in the 5th byte is set)",
                    ));
                }
            }
        }
    }

    Ok(result)
}

/// Read an unsigned integer from a byte slice in variable-length format.
pub fn read_unsigned_varint_bytes(bytes: &[u8]) -> io::Result<(u32, usize)> {
    if bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Cannot read varint from empty buffer",
        ));
    }

    let mut byte = bytes[0] as i8;
    let mut index = 1;

    if byte >= 0 {
        return Ok((byte as u32, index));
    }

    let mut result = (byte & 127) as u32;

    if index >= bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Incomplete varint",
        ));
    }
    byte = bytes[index] as i8;
    index += 1;
    if byte >= 0 {
        result |= (byte as u32) << 7;
    } else {
        result |= ((byte & 127) as u32) << 7;

        if index >= bytes.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Incomplete varint",
            ));
        }
        byte = bytes[index] as i8;
        index += 1;
        if byte >= 0 {
            result |= (byte as u32) << 14;
        } else {
            result |= ((byte & 127) as u32) << 14;

            if index >= bytes.len() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Incomplete varint",
                ));
            }
            byte = bytes[index] as i8;
            index += 1;
            if byte >= 0 {
                result |= (byte as u32) << 21;
            } else {
                result |= ((byte & 127) as u32) << 21;

                if index >= bytes.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Incomplete varint",
                    ));
                }
                byte = bytes[index] as i8;
                index += 1;
                result |= (byte as u32) << 28;

                if byte < 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid u32 varint encoding: too many bytes (most significant bit in the 5th byte is set)",
                    ));
                }
            }
        }
    }

    Ok((result, index))
}

/// Calculate the number of bytes needed to encode a u32 in variable-length format.
///
/// Varint encoding uses 7 bits per byte, so we need `ceil(bits_used / 7)` bytes.
/// This function computes that efficiently using the formula:
///
/// size = ((38 - leading_zeros) * 74899) >> 19  +  (leading_zeros >> 5)
///
/// Where:
/// - `38 = 32 + 6` (6 accounts for ceiling in division)
/// - `74899 = 2^19 / 7` (enables division by 7 via multiply + shift)
/// - `leading_zeros >> 5` adds 1 when value is 0 (minimum 1 byte)
pub fn size_of_unsigned_varint(value: u32) -> usize {
    let leading_zeros = value.leading_zeros();
    let leading_zeros_below_38_divided_by_7 = ((38 - leading_zeros) * 0b10010010010010011) >> 19;
    (leading_zeros_below_38_divided_by_7 + (leading_zeros >> 5)) as usize
}

/// Calculate the number of bytes needed to encode a u64 in variable-length format.
///
/// Varint encoding uses 7 bits per byte, so we need `ceil(bits_used / 7)` bytes.
/// This function computes that efficiently using the formula:
///
/// size = ((70 - leading_zeros) * 74899) >> 19  +  (leading_zeros >> 6)
///
/// - `70 = 64 + 6` (6 accounts for ceiling in division)
/// - `74899 = 2^19 / 7` (enables division by 7 via multiply + shift)
/// - `leading_zeros >> 6` adds 1 when value is 0 (minimum 1 byte)
#[allow(dead_code)]
pub fn size_of_unsigned_varint_u64(value: u64) -> usize {
    let leading_zeros = value.leading_zeros();
    let leading_zeros_below_70_divided_by_7 = ((70 - leading_zeros) * 0b10010010010010011) >> 19;
    (leading_zeros_below_70_divided_by_7 + (leading_zeros >> 6)) as usize
}

/// Write an unsigned 64-bit integer in variable-length format to a buffer.
#[allow(dead_code)]
pub fn write_unsigned_varint_u64_buf(value: u64, buf: &mut impl BufMut) {
    let mut v = value;
    while (v & !0x7F) != 0 {
        buf.put_u8(((v as u8) & 0x7F) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

/// Write directly to a mutable byte slice, returning the number of bytes written.
/// Used by CompactedRowWriter which manages its own position.
///
/// # Panics
/// Panics if the slice is too small to hold the encoded varint.
/// The slice must have at least 5 bytes available (the maximum size for a u32 varint).
/// Use [`size_of_unsigned_varint`] to calculate the required size beforehand.
pub fn write_unsigned_varint_to_slice(value: u32, slice: &mut [u8]) -> usize {
    let mut v = value;
    let mut written = 0;

    while (v & !0x7F) != 0 {
        slice[written] = ((v as u8) & 0x7F) | 0x80;
        written += 1;
        v >>= 7;
    }
    slice[written] = v as u8;
    written + 1
}

/// Write unsigned 64-bit varint directly to a mutable byte slice.
///
/// # Panics
/// Panics if the slice is too small to hold the encoded varint.
/// The slice must have at least 10 bytes available (the maximum size for a u64 varint).
pub fn write_unsigned_varint_u64_to_slice(value: u64, slice: &mut [u8]) -> usize {
    let mut v = value;
    let mut written = 0;

    while (v & !0x7F) != 0 {
        slice[written] = ((v as u8) & 0x7F) | 0x80;
        written += 1;
        v >>= 7;
    }
    slice[written] = v as u8;
    written + 1
}

/// Read unsigned varint from a slice starting at given position.
/// Returns (value, next_position).
/// Used by CompactedRowReader which manages positions.
pub fn read_unsigned_varint_at(
    slice: &[u8],
    mut pos: usize,
    max_bytes: usize,
) -> io::Result<(u32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0;

    for _ in 0..max_bytes {
        if pos >= slice.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Unexpected end of varint",
            ));
        }
        let b = slice[pos];
        pos += 1;
        result |= ((b & 0x7F) as u32) << shift;
        if (b & 0x80) == 0 {
            return Ok((result, pos));
        }
        shift += 7;
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid VarInt32 input stream",
    ))
}

/// Read unsigned 64-bit varint from a slice starting at given position.
pub fn read_unsigned_varint_u64_at(
    slice: &[u8],
    mut pos: usize,
    max_bytes: usize,
) -> io::Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;

    for _ in 0..max_bytes {
        if pos >= slice.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Unexpected end of varint",
            ));
        }
        let b = slice[pos];
        pos += 1;
        result |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            return Ok((result, pos));
        }
        shift += 7;
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "Invalid VarInt64 input stream",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_unsigned_varint_round_trip() {
        let test_values = vec![
            0u32,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            2097151,
            2097152,
            268435455,
            268435456,
            u32::MAX,
        ];

        for value in test_values {
            // Test with Write trait
            let mut buffer = Vec::new();
            let written = write_unsigned_varint(value, &mut buffer).unwrap();

            let mut reader = Cursor::new(&buffer);
            let read_value = read_unsigned_varint(&mut reader).unwrap();

            assert_eq!(value, read_value, "Round trip failed for value {value}");
            assert_eq!(
                written,
                buffer.len(),
                "Bytes written mismatch for value {value}"
            );

            // Test with BufMut
            let mut buf = bytes::BytesMut::new();
            write_unsigned_varint_buf(value, &mut buf);
            assert_eq!(buf.len(), written, "BufMut write length mismatch");

            // Test size calculation
            let calculated_size = size_of_unsigned_varint(value);
            assert_eq!(
                calculated_size,
                buffer.len(),
                "Size calculation failed for value {value}"
            );

            // Test reading from bytes
            let (read_value_bytes, bytes_read) = read_unsigned_varint_bytes(&buffer).unwrap();
            assert_eq!(
                value, read_value_bytes,
                "Bytes read failed for value {value}"
            );
            assert_eq!(
                bytes_read,
                buffer.len(),
                "Bytes read count mismatch for value {value}"
            );
        }
    }

    #[test]
    fn test_size_of_unsigned_varint() {
        assert_eq!(size_of_unsigned_varint(0), 1);
        assert_eq!(size_of_unsigned_varint(127), 1);
        assert_eq!(size_of_unsigned_varint(128), 2);
        assert_eq!(size_of_unsigned_varint(16383), 2);
        assert_eq!(size_of_unsigned_varint(16384), 3);
        assert_eq!(size_of_unsigned_varint(2097151), 3);
        assert_eq!(size_of_unsigned_varint(2097152), 4);
        assert_eq!(size_of_unsigned_varint(268435455), 4);
        assert_eq!(size_of_unsigned_varint(268435456), 5);
        assert_eq!(size_of_unsigned_varint(u32::MAX), 5);
    }

    #[test]
    fn test_size_of_unsigned_varint_u64() {
        assert_eq!(size_of_unsigned_varint_u64(0), 1);
        assert_eq!(size_of_unsigned_varint_u64(127), 1);
        assert_eq!(size_of_unsigned_varint_u64(128), 2);
        assert_eq!(size_of_unsigned_varint_u64(16383), 2);
        assert_eq!(size_of_unsigned_varint_u64(16384), 3);
        assert_eq!(size_of_unsigned_varint_u64(2097151), 3);
        assert_eq!(size_of_unsigned_varint_u64(2097152), 4);
        assert_eq!(size_of_unsigned_varint_u64(268435455), 4);
        assert_eq!(size_of_unsigned_varint_u64(268435456), 5);
        assert_eq!(size_of_unsigned_varint_u64(u32::MAX as u64), 5);
        assert_eq!(size_of_unsigned_varint_u64(34359738367), 5);
        assert_eq!(size_of_unsigned_varint_u64(34359738368), 6);
        assert_eq!(size_of_unsigned_varint_u64(4398046511103), 6);
        assert_eq!(size_of_unsigned_varint_u64(4398046511104), 7);
        assert_eq!(size_of_unsigned_varint_u64(562949953421311), 7);
        assert_eq!(size_of_unsigned_varint_u64(562949953421312), 8);
        assert_eq!(size_of_unsigned_varint_u64(72057594037927935), 8);
        assert_eq!(size_of_unsigned_varint_u64(72057594037927936), 9);
        assert_eq!(size_of_unsigned_varint_u64(9223372036854775807), 9);
        assert_eq!(size_of_unsigned_varint_u64(9223372036854775808), 10);
        assert_eq!(size_of_unsigned_varint_u64(u64::MAX), 10);
    }

    #[test]
    fn test_read_unsigned_varint_bytes_error_handling() {
        // Empty buffer
        assert!(read_unsigned_varint_bytes(&[]).is_err());

        // Incomplete varint (continuation bit set but no next byte)
        assert!(read_unsigned_varint_bytes(&[0x80]).is_err());
        assert!(read_unsigned_varint_bytes(&[0xFF, 0x80]).is_err());
    }

    #[test]
    fn test_write_read_to_slice() {
        // Test u32 varint to slice
        let test_values_u32 = vec![0u32, 127, 128, 16384, u32::MAX];

        for value in test_values_u32 {
            let mut buffer = vec![0u8; 10];
            let written = write_unsigned_varint_to_slice(value, &mut buffer);

            let (read_value, next_pos) = read_unsigned_varint_at(&buffer, 0, 5).unwrap();
            assert_eq!(value, read_value);
            assert_eq!(written, next_pos);
        }

        // Test u64 varint to slice
        let test_values_u64 = vec![0u64, 127, 128, 16384, u32::MAX as u64, u64::MAX];

        for value in test_values_u64 {
            let mut buffer = vec![0u8; 10];
            let written = write_unsigned_varint_u64_to_slice(value, &mut buffer);

            let (read_value, next_pos) = read_unsigned_varint_u64_at(&buffer, 0, 10).unwrap();
            assert_eq!(value, read_value);
            assert_eq!(written, next_pos);
        }
    }

    #[test]
    fn test_read_at_with_offset() {
        // Write multiple varints and read at different positions
        let mut buffer = vec![0u8; 20];
        let mut pos = 0;

        pos += write_unsigned_varint_to_slice(127, &mut buffer[pos..]);
        pos += write_unsigned_varint_to_slice(16384, &mut buffer[pos..]);
        let end_pos = pos + write_unsigned_varint_to_slice(u32::MAX, &mut buffer[pos..]);

        // Read back
        let (val1, pos1) = read_unsigned_varint_at(&buffer, 0, 5).unwrap();
        assert_eq!(val1, 127);

        let (val2, pos2) = read_unsigned_varint_at(&buffer, pos1, 5).unwrap();
        assert_eq!(val2, 16384);

        let (val3, pos3) = read_unsigned_varint_at(&buffer, pos2, 5).unwrap();
        assert_eq!(val3, u32::MAX);
        assert_eq!(pos3, end_pos);
    }
}
