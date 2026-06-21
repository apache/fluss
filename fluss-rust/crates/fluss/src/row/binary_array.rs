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

//! Binary array format matching Java's `BinaryArray.java` layout.
//!
//! Binary layout:
//! ```text
//! [size(4B)] + [null bits (4-byte word aligned)] + [fixed-length part] + [variable-length part]
//! ```
//!
//! Java reference: `BinaryArray.java`, `BinaryArrayWriter.java`

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataType, RowType};
use crate::row::Decimal;
use crate::row::InternalRow;
use crate::row::binary::{BinaryRowFormat, ValueWriter};
use crate::row::binary_map::FlussMap;
use crate::row::compacted::{CompactedRow, CompactedRowWriter, calculate_bit_set_width_in_bytes};
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
use crate::row::field_getter::FieldGetter;
use bytes::Bytes;
use serde::Serialize;
use std::fmt;
use std::hash::{Hash, Hasher};

const MAX_FIX_PART_DATA_SIZE: usize = 7;
const HIGHEST_FIRST_BIT: u64 = 0x80_u64 << 56;
const HIGHEST_SECOND_TO_EIGHTH_BIT: u64 = 0x7F_u64 << 56;

/// Calculates the header size in bytes: 4 (for element count) + null bits (4-byte word aligned).
/// Matches Java's `BinaryArray.calculateHeaderInBytes(numFields)`.
pub fn calculate_header_in_bytes(num_elements: usize) -> usize {
    4 + num_elements.div_ceil(32) * 4
}

/// Calculates the fixed-length part size per element for a given data type.
/// Matches Java's `BinaryArray.calculateFixLengthPartSize(DataType)`.
pub fn calculate_fix_length_part_size(element_type: &DataType) -> usize {
    match element_type {
        DataType::Boolean(_) | DataType::TinyInt(_) => 1,
        DataType::SmallInt(_) => 2,
        DataType::Int(_) | DataType::Float(_) | DataType::Date(_) | DataType::Time(_) => 4,
        DataType::BigInt(_)
        | DataType::Double(_)
        | DataType::Char(_)
        | DataType::String(_)
        | DataType::Binary(_)
        | DataType::Bytes(_)
        | DataType::Decimal(_)
        | DataType::Timestamp(_)
        | DataType::TimestampLTz(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Row(_) => 8,
    }
}

/// Rounds a byte count up to the nearest 8-byte word boundary.
/// Matches Java's `roundNumberOfBytesToNearestWord`.
fn round_to_nearest_word(num_bytes: usize) -> usize {
    (num_bytes + 7) & !7
}

fn is_variable_length_type(dt: &DataType) -> bool {
    match dt {
        DataType::Char(_)
        | DataType::String(_)
        | DataType::Binary(_)
        | DataType::Bytes(_)
        | DataType::Array(_)
        | DataType::Map(_)
        | DataType::Row(_) => true,
        DataType::Decimal(d) => !Decimal::is_compact_precision(d.precision()),
        DataType::Timestamp(t) => !TimestampNtz::is_compact(t.precision()),
        DataType::TimestampLTz(t) => !TimestampLtz::is_compact(t.precision()),
        _ => false,
    }
}

/// A Fluss binary array, wire-compatible with Java's `BinaryArray`.
///
/// Stores elements in a flat byte buffer with a header (element count + null bitmap)
/// followed by fixed-length slots and an optional variable-length section.
///
/// Uses `Bytes` internally so cloning is O(1) reference-counted.
// TODO: FlussArray currently exposes only fallible getters. Infallible
// fast-path variants may be added later as non-breaking extensions.
#[derive(Clone)]
pub struct FlussArray {
    data: Bytes,
    size: usize,
    element_offset: usize,
}

impl fmt::Debug for FlussArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussArray")
            .field("size", &self.size)
            .field("data_len", &self.data.len())
            .finish()
    }
}

impl fmt::Display for FlussArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlussArray[size={}]", self.size)
    }
}

impl PartialEq for FlussArray {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for FlussArray {}

impl PartialOrd for FlussArray {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FlussArray {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.data.cmp(&other.data)
    }
}

impl Hash for FlussArray {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl Serialize for FlussArray {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&self.data)
    }
}

impl FlussArray {
    /// Validates the raw bytes and computes derived fields (size, element_offset).
    fn validate(data: &[u8]) -> Result<(usize, usize)> {
        if data.len() < 4 {
            return Err(IllegalArgument {
                message: format!(
                    "FlussArray data too short: need at least 4 bytes, got {}",
                    data.len()
                ),
            });
        }
        let raw_size = i32::from_le_bytes(data[0..4].try_into().unwrap());
        if raw_size < 0 {
            return Err(IllegalArgument {
                message: format!("FlussArray size must be non-negative, got {raw_size}"),
            });
        }
        let size = raw_size as usize;
        let element_offset = calculate_header_in_bytes(size);
        if element_offset > data.len() {
            return Err(IllegalArgument {
                message: format!(
                    "FlussArray header exceeds payload: header={}, payload={}",
                    element_offset,
                    data.len()
                ),
            });
        }
        Ok((size, element_offset))
    }

    /// Creates a FlussArray from a byte slice (copies data).
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let (size, element_offset) = Self::validate(data)?;
        Ok(FlussArray {
            data: Bytes::copy_from_slice(data),
            size,
            element_offset,
        })
    }

    /// Creates a FlussArray from an owned `Vec<u8>` without copying.
    pub fn from_vec(data: Vec<u8>) -> Result<Self> {
        let (size, element_offset) = Self::validate(&data)?;
        Ok(FlussArray {
            data: Bytes::from(data),
            size,
            element_offset,
        })
    }

    /// Creates a FlussArray from owned bytes without copying.
    fn from_owned_bytes(data: Bytes) -> Result<Self> {
        let (size, element_offset) = Self::validate(&data)?;
        Ok(FlussArray {
            data,
            size,
            element_offset,
        })
    }

    /// Returns the number of elements.
    pub fn size(&self) -> usize {
        self.size
    }

    /// Returns the raw bytes of this array (the complete binary representation).
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Returns true if the element at position `pos` is null.
    pub fn is_null_at(&self, pos: usize) -> bool {
        let byte_index = pos >> 3;
        let bit = pos & 7;
        (self.data[4 + byte_index] & (1u8 << bit)) != 0
    }

    /// Returns the logically occupied bytes of this array, including the variable-length part.
    /// This is used to detect trailing garbage in binary containers.
    pub fn extent(&self, element_type: &DataType) -> Result<usize> {
        let header_size = calculate_header_in_bytes(self.size);
        let element_size = calculate_fix_length_part_size(element_type);
        let fixed_part_size = round_to_nearest_word(header_size + self.size * element_size);

        if !is_variable_length_type(element_type) {
            return Ok(fixed_part_size);
        }

        let mut max_extent = fixed_part_size;
        for i in 0..self.size {
            if !self.is_null_at(i) {
                let packed = self.read_i64(i, "extent calculation")? as u64;
                let mark = packed & HIGHEST_FIRST_BIT;
                if mark == 0 {
                    let offset = (packed >> 32) as usize;
                    let len = (packed & 0xFFFF_FFFF) as usize;
                    max_extent = max_extent.max(offset + len);
                }
            }
        }

        Ok(round_to_nearest_word(max_extent))
    }

    fn checked_slice(&self, start: usize, len: usize, context: &str) -> Result<&[u8]> {
        let end = start.checked_add(len).ok_or_else(|| IllegalArgument {
            message: format!("Overflow while reading {context}: start={start}, len={len}"),
        })?;
        if end > self.data.len() {
            return Err(IllegalArgument {
                message: format!(
                    "Out-of-bounds while reading {context}: start={start}, len={len}, payload={}",
                    self.data.len()
                ),
            });
        }
        Ok(&self.data[start..end])
    }

    fn checked_element_offset(
        &self,
        pos: usize,
        element_size: usize,
        context: &str,
    ) -> Result<usize> {
        if pos >= self.size {
            return Err(IllegalArgument {
                message: format!(
                    "Array element index out of bounds while reading {context}: pos={pos}, size={}",
                    self.size
                ),
            });
        }
        let rel = pos.checked_mul(element_size).ok_or_else(|| IllegalArgument {
            message: format!(
                "Overflow while calculating array element offset for {context}: pos={pos}, element_size={element_size}"
            ),
        })?;
        self.element_offset
            .checked_add(rel)
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "Overflow while adding base offset for {context}: base={}, rel={rel}",
                    self.element_offset
                ),
            })
    }

    fn read_fixed_bytes(&self, pos: usize, len: usize, context: &str) -> Result<&[u8]> {
        let offset = self.checked_element_offset(pos, len, context)?;
        self.checked_slice(offset, len, context)
    }

    fn read_i16(&self, pos: usize, context: &str) -> Result<i16> {
        let bytes = self.read_fixed_bytes(pos, 2, context)?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&self, pos: usize, context: &str) -> Result<i32> {
        let bytes = self.read_fixed_bytes(pos, 4, context)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64(&self, pos: usize, context: &str) -> Result<i64> {
        let bytes = self.read_fixed_bytes(pos, 8, context)?;
        let mut buf = [0_u8; 8];
        buf.copy_from_slice(bytes);
        Ok(i64::from_le_bytes(buf))
    }

    fn read_i64_at_offset(&self, offset: usize, context: &str) -> Result<i64> {
        let bytes = self.checked_slice(offset, 8, context)?;
        let mut buf = [0_u8; 8];
        buf.copy_from_slice(bytes);
        Ok(i64::from_le_bytes(buf))
    }

    fn read_var_len_span(&self, pos: usize) -> Result<(usize, usize)> {
        let field_offset = self.checked_element_offset(pos, 8, "variable-length array element")?;
        let packed = self.read_i64(pos, "variable-length array element")? as u64;
        let mark = packed & HIGHEST_FIRST_BIT;

        if mark == 0 {
            let offset = (packed >> 32) as usize;
            let len = (packed & 0xFFFF_FFFF) as usize;
            let _ = self.checked_slice(offset, len, "variable-length array element")?;
            Ok((offset, len))
        } else {
            let len = ((packed & HIGHEST_SECOND_TO_EIGHTH_BIT) >> 56) as usize;
            if len > MAX_FIX_PART_DATA_SIZE {
                return Err(IllegalArgument {
                    message: format!(
                        "Inline array element length must be <= {MAX_FIX_PART_DATA_SIZE}, got {len}"
                    ),
                });
            }
            // Java stores inline bytes in the 8-byte slot itself.
            // On little-endian, bytes start at field_offset; on big-endian they start at +1.
            let start = if cfg!(target_endian = "little") {
                field_offset
            } else {
                field_offset + 1
            };
            let _ = self.checked_slice(start, len, "inline array element")?;
            Ok((start, len))
        }
    }

    fn read_var_len_bytes(&self, pos: usize) -> Result<&[u8]> {
        let (start, len) = self.read_var_len_span(pos)?;
        Ok(&self.data[start..start + len])
    }

    pub fn get_boolean(&self, pos: usize) -> Result<bool> {
        let bytes = self.read_fixed_bytes(pos, 1, "boolean array element")?;
        Ok(bytes[0] != 0)
    }

    pub fn get_byte(&self, pos: usize) -> Result<i8> {
        let bytes = self.read_fixed_bytes(pos, 1, "byte array element")?;
        Ok(bytes[0] as i8)
    }

    pub fn get_short(&self, pos: usize) -> Result<i16> {
        self.read_i16(pos, "short array element")
    }

    pub fn get_int(&self, pos: usize) -> Result<i32> {
        self.read_i32(pos, "int array element")
    }

    pub fn get_long(&self, pos: usize) -> Result<i64> {
        self.read_i64(pos, "long array element")
    }

    pub fn get_float(&self, pos: usize) -> Result<f32> {
        let bits = self.read_i32(pos, "float array element")? as u32;
        Ok(f32::from_bits(bits))
    }

    pub fn get_double(&self, pos: usize) -> Result<f64> {
        let bits = self.read_i64(pos, "double array element")? as u64;
        Ok(f64::from_bits(bits))
    }

    /// Reads the offset_and_size packed long for variable-length elements.
    fn get_offset_and_size(&self, pos: usize) -> Result<(usize, usize)> {
        let packed = self.get_long(pos)? as u64;
        let offset = (packed >> 32) as usize;
        let size = (packed & 0xFFFF_FFFF) as usize;
        Ok((offset, size))
    }

    pub fn get_string(&self, pos: usize) -> Result<&str> {
        let bytes = self.read_var_len_bytes(pos)?;
        std::str::from_utf8(bytes).map_err(|e| IllegalArgument {
            message: format!("Invalid UTF-8 in array element at position {pos}: {e}"),
        })
    }

    pub fn get_binary(&self, pos: usize) -> Result<&[u8]> {
        self.read_var_len_bytes(pos)
    }

    pub fn get_decimal(&self, pos: usize, precision: u32, scale: u32) -> Result<Decimal> {
        if Decimal::is_compact_precision(precision) {
            let unscaled = self.get_long(pos)?;
            Decimal::from_unscaled_long(unscaled, precision, scale)
        } else {
            let (offset, size) = self.get_offset_and_size(pos)?;
            let bytes = self.checked_slice(offset, size, "decimal bytes")?;
            Decimal::from_unscaled_bytes(bytes, precision, scale)
        }
    }

    pub fn get_date(&self, pos: usize) -> Result<Date> {
        Ok(Date::new(self.get_int(pos)?))
    }

    pub fn get_time(&self, pos: usize) -> Result<Time> {
        Ok(Time::new(self.get_int(pos)?))
    }

    pub fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        if TimestampNtz::is_compact(precision) {
            Ok(TimestampNtz::new(self.get_long(pos)?))
        } else {
            let (offset, nanos_of_millis) = self.get_offset_and_size(pos)?;
            let millis = self.read_i64_at_offset(offset, "timestamp ntz millis")?;
            TimestampNtz::from_millis_nanos(millis, nanos_of_millis as i32)
        }
    }

    pub fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        if TimestampLtz::is_compact(precision) {
            Ok(TimestampLtz::new(self.get_long(pos)?))
        } else {
            let (offset, nanos_of_millis) = self.get_offset_and_size(pos)?;
            let millis = self.read_i64_at_offset(offset, "timestamp ltz millis")?;
            TimestampLtz::from_millis_nanos(millis, nanos_of_millis as i32)
        }
    }

    pub fn get_array(&self, pos: usize) -> Result<FlussArray> {
        let (start, len) = self.read_var_len_span(pos)?;
        FlussArray::from_owned_bytes(self.data.slice(start..start + len))
    }

    pub fn get_map(
        &self,
        pos: usize,
        key_type: &DataType,
        value_type: &DataType,
    ) -> Result<FlussMap> {
        let (start, len) = self.read_var_len_span(pos)?;
        FlussMap::from_owned_bytes(self.data.slice(start..start + len), key_type, value_type)
    }

    pub fn get_row<'a>(&'a self, pos: usize, row_type: &'a RowType) -> Result<CompactedRow<'a>> {
        let bytes = self.read_var_len_bytes(pos)?;
        let header_size = calculate_bit_set_width_in_bytes(row_type.fields().len());
        if bytes.len() < header_size {
            return Err(IllegalArgument {
                message: format!(
                    "FlussArray row bytes at position {} are too short for row type with {} fields: \
                     need at least {} header bytes, got {}",
                    pos,
                    row_type.fields().len(),
                    header_size,
                    bytes.len()
                ),
            });
        }
        Ok(CompactedRow::from_bytes(row_type, bytes))
    }
}

struct RowFieldAccessor {
    getter: FieldGetter,
    writer: ValueWriter,
    nullable: bool,
}

fn build_row_accessors(row_type: &RowType) -> Result<Vec<RowFieldAccessor>> {
    row_type
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            Ok(RowFieldAccessor {
                getter: FieldGetter::create(f.data_type(), i),
                writer: ValueWriter::create_value_writer(
                    f.data_type(),
                    Some(&BinaryRowFormat::Compacted),
                )?,
                nullable: f.data_type().is_nullable(),
            })
        })
        .collect()
}

/// Writer for building a `FlussArray` element by element.
/// Matches Java's `BinaryArrayWriter`.
pub struct FlussArrayWriter {
    data: Vec<u8>,
    null_bits_offset: usize,
    element_offset: usize,
    element_size: usize,
    cursor: usize,
    num_elements: usize,
    // Some(_) only when constructed with a DataType::Row(_) element type.
    row_accessors: Option<Vec<RowFieldAccessor>>,
}

impl FlussArrayWriter {
    /// Creates a new writer for an array with `num_elements` elements of the given element type.
    pub fn new(num_elements: usize, element_type: &DataType) -> Self {
        let element_size = calculate_fix_length_part_size(element_type);
        let row_accessors = match element_type {
            DataType::Row(rt) => Some(
                build_row_accessors(rt)
                    .expect("ROW element type contains a field with no ValueWriter"),
            ),
            _ => None,
        };
        Self::with_state(num_elements, element_size, row_accessors)
    }

    /// Creates a new writer with an explicit element size (in bytes). Does not support `write_row`.
    pub fn with_element_size(num_elements: usize, element_size: usize) -> Self {
        Self::with_state(num_elements, element_size, None)
    }

    fn with_state(
        num_elements: usize,
        element_size: usize,
        row_accessors: Option<Vec<RowFieldAccessor>>,
    ) -> Self {
        let header_in_bytes = calculate_header_in_bytes(num_elements);
        let fixed_size = round_to_nearest_word(header_in_bytes + element_size * num_elements);
        let mut data = vec![0u8; fixed_size];

        // Java's MemorySegment.putInt() stores little-endian.
        data[0..4].copy_from_slice(&(num_elements as i32).to_le_bytes());

        FlussArrayWriter {
            data,
            null_bits_offset: 4,
            element_offset: header_in_bytes,
            element_size,
            cursor: fixed_size,
            num_elements,
            row_accessors,
        }
    }

    fn get_element_offset(&self, pos: usize) -> usize {
        self.element_offset + self.element_size * pos
    }

    /// Sets the null bit for the element at position `pos`.
    pub fn set_null_at(&mut self, pos: usize) {
        let byte_index = pos >> 3;
        let bit = pos & 7;
        self.data[self.null_bits_offset + byte_index] |= 1u8 << bit;
    }

    pub fn write_boolean(&mut self, pos: usize, value: bool) {
        let offset = self.get_element_offset(pos);
        self.data[offset] = if value { 1 } else { 0 };
    }

    pub fn write_byte(&mut self, pos: usize, value: i8) {
        let offset = self.get_element_offset(pos);
        self.data[offset] = value as u8;
    }

    pub fn write_short(&mut self, pos: usize, value: i16) {
        let offset = self.get_element_offset(pos);
        self.data[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_int(&mut self, pos: usize, value: i32) {
        let offset = self.get_element_offset(pos);
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_long(&mut self, pos: usize, value: i64) {
        let offset = self.get_element_offset(pos);
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_float(&mut self, pos: usize, value: f32) {
        let offset = self.get_element_offset(pos);
        self.data[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_double(&mut self, pos: usize, value: f64) {
        let offset = self.get_element_offset(pos);
        self.data[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    }

    /// Writes variable-length bytes to the variable part and stores offset+size in the fixed slot.
    fn write_bytes_to_var_len_part(&mut self, pos: usize, bytes: &[u8]) {
        let rounded = round_to_nearest_word(bytes.len());
        let var_offset = self.cursor;
        self.data.resize(self.data.len() + rounded, 0);
        self.data[var_offset..var_offset + bytes.len()].copy_from_slice(bytes);
        self.set_offset_and_size(pos, var_offset, bytes.len());
        self.cursor += rounded;
    }

    fn set_offset_and_size(&mut self, pos: usize, offset: usize, size: usize) {
        let packed = ((offset as i64) << 32) | (size as i64);
        self.write_long(pos, packed);
    }

    fn write_bytes_to_fix_len_part(&mut self, pos: usize, bytes: &[u8]) {
        let len = bytes.len();
        debug_assert!(len <= MAX_FIX_PART_DATA_SIZE);
        let first_byte = (len as u64) | 0x80;
        let mut seven_bytes = 0_u64;
        if cfg!(target_endian = "little") {
            for (i, b) in bytes.iter().enumerate() {
                seven_bytes |= ((*b as u64) & 0xFF) << (i * 8);
            }
        } else {
            for (i, b) in bytes.iter().enumerate() {
                seven_bytes |= ((*b as u64) & 0xFF) << ((6 - i) * 8);
            }
        }
        let packed = ((first_byte << 56) | seven_bytes) as i64;
        self.write_long(pos, packed);
    }

    pub fn write_string(&mut self, pos: usize, value: &str) {
        let bytes = value.as_bytes();
        if bytes.len() <= MAX_FIX_PART_DATA_SIZE {
            self.write_bytes_to_fix_len_part(pos, bytes);
        } else {
            self.write_bytes_to_var_len_part(pos, bytes);
        }
    }

    pub fn write_binary_bytes(&mut self, pos: usize, value: &[u8]) {
        if value.len() <= MAX_FIX_PART_DATA_SIZE {
            self.write_bytes_to_fix_len_part(pos, value);
        } else {
            self.write_bytes_to_var_len_part(pos, value);
        }
    }

    pub fn write_decimal(&mut self, pos: usize, value: &Decimal, precision: u32) {
        if Decimal::is_compact_precision(precision) {
            self.write_long(
                pos,
                value
                    .to_unscaled_long()
                    .expect("Decimal should fit in i64 for compact precision"),
            );
        } else {
            let bytes = value.to_unscaled_bytes();
            self.write_bytes_to_var_len_part(pos, &bytes);
        }
    }

    pub fn write_date(&mut self, pos: usize, value: Date) {
        self.write_int(pos, value.get_inner());
    }

    pub fn write_time(&mut self, pos: usize, value: Time) {
        self.write_int(pos, value.get_inner());
    }

    pub fn write_timestamp_ntz(&mut self, pos: usize, value: &TimestampNtz, precision: u32) {
        if TimestampNtz::is_compact(precision) {
            self.write_long(pos, value.get_millisecond());
        } else {
            let millis_bytes = value.get_millisecond().to_le_bytes();
            let var_offset = self.cursor;
            let rounded = round_to_nearest_word(8);
            self.data.resize(self.data.len() + rounded, 0);
            self.data[var_offset..var_offset + 8].copy_from_slice(&millis_bytes);
            self.set_offset_and_size(pos, var_offset, value.get_nano_of_millisecond() as usize);
            self.cursor += rounded;
        }
    }

    pub fn write_timestamp_ltz(&mut self, pos: usize, value: &TimestampLtz, precision: u32) {
        if TimestampLtz::is_compact(precision) {
            self.write_long(pos, value.get_epoch_millisecond());
        } else {
            let millis_bytes = value.get_epoch_millisecond().to_le_bytes();
            let var_offset = self.cursor;
            let rounded = round_to_nearest_word(8);
            self.data.resize(self.data.len() + rounded, 0);
            self.data[var_offset..var_offset + 8].copy_from_slice(&millis_bytes);
            self.set_offset_and_size(pos, var_offset, value.get_nano_of_millisecond() as usize);
            self.cursor += rounded;
        }
    }

    /// Writes a nested FlussArray into this array at position `pos`.
    pub fn write_array(&mut self, pos: usize, value: &FlussArray) {
        self.write_bytes_to_var_len_part(pos, value.as_bytes());
    }

    /// Writes a nested FlussMap into this array at position `pos`.
    pub fn write_map(&mut self, pos: usize, value: &FlussMap) {
        self.write_bytes_to_var_len_part(pos, value.as_bytes());
    }

    /// Writes a nested row at `pos`. Requires the writer to have been
    /// constructed via [`new`](Self::new) with a `DataType::Row(_)` element type.
    pub fn write_row(&mut self, pos: usize, row: &dyn InternalRow) -> Result<()> {
        let accessors = self.row_accessors.as_ref().ok_or_else(|| IllegalArgument {
            message: "write_row requires a DataType::Row element type".to_string(),
        })?;
        let mut nested = CompactedRowWriter::new(accessors.len());
        for (i, accessor) in accessors.iter().enumerate() {
            if !accessor.nullable && row.is_null_at(i)? {
                return Err(IllegalArgument {
                    message: format!("nested row field {i} is non-nullable but received null"),
                });
            }
            let datum = accessor.getter.get_field(row)?;
            accessor.writer.write_value(&mut nested, i, &datum)?;
        }
        self.write_bytes_to_var_len_part(pos, nested.buffer());
        Ok(())
    }

    /// Finalizes the writer and returns the completed FlussArray.
    pub fn complete(self) -> Result<FlussArray> {
        let mut data = self.data;
        data.truncate(self.cursor);
        FlussArray::from_vec(data)
    }

    /// Returns the number of elements this writer was initialized with.
    pub fn num_elements(&self) -> usize {
        self.num_elements
    }
}

impl InternalRow for FlussArray {
    fn get_field_count(&self) -> usize {
        self.size()
    }

    fn is_null_at(&self, pos: usize) -> Result<bool> {
        Ok(self.is_null_at(pos))
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        self.get_boolean(pos)
    }
    fn get_byte(&self, pos: usize) -> Result<i8> {
        self.get_byte(pos)
    }
    fn get_short(&self, pos: usize) -> Result<i16> {
        self.get_short(pos)
    }
    fn get_int(&self, pos: usize) -> Result<i32> {
        self.get_int(pos)
    }
    fn get_long(&self, pos: usize) -> Result<i64> {
        self.get_long(pos)
    }
    fn get_float(&self, pos: usize) -> Result<f32> {
        self.get_float(pos)
    }
    fn get_double(&self, pos: usize) -> Result<f64> {
        self.get_double(pos)
    }

    fn get_char(&self, pos: usize, _length: usize) -> Result<&str> {
        self.get_string(pos)
    }

    fn get_string(&self, pos: usize) -> Result<&str> {
        self.get_string(pos)
    }

    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        self.get_decimal(pos, precision as u32, scale as u32)
    }

    fn get_date(&self, pos: usize) -> Result<Date> {
        self.get_date(pos)
    }
    fn get_time(&self, pos: usize) -> Result<Time> {
        self.get_time(pos)
    }
    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        self.get_timestamp_ntz(pos, precision)
    }
    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        self.get_timestamp_ltz(pos, precision)
    }

    fn get_binary(&self, pos: usize, _length: usize) -> Result<&[u8]> {
        self.get_binary(pos)
    }

    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        self.get_binary(pos)
    }

    fn get_array(&self, pos: usize) -> Result<FlussArray> {
        self.get_array(pos)
    }

    fn get_map(&self, pos: usize) -> Result<FlussMap> {
        // FlussArray carries no schema; nested map reads must go through the
        // inherent FlussArray::get_map(pos, key_type, value_type).
        Err(IllegalArgument {
            message: format!(
                "InternalRow::get_map is not supported on FlussArray (pos {pos}); \
                 use FlussArray::get_map(pos, key_type, value_type) directly"
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes};
    use crate::row::binary::BinaryWriter as BinaryWriterTrait;
    use crate::row::compacted::CompactedRowWriter;
    use crate::row::{Datum, GenericRow};

    #[test]
    fn test_header_calculation() {
        assert_eq!(calculate_header_in_bytes(0), 4);
        assert_eq!(calculate_header_in_bytes(1), 8);
        assert_eq!(calculate_header_in_bytes(31), 8);
        assert_eq!(calculate_header_in_bytes(32), 8);
        assert_eq!(calculate_header_in_bytes(33), 12);
        assert_eq!(calculate_header_in_bytes(64), 12);
        assert_eq!(calculate_header_in_bytes(65), 16);
    }

    #[test]
    fn test_fix_length_part_size() {
        assert_eq!(calculate_fix_length_part_size(&DataTypes::boolean()), 1);
        assert_eq!(calculate_fix_length_part_size(&DataTypes::tinyint()), 1);
        assert_eq!(calculate_fix_length_part_size(&DataTypes::smallint()), 2);
        assert_eq!(calculate_fix_length_part_size(&DataTypes::int()), 4);
        assert_eq!(calculate_fix_length_part_size(&DataTypes::bigint()), 8);
        assert_eq!(calculate_fix_length_part_size(&DataTypes::float()), 4);
        assert_eq!(calculate_fix_length_part_size(&DataTypes::double()), 8);
        assert_eq!(calculate_fix_length_part_size(&DataTypes::string()), 8);
        assert_eq!(
            calculate_fix_length_part_size(&DataTypes::array(DataTypes::int())),
            8
        );
    }

    #[test]
    fn test_round_trip_int_array() {
        let elem_type = DataTypes::int();
        let mut writer = FlussArrayWriter::new(3, &elem_type);
        writer.write_int(0, 10);
        writer.write_int(1, 20);
        writer.write_int(2, 30);
        let array = writer.complete().unwrap();

        assert_eq!(array.size(), 3);
        assert!(!array.is_null_at(0));
        assert_eq!(array.get_int(0).unwrap(), 10);
        assert_eq!(array.get_int(1).unwrap(), 20);
        assert_eq!(array.get_int(2).unwrap(), 30);
    }

    #[test]
    fn test_round_trip_with_nulls() {
        let elem_type = DataTypes::int();
        let mut writer = FlussArrayWriter::new(3, &elem_type);
        writer.write_int(0, 1);
        writer.set_null_at(1);
        writer.write_int(2, 3);
        let array = writer.complete().unwrap();

        assert_eq!(array.size(), 3);
        assert!(!array.is_null_at(0));
        assert!(array.is_null_at(1));
        assert!(!array.is_null_at(2));
        assert_eq!(array.get_int(0).unwrap(), 1);
        assert_eq!(array.get_int(2).unwrap(), 3);
    }

    #[test]
    fn test_round_trip_string_array() {
        let elem_type = DataTypes::string();
        let mut writer = FlussArrayWriter::new(3, &elem_type);
        writer.write_string(0, "hello");
        writer.write_string(1, "world");
        writer.write_string(2, "!");
        let array = writer.complete().unwrap();

        assert_eq!(array.size(), 3);
        assert_eq!(array.get_string(0).unwrap(), "hello");
        assert_eq!(array.get_string(1).unwrap(), "world");
        assert_eq!(array.get_string(2).unwrap(), "!");
    }

    #[test]
    fn test_java_inline_short_string_decoding() {
        // Manually construct Java-style inline encoded short string ("abc")
        // slot payload: [len|0x80 in top byte] + [bytes in low 7 bytes on little-endian]
        let mut data = vec![0_u8; 16];
        data[0..4].copy_from_slice(&(1_i32).to_le_bytes());
        // null bits remain 0
        let first_byte = (3_u64 | 0x80) << 56;
        let seven_bytes = (b'a' as u64) | ((b'b' as u64) << 8) | ((b'c' as u64) << 16);
        let packed = first_byte | seven_bytes;
        data[8..16].copy_from_slice(&packed.to_le_bytes());

        let arr = FlussArray::from_bytes(&data).unwrap();
        assert_eq!(arr.size(), 1);
        assert_eq!(arr.get_string(0).unwrap(), "abc");
    }

    #[test]
    fn test_java_inline_short_binary_decoding() {
        let elem_type = DataTypes::bytes();
        let mut writer = FlussArrayWriter::new(1, &elem_type);
        writer.write_binary_bytes(0, b"abc");
        let arr = writer.complete().unwrap();
        assert_eq!(arr.get_binary(0).unwrap(), b"abc");
    }

    #[test]
    fn test_round_trip_empty_array() {
        let elem_type = DataTypes::int();
        let writer = FlussArrayWriter::new(0, &elem_type);
        let array = writer.complete().unwrap();
        assert_eq!(array.size(), 0);
    }

    #[test]
    fn test_round_trip_boolean_array() {
        let elem_type = DataTypes::boolean();
        let mut writer = FlussArrayWriter::new(3, &elem_type);
        writer.write_boolean(0, true);
        writer.write_boolean(1, false);
        writer.write_boolean(2, true);
        let array = writer.complete().unwrap();

        assert_eq!(array.size(), 3);
        assert!(array.get_boolean(0).unwrap());
        assert!(!array.get_boolean(1).unwrap());
        assert!(array.get_boolean(2).unwrap());
    }

    #[test]
    fn test_round_trip_long_array() {
        let elem_type = DataTypes::bigint();
        let mut writer = FlussArrayWriter::new(2, &elem_type);
        writer.write_long(0, i64::MAX);
        writer.write_long(1, i64::MIN);
        let array = writer.complete().unwrap();

        assert_eq!(array.get_long(0).unwrap(), i64::MAX);
        assert_eq!(array.get_long(1).unwrap(), i64::MIN);
    }

    #[test]
    fn test_round_trip_double_array() {
        let elem_type = DataTypes::double();
        let mut writer = FlussArrayWriter::new(2, &elem_type);
        writer.write_double(0, 1.23);
        writer.write_double(1, -4.56);
        let array = writer.complete().unwrap();

        assert_eq!(array.get_double(0).unwrap(), 1.23);
        assert_eq!(array.get_double(1).unwrap(), -4.56);
    }

    #[test]
    fn test_round_trip_array_of_row() {
        let row_type_owned = DataTypes::row(vec![
            DataField::new("x", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);
        let element_type = row_type_owned.clone();
        let row_type = match &row_type_owned {
            DataType::Row(rt) => rt,
            _ => unreachable!(),
        };

        // Build array<row<int, string>> with two rows: (42, "hello"), (-1, null)
        let mut writer = FlussArrayWriter::new(2, &element_type);

        let mut r0 = GenericRow::new(2);
        r0.set_field(0, 42_i32);
        r0.set_field(1, "hello");
        writer.write_row(0, &r0).expect("write row 0");

        let mut r1 = GenericRow::new(2);
        r1.set_field(0, -1_i32);
        r1.set_field(1, Datum::Null);
        writer.write_row(1, &r1).expect("write row 1");

        let array = writer.complete().unwrap();
        assert_eq!(array.size(), 2);

        let row0 = array.get_row(0, row_type).expect("get row 0");
        assert_eq!(row0.get_int(0).unwrap(), 42);
        assert_eq!(row0.get_string(1).unwrap(), "hello");

        let row1 = array.get_row(1, row_type).expect("get row 1");
        assert_eq!(row1.get_int(0).unwrap(), -1);
        assert!(row1.is_null_at(1).unwrap());
    }

    #[test]
    fn test_get_row_rejects_oversized_row_type() {
        let small_row_type_owned =
            DataTypes::row(vec![DataField::new("n", DataTypes::int(), None)]);
        let small_row_type = match &small_row_type_owned {
            DataType::Row(rt) => rt,
            _ => unreachable!(),
        };
        let mut writer = FlussArrayWriter::new(1, &small_row_type_owned);
        let mut row = GenericRow::new(1);
        row.set_field(0, 7_i32);
        writer.write_row(0, &row).unwrap();
        let array = writer.complete().unwrap();

        let oversized_owned = DataTypes::row(
            (0..10)
                .map(|i| DataField::new(format!("f{i}"), DataTypes::int(), None))
                .collect(),
        );
        let oversized_row_type = match &oversized_owned {
            DataType::Row(rt) => rt,
            _ => unreachable!(),
        };
        let huge_owned = DataTypes::row(
            (0..100)
                .map(|i| DataField::new(format!("f{i}"), DataTypes::int(), None))
                .collect(),
        );
        let huge_row_type = match &huge_owned {
            DataType::Row(rt) => rt,
            _ => unreachable!(),
        };
        match array.get_row(0, huge_row_type) {
            Err(e) => assert!(
                e.to_string().contains("too short for row type"),
                "unexpected error: {e}"
            ),
            Ok(_) => panic!("expected oversized row_type to be rejected"),
        }

        let recovered = array.get_row(0, small_row_type).unwrap();
        assert_eq!(recovered.get_int(0).unwrap(), 7);

        let _ = oversized_row_type;
    }

    #[test]
    fn test_round_trip_array_of_row_with_nullable_element() {
        let row_type_owned = DataTypes::row(vec![DataField::new("n", DataTypes::int(), None)]);
        let element_type = row_type_owned.clone();
        let row_type = match &row_type_owned {
            DataType::Row(rt) => rt,
            _ => unreachable!(),
        };

        let mut writer = FlussArrayWriter::new(3, &element_type);

        let mut r0 = GenericRow::new(1);
        r0.set_field(0, 7_i32);
        writer.write_row(0, &r0).expect("write row 0");

        writer.set_null_at(1);

        let mut r2 = GenericRow::new(1);
        r2.set_field(0, 8_i32);
        writer.write_row(2, &r2).expect("write row 2");

        let array = writer.complete().unwrap();

        let row0 = array.get_row(0, row_type).unwrap();
        assert_eq!(row0.get_int(0).unwrap(), 7);
        assert!(array.is_null_at(1));
        let row2 = array.get_row(2, row_type).unwrap();
        assert_eq!(row2.get_int(0).unwrap(), 8);

        let strict_row_type_owned = DataTypes::row(vec![DataField::new(
            "n",
            DataTypes::int().as_non_nullable(),
            None,
        )]);
        let mut bad_writer = FlussArrayWriter::new(1, &strict_row_type_owned);
        let mut bad = GenericRow::new(1);
        bad.set_field(0, Datum::Null);
        let err = bad_writer.write_row(0, &bad).unwrap_err();
        assert!(
            err.to_string().contains("non-nullable"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_round_trip_array_of_row_of_array_of_string() {
        let inner_array_type = DataTypes::array(DataTypes::string());
        let inner_row_type_owned =
            DataTypes::row(vec![DataField::new("tags", inner_array_type.clone(), None)]);
        let inner_row_type = match &inner_row_type_owned {
            DataType::Row(rt) => rt,
            _ => unreachable!(),
        };

        let mut tags1 = FlussArrayWriter::new(2, &DataTypes::string());
        tags1.write_string(0, "alpha");
        tags1.write_string(1, "beta");
        let tags1 = tags1.complete().unwrap();
        let mut row1 = GenericRow::new(1);
        row1.set_field(0, tags1);

        let mut tags2 = FlussArrayWriter::new(3, &DataTypes::string());
        tags2.write_string(0, "x");
        tags2.set_null_at(1);
        tags2.write_string(2, "z");
        let tags2 = tags2.complete().unwrap();
        let mut row2 = GenericRow::new(1);
        row2.set_field(0, tags2);

        let mut outer_writer = FlussArrayWriter::new(2, &inner_row_type_owned);
        outer_writer.write_row(0, &row1).unwrap();
        outer_writer.write_row(1, &row2).unwrap();
        let outer = outer_writer.complete().unwrap();

        assert_eq!(outer.size(), 2);

        let r0 = outer.get_row(0, inner_row_type).unwrap();
        let r0_tags = r0.get_array(0).unwrap();
        assert_eq!(r0_tags.size(), 2);
        assert_eq!(r0_tags.get_string(0).unwrap(), "alpha");
        assert_eq!(r0_tags.get_string(1).unwrap(), "beta");

        let r1 = outer.get_row(1, inner_row_type).unwrap();
        let r1_tags = r1.get_array(0).unwrap();
        assert_eq!(r1_tags.size(), 3);
        assert_eq!(r1_tags.get_string(0).unwrap(), "x");
        assert!(r1_tags.is_null_at(1));
        assert_eq!(r1_tags.get_string(2).unwrap(), "z");
    }

    #[test]
    fn test_round_trip_row_of_array_of_row() {
        let inner_row_type_owned =
            DataTypes::row(vec![DataField::new("n", DataTypes::int(), None)]);
        let inner_array_type = DataTypes::array(inner_row_type_owned.clone());
        let outer_row_type_owned =
            DataTypes::row(vec![DataField::new("arr", inner_array_type.clone(), None)]);

        let outer_row_type = match &outer_row_type_owned {
            DataType::Row(rt) => rt,
            _ => unreachable!(),
        };
        let inner_row_type = match &inner_row_type_owned {
            DataType::Row(rt) => rt,
            _ => unreachable!(),
        };

        let mut arr_writer = FlussArrayWriter::new(2, &inner_row_type_owned);
        let mut r0 = GenericRow::new(1);
        r0.set_field(0, 1_i32);
        arr_writer.write_row(0, &r0).unwrap();
        let mut r1 = GenericRow::new(1);
        r1.set_field(0, 2_i32);
        arr_writer.write_row(1, &r1).unwrap();
        let inner_arr = arr_writer.complete().unwrap();

        let mut outer = GenericRow::new(1);
        outer.set_field(0, inner_arr.clone());

        let mut writer = CompactedRowWriter::new(1);
        writer.write_array(&inner_arr);
        let bytes = writer.to_bytes();

        let outer_compacted = CompactedRow::from_bytes(outer_row_type, &bytes);
        let recovered_arr = outer_compacted.get_array(0).unwrap();
        assert_eq!(recovered_arr.size(), 2);

        let recovered_r0 = recovered_arr.get_row(0, inner_row_type).unwrap();
        assert_eq!(recovered_r0.get_int(0).unwrap(), 1);
        let recovered_r1 = recovered_arr.get_row(1, inner_row_type).unwrap();
        assert_eq!(recovered_r1.get_int(0).unwrap(), 2);
    }

    #[test]
    fn test_round_trip_nested_array() {
        let inner_type = DataTypes::int();
        let outer_type = DataTypes::array(DataTypes::int());

        let mut inner_writer = FlussArrayWriter::new(2, &inner_type);
        inner_writer.write_int(0, 1);
        inner_writer.write_int(1, 2);
        let inner_array = inner_writer.complete().unwrap();

        let mut outer_writer = FlussArrayWriter::new(1, &outer_type);
        outer_writer.write_array(0, &inner_array);
        let outer_array = outer_writer.complete().unwrap();

        assert_eq!(outer_array.size(), 1);
        let nested = outer_array.get_array(0).unwrap();
        assert_eq!(nested.size(), 2);
        assert_eq!(nested.get_int(0).unwrap(), 1);
        assert_eq!(nested.get_int(1).unwrap(), 2);
    }

    #[test]
    fn test_primitive_getter_out_of_bounds_returns_error() {
        let elem_type = DataTypes::int();
        let mut writer = FlussArrayWriter::new(1, &elem_type);
        writer.write_int(0, 10);
        let array = writer.complete().unwrap();

        let err = array.get_int(1).unwrap_err();
        assert!(
            err.to_string().contains("out of bounds"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_primitive_getter_on_malformed_payload_returns_error() {
        // Size says 1, but payload only contains header (no element bytes).
        let mut data = vec![0_u8; 8];
        data[0..4].copy_from_slice(&(1_i32).to_le_bytes());
        let arr = FlussArray::from_bytes(&data).unwrap();

        let err = arr.get_int(0).unwrap_err();
        assert!(
            err.to_string().contains("Out-of-bounds"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_binary_layout_matches_java() {
        // Verify exact byte layout for a simple [1, 2, 3] int array
        let elem_type = DataTypes::int();
        let mut writer = FlussArrayWriter::new(3, &elem_type);
        writer.write_int(0, 1);
        writer.write_int(1, 2);
        writer.write_int(2, 3);
        let array = writer.complete().unwrap();
        let bytes = array.as_bytes();

        // size = 3 at offset 0 (4 bytes, little-endian per Java MemorySegment.putInt)
        assert_eq!(i32::from_le_bytes(bytes[0..4].try_into().unwrap()), 3);
        // null bits: 4 bytes starting at offset 4, should be all zeros
        assert_eq!(&bytes[4..8], &[0, 0, 0, 0]);
        // elements start at offset 8 (header = 4 + 4), each 4 bytes (little-endian)
        assert_eq!(i32::from_le_bytes(bytes[8..12].try_into().unwrap()), 1);
        assert_eq!(i32::from_le_bytes(bytes[12..16].try_into().unwrap()), 2);
        assert_eq!(i32::from_le_bytes(bytes[16..20].try_into().unwrap()), 3);
    }
}
