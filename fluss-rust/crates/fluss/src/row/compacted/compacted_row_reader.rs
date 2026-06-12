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
use crate::metadata::{DataType, RowType};
use crate::row::compacted::compacted_row::calculate_bit_set_width_in_bytes;
use crate::row::compacted::compacted_row_writer::CompactedRowWriter;
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
use crate::row::{Datum, Decimal, FlussArray, GenericRow};
use crate::util::varint::{read_unsigned_varint_at, read_unsigned_varint_u64_at};
use std::borrow::Cow;
use std::str::from_utf8;
use std::sync::Arc;

#[allow(dead_code)]
#[derive(Clone)]
pub struct CompactedRowDeserializer<'a> {
    row_type: Cow<'a, RowType>,
    // Index-parallel to row_type.fields(); Some(_) only for ROW-typed fields.
    nested: Vec<Option<Arc<CompactedRowDeserializer<'a>>>>,
}

fn build_nested_deserializers<'a>(
    row_type: &RowType,
) -> Vec<Option<Arc<CompactedRowDeserializer<'a>>>> {
    row_type
        .fields()
        .iter()
        .map(|f| {
            if let DataType::Row(inner) = &f.data_type {
                Some(Arc::new(CompactedRowDeserializer::new_from_owned(
                    inner.clone(),
                )))
            } else {
                None
            }
        })
        .collect()
}

#[allow(dead_code)]
impl<'a> CompactedRowDeserializer<'a> {
    pub fn new(row_type: &'a RowType) -> Self {
        let nested = build_nested_deserializers(row_type);
        Self {
            row_type: Cow::Borrowed(row_type),
            nested,
        }
    }

    pub fn new_from_owned(row_type: RowType) -> Self {
        let nested = build_nested_deserializers(&row_type);
        Self {
            row_type: Cow::Owned(row_type),
            nested,
        }
    }

    pub fn get_row_type(&self) -> &RowType {
        self.row_type.as_ref()
    }

    pub fn deserialize(&self, reader: &CompactedRowReader<'a>) -> Result<GenericRow<'a>> {
        let mut row = GenericRow::new(self.row_type.fields().len());
        let mut cursor = reader.initial_position();
        for (col_pos, data_field) in self.row_type.fields().iter().enumerate() {
            let dtype = &data_field.data_type;
            if dtype.is_nullable() && reader.is_null_at(col_pos) {
                row.set_field(col_pos, Datum::Null);
                continue;
            }
            let (datum, next_cursor) = match dtype {
                DataType::Boolean(_) => {
                    let (val, next) = reader.read_boolean(cursor)?;
                    (Datum::Bool(val), next)
                }
                DataType::TinyInt(_) => {
                    let (val, next) = reader.read_byte(cursor)?;
                    (Datum::Int8(val as i8), next)
                }
                DataType::SmallInt(_) => {
                    let (val, next) = reader.read_short(cursor)?;
                    (Datum::Int16(val), next)
                }
                DataType::Int(_) => {
                    let (val, next) = reader.read_int(cursor)?;
                    (Datum::Int32(val), next)
                }
                DataType::BigInt(_) => {
                    let (val, next) = reader.read_long(cursor)?;
                    (Datum::Int64(val), next)
                }
                DataType::Float(_) => {
                    let (val, next) = reader.read_float(cursor)?;
                    (Datum::Float32(val.into()), next)
                }
                DataType::Double(_) => {
                    let (val, next) = reader.read_double(cursor)?;
                    (Datum::Float64(val.into()), next)
                }
                // TODO: use read_char(length) in the future, but need to keep compatibility
                DataType::Char(_) | DataType::String(_) => {
                    let (val, next) = reader.read_string(cursor)?;
                    (Datum::String(val.into()), next)
                }
                // TODO: use read_binary(length) in the future, but need to keep compatibility
                DataType::Bytes(_) | DataType::Binary(_) => {
                    let (val, next) = reader.read_bytes(cursor)?;
                    (Datum::Blob(val.into()), next)
                }
                DataType::Decimal(decimal_type) => {
                    let precision = decimal_type.precision();
                    let scale = decimal_type.scale();
                    if Decimal::is_compact_precision(precision) {
                        // Compact: stored as i64
                        let (val, next) = reader.read_long(cursor)?;
                        let decimal =
                            Decimal::from_unscaled_long(val, precision, scale).map_err(|e| {
                                IllegalArgument {
                                    message: format!(
                                        "Failed to create decimal from unscaled long: {e}"
                                    ),
                                }
                            })?;
                        (Datum::Decimal(decimal), next)
                    } else {
                        // Non-compact: stored as minimal big-endian bytes
                        let (bytes, next) = reader.read_bytes(cursor)?;
                        let decimal = Decimal::from_unscaled_bytes(bytes, precision, scale)
                            .map_err(|e| IllegalArgument {
                                message: format!(
                                    "Failed to create decimal from unscaled bytes: {e}"
                                ),
                            })?;
                        (Datum::Decimal(decimal), next)
                    }
                }
                DataType::Date(_) => {
                    let (val, next) = reader.read_int(cursor)?;
                    (Datum::Date(Date::new(val)), next)
                }
                DataType::Time(_) => {
                    let (val, next) = reader.read_int(cursor)?;
                    (Datum::Time(Time::new(val)), next)
                }
                DataType::Timestamp(timestamp_type) => {
                    let precision = timestamp_type.precision();
                    if TimestampNtz::is_compact(precision) {
                        let (millis, next) = reader.read_long(cursor)?;
                        (Datum::TimestampNtz(TimestampNtz::new(millis)), next)
                    } else {
                        let (millis, mid) = reader.read_long(cursor)?;
                        let (nanos, next) = reader.read_int(mid)?;
                        let timestamp = TimestampNtz::from_millis_nanos(millis, nanos).map_err(
                            |e| IllegalArgument {
                                message: format!(
                                    "Invalid nano_of_millisecond value in compacted row timestamp: {e}"
                                ),
                            },
                        )?;
                        (Datum::TimestampNtz(timestamp), next)
                    }
                }
                DataType::TimestampLTz(timestamp_ltz_type) => {
                    let precision = timestamp_ltz_type.precision();
                    if TimestampLtz::is_compact(precision) {
                        let (epoch_millis, next) = reader.read_long(cursor)?;
                        (Datum::TimestampLtz(TimestampLtz::new(epoch_millis)), next)
                    } else {
                        let (epoch_millis, mid) = reader.read_long(cursor)?;
                        let (nanos, next) = reader.read_int(mid)?;
                        let timestamp_ltz =
                            TimestampLtz::from_millis_nanos(epoch_millis, nanos).map_err(|e| {
                                IllegalArgument {
                                    message: format!(
                                        "Invalid nano_of_millisecond value in compacted row timestamp_ltz: {e}"
                                    ),
                                }
                            })?;
                        (Datum::TimestampLtz(timestamp_ltz), next)
                    }
                }
                DataType::Array(_) => {
                    let (bytes, next) = reader.read_bytes(cursor)?;
                    (Datum::Array(FlussArray::from_bytes(bytes)?), next)
                }
                DataType::Row(row_type) => {
                    let (nested_bytes, next) = reader.read_bytes(cursor)?;
                    let nested_reader = CompactedRowReader::new(
                        row_type.fields().len(),
                        nested_bytes,
                        0,
                        nested_bytes.len(),
                    );
                    let nested_deser = self.nested[col_pos]
                        .as_ref()
                        .expect("ROW field must have nested deserializer");
                    let nested_row = nested_deser.deserialize(&nested_reader)?;
                    (Datum::Row(Box::new(nested_row)), next)
                }
                DataType::Map(map_type) => {
                    let (bytes, next) = reader.read_bytes(cursor)?;
                    let map = crate::row::binary_map::FlussMap::from_bytes(
                        bytes,
                        map_type.key_type(),
                        map_type.value_type(),
                    )?;
                    (Datum::Map(map), next)
                }
            };
            cursor = next_cursor;
            row.set_field(col_pos, datum);
        }
        Ok(row)
    }
}

// Reference implementation:
// https://github.com/apache/fluss/blob/main/fluss-common/src/main/java/org/apache/fluss/row/compacted/CompactedRowReader.java
#[allow(dead_code)]
pub struct CompactedRowReader<'a> {
    segment: &'a [u8],
    offset: usize,
    limit: usize,
    header_size_in_bytes: usize,
}

#[allow(dead_code)]
impl<'a> CompactedRowReader<'a> {
    pub fn new(field_count: usize, data: &'a [u8], offset: usize, length: usize) -> Self {
        let header_size_in_bytes = calculate_bit_set_width_in_bytes(field_count);
        let limit = offset + length;
        let position = offset + header_size_in_bytes;
        debug_assert!(limit <= data.len());
        debug_assert!(position <= limit);

        CompactedRowReader {
            segment: data,
            offset,
            limit,
            header_size_in_bytes,
        }
    }

    fn initial_position(&self) -> usize {
        self.offset + self.header_size_in_bytes
    }

    fn checked_pos(&self, pos: usize, width: usize, context: &str) -> Result<usize> {
        let next = pos.checked_add(width).ok_or_else(|| IllegalArgument {
            message: format!("Overflow while reading {context}: pos={pos}, width={width}"),
        })?;
        if next > self.limit {
            return Err(IllegalArgument {
                message: format!(
                    "Out-of-bounds while reading {context}: pos={pos}, width={width}, limit={}",
                    self.limit
                ),
            });
        }
        Ok(next)
    }

    pub fn is_null_at(&self, col_pos: usize) -> bool {
        let byte_index = col_pos >> 3;
        let bit = col_pos & 7;
        debug_assert!(byte_index < self.header_size_in_bytes);
        let idx = self.offset + byte_index;
        (self.segment[idx] & (1u8 << bit)) != 0
    }

    pub fn read_boolean(&self, pos: usize) -> Result<(bool, usize)> {
        let (val, next) = self.read_byte(pos)?;
        Ok((val != 0, next))
    }

    pub fn read_byte(&self, pos: usize) -> Result<(u8, usize)> {
        let next = self.checked_pos(pos, 1, "byte")?;
        Ok((self.segment[pos], next))
    }

    pub fn read_short(&self, pos: usize) -> Result<(i16, usize)> {
        let next_pos = self.checked_pos(pos, 2, "short")?;
        let mut arr = [0u8; 2];
        arr.copy_from_slice(&self.segment[pos..next_pos]);
        Ok((i16::from_ne_bytes(arr), next_pos))
    }

    pub fn read_int(&self, pos: usize) -> Result<(i32, usize)> {
        match read_unsigned_varint_at(self.segment, pos, CompactedRowWriter::MAX_INT_SIZE) {
            Ok((value, next_pos)) => Ok((value as i32, next_pos)),
            Err(e) => Err(IllegalArgument {
                message: format!("Invalid VarInt32 input stream at pos {pos}: {e}"),
            }),
        }
    }

    pub fn read_long(&self, pos: usize) -> Result<(i64, usize)> {
        match read_unsigned_varint_u64_at(self.segment, pos, CompactedRowWriter::MAX_LONG_SIZE) {
            Ok((value, next_pos)) => Ok((value as i64, next_pos)),
            Err(e) => Err(IllegalArgument {
                message: format!("Invalid VarInt64 input stream at pos {pos}: {e}"),
            }),
        }
    }

    pub fn read_float(&self, pos: usize) -> Result<(f32, usize)> {
        let next_pos = self.checked_pos(pos, 4, "float")?;
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&self.segment[pos..next_pos]);
        Ok((f32::from_ne_bytes(arr), next_pos))
    }

    pub fn read_double(&self, pos: usize) -> Result<(f64, usize)> {
        let next_pos = self.checked_pos(pos, 8, "double")?;
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&self.segment[pos..next_pos]);
        Ok((f64::from_ne_bytes(arr), next_pos))
    }

    pub fn read_binary(&self, pos: usize) -> Result<(&'a [u8], usize)> {
        self.read_bytes(pos)
    }

    pub fn read_bytes(&self, pos: usize) -> Result<(&'a [u8], usize)> {
        let (len, data_pos) = self.read_int(pos)?;
        let len = usize::try_from(len).map_err(|_| IllegalArgument {
            message: format!("Negative length while reading bytes at pos {pos}: {len}"),
        })?;
        let next_pos = self.checked_pos(data_pos, len, "bytes payload")?;
        Ok((&self.segment[data_pos..next_pos], next_pos))
    }

    pub fn read_string(&self, pos: usize) -> Result<(&'a str, usize)> {
        let (bytes, next_pos) = self.read_bytes(pos)?;
        let s = from_utf8(bytes).map_err(|e| IllegalArgument {
            message: format!("Invalid UTF-8 when reading string at pos {pos}: {e}"),
        })?;
        Ok((s, next_pos))
    }
}

#[cfg(test)]
mod row_type_tests {
    use crate::metadata::{DataType, DataTypes, RowType};
    use crate::row::binary::ValueWriter;
    use crate::row::compacted::compacted_row_reader::{
        CompactedRowDeserializer, CompactedRowReader,
    };
    use crate::row::compacted::compacted_row_writer::CompactedRowWriter;
    use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
    use crate::row::field_getter::FieldGetter;
    use crate::row::{Datum, GenericRow, InternalRow};

    fn round_trip<F>(outer_row_type: &RowType, outer_row: &GenericRow, verify: F)
    where
        F: FnOnce(&GenericRow),
    {
        // Write
        let field_getters = FieldGetter::create_field_getters(outer_row_type);
        let value_writers: Vec<ValueWriter> = outer_row_type
            .fields()
            .iter()
            .map(|f| ValueWriter::create_value_writer(f.data_type(), None).unwrap())
            .collect();
        let mut writer = CompactedRowWriter::new(outer_row_type.fields().len());
        for (i, (getter, vw)) in field_getters.iter().zip(value_writers.iter()).enumerate() {
            let datum = getter.get_field(outer_row as &dyn InternalRow).unwrap();
            vw.write_value(&mut writer, i, &datum).unwrap();
        }
        let bytes = writer.to_bytes();

        // Read
        let deser = CompactedRowDeserializer::new(outer_row_type);
        let reader = CompactedRowReader::new(
            outer_row_type.fields().len(),
            bytes.as_ref(),
            0,
            bytes.len(),
        );
        let result = deser.deserialize(&reader).expect("deserialize");
        verify(&result);
    }

    #[test]
    fn test_row_simple_nesting() {
        let inner_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["x", "label"],
        );
        let outer_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataType::Row(inner_row_type.clone())],
            vec!["id", "nested"],
        );

        let mut inner = GenericRow::new(2);
        inner.set_field(0, 42_i32);
        inner.set_field(1, "hello");

        let mut outer = GenericRow::new(2);
        outer.set_field(0, 1_i32);
        outer.set_field(1, Datum::Row(Box::new(inner)));

        round_trip(&outer_row_type, &outer, |result| {
            assert_eq!(result.get_int(0).unwrap(), 1);
            let nested = result.get_row(1).unwrap();
            assert_eq!(nested.get_int(0).unwrap(), 42);
            assert_eq!(nested.get_string(1).unwrap(), "hello");
        });
    }

    #[test]
    fn test_row_deep_nesting() {
        let inner_inner_row_type =
            RowType::with_data_types_and_field_names(vec![DataTypes::int()], vec!["n"]);
        let inner_row_type = RowType::with_data_types_and_field_names(
            vec![DataType::Row(inner_inner_row_type.clone())],
            vec!["inner"],
        );
        let outer_row_type = RowType::with_data_types_and_field_names(
            vec![DataType::Row(inner_row_type.clone())],
            vec!["outer"],
        );

        let mut innermost = GenericRow::new(1);
        innermost.set_field(0, 99_i32);

        let mut middle = GenericRow::new(1);
        middle.set_field(0, Datum::Row(Box::new(innermost)));

        let mut outer = GenericRow::new(1);
        outer.set_field(0, Datum::Row(Box::new(middle)));

        round_trip(&outer_row_type, &outer, |result| {
            let mid = result.get_row(0).unwrap();
            let inner = mid.get_row(0).unwrap();
            assert_eq!(inner.get_int(0).unwrap(), 99);
        });
    }

    #[test]
    fn test_row_with_nullable_fields() {
        // Outer nullable ROW column; nested row with a nullable STRING field set to null
        let inner_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["id", "optional_name"],
        );
        let outer_row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataType::Row(inner_row_type.clone())],
            vec!["k", "nested"],
        );

        // Case 1: non-null nested row with a null field inside
        let mut inner = GenericRow::new(2);
        inner.set_field(0, 7_i32);
        inner.set_field(1, Datum::Null);

        let mut outer = GenericRow::new(2);
        outer.set_field(0, 10_i32);
        outer.set_field(1, Datum::Row(Box::new(inner)));

        round_trip(&outer_row_type, &outer, |result| {
            assert_eq!(result.get_int(0).unwrap(), 10);
            let nested = result.get_row(1).unwrap();
            assert_eq!(nested.get_int(0).unwrap(), 7);
            assert!(nested.is_null_at(1).unwrap());
        });

        // Case 2: outer ROW column is null
        let mut outer_null = GenericRow::new(2);
        outer_null.set_field(0, 20_i32);
        outer_null.set_field(1, Datum::Null);

        round_trip(&outer_row_type, &outer_null, |result2| {
            assert_eq!(result2.get_int(0).unwrap(), 20);
            assert!(result2.is_null_at(1).unwrap());
        });
    }

    #[test]
    fn test_row_all_primitives_round_trip() {
        let inner_row_type = RowType::with_data_types_and_field_names(
            vec![
                DataTypes::boolean(),
                DataTypes::tinyint(),
                DataTypes::smallint(),
                DataTypes::int(),
                DataTypes::bigint(),
                DataTypes::float(),
                DataTypes::double(),
                DataTypes::string(),
                DataTypes::bytes(),
                DataTypes::date(),
                DataTypes::time(),
                DataTypes::timestamp(),
                DataTypes::timestamp_ltz(),
            ],
            vec![
                "b", "tin", "sm", "i", "lo", "fl", "db", "str", "by", "dt", "ti", "tsn", "tsl",
            ],
        );
        let outer_row_type = RowType::with_data_types_and_field_names(
            vec![DataType::Row(inner_row_type.clone())],
            vec!["nested"],
        );

        let mut inner = GenericRow::new(13);
        inner.set_field(0, true);
        inner.set_field(1, 7_i8);
        inner.set_field(2, -42_i16);
        inner.set_field(3, 100_000_i32);
        inner.set_field(4, 9_876_543_210_i64);
        inner.set_field(5, std::f32::consts::PI);
        inner.set_field(6, std::f64::consts::E);
        inner.set_field(7, "hello world");
        inner.set_field(8, b"binary".as_slice());
        inner.set_field(9, Datum::Date(Date::new(20476)));
        inner.set_field(10, Datum::Time(Time::new(36_827_123)));
        inner.set_field(
            11,
            Datum::TimestampNtz(TimestampNtz::new(1_769_163_227_123)),
        );
        inner.set_field(
            12,
            Datum::TimestampLtz(TimestampLtz::new(1_769_163_227_123)),
        );

        let mut outer = GenericRow::new(1);
        outer.set_field(0, Datum::Row(Box::new(inner)));

        round_trip(&outer_row_type, &outer, |result| {
            let n = result.get_row(0).unwrap();
            assert!(n.get_boolean(0).unwrap());
            assert_eq!(n.get_byte(1).unwrap(), 7);
            assert_eq!(n.get_short(2).unwrap(), -42);
            assert_eq!(n.get_int(3).unwrap(), 100_000);
            assert_eq!(n.get_long(4).unwrap(), 9_876_543_210);
            assert!((n.get_float(5).unwrap() - std::f32::consts::PI).abs() < f32::EPSILON);
            assert!((n.get_double(6).unwrap() - std::f64::consts::E).abs() < f64::EPSILON);
            assert_eq!(n.get_string(7).unwrap(), "hello world");
            assert_eq!(n.get_bytes(8).unwrap(), b"binary");
            assert_eq!(n.get_date(9).unwrap().get_inner(), 20476);
            assert_eq!(n.get_time(10).unwrap().get_inner(), 36_827_123);
            assert_eq!(
                n.get_timestamp_ntz(11, 6).unwrap().get_millisecond(),
                1_769_163_227_123,
            );
            assert_eq!(
                n.get_timestamp_ltz(12, 6).unwrap().get_epoch_millisecond(),
                1_769_163_227_123,
            );
        });
    }
}
