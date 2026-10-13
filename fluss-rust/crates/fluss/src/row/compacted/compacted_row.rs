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

use crate::client::WriteFormat;
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::RowType;
use crate::row::compacted::compacted_row_reader::{CompactedRowDeserializer, CompactedRowReader};
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
use crate::row::view::{ArrayView, MapView, RowView};
use crate::row::{DataGetters, Decimal, GenericRow, InternalRow};
use std::sync::{Arc, OnceLock};

pub fn calculate_bit_set_width_in_bytes(arity: usize) -> usize {
    arity.div_ceil(8)
}

// Reference implementation:
// https://github.com/apache/fluss/blob/main/fluss-common/src/main/java/org/apache/fluss/row/compacted/CompactedRow.java
#[allow(dead_code)]
pub struct CompactedRow<'a> {
    arity: usize,
    size_in_bytes: usize,
    decoded_row: OnceLock<GenericRow<'a>>,
    deserializer: Arc<CompactedRowDeserializer<'a>>,
    reader: CompactedRowReader<'a>,
    data: &'a [u8],
}

#[allow(dead_code)]
impl<'a> CompactedRow<'a> {
    pub fn from_bytes(row_type: &'a RowType, data: &'a [u8]) -> Self {
        Self::deserialize(
            Arc::new(CompactedRowDeserializer::new(row_type)),
            row_type.fields().len(),
            data,
        )
    }

    pub fn deserialize(
        deserializer: Arc<CompactedRowDeserializer<'a>>,
        arity: usize,
        data: &'a [u8],
    ) -> Self {
        Self {
            arity,
            size_in_bytes: data.len(),
            decoded_row: OnceLock::new(),
            deserializer: Arc::clone(&deserializer),
            reader: CompactedRowReader::new(arity, data, 0, data.len()),
            data,
        }
    }

    pub fn get_size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    fn decoded_row(&self) -> Result<&GenericRow<'_>> {
        if let Some(row) = self.decoded_row.get() {
            return Ok(row);
        }

        // `OnceLock::get_or_try_init` is still unstable on our toolchain.
        // Keep the same semantics by performing the fallible decode first,
        // then atomically installing it via `get_or_init`.
        let decoded = self.deserializer.deserialize(&self.reader)?;
        Ok(self.decoded_row.get_or_init(|| decoded))
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data
    }
}

impl<'a> InternalRow for CompactedRow<'a> {
    fn get_field_count(&self) -> usize {
        self.arity
    }

    fn as_encoded_bytes(&self, write_format: WriteFormat) -> Option<&[u8]> {
        match write_format {
            WriteFormat::CompactedKv => Some(self.as_bytes()),
            WriteFormat::ArrowLog => None,
            WriteFormat::CompactedLog => None,
        }
    }
}

impl<'a> DataGetters for CompactedRow<'a> {
    fn is_null_at(&self, pos: usize) -> Result<bool> {
        let fields = self.deserializer.get_row_type().fields();
        if pos >= fields.len() {
            return Err(IllegalArgument {
                message: format!(
                    "position {pos} out of bounds (row has {} fields)",
                    fields.len()
                ),
            });
        }
        Ok(fields.as_slice()[pos].data_type.is_nullable() && self.reader.is_null_at(pos))
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        self.decoded_row()?.get_boolean(pos)
    }

    fn get_byte(&self, pos: usize) -> Result<i8> {
        self.decoded_row()?.get_byte(pos)
    }

    fn get_short(&self, pos: usize) -> Result<i16> {
        self.decoded_row()?.get_short(pos)
    }

    fn get_int(&self, pos: usize) -> Result<i32> {
        self.decoded_row()?.get_int(pos)
    }

    fn get_long(&self, pos: usize) -> Result<i64> {
        self.decoded_row()?.get_long(pos)
    }

    fn get_float(&self, pos: usize) -> Result<f32> {
        self.decoded_row()?.get_float(pos)
    }

    fn get_double(&self, pos: usize) -> Result<f64> {
        self.decoded_row()?.get_double(pos)
    }

    fn get_char(&self, pos: usize, length: usize) -> Result<&str> {
        self.decoded_row()?.get_char(pos, length)
    }

    fn get_string(&self, pos: usize) -> Result<&str> {
        self.decoded_row()?.get_string(pos)
    }

    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        self.decoded_row()?.get_decimal(pos, precision, scale)
    }

    fn get_date(&self, pos: usize) -> Result<Date> {
        self.decoded_row()?.get_date(pos)
    }

    fn get_time(&self, pos: usize) -> Result<Time> {
        self.decoded_row()?.get_time(pos)
    }

    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        self.decoded_row()?.get_timestamp_ntz(pos, precision)
    }

    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        self.decoded_row()?.get_timestamp_ltz(pos, precision)
    }

    fn get_binary(&self, pos: usize, length: usize) -> Result<&[u8]> {
        self.decoded_row()?.get_binary(pos, length)
    }

    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        self.decoded_row()?.get_bytes(pos)
    }

    fn get_array(&self, pos: usize) -> Result<ArrayView<'_>> {
        self.decoded_row()?.get_array(pos)
    }

    fn get_map(&self, pos: usize) -> Result<MapView<'_>> {
        self.decoded_row()?.get_map(pos)
    }

    fn get_row(&self, pos: usize) -> Result<RowView<'_>> {
        self.decoded_row()?.get_row(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::row::binary::BinaryWriter;
    use crate::row::binary_array::FlussArrayWriter;
    use crate::row::binary_map::FlussMapWriter;
    use crate::row::{InternalArray, InternalMap};

    use crate::metadata::{
        BigIntType, BooleanType, BytesType, DataType, DoubleType, FloatType, IntType, SmallIntType,
        StringType, TinyIntType,
    };
    use crate::row::Datum;
    use crate::row::compacted::compacted_row_writer::CompactedRowWriter;

    #[test]
    fn test_compacted_row() {
        // Test all primitive types
        let row_type = RowType::with_data_types(vec![
            DataType::Boolean(BooleanType::new()),
            DataType::TinyInt(TinyIntType::new()),
            DataType::SmallInt(SmallIntType::new()),
            DataType::Int(IntType::new()),
            DataType::BigInt(BigIntType::new()),
            DataType::Float(FloatType::new()),
            DataType::Double(DoubleType::new()),
            DataType::String(StringType::new()),
            DataType::Bytes(BytesType::new()),
        ]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        writer.write_boolean(true);
        writer.write_byte(1);
        writer.write_short(100);
        writer.write_int(1000);
        writer.write_long(10000);
        writer.write_float(1.5);
        writer.write_double(2.5);
        writer.write_string("Hello World");
        writer.write_bytes(&[1, 2, 3, 4, 5]);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        assert_eq!(row.get_field_count(), 9);
        assert!(row.get_boolean(0).unwrap());
        assert_eq!(row.get_byte(1).unwrap(), 1);
        assert_eq!(row.get_short(2).unwrap(), 100);
        assert_eq!(row.get_int(3).unwrap(), 1000);
        assert_eq!(row.get_long(4).unwrap(), 10000);
        assert_eq!(row.get_float(5).unwrap(), 1.5);
        assert_eq!(row.get_double(6).unwrap(), 2.5);
        assert_eq!(row.get_string(7).unwrap(), "Hello World");
        assert_eq!(row.get_bytes(8).unwrap(), &[1, 2, 3, 4, 5]);

        // Test with nulls and negative values
        let row_type = RowType::with_data_types(vec![
            DataType::Int(IntType::new()),
            DataType::String(StringType::new()),
            DataType::Double(DoubleType::new()),
        ]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());
        writer.write_int(-42);
        writer.set_null_at(1);
        writer.write_double(2.71);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        assert!(!row.is_null_at(0).unwrap());
        assert!(row.is_null_at(1).unwrap());
        assert!(!row.is_null_at(2).unwrap());
        assert_eq!(row.get_int(0).unwrap(), -42);
        assert_eq!(row.get_double(2).unwrap(), 2.71);
        // Verify caching works on repeated reads
        assert_eq!(row.get_int(0).unwrap(), -42);
    }

    #[test]
    fn test_compacted_row_temporal_and_decimal_types() {
        // Comprehensive test covering DATE, TIME, TIMESTAMP (compact/non-compact), and DECIMAL (compact/non-compact)
        use crate::metadata::{DecimalType, TimestampLTzType, TimestampType};
        use crate::row::Decimal;
        use crate::row::datum::{TimestampLtz, TimestampNtz};
        use bigdecimal::{BigDecimal, num_bigint::BigInt};

        let row_type = RowType::with_data_types(vec![
            DataTypes::date(),
            DataTypes::time(),
            DataType::Timestamp(TimestampType::with_nullable(true, 3).unwrap()), // Compact (precision <= 3)
            DataType::TimestampLTz(TimestampLTzType::with_nullable(true, 3).unwrap()), // Compact
            DataType::Timestamp(TimestampType::with_nullable(true, 6).unwrap()), // Non-compact (precision > 3)
            DataType::TimestampLTz(TimestampLTzType::with_nullable(true, 9).unwrap()), // Non-compact
            DataType::Decimal(DecimalType::new(10, 2).unwrap()), // Compact (precision <= 18)
            DataType::Decimal(DecimalType::new(28, 10).unwrap()), // Non-compact (precision > 18)
        ]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        // Write values
        writer.write_int(19651); // Date: 2023-10-25
        writer.write_time(34200000, 0); // Time: 09:30:00.0
        writer.write_timestamp_ntz(&TimestampNtz::new(1698235273182), 3); // Compact timestamp
        writer.write_timestamp_ltz(&TimestampLtz::new(1698235273182), 3); // Compact timestamp ltz
        let ts_ntz_high = TimestampNtz::from_millis_nanos(1698235273182, 123456).unwrap();
        let ts_ltz_high = TimestampLtz::from_millis_nanos(1698235273182, 987654).unwrap();
        writer.write_timestamp_ntz(&ts_ntz_high, 6); // Non-compact timestamp with nanos
        writer.write_timestamp_ltz(&ts_ltz_high, 9); // Non-compact timestamp ltz with nanos

        // Create Decimal values for testing
        let small_decimal =
            Decimal::from_big_decimal(BigDecimal::new(BigInt::from(12345), 2), 10, 2).unwrap(); // Compact decimal: 123.45
        let large_decimal = Decimal::from_big_decimal(
            BigDecimal::new(BigInt::from(999999999999999999i128), 10),
            28,
            10,
        )
        .unwrap(); // Non-compact decimal

        writer.write_decimal(&small_decimal, 10);
        writer.write_decimal(&large_decimal, 28);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        // Verify all values
        assert_eq!(row.get_date(0).unwrap().get_inner(), 19651);
        assert_eq!(row.get_time(1).unwrap().get_inner(), 34200000);
        assert_eq!(
            row.get_timestamp_ntz(2, 3).unwrap().get_millisecond(),
            1698235273182
        );
        assert_eq!(
            row.get_timestamp_ltz(3, 3).unwrap().get_epoch_millisecond(),
            1698235273182
        );
        let read_ts_ntz = row.get_timestamp_ntz(4, 6).unwrap();
        assert_eq!(read_ts_ntz.get_millisecond(), 1698235273182);
        assert_eq!(read_ts_ntz.get_nano_of_millisecond(), 123456);
        let read_ts_ltz = row.get_timestamp_ltz(5, 9).unwrap();
        assert_eq!(read_ts_ltz.get_epoch_millisecond(), 1698235273182);
        assert_eq!(read_ts_ltz.get_nano_of_millisecond(), 987654);
        // Assert on Decimal equality
        assert_eq!(row.get_decimal(6, 10, 2).unwrap(), small_decimal);
        assert_eq!(row.get_decimal(7, 28, 10).unwrap(), large_decimal);

        // Assert on Decimal components to catch any regressions
        let read_small_decimal = row.get_decimal(6, 10, 2).unwrap();
        assert_eq!(read_small_decimal.precision(), 10);
        assert_eq!(read_small_decimal.scale(), 2);
        assert_eq!(read_small_decimal.to_unscaled_long().unwrap(), 12345);

        let read_large_decimal = row.get_decimal(7, 28, 10).unwrap();
        assert_eq!(read_large_decimal.precision(), 28);
        assert_eq!(read_large_decimal.scale(), 10);
        assert_eq!(
            read_large_decimal.to_unscaled_long().unwrap(),
            999999999999999999i64
        );
    }

    #[test]
    fn test_compacted_row_int_array() {
        let row_type =
            RowType::with_data_types(vec![DataTypes::int(), DataTypes::array(DataTypes::int())]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());
        writer.write_int(42);

        let elem_type = DataTypes::int();
        let mut arr_writer = FlussArrayWriter::new(3, &elem_type);
        arr_writer.write_int(0, 1);
        arr_writer.write_int(1, 2);
        arr_writer.write_int(2, 3);
        let arr = arr_writer.complete().unwrap();
        writer.write_array(&arr);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        assert_eq!(row.get_int(0).unwrap(), 42);
        let read_arr = row.get_array(1).unwrap();
        assert_eq!(read_arr.size(), 3);
        assert_eq!(read_arr.get_int(0).unwrap(), 1);
        assert_eq!(read_arr.get_int(1).unwrap(), 2);
        assert_eq!(read_arr.get_int(2).unwrap(), 3);
    }

    #[test]
    fn test_compacted_row_string_array() {
        let row_type = RowType::with_data_types(vec![DataTypes::array(DataTypes::string())]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        let elem_type = DataTypes::string();
        let mut arr_writer = FlussArrayWriter::new(3, &elem_type);
        arr_writer.write_string(0, "hello");
        arr_writer.write_string(1, "fluss");
        arr_writer.write_string(2, "rust");
        let arr = arr_writer.complete().unwrap();
        writer.write_array(&arr);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        let read_arr = row.get_array(0).unwrap();
        assert_eq!(read_arr.size(), 3);
        assert_eq!(read_arr.get_string(0).unwrap(), "hello");
        assert_eq!(read_arr.get_string(1).unwrap(), "fluss");
        assert_eq!(read_arr.get_string(2).unwrap(), "rust");
    }

    #[test]
    fn test_compacted_row_array_with_nulls() {
        let row_type = RowType::with_data_types(vec![DataTypes::array(DataTypes::int())]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        let elem_type = DataTypes::int();
        let mut arr_writer = FlussArrayWriter::new(3, &elem_type);
        arr_writer.write_int(0, 10);
        arr_writer.set_null_at(1);
        arr_writer.write_int(2, 30);
        let arr = arr_writer.complete().unwrap();
        writer.write_array(&arr);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        let read_arr = row.get_array(0).unwrap();
        assert_eq!(read_arr.size(), 3);
        assert!(!read_arr.is_null_at(0).unwrap());
        assert_eq!(read_arr.get_int(0).unwrap(), 10);
        assert!(read_arr.is_null_at(1).unwrap());
        assert!(!read_arr.is_null_at(2).unwrap());
        assert_eq!(read_arr.get_int(2).unwrap(), 30);
    }

    #[test]
    fn test_compacted_row_empty_array() {
        let row_type = RowType::with_data_types(vec![DataTypes::array(DataTypes::int())]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        let elem_type = DataTypes::int();
        let arr_writer = FlussArrayWriter::new(0, &elem_type);
        let arr = arr_writer.complete().unwrap();
        writer.write_array(&arr);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        let read_arr = row.get_array(0).unwrap();
        assert_eq!(read_arr.size(), 0);
    }

    #[test]
    fn test_compacted_row_nested_array() {
        let row_type =
            RowType::with_data_types(vec![DataTypes::array(DataTypes::array(DataTypes::int()))]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        // Build inner arrays
        let inner_type = DataTypes::int();
        let mut inner1 = FlussArrayWriter::new(2, &inner_type);
        inner1.write_int(0, 1);
        inner1.write_int(1, 2);
        let inner1_arr = inner1.complete().unwrap();

        let mut inner2 = FlussArrayWriter::new(1, &inner_type);
        inner2.write_int(0, 99);
        let inner2_arr = inner2.complete().unwrap();

        // Build outer array
        let outer_type = DataTypes::array(DataTypes::int());
        let mut outer_writer = FlussArrayWriter::new(2, &outer_type);
        outer_writer.write_array(0, &inner1_arr);
        outer_writer.write_array(1, &inner2_arr);
        let outer_arr = outer_writer.complete().unwrap();

        writer.write_array(&outer_arr);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        let read_outer = row.get_array(0).unwrap();
        assert_eq!(read_outer.size(), 2);

        let nested1 = read_outer.get_array(0).unwrap();
        assert_eq!(nested1.size(), 2);
        assert_eq!(nested1.get_int(0).unwrap(), 1);
        assert_eq!(nested1.get_int(1).unwrap(), 2);

        let nested2 = read_outer.get_array(1).unwrap();
        assert_eq!(nested2.size(), 1);
        assert_eq!(nested2.get_int(0).unwrap(), 99);
    }

    #[test]
    fn test_compacted_row_map() {
        let row_type =
            RowType::with_data_types(vec![DataTypes::map(DataTypes::int(), DataTypes::string())]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        let mut map_writer = FlussMapWriter::new(2, &DataTypes::int(), &DataTypes::string());
        map_writer.write_entry(1.into(), "a".into()).unwrap();
        map_writer.write_entry(2.into(), "b".into()).unwrap();
        let map = map_writer.complete().unwrap();
        writer.write_map(&map);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        let read_map = row.get_map(0).unwrap();
        assert_eq!(read_map.size(), 2);
        assert_eq!(read_map.key_array().get_int(0).unwrap(), 1);
        assert_eq!(read_map.value_array().get_string(0).unwrap(), "a");
    }

    #[test]
    fn test_compacted_row_map_with_nulls() {
        // Row with two columns: an INT and a nullable MAP
        let row_type = RowType::with_data_types(vec![
            DataTypes::int(),
            DataTypes::map(DataTypes::int(), DataTypes::string()),
        ]);

        // Write row with null map
        let mut writer = CompactedRowWriter::new(row_type.fields().len());
        writer.write_int(42);
        writer.set_null_at(1);
        writer.complete();

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        assert_eq!(row.get_int(0).unwrap(), 42);
        assert!(row.is_null_at(1).unwrap());

        // Write row with non-null map
        writer.reset();
        writer.write_int(99);
        let mut map_writer = FlussMapWriter::new(1, &DataTypes::int(), &DataTypes::string());
        map_writer.write_entry(7.into(), "hello".into()).unwrap();
        let map = map_writer.complete().unwrap();
        writer.write_map(&map);
        writer.complete();

        let bytes2 = writer.to_bytes();
        let row2 = CompactedRow::from_bytes(&row_type, bytes2.as_ref());
        assert_eq!(row2.get_int(0).unwrap(), 99);
        assert!(!row2.is_null_at(1).unwrap());
        let read_map = row2.get_map(1).unwrap();
        assert_eq!(read_map.size(), 1);
        assert_eq!(read_map.key_array().get_int(0).unwrap(), 7);
        assert_eq!(read_map.value_array().get_string(0).unwrap(), "hello");
    }

    #[test]
    fn test_compacted_row_nested_map() {
        // Map<STRING, ARRAY<INT>>
        let row_type = RowType::with_data_types(vec![DataTypes::map(
            DataTypes::string(),
            DataTypes::array(DataTypes::int()),
        )]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        // Values: [[1, 2], [3]]
        let inner_type = DataTypes::int();
        let mut inner1 = FlussArrayWriter::new(2, &inner_type);
        inner1.write_int(0, 1);
        inner1.write_int(1, 2);
        let inner1_arr = inner1.complete().unwrap();

        let mut inner2 = FlussArrayWriter::new(1, &inner_type);
        inner2.write_int(0, 3);
        let inner2_arr = inner2.complete().unwrap();

        let array_type = DataTypes::array(DataTypes::int());

        let mut map_writer = FlussMapWriter::new(2, &DataTypes::string(), &array_type);
        map_writer
            .write_entry("a".into(), Datum::Array(inner1_arr))
            .unwrap();
        map_writer
            .write_entry("b".into(), Datum::Array(inner2_arr))
            .unwrap();
        let map = map_writer.complete().unwrap();
        writer.write_map(&map);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        let read_map = row.get_map(0).unwrap();
        assert_eq!(read_map.size(), 2);
        assert_eq!(read_map.key_array().get_string(0).unwrap(), "a");
        assert_eq!(read_map.key_array().get_string(1).unwrap(), "b");

        let nested1 = read_map.value_array().get_array(0).unwrap();
        assert_eq!(nested1.size(), 2);
        assert_eq!(nested1.get_int(0).unwrap(), 1);
        assert_eq!(nested1.get_int(1).unwrap(), 2);

        let nested2 = read_map.value_array().get_array(1).unwrap();
        assert_eq!(nested2.size(), 1);
        assert_eq!(nested2.get_int(0).unwrap(), 3);
    }

    #[test]
    fn test_compacted_row_empty_map() {
        let row_type =
            RowType::with_data_types(vec![DataTypes::map(DataTypes::int(), DataTypes::string())]);

        let mut writer = CompactedRowWriter::new(row_type.fields().len());

        let map_writer = FlussMapWriter::new(0, &DataTypes::int(), &DataTypes::string());
        let map = map_writer.complete().unwrap();
        writer.write_map(&map);

        let bytes = writer.to_bytes();
        let row = CompactedRow::from_bytes(&row_type, bytes.as_ref());

        let read_map = row.get_map(0).unwrap();
        assert_eq!(read_map.size(), 0);
    }
}
