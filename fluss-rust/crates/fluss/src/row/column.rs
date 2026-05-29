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
use crate::record::from_arrow_field;
use crate::row::binary_array::FlussArrayWriter;
use crate::row::binary_map::FlussMap;
use crate::row::datum::{Date, Datum, Time, TimestampLtz, TimestampNtz};
use crate::row::{Decimal, FlussArray, GenericRow, InternalRow};
use arrow::array::{
    Array, AsArray, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray,
    RecordBatch, StringArray, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{
    DataType as ArrowDataType, Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct ColumnarRow {
    record_batch: Arc<RecordBatch>,
    row_type: Arc<RowType>,
    row_id: usize,
    fluss_row_type: Option<Arc<RowType>>,
    row_column_indices: Arc<[usize]>,
    row_caches: Box<[std::sync::OnceLock<GenericRow<'static>>]>,
}

pub(crate) fn fluss_row_column_indices(row_type: &RowType) -> Arc<[usize]> {
    row_type
        .fields()
        .iter()
        .enumerate()
        .filter_map(|(i, f)| matches!(f.data_type, DataType::Row(_)).then_some(i))
        .collect()
}

pub(crate) fn arrow_row_column_indices(batch: &RecordBatch) -> Arc<[usize]> {
    batch
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, c)| matches!(c.data_type(), ArrowDataType::Struct(_)).then_some(i))
        .collect()
}

fn make_row_caches(indices: &[usize]) -> Box<[std::sync::OnceLock<GenericRow<'static>>]> {
    indices.iter().map(|_| std::sync::OnceLock::new()).collect()
}

impl ColumnarRow {
    pub fn new(
        batch: Arc<RecordBatch>,
        row_type: Arc<RowType>,
        row_id: usize,
        fluss_row_type: Option<Arc<RowType>>,
    ) -> Self {
        let row_column_indices = match &fluss_row_type {
            Some(rt) => fluss_row_column_indices(rt),
            None => arrow_row_column_indices(&batch),
        };
        Self::with_indices(batch, row_type, row_id, fluss_row_type, row_column_indices)
    }

    pub(crate) fn with_indices(
        batch: Arc<RecordBatch>,
        row_type: Arc<RowType>,
        row_id: usize,
        fluss_row_type: Option<Arc<RowType>>,
        row_column_indices: Arc<[usize]>,
    ) -> Self {
        let row_caches = make_row_caches(&row_column_indices);
        ColumnarRow {
            record_batch: batch,
            row_type,
            row_id,
            fluss_row_type,
            row_column_indices,
            row_caches,
        }
    }

    pub fn fluss_row_type(&self) -> Option<&Arc<RowType>> {
        self.fluss_row_type.as_ref()
    }

    pub fn set_row_id(&mut self, row_id: usize) {
        self.row_id = row_id;
        for lock in self.row_caches.iter_mut() {
            *lock = std::sync::OnceLock::new();
        }
    }

    pub fn get_row_id(&self) -> usize {
        self.row_id
    }

    pub fn get_record_batch(&self) -> &RecordBatch {
        &self.record_batch
    }

    fn column(&self, pos: usize) -> Result<&Arc<dyn Array>> {
        self.record_batch
            .columns()
            .get(pos)
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "column index {pos} out of bounds (batch has {} columns)",
                    self.record_batch.num_columns()
                ),
            })
    }

    /// Generic helper to read timestamp from Arrow, handling all TimeUnit conversions.
    /// Like Java, the precision parameter is ignored - conversion is determined by Arrow TimeUnit.
    fn read_timestamp_from_arrow<T>(
        &self,
        pos: usize,
        _precision: u32,
        construct_compact: impl FnOnce(i64) -> T,
        construct_with_nanos: impl FnOnce(i64, i32) -> Result<T>,
    ) -> Result<T> {
        let column = self.column(pos)?;

        // Read value and time unit based on the actual Arrow timestamp type
        let (value, time_unit) = match column.data_type() {
            ArrowDataType::Timestamp(TimeUnit::Second, _) => (
                column
                    .as_primitive_opt::<TimestampSecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected TimestampSecondArray at position {pos}"),
                    })?
                    .value(self.row_id),
                TimeUnit::Second,
            ),
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => (
                column
                    .as_primitive_opt::<TimestampMillisecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected TimestampMillisecondArray at position {pos}"),
                    })?
                    .value(self.row_id),
                TimeUnit::Millisecond,
            ),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => (
                column
                    .as_primitive_opt::<TimestampMicrosecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected TimestampMicrosecondArray at position {pos}"),
                    })?
                    .value(self.row_id),
                TimeUnit::Microsecond,
            ),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => (
                column
                    .as_primitive_opt::<TimestampNanosecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected TimestampNanosecondArray at position {pos}"),
                    })?
                    .value(self.row_id),
                TimeUnit::Nanosecond,
            ),
            other => {
                return Err(IllegalArgument {
                    message: format!("expected Timestamp column at position {pos}, got {other:?}"),
                });
            }
        };

        // Convert based on Arrow TimeUnit
        let (millis, nanos) = match time_unit {
            TimeUnit::Second => (value * 1000, 0),
            TimeUnit::Millisecond => (value, 0),
            TimeUnit::Microsecond => {
                // Use Euclidean division so that nanos is always non-negative,
                // even for timestamps before the Unix epoch.
                let millis = value.div_euclid(1000);
                let nanos = (value.rem_euclid(1000) * 1000) as i32;
                (millis, nanos)
            }
            TimeUnit::Nanosecond => {
                // Use Euclidean division so that nanos is always in [0, 999_999].
                let millis = value.div_euclid(1_000_000);
                let nanos = value.rem_euclid(1_000_000) as i32;
                (millis, nanos)
            }
        };

        if nanos == 0 {
            Ok(construct_compact(millis))
        } else {
            construct_with_nanos(millis, nanos)
        }
    }

    /// Read date value from Arrow Date32Array
    fn read_date_from_arrow(&self, pos: usize) -> Result<i32> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Date32Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected Date32Array at position {pos}"),
            })?
            .value(self.row_id))
    }

    /// Read time value from Arrow Time32/Time64 arrays, converting to milliseconds
    fn read_time_from_arrow(&self, pos: usize) -> Result<i32> {
        let column = self.column(pos)?;

        match column.data_type() {
            ArrowDataType::Time32(TimeUnit::Second) => {
                let value = column
                    .as_primitive_opt::<Time32SecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected Time32SecondArray at position {pos}"),
                    })?
                    .value(self.row_id);
                Ok(value * 1000) // Convert seconds to milliseconds
            }
            ArrowDataType::Time32(TimeUnit::Millisecond) => Ok(column
                .as_primitive_opt::<Time32MillisecondType>()
                .ok_or_else(|| IllegalArgument {
                    message: format!("expected Time32MillisecondArray at position {pos}"),
                })?
                .value(self.row_id)),
            ArrowDataType::Time64(TimeUnit::Microsecond) => {
                let value = column
                    .as_primitive_opt::<Time64MicrosecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected Time64MicrosecondArray at position {pos}"),
                    })?
                    .value(self.row_id);
                Ok((value / 1000) as i32) // Convert microseconds to milliseconds
            }
            ArrowDataType::Time64(TimeUnit::Nanosecond) => {
                let value = column
                    .as_primitive_opt::<Time64NanosecondType>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected Time64NanosecondArray at position {pos}"),
                    })?
                    .value(self.row_id);
                Ok((value / 1_000_000) as i32) // Convert nanoseconds to milliseconds
            }
            other => Err(IllegalArgument {
                message: format!("expected Time column at position {pos}, got {other:?}"),
            }),
        }
    }
}

fn extract_struct_from_array(
    array: &dyn Array,
    row_id: usize,
    row_type: Option<&RowType>,
) -> Result<GenericRow<'static>> {
    let sa = array
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| IllegalArgument {
            message: format!("expected StructArray, got {:?}", array.data_type()),
        })?;
    if let Some(rt) = row_type
        && rt.fields().len() != sa.num_columns()
    {
        return Err(IllegalArgument {
            message: format!(
                "Fluss RowType has {} fields but Arrow StructArray has {}",
                rt.fields().len(),
                sa.num_columns(),
            ),
        });
    }
    let mut values = Vec::with_capacity(sa.num_columns());
    for i in 0..sa.num_columns() {
        let child = sa.column(i);
        let fluss_type = row_type.map(|rt| &rt.fields()[i].data_type);
        values.push(arrow_value_to_datum(child.as_ref(), row_id, fluss_type)?);
    }
    Ok(GenericRow { values })
}

fn arrow_value_to_datum(
    array: &dyn Array,
    row_id: usize,
    fluss_type: Option<&DataType>,
) -> Result<Datum<'static>> {
    if array.is_null(row_id) {
        return Ok(Datum::Null);
    }

    macro_rules! downcast {
        ($ty:ty) => {
            array
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| IllegalArgument {
                    message: format!(
                        "expected {} for arrow type {:?}",
                        stringify!($ty),
                        array.data_type()
                    ),
                })?
        };
    }

    match array.data_type() {
        ArrowDataType::Boolean => Ok(Datum::Bool(downcast!(BooleanArray).value(row_id))),
        ArrowDataType::Int8 => Ok(Datum::Int8(downcast!(Int8Array).value(row_id))),
        ArrowDataType::Int16 => Ok(Datum::Int16(downcast!(Int16Array).value(row_id))),
        ArrowDataType::Int32 => Ok(Datum::Int32(downcast!(Int32Array).value(row_id))),
        ArrowDataType::Int64 => Ok(Datum::Int64(downcast!(Int64Array).value(row_id))),
        ArrowDataType::Float32 => Ok(Datum::Float32(downcast!(Float32Array).value(row_id).into())),
        ArrowDataType::Float64 => Ok(Datum::Float64(downcast!(Float64Array).value(row_id).into())),
        ArrowDataType::Utf8 => Ok(Datum::String(std::borrow::Cow::Owned(
            downcast!(StringArray).value(row_id).to_owned(),
        ))),
        ArrowDataType::Binary => Ok(Datum::Blob(std::borrow::Cow::Owned(
            downcast!(BinaryArray).value(row_id).to_vec(),
        ))),
        ArrowDataType::FixedSizeBinary(_) => Ok(Datum::Blob(std::borrow::Cow::Owned(
            downcast!(FixedSizeBinaryArray).value(row_id).to_vec(),
        ))),
        ArrowDataType::Decimal128(p, s) => {
            let (p, s) = (*p, *s);
            let i128_val = downcast!(Decimal128Array).value(row_id);
            Ok(Datum::Decimal(Decimal::from_arrow_decimal128(
                i128_val, s as i64, p as u32, s as u32,
            )?))
        }
        ArrowDataType::Date32 => Ok(Datum::Date(Date::new(downcast!(Date32Array).value(row_id)))),
        ArrowDataType::Time32(TimeUnit::Second) => Ok(Datum::Time(Time::new(
            downcast!(Time32SecondArray).value(row_id) * 1000,
        ))),
        ArrowDataType::Time32(TimeUnit::Millisecond) => Ok(Datum::Time(Time::new(
            downcast!(Time32MillisecondArray).value(row_id),
        ))),
        ArrowDataType::Time64(TimeUnit::Microsecond) => Ok(Datum::Time(Time::new(
            (downcast!(Time64MicrosecondArray).value(row_id) / 1000) as i32,
        ))),
        ArrowDataType::Time64(TimeUnit::Nanosecond) => Ok(Datum::Time(Time::new(
            (downcast!(Time64NanosecondArray).value(row_id) / 1_000_000) as i32,
        ))),
        ArrowDataType::Timestamp(time_unit, _tz) => {
            let value: i64 = match time_unit {
                TimeUnit::Second => downcast!(TimestampSecondArray).value(row_id),
                TimeUnit::Millisecond => downcast!(TimestampMillisecondArray).value(row_id),
                TimeUnit::Microsecond => downcast!(TimestampMicrosecondArray).value(row_id),
                TimeUnit::Nanosecond => downcast!(TimestampNanosecondArray).value(row_id),
            };
            let (millis, nanos) = match time_unit {
                TimeUnit::Second => (value * 1000, 0i32),
                TimeUnit::Millisecond => (value, 0i32),
                TimeUnit::Microsecond => {
                    let millis = value.div_euclid(1000);
                    let nanos = (value.rem_euclid(1000) * 1000) as i32;
                    (millis, nanos)
                }
                TimeUnit::Nanosecond => {
                    let millis = value.div_euclid(1_000_000);
                    let nanos = value.rem_euclid(1_000_000) as i32;
                    (millis, nanos)
                }
            };
            // TIMESTAMP and TIMESTAMP_LTZ both map to `Timestamp(unit, None)` in Arrow.
            let is_ltz = matches!(fluss_type, Some(DataType::TimestampLTz(_)));
            if is_ltz {
                if nanos == 0 {
                    Ok(Datum::TimestampLtz(TimestampLtz::new(millis)))
                } else {
                    Ok(Datum::TimestampLtz(TimestampLtz::from_millis_nanos(
                        millis, nanos,
                    )?))
                }
            } else if nanos == 0 {
                Ok(Datum::TimestampNtz(TimestampNtz::new(millis)))
            } else {
                Ok(Datum::TimestampNtz(TimestampNtz::from_millis_nanos(
                    millis, nanos,
                )?))
            }
        }
        ArrowDataType::Struct(_) => {
            let nested_row_type = fluss_type.and_then(|t| match t {
                DataType::Row(rt) => Some(rt),
                _ => None,
            });
            let nested = extract_struct_from_array(array, row_id, nested_row_type)?;
            Ok(Datum::Row(Box::new(nested)))
        }
        ArrowDataType::List(field) => {
            let list_arr = downcast!(ListArray);
            let values = list_arr.value(row_id);
            // Infer via from_arrow_field so the inferred element type
            // matches what `arrow_map_entry_to_fluss_map` / strict `==`
            // expect when there's no upstream Fluss schema.
            let element_fluss_type = match fluss_type {
                Some(DataType::Array(at)) => at.get_element_type().clone(),
                _ => from_arrow_field(field)?,
            };
            let mut writer = FlussArrayWriter::new(values.len(), &element_fluss_type);
            write_arrow_values_to_fluss_array(&*values, &element_fluss_type, &mut writer)?;
            Ok(Datum::Array(writer.complete()?))
        }
        ArrowDataType::Map(entries_field, _) => {
            let map_arr = downcast!(MapArray);
            let entries = map_arr.value(row_id);
            let (key_type, value_type) = match fluss_type {
                Some(DataType::Map(m)) => (m.key_type().clone(), m.value_type().clone()),
                _ => {
                    let fields = match entries_field.data_type() {
                        ArrowDataType::Struct(f) => f,
                        other => {
                            return Err(IllegalArgument {
                                message: format!("expected Struct for Map entries, got {other:?}"),
                            });
                        }
                    };
                    if fields.len() != 2 {
                        return Err(IllegalArgument {
                            message: format!(
                                "Map entries Struct must have 2 fields, got {}",
                                fields.len()
                            ),
                        });
                    }
                    (from_arrow_field(&fields[0])?, from_arrow_field(&fields[1])?)
                }
            };
            Ok(Datum::Map(arrow_map_entry_to_fluss_map(
                &entries,
                &key_type,
                &value_type,
            )?))
        }
        other => Err(IllegalArgument {
            message: format!("unsupported Arrow data type for nested row extraction: {other:?}"),
        }),
    }
}

impl InternalRow for ColumnarRow {
    fn get_field_count(&self) -> usize {
        self.record_batch.num_columns()
    }

    fn is_null_at(&self, pos: usize) -> Result<bool> {
        Ok(self.column(pos)?.is_null(self.row_id))
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        Ok(self
            .column(pos)?
            .as_boolean_opt()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected boolean array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_byte(&self, pos: usize) -> Result<i8> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Int8Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected byte array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_short(&self, pos: usize) -> Result<i16> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Int16Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected short array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_int(&self, pos: usize) -> Result<i32> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Int32Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected int array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_long(&self, pos: usize) -> Result<i64> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Int64Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected long array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_float(&self, pos: usize) -> Result<f32> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Float32Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected float32 array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_double(&self, pos: usize) -> Result<f64> {
        Ok(self
            .column(pos)?
            .as_primitive_opt::<Float64Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected float64 array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_char(&self, pos: usize, _length: usize) -> Result<&str> {
        Ok(self
            .column(pos)?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected String array for char type at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_string(&self, pos: usize) -> Result<&str> {
        Ok(self
            .column(pos)?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected String array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        let column = self.column(pos)?;
        let array = column
            .as_primitive_opt::<Decimal128Type>()
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "expected Decimal128Array at column {pos}, found: {:?}",
                    column.data_type()
                ),
            })?;

        // Contract: caller must check is_null_at() before calling get_decimal.
        debug_assert!(
            !array.is_null(self.row_id),
            "get_decimal called on null value at pos {} row {}",
            pos,
            self.row_id
        );

        let arrow_scale = match column.data_type() {
            ArrowDataType::Decimal128(_p, s) => *s as i64,
            dt => {
                return Err(IllegalArgument {
                    message: format!(
                        "expected Decimal128 data type at column {pos}, found: {dt:?}"
                    ),
                });
            }
        };

        Decimal::from_arrow_decimal128(
            array.value(self.row_id),
            arrow_scale,
            precision as u32,
            scale as u32,
        )
    }

    fn get_date(&self, pos: usize) -> Result<Date> {
        Ok(Date::new(self.read_date_from_arrow(pos)?))
    }

    fn get_time(&self, pos: usize) -> Result<Time> {
        Ok(Time::new(self.read_time_from_arrow(pos)?))
    }

    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        self.read_timestamp_from_arrow(
            pos,
            precision,
            TimestampNtz::new,
            TimestampNtz::from_millis_nanos,
        )
    }

    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        self.read_timestamp_from_arrow(
            pos,
            precision,
            TimestampLtz::new,
            TimestampLtz::from_millis_nanos,
        )
    }

    fn get_binary(&self, pos: usize, _length: usize) -> Result<&[u8]> {
        Ok(self
            .column(pos)?
            .as_fixed_size_binary_opt()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected binary array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        Ok(self
            .column(pos)?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| IllegalArgument {
                message: format!("expected bytes array at position {pos}"),
            })?
            .value(self.row_id))
    }

    fn get_array(&self, pos: usize) -> Result<FlussArray> {
        let expected_type = self.row_type.fields()[pos].data_type();
        let element_fluss_type = match expected_type {
            DataType::Array(a) => a.get_element_type(),
            _ => {
                return Err(IllegalArgument {
                    message: format!(
                        "expected Array type at position {pos}, got {expected_type:?}"
                    ),
                });
            }
        };

        let column = self.column(pos)?;
        match column.data_type() {
            ArrowDataType::List(_) => {}
            other => {
                return Err(IllegalArgument {
                    message: format!("expected List array at position {pos}, got {other:?}"),
                });
            }
        }

        // `to_arrow_type` is lossy (e.g. TIMESTAMP_LTZ → plain Arrow Timestamp);
        // trust the Fluss schema and let the per-element conversion below catch
        // real shape mismatches.

        let list_arr = column
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("data_type matched List but downcast failed; arrow-rs invariant violated");
        let values = list_arr.value(self.row_id);
        let mut writer = FlussArrayWriter::new(values.len(), element_fluss_type);
        write_arrow_values_to_fluss_array(&*values, element_fluss_type, &mut writer)?;
        writer.complete()
    }

    fn get_map(&self, pos: usize) -> Result<FlussMap> {
        let expected_type = self.row_type.fields()[pos].data_type();
        let map_type = match expected_type {
            DataType::Map(m) => m,
            _ => {
                return Err(IllegalArgument {
                    message: format!("expected Map type at position {pos}, got {expected_type:?}"),
                });
            }
        };

        let column = self.column(pos)?;
        let map_arr =
            column
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| IllegalArgument {
                    message: format!(
                        "expected Map array at position {pos}, got {:?}",
                        column.data_type()
                    ),
                })?;

        arrow_map_entry_to_fluss_map(
            &map_arr.value(self.row_id),
            map_type.key_type(),
            map_type.value_type(),
        )
    }

    fn get_row(&self, pos: usize) -> Result<&GenericRow<'_>> {
        let cache_idx = self
            .row_column_indices
            .iter()
            .position(|&i| i == pos)
            .ok_or_else(|| IllegalArgument {
                message: format!("get_row called on non-ROW column at position {pos}"),
            })?;
        let column = self.record_batch.column(pos);
        // Children of a null parent may carry stale bytes; caller must
        // check is_null_at first rather than rely on what we'd read.
        if column.is_null(self.row_id) {
            return Err(IllegalArgument {
                message: format!(
                    "get_row called on null ROW cell at position {pos}, row {}; \
                     check is_null_at({pos}) first",
                    self.row_id
                ),
            });
        }
        let lock = &self.row_caches[cache_idx];
        if let Some(row) = lock.get() {
            return Ok(row);
        }
        let nested_row_type = self.fluss_row_type.as_ref().and_then(|rt| {
            rt.fields().get(pos).and_then(|f| match &f.data_type {
                DataType::Row(inner) => Some(inner),
                _ => None,
            })
        });
        let extracted = extract_struct_from_array(column.as_ref(), self.row_id, nested_row_type)?;
        Ok(lock.get_or_init(|| extracted))
    }
}

#[inline]
fn arrow_map_entry_to_fluss_map(
    struct_arr: &arrow::array::StructArray,
    key_type: &DataType,
    value_type: &DataType,
) -> Result<FlussMap> {
    let fields = match struct_arr.data_type() {
        ArrowDataType::Struct(f) => f,
        other => {
            return Err(IllegalArgument {
                message: format!("expected Struct for Map entries, got {other:?}"),
            });
        }
    };
    if fields.len() != 2 {
        return Err(IllegalArgument {
            message: format!(
                "Expected 2 columns in Map entries struct, got {}",
                fields.len()
            ),
        });
    }

    // `to_arrow_type` is lossy (e.g. TIMESTAMP_LTZ → plain Arrow Timestamp);
    // trust the Fluss schema and let the per-element conversion below catch
    // real shape mismatches.

    let keys_arrow = struct_arr.column(0);
    let values_arrow = struct_arr.column(1);

    let len = keys_arrow.len();

    // Convert Arrow keys → FlussArray
    let mut key_writer = FlussArrayWriter::new(len, key_type);
    write_arrow_values_to_fluss_array(&**keys_arrow, key_type, &mut key_writer)?;
    let key_array = key_writer.complete()?;

    // Convert Arrow values → FlussArray
    let mut value_writer = FlussArrayWriter::new(len, value_type);
    write_arrow_values_to_fluss_array(&**values_arrow, value_type, &mut value_writer)?;
    let value_array = value_writer.complete()?;

    FlussMap::from_arrays(&key_array, &value_array, key_type, value_type)
}

/// Downcast to a primitive Arrow array type, then loop with null checks calling a writer method.
macro_rules! write_primitive_elements {
    ($values:expr, $arrow_type:ty, $element_type:expr, $writer:expr, $write_method:ident) => {{
        let arr = $values
            .as_primitive_opt::<$arrow_type>()
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "expected {} for {:?} element",
                    stringify!($arrow_type),
                    $element_type
                ),
            })?;
        for i in 0..arr.len() {
            if arr.is_null(i) {
                $writer.set_null_at(i);
            } else {
                $writer.$write_method(i, arr.value(i));
            }
        }
    }};
}

/// Downcast via `downcast_ref`, then loop with null checks calling a writer method.
macro_rules! write_downcast_elements {
    ($values:expr, $array_type:ty, $element_type:expr, $writer:expr, $write_method:ident) => {{
        let arr = $values
            .as_any()
            .downcast_ref::<$array_type>()
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "expected {} for {:?} element",
                    stringify!($array_type),
                    $element_type
                ),
            })?;
        for i in 0..arr.len() {
            if arr.is_null(i) {
                $writer.set_null_at(i);
            } else {
                $writer.$write_method(i, arr.value(i));
            }
        }
    }};
}

/// Downcast via `downcast_ref` to a List array type, then loop with null checks.
macro_rules! write_list_elements {
    ($values:expr, $list_array_type:ty, $len:expr, $element_type:expr, $writer:expr) => {{
        let arr = $values
            .as_any()
            .downcast_ref::<$list_array_type>()
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "expected {} for {:?} element",
                    stringify!($list_array_type),
                    $element_type
                ),
            })?;
        let nested_element_type = match $element_type {
            DataType::Array(a) => a.get_element_type(),
            _ => unreachable!("Expected Array type for write_list_elements"),
        };
        for i in 0..$len {
            if arr.is_null(i) {
                $writer.set_null_at(i);
            } else {
                let nested_values = arr.value(i);
                let mut nested_writer =
                    FlussArrayWriter::new(nested_values.len(), &nested_element_type);
                write_arrow_values_to_fluss_array(
                    &*nested_values,
                    &nested_element_type,
                    &mut nested_writer,
                )?;
                let nested_array = nested_writer.complete()?;
                $writer.write_array(i, &nested_array);
            }
        }
    }};
}

/// Converts all elements of an Arrow array into a `FlussArrayWriter`, downcasting
/// the Arrow array once per call rather than per element.
fn write_arrow_values_to_fluss_array(
    values: &dyn Array,
    element_type: &DataType,
    writer: &mut FlussArrayWriter,
) -> Result<()> {
    let len = values.len();

    match element_type {
        DataType::Boolean(_) => {
            write_downcast_elements!(values, BooleanArray, element_type, writer, write_boolean)
        }
        DataType::TinyInt(_) => {
            write_primitive_elements!(values, Int8Type, element_type, writer, write_byte)
        }
        DataType::SmallInt(_) => {
            write_primitive_elements!(values, Int16Type, element_type, writer, write_short)
        }
        DataType::Int(_) => {
            write_primitive_elements!(values, Int32Type, element_type, writer, write_int)
        }
        DataType::BigInt(_) => {
            write_primitive_elements!(values, Int64Type, element_type, writer, write_long)
        }
        DataType::Float(_) => {
            write_primitive_elements!(values, Float32Type, element_type, writer, write_float)
        }
        DataType::Double(_) => {
            write_primitive_elements!(values, Float64Type, element_type, writer, write_double)
        }
        DataType::Char(_) | DataType::String(_) => {
            write_downcast_elements!(values, StringArray, element_type, writer, write_string)
        }
        DataType::Binary(_) => {
            write_downcast_elements!(
                values,
                FixedSizeBinaryArray,
                element_type,
                writer,
                write_binary_bytes
            )
        }
        DataType::Bytes(_) => {
            write_downcast_elements!(
                values,
                BinaryArray,
                element_type,
                writer,
                write_binary_bytes
            )
        }
        DataType::Decimal(dt) => {
            let arr =
                values
                    .as_primitive_opt::<Decimal128Type>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!("expected Decimal128Array for {element_type:?} element"),
                    })?;
            let arrow_scale = match values.data_type() {
                ArrowDataType::Decimal128(_p, s) => *s as i64,
                other => {
                    return Err(IllegalArgument {
                        message: format!(
                            "expected Decimal128 data type for {element_type:?} element, got {other:?}"
                        ),
                    });
                }
            };
            let precision = dt.precision();
            let scale = dt.scale();
            for i in 0..len {
                if arr.is_null(i) {
                    writer.set_null_at(i);
                } else {
                    let d = Decimal::from_arrow_decimal128(
                        arr.value(i),
                        arrow_scale,
                        precision,
                        scale,
                    )?;
                    writer.write_decimal(i, &d, precision);
                }
            }
        }
        DataType::Date(_) => {
            let arr = values
                .as_primitive_opt::<Date32Type>()
                .ok_or_else(|| IllegalArgument {
                    message: format!("expected Date32Array for {element_type:?} element"),
                })?;
            for i in 0..len {
                if arr.is_null(i) {
                    writer.set_null_at(i);
                } else {
                    writer.write_date(i, Date::new(arr.value(i)));
                }
            }
        }
        DataType::Time(_) => {
            write_time_elements(values, element_type, writer)?;
        }
        DataType::Timestamp(ts_type) => {
            write_timestamp_elements(
                values,
                element_type,
                writer,
                ts_type.precision(),
                TimestampNtz::new,
                TimestampNtz::from_millis_nanos,
                |w, i, ts, p| w.write_timestamp_ntz(i, &ts, p),
            )?;
        }
        DataType::TimestampLTz(ts_type) => {
            write_timestamp_elements(
                values,
                element_type,
                writer,
                ts_type.precision(),
                TimestampLtz::new,
                TimestampLtz::from_millis_nanos,
                |w, i, ts, p| w.write_timestamp_ltz(i, &ts, p),
            )?;
        }
        DataType::Array(_) => {
            if values.as_any().is::<ListArray>() {
                write_list_elements!(values, ListArray, len, element_type, writer);
            } else {
                return Err(IllegalArgument {
                    message: format!(
                        "expected ListArray for {element_type:?} element, got {:?}",
                        values.data_type()
                    ),
                });
            }
        }
        DataType::Map(_) => {
            let map_arr =
                values
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .ok_or_else(|| IllegalArgument {
                        message: format!(
                            "Expected MapArray for {element_type:?} element, got {:?}",
                            values.data_type()
                        ),
                    })?;
            for i in 0..len {
                if map_arr.is_null(i) {
                    writer.set_null_at(i);
                } else {
                    let expected_map_type = match element_type {
                        DataType::Map(m) => m,
                        _ => unreachable!("Expected Map type for Map variant"),
                    };
                    let fluss_map = arrow_map_entry_to_fluss_map(
                        &map_arr.value(i),
                        expected_map_type.key_type(),
                        expected_map_type.value_type(),
                    )?;
                    writer.write_map(i, &fluss_map);
                }
            }
        }
        DataType::Row(row_type) => {
            let struct_arr = values
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| IllegalArgument {
                    message: format!(
                        "expected StructArray for {element_type:?} element, got {:?}",
                        values.data_type()
                    ),
                })?;
            for i in 0..len {
                if struct_arr.is_null(i) {
                    writer.set_null_at(i);
                } else {
                    let nested = extract_struct_from_array(struct_arr, i, Some(row_type))?;
                    writer.write_row(i, &nested)?;
                }
            }
        }
    }
    Ok(())
}

fn write_time_elements(
    values: &dyn Array,
    element_type: &DataType,
    writer: &mut FlussArrayWriter,
) -> Result<()> {
    macro_rules! process_time {
        ($arrow_type:ty, $to_millis:expr) => {{
            let arr = values
                .as_primitive_opt::<$arrow_type>()
                .ok_or_else(|| IllegalArgument {
                    message: format!(
                        "expected {} for {:?} element",
                        stringify!($arrow_type),
                        element_type
                    ),
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    writer.set_null_at(i);
                } else {
                    let to_millis_fn = $to_millis;
                    writer.write_time(i, Time::new(to_millis_fn(arr.value(i))));
                }
            }
        }};
    }

    match values.data_type() {
        ArrowDataType::Time32(TimeUnit::Second) => {
            process_time!(Time32SecondType, |v: i32| v * 1000);
        }
        ArrowDataType::Time32(TimeUnit::Millisecond) => {
            process_time!(Time32MillisecondType, |v: i32| v);
        }
        ArrowDataType::Time64(TimeUnit::Microsecond) => {
            process_time!(Time64MicrosecondType, |v: i64| (v / 1000) as i32);
        }
        ArrowDataType::Time64(TimeUnit::Nanosecond) => {
            process_time!(Time64NanosecondType, |v: i64| (v / 1_000_000) as i32);
        }
        other => {
            return Err(IllegalArgument {
                message: format!(
                    "expected Time column for {element_type:?} element, got {other:?}"
                ),
            });
        }
    }
    Ok(())
}

fn convert_timestamp_raw(raw: i64, unit: &TimeUnit) -> (i64, i32) {
    match unit {
        TimeUnit::Second => (raw * 1000, 0),
        TimeUnit::Millisecond => (raw, 0),
        TimeUnit::Microsecond => {
            let millis = raw.div_euclid(1000);
            let nanos = (raw.rem_euclid(1000) * 1000) as i32;
            (millis, nanos)
        }
        TimeUnit::Nanosecond => {
            let millis = raw.div_euclid(1_000_000);
            let nanos = raw.rem_euclid(1_000_000) as i32;
            (millis, nanos)
        }
    }
}

fn write_timestamp_elements<T>(
    values: &dyn Array,
    element_type: &DataType,
    writer: &mut FlussArrayWriter,
    precision: u32,
    construct_compact: impl Fn(i64) -> T,
    construct_with_nanos: impl Fn(i64, i32) -> Result<T>,
    write_fn: impl Fn(&mut FlussArrayWriter, usize, T, u32),
) -> Result<()> {
    let unit = match values.data_type() {
        ArrowDataType::Timestamp(unit, _) => unit,
        other => {
            return Err(IllegalArgument {
                message: format!(
                    "expected Timestamp column for {element_type:?} element, got {other:?}"
                ),
            });
        }
    };

    macro_rules! process_ts {
        ($arrow_type:ty) => {{
            let arr = values
                .as_primitive_opt::<$arrow_type>()
                .ok_or_else(|| IllegalArgument {
                    message: format!(
                        "expected {} for {:?} element",
                        stringify!($arrow_type),
                        element_type
                    ),
                })?;
            for i in 0..arr.len() {
                if arr.is_null(i) {
                    writer.set_null_at(i);
                    continue;
                }
                let (millis, nanos) = convert_timestamp_raw(arr.value(i), unit);
                let ts = if nanos == 0 {
                    construct_compact(millis)
                } else {
                    construct_with_nanos(millis, nanos)?
                };
                write_fn(writer, i, ts, precision);
            }
        }};
    }

    match unit {
        TimeUnit::Second => process_ts!(TimestampSecondType),
        TimeUnit::Millisecond => process_ts!(TimestampMillisecondType),
        TimeUnit::Microsecond => process_ts!(TimestampMicrosecondType),
        TimeUnit::Nanosecond => process_ts!(TimestampNanosecondType),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, RowType};
    use arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int32Builder, Int64Array, ListBuilder, StringArray,
        StructArray, UInt32Builder,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema};

    fn infer_fluss_type(arrow_dt: &arrow_schema::DataType) -> crate::metadata::DataType {
        match arrow_dt {
            arrow_schema::DataType::Int32 => {
                crate::metadata::DataType::Int(crate::metadata::IntType::new())
            }
            arrow_schema::DataType::List(f) => crate::metadata::DataType::Array(
                crate::metadata::ArrayType::new(infer_fluss_type(f.data_type())),
            ),
            _ => crate::metadata::DataType::Int(crate::metadata::IntType::new()),
        }
    }

    fn single_column_row(array: ArrayRef) -> ColumnarRow {
        let dt = infer_fluss_type(array.data_type());
        let batch =
            RecordBatch::try_from_iter(vec![("arr", array)]).expect("record batch with one column");
        let row_type = Arc::new(RowType::with_data_types(vec![dt]));
        ColumnarRow::new(Arc::new(batch), row_type, 0, None)
    }

    #[test]
    fn columnar_row_reads_values() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", ArrowDataType::Boolean, false),
            Field::new("i8", ArrowDataType::Int8, false),
            Field::new("i16", ArrowDataType::Int16, false),
            Field::new("i32", ArrowDataType::Int32, false),
            Field::new("i64", ArrowDataType::Int64, false),
            Field::new("f32", ArrowDataType::Float32, false),
            Field::new("f64", ArrowDataType::Float64, false),
            Field::new("s", ArrowDataType::Utf8, false),
            Field::new("bin", ArrowDataType::Binary, false),
            Field::new("char", ArrowDataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(BooleanArray::from(vec![true])),
                Arc::new(Int8Array::from(vec![1])),
                Arc::new(Int16Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![3])),
                Arc::new(Int64Array::from(vec![4])),
                Arc::new(Float32Array::from(vec![1.25])),
                Arc::new(Float64Array::from(vec![2.5])),
                Arc::new(StringArray::from(vec!["hello"])),
                Arc::new(BinaryArray::from(vec![b"data".as_slice()])),
                Arc::new(StringArray::from(vec!["ab"])),
            ],
        )
        .expect("record batch");

        let mut row = ColumnarRow::new(Arc::new(batch), Arc::new(RowType::new(vec![])), 0, None);
        assert_eq!(row.get_field_count(), 10);
        assert!(row.get_boolean(0).unwrap());
        assert_eq!(row.get_byte(1).unwrap(), 1);
        assert_eq!(row.get_short(2).unwrap(), 2);
        assert_eq!(row.get_int(3).unwrap(), 3);
        assert_eq!(row.get_long(4).unwrap(), 4);
        assert_eq!(row.get_float(5).unwrap(), 1.25);
        assert_eq!(row.get_double(6).unwrap(), 2.5);
        assert_eq!(row.get_string(7).unwrap(), "hello");
        assert_eq!(row.get_bytes(8).unwrap(), b"data");
        assert_eq!(row.get_char(9, 2).unwrap(), "ab");
        row.set_row_id(0);
        assert_eq!(row.get_row_id(), 0);
    }

    #[test]
    fn columnar_row_reads_decimal() {
        use bigdecimal::{BigDecimal, num_bigint::BigInt};

        // Test with Decimal128
        let schema = Arc::new(Schema::new(vec![
            Field::new("dec1", ArrowDataType::Decimal128(10, 2), false),
            Field::new("dec2", ArrowDataType::Decimal128(20, 5), false),
            Field::new("dec3", ArrowDataType::Decimal128(38, 10), false),
        ]));

        // Create decimal values: 123.45, 12345.67890, large decimal
        let dec1_val = 12345i128; // 123.45 with scale 2
        let dec2_val = 1234567890i128; // 12345.67890 with scale 5
        let dec3_val = 999999999999999999i128; // Large value (18 nines) with scale 10

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(
                    Decimal128Array::from(vec![dec1_val])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![dec2_val])
                        .with_precision_and_scale(20, 5)
                        .unwrap(),
                ),
                Arc::new(
                    Decimal128Array::from(vec![dec3_val])
                        .with_precision_and_scale(38, 10)
                        .unwrap(),
                ),
            ],
        )
        .expect("record batch");

        let row = ColumnarRow::new(Arc::new(batch), Arc::new(RowType::new(vec![])), 0, None);
        assert_eq!(row.get_field_count(), 3);

        // Verify decimal values
        assert_eq!(
            row.get_decimal(0, 10, 2).unwrap(),
            Decimal::from_big_decimal(BigDecimal::new(BigInt::from(12345), 2), 10, 2).unwrap()
        );
        assert_eq!(
            row.get_decimal(1, 20, 5).unwrap(),
            Decimal::from_big_decimal(BigDecimal::new(BigInt::from(1234567890), 5), 20, 5).unwrap()
        );
        assert_eq!(
            row.get_decimal(2, 38, 10).unwrap(),
            Decimal::from_big_decimal(
                BigDecimal::new(BigInt::from(999999999999999999i128), 10),
                38,
                10
            )
            .unwrap()
        );
    }

    #[test]
    fn columnar_row_get_array_int_roundtrip() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_value(2);
        builder.values().append_value(3);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;

        let row = single_column_row(array);
        let arr = row.get_array(0).unwrap();
        assert_eq!(arr.size(), 3);
        assert_eq!(arr.get_int(0).unwrap(), 1);
        assert_eq!(arr.get_int(1).unwrap(), 2);
        assert_eq!(arr.get_int(2).unwrap(), 3);
    }

    #[test]
    fn columnar_row_get_array_with_nulls() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.values().append_null();
        builder.values().append_value(3);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;

        let row = single_column_row(array);
        let arr = row.get_array(0).unwrap();
        assert_eq!(arr.size(), 3);
        assert_eq!(arr.get_int(0).unwrap(), 1);
        assert!(arr.is_null_at(1));
        assert_eq!(arr.get_int(2).unwrap(), 3);
    }

    #[test]
    fn columnar_row_get_array_nested_array() {
        let mut outer = ListBuilder::new(ListBuilder::new(Int32Builder::new()));

        // first nested array: [1, 2]
        outer.values().values().append_value(1);
        outer.values().values().append_value(2);
        outer.values().append(true);

        // second nested array: [99]
        outer.values().values().append_value(99);
        outer.values().append(true);

        // one row containing two nested arrays
        outer.append(true);
        let array = Arc::new(outer.finish()) as ArrayRef;

        let row = single_column_row(array);
        let arr = row.get_array(0).unwrap();
        assert_eq!(arr.size(), 2);

        let nested0 = arr.get_array(0).unwrap();
        assert_eq!(nested0.size(), 2);
        assert_eq!(nested0.get_int(0).unwrap(), 1);
        assert_eq!(nested0.get_int(1).unwrap(), 2);

        let nested1 = arr.get_array(1).unwrap();
        assert_eq!(nested1.size(), 1);
        assert_eq!(nested1.get_int(0).unwrap(), 99);
    }

    #[test]
    fn columnar_row_get_array_non_list_column_returns_error() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let row = single_column_row(array);
        let err = row.get_array(0).unwrap_err();
        assert!(
            err.to_string().contains("expected Array type"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn columnar_row_get_array_unsupported_element_type_returns_error() {
        let mut builder = ListBuilder::new(UInt32Builder::new());
        builder.values().append_value(7);
        builder.append(true);
        let array = Arc::new(builder.finish()) as ArrayRef;

        let batch = RecordBatch::try_from_iter(vec![("arr", array)]).expect("record batch");
        // We manually create a row type that claims to be Array(Int) even though it's List(UInt32)
        // to test the validation in get_array.
        let row_type = Arc::new(RowType::new(vec![DataField::new(
            "arr",
            crate::metadata::DataTypes::array(crate::metadata::DataTypes::int()),
            None,
        )]));
        let row = ColumnarRow::new(Arc::new(batch), row_type, 0, None);

        let err = row.get_array(0).unwrap_err();
        assert!(
            err.to_string().contains("expected Int32Type"),
            "unexpected error: {err}"
        );
    }

    fn make_struct_batch(
        field_name: &str,
        child_fields: Fields,
        child_arrays: Vec<Arc<dyn Array>>,
        _num_rows: usize,
    ) -> Arc<RecordBatch> {
        let struct_array = StructArray::new(child_fields.clone(), child_arrays, None);
        let schema = Arc::new(Schema::new(vec![Field::new(
            field_name,
            DataType::Struct(child_fields),
            false,
        )]));
        Arc::new(RecordBatch::try_new(schema, vec![Arc::new(struct_array)]).expect("record batch"))
    }

    #[test]
    fn columnar_row_reads_nested_row() {
        let child_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
        ]);
        let child_arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int32Array::from(vec![42, 99])),
            Arc::new(StringArray::from(vec!["hello", "world"])),
        ];
        let batch = make_struct_batch("nested", child_fields, child_arrays, 2);

        let mut row = ColumnarRow::new(batch, Arc::new(RowType::new(vec![])), 0, None);

        // row_id = 0
        let nested = row.get_row(0).unwrap();
        assert_eq!(nested.get_field_count(), 2);
        assert_eq!(nested.get_int(0).unwrap(), 42);
        assert_eq!(nested.get_string(1).unwrap(), "hello");

        // row_id = 1
        row.set_row_id(1);
        let nested = row.get_row(0).unwrap();
        assert_eq!(nested.get_int(0).unwrap(), 99);
        assert_eq!(nested.get_string(1).unwrap(), "world");
    }

    #[test]
    fn columnar_row_reads_deeply_nested_row() {
        // Build: outer struct { i32, inner struct { string } }
        let inner_fields = Fields::from(vec![Field::new("s", DataType::Utf8, false)]);
        let inner_array = Arc::new(StructArray::new(
            inner_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["deep", "deeper"])) as Arc<dyn Array>],
            None,
        ));

        let outer_fields = Fields::from(vec![
            Field::new("n", DataType::Int32, false),
            Field::new("inner", DataType::Struct(inner_fields), false),
        ]);
        let outer_array = Arc::new(StructArray::new(
            outer_fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>,
                inner_array as Arc<dyn Array>,
            ],
            None,
        ));

        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Struct(outer_fields),
            false,
        )]));
        let batch =
            Arc::new(RecordBatch::try_new(schema, vec![outer_array]).expect("record batch"));

        let mut row = ColumnarRow::new(batch, Arc::new(RowType::new(vec![])), 0, None);

        // row_id = 0
        let outer = row.get_row(0).unwrap();
        assert_eq!(outer.get_int(0).unwrap(), 1);
        let inner = outer.get_row(1).unwrap();
        assert_eq!(inner.get_string(0).unwrap(), "deep");

        // row_id = 1
        row.set_row_id(1);
        let outer = row.get_row(0).unwrap();
        assert_eq!(outer.get_int(0).unwrap(), 2);
        let inner = outer.get_row(1).unwrap();
        assert_eq!(inner.get_string(0).unwrap(), "deeper");
    }

    #[test]
    fn columnar_row_get_row_cache_invalidated_on_set_row_id() {
        let child_fields = Fields::from(vec![Field::new("x", DataType::Int32, false)]);
        let child_arrays: Vec<Arc<dyn Array>> = vec![Arc::new(Int32Array::from(vec![10, 20]))];
        let batch = make_struct_batch("s", child_fields, child_arrays, 2);

        let mut row = ColumnarRow::new(batch, Arc::new(RowType::new(vec![])), 0, None);

        // row_id = 0: nested x = 10
        let nested_0 = row.get_row(0).unwrap();
        assert_eq!(nested_0.get_int(0).unwrap(), 10);

        // After set_row_id(1), cache is cleared → nested x = 20
        row.set_row_id(1);
        let nested_1 = row.get_row(0).unwrap();
        assert_eq!(nested_1.get_int(0).unwrap(), 20);
    }

    #[test]
    fn columnar_row_get_map_accepts_non_nullable_key_from_map_type() {
        use crate::metadata::DataTypes;
        use arrow::array::{MapBuilder, StringBuilder};

        // Arrow map column with INT keys, STRING values.
        let mut builder = MapBuilder::new(None, Int32Builder::new(), StringBuilder::new());
        builder.keys().append_value(1);
        builder.values().append_value("a");
        builder.append(true).unwrap();
        let map_arr = builder.finish();

        let map_arrow_type = map_arr.data_type().clone();
        let schema = Arc::new(Schema::new(vec![Field::new("m", map_arrow_type, true)]));
        let batch =
            Arc::new(RecordBatch::try_new(schema, vec![Arc::new(map_arr)]).expect("record batch"));

        let map_type = DataTypes::map(DataTypes::int(), DataTypes::string());
        let row_type = Arc::new(RowType::with_data_types(vec![map_type]));
        let row = ColumnarRow::new(batch, row_type, 0, None);

        let fluss_map = row
            .get_map(0)
            .expect("get_map should succeed on ColumnarRow");
        assert_eq!(fluss_map.size(), 1);
        assert_eq!(fluss_map.key_array().get_int(0).unwrap(), 1);
        assert_eq!(fluss_map.value_array().get_string(0).unwrap(), "a");
    }

    #[test]
    fn columnar_row_reads_row_containing_map() {
        use crate::metadata::DataTypes;
        use arrow::array::{MapBuilder, StringBuilder};

        // Inner Map<String, Int> Arrow column with one entry per row, 2 rows.
        let mut mb = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        mb.keys().append_value("k1");
        mb.values().append_value(42);
        mb.append(true).unwrap();
        mb.keys().append_value("k2");
        mb.values().append_value(7);
        mb.append(true).unwrap();
        let map_arr = mb.finish();

        // Struct { id: Int32, m: Map<String, Int> }
        let id_arr = Int32Array::from(vec![10, 20]);
        let struct_fields = Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("m", map_arr.data_type().clone(), false),
        ]);
        let struct_arr = Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(id_arr), Arc::new(map_arr)],
            None,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            DataType::Struct(struct_fields),
            false,
        )]));
        let batch = Arc::new(RecordBatch::try_new(schema, vec![struct_arr]).expect("record batch"));

        // Fluss outer ROW<id INT, m MAP<STRING, INT>>
        let inner_row_type = RowType::with_data_types(vec![
            DataTypes::int(),
            DataTypes::map(DataTypes::string(), DataTypes::int()),
        ]);
        let outer_row_type = Arc::new(RowType::with_data_types(vec![
            crate::metadata::DataType::Row(inner_row_type),
        ]));

        let mut row = ColumnarRow::new(
            batch,
            outer_row_type.clone(),
            0,
            Some(outer_row_type.clone()),
        );

        let nested = row
            .get_row(0)
            .expect("reading row with Map field must succeed");
        assert_eq!(nested.get_int(0).unwrap(), 10);
        let inner_map = nested.get_map(1).expect("nested map should be accessible");
        assert_eq!(inner_map.size(), 1);
        assert_eq!(inner_map.key_array().get_string(0).unwrap(), "k1");
        assert_eq!(inner_map.value_array().get_int(0).unwrap(), 42);

        // Verify cache invalidation across rows works for Row-with-Map too.
        row.set_row_id(1);
        let nested = row.get_row(0).expect("row 1 must read");
        assert_eq!(nested.get_int(0).unwrap(), 20);
        let inner_map = nested.get_map(1).unwrap();
        assert_eq!(inner_map.key_array().get_string(0).unwrap(), "k2");
        assert_eq!(inner_map.value_array().get_int(0).unwrap(), 7);
    }

    #[test]
    fn columnar_row_reads_array_of_maps() {
        use crate::metadata::DataTypes;
        use arrow::array::{ListBuilder, MapBuilder, StringBuilder};

        // One row whose ARRAY<MAP<STRING, INT>> contains two maps:
        // [{"k1" -> 1}, {"k2" -> 2, "k3" -> 3}].
        let mut outer = ListBuilder::new(MapBuilder::new(
            None,
            StringBuilder::new(),
            Int32Builder::new(),
        ));
        {
            let mb = outer.values();
            // Map 0: {"k1" -> 1}
            mb.keys().append_value("k1");
            mb.values().append_value(1);
            mb.append(true).unwrap();
            // Map 1: {"k2" -> 2, "k3" -> 3}
            mb.keys().append_value("k2");
            mb.values().append_value(2);
            mb.keys().append_value("k3");
            mb.values().append_value(3);
            mb.append(true).unwrap();
        }
        outer.append(true);
        let list_arr = outer.finish();
        let arrow_dt = list_arr.data_type().clone();

        let schema = Arc::new(Schema::new(vec![Field::new("a", arrow_dt, false)]));
        let batch =
            Arc::new(RecordBatch::try_new(schema, vec![Arc::new(list_arr)]).expect("record batch"));

        let array_type = DataTypes::array(DataTypes::map(DataTypes::string(), DataTypes::int()));
        let row_type = Arc::new(RowType::with_data_types(vec![array_type]));
        let row = ColumnarRow::new(batch, row_type, 0, None);

        let arr = row.get_array(0).expect("get_array on ARRAY<MAP> must work");
        assert_eq!(arr.size(), 2);

        let m0 = arr
            .get_map(0, &DataTypes::string(), &DataTypes::int())
            .unwrap();
        assert_eq!(m0.size(), 1);
        assert_eq!(m0.key_array().get_string(0).unwrap(), "k1");
        assert_eq!(m0.value_array().get_int(0).unwrap(), 1);

        let m1 = arr
            .get_map(1, &DataTypes::string(), &DataTypes::int())
            .unwrap();
        assert_eq!(m1.size(), 2);
        assert_eq!(m1.key_array().get_string(0).unwrap(), "k2");
        assert_eq!(m1.value_array().get_int(0).unwrap(), 2);
        assert_eq!(m1.key_array().get_string(1).unwrap(), "k3");
        assert_eq!(m1.value_array().get_int(1).unwrap(), 3);
    }

    #[test]
    fn columnar_row_get_map_rejects_real_type_mismatch() {
        use crate::metadata::DataTypes;
        use arrow::array::{MapBuilder, StringBuilder};

        let mut mb = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        mb.keys().append_value("k");
        mb.values().append_value(1);
        mb.append(true).unwrap();
        let map_arr = mb.finish();
        let map_arrow_type = map_arr.data_type().clone();

        let schema = Arc::new(Schema::new(vec![Field::new("m", map_arrow_type, true)]));
        let batch =
            Arc::new(RecordBatch::try_new(schema, vec![Arc::new(map_arr)]).expect("record batch"));

        // Caller mis-declares the value type as STRING.
        let row_type = Arc::new(RowType::with_data_types(vec![DataTypes::map(
            DataTypes::string(),
            DataTypes::string(),
        )]));
        let row = ColumnarRow::new(batch, row_type, 0, None);

        let err = row.get_map(0).expect_err("type mismatch must error");
        let msg = err.to_string();
        assert!(
            msg.contains("expected StringArray"),
            "unexpected error: {msg}"
        );
    }
}
