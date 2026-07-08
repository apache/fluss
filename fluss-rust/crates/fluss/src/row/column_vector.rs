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

//! Typed Arrow column vectors built once per `RecordBatch`. Row accessors
//! match on the variant — no per-call downcasting. Nested `ARRAY<T>` /
//! `MAP<K,V>` / `ROW` columns own their element vectors recursively.

use crate::error::Error::IllegalArgument;
use crate::error::{Error, Result};
use crate::metadata::{DataType, RowType};
use crate::row::Decimal;
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, ListArray, MapArray,
    RecordBatch, StringArray, StructArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use std::sync::Arc;

/// One typed Arrow column.
#[derive(Debug, Clone)]
pub(crate) enum TypedColumn {
    Boolean(BooleanArray),
    TinyInt(Int8Array),
    SmallInt(Int16Array),
    Int(Int32Array),
    BigInt(Int64Array),
    Float(Float32Array),
    Double(Float64Array),
    /// Covers both `STRING` and `CHAR(n)` — both serialize as Arrow `Utf8`.
    String(StringArray),
    Binary(FixedSizeBinaryArray),
    Bytes(BinaryArray),
    /// Companion `i64` is the Arrow scale, extracted once at build.
    Decimal(Decimal128Array, i64),
    Date(Date32Array),
    Time(TimeColumn),
    /// `TIMESTAMP` and `TIMESTAMP_LTZ` share the Arrow representation; the
    /// distinction is purely semantic.
    Timestamp(TimestampColumn),
    TimestampLtz(TimestampColumn),
    Array(ListArray, Box<TypedColumn>),
    Map(MapArray, Box<TypedColumn>, Box<TypedColumn>),
    Row(StructArray, Arc<TypedBatch>),
}

/// Arrow time-of-day in whichever unit the writer chose. Read accessors
/// down-convert to milliseconds since midnight per Fluss's `get_time` contract.
#[derive(Debug, Clone)]
pub(crate) enum TimeColumn {
    Second(Time32SecondArray),
    Millisecond(Time32MillisecondArray),
    Microsecond(Time64MicrosecondArray),
    Nanosecond(Time64NanosecondArray),
}

#[derive(Debug, Clone)]
pub(crate) enum TimestampColumn {
    Second(TimestampSecondArray),
    Millisecond(TimestampMillisecondArray),
    Microsecond(TimestampMicrosecondArray),
    Nanosecond(TimestampNanosecondArray),
}

/// Typed Arrow columns sharing a row count. Built once per `ArrowReader` and
/// shared via `Arc` across rows. `record_batch` is `Some` only for the outer
/// batch (the FFI/pyarrow path hands it back); nested batches built from a
/// `StructArray` carry `None`.
#[derive(Debug)]
pub(crate) struct TypedBatch {
    pub(crate) columns: Vec<TypedColumn>,
    pub(crate) num_rows: usize,
    pub(crate) record_batch: Option<RecordBatch>,
}

impl TypedBatch {
    /// Builds a typed batch from `batch` + `row_type`, recursing into nested
    /// `ARRAY` / `MAP` / `ROW` columns.
    pub(crate) fn build(batch: &RecordBatch, row_type: &RowType) -> Result<Self> {
        let fields = row_type.fields();
        if batch.num_columns() != fields.len() {
            return Err(IllegalArgument {
                message: format!(
                    "RecordBatch has {} columns but RowType has {} fields",
                    batch.num_columns(),
                    fields.len()
                ),
            });
        }
        let mut columns = Vec::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            columns.push(TypedColumn::build(
                batch.column(i).as_ref(),
                &field.data_type,
            )?);
        }
        Ok(TypedBatch {
            columns,
            num_rows: batch.num_rows(),
            record_batch: Some(batch.clone()),
        })
    }

    fn from_struct(struct_arr: &StructArray, row_type: &RowType) -> Result<Self> {
        let fields = row_type.fields();
        if struct_arr.num_columns() != fields.len() {
            return Err(IllegalArgument {
                message: format!(
                    "Arrow StructArray has {} columns but RowType has {} fields",
                    struct_arr.num_columns(),
                    fields.len()
                ),
            });
        }
        let mut columns = Vec::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            columns.push(TypedColumn::build(
                struct_arr.column(i).as_ref(),
                &field.data_type,
            )?);
        }
        Ok(TypedBatch {
            columns,
            num_rows: struct_arr.len(),
            record_batch: None,
        })
    }
}

impl TypedColumn {
    /// Recursively types `array` against the Fluss `fluss_type` it represents.
    pub(crate) fn build(array: &dyn Array, fluss_type: &DataType) -> Result<Self> {
        match fluss_type {
            DataType::Boolean(_) => Ok(Self::Boolean(downcast(array, "BooleanArray")?)),
            DataType::TinyInt(_) => Ok(Self::TinyInt(downcast(array, "Int8Array")?)),
            DataType::SmallInt(_) => Ok(Self::SmallInt(downcast(array, "Int16Array")?)),
            DataType::Int(_) => Ok(Self::Int(downcast(array, "Int32Array")?)),
            DataType::BigInt(_) => Ok(Self::BigInt(downcast(array, "Int64Array")?)),
            DataType::Float(_) => Ok(Self::Float(downcast(array, "Float32Array")?)),
            DataType::Double(_) => Ok(Self::Double(downcast(array, "Float64Array")?)),
            DataType::Char(_) | DataType::String(_) => {
                Ok(Self::String(downcast(array, "StringArray")?))
            }
            DataType::Binary(_) => Ok(Self::Binary(downcast(array, "FixedSizeBinaryArray")?)),
            DataType::Bytes(_) => Ok(Self::Bytes(downcast(array, "BinaryArray")?)),
            DataType::Decimal(_) => {
                let arr: Decimal128Array = downcast(array, "Decimal128Array")?;
                let arrow_scale = match arr.data_type() {
                    ArrowDataType::Decimal128(_, s) => *s as i64,
                    other => {
                        return Err(IllegalArgument {
                            message: format!(
                                "Decimal128Array carries unexpected data type: {other:?}"
                            ),
                        });
                    }
                };
                Ok(Self::Decimal(arr, arrow_scale))
            }
            DataType::Date(_) => Ok(Self::Date(downcast(array, "Date32Array")?)),
            DataType::Time(_) => Ok(Self::Time(TimeColumn::build(array)?)),
            DataType::Timestamp(_) => Ok(Self::Timestamp(TimestampColumn::build(array)?)),
            DataType::TimestampLTz(_) => Ok(Self::TimestampLtz(TimestampColumn::build(array)?)),
            DataType::Array(at) => {
                let list_arr: ListArray = downcast(array, "ListArray")?;
                let element = Self::build(list_arr.values().as_ref(), at.get_element_type())?;
                Ok(Self::Array(list_arr, Box::new(element)))
            }
            DataType::Map(mt) => {
                let map_arr: MapArray = downcast(array, "MapArray")?;
                let key_typed = Self::build(map_arr.keys().as_ref(), mt.key_type())?;
                let value_typed = Self::build(map_arr.values().as_ref(), mt.value_type())?;
                Ok(Self::Map(
                    map_arr,
                    Box::new(key_typed),
                    Box::new(value_typed),
                ))
            }
            DataType::Row(rt) => {
                let struct_arr: StructArray = downcast(array, "StructArray")?;
                let inner = TypedBatch::from_struct(&struct_arr, rt)?;
                Ok(Self::Row(struct_arr, Arc::new(inner)))
            }
        }
    }

    /// Returns the underlying Arrow array for length / nullability access.
    fn outer_array(&self) -> &dyn Array {
        match self {
            Self::Boolean(a) => a,
            Self::TinyInt(a) => a,
            Self::SmallInt(a) => a,
            Self::Int(a) => a,
            Self::BigInt(a) => a,
            Self::Float(a) => a,
            Self::Double(a) => a,
            Self::String(a) => a,
            Self::Binary(a) => a,
            Self::Bytes(a) => a,
            Self::Decimal(a, _) => a,
            Self::Date(a) => a,
            Self::Time(t) => t.as_array(),
            Self::Timestamp(t) | Self::TimestampLtz(t) => t.as_array(),
            Self::Array(a, _) => a,
            Self::Map(a, _, _) => a,
            Self::Row(a, _) => a,
        }
    }

    pub(crate) fn is_null_at(&self, row_id: usize) -> bool {
        self.outer_array().is_null(row_id)
    }

    pub(crate) fn get_boolean(&self, row_id: usize) -> Result<bool> {
        match self {
            Self::Boolean(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("BOOLEAN")),
        }
    }

    pub(crate) fn get_byte(&self, row_id: usize) -> Result<i8> {
        match self {
            Self::TinyInt(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("TINYINT")),
        }
    }

    pub(crate) fn get_short(&self, row_id: usize) -> Result<i16> {
        match self {
            Self::SmallInt(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("SMALLINT")),
        }
    }

    pub(crate) fn get_int(&self, row_id: usize) -> Result<i32> {
        match self {
            Self::Int(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("INT")),
        }
    }

    pub(crate) fn get_long(&self, row_id: usize) -> Result<i64> {
        match self {
            Self::BigInt(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("BIGINT")),
        }
    }

    pub(crate) fn get_float(&self, row_id: usize) -> Result<f32> {
        match self {
            Self::Float(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("FLOAT")),
        }
    }

    pub(crate) fn get_double(&self, row_id: usize) -> Result<f64> {
        match self {
            Self::Double(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("DOUBLE")),
        }
    }

    pub(crate) fn get_string(&self, row_id: usize) -> Result<&str> {
        match self {
            Self::String(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("STRING")),
        }
    }

    pub(crate) fn get_char(&self, row_id: usize) -> Result<&str> {
        self.get_string(row_id)
    }

    pub(crate) fn get_binary(&self, row_id: usize) -> Result<&[u8]> {
        match self {
            Self::Binary(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("BINARY")),
        }
    }

    pub(crate) fn get_bytes(&self, row_id: usize) -> Result<&[u8]> {
        match self {
            Self::Bytes(a) => Ok(a.value(row_id)),
            _ => Err(self.type_mismatch("BYTES")),
        }
    }

    pub(crate) fn get_decimal(&self, row_id: usize, precision: u32, scale: u32) -> Result<Decimal> {
        match self {
            Self::Decimal(a, arrow_scale) => {
                Decimal::from_arrow_decimal128(a.value(row_id), *arrow_scale, precision, scale)
            }
            _ => Err(self.type_mismatch("DECIMAL")),
        }
    }

    pub(crate) fn get_date(&self, row_id: usize) -> Result<Date> {
        match self {
            Self::Date(a) => Ok(Date::new(a.value(row_id))),
            _ => Err(self.type_mismatch("DATE")),
        }
    }

    pub(crate) fn get_time(&self, row_id: usize) -> Result<Time> {
        match self {
            Self::Time(t) => Ok(Time::new(t.get_millis(row_id))),
            _ => Err(self.type_mismatch("TIME")),
        }
    }

    pub(crate) fn get_timestamp_ntz(&self, row_id: usize) -> Result<TimestampNtz> {
        match self {
            Self::Timestamp(t) => {
                let (millis, nanos) = t.get_millis_nanos(row_id);
                if nanos == 0 {
                    Ok(TimestampNtz::new(millis))
                } else {
                    TimestampNtz::from_millis_nanos(millis, nanos)
                }
            }
            _ => Err(self.type_mismatch("TIMESTAMP")),
        }
    }

    pub(crate) fn get_timestamp_ltz(&self, row_id: usize) -> Result<TimestampLtz> {
        match self {
            Self::TimestampLtz(t) => {
                let (millis, nanos) = t.get_millis_nanos(row_id);
                if nanos == 0 {
                    Ok(TimestampLtz::new(millis))
                } else {
                    TimestampLtz::from_millis_nanos(millis, nanos)
                }
            }
            _ => Err(self.type_mismatch("TIMESTAMP_LTZ")),
        }
    }

    fn type_mismatch(&self, expected: &str) -> Error {
        IllegalArgument {
            message: format!(
                "expected {expected} column, got Arrow type {:?}",
                self.outer_array().data_type()
            ),
        }
    }
}

impl TimeColumn {
    fn build(array: &dyn Array) -> Result<Self> {
        match array.data_type() {
            ArrowDataType::Time32(TimeUnit::Second) => {
                Ok(Self::Second(downcast(array, "Time32SecondArray")?))
            }
            ArrowDataType::Time32(TimeUnit::Millisecond) => Ok(Self::Millisecond(downcast(
                array,
                "Time32MillisecondArray",
            )?)),
            ArrowDataType::Time64(TimeUnit::Microsecond) => Ok(Self::Microsecond(downcast(
                array,
                "Time64MicrosecondArray",
            )?)),
            ArrowDataType::Time64(TimeUnit::Nanosecond) => {
                Ok(Self::Nanosecond(downcast(array, "Time64NanosecondArray")?))
            }
            other => Err(IllegalArgument {
                message: format!("expected Time column, got {other:?}"),
            }),
        }
    }

    fn as_array(&self) -> &dyn Array {
        match self {
            Self::Second(a) => a,
            Self::Millisecond(a) => a,
            Self::Microsecond(a) => a,
            Self::Nanosecond(a) => a,
        }
    }

    fn get_millis(&self, row_id: usize) -> i32 {
        match self {
            Self::Second(a) => a.value(row_id) * 1000,
            Self::Millisecond(a) => a.value(row_id),
            Self::Microsecond(a) => (a.value(row_id) / 1_000) as i32,
            Self::Nanosecond(a) => (a.value(row_id) / 1_000_000) as i32,
        }
    }
}

impl TimestampColumn {
    fn build(array: &dyn Array) -> Result<Self> {
        match array.data_type() {
            ArrowDataType::Timestamp(TimeUnit::Second, _) => {
                Ok(Self::Second(downcast(array, "TimestampSecondArray")?))
            }
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => Ok(Self::Millisecond(downcast(
                array,
                "TimestampMillisecondArray",
            )?)),
            ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => Ok(Self::Microsecond(downcast(
                array,
                "TimestampMicrosecondArray",
            )?)),
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => Ok(Self::Nanosecond(downcast(
                array,
                "TimestampNanosecondArray",
            )?)),
            other => Err(IllegalArgument {
                message: format!("expected Timestamp column, got {other:?}"),
            }),
        }
    }

    fn as_array(&self) -> &dyn Array {
        match self {
            Self::Second(a) => a,
            Self::Millisecond(a) => a,
            Self::Microsecond(a) => a,
            Self::Nanosecond(a) => a,
        }
    }

    /// Reads as (millis-since-epoch, nanos-within-millis). Euclidean division
    /// keeps `nanos` in `[0, 999_999]` even for pre-epoch values.
    fn get_millis_nanos(&self, row_id: usize) -> (i64, i32) {
        match self {
            Self::Second(a) => (a.value(row_id) * 1_000, 0),
            Self::Millisecond(a) => (a.value(row_id), 0),
            Self::Microsecond(a) => {
                let v = a.value(row_id);
                (v.div_euclid(1_000), (v.rem_euclid(1_000) * 1_000) as i32)
            }
            Self::Nanosecond(a) => {
                let v = a.value(row_id);
                (v.div_euclid(1_000_000), v.rem_euclid(1_000_000) as i32)
            }
        }
    }
}

fn downcast<T: Array + Clone + 'static>(array: &dyn Array, expected: &str) -> Result<T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .cloned()
        .ok_or_else(|| IllegalArgument {
            message: format!(
                "expected {expected}, got Arrow type {:?}",
                array.data_type()
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes};
    use arrow::array::{ArrayRef, Decimal128Array};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{Field, Fields, Schema};

    fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, a)| a).collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("batch")
    }

    #[test]
    fn builds_and_reads_primitives() {
        let b = batch(vec![
            ("i", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "s",
                Arc::new(StringArray::from(vec!["hello", "world", "!"])) as ArrayRef,
            ),
            (
                "b",
                Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef,
            ),
        ]);
        let rt = RowType::new(vec![
            DataField::new("i", DataTypes::int(), None),
            DataField::new("s", DataTypes::string(), None),
            DataField::new("b", DataTypes::boolean(), None),
        ]);

        let tb = TypedBatch::build(&b, &rt).expect("build");

        assert_eq!(tb.num_rows, 3);
        assert_eq!(tb.columns.len(), 3);

        assert_eq!(tb.columns[0].get_int(0).unwrap(), 1);
        assert_eq!(tb.columns[0].get_int(2).unwrap(), 3);
        assert_eq!(tb.columns[1].get_string(0).unwrap(), "hello");
        assert!(tb.columns[2].get_boolean(0).unwrap());
        assert!(!tb.columns[2].get_boolean(1).unwrap());
    }

    #[test]
    fn type_mismatch_is_clear_error() {
        let b = batch(vec![("i", Arc::new(Int32Array::from(vec![1])) as ArrayRef)]);
        let rt = RowType::new(vec![DataField::new("i", DataTypes::int(), None)]);
        let tb = TypedBatch::build(&b, &rt).expect("build");

        let err = tb.columns[0].get_string(0).unwrap_err().to_string();
        assert!(err.contains("expected STRING"), "{err}");
    }

    #[test]
    fn build_fails_on_arity_mismatch() {
        let b = batch(vec![("i", Arc::new(Int32Array::from(vec![1])) as ArrayRef)]);
        let rt = RowType::new(vec![
            DataField::new("i", DataTypes::int(), None),
            DataField::new("j", DataTypes::int(), None),
        ]);

        let err = TypedBatch::build(&b, &rt).unwrap_err().to_string();
        assert!(
            err.contains("1 columns") && err.contains("2 fields"),
            "{err}"
        );
    }

    #[test]
    fn null_check_uses_arrow_validity_buffer() {
        let arr = Int32Array::from(vec![Some(1), None, Some(3)]);
        let b = batch(vec![("i", Arc::new(arr) as ArrayRef)]);
        let rt = RowType::new(vec![DataField::new("i", DataTypes::int(), None)]);
        let tb = TypedBatch::build(&b, &rt).expect("build");

        assert!(!tb.columns[0].is_null_at(0));
        assert!(tb.columns[0].is_null_at(1));
        assert!(!tb.columns[0].is_null_at(2));
    }

    #[test]
    fn recursive_array_builds_element_vector() {
        let inner = Int32Array::from(vec![10, 20, 30, 40]);
        let offsets = OffsetBuffer::new(vec![0_i32, 2, 4].into());
        let list = ListArray::new(
            Arc::new(Field::new("item", ArrowDataType::Int32, true)),
            offsets,
            Arc::new(inner),
            None,
        );
        let b = batch(vec![("arr", Arc::new(list) as ArrayRef)]);
        let rt = RowType::new(vec![DataField::new(
            "arr",
            DataTypes::array(DataTypes::int()),
            None,
        )]);
        let tb = TypedBatch::build(&b, &rt).expect("build");

        match &tb.columns[0] {
            TypedColumn::Array(_, element) => match element.as_ref() {
                TypedColumn::Int(arr) => {
                    assert_eq!(arr.len(), 4);
                    assert_eq!(arr.value(0), 10);
                    assert_eq!(arr.value(3), 40);
                }
                other => panic!(
                    "expected Int element vector, got {:?}",
                    other.outer_array().data_type()
                ),
            },
            other => panic!("expected Array, got {:?}", other.outer_array().data_type()),
        }
    }

    #[test]
    fn recursive_row_builds_inner_batch() {
        let fields = Fields::from(vec![
            Field::new("x", ArrowDataType::Int32, true),
            Field::new("y", ArrowDataType::Utf8, true),
        ]);
        let struct_arr = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ],
            None,
        );
        let b = batch(vec![("nested", Arc::new(struct_arr) as ArrayRef)]);
        let rt = RowType::new(vec![DataField::new(
            "nested",
            DataTypes::row(vec![
                DataField::new("x", DataTypes::int(), None),
                DataField::new("y", DataTypes::string(), None),
            ]),
            None,
        )]);
        let tb = TypedBatch::build(&b, &rt).expect("build");

        match &tb.columns[0] {
            TypedColumn::Row(_, inner) => {
                assert_eq!(inner.num_rows, 2);
                assert_eq!(inner.columns.len(), 2);
                assert!(matches!(inner.columns[0], TypedColumn::Int(_)));
                assert!(matches!(inner.columns[1], TypedColumn::String(_)));
                assert_eq!(inner.columns[0].get_int(0).unwrap(), 1);
                assert_eq!(inner.columns[1].get_string(1).unwrap(), "b");
            }
            other => panic!("expected Row, got {:?}", other.outer_array().data_type()),
        }
    }

    #[test]
    fn decimal_carries_arrow_scale() {
        let arr = Decimal128Array::from(vec![12345_i128])
            .with_precision_and_scale(10, 2)
            .unwrap();
        let b = batch(vec![("d", Arc::new(arr) as ArrayRef)]);
        let rt = RowType::new(vec![DataField::new("d", DataTypes::decimal(10, 2), None)]);
        let tb = TypedBatch::build(&b, &rt).expect("build");

        match &tb.columns[0] {
            TypedColumn::Decimal(_, arrow_scale) => assert_eq!(*arrow_scale, 2),
            other => panic!(
                "expected Decimal, got {:?}",
                other.outer_array().data_type()
            ),
        }
        let d = tb.columns[0].get_decimal(0, 10, 2).unwrap();
        assert_eq!(d.precision(), 10);
        assert_eq!(d.scale(), 2);
        assert_eq!(d.to_unscaled_long().unwrap(), 12345);
    }
}
