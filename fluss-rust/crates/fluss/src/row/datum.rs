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

use crate::error::Error::{IllegalArgument, RowConvertError};
use crate::error::Result;
use crate::metadata::{DataType, RowType};
use crate::row::Decimal;
use crate::row::GenericRow;
use crate::row::InternalRow;
use crate::row::binary_array::FlussArray;
use crate::row::binary_map::FlussMap;
use crate::row::field_getter::FieldGetter;
use arrow::array::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, ListBuilder, MapBuilder, StringBuilder, StructBuilder,
    Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampMillisecondBuilder,
    TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow::datatypes as arrow_schema;
use arrow::error::ArrowError;
use jiff::ToSpan;
use ordered_float::OrderedFloat;
use parse_display::Display;
use serde::Serialize;
use std::borrow::Cow;

#[allow(dead_code)]
const THIRTY_YEARS_MICROSECONDS: i64 = 946_684_800_000_000;

#[derive(Debug, Clone, Display, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub enum Datum<'a> {
    #[display("null")]
    Null,
    #[display("{0}")]
    Bool(bool),
    #[display("{0}")]
    Int8(i8),
    #[display("{0}")]
    Int16(i16),
    #[display("{0}")]
    Int32(i32),
    #[display("{0}")]
    Int64(i64),
    #[display("{0}")]
    Float32(F32),
    #[display("{0}")]
    Float64(F64),
    #[display("'{0}'")]
    String(Str<'a>),
    #[display("{:?}")]
    Blob(Blob<'a>),
    #[display("{0}")]
    Decimal(Decimal),
    #[display("{0}")]
    Date(Date),
    #[display("{0}")]
    Time(Time),
    #[display("{0}")]
    TimestampNtz(TimestampNtz),
    #[display("{0}")]
    TimestampLtz(TimestampLtz),
    #[display("{0}")]
    Array(FlussArray),
    #[display("{0}")]
    Map(FlussMap),
    #[display("{0:?}")]
    Row(Box<GenericRow<'a>>),
}

impl Datum<'_> {
    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::String(s) => s,
            _ => panic!("not a string: {self:?}"),
        }
    }

    pub fn as_blob(&self) -> &[u8] {
        match self {
            Self::Blob(blob) => blob.as_ref(),
            _ => panic!("not a blob: {self:?}"),
        }
    }

    pub fn as_decimal(&self) -> &Decimal {
        match self {
            Self::Decimal(d) => d,
            _ => panic!("not a decimal: {self:?}"),
        }
    }

    pub fn as_date(&self) -> Date {
        match self {
            Self::Date(d) => *d,
            _ => panic!("not a date: {self:?}"),
        }
    }

    pub fn as_time(&self) -> Time {
        match self {
            Self::Time(t) => *t,
            _ => panic!("not a time: {self:?}"),
        }
    }

    pub fn as_timestamp_ntz(&self) -> TimestampNtz {
        match self {
            Self::TimestampNtz(ts) => *ts,
            _ => panic!("not a timestamp ntz: {self:?}"),
        }
    }

    pub fn as_timestamp_ltz(&self) -> TimestampLtz {
        match self {
            Self::TimestampLtz(ts) => *ts,
            _ => panic!("not a timestamp ltz: {self:?}"),
        }
    }

    pub fn as_array(&self) -> &FlussArray {
        match self {
            Self::Array(a) => a,
            _ => panic!("not an array: {self:?}"),
        }
    }

    pub fn is_map(&self) -> bool {
        matches!(self, Datum::Map(_))
    }

    pub fn as_map(&self) -> &FlussMap {
        match self {
            Self::Map(m) => m,
            _ => panic!("not a map: {self:?}"),
        }
    }

    pub fn as_row(&self) -> &GenericRow<'_> {
        match self {
            Self::Row(r) => r.as_ref(),
            _ => panic!("not a row: {self:?}"),
        }
    }
}

impl<'a> Datum<'a> {
    pub fn into_owned(self) -> Datum<'static> {
        match self {
            Datum::Null => Datum::Null,
            Datum::Bool(v) => Datum::Bool(v),
            Datum::Int8(v) => Datum::Int8(v),
            Datum::Int16(v) => Datum::Int16(v),
            Datum::Int32(v) => Datum::Int32(v),
            Datum::Int64(v) => Datum::Int64(v),
            Datum::Float32(v) => Datum::Float32(v),
            Datum::Float64(v) => Datum::Float64(v),
            Datum::String(s) => Datum::String(Cow::Owned(s.into_owned())),
            Datum::Blob(b) => Datum::Blob(Cow::Owned(b.into_owned())),
            Datum::Decimal(d) => Datum::Decimal(d),
            Datum::Date(d) => Datum::Date(d),
            Datum::Time(t) => Datum::Time(t),
            Datum::TimestampNtz(t) => Datum::TimestampNtz(t),
            Datum::TimestampLtz(t) => Datum::TimestampLtz(t),
            Datum::Array(a) => Datum::Array(a),
            Datum::Map(m) => Datum::Map(m),
            Datum::Row(boxed) => Datum::Row(Box::new(boxed.into_owned())),
        }
    }
}

// ----------- implement from
impl<'a> From<i32> for Datum<'a> {
    #[inline]
    fn from(i: i32) -> Datum<'a> {
        Datum::Int32(i)
    }
}

impl<'a> From<i64> for Datum<'a> {
    #[inline]
    fn from(i: i64) -> Datum<'a> {
        Datum::Int64(i)
    }
}

impl<'a> From<i8> for Datum<'a> {
    #[inline]
    fn from(i: i8) -> Datum<'a> {
        Datum::Int8(i)
    }
}

impl<'a> From<i16> for Datum<'a> {
    #[inline]
    fn from(i: i16) -> Datum<'a> {
        Datum::Int16(i)
    }
}

pub type Str<'a> = Cow<'a, str>;

impl<'a> From<String> for Datum<'a> {
    #[inline]
    fn from(s: String) -> Self {
        Datum::String(Cow::Owned(s))
    }
}

impl<'a> From<&'a str> for Datum<'a> {
    #[inline]
    fn from(s: &'a str) -> Datum<'a> {
        Datum::String(Cow::Borrowed(s))
    }
}

impl From<Option<&()>> for Datum<'_> {
    fn from(_: Option<&()>) -> Self {
        Self::Null
    }
}

impl<'a> From<f32> for Datum<'a> {
    #[inline]
    fn from(f: f32) -> Datum<'a> {
        Datum::Float32(F32::from(f))
    }
}

impl<'a> From<f64> for Datum<'a> {
    #[inline]
    fn from(f: f64) -> Datum<'a> {
        Datum::Float64(F64::from(f))
    }
}

impl TryFrom<&Datum<'_>> for i32 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Int32(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for i16 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Int16(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for i64 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Int64(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for f32 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Float32(f) => Ok(f.into_inner()),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for f64 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Float64(f) => Ok(f.into_inner()),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for bool {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Bool(b) => Ok(*b),
            _ => Err(()),
        }
    }
}

impl<'b, 'a: 'b> TryFrom<&'b Datum<'a>> for &'b str {
    type Error = ();

    #[inline]
    fn try_from(from: &'b Datum<'a>) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::String(s) => Ok(s.as_ref()),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for i8 {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Int8(i) => Ok(*i),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for Decimal {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Decimal(d) => Ok(d.clone()),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for Date {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Date(d) => Ok(*d),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for Time {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::Time(t) => Ok(*t),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for TimestampNtz {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::TimestampNtz(ts) => Ok(*ts),
            _ => Err(()),
        }
    }
}

impl TryFrom<&Datum<'_>> for TimestampLtz {
    type Error = ();

    #[inline]
    fn try_from(from: &Datum) -> std::result::Result<Self, Self::Error> {
        match from {
            Datum::TimestampLtz(ts) => Ok(*ts),
            _ => Err(()),
        }
    }
}

impl<'a> From<bool> for Datum<'a> {
    #[inline]
    fn from(b: bool) -> Datum<'a> {
        Datum::Bool(b)
    }
}

impl<'a> From<Decimal> for Datum<'a> {
    #[inline]
    fn from(d: Decimal) -> Datum<'a> {
        Datum::Decimal(d)
    }
}

impl<'a> From<Date> for Datum<'a> {
    #[inline]
    fn from(d: Date) -> Datum<'a> {
        Datum::Date(d)
    }
}

impl<'a> From<Time> for Datum<'a> {
    #[inline]
    fn from(t: Time) -> Datum<'a> {
        Datum::Time(t)
    }
}

impl<'a> From<TimestampNtz> for Datum<'a> {
    #[inline]
    fn from(ts: TimestampNtz) -> Datum<'a> {
        Datum::TimestampNtz(ts)
    }
}

impl<'a> From<TimestampLtz> for Datum<'a> {
    #[inline]
    fn from(ts: TimestampLtz) -> Datum<'a> {
        Datum::TimestampLtz(ts)
    }
}

impl<'a> From<FlussArray> for Datum<'a> {
    #[inline]
    fn from(arr: FlussArray) -> Datum<'a> {
        Datum::Array(arr)
    }
}

impl<'a> From<FlussMap> for Datum<'a> {
    #[inline]
    fn from(map: FlussMap) -> Datum<'a> {
        Datum::Map(map)
    }
}

pub trait ToArrow {
    fn append_to(
        &self,
        builder: &mut dyn ArrayBuilder,
        fluss_type: &crate::metadata::DataType,
        arrow_type: &arrow_schema::DataType,
    ) -> Result<()>;
}

// Time unit conversion constants
pub(crate) const MILLIS_PER_SECOND: i64 = 1_000;
pub(crate) const MICROS_PER_MILLI: i64 = 1_000;
pub(crate) const NANOS_PER_MILLI: i64 = 1_000_000;

/// Converts milliseconds and nanoseconds-within-millisecond to total microseconds.
/// Returns an error if the conversion would overflow.
pub(crate) fn millis_nanos_to_micros(millis: i64, nanos: i32) -> Result<i64> {
    let millis_micros = millis
        .checked_mul(MICROS_PER_MILLI)
        .ok_or_else(|| RowConvertError {
            message: format!(
                "Timestamp milliseconds {millis} overflows when converting to microseconds"
            ),
        })?;
    let nanos_micros = (nanos as i64) / MICROS_PER_MILLI;
    millis_micros
        .checked_add(nanos_micros)
        .ok_or_else(|| RowConvertError {
            message: format!(
                "Timestamp overflow when adding microseconds: {millis_micros} + {nanos_micros}"
            ),
        })
}

/// Converts milliseconds and nanoseconds-within-millisecond to total nanoseconds.
/// Returns an error if the conversion would overflow.
pub(crate) fn millis_nanos_to_nanos(millis: i64, nanos: i32) -> Result<i64> {
    let millis_nanos = millis
        .checked_mul(NANOS_PER_MILLI)
        .ok_or_else(|| RowConvertError {
            message: format!(
                "Timestamp milliseconds {millis} overflows when converting to nanoseconds"
            ),
        })?;
    millis_nanos
        .checked_add(nanos as i64)
        .ok_or_else(|| RowConvertError {
            message: format!(
                "Timestamp overflow when adding nanoseconds: {millis_nanos} + {nanos}"
            ),
        })
}

/// Rescales a [`Decimal`] to the given Arrow target precision/scale and appends
/// the resulting i128 to the builder.
pub(crate) fn append_decimal_to_builder(
    decimal: &Decimal,
    target_precision: u32,
    target_scale: i64,
    builder: &mut Decimal128Builder,
) -> Result<()> {
    use bigdecimal::RoundingMode;

    let bd = decimal.to_big_decimal();
    let rescaled = bd.with_scale_round(target_scale, RoundingMode::HalfUp);
    let (unscaled, _) = rescaled.as_bigint_and_exponent();

    let actual_precision = Decimal::compute_precision(&unscaled);
    if actual_precision > target_precision as usize {
        return Err(RowConvertError {
            message: format!(
                "Decimal precision overflow: value has {actual_precision} digits but Arrow expects {target_precision} (value: {rescaled})"
            ),
        });
    }

    let i128_val: i128 = match unscaled.try_into() {
        Ok(v) => v,
        Err(_) => {
            return Err(RowConvertError {
                message: format!("Decimal value exceeds i128 range: {rescaled}"),
            });
        }
    };

    builder.append_value(i128_val);
    Ok(())
}

trait AppendResult {
    fn into_append_result(self) -> Result<()>;
}

impl AppendResult for () {
    fn into_append_result(self) -> Result<()> {
        Ok(())
    }
}

impl AppendResult for std::result::Result<(), ArrowError> {
    fn into_append_result(self) -> Result<()> {
        self.map_err(|e| RowConvertError {
            message: format!("Failed to append value: {e}"),
        })
    }
}

fn append_fluss_array_to_list_builder(
    arr: &FlussArray,
    builder: &mut dyn ArrayBuilder,
    fluss_type: &crate::metadata::DataType,
    arrow_type: &arrow_schema::DataType,
) -> Result<()> {
    let list_builder = builder
        .as_any_mut()
        .downcast_mut::<ListBuilder<Box<dyn ArrayBuilder>>>()
        .ok_or_else(|| RowConvertError {
            message: "Builder type mismatch for Array: expected ListBuilder".to_string(),
        })?;

    let element_fluss_type = match fluss_type {
        crate::metadata::DataType::Array(a) => a.get_element_type(),
        _ => {
            return Err(RowConvertError {
                message: format!("Expected Array Fluss type for Array datum, got: {fluss_type:?}"),
            });
        }
    };

    let element_arrow_type = match arrow_type {
        arrow_schema::DataType::List(field) => field.data_type().clone(),
        _ => {
            return Err(RowConvertError {
                message: format!("Expected List Arrow type for Array datum, got: {arrow_type:?}"),
            });
        }
    };

    let values_builder = list_builder.values();

    for i in 0..arr.size() {
        if arr.is_null_at(i) {
            append_null_for_type(values_builder, &element_arrow_type)?;
        } else {
            let datum = read_datum_from_fluss_array(arr, i, element_fluss_type)?;
            datum.append_to(values_builder, element_fluss_type, &element_arrow_type)?;
        }
    }
    list_builder.append(true);
    Ok(())
}

fn append_fluss_map_to_map_builder(
    map: &crate::row::FlussMap,
    builder: &mut dyn ArrayBuilder,
    fluss_type: &crate::metadata::DataType,
    arrow_type: &arrow_schema::DataType,
) -> Result<()> {
    let map_builder = builder
        .as_any_mut()
        .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
        .ok_or_else(|| RowConvertError {
            message: "Builder type mismatch for Map: expected MapBuilder".to_string(),
        })?;

    let expected_map_type = match fluss_type {
        crate::metadata::DataType::Map(m) => m,
        _ => {
            return Err(RowConvertError {
                message: format!("Expected Map Fluss type for Map datum, got: {fluss_type:?}"),
            });
        }
    };

    let (key_arrow_type, value_arrow_type) = match arrow_type {
        arrow_schema::DataType::Map(entries_field, _) => match entries_field.data_type() {
            arrow_schema::DataType::Struct(fields) if fields.len() == 2 => {
                (fields[0].data_type().clone(), fields[1].data_type().clone())
            }
            other => {
                return Err(RowConvertError {
                    message: format!(
                        "Expected Struct with 2 fields for Map entries, got: {other:?}"
                    ),
                });
            }
        },
        _ => {
            return Err(RowConvertError {
                message: format!("Expected Map Arrow type for Map datum, got: {arrow_type:?}"),
            });
        }
    };

    let key_fluss_type = expected_map_type.key_type();
    let value_fluss_type = expected_map_type.value_type();
    let key_array = map.key_array();
    let value_array = map.value_array();

    for i in 0..map.size() {
        let key_datum = read_datum_from_fluss_array(key_array, i, key_fluss_type)?;
        key_datum.append_to(map_builder.keys(), key_fluss_type, &key_arrow_type)?;

        if value_array.is_null_at(i) {
            append_null_for_type(map_builder.values(), &value_arrow_type)?;
        } else {
            let val_datum = read_datum_from_fluss_array(value_array, i, value_fluss_type)?;
            val_datum.append_to(map_builder.values(), value_fluss_type, &value_arrow_type)?;
        }
    }
    map_builder.append(true).map_err(|e| RowConvertError {
        message: format!("Failed to append Map entries: {e}"),
    })?;
    Ok(())
}

pub(crate) fn read_datum_from_fluss_array<'a>(
    arr: &FlussArray,
    pos: usize,
    element_type: &crate::metadata::DataType,
) -> Result<Datum<'a>> {
    if let DataType::Row(row_type) = element_type {
        let compacted = arr.get_row(pos, row_type)?;
        return Ok(Datum::Row(Box::new(internal_row_to_owned_generic(
            &compacted, row_type,
        )?)));
    }

    // FlussArray has no attached schema; use the typed inherent accessor.
    if let DataType::Map(map_type) = element_type {
        return Ok(Datum::Map(arr.get_map(
            pos,
            map_type.key_type(),
            map_type.value_type(),
        )?));
    }

    let getter = FieldGetter::create(element_type, pos);
    Ok(getter.get_field(arr)?.into_owned())
}

pub(crate) fn internal_row_to_owned_generic(
    row: &dyn InternalRow,
    row_type: &RowType,
) -> Result<GenericRow<'static>> {
    let mut owned = GenericRow::new(row_type.fields().len());
    for (i, field) in row_type.fields().iter().enumerate() {
        let getter = FieldGetter::create(field.data_type(), i);
        owned.set_field(i, getter.get_field(row)?.into_owned());
    }
    Ok(owned)
}

fn append_null_for_type(
    builder: &mut dyn ArrayBuilder,
    data_type: &arrow_schema::DataType,
) -> Result<()> {
    macro_rules! downcast_null {
        ($builder_type:ty) => {{
            let b = builder
                .as_any_mut()
                .downcast_mut::<$builder_type>()
                .ok_or_else(|| RowConvertError {
                    message: format!(
                        "Builder type mismatch: expected {} for {data_type:?}",
                        stringify!($builder_type),
                    ),
                })?;
            b.append_null();
            Ok(())
        }};
    }

    match data_type {
        arrow_schema::DataType::Boolean => downcast_null!(BooleanBuilder),
        arrow_schema::DataType::Int8 => downcast_null!(Int8Builder),
        arrow_schema::DataType::Int16 => downcast_null!(Int16Builder),
        arrow_schema::DataType::Int32 => downcast_null!(Int32Builder),
        arrow_schema::DataType::Int64 => downcast_null!(Int64Builder),
        arrow_schema::DataType::Float32 => downcast_null!(Float32Builder),
        arrow_schema::DataType::Float64 => downcast_null!(Float64Builder),
        arrow_schema::DataType::Utf8 => downcast_null!(StringBuilder),
        arrow_schema::DataType::Binary => downcast_null!(BinaryBuilder),
        arrow_schema::DataType::FixedSizeBinary(_) => downcast_null!(FixedSizeBinaryBuilder),
        arrow_schema::DataType::Decimal128(_, _) => downcast_null!(Decimal128Builder),
        arrow_schema::DataType::Date32 => downcast_null!(Date32Builder),
        arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Second) => {
            downcast_null!(Time32SecondBuilder)
        }
        arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
            downcast_null!(Time32MillisecondBuilder)
        }
        arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
            downcast_null!(Time64MicrosecondBuilder)
        }
        arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
            downcast_null!(Time64NanosecondBuilder)
        }
        arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
            downcast_null!(TimestampSecondBuilder)
        }
        arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            downcast_null!(TimestampMillisecondBuilder)
        }
        arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            downcast_null!(TimestampMicrosecondBuilder)
        }
        arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            downcast_null!(TimestampNanosecondBuilder)
        }
        arrow_schema::DataType::List(_) => {
            downcast_null!(ListBuilder<Box<dyn ArrayBuilder>>)
        }
        arrow_schema::DataType::Map(_, _) => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<MapBuilder<Box<dyn ArrayBuilder>, Box<dyn ArrayBuilder>>>()
                .ok_or_else(|| RowConvertError {
                    message: format!(
                        "Builder type mismatch: expected MapBuilder for {data_type:?}",
                    ),
                })?;
            b.append(false).map_err(|e| RowConvertError {
                message: format!("Failed to append null Map entries: {e}"),
            })?;
            Ok(())
        }
        arrow_schema::DataType::Struct(fields) => {
            // StructBuilder::append_null only flips parent validity; children must each get a null too.
            let struct_builder = builder
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .ok_or_else(|| RowConvertError {
                    message: format!(
                        "Builder type mismatch: expected StructBuilder for {data_type:?}",
                    ),
                })?;
            let cloned_fields = fields.clone();
            {
                let field_builders = struct_builder.field_builders_mut();
                for (i, field) in cloned_fields.iter().enumerate() {
                    append_null_for_type(field_builders[i].as_mut(), field.data_type())?;
                }
            }
            struct_builder.append(false);
            Ok(())
        }
        _ => Err(RowConvertError {
            message: format!("Unsupported Arrow data type for null append: {data_type:?}"),
        }),
    }
}

fn append_generic_row_to_struct_builder(
    row: &GenericRow<'_>,
    builder: &mut dyn ArrayBuilder,
    fluss_type: &crate::metadata::DataType,
    arrow_type: &arrow_schema::DataType,
) -> Result<()> {
    let struct_builder = builder
        .as_any_mut()
        .downcast_mut::<StructBuilder>()
        .ok_or_else(|| RowConvertError {
            message: "Builder type mismatch for Row: expected StructBuilder".to_string(),
        })?;

    let row_type = match fluss_type {
        DataType::Row(rt) => rt,
        _ => {
            return Err(RowConvertError {
                message: format!("Expected Row Fluss type for Row datum, got: {fluss_type:?}"),
            });
        }
    };

    let fields = match arrow_type {
        arrow_schema::DataType::Struct(fields) => fields.clone(),
        _ => {
            return Err(RowConvertError {
                message: format!("Expected Struct Arrow type for Row datum, got: {arrow_type:?}"),
            });
        }
    };

    if row.values.len() != fields.len() {
        return Err(RowConvertError {
            message: format!(
                "Row arity mismatch: schema has {} fields, got {}",
                fields.len(),
                row.values.len(),
            ),
        });
    }

    {
        let field_builders = struct_builder.field_builders_mut();
        for (i, datum) in row.values.iter().enumerate() {
            let child = field_builders[i].as_mut();
            let child_fluss_type = row_type.fields()[i].data_type();
            datum.append_to(child, child_fluss_type, fields[i].data_type())?;
        }
    }
    struct_builder.append(true);
    Ok(())
}

impl Datum<'_> {
    pub fn append_to(
        &self,
        builder: &mut dyn ArrayBuilder,
        fluss_type: &crate::metadata::DataType,
        arrow_type: &arrow_schema::DataType,
    ) -> Result<()> {
        macro_rules! append_value_to_arrow {
            ($builder_type:ty, $value:expr) => {
                if let Some(b) = builder.as_any_mut().downcast_mut::<$builder_type>() {
                    b.append_value($value).into_append_result()?;
                    return Ok(());
                }
            };
        }

        match self {
            Datum::Null => return append_null_for_type(builder, arrow_type),
            Datum::Bool(v) => append_value_to_arrow!(BooleanBuilder, *v),
            Datum::Int8(v) => append_value_to_arrow!(Int8Builder, *v),
            Datum::Int16(v) => append_value_to_arrow!(Int16Builder, *v),
            Datum::Int32(v) => append_value_to_arrow!(Int32Builder, *v),
            Datum::Int64(v) => append_value_to_arrow!(Int64Builder, *v),
            Datum::Float32(v) => append_value_to_arrow!(Float32Builder, v.into_inner()),
            Datum::Float64(v) => append_value_to_arrow!(Float64Builder, v.into_inner()),
            Datum::String(v) => append_value_to_arrow!(StringBuilder, v.as_ref()),
            Datum::Blob(v) => match arrow_type {
                arrow_schema::DataType::Binary => {
                    append_value_to_arrow!(BinaryBuilder, v.as_ref());
                }
                arrow_schema::DataType::FixedSizeBinary(_) => {
                    append_value_to_arrow!(FixedSizeBinaryBuilder, v.as_ref());
                }
                _ => {
                    return Err(RowConvertError {
                        message: format!(
                            "Expected Binary or FixedSizeBinary Arrow type, got: {arrow_type:?}"
                        ),
                    });
                }
            },
            Datum::Decimal(decimal) => {
                // Extract target precision and scale from Arrow schema
                let (p, s) = match arrow_type {
                    arrow_schema::DataType::Decimal128(p, s) => (*p, *s),
                    _ => {
                        return Err(RowConvertError {
                            message: format!("Expected Decimal128 Arrow type, got: {arrow_type:?}"),
                        });
                    }
                };

                if s < 0 {
                    return Err(RowConvertError {
                        message: format!("Negative decimal scale {s} is not supported"),
                    });
                }

                if let Some(b) = builder.as_any_mut().downcast_mut::<Decimal128Builder>() {
                    append_decimal_to_builder(decimal, p as u32, s as i64, b)?;
                    return Ok(());
                }

                return Err(RowConvertError {
                    message: "Builder type mismatch for Decimal128".to_string(),
                });
            }
            Datum::Date(date) => {
                append_value_to_arrow!(Date32Builder, date.get_inner());
            }
            Datum::Time(time) => {
                // Time is stored as milliseconds since midnight in Fluss
                // Convert to Arrow's time unit based on schema
                let millis = time.get_inner();

                match arrow_type {
                    arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Second) => {
                        if let Some(b) = builder.as_any_mut().downcast_mut::<Time32SecondBuilder>()
                        {
                            // Validate no sub-second precision is lost
                            if millis % MILLIS_PER_SECOND as i32 != 0 {
                                return Err(RowConvertError {
                                    message: format!(
                                        "Time value {millis} ms has sub-second precision but schema expects seconds only"
                                    ),
                                });
                            }
                            b.append_value(millis / MILLIS_PER_SECOND as i32);
                            return Ok(());
                        }
                    }
                    arrow_schema::DataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
                        if let Some(b) = builder
                            .as_any_mut()
                            .downcast_mut::<Time32MillisecondBuilder>()
                        {
                            b.append_value(millis);
                            return Ok(());
                        }
                    }
                    arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                        if let Some(b) = builder
                            .as_any_mut()
                            .downcast_mut::<Time64MicrosecondBuilder>()
                        {
                            let micros = (millis as i64)
                                .checked_mul(MICROS_PER_MILLI)
                                .ok_or_else(|| RowConvertError {
                                    message: format!(
                                        "Time value {millis} ms overflows when converting to microseconds"
                                    ),
                                })?;
                            b.append_value(micros);
                            return Ok(());
                        }
                    }
                    arrow_schema::DataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
                        if let Some(b) = builder
                            .as_any_mut()
                            .downcast_mut::<Time64NanosecondBuilder>()
                        {
                            let nanos = (millis as i64).checked_mul(NANOS_PER_MILLI).ok_or_else(
                                || RowConvertError {
                                    message: format!(
                                        "Time value {millis} ms overflows when converting to nanoseconds"
                                    ),
                                },
                            )?;
                            b.append_value(nanos);
                            return Ok(());
                        }
                    }
                    _ => {
                        return Err(RowConvertError {
                            message: format!(
                                "Expected Time32/Time64 Arrow type, got: {arrow_type:?}"
                            ),
                        });
                    }
                }

                return Err(RowConvertError {
                    message: "Builder type mismatch for Time".to_string(),
                });
            }
            Datum::TimestampNtz(ts) => {
                let millis = ts.get_millisecond();
                let nanos = ts.get_nano_of_millisecond();

                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampSecondBuilder>()
                {
                    b.append_value(millis / MILLIS_PER_SECOND);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMillisecondBuilder>()
                {
                    b.append_value(millis);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                {
                    b.append_value(millis_nanos_to_micros(millis, nanos)?);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                {
                    b.append_value(millis_nanos_to_nanos(millis, nanos)?);
                    return Ok(());
                }

                return Err(RowConvertError {
                    message: "Builder type mismatch for TimestampNtz".to_string(),
                });
            }
            Datum::TimestampLtz(ts) => {
                let millis = ts.get_epoch_millisecond();
                let nanos = ts.get_nano_of_millisecond();

                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampSecondBuilder>()
                {
                    b.append_value(millis / MILLIS_PER_SECOND);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMillisecondBuilder>()
                {
                    b.append_value(millis);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampMicrosecondBuilder>()
                {
                    b.append_value(millis_nanos_to_micros(millis, nanos)?);
                    return Ok(());
                }
                if let Some(b) = builder
                    .as_any_mut()
                    .downcast_mut::<TimestampNanosecondBuilder>()
                {
                    b.append_value(millis_nanos_to_nanos(millis, nanos)?);
                    return Ok(());
                }

                return Err(RowConvertError {
                    message: "Builder type mismatch for TimestampLtz".to_string(),
                });
            }
            Datum::Array(arr) => {
                return append_fluss_array_to_list_builder(arr, builder, fluss_type, arrow_type);
            }
            Datum::Map(map) => {
                return append_fluss_map_to_map_builder(map, builder, fluss_type, arrow_type);
            }
            Datum::Row(row) => {
                return append_generic_row_to_struct_builder(row, builder, fluss_type, arrow_type);
            }
        }

        Err(RowConvertError {
            message: format!(
                "Cannot append {:?} to builder of type {}",
                self,
                std::any::type_name_of_val(builder)
            ),
        })
    }
}

macro_rules! impl_to_arrow {
    ($ty:ty, $variant:ident) => {
        impl ToArrow for $ty {
            fn append_to(
                &self,
                builder: &mut dyn ArrayBuilder,
                _fluss_type: &crate::metadata::DataType,
                _arrow_type: &arrow_schema::DataType,
            ) -> Result<()> {
                if let Some(b) = builder.as_any_mut().downcast_mut::<$variant>() {
                    b.append_value(*self);
                    Ok(())
                } else {
                    Err(RowConvertError {
                        message: format!(
                            "Cannot cast {} to {} builder",
                            stringify!($ty),
                            stringify!($variant)
                        ),
                    })
                }
            }
        }
    };
}

impl_to_arrow!(i8, Int8Builder);
impl_to_arrow!(i16, Int16Builder);
impl_to_arrow!(i32, Int32Builder);
impl_to_arrow!(f32, Float32Builder);
impl_to_arrow!(f64, Float64Builder);
impl_to_arrow!(&str, StringBuilder);

pub type F32 = OrderedFloat<f32>;
pub type F64 = OrderedFloat<f64>;
#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Date(i32);

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
pub struct Time(i32);

impl Time {
    pub const fn new(inner: i32) -> Self {
        Time(inner)
    }

    /// Get the inner value of time type (milliseconds since midnight)
    pub fn get_inner(&self) -> i32 {
        self.0
    }
}

/// Maximum timestamp precision that can be stored compactly (milliseconds only).
/// Values with precision > MAX_COMPACT_TIMESTAMP_PRECISION require additional nanosecond storage.
pub const MAX_COMPACT_TIMESTAMP_PRECISION: u32 = 3;

/// Maximum valid value for nanoseconds within a millisecond (0 to 999,999 inclusive).
/// A millisecond contains 1,000,000 nanoseconds, so the fractional part ranges from 0 to 999,999.
pub const MAX_NANO_OF_MILLISECOND: i32 = 999_999;

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
#[display("{millisecond}")]
pub struct TimestampNtz {
    millisecond: i64,
    nano_of_millisecond: i32,
}

impl TimestampNtz {
    pub const fn new(millisecond: i64) -> Self {
        TimestampNtz {
            millisecond,
            nano_of_millisecond: 0,
        }
    }

    pub fn from_millis_nanos(millisecond: i64, nano_of_millisecond: i32) -> Result<Self> {
        if !(0..=MAX_NANO_OF_MILLISECOND).contains(&nano_of_millisecond) {
            return Err(IllegalArgument {
                message: format!(
                    "nanoOfMillisecond must be in range [0, {MAX_NANO_OF_MILLISECOND}], got: {nano_of_millisecond}"
                ),
            });
        }
        Ok(TimestampNtz {
            millisecond,
            nano_of_millisecond,
        })
    }

    pub fn get_millisecond(&self) -> i64 {
        self.millisecond
    }

    pub fn get_nano_of_millisecond(&self) -> i32 {
        self.nano_of_millisecond
    }

    /// Check if the timestamp is compact based on precision.
    /// Precision <= MAX_COMPACT_TIMESTAMP_PRECISION means millisecond precision, no need for nanos.
    pub fn is_compact(precision: u32) -> bool {
        precision <= MAX_COMPACT_TIMESTAMP_PRECISION
    }
}

#[derive(PartialOrd, Ord, Display, PartialEq, Eq, Debug, Copy, Clone, Default, Hash, Serialize)]
#[display("{epoch_millisecond}")]
pub struct TimestampLtz {
    epoch_millisecond: i64,
    nano_of_millisecond: i32,
}

impl TimestampLtz {
    pub const fn new(epoch_millisecond: i64) -> Self {
        TimestampLtz {
            epoch_millisecond,
            nano_of_millisecond: 0,
        }
    }

    pub fn from_millis_nanos(epoch_millisecond: i64, nano_of_millisecond: i32) -> Result<Self> {
        if !(0..=MAX_NANO_OF_MILLISECOND).contains(&nano_of_millisecond) {
            return Err(IllegalArgument {
                message: format!(
                    "nanoOfMillisecond must be in range [0, {MAX_NANO_OF_MILLISECOND}], got: {nano_of_millisecond}"
                ),
            });
        }
        Ok(TimestampLtz {
            epoch_millisecond,
            nano_of_millisecond,
        })
    }

    pub fn get_epoch_millisecond(&self) -> i64 {
        self.epoch_millisecond
    }

    pub fn get_nano_of_millisecond(&self) -> i32 {
        self.nano_of_millisecond
    }

    /// Check if the timestamp is compact based on precision.
    /// Precision <= MAX_COMPACT_TIMESTAMP_PRECISION means millisecond precision, no need for nanos.
    pub fn is_compact(precision: u32) -> bool {
        precision <= MAX_COMPACT_TIMESTAMP_PRECISION
    }
}

pub type Blob<'a> = Cow<'a, [u8]>;

impl<'a> From<Vec<u8>> for Datum<'a> {
    fn from(vec: Vec<u8>) -> Self {
        Datum::Blob(Blob::from(vec))
    }
}

impl<'a> From<&'a [u8]> for Datum<'a> {
    fn from(bytes: &'a [u8]) -> Datum<'a> {
        Datum::Blob(Blob::from(bytes))
    }
}

const UNIX_EPOCH_DAY: jiff::civil::Date = jiff::civil::date(1970, 1, 1);

impl Date {
    pub const fn new(inner: i32) -> Self {
        Date(inner)
    }

    /// Get the inner value of date type
    pub fn get_inner(&self) -> i32 {
        self.0
    }

    pub fn year(&self) -> i16 {
        let date = UNIX_EPOCH_DAY + self.0.days();
        date.year()
    }
    pub fn month(&self) -> i8 {
        let date = UNIX_EPOCH_DAY + self.0.days();
        date.month()
    }

    pub fn day(&self) -> i8 {
        let date = UNIX_EPOCH_DAY + self.0.days();
        date.day()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Builder, StringBuilder};

    #[test]
    fn datum_accessors_and_conversions() {
        let datum = Datum::String("value".into());
        assert_eq!(datum.as_str(), "value");
        assert!(!datum.is_null());

        let blob = Blob::from(vec![1, 2, 3]);
        let datum = Datum::Blob(blob);
        assert_eq!(datum.as_blob(), &[1, 2, 3]);

        assert!(Datum::Null.is_null());

        let datum = Datum::Int32(42);
        let value: i32 = (&datum).try_into().unwrap();
        assert_eq!(value, 42);
        let value: std::result::Result<i16, _> = (&datum).try_into();
        assert!(value.is_err());

        // Test temporal types
        let decimal = Decimal::from_unscaled_long(12345, 10, 2).unwrap();
        let datum: Datum = decimal.clone().into();
        assert_eq!(datum.as_decimal(), &decimal);
        let extracted: Decimal = (&datum).try_into().unwrap();
        assert_eq!(extracted, decimal);

        let date = Date::new(19000);
        let datum: Datum = date.into();
        assert_eq!(datum.as_date(), date);

        let ts_ltz = TimestampLtz::new(1672531200000);
        let datum: Datum = ts_ltz.into();
        assert_eq!(datum.as_timestamp_ltz(), ts_ltz);
    }

    #[test]
    fn datum_append_to_builder() {
        use crate::metadata::DataTypes;
        let mut builder = Int32Builder::new();
        let int_type = DataTypes::int();
        Datum::Null
            .append_to(&mut builder, &int_type, &arrow_schema::DataType::Int32)
            .unwrap();
        Datum::Int32(5)
            .append_to(&mut builder, &int_type, &arrow_schema::DataType::Int32)
            .unwrap();
        let array = builder.finish();
        assert!(array.is_null(0));
        assert_eq!(array.value(1), 5);

        let mut builder = StringBuilder::new();
        let string_type = DataTypes::string();
        let err = Datum::Int32(1)
            .append_to(&mut builder, &string_type, &arrow_schema::DataType::Utf8)
            .unwrap_err();
        assert!(matches!(err, RowConvertError { .. }));
    }

    #[test]
    #[should_panic]
    fn datum_as_str_panics_on_non_string() {
        let _ = Datum::Int32(1).as_str();
    }

    #[test]
    #[should_panic]
    fn datum_as_blob_panics_on_non_blob() {
        let _ = Datum::Int16(1).as_blob();
    }

    #[test]
    fn date_components() {
        let date = Date::new(0);
        assert_eq!(date.get_inner(), 0);
        assert_eq!(date.year(), 1970);
        assert_eq!(date.month(), 1);
        assert_eq!(date.day(), 1);
    }
    #[test]
    fn test_datum_map_appends_to_arrow() {
        use crate::metadata::DataTypes;
        use crate::row::binary_map::FlussMapWriter;
        use arrow::array::MapBuilder;
        use std::sync::Arc;

        let mut writer = FlussMapWriter::new(1, &DataTypes::int(), &DataTypes::string());
        writer.write_entry(99.into(), "arrow_test".into()).unwrap();
        let map = writer.complete().unwrap();

        let arrow_type = arrow_schema::DataType::Map(
            Arc::new(arrow_schema::Field::new(
                "entries",
                arrow_schema::DataType::Struct(arrow_schema::Fields::from(vec![
                    arrow_schema::Field::new("key", arrow_schema::DataType::Int32, false),
                    arrow_schema::Field::new("value", arrow_schema::DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        );

        let mut map_builder: MapBuilder<
            Box<dyn arrow::array::ArrayBuilder>,
            Box<dyn arrow::array::ArrayBuilder>,
        > = MapBuilder::new(
            None,
            Box::new(Int32Builder::new()),
            Box::new(StringBuilder::new()),
        );

        let map_type = DataTypes::map(DataTypes::int(), DataTypes::string());
        Datum::Map(map)
            .append_to(&mut map_builder, &map_type, &arrow_type)
            .unwrap();

        let array = map_builder.finish();
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
    }

    #[test]
    fn test_datum_map_append_type_mismatch() {
        use crate::metadata::DataTypes;
        use crate::row::binary_map::FlussMapWriter;
        use arrow::array::{Float64Builder, MapBuilder, StringBuilder};
        use std::sync::Arc;

        // 1. Construct a Map with Keys: String, Values: Float64
        let mut writer = FlussMapWriter::new(1, &DataTypes::string(), &DataTypes::double());
        writer.write_entry("key1".into(), 1.23.into()).unwrap();
        let map = writer.complete().unwrap();

        // 2. Define an Arrow Map builder for (String, Float64) using Boxed builders
        let mut map_builder: MapBuilder<
            Box<dyn arrow::array::ArrayBuilder>,
            Box<dyn arrow::array::ArrayBuilder>,
        > = MapBuilder::new(
            None,
            Box::new(StringBuilder::new()),
            Box::new(Float64Builder::new()),
        );

        // 3. Define an INCOMPATIBLE expected Fluss type (Int32 instead of Map)
        let mismatched_type = DataTypes::int();

        // 4. Define the Arrow type (must match the builder structure)
        let arrow_type = arrow_schema::DataType::Map(
            Arc::new(arrow_schema::Field::new(
                "entries",
                arrow_schema::DataType::Struct(arrow_schema::Fields::from(vec![
                    arrow_schema::Field::new("key", arrow_schema::DataType::Utf8, false),
                    arrow_schema::Field::new("value", arrow_schema::DataType::Float64, true),
                ])),
                false,
            )),
            false,
        );

        // 5. Assert that append_to returns an error
        let result = Datum::Map(map).append_to(&mut map_builder, &mismatched_type, &arrow_type);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("row convert error Expected Map Fluss type for Map datum"));
        assert!(err.contains("Int(IntType { nullable: true })"));
    }
}

#[cfg(test)]
mod timestamp_tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes, RowType};
    use crate::record::to_arrow_type;
    use crate::row::DataGetters;
    use crate::row::column::ColumnarRow;
    use arrow::array::{RecordBatch, StructArray, StructBuilder};
    use arrow::datatypes::{Field, Fields, Schema};
    use std::sync::Arc;

    #[test]
    fn test_timestamp_valid_nanos() {
        // Valid range: 0 to MAX_NANO_OF_MILLISECOND for both TimestampNtz and TimestampLtz
        let ntz1 = TimestampNtz::from_millis_nanos(1000, 0).unwrap();
        assert_eq!(ntz1.get_nano_of_millisecond(), 0);

        let ntz2 = TimestampNtz::from_millis_nanos(1000, MAX_NANO_OF_MILLISECOND).unwrap();
        assert_eq!(ntz2.get_nano_of_millisecond(), MAX_NANO_OF_MILLISECOND);

        let ntz3 = TimestampNtz::from_millis_nanos(1000, 500_000).unwrap();
        assert_eq!(ntz3.get_nano_of_millisecond(), 500_000);

        let ltz1 = TimestampLtz::from_millis_nanos(1000, 0).unwrap();
        assert_eq!(ltz1.get_nano_of_millisecond(), 0);

        let ltz2 = TimestampLtz::from_millis_nanos(1000, MAX_NANO_OF_MILLISECOND).unwrap();
        assert_eq!(ltz2.get_nano_of_millisecond(), MAX_NANO_OF_MILLISECOND);
    }

    #[test]
    fn test_timestamp_nanos_out_of_range() {
        // Test that both TimestampNtz and TimestampLtz reject invalid nanos
        let expected_msg =
            format!("nanoOfMillisecond must be in range [0, {MAX_NANO_OF_MILLISECOND}]");

        // Too large (1,000,000 is just beyond the valid range)
        let result_ntz = TimestampNtz::from_millis_nanos(1000, MAX_NANO_OF_MILLISECOND + 1);
        assert!(result_ntz.is_err());
        assert!(result_ntz.unwrap_err().to_string().contains(&expected_msg));

        let result_ltz = TimestampLtz::from_millis_nanos(1000, MAX_NANO_OF_MILLISECOND + 1);
        assert!(result_ltz.is_err());
        assert!(result_ltz.unwrap_err().to_string().contains(&expected_msg));

        // Negative
        let result_ntz = TimestampNtz::from_millis_nanos(1000, -1);
        assert!(result_ntz.is_err());
        assert!(result_ntz.unwrap_err().to_string().contains(&expected_msg));

        let result_ltz = TimestampLtz::from_millis_nanos(1000, -1);
        assert!(result_ltz.is_err());
        assert!(result_ltz.unwrap_err().to_string().contains(&expected_msg));
    }

    #[test]
    fn test_row_arrow_struct_round_trip() {
        let row_type = RowType::new(vec![
            DataField::new("x", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);
        let row_type_owned = DataType::Row(row_type.clone());
        let arrow_struct_dt = to_arrow_type(&row_type_owned).unwrap();
        let struct_fields: Fields = match &arrow_struct_dt {
            arrow_schema::DataType::Struct(f) => f.clone(),
            _ => unreachable!(),
        };

        let mut struct_builder = StructBuilder::from_fields(struct_fields.clone(), 3);

        let mut r0 = GenericRow::new(2);
        r0.set_field(0, 42_i32);
        r0.set_field(1, "hello");
        Datum::Row(Box::new(r0))
            .append_to(&mut struct_builder, &row_type_owned, &arrow_struct_dt)
            .expect("append row 0");

        Datum::Null
            .append_to(&mut struct_builder, &row_type_owned, &arrow_struct_dt)
            .expect("append null row");

        let mut r2 = GenericRow::new(2);
        r2.set_field(0, -7_i32);
        r2.set_field(1, Datum::Null);
        Datum::Row(Box::new(r2))
            .append_to(&mut struct_builder, &row_type_owned, &arrow_struct_dt)
            .expect("append row 2");

        let struct_array: StructArray = struct_builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "nested",
            arrow_struct_dt.clone(),
            true,
        )]));
        let batch = Arc::new(
            RecordBatch::try_new(schema, vec![Arc::new(struct_array)]).expect("record batch"),
        );

        // Outer batch has one column ("nested") whose type is ROW<x INT, label STRING>.
        let outer_row_type = RowType::new(vec![DataField::new(
            "nested",
            DataType::Row(row_type),
            None,
        )]);
        let mut columnar =
            ColumnarRow::new(batch, Arc::new(outer_row_type), 0, None).expect("ColumnarRow");

        let nested = columnar.get_row(0).expect("get_row 0");
        assert_eq!(nested.get_int(0).unwrap(), 42);
        assert_eq!(nested.get_string(1).unwrap(), "hello");

        columnar.set_row_id(1);
        assert!(columnar.is_null_at(0).unwrap(), "row 1 should be null");

        columnar.set_row_id(2);
        let nested = columnar.get_row(0).expect("get_row 2");
        assert_eq!(nested.get_int(0).unwrap(), -7);
        assert!(nested.is_null_at(1).unwrap(), "label should be null");
    }
}
