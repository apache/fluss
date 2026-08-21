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

//! Per-column statistics carried by a V1 log record batch, which the server
//! uses to skip whole batches that a pushed-down filter cannot match.
//!
//! Statistics are derived from the finished Arrow batch rather than row by row
//! as Java does, so both the row-append and batch-append writer paths are
//! covered by one implementation.
//!
//! Collecting two columns, with `mapping = [1, 0]`, lays out as:
//!
//! ```text
//! offset 0     1         3            7             15
//!        |-----|---------|------------|-------------|
//!        | ver | count=2 | indexes    | null counts |
//!        | 0x01| i16     | i16 x 2    | i32 x 2     |
//!        |-----|---------|------------|-------------|
//!                           [1, 0]       [n1, n0]
//!
//!        15            19                    ...
//!        |-------------|---------------------|
//!        | min row len | min row (AlignedRow)|
//!        | i32         | 2 fields            |
//!        |-------------|---------------------|
//!        | max row len | max row (AlignedRow)|
//!        | i32         | 2 fields            |
//!        |-------------|---------------------|
//! ```
//!
//! Everything is little-endian, and the two rows carry one field per collected
//! column in the same order as the index array.

use crate::error::{Error, Result};
use crate::metadata::{DataType, RowType};
use crate::row::aligned::AlignedRowWriter;
use crate::row::binary::BinaryWriter;
use crate::row::{Decimal, TimestampLtz, TimestampNtz};
use arrow::array::{Array, RecordBatch};
use arrow::compute::kernels::aggregate;
use arrow::datatypes::{
    Date32Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type,
    Int64Type, Time32MillisecondType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};

/// Version byte leading the statistics block, matching Java's
/// `LogRecordBatchFormat.STATISTICS_VERSION`.
const STATISTICS_VERSION: u8 = 1;

/// Whether statistics can be collected for `data_type`, mirroring Java's
/// `DataTypeChecks.isSupportedStatisticsType`.
pub(crate) fn is_supported_statistics_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::String(_)
            | DataType::Char(_)
            | DataType::Decimal(_)
            | DataType::Date(_)
            | DataType::Time(_)
            | DataType::Timestamp(_)
            | DataType::TimestampLTz(_)
    )
}

/// The minimum and maximum of one column, or `None` when every value is null.
enum ColumnBounds {
    Bool(bool, bool),
    Int8(i8, i8),
    Int16(i16, i16),
    Int32(i32, i32),
    Int64(i64, i64),
    Float32(f32, f32),
    Float64(f64, f64),
    Str(String, String),
    Decimal(Decimal, Decimal),
    TimestampNtz(TimestampNtz, TimestampNtz),
    TimestampLtz(TimestampLtz, TimestampLtz),
}

/// Serialises the statistics of `batch` for the columns named by `mapping`.
///
/// `mapping[i]` is the table column whose min, max and null count go into the
/// block's `i`th position, so `[1, 0]` collects column 1 before column 0.
///
/// Returns `None` when there is nothing worth sending, which is how an empty
/// batch or an empty mapping is signalled to the caller.
pub(crate) fn serialize_statistics(
    batch: &RecordBatch,
    row_type: &RowType,
    mapping: &[usize],
) -> Result<Option<Vec<u8>>> {
    if mapping.is_empty() || batch.num_rows() == 0 {
        return Ok(None);
    }

    // Indexing the batch and the row type below would panic rather than error,
    // so make both bounds explicit. Column types are checked lazily by
    // `column_bounds` as each one is read.
    let field_count = row_type.fields().len();
    if batch.num_columns() != field_count {
        return Err(Error::IllegalArgument {
            message: format!(
                "Statistics need a batch matching the table schema, got {} columns for {field_count} fields",
                batch.num_columns()
            ),
        });
    }
    if let Some(&index) = mapping.iter().find(|&&index| index >= field_count) {
        return Err(Error::IllegalArgument {
            message: format!(
                "Statistics column index {index} is out of range for {field_count} fields"
            ),
        });
    }

    let mut null_counts = Vec::with_capacity(mapping.len());
    let mut bounds = Vec::with_capacity(mapping.len());
    for &column_index in mapping {
        let column = batch.column(column_index);
        null_counts.push(column.null_count() as i32);
        bounds.push(column_bounds(
            column,
            row_type.fields()[column_index].data_type(),
        )?);
    }

    // Everything below is little-endian, as Java's memory segments are.
    let mut out = Vec::new();
    out.push(STATISTICS_VERSION);
    // Append the column count, which sizes the two arrays that follow.
    out.extend_from_slice(&(mapping.len() as i16).to_le_bytes());
    // Append the table column index each position describes.
    for &column_index in mapping {
        out.extend_from_slice(&(column_index as i16).to_le_bytes());
    }
    // Append the null count of each of those columns.
    for count in &null_counts {
        out.extend_from_slice(&count.to_le_bytes());
    }

    let types: Vec<&DataType> = mapping
        .iter()
        .map(|&i| row_type.fields()[i].data_type())
        .collect();
    append_row(&mut out, &bounds, &types, Bound::Min);
    append_row(&mut out, &bounds, &types, Bound::Max);
    Ok(Some(out))
}

#[derive(Clone, Copy)]
enum Bound {
    Min,
    Max,
}

/// Writes one aligned row of bounds, length-prefixed as Java's
/// `LogRecordBatchStatisticsWriter.writeRowData`.
fn append_row(out: &mut Vec<u8>, bounds: &[Option<ColumnBounds>], types: &[&DataType], b: Bound) {
    let mut writer = AlignedRowWriter::new(bounds.len());
    for (index, bound) in bounds.iter().enumerate() {
        match bound {
            // An all-null column has no bound, so the slot itself is null.
            None => writer.set_null_at(index),
            Some(bound) => write_bound(&mut writer, bound, types[index], b),
        }
    }
    writer.complete();
    let row = writer.to_bytes();
    out.extend_from_slice(&(row.len() as i32).to_le_bytes());
    out.extend_from_slice(&row);
}

/// Picks the requested end of a bound pair.
fn pick<T>(b: Bound, min: T, max: T) -> T {
    match b {
        Bound::Min => min,
        Bound::Max => max,
    }
}

fn write_bound(writer: &mut AlignedRowWriter, bound: &ColumnBounds, ty: &DataType, b: Bound) {
    match bound {
        ColumnBounds::Bool(min, max) => writer.write_boolean(pick(b, *min, *max)),
        ColumnBounds::Int8(min, max) => writer.write_byte(pick(b, *min, *max) as u8),
        ColumnBounds::Int16(min, max) => writer.write_short(pick(b, *min, *max)),
        ColumnBounds::Int32(min, max) => writer.write_int(pick(b, *min, *max)),
        ColumnBounds::Int64(min, max) => writer.write_long(pick(b, *min, *max)),
        ColumnBounds::Float32(min, max) => writer.write_float(pick(b, *min, *max)),
        ColumnBounds::Float64(min, max) => writer.write_double(pick(b, *min, *max)),
        ColumnBounds::Str(min, max) => writer.write_string(pick(b, min, max)),
        ColumnBounds::Decimal(min, max) => {
            let value = pick(b, min, max);
            let precision = match ty {
                DataType::Decimal(decimal_type) => decimal_type.precision(),
                _ => value.precision(),
            };
            writer.write_decimal(value, precision);
        }
        ColumnBounds::TimestampNtz(min, max) => {
            writer.write_timestamp_ntz(pick(b, min, max), precision_of(ty));
        }
        ColumnBounds::TimestampLtz(min, max) => {
            writer.write_timestamp_ltz(pick(b, min, max), precision_of(ty));
        }
    }
}

fn precision_of(ty: &DataType) -> u32 {
    match ty {
        DataType::Timestamp(t) => t.precision(),
        DataType::TimestampLTz(t) => t.precision(),
        _ => 6,
    }
}

/// Reduces one Arrow column to its bounds, returning `None` when it is entirely
/// null and therefore has none.
fn column_bounds(column: &dyn Array, data_type: &DataType) -> Result<Option<ColumnBounds>> {
    use arrow::array::*;
    use arrow::datatypes::DataType as ArrowType;

    macro_rules! primitive {
        ($arrow_ty:ty, $variant:ident) => {{
            let array = column
                .as_any()
                .downcast_ref::<PrimitiveArray<$arrow_ty>>()
                .ok_or_else(|| unexpected_array(column, data_type))?;
            match (aggregate::min(array), aggregate::max(array)) {
                (Some(min), Some(max)) => Ok(Some(ColumnBounds::$variant(min, max))),
                _ => Ok(None),
            }
        }};
    }

    match data_type {
        DataType::Boolean(_) => {
            let array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| unexpected_array(column, data_type))?;
            match (aggregate::min_boolean(array), aggregate::max_boolean(array)) {
                (Some(min), Some(max)) => Ok(Some(ColumnBounds::Bool(min, max))),
                _ => Ok(None),
            }
        }
        DataType::TinyInt(_) => primitive!(Int8Type, Int8),
        DataType::SmallInt(_) => primitive!(Int16Type, Int16),
        DataType::Int(_) => primitive!(Int32Type, Int32),
        DataType::Date(_) => primitive!(Date32Type, Int32),
        DataType::BigInt(_) => primitive!(Int64Type, Int64),
        DataType::Float(_) => primitive!(Float32Type, Float32),
        DataType::Double(_) => primitive!(Float64Type, Float64),
        // Fluss stores TIME as millis of day, so every unit but millisecond
        // has to be converted back from what the Arrow array holds.
        DataType::Time(_) => match column.data_type() {
            ArrowType::Time32(arrow::datatypes::TimeUnit::Second) => {
                let array = column
                    .as_any()
                    .downcast_ref::<Time32SecondArray>()
                    .ok_or_else(|| unexpected_array(column, data_type))?;
                match (aggregate::min(array), aggregate::max(array)) {
                    (Some(min), Some(max)) => {
                        Ok(Some(ColumnBounds::Int32(min * 1_000, max * 1_000)))
                    }
                    _ => Ok(None),
                }
            }
            ArrowType::Time32(_) => primitive!(Time32MillisecondType, Int32),
            ArrowType::Time64(arrow::datatypes::TimeUnit::Microsecond) => time64_as_millis(
                column.as_any().downcast_ref::<Time64MicrosecondArray>(),
                1_000,
            ),
            ArrowType::Time64(_) => time64_as_millis(
                column.as_any().downcast_ref::<Time64NanosecondArray>(),
                1_000_000,
            ),
            _ => Err(unexpected_array(column, data_type)),
        },
        DataType::String(_) | DataType::Char(_) => {
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| unexpected_array(column, data_type))?;
            match (aggregate::min_string(array), aggregate::max_string(array)) {
                (Some(min), Some(max)) => {
                    Ok(Some(ColumnBounds::Str(min.to_string(), max.to_string())))
                }
                _ => Ok(None),
            }
        }
        DataType::Decimal(decimal_type) => {
            let array = column
                .as_any()
                .downcast_ref::<PrimitiveArray<Decimal128Type>>()
                .ok_or_else(|| unexpected_array(column, data_type))?;
            // The array stores raw unscaled integers and keeps its scale in its
            // own type, which an appended batch can declare differently from the
            // column. Rescale from the array's as `column_vector` does.
            let ArrowType::Decimal128(_, arrow_scale) = column.data_type() else {
                return Err(unexpected_array(column, data_type));
            };
            let (precision, scale) = (decimal_type.precision(), decimal_type.scale());
            match (aggregate::min(array), aggregate::max(array)) {
                (Some(min), Some(max)) => Ok(Some(ColumnBounds::Decimal(
                    Decimal::from_arrow_decimal128(min, *arrow_scale as i64, precision, scale)?,
                    Decimal::from_arrow_decimal128(max, *arrow_scale as i64, precision, scale)?,
                ))),
                _ => Ok(None),
            }
        }
        DataType::Timestamp(_) => {
            let (min, max) = match timestamp_bounds(column, data_type)? {
                Some(bounds) => bounds,
                None => return Ok(None),
            };
            Ok(Some(ColumnBounds::TimestampNtz(
                TimestampNtz::from_millis_nanos(min.0, min.1)?,
                TimestampNtz::from_millis_nanos(max.0, max.1)?,
            )))
        }
        DataType::TimestampLTz(_) => {
            let (min, max) = match timestamp_bounds(column, data_type)? {
                Some(bounds) => bounds,
                None => return Ok(None),
            };
            Ok(Some(ColumnBounds::TimestampLtz(
                TimestampLtz::from_millis_nanos(min.0, min.1)?,
                TimestampLtz::from_millis_nanos(max.0, max.1)?,
            )))
        }
        other => Err(Error::IllegalArgument {
            message: format!("Statistics are not supported for column type {other:?}"),
        }),
    }
}

/// Fluss stores TIME as milliseconds of day, so a finer Arrow unit is scaled
/// down by `divisor` before it becomes a bound.
fn time64_as_millis<T>(
    array: Option<&arrow::array::PrimitiveArray<T>>,
    divisor: i64,
) -> Result<Option<ColumnBounds>>
where
    T: arrow::datatypes::ArrowPrimitiveType<Native = i64>,
{
    let array = array.ok_or_else(|| Error::IllegalArgument {
        message: "TIME column is not backed by a Time64 array".to_string(),
    })?;
    match (aggregate::min(array), aggregate::max(array)) {
        (Some(min), Some(max)) => Ok(Some(ColumnBounds::Int32(
            (min / divisor) as i32,
            (max / divisor) as i32,
        ))),
        _ => Ok(None),
    }
}

/// Returns the (millis, nano-of-milli) bounds of a timestamp column.
#[allow(clippy::type_complexity)]
fn timestamp_bounds(
    column: &dyn Array,
    data_type: &DataType,
) -> Result<Option<((i64, i32), (i64, i32))>> {
    use arrow::array::PrimitiveArray;
    use arrow::datatypes::DataType as ArrowType;
    use arrow::datatypes::TimeUnit;

    macro_rules! bounds {
        ($arrow_ty:ty, $to_parts:expr) => {{
            let array = column
                .as_any()
                .downcast_ref::<PrimitiveArray<$arrow_ty>>()
                .ok_or_else(|| unexpected_array(column, data_type))?;
            match (aggregate::min(array), aggregate::max(array)) {
                (Some(min), Some(max)) => Ok(Some(($to_parts(min), $to_parts(max)))),
                _ => Ok(None),
            }
        }};
    }

    match column.data_type() {
        ArrowType::Timestamp(TimeUnit::Second, _) => {
            bounds!(TimestampSecondType, |v: i64| (v * 1_000, 0))
        }
        ArrowType::Timestamp(TimeUnit::Millisecond, _) => {
            bounds!(TimestampMillisecondType, |v: i64| (v, 0))
        }
        ArrowType::Timestamp(TimeUnit::Microsecond, _) => {
            bounds!(TimestampMicrosecondType, |v: i64| (
                v.div_euclid(1_000),
                (v.rem_euclid(1_000) * 1_000) as i32
            ))
        }
        ArrowType::Timestamp(TimeUnit::Nanosecond, _) => {
            bounds!(TimestampNanosecondType, |v: i64| (
                v.div_euclid(1_000_000),
                v.rem_euclid(1_000_000) as i32
            ))
        }
        _ => Err(unexpected_array(column, data_type)),
    }
}

fn unexpected_array(column: &dyn Array, data_type: &DataType) -> Error {
    Error::IllegalArgument {
        message: format!(
            "Column of Fluss type {data_type:?} is backed by unexpected Arrow type {:?}",
            column.data_type()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes};
    use arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int8Array,
        Int16Array, Int32Array, Int64Array, StringArray, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::{DataType as ArrowType, Field, Schema};
    use std::sync::Arc;

    fn row_type() -> RowType {
        RowType::new(vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("name", DataTypes::string(), None),
            DataField::new("score", DataTypes::bigint(), None),
        ])
    }

    fn batch(
        ids: Vec<Option<i32>>,
        names: Vec<Option<&str>>,
        scores: Vec<Option<i64>>,
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("name", ArrowType::Utf8, true),
            Field::new("score", ArrowType::Int64, true),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
                Arc::new(Int64Array::from(scores)),
            ],
        )
        .expect("batch")
    }

    /// Reads back the fixed prefix so a layout change cannot pass silently.
    fn parse_prefix(bytes: &[u8], columns: usize) -> (u8, i16, Vec<i16>, Vec<i32>) {
        let version = bytes[0];
        let count = i16::from_le_bytes(bytes[1..3].try_into().unwrap());
        let mut at = 3;
        let mut indexes = Vec::new();
        for _ in 0..columns {
            indexes.push(i16::from_le_bytes(bytes[at..at + 2].try_into().unwrap()));
            at += 2;
        }
        let mut nulls = Vec::new();
        for _ in 0..columns {
            nulls.push(i32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()));
            at += 4;
        }
        (version, count, indexes, nulls)
    }

    #[test]
    fn serializes_the_documented_prefix() {
        let rt = row_type();
        let batch = batch(
            vec![Some(3), Some(1), Some(2)],
            vec![Some("c"), Some("a"), None],
            vec![Some(30), Some(10), Some(20)],
        );
        let bytes = serialize_statistics(&batch, &rt, &[0, 1, 2])
            .expect("serialize")
            .expect("statistics");

        let (version, count, indexes, nulls) = parse_prefix(&bytes, 3);
        assert_eq!(version, 1);
        assert_eq!(count, 3);
        assert_eq!(indexes, vec![0, 1, 2]);
        assert_eq!(nulls, vec![0, 1, 0]);
    }

    #[test]
    fn collects_bounds_over_the_whole_column() {
        let rt = row_type();
        let batch = batch(
            vec![Some(3), Some(1), Some(2)],
            vec![Some("c"), Some("a"), Some("b")],
            vec![Some(30), Some(10), Some(20)],
        );
        let bytes = serialize_statistics(&batch, &rt, &[0])
            .expect("serialize")
            .expect("statistics");

        // version(1) + count(2) + index(2) + nullCount(4) = 9 bytes of prefix.
        let min_size = i32::from_le_bytes(bytes[9..13].try_into().unwrap()) as usize;
        let min_row = &bytes[13..13 + min_size];
        // One field: 8 null-bit bytes then the value slot.
        assert_eq!(i32::from_le_bytes(min_row[8..12].try_into().unwrap()), 1);

        let max_at = 13 + min_size;
        let max_size = i32::from_le_bytes(bytes[max_at..max_at + 4].try_into().unwrap()) as usize;
        let max_row = &bytes[max_at + 4..max_at + 4 + max_size];
        assert_eq!(i32::from_le_bytes(max_row[8..12].try_into().unwrap()), 3);
    }

    #[test]
    fn marks_an_all_null_column_as_having_no_bounds() {
        let rt = row_type();
        let batch = batch(vec![None, None], vec![None, None], vec![None, None]);
        let bytes = serialize_statistics(&batch, &rt, &[0])
            .expect("serialize")
            .expect("statistics");

        let (_, _, _, nulls) = parse_prefix(&bytes, 1);
        assert_eq!(nulls, vec![2]);
        let min_size = i32::from_le_bytes(bytes[9..13].try_into().unwrap()) as usize;
        let min_row = &bytes[13..13 + min_size];
        // Field 0's null bit is bit 8, the first after the reserved header bits.
        assert_eq!(min_row[1] & 0x01, 0x01);
    }

    #[test]
    fn skips_an_empty_batch_and_an_empty_mapping() {
        let rt = row_type();
        let empty = batch(vec![], vec![], vec![]);
        assert!(
            serialize_statistics(&empty, &rt, &[0])
                .expect("serialize")
                .is_none()
        );

        let populated = batch(vec![Some(1)], vec![Some("a")], vec![Some(1)]);
        assert!(
            serialize_statistics(&populated, &rt, &[])
                .expect("serialize")
                .is_none()
        );
    }

    #[test]
    fn rejects_a_batch_that_does_not_match_the_schema() {
        // Two Arrow columns against a three field row type: indexing the batch
        // by a mapping built from the schema would otherwise panic.
        let schema = Schema::new(vec![
            Field::new("id", ArrowType::Int32, true),
            Field::new("name", ArrowType::Utf8, true),
        ]);
        let narrow = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(1)])) as arrow::array::ArrayRef,
                Arc::new(StringArray::from(vec![Some("a")])),
            ],
        )
        .expect("batch");

        assert!(matches!(
            serialize_statistics(&narrow, &row_type(), &[0, 1, 2]),
            Err(Error::IllegalArgument { .. })
        ));
    }

    #[test]
    fn rejects_a_mapping_beyond_the_row_type() {
        let batch = batch(vec![Some(1)], vec![Some("a")], vec![Some(1)]);
        assert!(matches!(
            serialize_statistics(&batch, &row_type(), &[3]),
            Err(Error::IllegalArgument { .. })
        ));
    }

    #[test]
    fn rejects_a_column_type_without_statistics_support() {
        assert!(!is_supported_statistics_type(&DataTypes::bytes()));
        assert!(is_supported_statistics_type(&DataTypes::string()));
        assert!(is_supported_statistics_type(&DataTypes::timestamp()));
    }

    /// Serialises a one-column batch and returns its min and max aligned rows.
    fn single_column_rows(
        data_type: DataType,
        arrow_type: ArrowType,
        array: arrow::array::ArrayRef,
    ) -> (Vec<u8>, Vec<u8>) {
        let rt = RowType::new(vec![DataField::new("v", data_type, None)]);
        let schema = Schema::new(vec![Field::new("v", arrow_type, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array]).expect("batch");
        let bytes = serialize_statistics(&batch, &rt, &[0])
            .expect("serialize")
            .expect("statistics");

        // One column means a 9 byte prefix before the length-prefixed rows.
        let min_len = i32::from_le_bytes(bytes[9..13].try_into().unwrap()) as usize;
        let min = bytes[13..13 + min_len].to_vec();
        let max_at = 13 + min_len;
        let max_len = i32::from_le_bytes(bytes[max_at..max_at + 4].try_into().unwrap()) as usize;
        let max = bytes[max_at + 4..max_at + 4 + max_len].to_vec();
        (min, max)
    }

    /// The 8-byte slot of the single column, which starts after the null bits.
    fn only_slot(row: &[u8]) -> &[u8] {
        &row[8..16]
    }

    #[test]
    fn collects_bounds_for_boolean() {
        let (min, max) = single_column_rows(
            DataTypes::boolean(),
            ArrowType::Boolean,
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
            ])),
        );
        assert_eq!(only_slot(&min)[0], 0);
        assert_eq!(only_slot(&max)[0], 1);
    }

    #[test]
    fn collects_bounds_for_the_narrow_integers() {
        let (min, max) = single_column_rows(
            DataTypes::tinyint(),
            ArrowType::Int8,
            Arc::new(Int8Array::from(vec![Some(7), Some(-3)])),
        );
        assert_eq!(only_slot(&min)[0] as i8, -3);
        assert_eq!(only_slot(&max)[0] as i8, 7);

        let (min, max) = single_column_rows(
            DataTypes::smallint(),
            ArrowType::Int16,
            Arc::new(Int16Array::from(vec![Some(300), Some(-300)])),
        );
        assert_eq!(i16::from_le_bytes(min[8..10].try_into().unwrap()), -300);
        assert_eq!(i16::from_le_bytes(max[8..10].try_into().unwrap()), 300);
    }

    #[test]
    fn collects_bounds_for_the_floating_types() {
        let (min, max) = single_column_rows(
            DataTypes::float(),
            ArrowType::Float32,
            Arc::new(Float32Array::from(vec![Some(2.5), Some(-1.5)])),
        );
        assert_eq!(f32::from_le_bytes(min[8..12].try_into().unwrap()), -1.5);
        assert_eq!(f32::from_le_bytes(max[8..12].try_into().unwrap()), 2.5);

        let (min, max) = single_column_rows(
            DataTypes::double(),
            ArrowType::Float64,
            Arc::new(Float64Array::from(vec![Some(2.5), Some(-1.5)])),
        );
        assert_eq!(f64::from_le_bytes(min[8..16].try_into().unwrap()), -1.5);
        assert_eq!(f64::from_le_bytes(max[8..16].try_into().unwrap()), 2.5);
    }

    #[test]
    fn collects_bounds_for_char_like_a_string() {
        let (min, max) = single_column_rows(
            DataTypes::char(2),
            ArrowType::Utf8,
            Arc::new(StringArray::from(vec![Some("bb"), Some("aa")])),
        );
        // Two bytes inline, with the length marker in the slot's top byte.
        assert_eq!(&only_slot(&min)[..2], b"aa");
        assert_eq!(only_slot(&min)[7], 0x82);
        assert_eq!(&only_slot(&max)[..2], b"bb");
    }

    #[test]
    fn collects_bounds_for_date_as_epoch_days() {
        let (min, max) = single_column_rows(
            DataTypes::date(),
            ArrowType::Date32,
            Arc::new(Date32Array::from(vec![Some(19_000), Some(18_000)])),
        );
        assert_eq!(i32::from_le_bytes(min[8..12].try_into().unwrap()), 18_000);
        assert_eq!(i32::from_le_bytes(max[8..12].try_into().unwrap()), 19_000);
    }

    /// The Arrow array holds seconds at precision 0, but the statistics format
    /// is always millis of day.
    #[test]
    fn scales_a_second_precision_time_to_millis() {
        let (min, max) = single_column_rows(
            DataTypes::time_with_precision(0),
            ArrowType::Time32(arrow::datatypes::TimeUnit::Second),
            Arc::new(Time32SecondArray::from(vec![Some(7_200), Some(3_600)])),
        );
        assert_eq!(
            i32::from_le_bytes(min[8..12].try_into().unwrap()),
            3_600_000
        );
        assert_eq!(
            i32::from_le_bytes(max[8..12].try_into().unwrap()),
            7_200_000
        );
    }

    #[test]
    fn collects_bounds_for_time_as_millis_of_day() {
        let (min, max) = single_column_rows(
            DataTypes::time(),
            ArrowType::Time32(arrow::datatypes::TimeUnit::Millisecond),
            Arc::new(Time32MillisecondArray::from(vec![
                Some(7_200_000),
                Some(3_600_000),
            ])),
        );
        assert_eq!(
            i32::from_le_bytes(min[8..12].try_into().unwrap()),
            3_600_000
        );
        assert_eq!(
            i32::from_le_bytes(max[8..12].try_into().unwrap()),
            7_200_000
        );
    }

    #[test]
    fn scales_a_microsecond_time_to_millis() {
        let (min, max) = single_column_rows(
            DataTypes::time_with_precision(6),
            ArrowType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            Arc::new(Time64MicrosecondArray::from(vec![
                Some(7_200_000_000),
                Some(3_600_000_000),
            ])),
        );
        assert_eq!(
            i32::from_le_bytes(min[8..12].try_into().unwrap()),
            3_600_000
        );
        assert_eq!(
            i32::from_le_bytes(max[8..12].try_into().unwrap()),
            7_200_000
        );
    }

    #[test]
    fn scales_a_nanosecond_time_to_millis() {
        let (min, max) = single_column_rows(
            DataTypes::time_with_precision(9),
            ArrowType::Time64(arrow::datatypes::TimeUnit::Nanosecond),
            Arc::new(Time64NanosecondArray::from(vec![
                Some(7_200_000_000_000),
                Some(3_600_000_000_000),
            ])),
        );
        assert_eq!(
            i32::from_le_bytes(min[8..12].try_into().unwrap()),
            3_600_000
        );
        assert_eq!(
            i32::from_le_bytes(max[8..12].try_into().unwrap()),
            7_200_000
        );
    }

    #[test]
    fn scales_a_second_precision_timestamp_to_millis() {
        let (min, max) = single_column_rows(
            DataTypes::timestamp_with_precision(0),
            ArrowType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            Arc::new(TimestampSecondArray::from(vec![Some(2), Some(1)])),
        );
        // Precision 0 is compact, so the millis sit in the slot.
        assert_eq!(i64::from_le_bytes(min[8..16].try_into().unwrap()), 1_000);
        assert_eq!(i64::from_le_bytes(max[8..16].try_into().unwrap()), 2_000);
    }

    /// Decodes a non-compact timestamp field into its (millis, nanos) pair.
    fn split_timestamp(row: &[u8]) -> (i64, i32) {
        let packed = i64::from_le_bytes(row[8..16].try_into().unwrap());
        let offset = (packed >> 32) as usize;
        let nanos = (packed & 0xFFFF_FFFF) as i32;
        let millis = i64::from_le_bytes(row[offset..offset + 8].try_into().unwrap());
        (millis, nanos)
    }

    #[test]
    fn splits_a_nanosecond_timestamp_into_millis_and_nanos() {
        let (min, max) = single_column_rows(
            DataTypes::timestamp_with_precision(9),
            ArrowType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(2_000_456_789),
                Some(1_000_654_321),
            ])),
        );
        assert_eq!(split_timestamp(&min), (1_000, 654_321));
        assert_eq!(split_timestamp(&max), (2_000, 456_789));
    }

    /// An appended batch can declare a different scale from the column, and the
    /// raw integers alone cannot tell the two apart.
    #[test]
    fn rescales_a_decimal_from_the_arrays_own_scale() {
        // 1234 at the array's scale 2 is 12.34, which the column stores at
        // scale 3 as 12340 to keep it as 12.34.
        let array = Decimal128Array::from(vec![Some(1_234_i128), Some(500_i128)])
            .with_precision_and_scale(10, 2)
            .expect("decimal array");
        let (min, max) = single_column_rows(
            DataTypes::decimal(10, 3),
            ArrowType::Decimal128(10, 2),
            Arc::new(array),
        );
        assert_eq!(i64::from_le_bytes(min[8..16].try_into().unwrap()), 5_000);
        assert_eq!(i64::from_le_bytes(max[8..16].try_into().unwrap()), 12_340);
    }

    #[test]
    fn spills_a_non_compact_decimal_bound_to_the_tail() {
        let array = Decimal128Array::from(vec![Some(555_000_i128), Some(100_000_i128)])
            .with_precision_and_scale(25, 5)
            .expect("decimal array");
        let (min, _) = single_column_rows(
            DataTypes::decimal(25, 5),
            ArrowType::Decimal128(25, 5),
            Arc::new(array),
        );
        // Precision 25 is not compact, so the slot points into the tail.
        let packed = i64::from_le_bytes(min[8..16].try_into().unwrap());
        let (offset, size) = ((packed >> 32) as usize, (packed & 0xFFFF_FFFF) as usize);
        assert_eq!(offset, 16);
        let unscaled = Decimal::from_unscaled_bytes(&min[offset..offset + size], 25, 5)
            .expect("decimal")
            .to_big_decimal();
        assert_eq!(unscaled.to_string(), "1.00000");
    }

    #[test]
    fn collects_bounds_for_a_compact_decimal() {
        let array = Decimal128Array::from(vec![Some(12_345_i128), Some(500_i128)])
            .with_precision_and_scale(10, 2)
            .expect("decimal array");
        let (min, max) = single_column_rows(
            DataTypes::decimal(10, 2),
            ArrowType::Decimal128(10, 2),
            Arc::new(array),
        );
        // Precision 10 is compact, so the unscaled value sits in the slot.
        assert_eq!(i64::from_le_bytes(min[8..16].try_into().unwrap()), 500);
        assert_eq!(i64::from_le_bytes(max[8..16].try_into().unwrap()), 12_345);
    }

    #[test]
    fn keeps_a_millisecond_timestamp_in_its_slot() {
        let (min, max) = single_column_rows(
            DataTypes::timestamp_with_precision(3),
            ArrowType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            Arc::new(TimestampMillisecondArray::from(vec![
                Some(2_000),
                Some(1_000),
            ])),
        );
        assert_eq!(i64::from_le_bytes(min[8..16].try_into().unwrap()), 1_000);
        assert_eq!(i64::from_le_bytes(max[8..16].try_into().unwrap()), 2_000);
    }

    #[test]
    fn splits_a_microsecond_timestamp_into_millis_and_nanos() {
        let (min, max) = single_column_rows(
            DataTypes::timestamp_with_precision(6),
            ArrowType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(2_000_500),
                Some(1_000_456),
            ])),
        );
        // Precision 6 is not compact, so millis move to the tail and the slot
        // carries the offset with the nano-of-millisecond.
        assert_eq!(split_timestamp(&min), (1_000, 456_000));
        assert_eq!(split_timestamp(&max), (2_000, 500_000));
    }

    #[test]
    fn collects_bounds_for_a_local_zoned_timestamp() {
        let array =
            TimestampMillisecondArray::from(vec![Some(2_000), Some(1_000)]).with_timezone("UTC");
        let (min, max) = single_column_rows(
            DataTypes::timestamp_ltz_with_precision(3),
            ArrowType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, Some("UTC".into())),
            Arc::new(array),
        );
        assert_eq!(i64::from_le_bytes(min[8..16].try_into().unwrap()), 1_000);
        assert_eq!(i64::from_le_bytes(max[8..16].try_into().unwrap()), 2_000);
    }
}
