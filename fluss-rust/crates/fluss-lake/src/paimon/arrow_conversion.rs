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

//! Lossless adaptation of Paimon's Arrow representation to the Fluss schema.
//! This is deliberately not a general schema-evolution or coercion layer.

use crate::{FlussLakeError, Result};
use arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, ListArray, MapArray, StructArray, make_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use std::sync::Arc;

pub(super) fn normalize_batch(batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    if batch.schema_ref() == &schema {
        return Ok(batch);
    }
    let incompatible = FlussLakeError::SchemaIncompatible;
    if batch.num_columns() != schema.fields().len() {
        return Err(incompatible(format!(
            "Paimon returned {} columns, expected {}",
            batch.num_columns(),
            schema.fields().len()
        )));
    }
    let columns = batch
        .columns()
        .iter()
        .zip(batch.schema_ref().fields())
        .zip(schema.fields())
        .map(|((array, actual), expected)| {
            if actual.name() != expected.name() {
                return Err(incompatible(format!(
                    "Paimon returned column {}, expected {}",
                    actual.name(),
                    expected.name()
                )));
            }
            normalize_array(array, expected.data_type()).map_err(|error| {
                incompatible(format!(
                    "cannot adapt Paimon column {} from {} to {}: {error}",
                    expected.name(),
                    array.data_type(),
                    expected.data_type()
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new_with_options(
        schema,
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|error| incompatible(format!("invalid Paimon result batch: {error}")))
}

fn normalize_array(array: &ArrayRef, expected: &DataType) -> arrow::error::Result<ArrayRef> {
    if array.data_type() == expected {
        return Ok(array.clone());
    }
    match (array.data_type(), expected) {
        (DataType::List(_), DataType::List(field)) => {
            let list = array.as_list::<i32>();
            let (offsets, values) = normalize_children(
                list.values(),
                list.offsets(),
                list.nulls(),
                field.data_type(),
            )?;
            Ok(Arc::new(ListArray::try_new(
                field.clone(),
                offsets,
                values,
                list.nulls().cloned(),
            )?))
        }
        (DataType::Struct(actual), DataType::Struct(fields))
            if actual.len() == fields.len()
                && actual.iter().zip(fields).all(|(a, b)| a.name() == b.name()) =>
        {
            let structure = array.as_struct();
            let children = structure
                .columns()
                .iter()
                .zip(fields)
                .map(|(child, field)| {
                    if structure.null_count() == 0 || child.data_type() == field.data_type() {
                        return normalize_array(child, field.data_type());
                    }
                    // A null struct can contain arbitrary child placeholders.
                    // They are not logical values and must not fail a strict
                    // binary-length or temporal-precision check.
                    let child = make_array(
                        child
                            .to_data()
                            .into_builder()
                            .nulls(NullBuffer::union(child.nulls(), structure.nulls()))
                            .build()?,
                    );
                    normalize_array(&child, field.data_type())
                })
                .collect::<arrow::error::Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                children,
                structure.nulls().cloned(),
                structure.len(),
            )?))
        }
        (DataType::Map(_, sorted), DataType::Map(field, expected_sorted))
            if sorted == expected_sorted =>
        {
            let map = array.as_map();
            let entries: ArrayRef = Arc::new(map.entries().clone());
            let (offsets, entries) =
                normalize_children(&entries, map.offsets(), map.nulls(), field.data_type())?;
            Ok(Arc::new(MapArray::try_new(
                field.clone(),
                offsets,
                entries.as_struct().clone(),
                map.nulls().cloned(),
                *expected_sorted,
            )?))
        }
        (DataType::Binary, DataType::FixedSizeBinary(width)) if *width > 0 => {
            // Strict casting checks every non-null value's length. The default
            // safe cast would instead silently replace invalid values with null.
            cast_with_options(array, expected, &strict_cast_options())
        }
        (DataType::Timestamp(from, actual_zone), DataType::Timestamp(to, expected_zone))
            if actual_zone == expected_zone =>
        {
            cast_temporal_losslessly(array, expected, *from, *to)
        }
        (
            DataType::Time32(from) | DataType::Time64(from),
            DataType::Time32(to) | DataType::Time64(to),
        ) => cast_temporal_losslessly(array, expected, *from, *to),
        _ => Err(ArrowError::CastError(format!(
            "unsupported Paimon representation change from {} to {expected}",
            array.data_type()
        ))),
    }
}

// List and Map slices share child buffers with the original array. Strict casts
// must not inspect values outside the slice or hidden behind a null parent.
fn normalize_children(
    values: &ArrayRef,
    offsets: &OffsetBuffer<i32>,
    nulls: Option<&NullBuffer>,
    expected: &DataType,
) -> arrow::error::Result<(OffsetBuffer<i32>, ArrayRef)> {
    if values.data_type() == expected {
        return Ok((offsets.clone(), values.clone()));
    }
    let (offsets, values) =
        if let Some(nulls) = nulls.filter(|validity| offsets.has_non_empty_nulls(Some(validity))) {
            let indices = Int32Array::from_iter_values(
                nulls
                    .valid_slices()
                    .flat_map(|(start, end)| offsets[start]..offsets[end]),
            );
            let lengths = offsets
                .lengths()
                .zip(nulls.iter())
                .map(|(len, valid)| if valid { len } else { 0 });
            (
                OffsetBuffer::from_lengths(lengths),
                arrow::compute::take(values, &indices, None)?,
            )
        } else {
            let start = offsets[0];
            let len = offsets[offsets.len() - 1] - start;
            (
                offsets.clone().subtract(start),
                values.slice(start as usize, len as usize),
            )
        };
    Ok((offsets, normalize_array(&values, expected)?))
}

fn strict_cast_options() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..Default::default()
    }
}

fn cast_temporal_losslessly(
    array: &ArrayRef,
    expected: &DataType,
    from: TimeUnit,
    to: TimeUnit,
) -> arrow::error::Result<ArrayRef> {
    let options = strict_cast_options();
    let converted = cast_with_options(array, expected, &options)?;
    // Upscaling cannot truncate, and strict casts reject timestamp overflow.
    if to >= from {
        return Ok(converted);
    }
    // Downscaling can truncate, so the value must survive a round trip.
    let restored = cast_with_options(&converted, array.data_type(), &options)?;
    if restored.to_data() != array.to_data() {
        return Err(ArrowError::CastError(
            "temporal conversion would lose precision".to_string(),
        ));
    }
    Ok(converted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BinaryArray, BinaryBuilder, ListBuilder, MapBuilder, MapFieldNames, Time32MillisecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{Field, Schema, Time64MicrosecondType, Time64NanosecondType};
    use fluss::metadata::{DataField, DataTypes};

    const LAST_MILLI_OF_DAY: i32 = 86_399_999;

    fn schema(data_type: fluss::metadata::DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            "value",
            fluss::record::to_arrow_type(&data_type).unwrap(),
            true,
        )]))
    }

    fn convert(array: ArrayRef, data_type: fluss::metadata::DataType) -> Result<RecordBatch> {
        let batch = RecordBatch::try_from_iter([("value", array)]).unwrap();
        normalize_batch(batch, schema(data_type))
    }

    #[test]
    fn binary_conversion_preserves_values_and_rejects_invalid_lengths() {
        let source = RecordBatch::try_from_iter([(
            "value",
            Arc::new(arrow::array::BinaryArray::from(vec![
                Some(b"abcd".as_slice()),
                None,
                Some(b"\0\0\xff\0".as_slice()),
            ])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let result = normalize_batch(source.slice(1, 2), schema(DataTypes::binary(4))).unwrap();
        let values = result.column(0).as_fixed_size_binary();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![None, Some(b"\0\0\xff\0".as_slice())]
        );
        for invalid in [b"".as_slice(), b"abc", b"abcde"] {
            let result = convert(
                Arc::new(BinaryArray::from(vec![
                    Some(b"abcd".as_slice()),
                    None,
                    Some(invalid),
                ])),
                DataTypes::binary(4),
            );
            assert!(matches!(result, Err(FlussLakeError::SchemaIncompatible(_))));
        }
    }

    #[test]
    fn temporal_conversion_is_exact_for_negative_values_nulls_and_timezones() {
        for timezone in [None, Some("UTC")] {
            let source = arrow::array::TimestampMillisecondArray::from(vec![
                Some(-2000),
                None,
                Some(0),
                Some(3000),
            ])
            .with_timezone_opt(timezone);
            let source =
                RecordBatch::try_from_iter([("value", Arc::new(source) as arrow::array::ArrayRef)])
                    .unwrap();
            let logical_type = if timezone.is_some() {
                DataTypes::timestamp_ltz_with_precision(0)
            } else {
                DataTypes::timestamp_with_precision(0)
            };
            let result = normalize_batch(source, schema(logical_type)).unwrap();
            let values = result
                .column(0)
                .as_primitive::<arrow::datatypes::TimestampSecondType>();
            assert_eq!(
                values.iter().collect::<Vec<_>>(),
                vec![Some(-2), None, Some(0), Some(3)]
            );
        }
        let result = convert(
            Arc::new(Time32MillisecondArray::from(vec![Some(2000), None])),
            DataTypes::time_with_precision(0),
        )
        .unwrap();
        assert_eq!(
            result
                .column(0)
                .as_primitive::<arrow::datatypes::Time32SecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), None],
        );
    }

    #[test]
    fn temporal_conversion_rejects_precision_loss_overflow_and_timezone_changes() {
        for value in [1, -1, 1001, -1001, i64::MIN, i64::MAX] {
            let result = convert(
                Arc::new(TimestampMillisecondArray::from(vec![None, Some(value)])),
                DataTypes::timestamp_with_precision(0),
            );
            assert!(matches!(result, Err(FlussLakeError::SchemaIncompatible(_))));
        }
        let overflow = convert(
            Arc::new(TimestampMillisecondArray::from(vec![Some(i64::MAX)])),
            DataTypes::timestamp_with_precision(9),
        );
        assert!(matches!(
            overflow,
            Err(FlussLakeError::SchemaIncompatible(_))
        ));
        let time_loss = convert(
            Arc::new(Time32MillisecondArray::from(vec![Some(1)])),
            DataTypes::time_with_precision(0),
        );
        assert!(matches!(
            time_loss,
            Err(FlussLakeError::SchemaIncompatible(_))
        ));
        let timezone_change = convert(
            Arc::new(
                TimestampMillisecondArray::from(vec![Some(1000)]).with_timezone("Europe/Paris"),
            ),
            DataTypes::timestamp_ltz_with_precision(0),
        );
        assert!(matches!(
            timezone_change,
            Err(FlussLakeError::SchemaIncompatible(_))
        ));
        // Nanosecond extremes are valid when no conversion is needed.
        let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(i64::MIN),
            Some(i64::MAX),
        ]));
        let result = convert(array.clone(), DataTypes::timestamp_with_precision(9)).unwrap();
        assert!(Arc::ptr_eq(result.column(0), &array));
    }

    #[test]
    fn time_upscaling_keeps_every_value() {
        let source: ArrayRef = Arc::new(Time32MillisecondArray::from(vec![
            Some(-1),
            None,
            Some(LAST_MILLI_OF_DAY),
        ]));
        let micros = convert(source.clone(), DataTypes::time_with_precision(6)).unwrap();
        assert_eq!(
            micros
                .column(0)
                .as_primitive::<Time64MicrosecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-1_000), None, Some(86_399_999_000)],
        );
        let nanos = convert(source, DataTypes::time_with_precision(9)).unwrap();
        assert_eq!(
            nanos
                .column(0)
                .as_primitive::<Time64NanosecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-1_000_000), None, Some(86_399_999_000_000)],
        );
    }

    #[test]
    fn list_field_normalization_reuses_buffers_and_preserves_slices() {
        let values: ArrayRef = Arc::new(BinaryArray::from(vec![
            b"skip".as_slice(),
            b"x",
            b"abcd",
            b"y",
        ]));
        let list = ListArray::new(
            Arc::new(Field::new("element", DataType::Binary, true)),
            arrow::buffer::OffsetBuffer::from_lengths([1, 1, 1, 1]),
            values.clone(),
            Some(vec![true, false, true, true].into()),
        );
        let slice: ArrayRef = Arc::new(list.slice(1, 2));
        let unchanged = convert(slice.clone(), DataTypes::array(DataTypes::bytes())).unwrap();
        assert!(Arc::ptr_eq(
            unchanged.column(0).as_list::<i32>().values(),
            &values
        ));
        // The null list and the rows outside the slice are not logical values.
        let result = convert(slice, DataTypes::array(DataTypes::binary(4))).unwrap();
        let list = result.column(0).as_list::<i32>();
        assert!(list.is_null(0));
        assert_eq!(list.value(1).as_fixed_size_binary().value(0), b"abcd");
    }

    #[test]
    fn nested_struct_map_and_list_are_adapted_recursively() {
        let names = MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        };
        let mut map = MapBuilder::new(
            Some(names),
            arrow::array::Int32Builder::new(),
            ListBuilder::new(BinaryBuilder::new()),
        );
        map.keys().append_value(6);
        map.values().values().append_value(b"x");
        map.values().append(true);
        map.append(true).unwrap();
        map.keys().append_value(7);
        map.values().values().append_value(b"abcd");
        map.values().values().append_null();
        map.values().append(true);
        map.append(true).unwrap();
        map.append(false).unwrap();
        let map: ArrayRef = Arc::new(map.finish().slice(1, 2));
        let row: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("nested", map.data_type().clone(), true)].into(),
            vec![map],
            None,
        ));
        let ty = DataTypes::row(vec![DataField::new(
            "nested",
            DataTypes::map(DataTypes::int(), DataTypes::array(DataTypes::binary(4))),
            None,
        )]);
        let result = convert(row, ty).unwrap();
        let map = result.column(0).as_struct().column(0).as_map();
        assert!(map.is_null(1));
        let list = map.values().as_list::<i32>();
        let binary = list.values().as_fixed_size_binary();
        assert_eq!(
            binary.iter().collect::<Vec<_>>(),
            vec![Some(b"abcd".as_slice()), None]
        );
    }

    #[test]
    fn null_struct_placeholders_are_not_converted_as_values() {
        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![b"abcd".as_slice(), b""]));
        let row = StructArray::new(
            vec![Field::new("bytes", DataType::Binary, false)].into(),
            vec![binary],
            Some(vec![true, false].into()),
        );
        let expected =
            DataType::Struct(vec![Field::new("bytes", DataType::FixedSizeBinary(4), false)].into());
        for array in [row.clone(), row.slice(1, 1)] {
            let array: ArrayRef = Arc::new(array);
            let result = normalize_array(&array, &expected).unwrap();
            assert_eq!(result.len(), array.len());
            assert_eq!(result.nulls(), array.nulls());
            let values = result.as_struct().column(0).as_fixed_size_binary();
            assert!(values.is_null(values.len() - 1));
            if values.len() == 2 {
                assert_eq!(values.value(0), b"abcd");
            }
        }
    }
}
