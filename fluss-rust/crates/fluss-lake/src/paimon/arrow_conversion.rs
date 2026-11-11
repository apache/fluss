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
use arrow::array::{Array, ArrayRef, AsArray, ListArray, MapArray, StructArray, make_array};
use arrow::buffer::NullBuffer;
use arrow::compute::{CastOptions, cast_with_options};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use std::sync::Arc;

pub(super) fn normalize_batch(batch: RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    if batch.schema_ref() == &schema {
        return Ok(batch);
    }
    let incompatible = |message: String| FlussLakeError::SchemaIncompatible(message);
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
            Ok(Arc::new(ListArray::try_new(
                field.clone(),
                list.offsets().clone(),
                normalize_array(list.values(), field.data_type())?,
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
            let entries = normalize_array(&entries, field.data_type())?;
            Ok(Arc::new(MapArray::try_new(
                field.clone(),
                map.offsets().clone(),
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
        (DataType::Timestamp(_, actual_zone), DataType::Timestamp(_, expected_zone))
            if actual_zone == expected_zone =>
        {
            cast_temporal_losslessly(array, expected)
        }
        (DataType::Time32(_) | DataType::Time64(_), DataType::Time32(_) | DataType::Time64(_)) => {
            cast_temporal_losslessly(array, expected)
        }
        _ => Err(ArrowError::CastError(format!(
            "unsupported Paimon representation change from {} to {expected}",
            array.data_type()
        ))),
    }
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
) -> arrow::error::Result<ArrayRef> {
    let options = strict_cast_options();
    let converted = cast_with_options(array, expected, &options)?;
    // Strict casts catch overflow, but downscaling timestamps can still
    // truncate. A round trip also rejects precision loss for negative values,
    // while Arrow equality ignores the unspecified contents of null slots.
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
        BinaryArray, BinaryBuilder, Int32Array, ListBuilder, MapBuilder, MapFieldNames,
        Time32MillisecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use fluss::metadata::{DataField, DataTypes};
    use paimon::spec::{ArrayType, BinaryType, DataType as PaimonType, IntType, TimestampType};

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
    fn actual_type_converters_and_ffi_agree_after_normalization() {
        let cases = [
            (
                DataTypes::array(DataTypes::int()),
                PaimonType::Array(ArrayType::new(PaimonType::Int(IntType::new()))),
            ),
            (
                DataTypes::binary(4),
                PaimonType::Binary(BinaryType::new(4).unwrap()),
            ),
            (
                DataTypes::timestamp_with_precision(0),
                PaimonType::Timestamp(TimestampType::new(0).unwrap()),
            ),
        ];
        for (fluss_type, paimon_type) in cases {
            let source_type = paimon::arrow::paimon_type_to_arrow(&paimon_type).unwrap();
            for length in [0, 2] {
                let source = arrow_array_58::RecordBatch::try_from_iter([(
                    "value",
                    arrow_array_58::new_null_array(&source_type, length),
                )])
                .unwrap();
                let imported = super::super::upgrade_paimon_arrow_batch(source).unwrap();
                let expected_schema = schema(fluss_type.clone());
                let normalized = normalize_batch(imported, expected_schema.clone()).unwrap();
                assert_eq!(normalized.schema(), expected_schema);
                assert_eq!(normalized.num_rows(), length);
                assert_eq!(normalized.column(0).null_count(), length);
            }
        }
    }

    #[test]
    fn binary_conversion_preserves_values_and_rejects_invalid_lengths() {
        let source = arrow_array_58::RecordBatch::try_from_iter([(
            "value",
            Arc::new(arrow_array_58::BinaryArray::from(vec![
                Some(b"abcd".as_slice()),
                None,
                Some(b"\0\0\xff\0".as_slice()),
            ])) as arrow_array_58::ArrayRef,
        )])
        .unwrap();
        let imported = super::super::upgrade_paimon_arrow_batch(source).unwrap();
        let result = normalize_batch(imported.slice(1, 2), schema(DataTypes::binary(4))).unwrap();
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
            let source = arrow_array_58::TimestampMillisecondArray::from(vec![
                Some(-2000),
                None,
                Some(0),
                Some(3000),
            ])
            .with_timezone_opt(timezone);
            let source = arrow_array_58::RecordBatch::try_from_iter([(
                "value",
                Arc::new(source) as arrow_array_58::ArrayRef,
            )])
            .unwrap();
            let imported = super::super::upgrade_paimon_arrow_batch(source).unwrap();
            let logical_type = if timezone.is_some() {
                DataTypes::timestamp_ltz_with_precision(0)
            } else {
                DataTypes::timestamp_with_precision(0)
            };
            let result = normalize_batch(imported, schema(logical_type)).unwrap();
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
    fn list_field_normalization_reuses_buffers_and_preserves_slices() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let list = ListArray::new(
            Arc::new(Field::new("element", DataType::Int32, true)),
            arrow::buffer::OffsetBuffer::new(vec![0_i32, 1, 1, 3].into()),
            values.clone(),
            Some(vec![true, false, true].into()),
        );
        let result = convert(
            Arc::new(list.slice(1, 2)),
            DataTypes::array(DataTypes::int()),
        )
        .unwrap();
        let list = result.column(0).as_list::<i32>();
        assert_eq!(list.value_offsets(), &[1, 1, 3]);
        assert!(list.is_null(0));
        assert_eq!(
            list.value(1)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some(3)]
        );
        assert!(Arc::ptr_eq(list.values(), &values));
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
        map.keys().append_value(7);
        map.values().values().append_value(b"abcd");
        map.values().values().append_null();
        map.values().append(true);
        map.append(true).unwrap();
        map.append(false).unwrap();
        let map: ArrayRef = Arc::new(map.finish());
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
    fn adaptation_does_not_permit_arbitrary_coercion_or_column_reordering() {
        let result = convert(Arc::new(Int32Array::from(vec![1])), DataTypes::bigint());
        assert!(matches!(result, Err(FlussLakeError::SchemaIncompatible(_))));
        let batch = RecordBatch::try_from_iter([(
            "other",
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        assert!(matches!(
            normalize_batch(batch, schema(DataTypes::int())),
            Err(FlussLakeError::SchemaIncompatible(_))
        ));
        let row: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new(
                "old",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            )),
            Arc::new(TimestampMillisecondArray::from(vec![Some(1000)])) as ArrayRef,
        )]));
        assert!(matches!(
            convert(
                row,
                DataTypes::row(vec![DataField::new(
                    "new",
                    DataTypes::timestamp_with_precision(0),
                    None
                )])
            ),
            Err(FlussLakeError::SchemaIncompatible(_))
        ));
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
