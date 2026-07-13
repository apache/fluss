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

//! View enums bridging binary and columnar `ARRAY` / `MAP` / `ROW`. Callers
//! dispatch through [`InternalArray`] / [`InternalMap`] / [`InternalRow`] and
//! pattern-match only when they need to distinguish the representation.

use crate::client::WriteFormat;
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataField, DataType, DataTypes, RowType};
use crate::row::Decimal;
use crate::row::binary_array::{FlussArray, FlussArrayWriter};
use crate::row::binary_map::FlussMap;
use crate::row::column::ColumnarRow;
use crate::row::column_vector::{TimeColumn, TimestampColumn, TypedColumn};
use crate::row::columnar::{ColumnarArray, ColumnarMap};
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz, internal_row_to_owned_generic};
use crate::row::{DataGetters, GenericRow, InternalArray, InternalMap, InternalRow};
use arrow::array::Array;
use arrow::datatypes::DataType as ArrowDataType;

/// Either an owned binary [`FlussArray`] or a borrowed columnar slice view
/// over an Arrow buffer. The `'a` lifetime is used only by the columnar
/// variant; binary values are self-owned (`FlussArray`'s `Bytes` is
/// reference-counted, so cloning is O(1)).
#[derive(Debug, Clone)]
pub enum ArrayView<'a> {
    Binary(FlussArray),
    Columnar(ColumnarArray<'a>),
}

impl ArrayView<'_> {
    /// Returns the owned binary [`FlussArray`] form, re-encoding a columnar
    /// slice if needed. Prefer the trait accessors for zero-copy reads.
    pub fn try_into_binary(self) -> Result<FlussArray> {
        match self {
            Self::Binary(a) => Ok(a),
            Self::Columnar(c) => materialize_columnar_array(&c),
        }
    }

    /// [`try_into_binary`](Self::try_into_binary) with `.expect()`. Test-only.
    #[cfg(any(test, feature = "integration_tests"))]
    pub fn expect_binary(self) -> FlussArray {
        self.try_into_binary()
            .expect("materialize ColumnarArray to FlussArray")
    }
}

impl InternalArray for ArrayView<'_> {
    fn size(&self) -> usize {
        delegate_array_view!(InternalArray, self.size())
    }
}

impl DataGetters for ArrayView<'_> {
    fn is_null_at(&self, pos: usize) -> Result<bool> {
        delegate_array_view!(DataGetters, self.is_null_at(pos))
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        delegate_array_view!(DataGetters, self.get_boolean(pos))
    }
    fn get_byte(&self, pos: usize) -> Result<i8> {
        delegate_array_view!(DataGetters, self.get_byte(pos))
    }
    fn get_short(&self, pos: usize) -> Result<i16> {
        delegate_array_view!(DataGetters, self.get_short(pos))
    }
    fn get_int(&self, pos: usize) -> Result<i32> {
        delegate_array_view!(DataGetters, self.get_int(pos))
    }
    fn get_long(&self, pos: usize) -> Result<i64> {
        delegate_array_view!(DataGetters, self.get_long(pos))
    }
    fn get_float(&self, pos: usize) -> Result<f32> {
        delegate_array_view!(DataGetters, self.get_float(pos))
    }
    fn get_double(&self, pos: usize) -> Result<f64> {
        delegate_array_view!(DataGetters, self.get_double(pos))
    }
    fn get_char(&self, pos: usize, length: usize) -> Result<&str> {
        delegate_array_view!(DataGetters, self.get_char(pos, length))
    }
    fn get_string(&self, pos: usize) -> Result<&str> {
        delegate_array_view!(DataGetters, self.get_string(pos))
    }
    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        delegate_array_view!(DataGetters, self.get_decimal(pos, precision, scale))
    }
    fn get_date(&self, pos: usize) -> Result<Date> {
        delegate_array_view!(DataGetters, self.get_date(pos))
    }
    fn get_time(&self, pos: usize) -> Result<Time> {
        delegate_array_view!(DataGetters, self.get_time(pos))
    }
    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        delegate_array_view!(DataGetters, self.get_timestamp_ntz(pos, precision))
    }
    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        delegate_array_view!(DataGetters, self.get_timestamp_ltz(pos, precision))
    }
    fn get_binary(&self, pos: usize, length: usize) -> Result<&[u8]> {
        delegate_array_view!(DataGetters, self.get_binary(pos, length))
    }
    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        delegate_array_view!(DataGetters, self.get_bytes(pos))
    }

    fn get_array(&self, pos: usize) -> Result<ArrayView<'_>> {
        delegate_array_view!(DataGetters, self.get_array(pos))
    }
    fn get_map(&self, pos: usize) -> Result<MapView<'_>> {
        delegate_array_view!(DataGetters, self.get_map(pos))
    }
    fn get_row(&self, pos: usize) -> Result<RowView<'_>> {
        delegate_array_view!(DataGetters, self.get_row(pos))
    }
}

/// Either an owned binary [`FlussMap`] or a borrowed columnar slice view.
#[derive(Debug, Clone)]
pub enum MapView<'a> {
    Binary(FlussMap),
    Columnar(ColumnarMap<'a>),
}

impl MapView<'_> {
    /// Returns the owned binary [`FlussMap`] form, re-encoding if needed.
    /// See [`ArrayView::try_into_binary`].
    pub fn try_into_binary(self) -> Result<FlussMap> {
        match self {
            Self::Binary(m) => Ok(m),
            Self::Columnar(c) => materialize_columnar_map(&c),
        }
    }

    /// [`try_into_binary`](Self::try_into_binary) with `.expect()`. Test-only.
    #[cfg(any(test, feature = "integration_tests"))]
    pub fn expect_binary(self) -> FlussMap {
        self.try_into_binary()
            .expect("materialize ColumnarMap to FlussMap")
    }
}

impl InternalMap for MapView<'_> {
    fn size(&self) -> usize {
        match self {
            Self::Binary(m) => InternalMap::size(m),
            Self::Columnar(m) => InternalMap::size(m),
        }
    }

    fn key_array(&self) -> &dyn InternalArray {
        match self {
            Self::Binary(m) => m.key_array(),
            Self::Columnar(m) => m.key_array(),
        }
    }

    fn value_array(&self) -> &dyn InternalArray {
        match self {
            Self::Binary(m) => m.value_array(),
            Self::Columnar(m) => m.value_array(),
        }
    }
}

/// Reconstructs a Fluss `DataType` from a `TypedColumn`. ROW field names
/// come from the underlying Arrow `StructArray`'s schema.
fn fluss_type_of(col: &TypedColumn) -> Result<DataType> {
    Ok(match col {
        TypedColumn::Boolean(_) => DataTypes::boolean(),
        TypedColumn::TinyInt(_) => DataTypes::tinyint(),
        TypedColumn::SmallInt(_) => DataTypes::smallint(),
        TypedColumn::Int(_) => DataTypes::int(),
        TypedColumn::BigInt(_) => DataTypes::bigint(),
        TypedColumn::Float(_) => DataTypes::float(),
        TypedColumn::Double(_) => DataTypes::double(),
        TypedColumn::String(_) => DataTypes::string(),
        TypedColumn::Bytes(_) => DataTypes::bytes(),
        TypedColumn::Binary(a) => DataTypes::binary(a.value_length() as usize),
        TypedColumn::Decimal(a, _) => {
            let (p, s) = match a.data_type() {
                ArrowDataType::Decimal128(p, s) => (*p, *s),
                other => {
                    return Err(IllegalArgument {
                        message: format!("Decimal128Array expected, got {other:?}"),
                    });
                }
            };
            DataTypes::decimal(p as u32, s as u32)
        }
        TypedColumn::Date(_) => DataTypes::date(),
        TypedColumn::Time(t) => DataTypes::time_with_precision(time_precision(t)),
        TypedColumn::Timestamp(t) => DataTypes::timestamp_with_precision(timestamp_precision(t)),
        TypedColumn::TimestampLtz(t) => {
            DataTypes::timestamp_ltz_with_precision(timestamp_precision(t))
        }
        TypedColumn::Array(_, inner) => DataTypes::array(fluss_type_of(inner)?),
        TypedColumn::Map(_, k, v) => DataTypes::map(fluss_type_of(k)?, fluss_type_of(v)?),
        TypedColumn::Row(struct_arr, inner_batch) => {
            let arrow_fields = match struct_arr.data_type() {
                ArrowDataType::Struct(f) => f,
                other => {
                    return Err(IllegalArgument {
                        message: format!("Struct expected for ROW column, got {other:?}"),
                    });
                }
            };
            let mut fields = Vec::with_capacity(inner_batch.columns.len());
            for (i, col) in inner_batch.columns.iter().enumerate() {
                let name = arrow_fields.get(i).map(|f| f.name().as_str()).unwrap_or("");
                fields.push(DataField::new(name.to_string(), fluss_type_of(col)?, None));
            }
            DataTypes::row(fields)
        }
    })
}

fn time_precision(t: &TimeColumn) -> u32 {
    match t {
        TimeColumn::Second(_) => 0,
        TimeColumn::Millisecond(_) => 3,
        TimeColumn::Microsecond(_) => 6,
        TimeColumn::Nanosecond(_) => 9,
    }
}

fn timestamp_precision(t: &TimestampColumn) -> u32 {
    match t {
        TimestampColumn::Second(_) => 0,
        TimestampColumn::Millisecond(_) => 3,
        TimestampColumn::Microsecond(_) => 6,
        TimestampColumn::Nanosecond(_) => 9,
    }
}

fn materialize_columnar_array(view: &ColumnarArray<'_>) -> Result<FlussArray> {
    let elem_type = fluss_type_of(view.element())?;
    let mut writer = FlussArrayWriter::new(view.size(), &elem_type);
    write_view_elements_into(view, &elem_type, &mut writer)?;
    writer.complete()
}

fn materialize_columnar_map(view: &ColumnarMap<'_>) -> Result<FlussMap> {
    // Map keys are non-nullable by the Fluss spec (mirrors `MapType::with_nullable`).
    let key_type = fluss_type_of(view.keys_slice().element())?.as_non_nullable();
    let value_type = fluss_type_of(view.values_slice().element())?;
    let key_array = materialize_columnar_array(view.keys_slice())?;
    let value_array = materialize_columnar_array(view.values_slice())?;
    FlussMap::from_arrays(&key_array, &value_array, &key_type, &value_type)
}

fn write_view_elements_into(
    view: &ColumnarArray<'_>,
    dt: &DataType,
    writer: &mut FlussArrayWriter,
) -> Result<()> {
    for i in 0..view.size() {
        if view.is_null_at(i)? {
            writer.set_null_at(i);
            continue;
        }
        match dt {
            DataType::Boolean(_) => writer.write_boolean(i, view.get_boolean(i)?),
            DataType::TinyInt(_) => writer.write_byte(i, view.get_byte(i)?),
            DataType::SmallInt(_) => writer.write_short(i, view.get_short(i)?),
            DataType::Int(_) => writer.write_int(i, view.get_int(i)?),
            DataType::BigInt(_) => writer.write_long(i, view.get_long(i)?),
            DataType::Float(_) => writer.write_float(i, view.get_float(i)?),
            DataType::Double(_) => writer.write_double(i, view.get_double(i)?),
            DataType::Char(_) | DataType::String(_) => {
                writer.write_string(i, view.get_string(i)?);
            }
            DataType::Binary(t) => {
                writer.write_binary_bytes(i, view.get_binary(i, t.length())?);
            }
            DataType::Bytes(_) => writer.write_binary_bytes(i, view.get_bytes(i)?),
            DataType::Decimal(d) => {
                let dec = view.get_decimal(i, d.precision() as usize, d.scale() as usize)?;
                writer.write_decimal(i, &dec, d.precision());
            }
            DataType::Date(_) => writer.write_date(i, view.get_date(i)?),
            DataType::Time(_) => writer.write_time(i, view.get_time(i)?),
            DataType::Timestamp(t) => {
                let ts = view.get_timestamp_ntz(i, t.precision())?;
                writer.write_timestamp_ntz(i, &ts, t.precision());
            }
            DataType::TimestampLTz(t) => {
                let ts = view.get_timestamp_ltz(i, t.precision())?;
                writer.write_timestamp_ltz(i, &ts, t.precision());
            }
            DataType::Array(_) => {
                let nested = view.get_array(i)?.try_into_binary()?;
                writer.write_array(i, &nested);
            }
            DataType::Map(_) => {
                let nested = view.get_map(i)?.try_into_binary()?;
                writer.write_map(i, &nested);
            }
            DataType::Row(_) => match view.get_row(i)? {
                RowView::Columnar(cr) => writer.write_row(i, &cr)?,
                RowView::Generic(g) => writer.write_row(i, g)?,
            },
        }
    }
    Ok(())
}

/// Delegates an [`ArrayView`] method to whichever variant is present, going
/// through the trait so the call binds to the [`InternalArray`] impl rather
/// than any inherent method of the same name (which may have different
/// argument types — e.g. `FlussArray::get_decimal` takes `u32`s).
macro_rules! delegate_array_view {
    ($trait:ident, $self:ident . $method:ident ( $($arg:expr),* )) => {
        match $self {
            Self::Binary(x) => $trait::$method(x, $($arg),*),
            Self::Columnar(x) => $trait::$method(x, $($arg),*),
        }
    };
}
use delegate_array_view;

/// Either a borrowed reference to a materialized [`GenericRow`] (binary /
/// compacted paths) or an owned [`ColumnarRow`] cursor (Arrow scan path).
#[derive(Debug)]
pub enum RowView<'a> {
    Generic(&'a GenericRow<'a>),
    Columnar(ColumnarRow),
}

impl RowView<'_> {
    /// Materializes into an owned [`GenericRow`], walking a columnar cursor
    /// field-by-field if needed. Read-only consumers should iterate the
    /// [`InternalRow`] trait directly.
    pub fn try_into_generic(self, row_type: &RowType) -> Result<GenericRow<'static>> {
        match self {
            Self::Generic(g) => Ok(g.clone().into_owned()),
            Self::Columnar(cr) => internal_row_to_owned_generic(&cr, row_type),
        }
    }
}

/// Delegates a [`RowView`] method through the [`InternalRow`] trait. Method
/// syntax handles the `&&GenericRow → &GenericRow` auto-deref on the
/// Generic variant.
macro_rules! delegate_row_view {
    ($trait:ident, $self:ident . $method:ident ( $($arg:expr),* )) => {
        match $self {
            Self::Generic(x) => $trait::$method(*x, $($arg),*),
            Self::Columnar(x) => $trait::$method(x, $($arg),*),
        }
    };
}

impl InternalRow for RowView<'_> {
    fn get_field_count(&self) -> usize {
        delegate_row_view!(InternalRow, self.get_field_count())
    }

    fn as_encoded_bytes(&self, write_format: WriteFormat) -> Option<&[u8]> {
        delegate_row_view!(InternalRow, self.as_encoded_bytes(write_format))
    }
}

impl DataGetters for RowView<'_> {
    fn is_null_at(&self, pos: usize) -> Result<bool> {
        delegate_row_view!(DataGetters, self.is_null_at(pos))
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        delegate_row_view!(DataGetters, self.get_boolean(pos))
    }
    fn get_byte(&self, pos: usize) -> Result<i8> {
        delegate_row_view!(DataGetters, self.get_byte(pos))
    }
    fn get_short(&self, pos: usize) -> Result<i16> {
        delegate_row_view!(DataGetters, self.get_short(pos))
    }
    fn get_int(&self, pos: usize) -> Result<i32> {
        delegate_row_view!(DataGetters, self.get_int(pos))
    }
    fn get_long(&self, pos: usize) -> Result<i64> {
        delegate_row_view!(DataGetters, self.get_long(pos))
    }
    fn get_float(&self, pos: usize) -> Result<f32> {
        delegate_row_view!(DataGetters, self.get_float(pos))
    }
    fn get_double(&self, pos: usize) -> Result<f64> {
        delegate_row_view!(DataGetters, self.get_double(pos))
    }

    fn get_char(&self, pos: usize, length: usize) -> Result<&str> {
        delegate_row_view!(DataGetters, self.get_char(pos, length))
    }
    fn get_string(&self, pos: usize) -> Result<&str> {
        delegate_row_view!(DataGetters, self.get_string(pos))
    }

    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        delegate_row_view!(DataGetters, self.get_decimal(pos, precision, scale))
    }

    fn get_date(&self, pos: usize) -> Result<Date> {
        delegate_row_view!(DataGetters, self.get_date(pos))
    }
    fn get_time(&self, pos: usize) -> Result<Time> {
        delegate_row_view!(DataGetters, self.get_time(pos))
    }
    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        delegate_row_view!(DataGetters, self.get_timestamp_ntz(pos, precision))
    }
    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        delegate_row_view!(DataGetters, self.get_timestamp_ltz(pos, precision))
    }

    fn get_binary(&self, pos: usize, length: usize) -> Result<&[u8]> {
        delegate_row_view!(DataGetters, self.get_binary(pos, length))
    }
    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        delegate_row_view!(DataGetters, self.get_bytes(pos))
    }

    fn get_array(&self, pos: usize) -> Result<ArrayView<'_>> {
        delegate_row_view!(DataGetters, self.get_array(pos))
    }
    fn get_map(&self, pos: usize) -> Result<MapView<'_>> {
        delegate_row_view!(DataGetters, self.get_map(pos))
    }
    fn get_row(&self, pos: usize) -> Result<RowView<'_>> {
        delegate_row_view!(DataGetters, self.get_row(pos))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypes, RowType};
    use crate::row::binary_array::FlussArrayWriter;
    use crate::row::binary_map::FlussMapWriter;
    use crate::row::column::ColumnarRow;
    use crate::row::column_vector::{TypedBatch, TypedColumn};
    use crate::row::field_getter::FieldGetter;
    use crate::row::{DataGetters, Datum};
    use arrow::array::{
        Array, ArrayRef, Int32Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema};
    use std::sync::Arc;

    #[test]
    fn binary_array_view_delegates_to_fluss_array() {
        let mut w = FlussArrayWriter::new(2, &DataTypes::int());
        w.write_int(0, 7);
        w.write_int(1, 8);
        let arr = w.complete().unwrap();
        let view = ArrayView::Binary(arr);

        let dispatched: &dyn InternalArray = &view;
        assert_eq!(dispatched.size(), 2);
        assert_eq!(dispatched.get_int(0).unwrap(), 7);
        assert_eq!(dispatched.get_int(1).unwrap(), 8);
    }

    #[test]
    fn columnar_array_view_delegates_to_slice() {
        let values = Int32Array::from(vec![10, 20, 30, 40, 50]);
        let offsets = OffsetBuffer::new(vec![0_i32, 2, 5].into());
        let list = ListArray::new(
            Arc::new(Field::new("item", ArrowDataType::Int32, true)),
            offsets,
            Arc::new(values),
            None,
        );
        let rb = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "arr",
                list.data_type().clone(),
                true,
            )])),
            vec![Arc::new(list.clone()) as ArrayRef],
        )
        .unwrap();
        let rt = RowType::new(vec![DataField::new(
            "arr",
            DataTypes::array(DataTypes::int()),
            None,
        )]);
        let tb = TypedBatch::build(&rb, &rt).unwrap();
        let element = match &tb.columns[0] {
            TypedColumn::Array(_, e) => e.as_ref(),
            _ => unreachable!(),
        };
        // Second row's slice — [30, 40, 50].
        let view = ArrayView::Columnar(ColumnarArray::new(element, 2, 3));

        let dispatched: &dyn InternalArray = &view;
        assert_eq!(dispatched.size(), 3);
        assert_eq!(dispatched.get_int(0).unwrap(), 30);
        assert_eq!(dispatched.get_int(2).unwrap(), 50);
    }

    #[test]
    fn binary_map_view_delegates_to_fluss_map() {
        let mut w = FlussMapWriter::new(2, &DataTypes::int(), &DataTypes::string());
        w.write_entry(1.into(), "a".into()).unwrap();
        w.write_entry(2.into(), "b".into()).unwrap();
        let map = w.complete().unwrap();
        let view = MapView::Binary(map);

        let dispatched: &dyn InternalMap = &view;
        assert_eq!(dispatched.size(), 2);
        assert_eq!(dispatched.key_array().get_int(0).unwrap(), 1);
        assert_eq!(dispatched.value_array().get_string(0).unwrap(), "a");
    }

    #[test]
    fn columnar_map_view_delegates_to_slice() {
        let keys = StringArray::from(vec!["x", "y"]);
        let values = Int32Array::from(vec![100, 200]);
        let entries_fields = Fields::from(vec![
            Field::new("keys", ArrowDataType::Utf8, false),
            Field::new("values", ArrowDataType::Int32, true),
        ]);
        let entries = StructArray::new(
            entries_fields.clone(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values) as ArrayRef],
            None,
        );
        let offsets = OffsetBuffer::new(vec![0_i32, 2].into());
        let map = MapArray::new(
            Arc::new(Field::new(
                "entries",
                ArrowDataType::Struct(entries_fields),
                false,
            )),
            offsets,
            entries,
            None,
            false,
        );
        let rb = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "m",
                map.data_type().clone(),
                true,
            )])),
            vec![Arc::new(map.clone()) as ArrayRef],
        )
        .unwrap();
        let rt = RowType::new(vec![DataField::new(
            "m",
            DataTypes::map(DataTypes::string(), DataTypes::int()),
            None,
        )]);
        let tb = TypedBatch::build(&rb, &rt).unwrap();
        let (key_col, value_col) = match &tb.columns[0] {
            TypedColumn::Map(_, k, v) => (k.as_ref(), v.as_ref()),
            _ => unreachable!(),
        };
        let view = MapView::Columnar(ColumnarMap::new(key_col, value_col, 0, 2));

        let dispatched: &dyn InternalMap = &view;
        assert_eq!(dispatched.size(), 2);
        assert_eq!(dispatched.key_array().get_string(0).unwrap(), "x");
        assert_eq!(dispatched.value_array().get_int(1).unwrap(), 200);
    }

    #[test]
    fn array_of_row_with_nested_array_materializes_end_to_end() {
        // [ { x: 1, y: [10, 20] }, { x: 2, y: [30] } ]
        let inner_x = Int32Array::from(vec![1, 2]);
        let inner_y_values = Int32Array::from(vec![10, 20, 30]);
        let inner_y_offsets = OffsetBuffer::new(vec![0_i32, 2, 3].into());
        let inner_y_list = ListArray::new(
            Arc::new(Field::new("item", ArrowDataType::Int32, true)),
            inner_y_offsets,
            Arc::new(inner_y_values),
            None,
        );
        let row_fields = Fields::from(vec![
            Field::new("x", ArrowDataType::Int32, true),
            Field::new(
                "y",
                ArrowDataType::List(Arc::new(Field::new("item", ArrowDataType::Int32, true))),
                true,
            ),
        ]);
        let struct_arr = StructArray::new(
            row_fields.clone(),
            vec![
                Arc::new(inner_x) as ArrayRef,
                Arc::new(inner_y_list) as ArrayRef,
            ],
            None,
        );
        let outer_offsets = OffsetBuffer::new(vec![0_i32, 2].into());
        let outer_list = ListArray::new(
            Arc::new(Field::new("item", ArrowDataType::Struct(row_fields), true)),
            outer_offsets,
            Arc::new(struct_arr),
            None,
        );
        let rb = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "arr",
                outer_list.data_type().clone(),
                true,
            )])),
            vec![Arc::new(outer_list) as ArrayRef],
        )
        .unwrap();

        let inner_row_type = DataTypes::row(vec![
            DataField::new("x", DataTypes::int(), None),
            DataField::new("y", DataTypes::array(DataTypes::int()), None),
        ]);
        let outer_row_type = Arc::new(RowType::with_data_types(vec![DataTypes::array(
            inner_row_type,
        )]));
        let row = ColumnarRow::new(Arc::new(rb), outer_row_type.clone(), 0, None).unwrap();

        let outer = row.get_array(0).unwrap().try_into_binary().unwrap();
        assert_eq!(outer.size(), 2);

        let inner_row_dt = DataTypes::row(vec![
            DataField::new("x", DataTypes::int(), None),
            DataField::new("y", DataTypes::array(DataTypes::int()), None),
        ]);
        let inner_row_type_unwrap = match &inner_row_dt {
            DataType::Row(rt) => rt.clone(),
            _ => unreachable!(),
        };
        let r0 = outer.get_row(0, &inner_row_type_unwrap).unwrap();
        assert_eq!(r0.get_int(0).unwrap(), 1);
        let r0_y = r0.get_array(1).unwrap().try_into_binary().unwrap();
        assert_eq!(r0_y.size(), 2);
        assert_eq!(r0_y.get_int(0).unwrap(), 10);
        assert_eq!(r0_y.get_int(1).unwrap(), 20);

        let r1 = outer.get_row(1, &inner_row_type_unwrap).unwrap();
        assert_eq!(r1.get_int(0).unwrap(), 2);
        let r1_y = r1.get_array(1).unwrap().try_into_binary().unwrap();
        assert_eq!(r1_y.size(), 1);
        assert_eq!(r1_y.get_int(0).unwrap(), 30);

        // FieldGetter is the surface every binding's row→Datum converter hits.
        let getter = FieldGetter::create(&DataTypes::array(inner_row_dt), 0);
        let datum = getter.get_field(&row).unwrap();
        match datum {
            Datum::Array(a) => assert_eq!(a.size(), 2),
            other => panic!("expected Datum::Array, got {other:?}"),
        }
    }
}
