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

//! Zero-copy slice views over a typed Arrow batch. An `ARRAY` cell is
//! `(element column, start, length)`; a `MAP` is two parallel slices over
//! the typed key/value vectors.

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::row::Decimal;
use crate::row::column::ColumnarRow;
use crate::row::column_vector::TypedColumn;
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
use crate::row::view::{ArrayView, MapView, RowView};
use crate::row::{DataGetters, InternalArray, InternalMap};
use std::sync::Arc;

/// A slice of an element `TypedColumn` representing one `ARRAY` cell.
#[derive(Debug, Clone, Copy)]
pub struct ColumnarArray<'a> {
    element: &'a TypedColumn,
    start: usize,
    length: usize,
}

impl<'a> ColumnarArray<'a> {
    pub(crate) fn new(element: &'a TypedColumn, start: usize, length: usize) -> Self {
        Self {
            element,
            start,
            length,
        }
    }

    /// The element-level `TypedColumn` this slice borrows from.
    pub(crate) fn element(&self) -> &'a TypedColumn {
        self.element
    }

    fn absolute_index(&self, pos: usize) -> Result<usize> {
        if pos >= self.length {
            return Err(IllegalArgument {
                message: format!(
                    "columnar array index out of bounds: pos={pos}, length={}",
                    self.length
                ),
            });
        }
        Ok(self.start + pos)
    }
}

impl InternalArray for ColumnarArray<'_> {
    fn size(&self) -> usize {
        self.length
    }
}

impl DataGetters for ColumnarArray<'_> {
    fn is_null_at(&self, pos: usize) -> Result<bool> {
        let abs = self.absolute_index(pos)?;
        Ok(self.element.is_null_at(abs))
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        self.element.get_boolean(self.absolute_index(pos)?)
    }
    fn get_byte(&self, pos: usize) -> Result<i8> {
        self.element.get_byte(self.absolute_index(pos)?)
    }
    fn get_short(&self, pos: usize) -> Result<i16> {
        self.element.get_short(self.absolute_index(pos)?)
    }
    fn get_int(&self, pos: usize) -> Result<i32> {
        self.element.get_int(self.absolute_index(pos)?)
    }
    fn get_long(&self, pos: usize) -> Result<i64> {
        self.element.get_long(self.absolute_index(pos)?)
    }
    fn get_float(&self, pos: usize) -> Result<f32> {
        self.element.get_float(self.absolute_index(pos)?)
    }
    fn get_double(&self, pos: usize) -> Result<f64> {
        self.element.get_double(self.absolute_index(pos)?)
    }

    fn get_char(&self, pos: usize, _length: usize) -> Result<&str> {
        self.element.get_char(self.absolute_index(pos)?)
    }
    fn get_string(&self, pos: usize) -> Result<&str> {
        self.element.get_string(self.absolute_index(pos)?)
    }

    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        self.element
            .get_decimal(self.absolute_index(pos)?, precision as u32, scale as u32)
    }

    fn get_date(&self, pos: usize) -> Result<Date> {
        self.element.get_date(self.absolute_index(pos)?)
    }
    fn get_time(&self, pos: usize) -> Result<Time> {
        self.element.get_time(self.absolute_index(pos)?)
    }
    fn get_timestamp_ntz(&self, pos: usize, _precision: u32) -> Result<TimestampNtz> {
        self.element.get_timestamp_ntz(self.absolute_index(pos)?)
    }
    fn get_timestamp_ltz(&self, pos: usize, _precision: u32) -> Result<TimestampLtz> {
        self.element.get_timestamp_ltz(self.absolute_index(pos)?)
    }

    fn get_binary(&self, pos: usize, _length: usize) -> Result<&[u8]> {
        self.element.get_binary(self.absolute_index(pos)?)
    }
    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        self.element.get_bytes(self.absolute_index(pos)?)
    }

    fn get_array(&self, pos: usize) -> Result<ArrayView<'_>> {
        let abs = self.absolute_index(pos)?;
        match self.element {
            TypedColumn::Array(list_arr, inner) => {
                let start = list_arr.value_offsets()[abs] as usize;
                let end = list_arr.value_offsets()[abs + 1] as usize;
                Ok(ArrayView::Columnar(ColumnarArray::new(
                    inner.as_ref(),
                    start,
                    end - start,
                )))
            }
            other => Err(IllegalArgument {
                message: format!("expected ARRAY element at position {pos}, got {other:?}"),
            }),
        }
    }

    fn get_map(&self, pos: usize) -> Result<MapView<'_>> {
        let abs = self.absolute_index(pos)?;
        match self.element {
            TypedColumn::Map(map_arr, keys, values) => {
                let start = map_arr.value_offsets()[abs] as usize;
                let end = map_arr.value_offsets()[abs + 1] as usize;
                Ok(MapView::Columnar(ColumnarMap::new(
                    keys.as_ref(),
                    values.as_ref(),
                    start,
                    end - start,
                )))
            }
            other => Err(IllegalArgument {
                message: format!("expected MAP element at position {pos}, got {other:?}"),
            }),
        }
    }

    fn get_row(&self, pos: usize) -> Result<RowView<'_>> {
        let abs = self.absolute_index(pos)?;
        match self.element {
            TypedColumn::Row(_, inner_batch) => Ok(RowView::Columnar(
                ColumnarRow::from_typed_batch(Arc::clone(inner_batch), abs),
            )),
            other => Err(IllegalArgument {
                message: format!("expected ROW element at position {pos}, got {other:?}"),
            }),
        }
    }
}

/// Two parallel slices over the typed key/value vectors. Map keys are
/// non-nullable per Fluss spec.
#[derive(Debug, Clone, Copy)]
pub struct ColumnarMap<'a> {
    keys: ColumnarArray<'a>,
    values: ColumnarArray<'a>,
}

impl<'a> ColumnarMap<'a> {
    pub(crate) fn new(
        keys_column: &'a TypedColumn,
        values_column: &'a TypedColumn,
        start: usize,
        length: usize,
    ) -> Self {
        Self {
            keys: ColumnarArray::new(keys_column, start, length),
            values: ColumnarArray::new(values_column, start, length),
        }
    }

    pub(crate) fn keys_slice(&self) -> &ColumnarArray<'a> {
        &self.keys
    }

    pub(crate) fn values_slice(&self) -> &ColumnarArray<'a> {
        &self.values
    }
}

impl InternalMap for ColumnarMap<'_> {
    fn size(&self) -> usize {
        self.keys.size()
    }

    fn key_array(&self) -> &dyn InternalArray {
        &self.keys
    }

    fn value_array(&self) -> &dyn InternalArray {
        &self.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes, RowType};
    use crate::row::column_vector::TypedBatch;
    use arrow::array::{
        Array, ArrayRef, Int32Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema};
    use std::sync::Arc;

    fn list_int_batch() -> (ListArray, Arc<TypedBatch>) {
        // ARRAY<INT>: two rows — [10,20], [30,40,50]
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
        .expect("batch");
        let rt = RowType::new(vec![DataField::new(
            "arr",
            DataTypes::array(DataTypes::int()),
            None,
        )]);
        (list, Arc::new(TypedBatch::build(&rb, &rt).expect("build")))
    }

    #[test]
    fn columnar_array_reads_a_slice_of_the_element_vector() {
        let (list, tb) = list_int_batch();

        let element = match &tb.columns[0] {
            TypedColumn::Array(_, e) => e.as_ref(),
            _ => unreachable!(),
        };

        // Row 1 — slice [30, 40, 50] of the flat element vector.
        let start = list.value_offsets()[1] as usize;
        let end = list.value_offsets()[2] as usize;
        let arr = ColumnarArray::new(element, start, end - start);

        assert_eq!(arr.size(), 3);
        assert!(!arr.is_null_at(0).unwrap());
        assert_eq!(arr.get_int(0).unwrap(), 30);
        assert_eq!(arr.get_int(1).unwrap(), 40);
        assert_eq!(arr.get_int(2).unwrap(), 50);
    }

    #[test]
    fn columnar_array_out_of_bounds_is_clear_error() {
        let (list, tb) = list_int_batch();
        let element = match &tb.columns[0] {
            TypedColumn::Array(_, e) => e.as_ref(),
            _ => unreachable!(),
        };
        let start = list.value_offsets()[0] as usize;
        let end = list.value_offsets()[1] as usize;
        let arr = ColumnarArray::new(element, start, end - start);

        let err = arr.get_int(5).unwrap_err().to_string();
        assert!(err.contains("out of bounds"), "{err}");
    }

    fn map_string_int_batch() -> (MapArray, Arc<TypedBatch>) {
        // MAP<STRING, INT>: one row — {"a": 1, "b": 2}
        let keys = StringArray::from(vec!["a", "b"]);
        let values = Int32Array::from(vec![1, 2]);
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
        .expect("batch");
        let rt = RowType::new(vec![DataField::new(
            "m",
            DataTypes::map(DataTypes::string(), DataTypes::int()),
            None,
        )]);
        (map, Arc::new(TypedBatch::build(&rb, &rt).expect("build")))
    }

    #[test]
    fn columnar_map_exposes_key_and_value_slices() {
        let (map, tb) = map_string_int_batch();
        let (key_col, value_col) = match &tb.columns[0] {
            TypedColumn::Map(_, k, v) => (k.as_ref(), v.as_ref()),
            _ => unreachable!(),
        };
        let start = map.value_offsets()[0] as usize;
        let end = map.value_offsets()[1] as usize;
        let cm = ColumnarMap::new(key_col, value_col, start, end - start);

        assert_eq!(cm.size(), 2);
        let keys = cm.key_array();
        let values = cm.value_array();
        assert_eq!(keys.size(), 2);
        assert_eq!(keys.get_string(0).unwrap(), "a");
        assert_eq!(keys.get_string(1).unwrap(), "b");
        assert_eq!(values.get_int(0).unwrap(), 1);
        assert_eq!(values.get_int(1).unwrap(), 2);
    }
}
