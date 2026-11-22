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

//! Arrow scan-path cursor over a typed column-vector batch. Accessors are a
//! single match arm with no per-call downcasting. Nested `ARRAY` / `MAP` /
//! `ROW` reads return columnar slice/cursor views borrowed from the buffers.

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataField, RowType};
use crate::record::from_arrow_field;
use crate::row::column_vector::{TypedBatch, TypedColumn};
use crate::row::columnar::{ColumnarArray, ColumnarMap};
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
use crate::row::view::{ArrayView, MapView, RowView};
use crate::row::{DataGetters, Decimal, InternalRow};
use arrow::array::{Array, RecordBatch};
use std::sync::Arc;

/// Cursor over one row of a typed Arrow column-vector batch. Cheap to clone;
/// advance via [`set_row_id`](Self::set_row_id).
#[derive(Debug, Clone)]
pub struct ColumnarRow {
    batch: Arc<TypedBatch>,
    row_id: usize,
}

impl ColumnarRow {
    /// Primary constructor — caller already has a typed batch.
    pub(crate) fn from_typed_batch(batch: Arc<TypedBatch>, row_id: usize) -> Self {
        Self { batch, row_id }
    }

    /// Builds the typed batch in-line from a `RecordBatch` plus [`RowType`].
    /// `fluss_row_type`, when provided, overrides `row_type`. If both are
    /// empty, the Fluss types are inferred from the Arrow schema.
    pub fn new(
        record_batch: Arc<RecordBatch>,
        row_type: Arc<RowType>,
        row_id: usize,
        fluss_row_type: Option<Arc<RowType>>,
    ) -> Result<Self> {
        let schema = pick_schema(&record_batch, &row_type, fluss_row_type.as_deref())?;
        let typed = TypedBatch::build(&record_batch, &schema)?;
        Ok(Self::from_typed_batch(Arc::new(typed), row_id))
    }

    pub fn set_row_id(&mut self, row_id: usize) {
        self.row_id = row_id;
    }

    pub fn get_row_id(&self) -> usize {
        self.row_id
    }

    /// Returns the source `RecordBatch`, or `None` for a nested cursor
    /// (from [`DataGetters::get_row`]), which has no batch of its own.
    pub fn get_record_batch(&self) -> Option<&RecordBatch> {
        self.batch.record_batch.as_ref()
    }

    /// Resolves the column at `pos`, bounds-checking `row_id` first — the
    /// single guard shared by every accessor.
    fn column(&self, pos: usize) -> Result<&TypedColumn> {
        if self.row_id >= self.batch.num_rows {
            return Err(IllegalArgument {
                message: format!(
                    "row index {} out of bounds (batch has {} rows)",
                    self.row_id, self.batch.num_rows
                ),
            });
        }
        self.batch.columns.get(pos).ok_or_else(|| IllegalArgument {
            message: format!(
                "column index {pos} out of bounds (batch has {} columns)",
                self.batch.columns.len()
            ),
        })
    }
}

fn pick_schema(
    record_batch: &RecordBatch,
    row_type: &Arc<RowType>,
    fluss_row_type: Option<&RowType>,
) -> Result<Arc<RowType>> {
    if let Some(fluss) = fluss_row_type {
        return Ok(Arc::new(fluss.clone()));
    }
    if !row_type.fields().is_empty() {
        return Ok(row_type.clone());
    }
    let mut fields = Vec::with_capacity(record_batch.num_columns());
    for arrow_field in record_batch.schema().fields() {
        let dt = from_arrow_field(arrow_field)?;
        fields.push(DataField::new(arrow_field.name(), dt, None));
    }
    Ok(Arc::new(RowType::new(fields)))
}

impl InternalRow for ColumnarRow {
    fn get_field_count(&self) -> usize {
        self.batch.columns.len()
    }
}

impl DataGetters for ColumnarRow {
    fn is_null_at(&self, pos: usize) -> Result<bool> {
        Ok(self.column(pos)?.is_null_at(self.row_id))
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        self.column(pos)?.get_boolean(self.row_id)
    }
    fn get_byte(&self, pos: usize) -> Result<i8> {
        self.column(pos)?.get_byte(self.row_id)
    }
    fn get_short(&self, pos: usize) -> Result<i16> {
        self.column(pos)?.get_short(self.row_id)
    }
    fn get_int(&self, pos: usize) -> Result<i32> {
        self.column(pos)?.get_int(self.row_id)
    }
    fn get_long(&self, pos: usize) -> Result<i64> {
        self.column(pos)?.get_long(self.row_id)
    }
    fn get_float(&self, pos: usize) -> Result<f32> {
        self.column(pos)?.get_float(self.row_id)
    }
    fn get_double(&self, pos: usize) -> Result<f64> {
        self.column(pos)?.get_double(self.row_id)
    }

    fn get_char(&self, pos: usize, _length: usize) -> Result<&str> {
        self.column(pos)?.get_char(self.row_id)
    }
    fn get_string(&self, pos: usize) -> Result<&str> {
        self.column(pos)?.get_string(self.row_id)
    }

    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        self.column(pos)?
            .get_decimal(self.row_id, precision as u32, scale as u32)
    }

    fn get_date(&self, pos: usize) -> Result<Date> {
        self.column(pos)?.get_date(self.row_id)
    }
    fn get_time(&self, pos: usize) -> Result<Time> {
        self.column(pos)?.get_time(self.row_id)
    }
    fn get_timestamp_ntz(&self, pos: usize, _precision: u32) -> Result<TimestampNtz> {
        self.column(pos)?.get_timestamp_ntz(self.row_id)
    }
    fn get_timestamp_ltz(&self, pos: usize, _precision: u32) -> Result<TimestampLtz> {
        self.column(pos)?.get_timestamp_ltz(self.row_id)
    }

    fn get_binary(&self, pos: usize, _length: usize) -> Result<&[u8]> {
        self.column(pos)?.get_binary(self.row_id)
    }
    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        self.column(pos)?.get_bytes(self.row_id)
    }

    fn get_array(&self, pos: usize) -> Result<ArrayView<'_>> {
        let column = self.column(pos)?;
        match column {
            TypedColumn::Array(list_arr, element) => {
                let row_id = self.row_id;
                let start = list_arr.value_offsets()[row_id] as usize;
                let end = list_arr.value_offsets()[row_id + 1] as usize;
                Ok(ArrayView::Columnar(ColumnarArray::new(
                    element.as_ref(),
                    start,
                    end - start,
                )))
            }
            other => Err(IllegalArgument {
                message: format!("expected ARRAY column at position {pos}, got {other:?}"),
            }),
        }
    }

    fn get_map(&self, pos: usize) -> Result<MapView<'_>> {
        let column = self.column(pos)?;
        match column {
            TypedColumn::Map(map_arr, keys, values) => {
                let row_id = self.row_id;
                let start = map_arr.value_offsets()[row_id] as usize;
                let end = map_arr.value_offsets()[row_id + 1] as usize;
                Ok(MapView::Columnar(ColumnarMap::new(
                    keys.as_ref(),
                    values.as_ref(),
                    start,
                    end - start,
                )))
            }
            other => Err(IllegalArgument {
                message: format!("expected MAP column at position {pos}, got {other:?}"),
            }),
        }
    }

    fn get_row(&self, pos: usize) -> Result<RowView<'_>> {
        let column = self.column(pos)?;
        match column {
            TypedColumn::Row(struct_arr, inner_batch) => {
                if struct_arr.is_null(self.row_id) {
                    return Err(IllegalArgument {
                        message: format!(
                            "get_row called on null ROW cell at position {pos}, row {}; \
                             check is_null_at({pos}) first",
                            self.row_id
                        ),
                    });
                }
                Ok(RowView::Columnar(Self {
                    batch: Arc::clone(inner_batch),
                    row_id: self.row_id,
                }))
            }
            other => Err(IllegalArgument {
                message: format!("expected ROW column at position {pos}, got {other:?}"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{ArrayType, DataField, DataType, DataTypes, IntType, RowType};
    use crate::row::{InternalArray, InternalMap};
    use arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int32Builder, Int64Array, ListBuilder, MapBuilder,
        StringArray, StringBuilder, StructArray, UInt32Builder,
    };
    use arrow::datatypes::{DataType as ArrowDataType, Field, Fields, Schema};
    use bigdecimal::BigDecimal;
    use bigdecimal::num_bigint::BigInt;

    fn infer_fluss_type(arrow_dt: &ArrowDataType) -> DataType {
        match arrow_dt {
            ArrowDataType::Int32 => DataType::Int(IntType::new()),
            ArrowDataType::List(f) => {
                DataType::Array(ArrayType::new(infer_fluss_type(f.data_type())))
            }
            _ => DataType::Int(IntType::new()),
        }
    }

    fn single_column_row(array: ArrayRef) -> ColumnarRow {
        let dt = infer_fluss_type(array.data_type());
        let batch =
            RecordBatch::try_from_iter(vec![("arr", array)]).expect("record batch with one column");
        let row_type = Arc::new(RowType::with_data_types(vec![dt]));
        ColumnarRow::new(Arc::new(batch), row_type, 0, None).expect("ColumnarRow")
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

        let mut row = ColumnarRow::new(Arc::new(batch), Arc::new(RowType::new(vec![])), 0, None)
            .expect("ColumnarRow");
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("dec1", ArrowDataType::Decimal128(10, 2), false),
            Field::new("dec2", ArrowDataType::Decimal128(20, 5), false),
            Field::new("dec3", ArrowDataType::Decimal128(38, 10), false),
        ]));

        let dec1_val = 12345i128;
        let dec2_val = 1234567890i128;
        let dec3_val = 999999999999999999i128;

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

        let row = ColumnarRow::new(Arc::new(batch), Arc::new(RowType::new(vec![])), 0, None)
            .expect("ColumnarRow");
        assert_eq!(row.get_field_count(), 3);

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
        assert!(arr.is_null_at(1).unwrap());
        assert_eq!(arr.get_int(2).unwrap(), 3);
    }

    #[test]
    fn columnar_row_get_array_nested_array() {
        let mut outer = ListBuilder::new(ListBuilder::new(Int32Builder::new()));

        outer.values().values().append_value(1);
        outer.values().values().append_value(2);
        outer.values().append(true);

        outer.values().values().append_value(99);
        outer.values().append(true);

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
            err.to_string().contains("expected ARRAY column"),
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
        let row_type = Arc::new(RowType::new(vec![DataField::new(
            "arr",
            DataTypes::array(DataTypes::int()),
            None,
        )]));
        let err = ColumnarRow::new(Arc::new(batch), row_type, 0, None).unwrap_err();
        assert!(
            err.to_string().contains("expected Int32Array"),
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
            ArrowDataType::Struct(child_fields),
            false,
        )]));
        Arc::new(RecordBatch::try_new(schema, vec![Arc::new(struct_array)]).expect("record batch"))
    }

    #[test]
    fn columnar_row_reads_nested_row() {
        let child_fields = Fields::from(vec![
            Field::new("x", ArrowDataType::Int32, false),
            Field::new("s", ArrowDataType::Utf8, false),
        ]);
        let child_arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int32Array::from(vec![42, 99])),
            Arc::new(StringArray::from(vec!["hello", "world"])),
        ];
        let batch = make_struct_batch("nested", child_fields, child_arrays, 2);

        let mut row =
            ColumnarRow::new(batch, Arc::new(RowType::new(vec![])), 0, None).expect("ColumnarRow");

        let nested = row.get_row(0).unwrap();
        assert_eq!(nested.get_field_count(), 2);
        assert_eq!(nested.get_int(0).unwrap(), 42);
        assert_eq!(nested.get_string(1).unwrap(), "hello");

        row.set_row_id(1);
        let nested = row.get_row(0).unwrap();
        assert_eq!(nested.get_int(0).unwrap(), 99);
        assert_eq!(nested.get_string(1).unwrap(), "world");
    }

    #[test]
    fn columnar_row_reads_deeply_nested_row() {
        let inner_fields = Fields::from(vec![Field::new("s", ArrowDataType::Utf8, false)]);
        let inner_array = Arc::new(StructArray::new(
            inner_fields.clone(),
            vec![Arc::new(StringArray::from(vec!["deep", "deeper"])) as Arc<dyn Array>],
            None,
        ));

        let outer_fields = Fields::from(vec![
            Field::new("n", ArrowDataType::Int32, false),
            Field::new("inner", ArrowDataType::Struct(inner_fields), false),
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
            ArrowDataType::Struct(outer_fields),
            false,
        )]));
        let batch =
            Arc::new(RecordBatch::try_new(schema, vec![outer_array]).expect("record batch"));

        let mut row =
            ColumnarRow::new(batch, Arc::new(RowType::new(vec![])), 0, None).expect("ColumnarRow");

        let outer = row.get_row(0).unwrap();
        assert_eq!(outer.get_int(0).unwrap(), 1);
        let inner = outer.get_row(1).unwrap();
        assert_eq!(inner.get_string(0).unwrap(), "deep");

        row.set_row_id(1);
        let outer = row.get_row(0).unwrap();
        assert_eq!(outer.get_int(0).unwrap(), 2);
        let inner = outer.get_row(1).unwrap();
        assert_eq!(inner.get_string(0).unwrap(), "deeper");
    }

    #[test]
    fn columnar_row_nested_row_follows_outer_row_id() {
        let child_fields = Fields::from(vec![Field::new("x", ArrowDataType::Int32, false)]);
        let child_arrays: Vec<Arc<dyn Array>> = vec![Arc::new(Int32Array::from(vec![10, 20]))];
        let batch = make_struct_batch("s", child_fields, child_arrays, 2);

        let mut row =
            ColumnarRow::new(batch, Arc::new(RowType::new(vec![])), 0, None).expect("ColumnarRow");

        let nested_0 = row.get_row(0).unwrap();
        assert_eq!(nested_0.get_int(0).unwrap(), 10);

        row.set_row_id(1);
        let nested_1 = row.get_row(0).unwrap();
        assert_eq!(nested_1.get_int(0).unwrap(), 20);
    }

    #[test]
    fn columnar_row_get_map_accepts_non_nullable_key_from_map_type() {
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
        let row = ColumnarRow::new(batch, row_type, 0, None).expect("ColumnarRow");

        let fluss_map = row
            .get_map(0)
            .expect("get_map should succeed on ColumnarRow");
        assert_eq!(fluss_map.size(), 1);
        assert_eq!(fluss_map.key_array().get_int(0).unwrap(), 1);
        assert_eq!(fluss_map.value_array().get_string(0).unwrap(), "a");
    }

    #[test]
    fn columnar_row_reads_row_containing_map() {
        let mut mb = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        mb.keys().append_value("k1");
        mb.values().append_value(42);
        mb.append(true).unwrap();
        mb.keys().append_value("k2");
        mb.values().append_value(7);
        mb.append(true).unwrap();
        let map_arr = mb.finish();

        let id_arr = Int32Array::from(vec![10, 20]);
        let struct_fields = Fields::from(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("m", map_arr.data_type().clone(), false),
        ]);
        let struct_arr = Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(id_arr), Arc::new(map_arr)],
            None,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "outer",
            ArrowDataType::Struct(struct_fields),
            false,
        )]));
        let batch = Arc::new(RecordBatch::try_new(schema, vec![struct_arr]).expect("record batch"));

        let inner_row_type = RowType::with_data_types(vec![
            DataTypes::int(),
            DataTypes::map(DataTypes::string(), DataTypes::int()),
        ]);
        let outer_row_type = Arc::new(RowType::with_data_types(vec![DataType::Row(
            inner_row_type,
        )]));

        let mut row = ColumnarRow::new(
            batch,
            outer_row_type.clone(),
            0,
            Some(outer_row_type.clone()),
        )
        .expect("ColumnarRow");

        let nested = row
            .get_row(0)
            .expect("reading row with Map field must succeed");
        assert_eq!(nested.get_int(0).unwrap(), 10);
        let inner_map = nested.get_map(1).expect("nested map should be accessible");
        assert_eq!(inner_map.size(), 1);
        assert_eq!(inner_map.key_array().get_string(0).unwrap(), "k1");
        assert_eq!(inner_map.value_array().get_int(0).unwrap(), 42);

        row.set_row_id(1);
        let nested = row.get_row(0).expect("row 1 must read");
        assert_eq!(nested.get_int(0).unwrap(), 20);
        let inner_map = nested.get_map(1).unwrap();
        assert_eq!(inner_map.key_array().get_string(0).unwrap(), "k2");
        assert_eq!(inner_map.value_array().get_int(0).unwrap(), 7);
    }

    #[test]
    fn columnar_row_reads_array_of_maps() {
        // One row whose ARRAY<MAP<STRING, INT>> contains two maps:
        // [{"k1" -> 1}, {"k2" -> 2, "k3" -> 3}].
        let mut outer = ListBuilder::new(MapBuilder::new(
            None,
            StringBuilder::new(),
            Int32Builder::new(),
        ));
        {
            let mb = outer.values();
            mb.keys().append_value("k1");
            mb.values().append_value(1);
            mb.append(true).unwrap();
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
        let row = ColumnarRow::new(batch, row_type, 0, None).expect("ColumnarRow");

        let arr = row.get_array(0).expect("get_array on ARRAY<MAP> must work");
        assert_eq!(arr.size(), 2);

        let m0 = arr.get_map(0).expect("nested map 0");
        assert_eq!(m0.size(), 1);
        assert_eq!(m0.key_array().get_string(0).unwrap(), "k1");
        assert_eq!(m0.value_array().get_int(0).unwrap(), 1);

        let m1 = arr.get_map(1).expect("nested map 1");
        assert_eq!(m1.size(), 2);
        assert_eq!(m1.key_array().get_string(0).unwrap(), "k2");
        assert_eq!(m1.value_array().get_int(0).unwrap(), 2);
        assert_eq!(m1.key_array().get_string(1).unwrap(), "k3");
        assert_eq!(m1.value_array().get_int(1).unwrap(), 3);
    }

    #[test]
    fn columnar_row_get_map_rejects_real_type_mismatch() {
        let mut mb = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        mb.keys().append_value("k");
        mb.values().append_value(1);
        mb.append(true).unwrap();
        let map_arr = mb.finish();
        let map_arrow_type = map_arr.data_type().clone();

        let schema = Arc::new(Schema::new(vec![Field::new("m", map_arrow_type, true)]));
        let batch =
            Arc::new(RecordBatch::try_new(schema, vec![Arc::new(map_arr)]).expect("record batch"));

        let row_type = Arc::new(RowType::with_data_types(vec![DataTypes::map(
            DataTypes::string(),
            DataTypes::string(),
        )]));
        let err = ColumnarRow::new(batch, row_type, 0, None).expect_err("type mismatch must error");
        let msg = err.to_string();
        assert!(
            msg.contains("expected StringArray"),
            "unexpected error: {msg}"
        );
    }
}
