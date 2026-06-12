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

//! Typed column writers that write directly from [`InternalRow`] to concrete
//! Arrow builders, bypassing the intermediate [`Datum`] enum and runtime
//! `downcast_mut` dispatch.

use crate::error::Error::RowConvertError;
use crate::error::{Error, Result};
use crate::metadata::{DataType, RowType};
use crate::row::datum::{
    MICROS_PER_MILLI, MILLIS_PER_SECOND, NANOS_PER_MILLI, append_decimal_to_builder,
    millis_nanos_to_micros, millis_nanos_to_nanos,
};
use crate::row::{FlussArray, FlussMap, InternalRow};
use arrow::array::{
    ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, StringBuilder, Time32MillisecondBuilder, Time32SecondBuilder,
    Time64MicrosecondBuilder, Time64NanosecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
};
use arrow_schema::DataType as ArrowDataType;

/// Round up to the next multiple of 8 (Arrow IPC buffer alignment).
#[inline]
pub(crate) fn round_up_to_8(n: usize) -> usize {
    (n + 7) & !7
}

/// Estimated average byte size for variable-width columns (Utf8, Binary).
/// Used to pre-allocate data buffers and avoid reallocations during batch building.
/// Matches Java Arrow's `BaseVariableWidthVector.DEFAULT_RECORD_BYTE_COUNT`.
const VARIABLE_WIDTH_AVG_BYTES: usize = 8;

/// A typed column writer that reads one column from an [`InternalRow`] and
/// appends directly to a concrete Arrow builder — no intermediate [`Datum`],
/// no `as_any_mut().downcast_mut()`.
pub struct ColumnWriter {
    pos: usize,
    nullable: bool,
    inner: TypedWriter,
}

enum TypedWriter {
    Bool(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Char {
        len: usize,
        builder: StringBuilder,
    },
    String(StringBuilder),
    Bytes(BinaryBuilder),
    Binary {
        len: usize,
        builder: FixedSizeBinaryBuilder,
    },
    Decimal128 {
        src_precision: usize,
        src_scale: usize,
        target_precision: u32,
        target_scale: i64,
        builder: Decimal128Builder,
    },
    Date32(Date32Builder),
    Time32Second(Time32SecondBuilder),
    Time32Millisecond(Time32MillisecondBuilder),
    Time64Microsecond(Time64MicrosecondBuilder),
    Time64Nanosecond(Time64NanosecondBuilder),
    TimestampNtzSecond {
        precision: u32,
        builder: TimestampSecondBuilder,
    },
    TimestampNtzMillisecond {
        precision: u32,
        builder: TimestampMillisecondBuilder,
    },
    TimestampNtzMicrosecond {
        precision: u32,
        builder: TimestampMicrosecondBuilder,
    },
    TimestampNtzNanosecond {
        precision: u32,
        builder: TimestampNanosecondBuilder,
    },
    TimestampLtzSecond {
        precision: u32,
        builder: TimestampSecondBuilder,
    },
    TimestampLtzMillisecond {
        precision: u32,
        builder: TimestampMillisecondBuilder,
    },
    TimestampLtzMicrosecond {
        precision: u32,
        builder: TimestampMicrosecondBuilder,
    },
    TimestampLtzNanosecond {
        precision: u32,
        builder: TimestampNanosecondBuilder,
    },
    List {
        element_writer: Box<ColumnWriter>,
        offsets: Vec<i32>,
        validity: Vec<bool>,
    },
    Map {
        key_writer: Box<ColumnWriter>,
        value_writer: Box<ColumnWriter>,
        key_type: DataType,
        value_type: DataType,
        offsets: Vec<i32>,
        validity: Vec<bool>,
    },
    Struct {
        field_writers: Vec<ColumnWriter>,
        validity: Vec<bool>,
        fields: arrow_schema::Fields,
        row_type: RowType,
    },
}

/// Dispatch to the inner builder across all `TypedWriter` variants.
/// Exhaustive matching ensures new variants won't compile without an arm.
macro_rules! with_builder {
    ($self:expr, $b:ident => $body:expr) => {
        match $self {
            TypedWriter::Bool($b) => $body,
            TypedWriter::Int8($b) => $body,
            TypedWriter::Int16($b) => $body,
            TypedWriter::Int32($b) => $body,
            TypedWriter::Int64($b) => $body,
            TypedWriter::Float32($b) => $body,
            TypedWriter::Float64($b) => $body,
            TypedWriter::Char { builder: $b, .. } => $body,
            TypedWriter::String($b) => $body,
            TypedWriter::Bytes($b) => $body,
            TypedWriter::Binary { builder: $b, .. } => $body,
            TypedWriter::Decimal128 { builder: $b, .. } => $body,
            TypedWriter::Date32($b) => $body,
            TypedWriter::Time32Second($b) => $body,
            TypedWriter::Time32Millisecond($b) => $body,
            TypedWriter::Time64Microsecond($b) => $body,
            TypedWriter::Time64Nanosecond($b) => $body,
            TypedWriter::TimestampNtzSecond { builder: $b, .. } => $body,
            TypedWriter::TimestampNtzMillisecond { builder: $b, .. } => $body,
            TypedWriter::TimestampNtzMicrosecond { builder: $b, .. } => $body,
            TypedWriter::TimestampNtzNanosecond { builder: $b, .. } => $body,
            TypedWriter::TimestampLtzSecond { builder: $b, .. } => $body,
            TypedWriter::TimestampLtzMillisecond { builder: $b, .. } => $body,
            TypedWriter::TimestampLtzMicrosecond { builder: $b, .. } => $body,
            TypedWriter::TimestampLtzNanosecond { builder: $b, .. } => $body,
            TypedWriter::List { .. } => panic!("List variant not supported in with_builder!"),
            TypedWriter::Map { .. } => panic!("Map variant not supported in with_builder!"),
            TypedWriter::Struct { .. } => panic!("Struct variant not supported in with_builder!"),
        }
    };
}

impl ColumnWriter {
    /// Create a column writer for the given Fluss `DataType` and Arrow
    /// `ArrowDataType` at position `pos` with the given pre-allocation
    /// `capacity`.
    pub fn create(
        fluss_type: &DataType,
        arrow_type: &ArrowDataType,
        pos: usize,
        capacity: usize,
    ) -> Result<Self> {
        let nullable = fluss_type.is_nullable();

        let inner = match fluss_type {
            DataType::Boolean(_) => TypedWriter::Bool(BooleanBuilder::with_capacity(capacity)),
            DataType::TinyInt(_) => TypedWriter::Int8(Int8Builder::with_capacity(capacity)),
            DataType::SmallInt(_) => TypedWriter::Int16(Int16Builder::with_capacity(capacity)),
            DataType::Int(_) => TypedWriter::Int32(Int32Builder::with_capacity(capacity)),
            DataType::BigInt(_) => TypedWriter::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float(_) => TypedWriter::Float32(Float32Builder::with_capacity(capacity)),
            DataType::Double(_) => TypedWriter::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Char(t) => TypedWriter::Char {
                len: t.length() as usize,
                builder: StringBuilder::with_capacity(
                    capacity,
                    capacity.saturating_mul(VARIABLE_WIDTH_AVG_BYTES),
                ),
            },
            DataType::String(_) => TypedWriter::String(StringBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(VARIABLE_WIDTH_AVG_BYTES),
            )),
            DataType::Bytes(_) => TypedWriter::Bytes(BinaryBuilder::with_capacity(
                capacity,
                capacity.saturating_mul(VARIABLE_WIDTH_AVG_BYTES),
            )),
            DataType::Binary(t) => {
                let arrow_len: i32 = t.length().try_into().map_err(|_| Error::IllegalArgument {
                    message: format!(
                        "Binary length {} exceeds Arrow's maximum (i32::MAX)",
                        t.length()
                    ),
                })?;
                TypedWriter::Binary {
                    len: t.length(),
                    builder: FixedSizeBinaryBuilder::with_capacity(capacity, arrow_len),
                }
            }
            DataType::Decimal(dt) => {
                let (target_p, target_s) = match arrow_type {
                    ArrowDataType::Decimal128(p, s) => (*p, *s),
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Expected Decimal128 Arrow type for Decimal, got: {arrow_type:?}"
                            ),
                        });
                    }
                };
                if target_s < 0 {
                    return Err(Error::IllegalArgument {
                        message: format!("Negative decimal scale {target_s} is not supported"),
                    });
                }
                let builder = Decimal128Builder::with_capacity(capacity)
                    .with_precision_and_scale(target_p, target_s)
                    .map_err(|e| Error::IllegalArgument {
                        message: format!(
                            "Invalid decimal precision {target_p} or scale {target_s}: {e}"
                        ),
                    })?;
                TypedWriter::Decimal128 {
                    src_precision: dt.precision() as usize,
                    src_scale: dt.scale() as usize,
                    target_precision: target_p as u32,
                    target_scale: target_s as i64,
                    builder,
                }
            }
            DataType::Date(_) => TypedWriter::Date32(Date32Builder::with_capacity(capacity)),
            DataType::Time(_) => match arrow_type {
                ArrowDataType::Time32(arrow_schema::TimeUnit::Second) => {
                    TypedWriter::Time32Second(Time32SecondBuilder::with_capacity(capacity))
                }
                ArrowDataType::Time32(arrow_schema::TimeUnit::Millisecond) => {
                    TypedWriter::Time32Millisecond(Time32MillisecondBuilder::with_capacity(
                        capacity,
                    ))
                }
                ArrowDataType::Time64(arrow_schema::TimeUnit::Microsecond) => {
                    TypedWriter::Time64Microsecond(Time64MicrosecondBuilder::with_capacity(
                        capacity,
                    ))
                }
                ArrowDataType::Time64(arrow_schema::TimeUnit::Nanosecond) => {
                    TypedWriter::Time64Nanosecond(Time64NanosecondBuilder::with_capacity(capacity))
                }
                _ => {
                    return Err(Error::IllegalArgument {
                        message: format!("Unsupported Arrow type for Time: {arrow_type:?}"),
                    });
                }
            },
            DataType::Timestamp(t) => {
                let precision = t.precision();
                match arrow_type {
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                        TypedWriter::TimestampNtzSecond {
                            precision,
                            builder: TimestampSecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                        TypedWriter::TimestampNtzMillisecond {
                            precision,
                            builder: TimestampMillisecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                        TypedWriter::TimestampNtzMicrosecond {
                            precision,
                            builder: TimestampMicrosecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                        TypedWriter::TimestampNtzNanosecond {
                            precision,
                            builder: TimestampNanosecondBuilder::with_capacity(capacity),
                        }
                    }
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Unsupported Arrow type for Timestamp: {arrow_type:?}"
                            ),
                        });
                    }
                }
            }
            DataType::TimestampLTz(t) => {
                let precision = t.precision();
                match arrow_type {
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Second, _) => {
                        TypedWriter::TimestampLtzSecond {
                            precision,
                            builder: TimestampSecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
                        TypedWriter::TimestampLtzMillisecond {
                            precision,
                            builder: TimestampMillisecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
                        TypedWriter::TimestampLtzMicrosecond {
                            precision,
                            builder: TimestampMicrosecondBuilder::with_capacity(capacity),
                        }
                    }
                    ArrowDataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                        TypedWriter::TimestampLtzNanosecond {
                            precision,
                            builder: TimestampNanosecondBuilder::with_capacity(capacity),
                        }
                    }
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Unsupported Arrow type for TimestampLTz: {arrow_type:?}"
                            ),
                        });
                    }
                }
            }
            DataType::Array(array_type) => {
                let element_type = array_type.get_element_type();
                let arrow_element_type = match arrow_type {
                    ArrowDataType::List(field) => field.data_type(),
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Expected List Arrow type for Array, got: {arrow_type:?}"
                            ),
                        });
                    }
                };
                let element_writer =
                    ColumnWriter::create(element_type, arrow_element_type, 0, capacity)?;
                TypedWriter::List {
                    element_writer: Box::new(element_writer),
                    offsets: vec![0],
                    validity: Vec::with_capacity(capacity),
                }
            }
            DataType::Map(m) => {
                let (key_arrow_type, value_arrow_type) = match arrow_type {
                    ArrowDataType::Map(field, _) => match field.data_type() {
                        ArrowDataType::Struct(fields) => {
                            if fields.len() != 2 {
                                return Err(Error::IllegalArgument {
                                    message: format!(
                                        "Expected Struct with 2 fields for Map, got {}",
                                        fields.len()
                                    ),
                                });
                            }
                            (fields[0].data_type().clone(), fields[1].data_type().clone())
                        }
                        struct_type => {
                            return Err(Error::IllegalArgument {
                                message: format!(
                                    "Expected Struct within Map Arrow type, got {:?}",
                                    struct_type
                                ),
                            });
                        }
                    },
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Expected Map Arrow type for Map, got: {arrow_type:?}"
                            ),
                        });
                    }
                };

                let key_writer = ColumnWriter::create(m.key_type(), &key_arrow_type, 0, capacity)?;
                let value_writer =
                    ColumnWriter::create(m.value_type(), &value_arrow_type, 1, capacity)?;
                TypedWriter::Map {
                    key_writer: Box::new(key_writer),
                    value_writer: Box::new(value_writer),
                    key_type: m.key_type().clone(),
                    value_type: m.value_type().clone(),
                    offsets: vec![0],
                    validity: Vec::with_capacity(capacity),
                }
            }
            DataType::Row(row_type) => {
                let arrow_fields = match arrow_type {
                    ArrowDataType::Struct(fields) => fields.clone(),
                    _ => {
                        return Err(Error::IllegalArgument {
                            message: format!(
                                "Expected Struct Arrow type for Row, got: {arrow_type:?}"
                            ),
                        });
                    }
                };
                if arrow_fields.len() != row_type.fields().len() {
                    return Err(Error::IllegalArgument {
                        message: format!(
                            "Row arity mismatch: Fluss type has {} fields, Arrow type has {}",
                            row_type.fields().len(),
                            arrow_fields.len(),
                        ),
                    });
                }
                let field_writers: Result<Vec<_>> = row_type
                    .fields()
                    .iter()
                    .zip(arrow_fields.iter())
                    .map(|(f, af)| ColumnWriter::create(&f.data_type, af.data_type(), 0, capacity))
                    .collect();
                TypedWriter::Struct {
                    field_writers: field_writers?,
                    validity: Vec::with_capacity(capacity),
                    fields: arrow_fields,
                    row_type: row_type.clone(),
                }
            }
        };

        Ok(Self {
            pos,
            nullable,
            inner,
        })
    }

    /// Read one value from `row` at this writer's column position and append it
    /// directly to the concrete Arrow builder.
    #[inline]
    pub fn write_field(&mut self, row: &dyn InternalRow) -> Result<()> {
        self.write_field_at(row, self.pos)
    }

    /// Read one value from `row` at position `pos` and append it
    /// directly to the concrete Arrow builder.
    #[inline]
    pub fn write_field_at(&mut self, row: &dyn InternalRow, pos: usize) -> Result<()> {
        if self.nullable && row.is_null_at(pos)? {
            self.append_null();
            return Ok(());
        }
        self.write_non_null_at(row, pos)
    }

    /// Finish the builder, producing the final Arrow array.
    pub fn finish(&mut self) -> ArrayRef {
        match &mut self.inner {
            TypedWriter::List {
                element_writer,
                offsets,
                validity,
            } => {
                let item_nullable = element_writer.nullable;
                let values = element_writer.finish();
                let taken_offsets = std::mem::replace(offsets, vec![0]);
                let taken_validity = std::mem::take(validity);
                finish_list_array(values, item_nullable, &taken_offsets, &taken_validity)
            }
            TypedWriter::Map {
                key_writer,
                value_writer,
                offsets,
                validity,
                ..
            } => {
                let value_nullable = value_writer.nullable;
                let keys = key_writer.finish();
                let values = value_writer.finish();
                let taken_offsets = std::mem::replace(offsets, vec![0]);
                let taken_validity = std::mem::take(validity);
                finish_map_array(
                    keys,
                    values,
                    value_nullable,
                    &taken_offsets,
                    &taken_validity,
                )
            }
            TypedWriter::Struct {
                field_writers,
                validity,
                fields,
                ..
            } => {
                let taken_validity = std::mem::take(validity);
                let child_arrays: Vec<ArrayRef> =
                    field_writers.iter_mut().map(|w| w.finish()).collect();
                finish_struct_array(fields.clone(), child_arrays, &taken_validity)
            }
            _ => with_builder!(&mut self.inner, b => (b as &mut dyn ArrayBuilder).finish()),
        }
    }

    /// Returns the total buffer size in bytes, rounded up to 8-byte alignment
    /// per buffer. Reads buffer lengths directly from the builders — O(1), no
    /// allocation. Analogous to Java's `ArrowUtils.estimateArrowBodyLength()`
    /// which sums `buf.readableBytes()` with 8-byte rounding per buffer.
    /// The IPC framing overhead not captured here is accounted for separately
    /// by `estimate_arrow_ipc_overhead()`.
    pub fn buffer_size(&self) -> usize {
        /// Validity bitmap size, rounded to 8-byte alignment.
        /// When no nulls have been appended, the builder does not materialize
        /// the bitmap and the IPC body contributes 0 bytes for this buffer.
        #[inline]
        fn validity_size(slice: Option<&[u8]>) -> usize {
            round_up_to_8(slice.map_or(0, |s| s.len()))
        }

        /// Primitive builder: validity + values (values_slice returns &[T::Native]).
        macro_rules! primitive_size {
            ($b:expr) => {
                validity_size($b.validity_slice())
                    + round_up_to_8(std::mem::size_of_val($b.values_slice()))
            };
        }

        /// Variable-width builder: validity + offsets + values.
        macro_rules! var_width_size {
            ($b:expr) => {
                validity_size($b.validity_slice())
                    + round_up_to_8(std::mem::size_of_val($b.offsets_slice()))
                    + round_up_to_8($b.values_slice().len())
            };
        }

        match &self.inner {
            TypedWriter::Bool(b) => {
                validity_size(b.validity_slice()) + round_up_to_8(b.values_slice().len())
            }
            TypedWriter::Int8(b) => primitive_size!(b),
            TypedWriter::Int16(b) => primitive_size!(b),
            TypedWriter::Int32(b) => primitive_size!(b),
            TypedWriter::Int64(b) => primitive_size!(b),
            TypedWriter::Float32(b) => primitive_size!(b),
            TypedWriter::Float64(b) => primitive_size!(b),
            TypedWriter::Decimal128 { builder: b, .. } => primitive_size!(b),
            TypedWriter::Date32(b) => primitive_size!(b),
            TypedWriter::Time32Second(b) => primitive_size!(b),
            TypedWriter::Time32Millisecond(b) => primitive_size!(b),
            TypedWriter::Time64Microsecond(b) => primitive_size!(b),
            TypedWriter::Time64Nanosecond(b) => primitive_size!(b),
            TypedWriter::TimestampNtzSecond { builder: b, .. } => primitive_size!(b),
            TypedWriter::TimestampNtzMillisecond { builder: b, .. } => primitive_size!(b),
            TypedWriter::TimestampNtzMicrosecond { builder: b, .. } => primitive_size!(b),
            TypedWriter::TimestampNtzNanosecond { builder: b, .. } => primitive_size!(b),
            TypedWriter::TimestampLtzSecond { builder: b, .. } => primitive_size!(b),
            TypedWriter::TimestampLtzMillisecond { builder: b, .. } => primitive_size!(b),
            TypedWriter::TimestampLtzMicrosecond { builder: b, .. } => primitive_size!(b),
            TypedWriter::TimestampLtzNanosecond { builder: b, .. } => primitive_size!(b),
            // Variable-width types: validity + offsets + values
            TypedWriter::Char { builder: b, .. } => var_width_size!(b),
            TypedWriter::String(b) => var_width_size!(b),
            TypedWriter::Bytes(b) => var_width_size!(b),
            TypedWriter::Binary { builder: b, .. } => {
                validity_size(b.validity_slice()) + round_up_to_8(b.values_slice().len())
            }
            TypedWriter::List {
                element_writer,
                offsets,
                validity,
            } => {
                let validity_bytes = round_up_to_8(validity.len().div_ceil(8));
                let offsets_bytes = round_up_to_8(offsets.len() * std::mem::size_of::<i32>());
                validity_bytes + offsets_bytes + element_writer.buffer_size()
            }
            TypedWriter::Map {
                key_writer,
                value_writer,
                offsets,
                validity,
                ..
            } => {
                let validity_bytes = round_up_to_8(validity.len().div_ceil(8));
                let offsets_bytes = round_up_to_8(offsets.len() * std::mem::size_of::<i32>());
                validity_bytes
                    + offsets_bytes
                    + key_writer.buffer_size()
                    + value_writer.buffer_size()
            }
            TypedWriter::Struct {
                field_writers,
                validity,
                ..
            } => {
                let validity_bytes = round_up_to_8(validity.len().div_ceil(8));
                let children_bytes: usize = field_writers.iter().map(|w| w.buffer_size()).sum();
                validity_bytes + children_bytes
            }
        }
    }

    fn append_null(&mut self) {
        match &mut self.inner {
            TypedWriter::List {
                offsets, validity, ..
            }
            | TypedWriter::Map {
                offsets, validity, ..
            } => {
                let last = *offsets.last().unwrap_or(&0);
                offsets.push(last);
                validity.push(false);
            }
            TypedWriter::Struct {
                field_writers,
                validity,
                ..
            } => {
                // Arrow StructArray children must match parent length.
                for child in field_writers.iter_mut() {
                    child.append_null();
                }
                validity.push(false);
            }
            _ => with_builder!(&mut self.inner, b => b.append_null()),
        }
    }

    #[inline]
    fn write_non_null_at(&mut self, row: &dyn InternalRow, pos: usize) -> Result<()> {
        match &mut self.inner {
            TypedWriter::Bool(b) => {
                b.append_value(row.get_boolean(pos)?);
                Ok(())
            }
            TypedWriter::Int8(b) => {
                b.append_value(row.get_byte(pos)?);
                Ok(())
            }
            TypedWriter::Int16(b) => {
                b.append_value(row.get_short(pos)?);
                Ok(())
            }
            TypedWriter::Int32(b) => {
                b.append_value(row.get_int(pos)?);
                Ok(())
            }
            TypedWriter::Int64(b) => {
                b.append_value(row.get_long(pos)?);
                Ok(())
            }
            TypedWriter::Float32(b) => {
                b.append_value(row.get_float(pos)?);
                Ok(())
            }
            TypedWriter::Float64(b) => {
                b.append_value(row.get_double(pos)?);
                Ok(())
            }
            TypedWriter::Char { len, builder } => {
                let v = row.get_char(pos, *len)?;
                builder.append_value(v);
                Ok(())
            }
            TypedWriter::String(b) => {
                let v = row.get_string(pos)?;
                b.append_value(v);
                Ok(())
            }
            TypedWriter::Bytes(b) => {
                let v = row.get_bytes(pos)?;
                b.append_value(v);
                Ok(())
            }
            TypedWriter::Binary { len, builder } => {
                let v = row.get_binary(pos, *len)?;
                builder.append_value(v).map_err(|e| RowConvertError {
                    message: format!("Failed to append binary value: {e}"),
                })?;
                Ok(())
            }
            TypedWriter::Decimal128 {
                src_precision,
                src_scale,
                target_precision,
                target_scale,
                builder,
            } => {
                let decimal = row.get_decimal(pos, *src_precision, *src_scale)?;
                append_decimal_to_builder(&decimal, *target_precision, *target_scale, builder)
            }
            TypedWriter::Date32(b) => {
                let date = row.get_date(pos)?;
                b.append_value(date.get_inner());
                Ok(())
            }
            TypedWriter::Time32Second(b) => {
                let millis = row.get_time(pos)?.get_inner();
                if millis % MILLIS_PER_SECOND as i32 != 0 {
                    return Err(RowConvertError {
                        message: format!(
                            "Time value {millis} ms has sub-second precision but schema expects seconds only"
                        ),
                    });
                }
                b.append_value(millis / MILLIS_PER_SECOND as i32);
                Ok(())
            }
            TypedWriter::Time32Millisecond(b) => {
                b.append_value(row.get_time(pos)?.get_inner());
                Ok(())
            }
            TypedWriter::Time64Microsecond(b) => {
                let millis = row.get_time(pos)?.get_inner();
                let micros = (millis as i64)
                    .checked_mul(MICROS_PER_MILLI)
                    .ok_or_else(|| RowConvertError {
                        message: format!(
                            "Time value {millis} ms overflows when converting to microseconds"
                        ),
                    })?;
                b.append_value(micros);
                Ok(())
            }
            TypedWriter::Time64Nanosecond(b) => {
                let millis = row.get_time(pos)?.get_inner();
                let nanos = (millis as i64)
                    .checked_mul(NANOS_PER_MILLI)
                    .ok_or_else(|| RowConvertError {
                        message: format!(
                            "Time value {millis} ms overflows when converting to nanoseconds"
                        ),
                    })?;
                b.append_value(nanos);
                Ok(())
            }
            // --- TimestampNtz variants ---
            TypedWriter::TimestampNtzSecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ntz(pos, *precision)?;
                builder.append_value(ts.get_millisecond() / MILLIS_PER_SECOND);
                Ok(())
            }
            TypedWriter::TimestampNtzMillisecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ntz(pos, *precision)?;
                builder.append_value(ts.get_millisecond());
                Ok(())
            }
            TypedWriter::TimestampNtzMicrosecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ntz(pos, *precision)?;
                builder.append_value(millis_nanos_to_micros(
                    ts.get_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
                Ok(())
            }
            TypedWriter::TimestampNtzNanosecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ntz(pos, *precision)?;
                builder.append_value(millis_nanos_to_nanos(
                    ts.get_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
                Ok(())
            }
            // --- TimestampLtz variants ---
            TypedWriter::TimestampLtzSecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ltz(pos, *precision)?;
                builder.append_value(ts.get_epoch_millisecond() / MILLIS_PER_SECOND);
                Ok(())
            }
            TypedWriter::TimestampLtzMillisecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ltz(pos, *precision)?;
                builder.append_value(ts.get_epoch_millisecond());
                Ok(())
            }
            TypedWriter::TimestampLtzMicrosecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ltz(pos, *precision)?;
                builder.append_value(millis_nanos_to_micros(
                    ts.get_epoch_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
                Ok(())
            }
            TypedWriter::TimestampLtzNanosecond {
                precision, builder, ..
            } => {
                let ts = row.get_timestamp_ltz(pos, *precision)?;
                builder.append_value(millis_nanos_to_nanos(
                    ts.get_epoch_millisecond(),
                    ts.get_nano_of_millisecond(),
                )?);
                Ok(())
            }
            TypedWriter::List {
                element_writer,
                offsets,
                validity,
            } => {
                let array = row.get_array(pos)?;
                let size = array.size();
                match &mut element_writer.inner {
                    TypedWriter::Struct {
                        field_writers,
                        validity: child_validity,
                        row_type,
                        ..
                    } => {
                        for i in 0..size {
                            if array.is_null_at(i) {
                                for child in field_writers.iter_mut() {
                                    child.append_null();
                                }
                                child_validity.push(false);
                            } else {
                                let nested = array.get_row(i, row_type)?;
                                for (j, child) in field_writers.iter_mut().enumerate() {
                                    child.write_field_at(&nested, j)?;
                                }
                                child_validity.push(true);
                            }
                        }
                    }
                    TypedWriter::Map {
                        key_writer,
                        value_writer,
                        key_type,
                        value_type,
                        offsets: child_offsets,
                        validity: child_validity,
                    } => {
                        for i in 0..size {
                            if array.is_null_at(i) {
                                child_validity.push(false);
                                let last = *child_offsets.last().unwrap();
                                child_offsets.push(last);
                            } else {
                                let map = array.get_map(i, key_type, value_type)?;
                                write_map_into(map, key_writer, value_writer, child_offsets)?;
                                child_validity.push(true);
                            }
                        }
                    }
                    _ => {
                        for i in 0..size {
                            element_writer.write_field_at(&array, i)?;
                        }
                    }
                }
                let last = *offsets.last().unwrap();
                offsets.push(
                    last + i32::try_from(size).map_err(|_| RowConvertError {
                        message: format!("Array size {size} exceeds i32 range"),
                    })?,
                );
                validity.push(true);
                Ok(())
            }
            TypedWriter::Map {
                key_writer,
                value_writer,
                offsets,
                validity,
                ..
            } => {
                let map = row.get_map(pos)?;
                write_map_into(map, key_writer, value_writer, offsets)?;
                validity.push(true);
                Ok(())
            }
            TypedWriter::Struct {
                field_writers,
                validity,
                ..
            } => {
                let nested = row.get_row(pos)?;
                for (i, child) in field_writers.iter_mut().enumerate() {
                    child.write_field_at(nested, i)?;
                }
                validity.push(true);
                Ok(())
            }
        }
    }
}

fn write_map_into(
    map: FlussMap,
    key_writer: &mut ColumnWriter,
    value_writer: &mut ColumnWriter,
    offsets: &mut Vec<i32>,
) -> Result<()> {
    let key_array = map.key_array();
    let value_array = map.value_array();
    for i in 0..map.size() {
        write_array_element_into_column(key_writer, key_array, i)?;
        write_array_element_into_column(value_writer, value_array, i)?;
    }
    let last = *offsets.last().unwrap();
    offsets.push(
        last + i32::try_from(map.size()).map_err(|_| RowConvertError {
            message: format!("Map size {} exceeds i32 range", map.size()),
        })?,
    );
    Ok(())
}

// FlussArray carries no schema; nested row/map elements need the typed
// inherent accessors (get_row/get_map with explicit types).
fn write_array_element_into_column(
    writer: &mut ColumnWriter,
    array: &FlussArray,
    index: usize,
) -> Result<()> {
    match &mut writer.inner {
        TypedWriter::Struct {
            field_writers,
            validity,
            row_type,
            ..
        } => {
            if array.is_null_at(index) {
                for child in field_writers.iter_mut() {
                    child.append_null();
                }
                validity.push(false);
            } else {
                let nested = array.get_row(index, row_type)?;
                for (j, child) in field_writers.iter_mut().enumerate() {
                    child.write_field_at(&nested, j)?;
                }
                validity.push(true);
            }
            Ok(())
        }
        TypedWriter::Map {
            key_writer,
            value_writer,
            key_type,
            value_type,
            offsets,
            validity,
        } => {
            if array.is_null_at(index) {
                validity.push(false);
                let last = *offsets.last().unwrap();
                offsets.push(last);
            } else {
                let nested = array.get_map(index, key_type, value_type)?;
                write_map_into(nested, key_writer, value_writer, offsets)?;
                validity.push(true);
            }
            Ok(())
        }
        _ => writer.write_field_at(array, index),
    }
}

fn finish_struct_array(
    fields: arrow_schema::Fields,
    child_arrays: Vec<ArrayRef>,
    validity: &[bool],
) -> ArrayRef {
    use arrow::array::StructArray;
    use arrow::buffer::NullBuffer;
    use std::sync::Arc;

    let null_buffer = if validity.iter().any(|v| !v) {
        Some(NullBuffer::from(validity.to_vec()))
    } else {
        None
    };
    Arc::new(StructArray::new(fields, child_arrays, null_buffer))
}

fn finish_list_array(
    values: ArrayRef,
    item_nullable: bool,
    offsets: &[i32],
    validity: &[bool],
) -> ArrayRef {
    use arrow::array::ListArray;
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::{Field, FieldRef};
    use std::sync::Arc;

    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets.to_vec()));
    let null_buffer = NullBuffer::from(validity.to_vec());
    let field = Arc::new(Field::new(
        "item",
        values.data_type().clone(),
        item_nullable,
    ));
    let field_ref: FieldRef = field;

    Arc::new(ListArray::new(
        field_ref,
        offsets_buffer,
        values,
        Some(null_buffer),
    ))
}

fn finish_map_array(
    keys: ArrayRef,
    values: ArrayRef,
    value_nullable: bool,
    offsets: &[i32],
    validity: &[bool],
) -> ArrayRef {
    use arrow::array::{Array, MapArray, StructArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::Field;
    use std::sync::Arc;

    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets.to_vec()));
    let null_buffer = NullBuffer::from(validity.to_vec());

    let key_field = Arc::new(Field::new("key", keys.data_type().clone(), false));
    let value_field = Arc::new(Field::new(
        "value",
        values.data_type().clone(),
        value_nullable,
    ));

    let struct_array = StructArray::from(vec![(key_field, keys), (value_field, values)]);

    let entries_field = Arc::new(Field::new(
        "entries",
        struct_array.data_type().clone(),
        false,
    ));

    Arc::new(MapArray::new(
        entries_field,
        offsets_buffer,
        struct_array,
        Some(null_buffer),
        false,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::record::to_arrow_type;
    use crate::row::binary_array::FlussArrayWriter;
    use crate::row::binary_map::FlussMapWriter;
    use crate::row::{Date, Datum, Decimal, GenericRow, Time, TimestampLtz, TimestampNtz};
    use arrow::array::*;
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    /// Helper: create a ColumnWriter from a Fluss DataType, deriving the Arrow type automatically.
    fn writer_for(fluss_type: &DataType, capacity: usize) -> ColumnWriter {
        let arrow_type = to_arrow_type(fluss_type).unwrap();
        ColumnWriter::create(fluss_type, &arrow_type, 0, capacity).unwrap()
    }

    /// Helper: write a single datum and return the finished array.
    fn write_one(fluss_type: &DataType, datum: Datum) -> ArrayRef {
        let mut w = writer_for(fluss_type, 4);
        w.write_field(&GenericRow::from_data(vec![datum])).unwrap();
        w.finish()
    }

    #[test]
    fn write_all_scalar_types() {
        // Boolean
        let arr = write_one(&DataTypes::boolean(), Datum::Bool(true));
        assert!(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0)
        );

        // Integer types
        let arr = write_one(&DataTypes::tinyint(), Datum::Int8(42));
        assert_eq!(
            arr.as_any().downcast_ref::<Int8Array>().unwrap().value(0),
            42
        );

        let arr = write_one(&DataTypes::smallint(), Datum::Int16(1000));
        assert_eq!(
            arr.as_any().downcast_ref::<Int16Array>().unwrap().value(0),
            1000
        );

        let arr = write_one(&DataTypes::int(), Datum::Int32(100_000));
        assert_eq!(
            arr.as_any().downcast_ref::<Int32Array>().unwrap().value(0),
            100_000
        );

        let arr = write_one(&DataTypes::bigint(), Datum::Int64(9_000_000_000));
        assert_eq!(
            arr.as_any().downcast_ref::<Int64Array>().unwrap().value(0),
            9_000_000_000
        );

        // Float types
        let arr = write_one(&DataTypes::float(), Datum::Float32(1.5.into()));
        assert!(
            (arr.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(0)
                - 1.5)
                .abs()
                < 0.001
        );

        let arr = write_one(&DataTypes::double(), Datum::Float64(1.125.into()));
        assert!(
            (arr.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                - 1.125)
                .abs()
                < 0.001
        );

        // String / Char
        let arr = write_one(&DataTypes::string(), Datum::String("hello".into()));
        assert_eq!(
            arr.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "hello"
        );

        let arr = write_one(&DataTypes::char(10), Datum::String("world".into()));
        assert_eq!(
            arr.as_any().downcast_ref::<StringArray>().unwrap().value(0),
            "world"
        );

        // Bytes / Binary
        let arr = write_one(&DataTypes::bytes(), Datum::Blob(vec![1, 2, 3].into()));
        assert_eq!(
            arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(0),
            &[1, 2, 3]
        );

        let arr = write_one(
            &DataTypes::binary(4),
            Datum::Blob(vec![10, 20, 30, 40].into()),
        );
        assert_eq!(
            arr.as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .value(0),
            &[10, 20, 30, 40]
        );

        // Date
        let arr = write_one(&DataTypes::date(), Datum::Date(Date::new(19000)));
        assert_eq!(
            arr.as_any().downcast_ref::<Date32Array>().unwrap().value(0),
            19000
        );

        // Time (precision 3 → Millisecond)
        let arr = write_one(
            &DataTypes::time_with_precision(3),
            Datum::Time(Time::new(45_000)),
        );
        assert_eq!(
            arr.as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .unwrap()
                .value(0),
            45_000
        );

        // Decimal
        let decimal =
            Decimal::from_big_decimal(BigDecimal::from_str("123.45").unwrap(), 10, 2).unwrap();
        let arr = write_one(&DataTypes::decimal(10, 2), Datum::Decimal(decimal));
        assert_eq!(
            arr.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            12345
        );

        // Timestamp NTZ (precision 3 → Millisecond)
        let arr = write_one(
            &DataTypes::timestamp_with_precision(3),
            Datum::TimestampNtz(TimestampNtz::new(1_700_000_000_000)),
        );
        assert_eq!(
            arr.as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            1_700_000_000_000
        );

        // Timestamp LTZ (precision 3 → Millisecond)
        let arr = write_one(
            &DataTypes::timestamp_ltz_with_precision(3),
            Datum::TimestampLtz(TimestampLtz::new(1_700_000_000_000)),
        );
        assert_eq!(
            arr.as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(0),
            1_700_000_000_000
        );
    }

    #[test]
    fn write_null_and_multiple_rows() {
        // Null
        let arr = write_one(&DataTypes::int(), Datum::Null);
        assert!(arr.is_null(0));

        // Multiple rows
        let mut w = writer_for(&DataTypes::int(), 8);
        for val in [10, 20, 30] {
            w.write_field(&GenericRow::from_data(vec![val])).unwrap();
        }
        let arr = w.finish();
        let int_arr = arr.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_arr.len(), 3);
        assert_eq!(int_arr.value(0), 10);
        assert_eq!(int_arr.value(1), 20);
        assert_eq!(int_arr.value(2), 30);

        // buffer_size grows with appended data and does not reset the builder
        let mut w = writer_for(&DataTypes::int(), 4);
        w.write_field(&GenericRow::from_data(vec![42_i32])).unwrap();
        assert!(w.buffer_size() > 0);
        w.write_field(&GenericRow::from_data(vec![99_i32])).unwrap();
        let int_arr = w
            .finish()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        assert_eq!((int_arr.value(0), int_arr.value(1)), (42, 99));
    }

    #[test]
    fn write_array_type() {
        let element_type = DataTypes::int();
        let mut array_writer = FlussArrayWriter::new(3, &element_type);
        array_writer.write_int(0, 10);
        array_writer.set_null_at(1);
        array_writer.write_int(2, 30);
        let fluss_array = array_writer.complete().unwrap();

        let fluss_type = DataTypes::array(element_type);

        let arr = write_one(&fluss_type, Datum::Array(fluss_array));
        let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 1);
        let values = list_arr.value(0);
        let int_values = values.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_values.len(), 3);
        assert_eq!(int_values.value(0), 10);
        assert!(int_values.is_null(1));
        assert_eq!(int_values.value(2), 30);
    }

    #[test]
    fn unsupported_type_returns_error() {
        // Map is currently unsupported in ColumnWriter
        let fluss_type = DataTypes::map(DataTypes::int(), DataTypes::string());
        let arrow_type = ArrowDataType::Boolean; // Any arrow type
        assert!(ColumnWriter::create(&fluss_type, &arrow_type, 0, 4).is_err());
    }

    #[test]
    fn write_non_nullable_array_type() {
        // 1. Define an array of non-nullable integers
        let element_type = DataTypes::int().as_non_nullable();
        let array_type = DataTypes::array(element_type);

        // 2. Create the writer
        let mut writer = writer_for(&array_type, 4);

        // (Optional but good practice) Write a dummy row containing an empty array
        // to ensure the builder processes it without panicking.
        let array_writer = FlussArrayWriter::new(0, &DataTypes::int().as_non_nullable());
        let fluss_array = array_writer.complete().unwrap();
        writer
            .write_field(&GenericRow::from_data(vec![Datum::Array(fluss_array)]))
            .unwrap();

        // 3. FINISH the array to get the actual Arrow output
        let arrow_array = writer.finish();

        // 4. Assert against the actual Arrow schema!
        let list_array = arrow_array
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Expected ListArray");
        let list_field = match list_array.data_type() {
            ArrowDataType::List(field) => field,
            _ => panic!("Expected List type"),
        };

        // This is the true test: Did the Arrow field get marked as NOT NULL?
        assert!(
            !list_field.is_nullable(),
            "Arrow field inside the list should be non-nullable"
        );
    }

    #[test]
    fn test_write_map_type() {
        use crate::metadata::DataTypes;
        let key_type = DataTypes::int();
        let value_type = DataTypes::string();
        let fluss_type = DataTypes::map(key_type.clone(), value_type.clone());

        let mut map_writer = FlussMapWriter::new(2, &key_type, &value_type);
        map_writer.write_entry(1.into(), "a".into()).unwrap();
        map_writer.write_entry(2.into(), "b".into()).unwrap();
        let map = map_writer.complete().unwrap();

        let arr = write_one(&fluss_type, Datum::Map(map));
        let map_arr = arr.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1);

        let entries = map_arr.value(0);
        let struct_arr = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_arr.num_columns(), 2);

        let keys = struct_arr
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = struct_arr
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(keys.len(), 2);
        assert_eq!(keys.value(0), 1);
        assert_eq!(keys.value(1), 2);

        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), "a");
        assert_eq!(values.value(1), "b");
    }

    #[test]
    fn test_write_null_map_type() {
        use crate::metadata::DataTypes;

        let fluss_type = DataTypes::map(DataTypes::int(), DataTypes::string());
        let arr = write_one(&fluss_type, Datum::Null);
        let map_arr = arr.as_any().downcast_ref::<MapArray>().unwrap();

        assert_eq!(map_arr.len(), 1);
        assert!(map_arr.is_null(0));
    }
}
