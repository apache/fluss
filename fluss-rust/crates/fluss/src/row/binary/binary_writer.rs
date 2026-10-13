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
use crate::row::Decimal;
use crate::row::binary::BinaryRowFormat;
use crate::row::datum::{TimestampLtz, TimestampNtz};
use crate::row::{Datum, FlussArray, FlussMap};

/// Writer to write a composite data format, like row, array,
#[allow(dead_code)]
pub trait BinaryWriter {
    /// Reset writer to prepare next write
    fn reset(&mut self);

    /// Set null to this field
    fn set_null_at(&mut self, pos: usize);

    fn write_boolean(&mut self, value: bool);

    fn write_byte(&mut self, value: u8);

    fn write_bytes(&mut self, value: &[u8]);

    fn write_char(&mut self, value: &str, length: usize);

    fn write_string(&mut self, value: &str);

    fn write_short(&mut self, value: i16);

    fn write_int(&mut self, value: i32);

    fn write_long(&mut self, value: i64);

    fn write_float(&mut self, value: f32);

    fn write_double(&mut self, value: f64);

    fn write_binary(&mut self, bytes: &[u8], length: usize);

    fn write_decimal(&mut self, value: &Decimal, precision: u32);

    /// Writes a TIME value.
    ///
    /// Note: TIME is physically stored as an i32 (milliseconds since midnight).
    /// This method exists for type safety and semantic clarity, even though it's
    /// currently equivalent to `write_int()`. The precision parameter is accepted
    /// for API consistency with TIMESTAMP types, though TIME encoding doesn't
    /// currently vary by precision.
    fn write_time(&mut self, value: i32, precision: u32);

    fn write_timestamp_ntz(&mut self, value: &TimestampNtz, precision: u32);

    fn write_timestamp_ltz(&mut self, value: &TimestampLtz, precision: u32);

    fn write_array(&mut self, value: &FlussArray);

    fn write_map(&mut self, value: &FlussMap);

    // TODO Row serializer
    // fn write_row(&mut self, pos: i32, value: &InternalRow);

    /// Finally, complete write to set real size to binary.
    fn complete(&mut self);
}

pub enum ValueWriter {
    Nullable(InnerValueWriter),
    NonNullable(InnerValueWriter),
}

impl ValueWriter {
    pub fn create_value_writer(
        element_type: &DataType,
        binary_row_format: Option<&BinaryRowFormat>,
    ) -> Result<ValueWriter> {
        let value_writer =
            InnerValueWriter::create_inner_value_writer(element_type, binary_row_format)?;
        if element_type.is_nullable() {
            Ok(Self::Nullable(value_writer))
        } else {
            Ok(Self::NonNullable(value_writer))
        }
    }

    pub fn write_value<W: BinaryWriter>(
        &self,
        writer: &mut W,
        pos: usize,
        value: &Datum,
    ) -> Result<()> {
        match self {
            Self::Nullable(inner_value_writer) => {
                if let Datum::Null = value {
                    writer.set_null_at(pos);
                    Ok(())
                } else {
                    inner_value_writer.write_value(writer, pos, value)
                }
            }
            Self::NonNullable(inner_value_writer) => {
                inner_value_writer.write_value(writer, pos, value)
            }
        }
    }
}

#[derive(Debug)]
pub enum InnerValueWriter {
    Char,
    String,
    Boolean,
    Binary,
    Bytes,
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal(u32, u32), // precision, scale
    Date,
    Time(u32),         // precision (not used in wire format, but kept for consistency)
    TimestampNtz(u32), // precision
    TimestampLtz(u32), // precision
    Array,
    Map,
    Row(NestedRowWriter),
}

#[derive(Debug)]
pub struct NestedRowWriter {
    field_writers: Vec<InnerValueWriter>,
    field_nullable: Vec<bool>,
}

impl NestedRowWriter {
    fn from_row_type(row_type: &RowType) -> Result<Self> {
        let fields = row_type.fields();
        let mut field_writers = Vec::with_capacity(fields.len());
        let mut field_nullable = Vec::with_capacity(fields.len());
        for field in fields {
            field_writers.push(InnerValueWriter::create_inner_value_writer(
                field.data_type(),
                None,
            )?);
            field_nullable.push(field.data_type().is_nullable());
        }
        Ok(Self {
            field_writers,
            field_nullable,
        })
    }

    fn field_count(&self) -> usize {
        self.field_writers.len()
    }
}

/// Accessor for writing the fields/elements of a binary writer during runtime, the
/// fields/elements must be written in the order.
impl InnerValueWriter {
    pub fn create_inner_value_writer(
        data_type: &DataType,
        _: Option<&BinaryRowFormat>,
    ) -> Result<InnerValueWriter> {
        match data_type {
            DataType::Char(_) => Ok(InnerValueWriter::Char),
            DataType::String(_) => Ok(InnerValueWriter::String),
            DataType::Boolean(_) => Ok(InnerValueWriter::Boolean),
            DataType::Binary(_) => Ok(InnerValueWriter::Binary),
            DataType::Bytes(_) => Ok(InnerValueWriter::Bytes),
            DataType::TinyInt(_) => Ok(InnerValueWriter::TinyInt),
            DataType::SmallInt(_) => Ok(InnerValueWriter::SmallInt),
            DataType::Int(_) => Ok(InnerValueWriter::Int),
            DataType::BigInt(_) => Ok(InnerValueWriter::BigInt),
            DataType::Float(_) => Ok(InnerValueWriter::Float),
            DataType::Double(_) => Ok(InnerValueWriter::Double),
            DataType::Decimal(d) => {
                // Validation is done at DecimalType construction time
                Ok(InnerValueWriter::Decimal(d.precision(), d.scale()))
            }
            DataType::Date(_) => Ok(InnerValueWriter::Date),
            DataType::Time(t) => {
                // Validation is done at TimeType construction time
                Ok(InnerValueWriter::Time(t.precision()))
            }
            DataType::Timestamp(t) => {
                // Validation is done at TimestampType construction time
                Ok(InnerValueWriter::TimestampNtz(t.precision()))
            }
            DataType::TimestampLTz(t) => {
                // Validation is done at TimestampLTzType construction time
                Ok(InnerValueWriter::TimestampLtz(t.precision()))
            }
            DataType::Array(_) => Ok(InnerValueWriter::Array),
            DataType::Map(_) => Ok(InnerValueWriter::Map),
            DataType::Row(row_type) => Ok(InnerValueWriter::Row(NestedRowWriter::from_row_type(
                row_type,
            )?)),
        }
    }
    pub fn write_value<W: BinaryWriter>(
        &self,
        writer: &mut W,
        _pos: usize,
        value: &Datum,
    ) -> Result<()> {
        match (self, value) {
            (InnerValueWriter::Char, Datum::String(v)) => {
                writer.write_char(v, v.len());
            }
            (InnerValueWriter::String, Datum::String(v)) => {
                writer.write_string(v);
            }
            (InnerValueWriter::Boolean, Datum::Bool(v)) => {
                writer.write_boolean(*v);
            }
            (InnerValueWriter::Binary, Datum::Blob(v)) => {
                let b = v.as_ref();
                writer.write_binary(b, b.len());
            }
            (InnerValueWriter::Bytes, Datum::Blob(v)) => {
                writer.write_bytes(v.as_ref());
            }
            (InnerValueWriter::TinyInt, Datum::Int8(v)) => {
                writer.write_byte(*v as u8);
            }
            (InnerValueWriter::SmallInt, Datum::Int16(v)) => {
                writer.write_short(*v);
            }
            (InnerValueWriter::Int, Datum::Int32(v)) => {
                writer.write_int(*v);
            }
            (InnerValueWriter::BigInt, Datum::Int64(v)) => {
                writer.write_long(*v);
            }
            (InnerValueWriter::Float, Datum::Float32(v)) => {
                writer.write_float(v.into_inner());
            }
            (InnerValueWriter::Double, Datum::Float64(v)) => {
                writer.write_double(v.into_inner());
            }
            (InnerValueWriter::Decimal(p, _s), Datum::Decimal(v)) => {
                writer.write_decimal(v, *p);
            }
            (InnerValueWriter::Date, Datum::Date(d)) => {
                writer.write_int(d.get_inner());
            }
            (InnerValueWriter::Time(p), Datum::Time(t)) => {
                writer.write_time(t.get_inner(), *p);
            }
            (InnerValueWriter::TimestampNtz(p), Datum::TimestampNtz(ts)) => {
                writer.write_timestamp_ntz(ts, *p);
            }
            (InnerValueWriter::TimestampLtz(p), Datum::TimestampLtz(ts)) => {
                writer.write_timestamp_ltz(ts, *p);
            }
            (InnerValueWriter::Array, Datum::Array(arr)) => {
                writer.write_array(arr);
            }
            (InnerValueWriter::Map, Datum::Map(map)) => {
                writer.write_map(map);
            }
            (InnerValueWriter::Row(nested_writer), Datum::Row(inner_row)) => {
                use crate::row::compacted::CompactedRowWriter;
                let field_count = nested_writer.field_count();
                if inner_row.values.len() != field_count {
                    return Err(IllegalArgument {
                        message: format!(
                            "nested row arity mismatch: schema has {} fields, got {}",
                            field_count,
                            inner_row.values.len(),
                        ),
                    });
                }
                let mut nested = CompactedRowWriter::new(field_count);
                for (i, datum) in inner_row.values.iter().enumerate() {
                    if datum.is_null() {
                        if !nested_writer.field_nullable[i] {
                            return Err(IllegalArgument {
                                message: format!(
                                    "nested row field {i} is non-nullable but received null",
                                ),
                            });
                        }
                        nested.set_null_at(i);
                    } else {
                        nested_writer.field_writers[i].write_value(&mut nested, i, datum)?;
                    }
                }
                writer.write_bytes(nested.buffer());
            }
            _ => {
                return Err(IllegalArgument {
                    message: format!("{self:?} used to write value {value:?}"),
                });
            }
        }
        Ok(())
    }
}
