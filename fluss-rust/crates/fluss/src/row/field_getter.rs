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

use crate::error::Result;
use crate::metadata::{DataType, RowType};
use crate::row::{Datum, InternalRow};

#[derive(Clone)]
pub enum FieldGetter {
    Nullable(InnerFieldGetter),
    NonNullable(InnerFieldGetter),
}
impl FieldGetter {
    pub fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Result<Datum<'a>> {
        match self {
            FieldGetter::Nullable(getter) => {
                if row.is_null_at(getter.pos())? {
                    Ok(Datum::Null)
                } else {
                    getter.get_field(row)
                }
            }
            FieldGetter::NonNullable(getter) => getter.get_field(row),
        }
    }

    #[allow(dead_code)]
    pub fn create_field_getters(row_type: &RowType) -> Box<[FieldGetter]> {
        row_type
            .fields()
            .iter()
            .enumerate()
            .map(|(pos, field)| Self::create(field.data_type(), pos))
            .collect()
    }

    pub fn create(data_type: &DataType, pos: usize) -> FieldGetter {
        let inner_field_getter = match data_type {
            DataType::Char(t) => InnerFieldGetter::Char {
                pos,
                len: t.length() as usize,
            },
            DataType::String(_) => InnerFieldGetter::String { pos },
            DataType::Boolean(_) => InnerFieldGetter::Bool { pos },
            DataType::Binary(t) => InnerFieldGetter::Binary {
                pos,
                len: t.length(),
            },
            DataType::Bytes(_) => InnerFieldGetter::Bytes { pos },
            DataType::TinyInt(_) => InnerFieldGetter::TinyInt { pos },
            DataType::SmallInt(_) => InnerFieldGetter::SmallInt { pos },
            DataType::Int(_) => InnerFieldGetter::Int { pos },
            DataType::BigInt(_) => InnerFieldGetter::BigInt { pos },
            DataType::Float(_) => InnerFieldGetter::Float { pos },
            DataType::Double(_) => InnerFieldGetter::Double { pos },
            DataType::Decimal(decimal_type) => InnerFieldGetter::Decimal {
                pos,
                precision: decimal_type.precision() as usize,
                scale: decimal_type.scale() as usize,
            },
            DataType::Date(_) => InnerFieldGetter::Date { pos },
            DataType::Time(_) => InnerFieldGetter::Time { pos },
            DataType::Timestamp(t) => InnerFieldGetter::Timestamp {
                pos,
                precision: t.precision(),
            },
            DataType::TimestampLTz(t) => InnerFieldGetter::TimestampLtz {
                pos,
                precision: t.precision(),
            },
            DataType::Array(_) => InnerFieldGetter::Array { pos },
            DataType::Map(m) => InnerFieldGetter::Map {
                pos,
                key_type: m.key_type().clone(),
                value_type: m.value_type().clone(),
            },
            DataType::Row(_) => InnerFieldGetter::Row { pos },
        };

        if data_type.is_nullable() {
            Self::Nullable(inner_field_getter)
        } else {
            Self::NonNullable(inner_field_getter)
        }
    }
}

#[derive(Clone)]
pub enum InnerFieldGetter {
    Char {
        pos: usize,
        len: usize,
    },
    String {
        pos: usize,
    },
    Bool {
        pos: usize,
    },
    Binary {
        pos: usize,
        len: usize,
    },
    Bytes {
        pos: usize,
    },
    TinyInt {
        pos: usize,
    },
    SmallInt {
        pos: usize,
    },
    Int {
        pos: usize,
    },
    BigInt {
        pos: usize,
    },
    Float {
        pos: usize,
    },
    Double {
        pos: usize,
    },
    Decimal {
        pos: usize,
        precision: usize,
        scale: usize,
    },
    Date {
        pos: usize,
    },
    Time {
        pos: usize,
    },
    Timestamp {
        pos: usize,
        precision: u32,
    },
    TimestampLtz {
        pos: usize,
        precision: u32,
    },
    Array {
        pos: usize,
    },
    Map {
        pos: usize,
        key_type: DataType,
        value_type: DataType,
    },
    Row {
        pos: usize,
    },
}

impl InnerFieldGetter {
    pub fn get_field<'a>(&self, row: &'a dyn InternalRow) -> Result<Datum<'a>> {
        Ok(match self {
            InnerFieldGetter::Char { pos, len } => Datum::from(row.get_char(*pos, *len)?),
            InnerFieldGetter::String { pos } => Datum::from(row.get_string(*pos)?),
            InnerFieldGetter::Bool { pos } => Datum::from(row.get_boolean(*pos)?),
            InnerFieldGetter::Binary { pos, len } => Datum::from(row.get_binary(*pos, *len)?),
            InnerFieldGetter::Bytes { pos } => Datum::from(row.get_bytes(*pos)?),
            InnerFieldGetter::TinyInt { pos } => Datum::from(row.get_byte(*pos)?),
            InnerFieldGetter::SmallInt { pos } => Datum::from(row.get_short(*pos)?),
            InnerFieldGetter::Int { pos } => Datum::from(row.get_int(*pos)?),
            InnerFieldGetter::BigInt { pos } => Datum::from(row.get_long(*pos)?),
            InnerFieldGetter::Float { pos } => Datum::from(row.get_float(*pos)?),
            InnerFieldGetter::Double { pos } => Datum::from(row.get_double(*pos)?),
            InnerFieldGetter::Decimal {
                pos,
                precision,
                scale,
            } => Datum::Decimal(row.get_decimal(*pos, *precision, *scale)?),
            InnerFieldGetter::Date { pos } => Datum::Date(row.get_date(*pos)?),
            InnerFieldGetter::Time { pos } => Datum::Time(row.get_time(*pos)?),
            InnerFieldGetter::Timestamp { pos, precision } => {
                Datum::TimestampNtz(row.get_timestamp_ntz(*pos, *precision)?)
            }
            InnerFieldGetter::TimestampLtz { pos, precision } => {
                Datum::TimestampLtz(row.get_timestamp_ltz(*pos, *precision)?)
            }
            InnerFieldGetter::Array { pos } => Datum::Array(row.get_array(*pos)?),
            InnerFieldGetter::Map { pos, .. } => Datum::Map(row.get_map(*pos)?),
            InnerFieldGetter::Row { pos } => Datum::Row(Box::new(row.get_row(*pos)?.clone())),
        })
    }

    pub fn pos(&self) -> usize {
        match self {
            Self::Char { pos, .. }
            | Self::String { pos }
            | Self::Bool { pos }
            | Self::Binary { pos, .. }
            | Self::Bytes { pos }
            | Self::TinyInt { pos }
            | Self::SmallInt { pos, .. }
            | Self::Int { pos }
            | Self::BigInt { pos }
            | Self::Float { pos, .. }
            | Self::Double { pos }
            | Self::Decimal { pos, .. }
            | Self::Date { pos }
            | Self::Time { pos }
            | Self::Timestamp { pos, .. }
            | Self::TimestampLtz { pos, .. }
            | Self::Array { pos }
            | Self::Map { pos, .. }
            | Self::Row { pos } => *pos,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;
    use crate::row::GenericRow;
    use crate::row::binary_array::FlussArrayWriter;
    use crate::row::binary_map::FlussMapWriter;

    #[test]
    fn test_field_getter_array() {
        let elem_type = DataTypes::int();
        let mut arr_writer = FlussArrayWriter::new(2, &elem_type);
        arr_writer.write_int(0, 10);
        arr_writer.write_int(1, 20);
        let arr = arr_writer.complete().unwrap();

        let mut row = GenericRow::new(2);
        row.set_field(0, Datum::Int32(42));
        row.set_field(1, Datum::Array(arr.clone()));

        let getter = FieldGetter::create(&DataTypes::array(DataTypes::int()), 1);
        let datum = getter.get_field(&row).unwrap();

        match datum {
            Datum::Array(a) => {
                assert_eq!(a.size(), 2);
                assert_eq!(a.get_int(0).unwrap(), 10);
                assert_eq!(a.get_int(1).unwrap(), 20);
            }
            _ => panic!("Expected Array datum"),
        }
    }

    #[test]
    fn test_field_getter_nullable_array() {
        let row = GenericRow::from_data(vec![Datum::Null]);

        let data_type = DataTypes::array(DataTypes::int());
        let getter = FieldGetter::create(&data_type, 0);
        let datum = getter.get_field(&row).unwrap();
        assert!(datum.is_null());
    }

    #[test]
    fn test_field_getter_map() {
        let mut map_writer = FlussMapWriter::new(1, &DataTypes::int(), &DataTypes::string());
        map_writer.write_entry(42.into(), "value".into()).unwrap();
        let map = map_writer.complete().unwrap();

        let mut row = GenericRow::new(2);
        row.set_field(0, Datum::Int32(1));
        row.set_field(1, Datum::Map(map));

        let data_type = DataTypes::map(DataTypes::int(), DataTypes::string());
        let getter = FieldGetter::create(&data_type, 1);
        let datum = getter.get_field(&row).unwrap();

        match datum {
            Datum::Map(m) => {
                assert_eq!(m.size(), 1);
                assert_eq!(m.key_array().get_int(0).unwrap(), 42);
                assert_eq!(m.value_array().get_string(0).unwrap(), "value");
            }
            _ => panic!("Expected Map datum"),
        }
    }

    #[test]
    fn test_field_getter_nullable_map() {
        let row = GenericRow::from_data(vec![Datum::Null]);

        let data_type = DataTypes::map(DataTypes::int(), DataTypes::string());
        let getter = FieldGetter::create(&data_type, 0);
        let datum = getter.get_field(&row).unwrap();
        assert!(datum.is_null());
    }
}
