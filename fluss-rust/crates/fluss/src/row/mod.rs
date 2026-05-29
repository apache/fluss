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

pub mod binary_array;
pub mod binary_map;
mod column;

pub(crate) mod datum;
mod decimal;

pub mod binary;
pub(crate) mod column_writer;
pub mod compacted;
pub mod encode;
pub mod field_getter;
mod fixed_schema_decoder;
mod lookup_row;
mod projected_row;
mod row_decoder;

use crate::client::WriteFormat;
pub use binary_array::{FlussArray, FlussArrayWriter};
pub use binary_map::{FlussMap, FlussMapWriter};
use bytes::Bytes;
pub use column::*;
pub use compacted::CompactedRow;
pub use datum::*;
pub use decimal::{Decimal, MAX_COMPACT_PRECISION};
pub use encode::KeyEncoder;
pub(crate) use fixed_schema_decoder::FixedSchemaDecoder;
pub use lookup_row::LookupRow;
pub(crate) use projected_row::ProjectedRow;
pub use row_decoder::{CompactedRowDecoder, RowDecoder, RowDecoderFactory};
use serde::Serialize;

pub struct BinaryRow<'a> {
    data: BinaryDataWrapper<'a>,
}

pub enum BinaryDataWrapper<'a> {
    Bytes(Bytes),
    Ref(&'a [u8]),
}

impl<'a> BinaryRow<'a> {
    /// Returns the binary representation of this row as a byte slice.
    pub fn as_bytes(&'a self) -> &'a [u8] {
        match &self.data {
            BinaryDataWrapper::Bytes(bytes) => bytes.as_ref(),
            BinaryDataWrapper::Ref(r) => r,
        }
    }
}

use crate::error::Error::IllegalArgument;
use crate::error::Result;

pub trait InternalRow: Send + Sync {
    /// Returns the number of fields in this row
    fn get_field_count(&self) -> usize;

    /// Returns true if the element is null at the given position
    fn is_null_at(&self, pos: usize) -> Result<bool>;

    /// Returns the boolean value at the given position
    fn get_boolean(&self, pos: usize) -> Result<bool>;

    /// Returns the byte value at the given position
    fn get_byte(&self, pos: usize) -> Result<i8>;

    /// Returns the short value at the given position
    fn get_short(&self, pos: usize) -> Result<i16>;

    /// Returns the integer value at the given position
    fn get_int(&self, pos: usize) -> Result<i32>;

    /// Returns the long value at the given position
    fn get_long(&self, pos: usize) -> Result<i64>;

    /// Returns the float value at the given position
    fn get_float(&self, pos: usize) -> Result<f32>;

    /// Returns the double value at the given position
    fn get_double(&self, pos: usize) -> Result<f64>;

    /// Returns the string value at the given position with fixed length
    fn get_char(&self, pos: usize, length: usize) -> Result<&str>;

    /// Returns the string value at the given position
    fn get_string(&self, pos: usize) -> Result<&str>;

    /// Returns the decimal value at the given position
    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal>;

    /// Returns the date value at the given position (date as days since epoch)
    fn get_date(&self, pos: usize) -> Result<Date>;

    /// Returns the time value at the given position (time as milliseconds since midnight)
    fn get_time(&self, pos: usize) -> Result<Time>;

    /// Returns the timestamp value at the given position (timestamp without timezone)
    ///
    /// The precision is required to determine whether the timestamp value was stored
    /// in a compact representation (precision <= 3) or with nanosecond precision.
    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz>;

    /// Returns the timestamp value at the given position (timestamp with local timezone)
    ///
    /// The precision is required to determine whether the timestamp value was stored
    /// in a compact representation (precision <= 3) or with nanosecond precision.
    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz>;

    /// Returns the binary value at the given position with fixed length
    fn get_binary(&self, pos: usize, length: usize) -> Result<&[u8]>;

    /// Returns the binary value at the given position
    fn get_bytes(&self, pos: usize) -> Result<&[u8]>;

    /// Returns the array value at the given position
    fn get_array(&self, pos: usize) -> Result<FlussArray>;

    /// Returns the map value at the given position
    fn get_map(&self, pos: usize) -> Result<FlussMap>;

    /// Returns the nested row value at the given position
    fn get_row(&self, pos: usize) -> Result<&GenericRow<'_>> {
        Err(IllegalArgument {
            message: format!("get_row not supported at position {pos}"),
        })
    }

    /// Returns encoded bytes if already encoded
    fn as_encoded_bytes(&self, _write_format: WriteFormat) -> Option<&[u8]> {
        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
pub struct GenericRow<'a> {
    pub values: Vec<Datum<'a>>,
}

impl<'a> GenericRow<'a> {
    fn get_value(&self, pos: usize) -> Result<&Datum<'a>> {
        self.values.get(pos).ok_or_else(|| IllegalArgument {
            message: format!(
                "position {pos} out of bounds (row has {} fields)",
                self.values.len()
            ),
        })
    }

    fn try_convert<T: TryFrom<&'a Datum<'a>>>(
        &'a self,
        pos: usize,
        expected_type: &str,
    ) -> Result<T> {
        let datum = self.get_value(pos)?;
        T::try_from(datum).map_err(|_| IllegalArgument {
            message: format!(
                "type mismatch at position {pos}: expected {expected_type}, got {datum:?}"
            ),
        })
    }
}

impl<'a> InternalRow for GenericRow<'a> {
    fn get_field_count(&self) -> usize {
        self.values.len()
    }

    fn is_null_at(&self, pos: usize) -> Result<bool> {
        Ok(self.get_value(pos)?.is_null())
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        self.try_convert(pos, "Boolean")
    }

    fn get_byte(&self, pos: usize) -> Result<i8> {
        self.try_convert(pos, "TinyInt")
    }

    fn get_short(&self, pos: usize) -> Result<i16> {
        self.try_convert(pos, "SmallInt")
    }

    fn get_int(&self, pos: usize) -> Result<i32> {
        self.try_convert(pos, "Int")
    }

    fn get_long(&self, pos: usize) -> Result<i64> {
        self.try_convert(pos, "BigInt")
    }

    fn get_float(&self, pos: usize) -> Result<f32> {
        self.try_convert(pos, "Float")
    }

    fn get_double(&self, pos: usize) -> Result<f64> {
        self.try_convert(pos, "Double")
    }

    fn get_char(&self, pos: usize, _length: usize) -> Result<&str> {
        // don't check length, following java client
        self.get_string(pos)
    }

    fn get_string(&self, pos: usize) -> Result<&str> {
        self.try_convert(pos, "String")
    }

    fn get_decimal(&self, pos: usize, _precision: usize, _scale: usize) -> Result<Decimal> {
        match self.get_value(pos)? {
            Datum::Decimal(d) => Ok(d.clone()),
            other => Err(IllegalArgument {
                message: format!(
                    "type mismatch at position {pos}: expected Decimal, got {other:?}"
                ),
            }),
        }
    }

    fn get_date(&self, pos: usize) -> Result<Date> {
        match self.get_value(pos)? {
            Datum::Date(d) => Ok(*d),
            Datum::Int32(i) => Ok(Date::new(*i)),
            other => Err(IllegalArgument {
                message: format!(
                    "type mismatch at position {pos}: expected Date or Int32, got {other:?}"
                ),
            }),
        }
    }

    fn get_time(&self, pos: usize) -> Result<Time> {
        match self.get_value(pos)? {
            Datum::Time(t) => Ok(*t),
            Datum::Int32(i) => Ok(Time::new(*i)),
            other => Err(IllegalArgument {
                message: format!(
                    "type mismatch at position {pos}: expected Time or Int32, got {other:?}"
                ),
            }),
        }
    }

    fn get_timestamp_ntz(&self, pos: usize, _precision: u32) -> Result<TimestampNtz> {
        match self.get_value(pos)? {
            Datum::TimestampNtz(t) => Ok(*t),
            other => Err(IllegalArgument {
                message: format!(
                    "type mismatch at position {pos}: expected TimestampNtz, got {other:?}"
                ),
            }),
        }
    }

    fn get_timestamp_ltz(&self, pos: usize, _precision: u32) -> Result<TimestampLtz> {
        match self.get_value(pos)? {
            Datum::TimestampLtz(t) => Ok(*t),
            other => Err(IllegalArgument {
                message: format!(
                    "type mismatch at position {pos}: expected TimestampLtz, got {other:?}"
                ),
            }),
        }
    }

    fn get_binary(&self, pos: usize, _length: usize) -> Result<&[u8]> {
        match self.get_value(pos)? {
            Datum::Blob(b) => Ok(b.as_ref()),
            other => Err(IllegalArgument {
                message: format!("type mismatch at position {pos}: expected Binary, got {other:?}"),
            }),
        }
    }

    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        match self.get_value(pos)? {
            Datum::Blob(b) => Ok(b.as_ref()),
            other => Err(IllegalArgument {
                message: format!("type mismatch at position {pos}: expected Bytes, got {other:?}"),
            }),
        }
    }

    fn get_array(&self, pos: usize) -> Result<FlussArray> {
        match self.get_value(pos)? {
            Datum::Array(a) => Ok(a.clone()),
            other => Err(IllegalArgument {
                message: format!("type mismatch at position {pos}: expected Array, got {other:?}"),
            }),
        }
    }

    fn get_map(&self, pos: usize) -> Result<FlussMap> {
        match self.get_value(pos)? {
            Datum::Map(m) => Ok(m.clone()),
            other => Err(IllegalArgument {
                message: format!("type mismatch at position {pos}: expected Map, got {other:?}"),
            }),
        }
    }

    fn get_row(&self, pos: usize) -> Result<&GenericRow<'_>> {
        match self.get_value(pos)? {
            Datum::Row(r) => Ok(r.as_ref()),
            other => Err(IllegalArgument {
                message: format!("type mismatch at position {pos}: expected Row, got {other:?}"),
            }),
        }
    }
}

impl<'a> GenericRow<'a> {
    /// Consumes this row and returns one whose `Datum` values are all
    /// `'static` (borrowed `Cow`s are promoted to owned, nested rows recurse).
    /// Lets a row outlive the bytes it was decoded from.
    pub fn into_owned(self) -> GenericRow<'static> {
        GenericRow {
            values: self.values.into_iter().map(Datum::into_owned).collect(),
        }
    }
}

impl<'a> GenericRow<'a> {
    pub fn from_data(data: Vec<impl Into<Datum<'a>>>) -> GenericRow<'a> {
        GenericRow {
            values: data.into_iter().map(Into::into).collect(),
        }
    }

    /// Creates a GenericRow with the specified number of fields, all initialized to null.
    ///
    /// This is useful when you need to create a row with a specific field count
    /// but only want to set some fields (e.g., for KV delete operations where
    /// only primary key fields need to be set).
    ///
    /// # Example
    /// ```
    /// use fluss::row::GenericRow;
    ///
    /// let mut row = GenericRow::new(3);
    /// row.set_field(0, 42); // Only set the primary key
    /// // Fields 1 and 2 remain null
    /// ```
    pub fn new(field_count: usize) -> GenericRow<'a> {
        GenericRow {
            values: vec![Datum::Null; field_count],
        }
    }

    /// Sets the field at the given position to the specified value.
    ///
    /// # Panics
    /// Panics if `pos` is out of bounds (>= field count).
    pub fn set_field(&mut self, pos: usize, value: impl Into<Datum<'a>>) {
        self.values[pos] = value.into();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_null_at_checks_datum_nullity() {
        let mut row = GenericRow::new(2);
        row.set_field(0, Datum::Null);
        row.set_field(1, 42_i32);

        assert!(row.is_null_at(0).unwrap());
        assert!(!row.is_null_at(1).unwrap());
    }

    #[test]
    fn is_null_at_out_of_bounds_returns_error() {
        let row = GenericRow::from_data(vec![42_i32]);
        let err = row.is_null_at(5).unwrap_err();
        assert!(
            err.to_string().contains("out of bounds"),
            "Expected out of bounds error, got: {err}"
        );
    }

    #[test]
    fn new_initializes_nulls() {
        let row = GenericRow::new(3);
        assert_eq!(row.get_field_count(), 3);
        assert!(row.is_null_at(0).unwrap());
        assert!(row.is_null_at(1).unwrap());
        assert!(row.is_null_at(2).unwrap());
    }

    #[test]
    fn partial_row_for_delete() {
        // Simulates delete scenario: only primary key (field 0) is set
        let mut row = GenericRow::new(3);
        row.set_field(0, 123_i32);
        // Fields 1 and 2 remain null
        assert_eq!(row.get_field_count(), 3);
        assert_eq!(row.get_int(0).unwrap(), 123);
        assert!(row.is_null_at(1).unwrap());
        assert!(row.is_null_at(2).unwrap());
    }

    #[test]
    fn type_mismatch_returns_error() {
        let row = GenericRow::from_data(vec![Datum::Int64(999)]);
        let err = row.get_string(0).unwrap_err();
        assert!(
            err.to_string().contains("type mismatch"),
            "Expected type mismatch error, got: {err}"
        );
    }

    #[test]
    fn out_of_bounds_returns_error() {
        let row = GenericRow::from_data(vec![42_i32]);
        let err = row.get_int(5).unwrap_err();
        assert!(
            err.to_string().contains("out of bounds"),
            "Expected out of bounds error, got: {err}"
        );
    }
}
