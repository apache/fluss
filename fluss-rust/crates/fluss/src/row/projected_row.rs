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

//! View over an [`InternalRow`] that re-orders, drops, and null-pads
//! fields according to a target→source index mapping.

use crate::client::WriteFormat;
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::UNEXIST_MAPPING;
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
use crate::row::{Decimal, FlussArray, FlussMap, GenericRow, InternalRow};
use std::sync::Arc;

pub(crate) struct ProjectedRow<R> {
    index_mapping: Arc<[i32]>,
    inner: R,
}

impl<R> ProjectedRow<R> {
    pub fn new(inner: R, index_mapping: Arc<[i32]>) -> Self {
        Self {
            index_mapping,
            inner,
        }
    }

    fn source_index(&self, pos: usize) -> Result<usize> {
        let mapped = self
            .index_mapping
            .get(pos)
            .copied()
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "position {pos} out of bounds (projected row has {} fields)",
                    self.index_mapping.len()
                ),
            })?;
        if mapped == UNEXIST_MAPPING {
            return Err(IllegalArgument {
                message: format!(
                    "field at position {pos} does not exist in the source row \
                     (caller should check is_null_at first)"
                ),
            });
        }
        Ok(mapped as usize)
    }
}

macro_rules! project {
    ($self:ident, $method:ident, $pos:expr $(, $arg:expr)*) => {
        $self.inner.$method($self.source_index($pos)?, $($arg),*)
    };
}

impl<R: InternalRow> InternalRow for ProjectedRow<R> {
    fn get_field_count(&self) -> usize {
        self.index_mapping.len()
    }

    fn is_null_at(&self, pos: usize) -> Result<bool> {
        let mapped = self
            .index_mapping
            .get(pos)
            .copied()
            .ok_or_else(|| IllegalArgument {
                message: format!(
                    "position {pos} out of bounds (projected row has {} fields)",
                    self.index_mapping.len()
                ),
            })?;
        if mapped == UNEXIST_MAPPING {
            return Ok(true);
        }
        self.inner.is_null_at(mapped as usize)
    }

    fn get_boolean(&self, pos: usize) -> Result<bool> {
        project!(self, get_boolean, pos)
    }
    fn get_byte(&self, pos: usize) -> Result<i8> {
        project!(self, get_byte, pos)
    }
    fn get_short(&self, pos: usize) -> Result<i16> {
        project!(self, get_short, pos)
    }
    fn get_int(&self, pos: usize) -> Result<i32> {
        project!(self, get_int, pos)
    }
    fn get_long(&self, pos: usize) -> Result<i64> {
        project!(self, get_long, pos)
    }
    fn get_float(&self, pos: usize) -> Result<f32> {
        project!(self, get_float, pos)
    }
    fn get_double(&self, pos: usize) -> Result<f64> {
        project!(self, get_double, pos)
    }
    fn get_char(&self, pos: usize, length: usize) -> Result<&str> {
        project!(self, get_char, pos, length)
    }
    fn get_string(&self, pos: usize) -> Result<&str> {
        project!(self, get_string, pos)
    }
    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        project!(self, get_decimal, pos, precision, scale)
    }
    fn get_date(&self, pos: usize) -> Result<Date> {
        project!(self, get_date, pos)
    }
    fn get_time(&self, pos: usize) -> Result<Time> {
        project!(self, get_time, pos)
    }
    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        project!(self, get_timestamp_ntz, pos, precision)
    }
    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        project!(self, get_timestamp_ltz, pos, precision)
    }
    fn get_binary(&self, pos: usize, length: usize) -> Result<&[u8]> {
        project!(self, get_binary, pos, length)
    }
    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        project!(self, get_bytes, pos)
    }
    fn get_array(&self, pos: usize) -> Result<FlussArray> {
        project!(self, get_array, pos)
    }

    fn get_map(&self, pos: usize) -> Result<FlussMap> {
        project!(self, get_map, pos)
    }

    fn get_row(&self, pos: usize) -> Result<&GenericRow<'_>> {
        project!(self, get_row, pos)
    }

    fn as_encoded_bytes(&self, _write_format: WriteFormat) -> Option<&[u8]> {
        // Projection changes the field layout, so the inner row's
        // encoded form no longer matches.
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::{Datum, GenericRow};

    fn mapping(slots: &[i32]) -> Arc<[i32]> {
        Arc::from(slots.to_vec().into_boxed_slice())
    }

    fn row_of<'a>(values: Vec<Datum<'a>>) -> GenericRow<'a> {
        GenericRow { values }
    }

    #[test]
    fn projects_and_reorders_longs() {
        let mapping = mapping(&[2, 0, 1, 4]);
        let inner = row_of(vec![
            Datum::Int64(0),
            Datum::Int64(1),
            Datum::Int64(2),
            Datum::Int64(3),
            Datum::Int64(4),
        ]);
        let projected = ProjectedRow::new(inner, mapping);

        assert_eq!(projected.get_field_count(), 4);
        assert_eq!(projected.get_long(0).unwrap(), 2);
        assert_eq!(projected.get_long(1).unwrap(), 0);
        assert_eq!(projected.get_long(2).unwrap(), 1);
        assert_eq!(projected.get_long(3).unwrap(), 4);
    }

    #[test]
    fn projects_strings_and_doubles() {
        let mapping = mapping(&[2, 0, 1, 4]);

        let strings = row_of(vec![
            Datum::String("0".into()),
            Datum::String("1".into()),
            Datum::String("2".into()),
            Datum::String("3".into()),
            Datum::String("4".into()),
        ]);
        let projected = ProjectedRow::new(strings, Arc::clone(&mapping));
        assert_eq!(projected.get_string(0).unwrap(), "2");
        assert_eq!(projected.get_string(1).unwrap(), "0");
        assert_eq!(projected.get_string(3).unwrap(), "4");

        let doubles = row_of(vec![
            Datum::Float64(0.5.into()),
            Datum::Float64(0.6.into()),
            Datum::Float64(0.7.into()),
            Datum::Float64(0.8.into()),
            Datum::Float64(0.9.into()),
            Datum::Float64(1.0.into()),
        ]);
        let projected = ProjectedRow::new(doubles, Arc::clone(&mapping));
        assert_eq!(projected.get_double(0).unwrap(), 0.7);
        assert_eq!(projected.get_double(1).unwrap(), 0.5);
        assert_eq!(projected.get_double(3).unwrap(), 0.9);
    }

    #[test]
    fn null_handling_passes_through_inner_nulls() {
        let mapping = mapping(&[2, 0, 1, 4]);
        let inner = row_of(vec![
            Datum::Int64(5),
            Datum::Int64(6),
            Datum::Null,
            Datum::Int64(8),
            Datum::Null,
            Datum::Int64(10),
        ]);
        let projected = ProjectedRow::new(inner, mapping);

        assert!(projected.is_null_at(0).unwrap());
        assert!(!projected.is_null_at(1).unwrap());
        assert!(!projected.is_null_at(2).unwrap());
        assert!(projected.is_null_at(3).unwrap());
    }

    #[test]
    fn unexist_mapping_reports_null_and_errors_on_get() {
        let mapping = mapping(&[0, 1, UNEXIST_MAPPING, 2]);
        let inner = row_of(vec![Datum::Int64(10), Datum::Int64(20), Datum::Int64(30)]);
        let projected = ProjectedRow::new(inner, mapping);

        assert_eq!(projected.get_field_count(), 4);
        assert_eq!(projected.get_long(0).unwrap(), 10);
        assert_eq!(projected.get_long(1).unwrap(), 20);
        assert!(projected.is_null_at(2).unwrap());
        let err = projected.get_long(2).unwrap_err();
        assert!(err.to_string().contains("does not exist"), "got: {err}");
        assert_eq!(projected.get_long(3).unwrap(), 30);
    }

    #[test]
    fn out_of_bounds_position_returns_error() {
        let mapping = mapping(&[0, 1]);
        let inner = row_of(vec![Datum::Int64(1), Datum::Int64(2)]);
        let projected = ProjectedRow::new(inner, mapping);

        let err = projected.is_null_at(5).unwrap_err();
        assert!(err.to_string().contains("out of bounds"), "got: {err}");
        let err = projected.get_long(5).unwrap_err();
        assert!(err.to_string().contains("out of bounds"), "got: {err}");
    }

    #[test]
    fn shared_mapping_can_back_many_rows() {
        let mapping = mapping(&[1, 0]);
        let row_a = ProjectedRow::new(
            row_of(vec![Datum::Int64(10), Datum::Int64(20)]),
            Arc::clone(&mapping),
        );
        let row_b = ProjectedRow::new(
            row_of(vec![Datum::Int64(30), Datum::Int64(40)]),
            Arc::clone(&mapping),
        );
        assert_eq!(row_a.get_long(0).unwrap(), 20);
        assert_eq!(row_a.get_long(1).unwrap(), 10);
        assert_eq!(row_b.get_long(0).unwrap(), 40);
        assert_eq!(row_b.get_long(1).unwrap(), 30);
    }
}
