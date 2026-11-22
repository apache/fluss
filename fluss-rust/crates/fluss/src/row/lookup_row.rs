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

//! Return type of [`crate::client::table::LookupResult`] getters: a row
//! decoded under the table's current schema, possibly via projection
//! over an older schema's bytes.

use crate::client::WriteFormat;
use crate::error::Result;
use crate::row::compacted::CompactedRow;
use crate::row::datum::{Date, Time, TimestampLtz, TimestampNtz};
use crate::row::projected_row::ProjectedRow;
use crate::row::view::{ArrayView, MapView, RowView};
use crate::row::{DataGetters, Decimal, InternalRow};

pub struct LookupRow<'a> {
    inner: Inner<'a>,
}

enum Inner<'a> {
    Raw(CompactedRow<'a>),
    Projected(ProjectedRow<CompactedRow<'a>>),
}

impl<'a> LookupRow<'a> {
    pub(crate) fn raw(row: CompactedRow<'a>) -> Self {
        Self {
            inner: Inner::Raw(row),
        }
    }

    pub(crate) fn projected(row: ProjectedRow<CompactedRow<'a>>) -> Self {
        Self {
            inner: Inner::Projected(row),
        }
    }
}

macro_rules! delegate {
    ($self:ident, $method:ident $(, $arg:expr)*) => {
        match &$self.inner {
            Inner::Raw(r) => r.$method($($arg),*),
            Inner::Projected(r) => r.$method($($arg),*),
        }
    };
}

impl<'a> InternalRow for LookupRow<'a> {
    fn get_field_count(&self) -> usize {
        delegate!(self, get_field_count)
    }

    fn as_encoded_bytes(&self, write_format: WriteFormat) -> Option<&[u8]> {
        delegate!(self, as_encoded_bytes, write_format)
    }
}

impl<'a> DataGetters for LookupRow<'a> {
    fn is_null_at(&self, pos: usize) -> Result<bool> {
        delegate!(self, is_null_at, pos)
    }
    fn get_boolean(&self, pos: usize) -> Result<bool> {
        delegate!(self, get_boolean, pos)
    }
    fn get_byte(&self, pos: usize) -> Result<i8> {
        delegate!(self, get_byte, pos)
    }
    fn get_short(&self, pos: usize) -> Result<i16> {
        delegate!(self, get_short, pos)
    }
    fn get_int(&self, pos: usize) -> Result<i32> {
        delegate!(self, get_int, pos)
    }
    fn get_long(&self, pos: usize) -> Result<i64> {
        delegate!(self, get_long, pos)
    }
    fn get_float(&self, pos: usize) -> Result<f32> {
        delegate!(self, get_float, pos)
    }
    fn get_double(&self, pos: usize) -> Result<f64> {
        delegate!(self, get_double, pos)
    }
    fn get_char(&self, pos: usize, length: usize) -> Result<&str> {
        delegate!(self, get_char, pos, length)
    }
    fn get_string(&self, pos: usize) -> Result<&str> {
        delegate!(self, get_string, pos)
    }
    fn get_decimal(&self, pos: usize, precision: usize, scale: usize) -> Result<Decimal> {
        delegate!(self, get_decimal, pos, precision, scale)
    }
    fn get_date(&self, pos: usize) -> Result<Date> {
        delegate!(self, get_date, pos)
    }
    fn get_time(&self, pos: usize) -> Result<Time> {
        delegate!(self, get_time, pos)
    }
    fn get_timestamp_ntz(&self, pos: usize, precision: u32) -> Result<TimestampNtz> {
        delegate!(self, get_timestamp_ntz, pos, precision)
    }
    fn get_timestamp_ltz(&self, pos: usize, precision: u32) -> Result<TimestampLtz> {
        delegate!(self, get_timestamp_ltz, pos, precision)
    }
    fn get_binary(&self, pos: usize, length: usize) -> Result<&[u8]> {
        delegate!(self, get_binary, pos, length)
    }
    fn get_bytes(&self, pos: usize) -> Result<&[u8]> {
        delegate!(self, get_bytes, pos)
    }
    fn get_array(&self, pos: usize) -> Result<ArrayView<'_>> {
        delegate!(self, get_array, pos)
    }
    fn get_map(&self, pos: usize) -> Result<MapView<'_>> {
        delegate!(self, get_map, pos)
    }
    fn get_row(&self, pos: usize) -> Result<RowView<'_>> {
        delegate!(self, get_row, pos)
    }
}
