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

use crate::row::compacted::compacted_row_writer::CompactedRowWriter;
use bytes::Bytes;

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::DataType;
use crate::row::Decimal;
use crate::row::binary::{BinaryRowFormat, BinaryWriter, ValueWriter};
use crate::row::binary_array::FlussArray;
use crate::row::binary_map::FlussMap;
use crate::row::datum::{TimestampLtz, TimestampNtz};
use delegate::delegate;

/// A wrapping of [`CompactedRowWriter`] used to encode key columns.
/// The encoding is the same as [`CompactedRowWriter`], but is without header of null bits to
/// represent whether the field value is null or not since the key columns must be not null.
pub struct CompactedKeyWriter {
    delegate: CompactedRowWriter,
}

impl Default for CompactedKeyWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactedKeyWriter {
    pub fn new() -> CompactedKeyWriter {
        CompactedKeyWriter {
            // in compacted key encoder, we don't need to set null bits as the key columns must be not
            // null, to use field count 0 to init to make the null bits 0
            delegate: CompactedRowWriter::new(0),
        }
    }

    pub fn create_value_writer(field_type: &DataType) -> Result<ValueWriter> {
        if matches!(field_type, DataType::Map(_)) {
            return Err(IllegalArgument {
                message: format!("Cannot use {field_type:?} as a key column type"),
            });
        }
        ValueWriter::create_value_writer(field_type, Some(&BinaryRowFormat::Compacted))
    }

    delegate! {
        to self.delegate {
            pub fn reset(&mut self);

            #[allow(dead_code)]
            pub fn position(&self) -> usize;

            #[allow(dead_code)]
            pub fn buffer(&self) -> &[u8];

            pub fn to_bytes(&self) -> Bytes;
        }
    }
}

impl BinaryWriter for CompactedKeyWriter {
    delegate! {
        to self.delegate {
            fn reset(&mut self);

            fn set_null_at(&mut self, pos: usize);

            fn write_boolean(&mut self, value: bool);

            fn write_byte(&mut self, value: u8);

            fn write_binary(&mut self, bytes: &[u8], length: usize);

            fn write_bytes(&mut self, value: &[u8]);

            fn write_char(&mut self, value: &str, _length: usize);

            fn write_string(&mut self, value: &str);

            fn write_short(&mut self, value: i16);

            fn write_int(&mut self, value: i32);

            fn write_long(&mut self, value: i64);

            fn write_float(&mut self, value: f32);

            fn write_double(&mut self, value: f64);

            fn write_decimal(&mut self, value: &Decimal, precision: u32);

            fn write_time(&mut self, value: i32, precision: u32);

            fn write_timestamp_ntz(&mut self, value: &TimestampNtz, precision: u32);

            fn write_timestamp_ltz(&mut self, value: &TimestampLtz, precision: u32);

            fn write_array(&mut self, value: &FlussArray);

            fn write_map(&mut self, value: &FlussMap);
        }
    }

    fn complete(&mut self) {
        // do nothing
    }
}
