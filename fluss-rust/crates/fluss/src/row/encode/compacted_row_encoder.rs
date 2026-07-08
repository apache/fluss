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
use crate::metadata::RowType;
use crate::row::Datum;
use crate::row::binary::{BinaryRowFormat, BinaryWriter, ValueWriter};
use crate::row::compacted::{CompactedRowDeserializer, CompactedRowWriter};
use crate::row::encode::RowEncoder;
use bytes::Bytes;
use std::sync::Arc;

#[allow(dead_code)]
pub struct CompactedRowEncoder<'a> {
    arity: usize,
    writer: CompactedRowWriter,
    field_writers: Vec<ValueWriter>,
    compacted_row_deserializer: Arc<CompactedRowDeserializer<'a>>,
}

impl<'a> CompactedRowEncoder<'a> {
    pub fn new(row_type: RowType) -> Result<Self> {
        let field_writers = row_type
            .field_types()
            .map(|d| ValueWriter::create_value_writer(d, Some(&BinaryRowFormat::Compacted)))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            arity: field_writers.len(),
            writer: CompactedRowWriter::new(field_writers.len()),
            field_writers,
            compacted_row_deserializer: Arc::new(CompactedRowDeserializer::new_from_owned(
                row_type,
            )),
        })
    }
}

impl RowEncoder for CompactedRowEncoder<'_> {
    fn start_new_row(&mut self) -> Result<()> {
        self.writer.reset();
        Ok(())
    }

    fn encode_field(&mut self, pos: usize, value: Datum) -> Result<()> {
        self.field_writers
            .get(pos)
            .ok_or_else(|| IllegalArgument {
                message: format!("invalid position {pos} when attempting to encode value {value}"),
            })?
            .write_value(&mut self.writer, pos, &value)
    }

    fn finish_row(&mut self) -> Result<Bytes> {
        Ok(self.writer.flush_bytes())
    }

    fn close(&mut self) -> Result<()> {
        // do nothing
        Ok(())
    }
}
