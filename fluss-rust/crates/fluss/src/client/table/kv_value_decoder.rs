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

//! Shared decoding of KV value payloads.
//!
//! Both primary-key / prefix lookups and the bounded KV batch scanner receive
//! values laid out as `[schema_id (i16 LE) | row bytes]`. Each payload is
//! decoded with the schema it was written under and projected onto a fixed
//! target schema. [`KvValueDecoder`] owns that logic so both paths share the
//! same schema id validation, decoder cache and Arrow batch building.

use crate::client::ClientSchemaGetter;
use crate::error::{Error, Result};
use crate::metadata::{KvFormat, RowType, Schema, TableInfo};
use crate::record::RowAppendRecordBatchBuilder;
use crate::record::kv::SCHEMA_ID_LENGTH;
use crate::row::{FixedSchemaDecoder, LookupRow};
use arrow::array::RecordBatch;
use byteorder::{ByteOrder, LittleEndian};
use futures::future::try_join_all;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Read the leading little-endian schema id from a `[schema_id | row]` payload.
pub(crate) fn read_schema_id(payload: &[u8]) -> Result<i16> {
    if payload.len() < SCHEMA_ID_LENGTH {
        return Err(Error::RowConvertError {
            message: format!(
                "Row payload too short: {} bytes, need at least {} for schema id",
                payload.len(),
                SCHEMA_ID_LENGTH
            ),
        });
    }
    let schema_id = LittleEndian::read_i16(&payload[..SCHEMA_ID_LENGTH]);
    if schema_id < 0 {
        return Err(Error::RowConvertError {
            message: format!("Invalid negative schema id {schema_id}; row prefix is corrupt"),
        });
    }
    Ok(schema_id)
}

/// Decodes KV value payloads into rows of a fixed target schema.
///
/// The target-schema decoder is held directly so the dominant decode path is a
/// single field access; decoders for older schemas are built on demand by
/// [`ensure_decoders`](Self::ensure_decoders) and cached for later calls.
pub(crate) struct KvValueDecoder {
    kv_format: KvFormat,
    target_schema: Arc<Schema>,
    target_id: i16,
    target_decoder: Arc<FixedSchemaDecoder>,
    others: RwLock<HashMap<i16, Arc<FixedSchemaDecoder>>>,
}

impl KvValueDecoder {
    /// Build a decoder targeting the table's current schema.
    pub(crate) fn for_table(table_info: &TableInfo) -> Result<Self> {
        let target_schema_id = table_info.get_schema_id();
        if !(0..=i16::MAX as i32).contains(&target_schema_id) {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Schema id {target_schema_id} does not fit in 16 bits — wire format violated"
                ),
                source: None,
            });
        }
        let target_id = target_schema_id as i16;
        let kv_format = table_info.get_table_config().get_kv_format()?;
        Self::new(
            kv_format,
            Arc::new(table_info.get_schema().clone()),
            target_id,
        )
    }

    pub(crate) fn new(
        kv_format: KvFormat,
        target_schema: Arc<Schema>,
        target_id: i16,
    ) -> Result<Self> {
        let target_decoder = Arc::new(FixedSchemaDecoder::new_no_projection(
            kv_format,
            target_schema.as_ref(),
        )?);
        Ok(Self {
            kv_format,
            target_schema,
            target_id,
            target_decoder,
            others: RwLock::new(HashMap::new()),
        })
    }

    fn contains(&self, schema_id: i16) -> bool {
        schema_id == self.target_id || self.others.read().contains_key(&schema_id)
    }

    fn insert(&self, schema_id: i16, decoder: Arc<FixedSchemaDecoder>) {
        self.others.write().insert(schema_id, decoder);
    }

    fn decoder_for(&self, schema_id: i16) -> Result<Arc<FixedSchemaDecoder>> {
        if schema_id == self.target_id {
            return Ok(Arc::clone(&self.target_decoder));
        }
        self.others
            .read()
            .get(&schema_id)
            .cloned()
            .ok_or_else(|| Error::RowConvertError {
                message: format!("No decoder available for schema id {schema_id}"),
            })
    }

    /// Make sure a decoder exists for every schema id referenced by `payloads`,
    /// fetching missing source schemas concurrently.
    pub(crate) async fn ensure_decoders<'a, I>(
        &self,
        schema_getter: &ClientSchemaGetter,
        payloads: I,
    ) -> Result<()>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut missing: Vec<i16> = Vec::new();
        for payload in payloads {
            let schema_id = read_schema_id(payload)?;
            if !self.contains(schema_id) && !missing.contains(&schema_id) {
                missing.push(schema_id);
            }
        }
        if missing.is_empty() {
            return Ok(());
        }

        let fetches = missing.into_iter().map(|schema_id| async move {
            let source = schema_getter.get_schema(schema_id as i32).await?;
            let decoder = FixedSchemaDecoder::new(
                self.kv_format,
                source.as_ref(),
                self.target_schema.as_ref(),
            )?;
            self.insert(schema_id, Arc::new(decoder));
            Ok::<_, Error>(())
        });
        try_join_all(fetches).await?;
        Ok(())
    }

    /// Decode a single `[schema_id | row]` payload. The decoder for its schema
    /// id must already be present (see [`ensure_decoders`](Self::ensure_decoders)).
    pub(crate) fn decode<'a>(&self, payload: &'a [u8]) -> Result<LookupRow<'a>> {
        let schema_id = read_schema_id(payload)?;
        if schema_id == self.target_id {
            return self.target_decoder.decode(payload);
        }
        self.decoder_for(schema_id)?.decode(payload)
    }

    /// Decode all `payloads` into one Arrow batch shaped by `target_row_type`.
    pub(crate) fn decode_to_record_batch<'a, I>(
        &self,
        payloads: I,
        target_row_type: &RowType,
    ) -> Result<RecordBatch>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        let mut builder = RowAppendRecordBatchBuilder::new(target_row_type)?;
        for payload in payloads {
            let row = self.decode(payload)?;
            builder.append(&row)?;
        }
        builder.build_arrow_record_batch().map(Arc::unwrap_or_clone)
    }

    /// Register a pre-built decoder for an older schema id.
    #[cfg(test)]
    pub(crate) fn with_decoder(self, schema_id: i16, decoder: FixedSchemaDecoder) -> Self {
        self.insert(schema_id, Arc::new(decoder));
        self
    }

    #[cfg(test)]
    pub(crate) fn get(&self, schema_id: i16) -> Option<Arc<FixedSchemaDecoder>> {
        self.decoder_for(schema_id).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Column, DataTypes};

    fn schema_with_ids(columns: &[(i32, &str, crate::metadata::DataType)]) -> Schema {
        let cols: Vec<Column> = columns
            .iter()
            .map(|(id, name, dt)| Column::new(*name, dt.clone()).with_id(*id))
            .collect();
        Schema::builder().with_columns(cols).build().unwrap()
    }

    #[test]
    fn read_schema_id_rejects_short_and_negative() {
        let err = read_schema_id(&[0u8]).unwrap_err().to_string();
        assert!(err.contains("too short"), "{err}");
        let err = read_schema_id(&[0xFF, 0xFF, 0]).unwrap_err().to_string();
        assert!(err.contains("Invalid negative schema id"), "{err}");
        assert_eq!(read_schema_id(&[7, 0, 1, 2]).unwrap(), 7);
    }

    #[test]
    fn target_decoder_is_shared_and_unknown_ids_error() {
        let target = Arc::new(schema_with_ids(&[(0, "a", DataTypes::int())]));
        let decoder = KvValueDecoder::new(KvFormat::COMPACTED, target, 7).unwrap();
        let first = decoder.get(7).expect("target id must be present");
        let second = decoder.get(7).expect("target id must be present");
        assert!(Arc::ptr_eq(&first, &second));
        assert!(decoder.get(99).is_none());

        let err = decoder
            .decode(&[99, 0, 0, 0])
            .map(|_| ())
            .unwrap_err()
            .to_string();
        assert!(err.contains("schema id 99"), "{err}");
    }
}
