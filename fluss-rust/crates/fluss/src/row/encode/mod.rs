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

mod compacted_key_encoder;
mod compacted_row_encoder;
mod paimon_key_encoder;

use crate::error::{Error, Result};
use crate::metadata::{DataLakeFormat, KvFormat, RowType};
use crate::row::encode::compacted_key_encoder::CompactedKeyEncoder;
use crate::row::encode::compacted_row_encoder::CompactedRowEncoder;
use crate::row::encode::paimon_key_encoder::PaimonKeyEncoder;
use crate::row::{Datum, InternalRow};
use bytes::Bytes;

/// An interface for encoding key of row into bytes.
#[allow(dead_code)]
pub trait KeyEncoder: Send + Sync {
    fn encode_key(&mut self, row: &dyn InternalRow) -> Result<Bytes>;
}

pub struct KeyEncoderFactory;

impl KeyEncoderFactory {
    /// Create a key encoder for the primary key, mirroring Java's
    /// `KeyEncoder.ofPrimaryKeyEncoder`.
    ///
    /// Use this encoder when encoding the primary key for KV writes/lookups,
    /// or when encoding a prefix-lookup key (which is a strict prefix of the
    /// primary key and must share its byte layout to support lexicographic
    /// prefix matching).
    ///
    /// - `kv_format_version == 2` with `is_default_bucket_key == false`
    ///   (i.e. the primary key differs from the bucket key) →
    ///   `CompactedKeyEncoder::create_key_encoder` regardless of lake format
    ///   (primary key must be decodable by the server without lake-specific
    ///   format awareness).
    /// - Otherwise (`kv_format_version == 1`, or `kv_format_version == 2`
    ///   with `is_default_bucket_key == true` so the primary key matches the
    ///   bucket key) → align with the lake format via `Self::of`, producing
    ///   the same physical layout as the bucket key.
    /// - Any other `kv_format_version` → [`Error::UnsupportedOperation`].
    pub fn of_primary_key_encoder(
        row_type: &RowType,
        key_fields: &[String],
        data_lake_format: &Option<DataLakeFormat>,
        kv_format_version: i32,
        is_default_bucket_key: bool,
    ) -> Result<Box<dyn KeyEncoder>> {
        match kv_format_version {
            1 => Self::of(row_type, key_fields, data_lake_format),
            2 if is_default_bucket_key => Self::of(row_type, key_fields, data_lake_format),
            2 => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
            v => Err(Error::UnsupportedOperation {
                message: format!("Unsupported kv format version: {v}"),
            }),
        }
    }

    /// Create a key encoder for the bucket key, mirroring Java's
    /// `KeyEncoder.ofBucketKeyEncoder`.
    ///
    /// Use this encoder when computing the bucket id for a row. The bucket
    /// key encoding is bound to the configured data lake format (delegating
    /// to `Self::of`) because the resulting bucket id must stay consistent
    /// with the bucket id computed by the downstream lake (e.g. Paimon /
    /// Lance / Iceberg); otherwise the same row would land in different
    /// buckets on the Fluss side and on the lake side, breaking lake-tiering
    /// semantics.
    pub fn of_bucket_key_encoder(
        row_type: &RowType,
        key_fields: &[String],
        data_lake_format: &Option<DataLakeFormat>,
    ) -> Result<Box<dyn KeyEncoder>> {
        Self::of(row_type, key_fields, data_lake_format)
    }

    /// Create a key encoder to encode the key bytes of the input row.
    /// # Arguments
    /// * `row_type` - the row type of the input row
    /// * `key_fields` - the key fields to encode
    /// * `lake_format` - the data lake format
    ///
    /// # Returns
    /// key encoder
    fn of(
        row_type: &RowType,
        key_fields: &[String],
        data_lake_format: &Option<DataLakeFormat>,
    ) -> Result<Box<dyn KeyEncoder>> {
        match data_lake_format {
            Some(DataLakeFormat::Paimon) => {
                Ok(Box::new(PaimonKeyEncoder::new(row_type, key_fields)?))
            }
            Some(DataLakeFormat::Lance) => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
            Some(DataLakeFormat::Iceberg) => Err(Error::UnsupportedOperation {
                message: "KeyEncoder for Iceberg format is not yet implemented".to_string(),
            }),
            None => Ok(Box::new(CompactedKeyEncoder::create_key_encoder(
                row_type, key_fields,
            )?)),
        }
    }
}

/// An encoder to write binary row data. It's used to write rows
/// one by one. When writing a new row:
///
/// 1. call method [`RowEncoder::start_new_row()`] to start the writing.
/// 2. call method [`RowEncoder::encode_field()`] to write the row's field.
/// 3. call method [`RowEncoder::finish_row()`] to finish the writing and get the written row.
#[allow(dead_code)]
pub trait RowEncoder: Send + Sync {
    /// Start to write a new row.
    ///
    /// # Returns
    /// * Ok(()) if successful
    fn start_new_row(&mut self) -> Result<()>;

    /// Write the row's field in given pos with given value.
    ///
    /// # Arguments
    /// * pos - the position of the field to write.
    /// * value - the value of the field to write.
    ///
    /// # Returns
    /// * Ok(()) if successful
    fn encode_field(&mut self, pos: usize, value: Datum) -> Result<()>;

    /// Finish write the row, returns the written row.
    ///
    /// Note that returned row borrows from [`RowEncoder`]'s internal buffer which is reused for subsequent rows
    /// [`RowEncoder::start_new_row()`] should only be called after the returned row goes out of scope.
    ///
    /// # Returns
    /// * the written row
    fn finish_row(&mut self) -> Result<Bytes>;

    /// Closes the row encoder
    ///
    /// # Returns
    /// * Ok(()) if successful
    fn close(&mut self) -> Result<()>;
}

#[allow(dead_code)]
pub struct RowEncoderFactory {}

#[allow(dead_code)]
impl RowEncoderFactory {
    pub fn create(kv_format: KvFormat, row_type: RowType) -> Result<impl RowEncoder> {
        Self::create_for_field_types(kv_format, row_type)
    }

    pub fn create_for_field_types(
        kv_format: KvFormat,
        row_type: RowType,
    ) -> Result<impl RowEncoder> {
        match kv_format {
            KvFormat::INDEXED => {
                todo!()
            }
            KvFormat::COMPACTED => CompactedRowEncoder::new(row_type),
        }
    }
}
