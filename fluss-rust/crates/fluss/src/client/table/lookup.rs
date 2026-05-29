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

use crate::bucketing::BucketingFunction;
use crate::client::ClientSchemaGetter;
use crate::client::lookup::LookupClient;
use crate::client::metadata::Metadata;
use crate::client::table::partition_getter::PartitionGetter;
use crate::error::{Error, Result};
use crate::metadata::{
    KvFormat, PhysicalTablePath, RowType, Schema, TableBucket, TableInfo, TablePath,
};
use crate::record::RowAppendRecordBatchBuilder;
use crate::record::kv::SCHEMA_ID_LENGTH;
use crate::row::encode::{KeyEncoder, KeyEncoderFactory};
use crate::row::{FixedSchemaDecoder, InternalRow, LookupRow};
use arrow::array::RecordBatch;
use byteorder::{ByteOrder, LittleEndian};
use futures::future::try_join_all;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-Lookuper decoder cache. The target-schema decoder is held
/// directly so the dominant decode path is a single field access; older
/// schemas are populated lazily on first observation.
struct DecoderCache {
    target_id: i16,
    target_decoder: Arc<FixedSchemaDecoder>,
    others: RwLock<HashMap<i16, Arc<FixedSchemaDecoder>>>,
}

impl DecoderCache {
    fn new(target_id: i16, target_decoder: Arc<FixedSchemaDecoder>) -> Self {
        Self {
            target_id,
            target_decoder,
            others: RwLock::new(HashMap::new()),
        }
    }

    fn decode<'a>(&self, schema_id: i16, bytes: &'a [u8]) -> Result<LookupRow<'a>> {
        if schema_id == self.target_id {
            return self.target_decoder.decode(bytes);
        }
        let decoder =
            self.others
                .read()
                .get(&schema_id)
                .cloned()
                .ok_or_else(|| Error::RowConvertError {
                    message: format!("No decoder available for schema id {schema_id}"),
                })?;
        decoder.decode(bytes)
    }

    fn contains(&self, schema_id: i16) -> bool {
        schema_id == self.target_id || self.others.read().contains_key(&schema_id)
    }

    fn insert(&self, schema_id: i16, decoder: Arc<FixedSchemaDecoder>) {
        self.others.write().insert(schema_id, decoder);
    }

    #[cfg(test)]
    fn get(&self, schema_id: i16) -> Option<Arc<FixedSchemaDecoder>> {
        if schema_id == self.target_id {
            return Some(Arc::clone(&self.target_decoder));
        }
        self.others.read().get(&schema_id).cloned()
    }
}

/// Rows returned from a lookup. Primary-key lookups produce at most one
/// row; prefix-key lookups may produce many. Rows written under older
/// schemas are decoded with their original schema and projected to the
/// schema captured when the `Lookuper` was created — schema evolutions
/// that land after that point are not picked up by an existing
/// `Lookuper`; create a new one to see them.
pub struct LookupResult {
    rows: Vec<Vec<u8>>,
    target_row_type: Arc<RowType>,
    decoders: Arc<DecoderCache>,
}

impl LookupResult {
    fn new(rows: Vec<Vec<u8>>, target_row_type: Arc<RowType>, decoders: Arc<DecoderCache>) -> Self {
        Self {
            rows,
            target_row_type,
            decoders,
        }
    }

    fn read_schema_id(bytes: &[u8]) -> Result<i16> {
        if bytes.len() < SCHEMA_ID_LENGTH {
            return Err(Error::RowConvertError {
                message: format!(
                    "Row payload too short: {} bytes, need at least {} for schema id",
                    bytes.len(),
                    SCHEMA_ID_LENGTH
                ),
            });
        }
        let schema_id = LittleEndian::read_i16(&bytes[..SCHEMA_ID_LENGTH]);
        if schema_id < 0 {
            return Err(Error::RowConvertError {
                message: format!("Invalid negative schema id {schema_id}; row prefix is corrupt"),
            });
        }
        Ok(schema_id)
    }

    fn decode<'a>(&self, bytes: &'a [u8]) -> Result<LookupRow<'a>> {
        let schema_id = Self::read_schema_id(bytes)?;
        self.decoders.decode(schema_id, bytes)
    }

    /// Returns the single row when exactly one is present, `None` for
    /// empty, or an error if the result holds more than one row.
    pub fn get_single_row(&self) -> Result<Option<LookupRow<'_>>> {
        match self.rows.len() {
            0 => Ok(None),
            1 => Ok(Some(self.decode(&self.rows[0])?)),
            _ => Err(Error::UnexpectedError {
                message: "LookupResult contains multiple rows, use get_rows() instead".to_string(),
                source: None,
            }),
        }
    }

    pub fn get_rows(&self) -> Result<Vec<LookupRow<'_>>> {
        self.rows.iter().map(|bytes| self.decode(bytes)).collect()
    }

    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        let mut builder = RowAppendRecordBatchBuilder::new(&self.target_row_type)?;
        for bytes in &self.rows {
            let row = self.decode(bytes)?;
            builder.append(&row)?;
        }
        builder.build_arrow_record_batch().map(Arc::unwrap_or_clone)
    }
}

struct LookupSchemaCtx {
    target_schema: Arc<Schema>,
    target_row_type: Arc<RowType>,
    kv_format: KvFormat,
    schema_getter: Arc<ClientSchemaGetter>,
    decoders: Arc<DecoderCache>,
}

impl LookupSchemaCtx {
    fn new(table_info: &TableInfo, schema_getter: Arc<ClientSchemaGetter>) -> Result<Self> {
        let target_schema_i32 = table_info.get_schema_id();
        if !(0..=i16::MAX as i32).contains(&target_schema_i32) {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Schema id {target_schema_i32} does not fit in 16 bits — wire format violated"
                ),
                source: None,
            });
        }
        let target_schema = Arc::new(table_info.get_schema().clone());
        let target_row_type = Arc::new(table_info.row_type().clone());
        let kv_format = table_info.get_table_config().get_kv_format()?;
        let target_decoder = Arc::new(FixedSchemaDecoder::new_no_projection(
            kv_format,
            target_schema.as_ref(),
        )?);
        let decoders = Arc::new(DecoderCache::new(target_schema_i32 as i16, target_decoder));
        Ok(Self {
            target_schema,
            target_row_type,
            kv_format,
            schema_getter,
            decoders,
        })
    }

    async fn ensure_decoders(&self, rows: &[Vec<u8>]) -> Result<()> {
        let mut missing: Vec<i16> = Vec::new();
        for bytes in rows {
            let schema_id = LookupResult::read_schema_id(bytes)?;
            if !self.decoders.contains(schema_id) && !missing.contains(&schema_id) {
                missing.push(schema_id);
            }
        }
        if missing.is_empty() {
            return Ok(());
        }

        let fetches = missing.into_iter().map(|schema_id| {
            let cache = Arc::clone(&self.decoders);
            let schema_getter = Arc::clone(&self.schema_getter);
            let target_schema = Arc::clone(&self.target_schema);
            let kv_format = self.kv_format;
            async move {
                let source = schema_getter.get_schema(schema_id as i32).await?;
                let decoder =
                    FixedSchemaDecoder::new(kv_format, source.as_ref(), target_schema.as_ref())?;
                cache.insert(schema_id, Arc::new(decoder));
                Ok::<_, Error>(())
            }
        });
        try_join_all(fetches).await?;
        Ok(())
    }

    async fn build_result(&self, rows: Vec<Vec<u8>>) -> Result<LookupResult> {
        if !rows.is_empty() {
            self.ensure_decoders(&rows).await?;
        }
        Ok(LookupResult::new(
            rows,
            Arc::clone(&self.target_row_type),
            Arc::clone(&self.decoders),
        ))
    }

    fn empty_result(&self) -> LookupResult {
        LookupResult::new(
            Vec::new(),
            Arc::clone(&self.target_row_type),
            Arc::clone(&self.decoders),
        )
    }
}

/// Builder for lookup operations. `create_lookuper()` builds a primary-key
/// `Lookuper`; `lookup_by(columns).create_lookuper()` builds a
/// `PrefixKeyLookuper` for prefix scans.
// TODO: Add create_typed_lookuper<T>() for typed lookups with POJO mapping
pub struct TableLookup {
    lookup_client: Arc<LookupClient>,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
    schema_getter: Arc<ClientSchemaGetter>,
}

impl TableLookup {
    pub(super) fn new(
        lookup_client: Arc<LookupClient>,
        table_info: TableInfo,
        metadata: Arc<Metadata>,
        schema_getter: Arc<ClientSchemaGetter>,
    ) -> Self {
        Self {
            lookup_client,
            table_info,
            metadata,
            schema_getter,
        }
    }

    /// Switches the builder into prefix-scan mode. `lookup_column_names`
    /// must list the table's partition keys (if any) plus the bucket keys,
    /// in that order — i.e. this is a **bucket-key prefix** scan, not an
    /// arbitrary primary-key prefix. Validation is deferred to
    /// `create_lookuper()`.
    pub fn lookup_by(self, lookup_column_names: Vec<String>) -> TablePrefixLookup {
        TablePrefixLookup {
            lookup_client: self.lookup_client,
            table_info: self.table_info,
            metadata: self.metadata,
            schema_getter: self.schema_getter,
            lookup_column_names,
        }
    }

    /// Creates a `Lookuper` for performing key-based lookups.
    ///
    /// The lookuper will automatically encode the key and compute the bucket
    /// for each lookup using the appropriate bucketing function.
    ///
    /// The lookuper uses a shared `LookupClient` that batches multiple lookup
    /// operations together to reduce network round trips. This achieves parity
    /// with the Java client implementation for improved throughput.
    pub fn create_lookuper(self) -> Result<Lookuper> {
        let num_buckets = self.table_info.get_num_buckets();

        // Get data lake format from table config for bucketing function
        let data_lake_format = self.table_info.get_table_config().get_datalake_format()?;
        let bucketing_function = <dyn BucketingFunction>::of(data_lake_format.as_ref());

        let row_type = self.table_info.row_type();
        let primary_keys = self.table_info.get_primary_keys();
        let lookup_row_type = row_type.project_with_field_names(primary_keys)?;

        let physical_primary_keys = self.table_info.get_physical_primary_keys().to_vec();
        let primary_key_encoder =
            KeyEncoderFactory::of(&lookup_row_type, &physical_primary_keys, &data_lake_format)?;

        let bucket_key_encoder = if self.table_info.is_default_bucket_key() {
            None
        } else {
            let bucket_keys = self.table_info.get_bucket_keys().to_vec();
            Some(KeyEncoderFactory::of(
                &lookup_row_type,
                &bucket_keys,
                &data_lake_format,
            )?)
        };

        let partition_getter = if self.table_info.is_partitioned() {
            Some(PartitionGetter::new(
                &lookup_row_type,
                Arc::clone(self.table_info.get_partition_keys()),
            )?)
        } else {
            None
        };

        let schema_ctx = LookupSchemaCtx::new(&self.table_info, self.schema_getter)?;

        Ok(Lookuper {
            table_path: Arc::new(self.table_info.table_path.clone()),
            table_info: self.table_info,
            metadata: self.metadata,
            lookup_client: self.lookup_client,
            bucketing_function,
            primary_key_encoder,
            bucket_key_encoder,
            partition_getter,
            num_buckets,
            schema_ctx,
        })
    }
}

/// Performs key-based lookups against a primary key table.
///
/// The `Lookuper` automatically encodes the lookup key, computes the target
/// bucket, and retrieves the value using the batched `LookupClient`.
///
/// # Example
/// ```ignore
/// let lookuper = table.new_lookup()?.create_lookuper()?;
/// let row = GenericRow::new(vec![Datum::Int32(42)]); // lookup key
/// let result = lookuper.lookup(&row).await?;
/// ```
pub struct Lookuper {
    table_path: Arc<TablePath>,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
    lookup_client: Arc<LookupClient>,
    bucketing_function: Box<dyn BucketingFunction>,
    primary_key_encoder: Box<dyn KeyEncoder>,
    bucket_key_encoder: Option<Box<dyn KeyEncoder>>,
    partition_getter: Option<PartitionGetter>,
    num_buckets: i32,
    schema_ctx: LookupSchemaCtx,
}

impl Lookuper {
    /// Looks up a value by its primary key.
    ///
    /// The key is encoded and the bucket is automatically computed using
    /// the table's bucketing function. The lookup is queued and batched
    /// with other lookups for improved throughput.
    ///
    /// # Arguments
    /// * `row` - The row containing the primary key field values
    ///
    /// # Returns
    /// * `Ok(LookupResult)` - The lookup result (may be empty if key not found)
    /// * `Err(Error)` - If the lookup fails
    pub async fn lookup(&mut self, row: &dyn InternalRow) -> Result<LookupResult> {
        let pk_bytes = self.primary_key_encoder.encode_key(row)?;
        let bk_bytes = match &mut self.bucket_key_encoder {
            Some(encoder) => encoder.encode_key(row)?,
            None => pk_bytes.clone(),
        };

        let partition_id = if let Some(ref partition_getter) = self.partition_getter {
            let partition_name = partition_getter.get_partition(row)?;
            let physical_table_path = PhysicalTablePath::of_partitioned(
                Arc::clone(&self.table_path),
                Some(partition_name),
            );
            match self
                .metadata
                .check_and_update_partition_metadata(&physical_table_path)
                .await?
            {
                Some(id) => Some(id),
                None => return Ok(self.schema_ctx.empty_result()),
            }
        } else {
            None
        };

        let bucket_id = self
            .bucketing_function
            .bucketing(&bk_bytes, self.num_buckets)?;

        let table_id = self.table_info.get_table_id();
        let table_bucket = TableBucket::new_with_partition(table_id, partition_id, bucket_id);

        // Use the batched lookup client
        let result = self
            .lookup_client
            .lookup(self.table_path.as_ref().clone(), table_bucket, pk_bytes)
            .await?;

        let rows = match result {
            Some(value_bytes) => vec![value_bytes],
            None => Vec::new(),
        };
        self.schema_ctx.build_result(rows).await
    }

    /// Returns a reference to the table info.
    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }
}

pub struct TablePrefixLookup {
    lookup_client: Arc<LookupClient>,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
    schema_getter: Arc<ClientSchemaGetter>,
    lookup_column_names: Vec<String>,
}

impl TablePrefixLookup {
    pub fn create_lookuper(self) -> Result<PrefixKeyLookuper> {
        validate_prefix_lookup(&self.table_info, &self.lookup_column_names)?;

        let num_buckets = self.table_info.get_num_buckets();
        let data_lake_format = self.table_info.get_table_config().get_datalake_format()?;
        let bucketing_function = <dyn BucketingFunction>::of(data_lake_format.as_ref());

        let row_type = self.table_info.row_type();
        let lookup_row_type = row_type.project_with_field_names(&self.lookup_column_names)?;

        let bucket_keys = self.table_info.get_bucket_keys().to_vec();
        let prefix_key_encoder =
            KeyEncoderFactory::of(&lookup_row_type, &bucket_keys, &data_lake_format)?;

        let partition_getter = if self.table_info.is_partitioned() {
            Some(PartitionGetter::new(
                &lookup_row_type,
                Arc::clone(self.table_info.get_partition_keys()),
            )?)
        } else {
            None
        };

        let schema_ctx = LookupSchemaCtx::new(&self.table_info, self.schema_getter)?;

        Ok(PrefixKeyLookuper {
            table_path: Arc::new(self.table_info.table_path.clone()),
            table_info: self.table_info,
            metadata: self.metadata,
            lookup_client: self.lookup_client,
            bucketing_function,
            prefix_key_encoder,
            partition_getter,
            num_buckets,
            schema_ctx,
        })
    }
}

fn validate_prefix_lookup(table_info: &TableInfo, lookup_columns: &[String]) -> Result<()> {
    if !table_info.has_primary_key() {
        return Err(Error::IllegalArgument {
            message: format!(
                "Log table {} doesn't support prefix lookup",
                table_info.get_table_path()
            ),
        });
    }

    let physical_primary_keys = table_info.get_physical_primary_keys();
    let bucket_keys = table_info.get_bucket_keys();

    if bucket_keys.is_empty() {
        return Err(Error::IllegalArgument {
            message: format!(
                "Can not perform prefix lookup on table '{}', because it has no bucket keys.",
                table_info.get_table_path()
            ),
        });
    }

    if !physical_primary_keys.starts_with(bucket_keys) {
        return Err(Error::IllegalArgument {
            message: format!(
                "Can not perform prefix lookup on table '{}', because the bucket keys {:?} \
                 is not a prefix subset of the physical primary keys {:?} \
                 (excluded partition fields if present).",
                table_info.get_table_path(),
                bucket_keys,
                physical_primary_keys,
            ),
        });
    }

    let partition_keys: &[String] = table_info.get_partition_keys();
    if table_info.is_partitioned() {
        for pk in partition_keys {
            if !lookup_columns.iter().any(|c| c == pk) {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Can not perform prefix lookup on table '{}', because the lookup columns \
                         {:?} must contain all partition fields {:?}.",
                        table_info.get_table_path(),
                        lookup_columns,
                        partition_keys,
                    ),
                });
            }
        }
    }

    let physical_lookup_columns: Vec<&String> = lookup_columns
        .iter()
        .filter(|c| !partition_keys.iter().any(|p| p == *c))
        .collect();
    if physical_lookup_columns.len() != bucket_keys.len()
        || !physical_lookup_columns
            .iter()
            .zip(bucket_keys.iter())
            .all(|(a, b)| *a == b)
    {
        return Err(Error::IllegalArgument {
            message: format!(
                "Can not perform prefix lookup on table '{}', because the lookup columns {:?} \
                 must contain all bucket keys {:?} in order.",
                table_info.get_table_path(),
                lookup_columns,
                bucket_keys,
            ),
        });
    }

    if bucket_keys == physical_primary_keys {
        return Err(Error::IllegalArgument {
            message: format!(
                "Can not perform prefix lookup on table '{}', because the lookup columns {:?} \
                 equals the physical primary keys {:?}. \
                 Please use primary key lookup (Lookuper without lookup_by) instead.",
                table_info.get_table_path(),
                lookup_columns,
                physical_primary_keys,
            ),
        });
    }

    Ok(())
}

pub struct PrefixKeyLookuper {
    table_path: Arc<TablePath>,
    table_info: TableInfo,
    metadata: Arc<Metadata>,
    lookup_client: Arc<LookupClient>,
    bucketing_function: Box<dyn BucketingFunction>,
    prefix_key_encoder: Box<dyn KeyEncoder>,
    partition_getter: Option<PartitionGetter>,
    num_buckets: i32,
    schema_ctx: LookupSchemaCtx,
}

impl PrefixKeyLookuper {
    pub async fn lookup(&mut self, row: &dyn InternalRow) -> Result<LookupResult> {
        let prefix_bytes = self.prefix_key_encoder.encode_key(row)?;

        let partition_id = if let Some(ref partition_getter) = self.partition_getter {
            let partition_name = partition_getter.get_partition(row)?;
            let physical_table_path = PhysicalTablePath::of_partitioned(
                Arc::clone(&self.table_path),
                Some(partition_name),
            );
            match self
                .metadata
                .check_and_update_partition_metadata(&physical_table_path)
                .await?
            {
                Some(id) => Some(id),
                None => return Ok(self.schema_ctx.empty_result()),
            }
        } else {
            None
        };

        let bucket_id = self
            .bucketing_function
            .bucketing(&prefix_bytes, self.num_buckets)?;

        let table_id = self.table_info.get_table_id();
        let table_bucket = TableBucket::new_with_partition(table_id, partition_id, bucket_id);

        let rows = self
            .lookup_client
            .prefix_lookup(self.table_path.as_ref().clone(), table_bucket, prefix_bytes)
            .await?;

        self.schema_ctx.build_result(rows).await
    }

    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Column, DataTypes, Schema};
    use crate::row::binary::BinaryWriter;
    use crate::row::compacted::CompactedRowWriter;
    use arrow::array::Int32Array;

    fn make_row_bytes(schema_id: i16, row_data: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(SCHEMA_ID_LENGTH + row_data.len());
        bytes.extend_from_slice(&schema_id.to_le_bytes());
        bytes.extend_from_slice(row_data);
        bytes
    }

    fn schema_with_ids(columns: &[(i32, &str, crate::metadata::DataType)]) -> Schema {
        let cols: Vec<Column> = columns
            .iter()
            .map(|(id, name, dt)| Column::new(*name, dt.clone()).with_id(*id))
            .collect();
        Schema::builder().with_columns(cols).build().unwrap()
    }

    fn cache_with(
        target_id: i16,
        target_decoder: FixedSchemaDecoder,
        others: Vec<(i16, FixedSchemaDecoder)>,
    ) -> Arc<DecoderCache> {
        let cache = DecoderCache::new(target_id, Arc::new(target_decoder));
        for (id, decoder) in others {
            cache.insert(id, Arc::new(decoder));
        }
        Arc::new(cache)
    }

    fn lookup_result_from(
        rows: Vec<Vec<u8>>,
        target_schema: &Schema,
        decoders: Arc<DecoderCache>,
    ) -> LookupResult {
        LookupResult::new(rows, Arc::new(target_schema.row_type().clone()), decoders)
    }

    #[test]
    fn test_to_record_batch_empty() {
        let target = schema_with_ids(&[(0, "id", DataTypes::int())]);
        let decoder = FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &target).unwrap();
        let result = lookup_result_from(Vec::new(), &target, cache_with(0, decoder, vec![]));
        let batch = result.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
    }

    #[test]
    fn test_to_record_batch_with_row_at_target_schema() {
        let target = schema_with_ids(&[(0, "id", DataTypes::int())]);

        let mut writer = CompactedRowWriter::new(1);
        writer.write_int(42);
        let row_bytes = make_row_bytes(0, writer.buffer());

        let decoder = FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &target).unwrap();
        let result = lookup_result_from(vec![row_bytes], &target, cache_with(0, decoder, vec![]));

        let batch = result.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[test]
    fn test_get_rows_decodes_per_row_schema_id_with_projection() {
        let source = schema_with_ids(&[(0, "a", DataTypes::int())]);
        let target = schema_with_ids(&[(0, "a", DataTypes::int()), (1, "b", DataTypes::string())]);

        let mut w = CompactedRowWriter::new(1);
        w.write_int(7);
        let old_row = make_row_bytes(3, w.buffer());

        let mut w = CompactedRowWriter::new(2);
        w.write_int(8);
        w.write_string("eight");
        let new_row = make_row_bytes(7, w.buffer());

        let target_decoder =
            FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &target).unwrap();
        let projection_decoder =
            FixedSchemaDecoder::new(KvFormat::COMPACTED, &source, &target).unwrap();
        let cache = cache_with(7, target_decoder, vec![(3, projection_decoder)]);
        let result = lookup_result_from(vec![old_row, new_row], &target, cache);

        let rows = result.get_rows().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get_int(0).unwrap(), 7);
        assert!(rows[0].is_null_at(1).unwrap());
        assert_eq!(rows[1].get_int(0).unwrap(), 8);
        assert_eq!(rows[1].get_string(1).unwrap(), "eight");
    }

    #[test]
    fn test_to_record_batch_payload_too_short() {
        let target = schema_with_ids(&[(0, "id", DataTypes::int())]);
        let decoder = FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &target).unwrap();
        let result = lookup_result_from(vec![vec![0u8]], &target, cache_with(0, decoder, vec![]));
        assert!(result.to_record_batch().is_err());
    }

    #[test]
    fn test_get_rows_errors_when_no_decoder_for_schema_id() {
        let target = schema_with_ids(&[(0, "id", DataTypes::int())]);
        let decoder = FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &target).unwrap();
        let mut w = CompactedRowWriter::new(1);
        w.write_int(1);
        let row = make_row_bytes(99, w.buffer());
        let result = lookup_result_from(vec![row], &target, cache_with(0, decoder, vec![]));

        let err = result
            .get_rows()
            .map(|_| ())
            .map_err(|e| e.to_string())
            .unwrap_err();
        assert!(err.contains("schema id 99"), "{err}");
    }

    #[test]
    fn test_read_schema_id_rejects_negative() {
        let bytes = [0xFFu8, 0xFFu8, 0u8];
        let err = LookupResult::read_schema_id(&bytes).unwrap_err();
        assert!(
            err.to_string().contains("Invalid negative schema id"),
            "{err}"
        );
    }

    #[test]
    fn test_decoder_cache_target_lookup_skips_lock() {
        let target = schema_with_ids(&[(0, "a", DataTypes::int())]);
        let target_decoder =
            Arc::new(FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &target).unwrap());
        let cache = DecoderCache::new(7, Arc::clone(&target_decoder));

        let returned = cache.get(7).expect("target id must hit the cache");
        assert!(Arc::ptr_eq(&returned, &target_decoder));
        assert!(cache.get(99).is_none());
    }
}
