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

use crate::PartitionId;
use crate::bucketing::BucketingFunction;
use crate::client::ClientSchemaGetter;
use crate::client::lookup::LookupClient;
use crate::client::metadata::Metadata;
use crate::client::table::kv_value_decoder::KvValueDecoder;
use crate::client::table::partition_getter::PartitionGetter;
use crate::error::{Error, Result};
use crate::metadata::{
    PhysicalTablePath, RowType, TableBucket, TableInfo, TableOrPartition, TablePath,
};
use crate::row::encode::{KeyEncoder, KeyEncoderFactory};
use crate::row::{InternalRow, LookupRow};
use arrow::array::RecordBatch;
use parking_lot::Mutex;
use std::sync::Arc;

/// Rows returned from a lookup. Primary-key lookups produce at most one
/// row; prefix-key lookups may produce many. Rows written under older
/// schemas are decoded with their original schema and projected to the
/// schema captured when the `Lookuper` was created — schema evolutions
/// that land after that point are not picked up by an existing
/// `Lookuper`; create a new one to see them.
pub struct LookupResult {
    rows: Vec<Vec<u8>>,
    target_row_type: Arc<RowType>,
    decoders: Arc<KvValueDecoder>,
}

impl LookupResult {
    fn new(
        rows: Vec<Vec<u8>>,
        target_row_type: Arc<RowType>,
        decoders: Arc<KvValueDecoder>,
    ) -> Self {
        Self {
            rows,
            target_row_type,
            decoders,
        }
    }

    fn decode<'a>(&self, bytes: &'a [u8]) -> Result<LookupRow<'a>> {
        self.decoders.decode(bytes)
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
        self.decoders
            .decode_to_record_batch(self.rows.iter().map(Vec::as_slice), &self.target_row_type)
    }
}

struct LookupSchemaCtx {
    target_row_type: Arc<RowType>,
    schema_getter: Arc<ClientSchemaGetter>,
    decoders: Arc<KvValueDecoder>,
}

impl LookupSchemaCtx {
    fn new(table_info: &TableInfo, schema_getter: Arc<ClientSchemaGetter>) -> Result<Self> {
        Ok(Self {
            target_row_type: Arc::new(table_info.row_type().clone()),
            schema_getter,
            decoders: Arc::new(KvValueDecoder::for_table(table_info)?),
        })
    }

    async fn build_result(&self, rows: Vec<Vec<u8>>) -> Result<LookupResult> {
        if !rows.is_empty() {
            self.decoders
                .ensure_decoders(&self.schema_getter, rows.iter().map(Vec::as_slice))
                .await?;
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
        // Get data lake format from table config for bucketing function
        let data_lake_format = self.table_info.get_table_config().get_datalake_format()?;
        let bucketing_function = <dyn BucketingFunction>::of(data_lake_format.as_ref());

        let row_type = self.table_info.row_type();
        let primary_keys = self.table_info.get_primary_keys();
        let lookup_row_type = row_type.project_with_field_names(primary_keys)?;

        let physical_primary_keys = self.table_info.get_physical_primary_keys().to_vec();
        let kv_format_version = self.table_info.get_table_config().get_kv_format_version()?;
        let is_default_bucket_key = self.table_info.is_default_bucket_key();
        let primary_key_encoder = KeyEncoderFactory::of_primary_key_encoder(
            &lookup_row_type,
            &physical_primary_keys,
            &data_lake_format,
            kv_format_version,
            is_default_bucket_key,
        )?;

        let bucket_key_encoder = if is_default_bucket_key {
            None
        } else {
            let bucket_keys = self.table_info.get_bucket_keys().to_vec();
            Some(KeyEncoderFactory::of_bucket_key_encoder(
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
            primary_key_encoder: Mutex::new(primary_key_encoder),
            bucket_key_encoder: bucket_key_encoder.map(Mutex::new),
            partition_getter,
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
    primary_key_encoder: Mutex<Box<dyn KeyEncoder>>,
    bucket_key_encoder: Option<Mutex<Box<dyn KeyEncoder>>>,
    partition_getter: Option<PartitionGetter>,
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
    pub async fn lookup(&self, row: &dyn InternalRow) -> Result<LookupResult> {
        let pk_bytes = self.primary_key_encoder.lock().encode_key(row)?;
        let bk_bytes = match &self.bucket_key_encoder {
            Some(encoder) => encoder.lock().encode_key(row)?,
            None => pk_bytes.clone(),
        };

        let (partition_id, bucket_count) = if let Some(ref partition_getter) = self.partition_getter
        {
            let partition_name = partition_getter.get_partition(row)?;
            let physical_table_path = PhysicalTablePath::of_partitioned(
                Arc::clone(&self.table_path),
                Some(partition_name),
            );
            let Some(partition_id) = self
                .metadata
                .check_and_update_partition_metadata(&physical_table_path)
                .await?
            else {
                return Ok(self.schema_ctx.empty_result());
            };
            let bucket_count = partition_bucket_count(
                &self.metadata,
                &self.table_info,
                physical_table_path,
                partition_id,
            )
            .await?;
            (Some(partition_id), bucket_count)
        } else {
            // A non-partitioned table never changes `bucket.num`.
            (None, self.table_info.num_buckets)
        };
        let bucket_id = self.bucketing_function.bucketing(&bk_bytes, bucket_count)?;

        let table_id = self.table_info.get_table_id();
        let table_bucket = TableBucket::new_with_partition(table_id, partition_id, bucket_id);

        // Use the batched lookup client
        let result = self
            .lookup_client
            .lookup(
                self.table_path.as_ref().clone(),
                table_bucket,
                bucket_count,
                pk_bytes,
            )
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

        let data_lake_format = self.table_info.get_table_config().get_datalake_format()?;
        let bucketing_function = <dyn BucketingFunction>::of(data_lake_format.as_ref());

        let row_type = self.table_info.row_type();
        let lookup_row_type = row_type.project_with_field_names(&self.lookup_column_names)?;

        let bucket_keys = self.table_info.get_bucket_keys().to_vec();
        let kv_format_version = self.table_info.get_table_config().get_kv_format_version()?;
        let is_default_bucket_key = self.table_info.is_default_bucket_key();

        // The bytes produced here are sent to the server to perform a
        // lexicographic byte-prefix match against the stored primary keys, so
        // the encoder must follow the primary key encoder rules (row-level
        // byte-comparable). The encoded fields are the bucket key columns,
        // which `validate_prefix_lookup` guarantees to be a strict prefix of
        // the physical primary keys.
        let prefix_lookup_key_encoder = KeyEncoderFactory::of_primary_key_encoder(
            &lookup_row_type,
            &bucket_keys,
            &data_lake_format,
            kv_format_version,
            is_default_bucket_key,
        )?;

        // Bucket id must stay consistent with the bucket id computed by the
        // downstream lake, so when the bucket key differs from the primary
        // key we need a separate lake-aligned encoder. When they are the
        // same, the prefix-lookup key encoder already produces the right
        // bytes and we fall back to it (None).
        let bucket_key_encoder = if is_default_bucket_key || data_lake_format.is_none() {
            None
        } else {
            Some(KeyEncoderFactory::of_bucket_key_encoder(
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

        Ok(PrefixKeyLookuper {
            table_path: Arc::new(self.table_info.table_path.clone()),
            table_info: self.table_info,
            metadata: self.metadata,
            lookup_client: self.lookup_client,
            bucketing_function,
            prefix_lookup_key_encoder: Mutex::new(prefix_lookup_key_encoder),
            bucket_key_encoder: bucket_key_encoder.map(Mutex::new),
            partition_getter,
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
    /// Encodes the lookup row into the prefix bytes sent to the server for
    /// byte-prefix matching against stored primary keys.
    prefix_lookup_key_encoder: Mutex<Box<dyn KeyEncoder>>,
    /// Optional lake-aligned encoder used solely to compute the bucket id.
    /// `None` when the bucket key equals the primary key, in which case the
    /// prefix lookup key bytes are reused for bucketing.
    bucket_key_encoder: Option<Mutex<Box<dyn KeyEncoder>>>,
    partition_getter: Option<PartitionGetter>,
    schema_ctx: LookupSchemaCtx,
}

impl PrefixKeyLookuper {
    pub async fn lookup(&self, row: &dyn InternalRow) -> Result<LookupResult> {
        let prefix_bytes = self.prefix_lookup_key_encoder.lock().encode_key(row)?;
        let bk_bytes = match &self.bucket_key_encoder {
            Some(encoder) => encoder.lock().encode_key(row)?,
            None => prefix_bytes.clone(),
        };

        let (partition_id, bucket_count) = if let Some(ref partition_getter) = self.partition_getter
        {
            let partition_name = partition_getter.get_partition(row)?;
            let physical_table_path = PhysicalTablePath::of_partitioned(
                Arc::clone(&self.table_path),
                Some(partition_name),
            );
            let Some(partition_id) = self
                .metadata
                .check_and_update_partition_metadata(&physical_table_path)
                .await?
            else {
                return Ok(self.schema_ctx.empty_result());
            };
            let bucket_count = partition_bucket_count(
                &self.metadata,
                &self.table_info,
                physical_table_path,
                partition_id,
            )
            .await?;
            (Some(partition_id), bucket_count)
        } else {
            // A non-partitioned table never changes `bucket.num`.
            (None, self.table_info.num_buckets)
        };
        let bucket_id = self.bucketing_function.bucketing(&bk_bytes, bucket_count)?;

        let table_id = self.table_info.get_table_id();
        let table_bucket = TableBucket::new_with_partition(table_id, partition_id, bucket_id);

        let rows = self
            .lookup_client
            .prefix_lookup(
                self.table_path.as_ref().clone(),
                table_bucket,
                bucket_count,
                prefix_bytes,
            )
            .await?;

        self.schema_ctx.build_result(rows).await
    }

    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }
}

/// Refreshes the partition's metadata when its bucket count is not cached.
async fn partition_bucket_count(
    metadata: &Metadata,
    table_info: &TableInfo,
    physical_table_path: PhysicalTablePath,
    partition_id: PartitionId,
) -> Result<i32> {
    if let Some(count) = metadata
        .get_cluster()
        .bucket_count(TableOrPartition::Partition(partition_id))
    {
        return Ok(count);
    }
    metadata
        .update_physical_table_metadata(&[Arc::new(physical_table_path)])
        .await?;
    let cluster = metadata.get_cluster();
    let current = cluster
        .get_table(&table_info.table_path)
        .unwrap_or(table_info);
    cluster.bucket_count_or_fallback(current, Some(partition_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Column, DataTypes, KvFormat, Schema};
    use crate::record::kv::SCHEMA_ID_LENGTH;
    use crate::row::DataGetters;
    use crate::row::FixedSchemaDecoder;
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
        target: &Schema,
        target_id: i16,
        others: Vec<(i16, FixedSchemaDecoder)>,
    ) -> Arc<KvValueDecoder> {
        let mut cache =
            KvValueDecoder::new(KvFormat::COMPACTED, Arc::new(target.clone()), target_id).unwrap();
        for (id, decoder) in others {
            cache = cache.with_decoder(id, decoder);
        }
        Arc::new(cache)
    }

    fn lookup_result_from(
        rows: Vec<Vec<u8>>,
        target_schema: &Schema,
        decoders: Arc<KvValueDecoder>,
    ) -> LookupResult {
        LookupResult::new(rows, Arc::new(target_schema.row_type().clone()), decoders)
    }

    #[test]
    fn test_to_record_batch_empty() {
        let target = schema_with_ids(&[(0, "id", DataTypes::int())]);
        let result = lookup_result_from(Vec::new(), &target, cache_with(&target, 0, vec![]));
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

        let result = lookup_result_from(vec![row_bytes], &target, cache_with(&target, 0, vec![]));

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

        let projection_decoder =
            FixedSchemaDecoder::new(KvFormat::COMPACTED, &source, &target).unwrap();
        let cache = cache_with(&target, 7, vec![(3, projection_decoder)]);
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
        let result = lookup_result_from(vec![vec![0u8]], &target, cache_with(&target, 0, vec![]));
        assert!(result.to_record_batch().is_err());
    }

    #[test]
    fn test_get_rows_errors_when_no_decoder_for_schema_id() {
        let target = schema_with_ids(&[(0, "id", DataTypes::int())]);
        let mut w = CompactedRowWriter::new(1);
        w.write_int(1);
        let row = make_row_bytes(99, w.buffer());
        let result = lookup_result_from(vec![row], &target, cache_with(&target, 0, vec![]));

        let err = result
            .get_rows()
            .map(|_| ())
            .map_err(|e| e.to_string())
            .unwrap_err();
        assert!(err.contains("schema id 99"), "{err}");
    }
}
