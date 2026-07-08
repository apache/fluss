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

//! Bounded batch scanner backed by a single `LimitScanRequest`, polled with
//! `next_batch` until it returns `None` (like `RecordBatchLogReader`).
//!
//! The KV branch decodes a [`ValueRecordBatch`], decoding each record against
//! its own schema id via [`FixedSchemaDecoder`] so older records are projected
//! onto the current schema (the same path as lookup).

use crate::client::ClientSchemaGetter;
use crate::client::metadata::Metadata;
use crate::error::{ApiError, Error, FlussError, Result};
use crate::metadata::{KvFormat, RowType, Schema, TableBucket, TableInfo};
use crate::proto::ErrorResponse;
use crate::record::kv::{SCHEMA_ID_LENGTH, ValueRecordBatch};
use crate::record::{
    LogRecordsBatches, ReadContext as ArrowReadContext, RowAppendRecordBatchBuilder, ScanBatch,
    to_arrow_schema,
};
use crate::row::FixedSchemaDecoder;
use crate::rpc::RpcClient;
use crate::rpc::message::LimitScanRequest;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

/// One-shot bounded scanner: a single `LimitScanRequest` yielded as one
/// [`ScanBatch`]. Creation is cheap; the request runs on the first
/// [`next_batch`](Self::next_batch), which returns the batch once, then `None`.
pub struct LimitBatchScanner {
    bucket: TableBucket,
    /// Taken on the first `next_batch` to run the scan; `None` afterward.
    pending: Option<PendingScan>,
}

/// Request inputs captured at creation, consumed by the first `next_batch`.
struct PendingScan {
    rpc_client: Arc<RpcClient>,
    metadata: Arc<Metadata>,
    table_info: TableInfo,
    schema_getter: Arc<ClientSchemaGetter>,
    projected_fields: Option<Vec<usize>>,
    limit: i32,
}

impl LimitBatchScanner {
    pub(super) fn new(
        rpc_client: Arc<RpcClient>,
        metadata: Arc<Metadata>,
        table_info: TableInfo,
        schema_getter: Arc<ClientSchemaGetter>,
        projected_fields: Option<Vec<usize>>,
        bucket: TableBucket,
        limit: i32,
    ) -> Self {
        Self {
            bucket,
            pending: Some(PendingScan {
                rpc_client,
                metadata,
                table_info,
                schema_getter,
                projected_fields,
                limit,
            }),
        }
    }

    /// Runs the scan on the first call and returns its batch, then `None`. Not
    /// retried — an error leaves the scanner spent; create a new one to retry.
    pub async fn next_batch(&mut self) -> Result<Option<ScanBatch>> {
        let Some(pending) = self.pending.take() else {
            return Ok(None);
        };
        run_limit_scan(&pending, &self.bucket).await.map(Some)
    }

    /// Drains the scanner into all of its batches.
    pub async fn collect_all_batches(&mut self) -> Result<Vec<ScanBatch>> {
        let mut batches = Vec::new();
        while let Some(batch) = self.next_batch().await? {
            batches.push(batch);
        }
        Ok(batches)
    }

    /// The bucket scanned by this `LimitBatchScanner`.
    pub fn bucket(&self) -> &TableBucket {
        &self.bucket
    }
}

/// Resolves the leader, sends the `LimitScanRequest`, and decodes the response
/// into one [`ScanBatch`].
async fn run_limit_scan(pending: &PendingScan, bucket: &TableBucket) -> Result<ScanBatch> {
    let leader = pending
        .metadata
        .leader_for(&pending.table_info.table_path, bucket)
        .await?
        .ok_or_else(|| {
            Error::leader_not_available(format!("No leader found for table bucket: {bucket}"))
        })?;
    let connection = pending.rpc_client.get_connection(&leader).await?;

    let request = LimitScanRequest::new(
        pending.table_info.table_id,
        bucket.partition_id(),
        bucket.bucket_id(),
        pending.limit,
    );
    let response = connection.request(request).await?;

    if let Some(error_code) = response.error_code
        && error_code != FlussError::None.code()
    {
        let err: ApiError = ErrorResponse {
            error_code,
            error_message: response.error_message.clone(),
        }
        .into();
        return Err(Error::FlussAPIError { api_error: err });
    }

    let raw = response.records.unwrap_or_default();
    // `limit` is validated positive by `TableScan::limit`.
    let limit = pending.limit.max(0) as usize;
    let projected = pending.projected_fields.as_deref();

    // Choose the payload format from table metadata, not the response's advisory
    // `is_log_table` flag.
    let (batch, base_offset) = if !pending.table_info.has_primary_key() {
        decode_log_batch(&pending.table_info, projected, raw, limit)?
    } else {
        // KV (primary-key) limit scan: no log offset, so base_offset is 0.
        let batch = decode_kv_batch(
            &pending.table_info,
            &pending.schema_getter,
            projected,
            raw,
            limit,
        )
        .await?;
        (batch, 0)
    };

    Ok(ScanBatch::new(bucket.clone(), batch, base_offset))
}

/// Decode the log payload into a single Arrow `RecordBatch`, concatenating any
/// inner batches. If more than `limit` rows are returned, the last `limit` are
/// kept and `base_offset` is advanced by the number dropped.
fn decode_log_batch(
    table_info: &TableInfo,
    projected_fields: Option<&[usize]>,
    raw: Vec<u8>,
    limit: usize,
) -> Result<(RecordBatch, i64)> {
    let row_type = Arc::new(table_info.get_row_type().clone());
    let full_schema = to_arrow_schema(table_info.get_row_type())?;
    // A limit scan returns every column (never projected server-side); decode
    // the full batch and project after, like the KV path. Pushdown here would
    // misparse the full-column body and corrupt the buffers.
    let read_context = ArrowReadContext::new(full_schema.clone(), row_type.clone(), false);

    if raw.is_empty() {
        let empty = RecordBatch::new_empty(full_schema);
        return Ok((
            project_batch(empty, table_info.get_row_type(), projected_fields)?,
            0,
        ));
    }

    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut base_offset: Option<i64> = None;
    for log_batch in LogRecordsBatches::new(raw) {
        let log_batch = log_batch?;
        if base_offset.is_none() {
            base_offset = Some(log_batch.base_log_offset());
        }
        let rb = log_batch.record_batch(&read_context)?;
        batches.push(rb);
    }

    let base_offset = base_offset.unwrap_or(0);
    let merged = if batches.is_empty() {
        RecordBatch::new_empty(full_schema)
    } else if batches.len() == 1 {
        batches.into_iter().next().unwrap()
    } else {
        concat_batches(&full_schema, batches.iter()).map_err(|e| Error::UnexpectedError {
            message: format!("Failed to concatenate log record batches: {e}"),
            source: None,
        })?
    };

    let (trimmed, base_offset) = take_last_rows(merged, base_offset, limit);
    Ok((
        project_batch(trimmed, table_info.get_row_type(), projected_fields)?,
        base_offset,
    ))
}

/// Decode a KV limit-scan [`ValueRecordBatch`] into a single Arrow
/// `RecordBatch`, decoding each record by its own schema id and projecting onto
/// the current schema.
async fn decode_kv_batch(
    table_info: &TableInfo,
    schema_getter: &ClientSchemaGetter,
    projected_fields: Option<&[usize]>,
    raw: Vec<u8>,
    limit: usize,
) -> Result<RecordBatch> {
    // No records: return an empty (projected) batch.
    if raw.is_empty() {
        return empty_record_batch(table_info.get_row_type(), projected_fields);
    }

    let kv_format = table_info.table_config.get_kv_format()?;
    let target_schema = table_info.get_schema();
    let target_schema_id =
        i16::try_from(table_info.get_schema_id()).map_err(|_| Error::UnexpectedError {
            message: format!(
                "Schema id {} does not fit in 16 bits — wire format violated",
                table_info.get_schema_id()
            ),
            source: None,
        })?;

    let batch = ValueRecordBatch::new(Bytes::from(raw));
    let ranges = batch.value_ranges()?;

    // Collect the distinct schema ids present, then build one decoder per id
    // (fetching older schemas via the coordinator as needed).
    let mut schema_ids: Vec<i16> = Vec::new();
    for range in &ranges {
        let id = read_schema_id(&batch.data()[range.clone()])?;
        if !schema_ids.contains(&id) {
            schema_ids.push(id);
        }
    }
    let decoders = build_kv_decoders(
        schema_getter,
        target_schema,
        target_schema_id,
        kv_format,
        &schema_ids,
    )
    .await?;

    value_records_to_record_batch(
        &batch,
        &ranges,
        &decoders,
        table_info.get_row_type(),
        projected_fields,
        limit,
    )
}

/// Build one [`FixedSchemaDecoder`] per distinct schema id. The current schema
/// decodes without projection; older schemas are fetched and projected onto the
/// current schema.
async fn build_kv_decoders(
    schema_getter: &ClientSchemaGetter,
    target_schema: &Schema,
    target_schema_id: i16,
    kv_format: KvFormat,
    schema_ids: &[i16],
) -> Result<HashMap<i16, FixedSchemaDecoder>> {
    let mut decoders = HashMap::with_capacity(schema_ids.len());
    for &id in schema_ids {
        if decoders.contains_key(&id) {
            continue;
        }
        let decoder = if id == target_schema_id {
            FixedSchemaDecoder::new_no_projection(kv_format, target_schema)?
        } else {
            let source = schema_getter.get_schema(id as i32).await?;
            FixedSchemaDecoder::new(kv_format, source.as_ref(), target_schema)?
        };
        decoders.insert(id, decoder);
    }
    Ok(decoders)
}

/// Decode every value record into a row shaped by `target_row_type`, build a
/// single Arrow batch, keep the last `limit` rows, then apply column projection.
fn value_records_to_record_batch(
    batch: &ValueRecordBatch,
    ranges: &[Range<usize>],
    decoders: &HashMap<i16, FixedSchemaDecoder>,
    target_row_type: &RowType,
    projected_fields: Option<&[usize]>,
    limit: usize,
) -> Result<RecordBatch> {
    let mut builder = RowAppendRecordBatchBuilder::new(target_row_type)?;
    for range in ranges {
        let payload = &batch.data()[range.clone()];
        let schema_id = read_schema_id(payload)?;
        let decoder = decoders
            .get(&schema_id)
            .ok_or_else(|| Error::UnexpectedError {
                message: format!("No decoder built for schema id {schema_id}"),
                source: None,
            })?;
        let row = decoder.decode(payload)?;
        builder.append(&row)?;
    }

    let full = Arc::unwrap_or_clone(builder.build_arrow_record_batch()?);
    let (full, _) = take_last_rows(full, 0, limit);
    project_batch(full, target_row_type, projected_fields)
}

/// Read the leading little-endian schema id from a `[schema_id | row]` payload.
fn read_schema_id(payload: &[u8]) -> Result<i16> {
    if payload.len() < SCHEMA_ID_LENGTH {
        return Err(Error::UnexpectedError {
            message: format!(
                "Value record payload too short: {} bytes, need {} for schema id",
                payload.len(),
                SCHEMA_ID_LENGTH
            ),
            source: None,
        });
    }
    let schema_id = LittleEndian::read_i16(&payload[..SCHEMA_ID_LENGTH]);
    if schema_id < 0 {
        return Err(Error::UnexpectedError {
            message: format!("Invalid negative schema id {schema_id}; payload is corrupt"),
            source: None,
        });
    }
    Ok(schema_id)
}

/// Keep the last `limit` rows of `batch`, advancing `base_offset` by the number
/// of dropped leading rows. A `batch` at or under the limit is returned as-is.
fn take_last_rows(batch: RecordBatch, base_offset: i64, limit: usize) -> (RecordBatch, i64) {
    let rows = batch.num_rows();
    if rows > limit {
        let dropped = rows - limit;
        (batch.slice(dropped, limit), base_offset + dropped as i64)
    } else {
        (batch, base_offset)
    }
}

/// An empty `RecordBatch` with the (optionally projected) target schema.
fn empty_record_batch(
    target_row_type: &RowType,
    projected_fields: Option<&[usize]>,
) -> Result<RecordBatch> {
    let empty = RecordBatch::new_empty(to_arrow_schema(target_row_type)?);
    project_batch(empty, target_row_type, projected_fields)
}

/// Project `batch` (shaped by `target_row_type`) onto the requested columns.
fn project_batch(
    batch: RecordBatch,
    target_row_type: &RowType,
    projected_fields: Option<&[usize]>,
) -> Result<RecordBatch> {
    match projected_fields {
        None => Ok(batch),
        Some(fields) => {
            let projected_schema =
                ArrowReadContext::project_schema(to_arrow_schema(target_row_type)?, fields)?;
            let columns: Vec<_> = fields
                .iter()
                .map(|&idx| batch.column(idx).clone())
                .collect();
            Ok(RecordBatch::try_new(projected_schema, columns)?)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::WriteRecord;
    use crate::compression::{
        ArrowCompressionInfo, ArrowCompressionRatioEstimator, ArrowCompressionType,
        DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
    };
    use crate::metadata::{
        Column, DataField, DataType, DataTypes, PhysicalTablePath, Schema, TableInfo, TablePath,
    };
    use crate::record::MemoryLogRecordsArrowBuilder;
    use crate::row::GenericRow;
    use crate::row::binary::BinaryWriter;
    use crate::row::compacted::CompactedRowWriter;
    use crate::test_utils::build_table_info_with_columns;
    use arrow::array::{Array, Int32Array, Int64Array, StringArray};

    fn build_two_col_table_info() -> TableInfo {
        build_table_info_with_columns(
            TablePath::new("db".to_string(), "tbl".to_string()),
            42,
            1,
            vec![
                DataField::new("id", DataTypes::int(), None),
                DataField::new("name", DataTypes::string(), None),
            ],
        )
    }

    /// Encode `rows` (built against `table_info`'s row type) as one Arrow log batch.
    fn build_log_batch(table_info: &TableInfo, rows: &[GenericRow]) -> Vec<u8> {
        let table_info_arc = Arc::new(table_info.clone());
        let physical = Arc::new(PhysicalTablePath::of(Arc::new(
            table_info.table_path.clone(),
        )));
        let mut builder = MemoryLogRecordsArrowBuilder::new(
            1,
            table_info.get_row_type(),
            false,
            ArrowCompressionInfo {
                compression_type: ArrowCompressionType::None,
                compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
            },
            usize::MAX,
            Arc::new(ArrowCompressionRatioEstimator::default()),
        )
        .expect("builder");
        for (i, row) in rows.iter().enumerate() {
            let record = WriteRecord::for_append(
                Arc::clone(&table_info_arc),
                physical.clone(),
                (i + 1) as i32,
                row,
            );
            builder.append(&record).expect("append");
        }
        builder.build().expect("build log batch")
    }

    fn build_log_records(
        table_info: &TableInfo,
        base_offset: i64,
        rows: &[(i32, &str)],
    ) -> Vec<u8> {
        let rows: Vec<GenericRow> = rows
            .iter()
            .map(|(id, name)| {
                let mut row = GenericRow::new(2);
                row.set_field(0, *id);
                row.set_field(1, *name);
                row
            })
            .collect();
        let mut data = build_log_batch(table_info, &rows);
        // Builder always writes base_log_offset=0; patch it so tests can verify
        // BatchScanner faithfully propagates whatever offset the server returned.
        let bytes = base_offset.to_le_bytes();
        data[..bytes.len()].copy_from_slice(&bytes);
        data
    }

    // ---- log path ----------------------------------------------------------

    #[test]
    fn decode_log_batch_empty_returns_empty_record_batch() {
        let table_info = build_two_col_table_info();
        let (batch, base_offset) =
            decode_log_batch(&table_info, None, Vec::new(), usize::MAX).expect("decode empty");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(base_offset, 0);
    }

    #[test]
    fn decode_log_batch_empty_with_projection() {
        let table_info = build_two_col_table_info();
        let (batch, base_offset) =
            decode_log_batch(&table_info, Some(&[1usize]), Vec::new(), usize::MAX)
                .expect("decode empty");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "name");
        assert_eq!(base_offset, 0);
    }

    #[test]
    fn decode_log_batch_extracts_base_offset_and_rows() {
        let table_info = build_two_col_table_info();
        let raw = build_log_records(&table_info, 17, &[(1, "alice"), (2, "bob"), (3, "carol")]);

        let (batch, base_offset) =
            decode_log_batch(&table_info, None, raw, usize::MAX).expect("decode populated");
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(base_offset, 17);
    }

    #[test]
    fn decode_log_batch_projection_keeps_requested_columns() {
        let table_info = build_two_col_table_info();
        let raw = build_log_records(&table_info, 0, &[(7, "x"), (8, "y")]);

        let (batch, _) = decode_log_batch(&table_info, Some(&[0usize]), raw, usize::MAX)
            .expect("decode projected");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "id");
    }

    /// Projection skipping a middle variable-length column — catches a
    /// full-column body being misparsed as the projected schema.
    #[test]
    fn decode_log_batch_projection_skips_middle_variable_length_column() {
        let table_info = build_table_info_with_columns(
            TablePath::new("db".to_string(), "tbl".to_string()),
            43,
            1,
            vec![
                DataField::new("c1", DataTypes::int(), None),
                DataField::new("c2", DataTypes::string(), None),
                DataField::new("c3", DataTypes::bigint(), None),
            ],
        );
        let rows: Vec<GenericRow> = [(1, "alice", 100i64), (2, "bob", 200i64)]
            .iter()
            .map(|(c1, c2, c3)| {
                let mut row = GenericRow::new(3);
                row.set_field(0, *c1);
                row.set_field(1, *c2);
                row.set_field(2, *c3);
                row
            })
            .collect();
        let raw = build_log_batch(&table_info, &rows);

        let (batch, _) = decode_log_batch(&table_info, Some(&[0usize, 2usize]), raw, usize::MAX)
            .expect("decode projected");
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(0).name(), "c1");
        assert_eq!(batch.schema().field(1).name(), "c3");
        let c1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let c3 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!((c1.value(0), c1.value(1)), (1, 2));
        assert_eq!((c3.value(0), c3.value(1)), (100, 200));
    }

    #[test]
    fn decode_log_batch_non_prefix_projection_reorders_columns() {
        let table_info = build_two_col_table_info();
        let raw = build_log_records(&table_info, 0, &[(1, "alice"), (2, "bob")]);

        // `[name, id]`: reversed, so neither a leading prefix nor in source order.
        let (batch, _) = decode_log_batch(&table_info, Some(&[1usize, 0usize]), raw, usize::MAX)
            .expect("decode reordered projection");

        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.schema().field(0).name(), "name");
        assert_eq!(batch.schema().field(1).name(), "id");

        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert_eq!(names.value(1), "bob");

        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
    }

    #[test]
    fn decode_log_batch_truncates_to_last_limit_rows() {
        let table_info = build_two_col_table_info();
        // Server returned 4 rows starting at offset 100, but limit is 2.
        let raw = build_log_records(&table_info, 100, &[(1, "a"), (2, "b"), (3, "c"), (4, "d")]);

        let (batch, base_offset) = decode_log_batch(&table_info, None, raw, 2).expect("decode");
        assert_eq!(batch.num_rows(), 2);
        // The last two rows are kept, so the base offset advances by 2.
        assert_eq!(base_offset, 102);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 3);
        assert_eq!(ids.value(1), 4);
    }

    // ---- KV path -----------------------------------------------------------

    fn schema_with_ids(columns: &[(i32, &str, DataType)]) -> Schema {
        let cols: Vec<Column> = columns
            .iter()
            .map(|(id, name, dt)| Column::new(*name, dt.clone()).with_id(*id))
            .collect();
        Schema::builder().with_columns(cols).build().unwrap()
    }

    /// Encode a value-record batch from `(schema_id, compacted-row-bytes)`
    /// pairs, matching the Java `DefaultValueRecordBatch` wire layout.
    fn value_batch(records: &[(i16, Vec<u8>)]) -> ValueRecordBatch {
        let mut body = Vec::new();
        for (schema_id, row) in records {
            let rec_len = (SCHEMA_ID_LENGTH + row.len()) as i32;
            body.extend_from_slice(&rec_len.to_le_bytes());
            body.extend_from_slice(&schema_id.to_le_bytes());
            body.extend_from_slice(row);
        }
        let mut out = Vec::new();
        out.extend_from_slice(&((1 + 4 + body.len()) as i32).to_le_bytes()); // Length
        out.push(0); // Magic
        out.extend_from_slice(&(records.len() as i32).to_le_bytes()); // RecordCount
        out.extend_from_slice(&body);
        ValueRecordBatch::new(Bytes::from(out))
    }

    fn compacted(field_count: usize, write: impl FnOnce(&mut CompactedRowWriter)) -> Vec<u8> {
        let mut w = CompactedRowWriter::new(field_count);
        write(&mut w);
        w.to_bytes().as_ref().to_vec()
    }

    fn id_name_schema() -> Schema {
        schema_with_ids(&[
            (0, "id", DataTypes::int()),
            (1, "name", DataTypes::string()),
        ])
    }

    #[test]
    fn value_records_empty_returns_empty_batch() {
        let schema = id_name_schema();
        let batch = value_batch(&[]);
        let ranges = batch.value_ranges().unwrap();
        let rb = value_records_to_record_batch(
            &batch,
            &ranges,
            &HashMap::new(),
            schema.row_type(),
            None,
            usize::MAX,
        )
        .expect("decode empty kv");
        assert_eq!(rb.num_rows(), 0);
        assert_eq!(rb.num_columns(), 2);
    }

    #[test]
    fn empty_kv_payload_returns_empty_batch() {
        let schema = id_name_schema();
        // Full schema.
        let rb = empty_record_batch(schema.row_type(), None).expect("empty");
        assert_eq!(rb.num_rows(), 0);
        assert_eq!(rb.num_columns(), 2);
        // Projected.
        let rb = empty_record_batch(schema.row_type(), Some(&[1usize])).expect("empty projected");
        assert_eq!(rb.num_rows(), 0);
        assert_eq!(rb.num_columns(), 1);
        assert_eq!(rb.schema().field(0).name(), "name");
    }

    #[test]
    fn value_records_decode_rows() {
        let schema = id_name_schema();
        let decoder = FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &schema).unwrap();
        let mut decoders = HashMap::new();
        decoders.insert(0i16, decoder);

        let r0 = compacted(2, |w| {
            w.write_int(1);
            w.write_string("alice");
        });
        let r1 = compacted(2, |w| {
            w.write_int(2);
            w.write_string("bob");
        });
        let batch = value_batch(&[(0, r0), (0, r1)]);
        let ranges = batch.value_ranges().unwrap();

        let rb = value_records_to_record_batch(
            &batch,
            &ranges,
            &decoders,
            schema.row_type(),
            None,
            usize::MAX,
        )
        .expect("decode kv rows");
        assert_eq!(rb.num_rows(), 2);
        let ids = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
    }

    #[test]
    fn value_records_limit_keeps_last_rows() {
        let schema = id_name_schema();
        let decoder = FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &schema).unwrap();
        let mut decoders = HashMap::new();
        decoders.insert(0i16, decoder);

        let records: Vec<(i16, Vec<u8>)> = (1..=5)
            .map(|i| {
                (
                    0i16,
                    compacted(2, |w| {
                        w.write_int(i);
                        w.write_string("x");
                    }),
                )
            })
            .collect();
        let batch = value_batch(&records);
        let ranges = batch.value_ranges().unwrap();

        let rb =
            value_records_to_record_batch(&batch, &ranges, &decoders, schema.row_type(), None, 3)
                .expect("decode kv rows");
        assert_eq!(rb.num_rows(), 3);
        let ids = rb.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        // Last 3 of [1,2,3,4,5].
        assert_eq!(ids.values(), &[3, 4, 5]);
    }

    #[test]
    fn value_records_projection_keeps_requested_columns() {
        let schema = id_name_schema();
        let decoder = FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &schema).unwrap();
        let mut decoders = HashMap::new();
        decoders.insert(0i16, decoder);

        let r0 = compacted(2, |w| {
            w.write_int(9);
            w.write_string("nine");
        });
        let batch = value_batch(&[(0, r0)]);
        let ranges = batch.value_ranges().unwrap();

        let rb = value_records_to_record_batch(
            &batch,
            &ranges,
            &decoders,
            schema.row_type(),
            Some(&[1usize]),
            usize::MAX,
        )
        .expect("decode projected kv");
        assert_eq!(rb.num_columns(), 1);
        assert_eq!(rb.schema().field(0).name(), "name");
    }

    #[test]
    fn value_records_decode_across_schema_evolution() {
        // Source schema (older): [id, name]. Target (current): added `age`.
        let source = id_name_schema();
        let target = schema_with_ids(&[
            (0, "id", DataTypes::int()),
            (1, "name", DataTypes::string()),
            (2, "age", DataTypes::bigint()),
        ]);

        let mut decoders = HashMap::new();
        // Records with schema id 0 were written under the old schema.
        decoders.insert(
            0i16,
            FixedSchemaDecoder::new(KvFormat::COMPACTED, &source, &target).unwrap(),
        );
        // Records with schema id 1 carry the current schema.
        decoders.insert(
            1i16,
            FixedSchemaDecoder::new_no_projection(KvFormat::COMPACTED, &target).unwrap(),
        );

        let old_row = compacted(2, |w| {
            w.write_int(1);
            w.write_string("alice");
        });
        let new_row = compacted(3, |w| {
            w.write_int(2);
            w.write_string("bob");
            w.write_long(30);
        });
        let batch = value_batch(&[(0, old_row), (1, new_row)]);
        let ranges = batch.value_ranges().unwrap();

        let rb = value_records_to_record_batch(
            &batch,
            &ranges,
            &decoders,
            target.row_type(),
            None,
            usize::MAX,
        )
        .expect("decode mixed-schema kv");

        assert_eq!(rb.num_rows(), 2);
        assert_eq!(rb.num_columns(), 3);
        let age = rb.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        // Old record has no `age` column -> null; new record carries 30.
        assert!(age.is_null(0), "old-schema record must read age as null");
        assert_eq!(age.value(1), 30);
    }
}
