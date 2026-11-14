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
use crate::client::table::partition_getter::{PartitionGetter, get_physical_path};
use crate::client::{
    MemoryPermit, ResultHandle, WriteRecord, WriteResultFuture, WriterClient,
    prebuilt_batch_alloc_size,
};
use crate::error::Error::{IllegalArgument, UnexpectedError};
use crate::error::Result;
use crate::metadata::{PhysicalTablePath, TableInfo, TablePath};
use crate::record::prepare_append_record_batch;
use crate::row::encode::{KeyEncoder, KeyEncoderFactory};
use crate::row::{ColumnarRow, InternalRow};
use arrow::array::{RecordBatch, Scalar, UInt32Array};
use arrow::buffer::BooleanBuffer;
use arrow::compute::kernels::cmp;
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

pub struct TableAppend {
    table_path: Arc<TablePath>,
    table_info: Arc<TableInfo>,
    writer_client: Arc<WriterClient>,
}

impl TableAppend {
    pub(super) fn new(
        table_path: TablePath,
        table_info: Arc<TableInfo>,
        writer_client: Arc<WriterClient>,
    ) -> Self {
        Self {
            table_path: Arc::new(table_path),
            table_info,
            writer_client,
        }
    }

    pub fn create_writer(&self) -> Result<AppendWriter> {
        let partition_getter = if self.table_info.is_partitioned() {
            Some(PartitionGetter::new(
                self.table_info.row_type(),
                Arc::clone(self.table_info.get_partition_keys()),
            )?)
        } else {
            None
        };

        let bucket_router = if self.table_info.has_bucket_key() {
            let data_lake_format = self.table_info.table_config.get_datalake_format()?;
            let encoder = KeyEncoderFactory::of_bucket_key_encoder(
                self.table_info.row_type(),
                self.table_info.get_bucket_keys(),
                &data_lake_format,
            )?;
            Some(BucketRouter {
                encoder: Mutex::new(encoder),
                bucketing: <dyn BucketingFunction>::of(data_lake_format.as_ref()),
            })
        } else {
            None
        };

        Ok(AppendWriter {
            table_path: Arc::clone(&self.table_path),
            partition_getter,
            writer_client: self.writer_client.clone(),
            table_info: Arc::clone(&self.table_info),
            bucket_router,
        })
    }
}

struct BucketRouter {
    encoder: Mutex<Box<dyn KeyEncoder>>,
    bucketing: Box<dyn BucketingFunction>,
}

impl BucketRouter {
    fn encode_key(&self, row: &dyn InternalRow) -> Result<Bytes> {
        self.encoder.lock().encode_key(row)
    }

    fn bucket_of(&self, row: &dyn InternalRow, num_buckets: i32) -> Result<(i32, Bytes)> {
        let key = self.encode_key(row)?;
        let bucket = self.bucketing.bucketing(&key, num_buckets)?;
        Ok((bucket, key))
    }
}

pub struct AppendWriter {
    table_path: Arc<TablePath>,
    partition_getter: Option<PartitionGetter>,
    writer_client: Arc<WriterClient>,
    table_info: Arc<TableInfo>,
    bucket_router: Option<BucketRouter>,
}

impl AppendWriter {
    fn check_field_count<R: InternalRow>(&self, row: &R) -> Result<()> {
        let expected = self.table_info.get_row_type().fields().len();
        if row.get_field_count() != expected {
            return Err(IllegalArgument {
                message: format!(
                    "The field count of the row does not match the table schema. \
                     Expected: {}, Actual: {}",
                    expected,
                    row.get_field_count()
                ),
            });
        }
        Ok(())
    }

    /// Appends a row to the table.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the write,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// # Arguments
    /// * row - the row to append.
    ///
    /// # Returns
    /// A [`WriteResultFuture`] that can be awaited to wait for server acknowledgment,
    /// or dropped for fire-and-forget behavior (use `flush()` to ensure delivery).
    pub fn append<R: InternalRow>(&self, row: &R) -> Result<WriteResultFuture> {
        self.append_with_deadline(row, None)
    }

    /// Internal: bounds the buffer-memory wait by `deadline` so a language binding can share
    /// one `client.writer.buffer.wait-timeout` budget with callback admission waits. Not a
    /// public deadline API; use [`Self::append`], which passes `None` and defers to the config.
    #[doc(hidden)]
    pub fn append_with_deadline<R: InternalRow>(
        &self,
        row: &R,
        deadline: Option<Instant>,
    ) -> Result<WriteResultFuture> {
        self.check_field_count(row)?;
        let physical_table_path = Arc::new(get_physical_path(
            &self.table_path,
            self.partition_getter.as_ref(),
            row,
        )?);
        let bucket_key = match &self.bucket_router {
            Some(router) => Some(router.encode_key(row)?),
            None => None,
        };
        let record = WriteRecord::for_append(
            Arc::clone(&self.table_info),
            physical_table_path,
            self.table_info.schema_id,
            row,
        )
        .with_bucket_key(bucket_key)
        .with_submit_deadline(deadline);
        let result_handle = self.writer_client.send(&record)?;
        Ok(WriteResultFuture::new(result_handle))
    }

    /// Appends an Arrow RecordBatch to the table.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the write,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// Rows are routed to their respective partitions and distributed across buckets by key.
    /// A null partition value is reported with its zero-based row index before queuing writes.
    ///
    /// # Returns
    /// A [`WriteResultFuture`] that can be awaited to wait for server acknowledgment,
    /// or dropped for fire-and-forget behavior (use `flush()` to ensure delivery).
    pub fn append_arrow_batch(&self, batch: RecordBatch) -> Result<WriteResultFuture> {
        self.append_arrow_batch_with_deadline(batch, None)
    }

    /// Internal: bounds the buffer-memory wait by `deadline` so a language binding can share
    /// one `client.writer.buffer.wait-timeout` budget with callback admission waits. Not a
    /// public deadline API; use [`Self::append_arrow_batch`], which passes `None`.
    #[doc(hidden)]
    pub fn append_arrow_batch_with_deadline(
        &self,
        batch: RecordBatch,
        deadline: Option<Instant>,
    ) -> Result<WriteResultFuture> {
        if batch.num_rows() == 0 {
            // Nothing to write; also avoids a keyless send to a bucket-key table.
            return Ok(WriteResultFuture::join(Vec::new()));
        }
        let partition_keys = self.table_info.get_partition_keys();
        // Check before schema validation so NOT NULL partition columns also report the row index.
        for key in partition_keys.iter() {
            if let Some(index) = batch
                .column_by_name(key)
                .and_then(|column| column.nulls())
                .filter(|nulls| nulls.null_count() != 0)
                .and_then(|nulls| nulls.iter().position(|valid| !valid))
            {
                return Err(IllegalArgument {
                    message: format!("Partition column '{key}' is null at row {index}."),
                });
            }
        }
        let batch = prepare_append_record_batch(&batch, self.table_info.row_type())?;
        if self.partition_getter.is_none() && self.bucket_router.is_none() {
            let path = Arc::new(PhysicalTablePath::of(Arc::clone(&self.table_path)));
            return self
                .send_arrow_batch(batch, &path, None, None, deadline, None)
                .map(WriteResultFuture::new);
        }
        if u32::try_from(batch.num_rows()).is_err() {
            return Err(IllegalArgument {
                message: format!(
                    "Arrow batch has too many rows to route: {}.",
                    batch.num_rows()
                ),
            });
        }
        let mut row = ColumnarRow::new(
            Arc::new(batch.clone()),
            Arc::new(self.table_info.row_type.clone()),
            0,
            None,
        )?;
        let mut parts = Vec::new();
        if let Some(getter) = &self.partition_getter {
            let first_partition = getter.get_partition(&row)?;
            let mut matches_first = BooleanBuffer::new_set(batch.num_rows());
            for key in partition_keys.iter() {
                // The prepared batch has the same column names as the table schema.
                let column = batch
                    .column_by_name(key)
                    .expect("validated partition column");
                let first_value = Scalar::new(column.slice(0, 1));
                let matches = cmp::not_distinct(&column.as_ref(), &first_value)?;
                matches_first = &matches_first & matches.values();
            }
            if matches_first.count_set_bits() == batch.num_rows() {
                let path = Arc::new(PhysicalTablePath::of_partitioned(
                    Arc::clone(&self.table_path),
                    Some(first_partition),
                ));
                self.enqueue_partition_batch(&batch, &path, None, &mut row, &mut parts)?;
            } else {
                let mut partitions = HashMap::<String, Vec<u32>>::new();
                let mut first_indices = Vec::new();
                for (i, same_partition) in matches_first.iter().enumerate() {
                    if same_partition {
                        first_indices.push(i as u32);
                    } else {
                        row.set_row_id(i);
                        let partition = getter.get_partition(&row)?;
                        if partition == first_partition {
                            first_indices.push(i as u32);
                        } else {
                            partitions.entry(partition).or_default().push(i as u32);
                        }
                    }
                }
                partitions.insert(first_partition, first_indices);
                for (partition, indices) in partitions {
                    let path = Arc::new(PhysicalTablePath::of_partitioned(
                        Arc::clone(&self.table_path),
                        Some(partition),
                    ));
                    self.enqueue_partition_batch(
                        &batch,
                        &path,
                        Some(&indices),
                        &mut row,
                        &mut parts,
                    )?;
                }
            }
        } else {
            let path = Arc::new(PhysicalTablePath::of(Arc::clone(&self.table_path)));
            self.enqueue_partition_batch(&batch, &path, None, &mut row, &mut parts)?;
        }
        let mut handles = self.send_parts(parts, deadline)?;
        if handles.len() == 1 {
            Ok(WriteResultFuture::new(handles.pop().unwrap()))
        } else {
            Ok(WriteResultFuture::join(handles))
        }
    }

    /// Queues every part of a split batch, reserving their buffer memory together first so
    /// that running out of it fails the whole batch before any part is queued. A caller
    /// retrying that error then doesn't write the queued parts twice.
    fn send_parts(
        &self,
        parts: Vec<BatchPart>,
        deadline: Option<Instant>,
    ) -> Result<Vec<ResultHandle>> {
        let batch_size = self.writer_client.writer_batch_size();
        let sizes: Vec<usize> = parts
            .iter()
            .map(|part| prebuilt_batch_alloc_size(&part.batch, batch_size))
            .collect();
        let total: usize = sizes.iter().sum();
        // A batch too large for the whole buffer can't be reserved at once; its parts wait
        // for memory one by one as before.
        let mut reserved = if parts.len() > 1 && total <= self.writer_client.buffer_total_bytes() {
            Some(self.writer_client.reserve_memory(total, deadline)?)
        } else {
            None
        };
        let mut handles = Vec::with_capacity(parts.len());
        for (part, size) in parts.into_iter().zip(sizes) {
            let permit = reserved.as_mut().map(|permit| permit.split(size));
            handles.push(self.send_arrow_batch(
                part.batch,
                &part.path,
                part.bucket_key,
                part.bucket_count,
                deadline,
                permit,
            )?);
        }
        Ok(handles)
    }

    fn enqueue_partition_batch(
        &self,
        batch: &RecordBatch,
        path: &Arc<PhysicalTablePath>,
        partition_indices: Option<&[u32]>,
        row: &mut ColumnarRow,
        parts: &mut Vec<BatchPart>,
    ) -> Result<()> {
        let Some(router) = self.bucket_router.as_ref() else {
            let batch = match partition_indices {
                Some(indices) => take_rows(batch, indices)?,
                None => batch.clone(),
            };
            parts.push(BatchPart {
                batch,
                path: Arc::clone(path),
                bucket_key: None,
                bucket_count: None,
            });
            return Ok(());
        };

        // Group rows by bucket, keeping one key per bucket (it hashes back there).
        let num_buckets = self
            .writer_client
            .routing_bucket_count(path, &self.table_info)?;
        let mut groups: HashMap<i32, (Vec<u32>, Bytes)> = HashMap::new();
        let num_rows = partition_indices.map_or(batch.num_rows(), <[u32]>::len);
        for i in 0..num_rows {
            let index = partition_indices.map_or(i as u32, |indices| indices[i]);
            row.set_row_id(index as usize);
            let (bucket, key) = router.bucket_of(row, num_buckets)?;
            groups
                .entry(bucket)
                .or_insert_with(|| (Vec::new(), key))
                .0
                .push(index);
        }

        for (indices, rep_key) in groups.into_values() {
            parts.push(BatchPart {
                batch: take_rows(batch, &indices)?,
                path: Arc::clone(path),
                bucket_key: Some(rep_key),
                bucket_count: Some(num_buckets),
            });
        }
        Ok(())
    }

    fn send_arrow_batch(
        &self,
        batch: RecordBatch,
        physical_table_path: &Arc<PhysicalTablePath>,
        bucket_key: Option<Bytes>,
        bucket_count: Option<i32>,
        deadline: Option<Instant>,
        reserved_memory: Option<MemoryPermit>,
    ) -> Result<ResultHandle> {
        let record = WriteRecord::for_append_record_batch(
            Arc::clone(&self.table_info),
            Arc::clone(physical_table_path),
            self.table_info.schema_id,
            batch,
        )
        .with_bucket_key(bucket_key)
        .with_submit_deadline(deadline)
        .with_reserved_memory(reserved_memory);
        match bucket_count {
            Some(bucket_count) => self
                .writer_client
                .send_with_bucket_count(&record, bucket_count),
            None => self.writer_client.send(&record),
        }
    }

    pub async fn flush(&self) -> Result<()> {
        self.writer_client.flush().await
    }
}

/// One partition and bucket's rows of a split Arrow batch, routed but not queued yet.
struct BatchPart {
    batch: RecordBatch,
    path: Arc<PhysicalTablePath>,
    bucket_key: Option<Bytes>,
    bucket_count: Option<i32>,
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    // Routing produces unique indices in input order, so a full group needs no copy.
    if indices.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    let idx = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column, &idx, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| UnexpectedError {
            message: format!("Failed to split Arrow batch: {e}"),
            source: None,
        })?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| UnexpectedError {
        message: format!("Failed to rebuild split arrow batch: {e}"),
        source: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{FlussAdmin, Metadata};
    use crate::config::Config;
    use crate::metadata::{DataTypes, Schema, TableDescriptor};
    use crate::rpc::RpcClient;
    use crate::test_utils::build_cluster;
    use arrow::array::{Int32Array, StringArray};
    use std::time::Duration;

    #[tokio::test]
    async fn null_partition_values_report_the_original_row_index() -> Result<()> {
        for bucket_keys in [vec![], vec!["id".to_string()]] {
            let table_path = TablePath::new("db", "partitioned");
            let descriptor = TableDescriptor::builder()
                .schema(
                    Schema::builder()
                        .column("id", DataTypes::int())
                        .column("region", DataTypes::string())
                        .column("country", DataTypes::string())
                        .build()?,
                )
                .partitioned_by(vec!["region", "country"])
                .distributed_by(Some(2), bucket_keys)
                .build()?;
            let table_info = Arc::new(TableInfo::of(table_path.clone(), 1, 1, descriptor, 0, 0));
            // Missing table metadata ensures invalid batches fail before routing or enqueueing.
            let metadata = Arc::new(Metadata::new_for_test(Arc::new(build_cluster(
                &TablePath::new("db", "other"),
                2,
                2,
            ))));
            let admin = Arc::new(FlussAdmin::new(
                Arc::new(RpcClient::new()),
                metadata.clone(),
            ));
            let client = Arc::new(WriterClient::new(Config::default(), metadata, admin)?);
            let writer =
                TableAppend::new(table_path, table_info.clone(), client.clone()).create_writer()?;
            for null_row in [0, 2] {
                let mut countries = vec![Some("US"), Some("CA"), Some("US")];
                countries[null_row] = None;
                let batch = RecordBatch::try_new(
                    crate::record::to_arrow_schema(table_info.row_type())?,
                    vec![
                        Arc::new(Int32Array::from(vec![1, 2, 3])),
                        Arc::new(StringArray::from(vec!["US", "US", "US"])),
                        Arc::new(StringArray::from(countries)),
                    ],
                )?;
                writer.append_arrow_batch(batch.slice(0, 0))?.await?;
                let error = writer.append_arrow_batch(batch).expect_err("invalid batch");
                match error {
                    IllegalArgument { message } => {
                        assert_eq!(
                            message,
                            format!("Partition column 'country' is null at row {null_row}.")
                        );
                    }
                    error => panic!("Expected partition validation error, got {error}"),
                }
            }
            client.close(Duration::from_secs(1)).await?;
        }
        Ok(())
    }

    fn bucketed_writer(
        buckets: i32,
        config: Config,
    ) -> Result<(Arc<WriterClient>, AppendWriter, Arc<TableInfo>)> {
        let table_path = TablePath::new("db", "t");
        let descriptor = TableDescriptor::builder()
            .schema(Schema::builder().column("id", DataTypes::int()).build()?)
            .distributed_by(Some(buckets), vec!["id".to_string()])
            .build()?;
        let table_info = Arc::new(TableInfo::of(table_path.clone(), 1, 1, descriptor, 0, 0));
        let metadata = Arc::new(Metadata::new_for_test(Arc::new(build_cluster(
            &table_path,
            1,
            buckets,
        ))));
        let admin = Arc::new(FlussAdmin::new(
            Arc::new(RpcClient::new()),
            metadata.clone(),
        ));
        let client = Arc::new(WriterClient::new(config, metadata, admin)?);
        let writer =
            TableAppend::new(table_path, table_info.clone(), client.clone()).create_writer()?;
        Ok((client, writer, table_info))
    }

    fn int_batch(table_info: &TableInfo, rows: i32) -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            crate::record::to_arrow_schema(table_info.row_type())?,
            vec![Arc::new(Int32Array::from((0..rows).collect::<Vec<i32>>()))],
        )?)
    }

    #[tokio::test]
    async fn split_batch_parts_reserve_their_size_not_a_full_batch() -> Result<()> {
        // 33 parts of a full 2 MiB batch each would not fit the default 64 MiB buffer.
        let config = Config {
            writer_buffer_wait_timeout_ms: 0,
            ..Config::default()
        };
        let (client, writer, table_info) = bucketed_writer(33, config)?;
        let available = client.buffer_available_bytes();
        writer.append_arrow_batch(int_batch(&table_info, 10_000)?)?;
        let reserved = available - client.buffer_available_bytes();
        assert!(
            reserved < 33 * Config::default().writer_batch_size as usize,
            "split parts reserved {reserved} bytes"
        );
        Ok(())
    }

    #[tokio::test]
    async fn split_batch_is_queued_whole_or_not_at_all() -> Result<()> {
        let config = Config {
            writer_buffer_wait_timeout_ms: 0,
            ..Config::default()
        };
        let (client, writer, table_info) = bucketed_writer(33, config)?;
        let batch = int_batch(&table_info, 100_000)?;
        // Leave room for some of the parts but not all of them.
        let free = 100 * 1024;
        let held = client.reserve_memory(client.buffer_available_bytes() - free, None)?;
        let error = writer
            .append_arrow_batch(batch.clone())
            .expect_err("buffer is too small for the whole batch");
        assert!(
            matches!(error, crate::error::Error::BufferExhausted { .. }),
            "unexpected error {error}"
        );
        // Nothing was queued, so retrying writes every row exactly once.
        assert_eq!(client.buffer_available_bytes(), free);

        drop(held);
        writer.append_arrow_batch(batch)?;
        Ok(())
    }
}
