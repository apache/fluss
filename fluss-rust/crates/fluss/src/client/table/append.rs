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
use crate::client::{WriteRecord, WriteResultFuture, WriterClient};
use crate::error::Error::{IllegalArgument, UnexpectedError};
use crate::error::Result;
use crate::metadata::{PhysicalTablePath, TableInfo, TablePath};
use crate::row::encode::{KeyEncoder, KeyEncoderFactory};
use crate::row::{ColumnarRow, InternalRow};
use arrow::array::{RecordBatch, UInt32Array};
use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

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
        .with_bucket_key(bucket_key);
        let result_handle = self.writer_client.send(&record)?;
        Ok(WriteResultFuture::new(result_handle))
    }

    /// Appends an Arrow RecordBatch to the table.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the write,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// For a partitioned table the partition is derived from the **first row**, so all rows
    /// in the batch must belong to the same partition. Rows are distributed across buckets by
    /// their bucket key automatically.
    ///
    /// # Returns
    /// A [`WriteResultFuture`] that can be awaited to wait for server acknowledgment,
    /// or dropped for fire-and-forget behavior (use `flush()` to ensure delivery).
    pub fn append_arrow_batch(&self, batch: RecordBatch) -> Result<WriteResultFuture> {
        let physical_table_path = if self.partition_getter.is_some() && batch.num_rows() > 0 {
            let first_row = ColumnarRow::new(
                Arc::new(batch.clone()),
                Arc::new(self.table_info.row_type.clone()),
                0,
                None,
            )?;
            Arc::new(get_physical_path(
                &self.table_path,
                self.partition_getter.as_ref(),
                &first_row,
            )?)
        } else {
            Arc::new(PhysicalTablePath::of(Arc::clone(&self.table_path)))
        };

        let Some(router) = self.bucket_router.as_ref() else {
            return self.send_arrow_batch(batch, physical_table_path, None);
        };
        if batch.num_rows() == 0 {
            return self.send_arrow_batch(batch, physical_table_path, None);
        }

        // Group rows by bucket, keeping one key per bucket (it hashes back there).
        let num_buckets = self.table_info.get_num_buckets();
        let batch_arc = Arc::new(batch.clone());
        let row_type = Arc::new(self.table_info.row_type.clone());
        let mut groups: HashMap<i32, (Vec<u32>, Bytes)> = HashMap::new();
        for i in 0..batch.num_rows() {
            let row = ColumnarRow::new(Arc::clone(&batch_arc), Arc::clone(&row_type), i, None)?;
            let (bucket, key) = router.bucket_of(&row, num_buckets)?;
            groups
                .entry(bucket)
                .or_insert_with(|| (Vec::new(), key))
                .0
                .push(i as u32);
        }

        if groups.len() == 1 {
            let (_, (_, rep_key)) = groups.into_iter().next().unwrap();
            return self.send_arrow_batch(batch, physical_table_path, Some(rep_key));
        }

        let mut handles = Vec::with_capacity(groups.len());
        for (_bucket, (indices, rep_key)) in groups {
            let sub_batch = take_rows(&batch, &indices)?;
            let record = WriteRecord::for_append_record_batch(
                Arc::clone(&self.table_info),
                Arc::clone(&physical_table_path),
                self.table_info.schema_id,
                sub_batch,
            )
            .with_bucket_key(Some(rep_key));
            handles.push(self.writer_client.send(&record)?);
        }
        Ok(WriteResultFuture::join(handles))
    }

    fn send_arrow_batch(
        &self,
        batch: RecordBatch,
        physical_table_path: Arc<PhysicalTablePath>,
        bucket_key: Option<Bytes>,
    ) -> Result<WriteResultFuture> {
        let record = WriteRecord::for_append_record_batch(
            Arc::clone(&self.table_info),
            physical_table_path,
            self.table_info.schema_id,
            batch,
        )
        .with_bucket_key(bucket_key);
        let result_handle = self.writer_client.send(&record)?;
        Ok(WriteResultFuture::new(result_handle))
    }

    pub async fn flush(&self) -> Result<()> {
        self.writer_client.flush().await
    }
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
    let idx = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| arrow::compute::take(column, &idx, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| UnexpectedError {
            message: format!("Failed to split arrow batch by bucket: {e}"),
            source: None,
        })?;
    RecordBatch::try_new(batch.schema(), columns).map_err(|e| UnexpectedError {
        message: format!("Failed to rebuild split arrow batch: {e}"),
        source: None,
    })
}
