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

use crate::client::table::partition_getter::{PartitionGetter, get_physical_path};
use crate::client::{WriteRecord, WriteResultFuture, WriterClient};
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{PhysicalTablePath, TableInfo, TablePath};
use crate::row::{ColumnarRow, InternalRow};
use arrow::array::RecordBatch;
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

        Ok(AppendWriter {
            table_path: Arc::clone(&self.table_path),
            partition_getter,
            writer_client: self.writer_client.clone(),
            table_info: Arc::clone(&self.table_info),
        })
    }
}

pub struct AppendWriter {
    table_path: Arc<TablePath>,
    partition_getter: Option<PartitionGetter>,
    writer_client: Arc<WriterClient>,
    table_info: Arc<TableInfo>,
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
        let record = WriteRecord::for_append(
            Arc::clone(&self.table_info),
            physical_table_path,
            self.table_info.schema_id,
            row,
        );
        let result_handle = self.writer_client.send(&record)?;
        Ok(WriteResultFuture::new(result_handle))
    }

    /// Appends an Arrow RecordBatch to the table.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the write,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// For partitioned tables, the partition is derived from the **first row** of the batch.
    /// Callers must ensure all rows in the batch belong to the same partition.
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

        let record = WriteRecord::for_append_record_batch(
            Arc::clone(&self.table_info),
            physical_table_path,
            self.table_info.schema_id,
            batch,
        );
        let result_handle = self.writer_client.send(&record)?;
        Ok(WriteResultFuture::new(result_handle))
    }

    pub async fn flush(&self) -> Result<()> {
        self.writer_client.flush().await
    }
}
