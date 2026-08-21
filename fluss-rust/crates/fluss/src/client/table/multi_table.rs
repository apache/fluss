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

use crate::client::admin::FlussAdmin;
use crate::client::table::{AppendWriter, TableAppend, TableUpsert, UpsertWriter};
use crate::client::{WriteResultFuture, WriterClient};
use crate::error::{Error, Result};
use crate::metadata::{TableInfo, TablePath};
use crate::row::InternalRow;
use std::collections::HashMap;
use std::sync::Arc;

/// An operation supported by [`MultiTableWriter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultiTableWriteOperation {
    /// Append a row to a log table.
    Append,
    /// Insert or update a row in a primary-key table.
    Upsert,
    /// Delete a row from a primary-key table.
    Delete,
}

/// A row and its target table, operation, and write-time schema id.
pub struct MultiTableWriteRecord<'a, R: InternalRow> {
    table_path: TablePath,
    operation: MultiTableWriteOperation,
    row: &'a R,
    schema_id: i32,
}

impl<'a, R: InternalRow> MultiTableWriteRecord<'a, R> {
    /// Creates an append record for a log table.
    pub fn for_append(table_path: &TablePath, row: &'a R, schema_id: i32) -> Self {
        Self::new(table_path, MultiTableWriteOperation::Append, row, schema_id)
    }

    /// Creates an upsert record for a primary-key table.
    pub fn for_upsert(table_path: &TablePath, row: &'a R, schema_id: i32) -> Self {
        Self::new(table_path, MultiTableWriteOperation::Upsert, row, schema_id)
    }

    /// Creates a delete record for a primary-key table.
    pub fn for_delete(table_path: &TablePath, row: &'a R, schema_id: i32) -> Self {
        Self::new(table_path, MultiTableWriteOperation::Delete, row, schema_id)
    }

    fn new(
        table_path: &TablePath,
        operation: MultiTableWriteOperation,
        row: &'a R,
        schema_id: i32,
    ) -> Self {
        Self {
            table_path: table_path.clone(),
            operation,
            row,
            schema_id,
        }
    }

    /// Returns the target table.
    pub fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    /// Returns the write operation.
    pub fn operation(&self) -> MultiTableWriteOperation {
        self.operation
    }

    /// Returns the row payload.
    pub fn row(&self) -> &R {
        self.row
    }

    /// Returns the schema id used to encode the row.
    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }
}

/// Routes records to multiple log and primary-key tables.
///
/// The writer is not thread-safe. It lazily resolves table metadata and caches an existing
/// [`AppendWriter`] or [`UpsertWriter`] for each `(table path, schema id)` pair.
pub struct MultiTableWriter {
    admin: Arc<FlussAdmin>,
    writer_client: Arc<WriterClient>,
    states: HashMap<(TablePath, i32), TableWriteState>,
}

impl MultiTableWriter {
    pub(crate) fn new(admin: Arc<FlussAdmin>, writer_client: Arc<WriterClient>) -> Self {
        Self {
            admin,
            writer_client,
            states: HashMap::new(),
        }
    }

    /// Resolves the target table and queues the record for writing.
    ///
    /// Await this method to finish metadata resolution. The returned [`WriteResultFuture`] can be
    /// awaited for per-record acknowledgment or dropped when [`Self::flush`] is used later.
    pub async fn write<R: InternalRow>(
        &mut self,
        record: MultiTableWriteRecord<'_, R>,
    ) -> Result<WriteResultFuture> {
        if !(0..=i16::MAX as i32).contains(&record.schema_id) {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Invalid schema id {} for table {}; expected a value from 0 to {}",
                    record.schema_id,
                    record.table_path,
                    i16::MAX
                ),
            });
        }

        let key = (record.table_path.clone(), record.schema_id);
        if !self.states.contains_key(&key) {
            let table_info = self
                .resolve_table_info(&record.table_path, record.schema_id)
                .await?;
            let state = TableWriteState::new(
                &record.table_path,
                table_info,
                Arc::clone(&self.writer_client),
            )?;
            self.states.insert(key.clone(), state);
        }

        self.states
            .get(&key)
            .expect("write state was inserted")
            .write(record.operation, record.row)
    }

    /// Flushes all pending writes queued through the connection's writer client.
    pub async fn flush(&self) -> Result<()> {
        self.writer_client.flush().await
    }

    async fn resolve_table_info(
        &self,
        table_path: &TablePath,
        schema_id: i32,
    ) -> Result<TableInfo> {
        let latest = self.admin.get_table_info(table_path).await?;

        if schema_id > latest.get_schema_id() {
            return Err(Error::IllegalArgument {
                message: format!(
                    "Schema id mismatch for table {table_path}: record schema id {schema_id} is ahead of the current table schema id {}",
                    latest.get_schema_id()
                ),
            });
        }
        if schema_id == latest.get_schema_id() {
            return Ok(latest);
        }

        let schema_info = self
            .admin
            .get_table_schema(table_path, Some(schema_id))
            .await?;
        if schema_info.schema_id() != schema_id {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Requested schema id {schema_id} for table {table_path}, but server returned {}",
                    schema_info.schema_id()
                ),
                source: None,
            });
        }

        Ok(TableInfo::new(
            table_path.clone(),
            latest.get_table_id(),
            schema_id,
            schema_info.into_parts().0,
            latest.get_bucket_keys().to_vec(),
            Arc::clone(latest.get_partition_keys()),
            latest.get_num_buckets(),
            latest.get_properties().clone(),
            latest.get_custom_properties().clone(),
            latest.get_comment().map(str::to_owned),
            latest.get_created_time(),
            latest.get_modified_time(),
        ))
    }
}

enum TableWriteState {
    Append(AppendWriter),
    Upsert(UpsertWriter),
}

impl TableWriteState {
    fn new(
        table_path: &TablePath,
        table_info: TableInfo,
        writer_client: Arc<WriterClient>,
    ) -> Result<Self> {
        if table_info.has_primary_key() {
            Ok(Self::Upsert(
                TableUpsert::new(table_path.clone(), table_info, writer_client).create_writer()?,
            ))
        } else {
            Ok(Self::Append(
                TableAppend::new(table_path.clone(), Arc::new(table_info), writer_client)
                    .create_writer()?,
            ))
        }
    }

    fn write<R: InternalRow>(
        &self,
        operation: MultiTableWriteOperation,
        row: &R,
    ) -> Result<WriteResultFuture> {
        match (self, operation) {
            (Self::Append(writer), MultiTableWriteOperation::Append) => writer.append(row),
            (Self::Upsert(writer), MultiTableWriteOperation::Upsert) => writer.upsert(row),
            (Self::Upsert(writer), MultiTableWriteOperation::Delete) => writer.delete(row),
            (Self::Append(_), operation) => Err(Error::UnsupportedOperation {
                message: format!("Operation {operation:?} is not supported for a log table"),
            }),
            (Self::Upsert(_), MultiTableWriteOperation::Append) => {
                Err(Error::UnsupportedOperation {
                    message: "Append is not supported for a primary-key table".to_string(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row::GenericRow;

    #[test]
    fn write_record_factories_preserve_fields() {
        let table_path = TablePath::new("db", "table");
        let row = GenericRow::new(1);

        let append = MultiTableWriteRecord::for_append(&table_path, &row, 1);
        assert_eq!(append.table_path(), &table_path);
        assert_eq!(append.operation(), MultiTableWriteOperation::Append);
        assert_eq!(append.row().get_field_count(), 1);
        assert_eq!(append.schema_id(), 1);

        let upsert = MultiTableWriteRecord::for_upsert(&table_path, &row, 2);
        assert_eq!(upsert.operation(), MultiTableWriteOperation::Upsert);

        let delete = MultiTableWriteRecord::for_delete(&table_path, &row, 3);
        assert_eq!(delete.operation(), MultiTableWriteOperation::Delete);
    }
}
