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

use crate::client::{RowBytes, WriteFormat, WriteRecord, WriteResultFuture, WriterClient};
use crate::error::Error::{IllegalArgument, UnexpectedError};
use crate::error::Result;
use crate::metadata::{RowType, TableInfo, TablePath};
use crate::row::InternalRow;
use crate::row::encode::{KeyEncoder, KeyEncoderFactory, RowEncoder, RowEncoderFactory};
use crate::row::field_getter::FieldGetter;
use std::sync::{Arc, Mutex};

use crate::client::table::partition_getter::{PartitionGetter, get_physical_path};
use bitvec::prelude::bitvec;
use bytes::Bytes;

#[allow(dead_code)]
pub struct TableUpsert {
    table_path: TablePath,
    table_info: TableInfo,
    writer_client: Arc<WriterClient>,
    target_columns: Option<Arc<Vec<usize>>>,
}

#[allow(dead_code)]
impl TableUpsert {
    pub fn new(
        table_path: TablePath,
        table_info: TableInfo,
        writer_client: Arc<WriterClient>,
    ) -> Self {
        Self {
            table_path,
            table_info,
            writer_client,
            target_columns: None,
        }
    }

    pub fn partial_update(&self, target_columns: Option<Vec<usize>>) -> Result<Self> {
        if let Some(columns) = &target_columns {
            let num_columns = self.table_info.row_type().fields().len();

            if let Some(&invalid_column) = columns.iter().find(|&&col| col >= num_columns) {
                return Err(IllegalArgument {
                    message: format!(
                        "Invalid target column index: {invalid_column} for table {}. The table only has {num_columns} columns.",
                        self.table_path
                    ),
                });
            }
        }

        Ok(Self {
            table_path: self.table_path.clone(),
            table_info: self.table_info.clone(),
            writer_client: self.writer_client.clone(),
            target_columns: target_columns.map(Arc::new),
        })
    }

    pub fn partial_update_with_column_names(&self, target_column_names: &[&str]) -> Result<Self> {
        let row_type = self.table_info.row_type();
        let col_indices: Vec<(&str, Option<usize>)> = target_column_names
            .iter()
            .map(|col_name| (*col_name, row_type.get_field_index(col_name)))
            .collect();

        if let Some((missing_name, _)) = col_indices.iter().find(|(_, ix)| ix.is_none()) {
            return Err(IllegalArgument {
                message: format!(
                    "Cannot find target column `{}` for table {}.",
                    missing_name, self.table_path
                ),
            });
        }

        let valid_col_indices: Vec<usize> = col_indices
            .into_iter()
            .map(|(_, index)| index.unwrap())
            .collect();

        self.partial_update(Some(valid_col_indices))
    }

    pub fn create_writer(&self) -> Result<UpsertWriter> {
        UpsertWriterFactory::create(
            Arc::new(self.table_path.clone()),
            Arc::new(self.table_info.clone()),
            self.target_columns.clone(),
            Arc::clone(&self.writer_client),
        )
    }
}

pub struct UpsertWriter {
    table_path: Arc<TablePath>,
    writer_client: Arc<WriterClient>,
    partition_field_getter: Option<PartitionGetter>,
    primary_key_encoder: Mutex<Box<dyn KeyEncoder>>,
    target_columns: Option<Arc<Vec<usize>>>,
    // Use primary key encoder as bucket key encoder when None
    bucket_key_encoder: Option<Mutex<Box<dyn KeyEncoder>>>,
    write_format: WriteFormat,
    row_encoder: Mutex<Box<dyn RowEncoder>>,
    field_getters: Box<[FieldGetter]>,
    table_info: Arc<TableInfo>,
}

struct UpsertWriterFactory;

impl UpsertWriterFactory {
    pub fn create(
        table_path: Arc<TablePath>,
        table_info: Arc<TableInfo>,
        partial_update_columns: Option<Arc<Vec<usize>>>,
        writer_client: Arc<WriterClient>,
    ) -> Result<UpsertWriter> {
        let data_lake_format = &table_info.table_config.get_datalake_format()?;
        let row_type = table_info.row_type();
        let physical_pks = table_info.get_physical_primary_keys();

        let names = table_info.get_schema().auto_increment_col_names();

        Self::sanity_check(
            row_type,
            &table_info.primary_keys,
            names,
            &partial_update_columns,
        )?;

        let primary_key_encoder = KeyEncoderFactory::of(row_type, physical_pks, data_lake_format)?;
        let bucket_key_encoder = if !table_info.is_default_bucket_key() {
            Some(KeyEncoderFactory::of(
                row_type,
                table_info.get_bucket_keys(),
                data_lake_format,
            )?)
        } else {
            // Defaults to using primary key encoder when None for bucket key
            None
        };

        let kv_format = table_info.get_table_config().get_kv_format()?;
        let write_format = WriteFormat::from_kv_format(&kv_format)?;

        let field_getters = FieldGetter::create_field_getters(row_type);

        let partition_field_getter = if table_info.is_partitioned() {
            Some(PartitionGetter::new(
                row_type,
                Arc::clone(table_info.get_partition_keys()),
            )?)
        } else {
            None
        };

        Ok(UpsertWriter {
            table_path,
            partition_field_getter,
            writer_client,
            primary_key_encoder: Mutex::new(primary_key_encoder),
            target_columns: partial_update_columns,
            bucket_key_encoder: bucket_key_encoder.map(Mutex::new),
            write_format,
            row_encoder: Mutex::new(Box::new(RowEncoderFactory::create(
                kv_format,
                row_type.clone(),
            )?)),
            field_getters,
            table_info: table_info.clone(),
        })
    }

    #[allow(dead_code)]
    fn sanity_check(
        row_type: &RowType,
        primary_keys: &Vec<String>,
        auto_increment_col_names: &Vec<String>,
        target_columns: &Option<Arc<Vec<usize>>>,
    ) -> Result<()> {
        if target_columns.is_none() {
            if !auto_increment_col_names.is_empty() {
                return Err(IllegalArgument {
                    message: format!(
                        "This table has auto increment column {}. Explicitly specifying values for an auto increment column is not allowed. Please Specify non-auto-increment columns as target columns using partialUpdate first.",
                        auto_increment_col_names.join(", ")
                    ),
                });
            }
            return Ok(());
        }

        let field_count = row_type.fields().len();

        let mut target_column_set = bitvec![0; field_count];

        let columns = target_columns.as_ref().unwrap().as_ref();

        for &target_index in columns {
            target_column_set.set(target_index, true);
        }

        let mut pk_column_set = bitvec![0; field_count];

        // check the target columns contains the primary key
        for primary_key in primary_keys {
            let pk_index = row_type.get_field_index(primary_key.as_str());
            match pk_index {
                Some(pk_index) => {
                    if !target_column_set[pk_index] {
                        return Err(IllegalArgument {
                            message: format!(
                                "The target write columns {} must contain the primary key columns {}",
                                row_type.project(columns)?.get_field_names().join(", "),
                                primary_keys.join(", ")
                            ),
                        });
                    }
                    pk_column_set.set(pk_index, true);
                }
                None => {
                    return Err(IllegalArgument {
                        message: format!(
                            "The specified primary key {primary_key} is not in row type {row_type}"
                        ),
                    });
                }
            }
        }

        let mut auto_increment_column_set = bitvec![0; field_count];
        // explicitly specifying values for an auto increment column is not allowed
        for auto_increment_col_name in auto_increment_col_names {
            let auto_increment_field_index =
                row_type.get_field_index(auto_increment_col_name.as_str());

            if let Some(index) = auto_increment_field_index {
                if target_column_set[index] {
                    return Err(IllegalArgument {
                        message: format!(
                            "Explicitly specifying values for the auto increment column {auto_increment_col_name} is not allowed."
                        ),
                    });
                }

                auto_increment_column_set.set(index, true);
            }
        }

        // check the columns not in targetColumns should be nullable
        for i in 0..field_count {
            // column not in primary key and not in auto increment column
            if !pk_column_set[i] && !auto_increment_column_set[i] {
                // the column should be nullable
                if !row_type.fields().get(i).unwrap().data_type.is_nullable() {
                    return Err(IllegalArgument {
                        message: format!(
                            "Partial Update requires all columns except primary key to be nullable, but column {} is NOT NULL.",
                            row_type.fields().get(i).unwrap().name()
                        ),
                    });
                }
            }
        }

        Ok(())
    }
}

impl UpsertWriter {
    fn check_field_count<R: InternalRow>(&self, row: &R) -> Result<()> {
        let expected = self.table_info.get_row_type().fields().len();
        if row.get_field_count() != expected {
            return Err(IllegalArgument {
                message: format!(
                    "The field count of the row does not match the table schema. Expected: {}, Actual: {}",
                    expected,
                    row.get_field_count()
                ),
            });
        }
        Ok(())
    }

    fn get_keys(&self, row: &dyn InternalRow) -> Result<(Bytes, Option<Bytes>)> {
        let key = self
            .primary_key_encoder
            .lock()
            .map_err(|e| UnexpectedError {
                message: format!("primary_key_encoder lock poisoned: {e}"),
                source: None,
            })?
            .encode_key(row)?;
        let bucket_key = match &self.bucket_key_encoder {
            Some(encoder) => Some(
                encoder
                    .lock()
                    .map_err(|e| UnexpectedError {
                        message: format!("bucket_key_encoder lock poisoned: {e}"),
                        source: None,
                    })?
                    .encode_key(row)?,
            ),
            None => Some(key.clone()),
        };
        Ok((key, bucket_key))
    }

    fn encode_row<R: InternalRow>(&self, row: &R) -> Result<Bytes> {
        let mut encoder = self.row_encoder.lock().map_err(|e| UnexpectedError {
            message: format!("row_encoder lock poisoned: {e}"),
            source: None,
        })?;
        encoder.start_new_row()?;
        for (pos, field_getter) in self.field_getters.iter().enumerate() {
            let datum = field_getter.get_field(row)?;
            encoder.encode_field(pos, datum)?;
        }
        encoder.finish_row()
    }

    /// Flush data written that have not yet been sent to the server, forcing the client to send the
    /// requests to server and blocks on the completion of the requests associated with these
    /// records. A request is considered completed when it is successfully acknowledged according to
    /// the CLIENT_WRITER_ACKS configuration option you have specified or else it
    /// results in an error.
    pub async fn flush(&self) -> Result<()> {
        self.writer_client.flush().await
    }

    /// Inserts row into Fluss table if they do not already exist, or updates them if they do exist.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the write,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// # Arguments
    /// * row - the row to upsert.
    ///
    /// # Returns
    /// A [`WriteResultFuture`] that can be awaited to wait for server acknowledgment,
    /// or dropped for fire-and-forget behavior (use `flush()` to ensure delivery).
    pub fn upsert<R: InternalRow>(&self, row: &R) -> Result<WriteResultFuture> {
        self.check_field_count(row)?;

        let (key, bucket_key) = self.get_keys(row)?;

        let row_bytes: RowBytes<'_> = match row.as_encoded_bytes(self.write_format) {
            Some(bytes) => RowBytes::Borrowed(bytes),
            None => RowBytes::Owned(self.encode_row(row)?),
        };

        let write_record = WriteRecord::for_upsert(
            Arc::clone(&self.table_info),
            Arc::new(get_physical_path(
                &self.table_path,
                self.partition_field_getter.as_ref(),
                row,
            )?),
            self.table_info.schema_id,
            key,
            bucket_key,
            self.write_format,
            self.target_columns.clone(),
            Some(row_bytes),
        );

        let result_handle = self.writer_client.send(&write_record)?;
        Ok(WriteResultFuture::new(result_handle))
    }

    /// Delete certain row by the input row in Fluss table, the input row must contain the primary
    /// key.
    ///
    /// This method returns a [`WriteResultFuture`] immediately after queueing the delete,
    /// enabling fire-and-forget semantics for efficient batching.
    ///
    /// # Arguments
    /// * row - the row to delete (must contain the primary key fields).
    ///
    /// # Returns
    /// A [`WriteResultFuture`] that can be awaited to wait for server acknowledgment,
    /// or dropped for fire-and-forget behavior (use `flush()` to ensure delivery).
    pub fn delete<R: InternalRow>(&self, row: &R) -> Result<WriteResultFuture> {
        self.check_field_count(row)?;

        let (key, bucket_key) = self.get_keys(row)?;

        let write_record = WriteRecord::for_upsert(
            Arc::clone(&self.table_info),
            Arc::new(get_physical_path(
                &self.table_path,
                self.partition_field_getter.as_ref(),
                row,
            )?),
            self.table_info.schema_id,
            key,
            bucket_key,
            self.write_format,
            self.target_columns.clone(),
            None,
        );

        let result_handle = self.writer_client.send(&write_record)?;
        Ok(WriteResultFuture::new(result_handle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes};

    #[test]
    fn sanity_check() {
        // No target columns specified but table has auto-increment column
        let fields = vec![
            DataField::new("id", DataTypes::int().as_non_nullable(), None),
            DataField::new("name", DataTypes::string(), None),
        ];
        let row_type = RowType::new(fields);
        let primary_keys = vec!["id".to_string()];
        let auto_increment_col_names = vec!["id".to_string()];
        let target_columns = None;

        let result = UpsertWriterFactory::sanity_check(
            &row_type,
            &primary_keys,
            &auto_increment_col_names,
            &target_columns,
        );

        assert!(result.unwrap_err().to_string().contains(
            "This table has auto increment column id. Explicitly specifying values for an auto increment column is not allowed. Please Specify non-auto-increment columns as target columns using partialUpdate first."
        ));

        // Target columns do not contain primary key
        let fields = vec![
            DataField::new("id", DataTypes::int().as_non_nullable(), None),
            DataField::new("name", DataTypes::string(), None),
            DataField::new("value", DataTypes::int(), None),
        ];
        let row_type = RowType::new(fields);
        let primary_keys = vec!["id".to_string()];
        let auto_increment_col_names = vec![];
        let target_columns = Some(Arc::new(vec![1usize]));

        let result = UpsertWriterFactory::sanity_check(
            &row_type,
            &primary_keys,
            &auto_increment_col_names,
            &target_columns,
        );

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("The target write columns name must contain the primary key columns id")
        );

        // Primary key column not found in row type
        let fields = vec![
            DataField::new("id", DataTypes::int().as_non_nullable(), None),
            DataField::new("name", DataTypes::string(), None),
        ];
        let row_type = RowType::new(fields);
        let primary_keys = vec!["nonexistent_pk".to_string()];
        let auto_increment_col_names = vec![];
        let target_columns = Some(Arc::new(vec![0usize, 1]));

        let result = UpsertWriterFactory::sanity_check(
            &row_type,
            &primary_keys,
            &auto_increment_col_names,
            &target_columns,
        );

        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("The specified primary key nonexistent_pk is not in row type")
        );

        // Target columns include auto-increment column
        let fields = vec![
            DataField::new("id", DataTypes::int().as_non_nullable(), None),
            DataField::new("seq", DataTypes::bigint().as_non_nullable(), None),
            DataField::new("name", DataTypes::string(), None),
        ];
        let row_type = RowType::new(fields);
        let primary_keys = vec!["id".to_string()];
        let auto_increment_col_names = vec!["seq".to_string()];
        let target_columns = Some(Arc::new(vec![0usize, 1, 2]));

        let result = UpsertWriterFactory::sanity_check(
            &row_type,
            &primary_keys,
            &auto_increment_col_names,
            &target_columns,
        );

        assert!(result.unwrap_err().to_string().contains(
            "Explicitly specifying values for the auto increment column seq is not allowed."
        ));

        // Non-nullable column not in target columns (partial update requires nullable)
        let fields = vec![
            DataField::new("id", DataTypes::int().as_non_nullable(), None),
            DataField::new(
                "required_field",
                DataTypes::string().as_non_nullable(),
                None,
            ),
            DataField::new("optional_field", DataTypes::int(), None),
        ];
        let row_type = RowType::new(fields);
        let primary_keys = vec!["id".to_string()];
        let auto_increment_col_names = vec![];
        let target_columns = Some(Arc::new(vec![0usize]));

        let result = UpsertWriterFactory::sanity_check(
            &row_type,
            &primary_keys,
            &auto_increment_col_names,
            &target_columns,
        );

        assert!(result.unwrap_err().to_string().contains(
            "Partial Update requires all columns except primary key to be nullable, but column required_field is NOT NULL."
        ));
    }
}

/// The result of upserting a record
/// Currently this is an empty struct to allow for compatible evolution in the future
#[derive(Default)]
#[allow(dead_code)]
pub struct UpsertResult;

/// The result of deleting a record
/// Currently this is an empty struct to allow for compatible evolution in the future
#[derive(Default)]
#[allow(dead_code)]
pub struct DeleteResult;
