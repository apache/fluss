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

use crate::client::connection::FlussConnection;
use crate::client::metadata::Metadata;
use crate::client::schema_getter::ClientSchemaGetter;
use crate::error::{Error, Result};
use crate::metadata::{SchemaInfo, TableInfo, TablePath};
use std::sync::Arc;

pub const EARLIEST_OFFSET: i64 = -2;

mod append;
mod batch_scanner;
mod lookup;

mod log_fetch_buffer;
mod partition_getter;
mod read_context_resolver;
mod reader;
mod remote_log;
mod scanner;
mod upsert;

pub use append::{AppendWriter, TableAppend};
pub use batch_scanner::LimitBatchScanner;
pub use lookup::{LookupResult, Lookuper, PrefixKeyLookuper, TableLookup, TablePrefixLookup};
pub use reader::{RecordBatchLogReader, SyncRecordBatchLogReader};
pub use remote_log::{
    DEFAULT_REMOTE_FILE_DOWNLOAD_THREAD_NUM, DEFAULT_SCANNER_REMOTE_LOG_PREFETCH_NUM,
};
pub use scanner::{LogScanner, RecordBatchLogScanner, TableScan};
pub use upsert::{TableUpsert, UpsertWriter};

pub(super) fn unsupported_implicit_partition_operation(operation: &str) -> Error {
    Error::UnsupportedOperation {
        message: format!(
            "Rust client v1 does not support {operation} for implicit partitioned Fluss tables. \
             Full Rust PartitionComputer support is planned for v2."
        ),
    }
}

#[allow(dead_code)]
pub struct FlussTable<'a> {
    conn: &'a FlussConnection,
    metadata: Arc<Metadata>,
    table_info: TableInfo,
    table_path: TablePath,
    has_primary_key: bool,
}

impl<'a> FlussTable<'a> {
    pub fn new(conn: &'a FlussConnection, metadata: Arc<Metadata>, table_info: TableInfo) -> Self {
        FlussTable {
            conn,
            table_path: table_info.table_path.clone(),
            has_primary_key: table_info.has_primary_key(),
            table_info,
            metadata,
        }
    }

    pub fn new_append(&self) -> Result<TableAppend> {
        if self.has_primary_key {
            return Err(Error::UnsupportedOperation {
                message: "Append is only supported for log tables (without primary key)"
                    .to_string(),
            });
        }
        if self.table_info.has_partition_expressions() {
            return Err(unsupported_implicit_partition_operation("append"));
        }
        Ok(TableAppend::new(
            self.table_path.clone(),
            Arc::new(self.table_info.clone()),
            self.conn.get_or_create_writer_client()?,
        ))
    }

    pub fn new_scan(&self) -> TableScan<'_> {
        TableScan::new(self.conn, self.table_info.clone(), self.metadata.clone())
    }

    pub fn metadata(&self) -> &Arc<Metadata> {
        &self.metadata
    }

    pub fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    pub fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    pub fn has_primary_key(&self) -> bool {
        self.has_primary_key
    }

    /// Creates a new `TableLookup` for configuring lookup operations.
    ///
    /// This follows the same pattern as `new_scan()` and `new_append()`,
    /// returning a configuration object that can be used to create a `Lookuper`.
    ///
    /// The table must have a primary key (be a primary key table).
    ///
    /// # Returns
    /// * `Ok(TableLookup)` - A lookup configuration object
    /// * `Err(Error)` - If the table doesn't have a primary key
    ///
    /// # Example
    /// ```ignore
    /// let table = conn.get_table(&table_path).await?;
    /// let lookuper = table.new_lookup()?.create_lookuper()?;
    /// let key = vec![1, 2, 3]; // encoded primary key bytes
    /// if let Some(value) = lookuper.lookup(key).await? {
    ///     println!("Found value: {:?}", value);
    /// }
    /// ```
    pub fn new_lookup(&self) -> Result<TableLookup> {
        if !self.has_primary_key {
            return Err(Error::UnsupportedOperation {
                message: "Lookup is only supported for primary key tables".to_string(),
            });
        }
        if self.table_info.has_partition_expressions() {
            return Err(unsupported_implicit_partition_operation("lookup"));
        }
        let lookup_client = self.conn.get_or_create_lookup_client()?;
        // Pre-seed the schema getter with the table's current schema —
        // rows written under it (the dominant case) never trigger an RPC.
        let latest = SchemaInfo::new(
            self.table_info.get_schema().clone(),
            self.table_info.get_schema_id(),
        );
        let schema_getter = Arc::new(ClientSchemaGetter::new(
            self.table_path.clone(),
            self.conn.get_admin()?,
            latest,
        ));
        Ok(TableLookup::new(
            lookup_client,
            self.table_info.clone(),
            self.metadata.clone(),
            schema_getter,
        ))
    }

    pub fn new_upsert(&self) -> Result<TableUpsert> {
        if !self.has_primary_key {
            return Err(Error::UnsupportedOperation {
                message: "Upsert is only supported for primary key tables".to_string(),
            });
        }
        if self.table_info.has_partition_expressions() {
            return Err(unsupported_implicit_partition_operation("upsert"));
        }

        Ok(TableUpsert::new(
            self.table_path.clone(),
            self.table_info.clone(),
            self.conn.get_or_create_writer_client()?,
        ))
    }
}

impl<'a> Drop for FlussTable<'a> {
    fn drop(&mut self) {
        // do-nothing now
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_implicit_partition_operation_returns_unsupported_operation() {
        let err = unsupported_implicit_partition_operation("append");

        match err {
            Error::UnsupportedOperation { message } => {
                assert!(message.contains("append"));
                assert!(message.contains("implicit partitioned Fluss tables"));
                assert!(message.contains("planned for v2"));
            }
            _ => panic!("expected UnsupportedOperation"),
        }
    }
}
