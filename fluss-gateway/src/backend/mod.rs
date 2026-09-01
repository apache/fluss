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

//! HTTP-independent Fluss operations used by protocol adapters.

pub mod client;
pub mod connection;
pub mod context;
pub mod errors;
pub mod types;

#[cfg(test)]
pub mod fake;

use crate::backend::context::RequestContext;
use crate::backend::types::ClusterId;
use crate::error::{GatewayError, GatewayResult, Resource};
use async_trait::async_trait;
use fluss::metadata::{
    AlterTableChanges, PartitionInfo, PartitionSpec, TableDescriptor, TableInfo, TablePath,
};
use fluss::record::ChangeType;
use fluss::row::GenericRow;
use std::collections::HashSet;

/// One batch decoded against one table metadata snapshot.
#[derive(Debug)]
pub struct WriteRequest {
    table: TableInfo,
    rows: Vec<GenericRow<'static>>,
    change_types: Vec<ChangeType>,
    partial_update_columns: Option<Vec<String>>,
}

impl WriteRequest {
    pub fn new(
        table: TableInfo,
        rows: Vec<GenericRow<'static>>,
        change_types: Vec<ChangeType>,
        partial_update_columns: Option<Vec<String>>,
    ) -> GatewayResult<Self> {
        if rows.is_empty() || rows.len() != change_types.len() {
            return Err(GatewayError::invalid_argument(
                "a write request needs at least one row and one change type per row",
            ));
        }
        let appends = change_types
            .iter()
            .filter(|change_type| matches!(change_type, ChangeType::AppendOnly))
            .count();
        if change_types.iter().any(|change_type| {
            !matches!(
                change_type,
                ChangeType::AppendOnly | ChangeType::Insert | ChangeType::Delete
            )
        }) || (appends != 0 && appends != change_types.len())
        {
            return Err(GatewayError::invalid_argument(
                "a write batch must contain only appends, or only upserts and deletes",
            ));
        }
        if let Some(columns) = &partial_update_columns {
            if appends != 0 || columns.is_empty() {
                return Err(GatewayError::invalid_argument(
                    "partial-update columns must be non-empty and cannot be used with appends",
                ));
            }
            let mut unique = HashSet::with_capacity(columns.len());
            if columns.iter().any(|column| !unique.insert(column)) {
                return Err(GatewayError::invalid_argument(
                    "partial-update columns must be unique",
                ));
            }
        }
        Ok(Self {
            table,
            rows,
            change_types,
            partial_update_columns,
        })
    }

    pub fn table(&self) -> &TablePath {
        &self.table.table_path
    }

    pub fn rows(&self) -> &[GenericRow<'static>] {
        &self.rows
    }

    pub fn partial_update_columns(&self) -> Option<&[String]> {
        self.partial_update_columns.as_deref()
    }

    pub(crate) fn is_append(&self) -> bool {
        matches!(self.change_types[0], ChangeType::AppendOnly)
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        TableInfo,
        Vec<GenericRow<'static>>,
        Vec<ChangeType>,
        Option<Vec<String>>,
    ) {
        (
            self.table,
            self.rows,
            self.change_types,
            self.partial_update_columns,
        )
    }
}

#[derive(Debug)]
pub struct WriteResult {
    pub row_count: u64,
    pub failures: Vec<RowWriteError>,
}

impl WriteResult {
    pub fn success_count(&self) -> u64 {
        self.row_count - self.error_count()
    }

    pub fn error_count(&self) -> u64 {
        self.failures.len() as u64
    }
}

#[derive(Debug)]
pub struct RowWriteError {
    pub index: usize,
    pub error: GatewayError,
}

/// The backend capabilities the protocol adapters depend on.
///
/// Native metadata crosses this boundary unchanged; protocol adapters own wire shapes.
#[async_trait]
pub trait FlussBackend: Send + Sync + 'static {
    /// The configured clusters, in lexical order.
    fn clusters(&self) -> Vec<ClusterId>;

    /// Whether `id` names a configured cluster.
    fn has_cluster(&self, id: &str) -> bool;

    async fn list_databases(&self, ctx: &RequestContext) -> GatewayResult<Vec<String>>;

    async fn create_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()>;

    /// Drops an empty database without cascading.
    async fn drop_database(&self, ctx: &RequestContext, database: &str) -> GatewayResult<()>;

    async fn list_tables(&self, ctx: &RequestContext, database: &str)
    -> GatewayResult<Vec<String>>;

    /// Returns the client's current snapshot, loading it on a metadata miss.
    async fn table_info(&self, ctx: &RequestContext, table: &TablePath)
    -> GatewayResult<TableInfo>;

    /// Describes one table, forcing a metadata refresh before returning it.
    ///
    /// Metadata crosses this boundary as the native `fluss-rs` type, which is the canonical model:
    /// it is what Fluss stores, so every adapter needs it, whereas a second gateway-owned model
    /// would be a copy each adapter has to learn in addition.
    async fn describe_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<TableInfo>;

    async fn create_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        descriptor: &TableDescriptor,
    ) -> GatewayResult<()>;

    /// Applies the change group in one native request.
    async fn alter_table(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        changes: AlterTableChanges,
    ) -> GatewayResult<()>;

    async fn drop_table(&self, ctx: &RequestContext, table: &TablePath) -> GatewayResult<()>;

    async fn list_partitions(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
    ) -> GatewayResult<Vec<PartitionInfo>>;

    async fn create_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()>;

    async fn drop_partition(
        &self,
        ctx: &RequestContext,
        table: &TablePath,
        spec: &PartitionSpec,
    ) -> GatewayResult<()>;

    /// Submits one batch of rows that has already been validated against the table schema.
    ///
    /// Setup errors precede submission. Completed calls report submission and acknowledgement
    /// failures per row. Cancelling this call does not currently cancel native writes.
    async fn write(
        &self,
        ctx: &RequestContext,
        request: WriteRequest,
    ) -> GatewayResult<WriteResult>;
}

/// Returns the error for an unconfigured cluster.
pub fn unknown_cluster(id: &str) -> GatewayError {
    GatewayError::not_found(format!("unknown cluster `{id}`")).with_resource(Resource::Cluster)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::fake::users_table_info;

    fn row(value: i32) -> GenericRow<'static> {
        let mut row = GenericRow::new(1);
        row.set_field(0, value);
        row.into_owned()
    }

    #[test]
    fn write_request_enforces_batch_shape() {
        WriteRequest::new(
            users_table_info(1),
            vec![row(1), row(2)],
            vec![ChangeType::Insert, ChangeType::Delete],
            Some(vec!["id".to_string()]),
        )
        .unwrap();

        for result in [
            WriteRequest::new(users_table_info(1), Vec::new(), Vec::new(), None),
            WriteRequest::new(
                users_table_info(1),
                vec![row(1), row(2)],
                vec![ChangeType::AppendOnly, ChangeType::Insert],
                None,
            ),
            WriteRequest::new(
                users_table_info(1),
                vec![row(1)],
                vec![ChangeType::Insert],
                Some(vec!["id".to_string(), "id".to_string()]),
            ),
        ] {
            assert_eq!(
                result.unwrap_err().kind(),
                crate::error::ErrorKind::InvalidArgument
            );
        }
    }
}
