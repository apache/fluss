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

//! Table entry point for capturing reusable UnionRead source boundaries.

use crate::error::planning_client_error;
use crate::{FlussLakeError, FlussLakeReadContext, Result};
use fluss::client::FlussConnection;
use fluss::metadata::{SchemaInfo, TableInfo, TablePath};
use fluss::predicate::Predicate;
use fluss::protocol::PbUnionReadContext;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Reusable table handle for capturing bounded source inputs.
#[derive(Clone)]
pub struct FlussLakeTable {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
}

impl FlussLakeTable {
    /// Creates a reusable table handle without fetching metadata or capturing boundaries.
    ///
    /// Table existence and configuration are checked by
    /// [`Self::capture_read_context`] or its partition-scoped variant.
    pub fn new(connection: Arc<FlussConnection>, table_path: &TablePath) -> Self {
        Self {
            connection,
            table_path: table_path.clone(),
        }
    }

    /// Captures table-wide source boundaries without planning lake files.
    ///
    /// The result may be shared by scans with different projections and
    /// predicates, including separate references to the table in one query.
    /// This does not open a lake catalog or acquire a retention lease.
    pub async fn capture_read_context(&self) -> Result<FlussLakeReadContext> {
        self.capture(None).await
    }

    /// Captures partitions matching a predicate over partition columns.
    /// Selection precedes offset queries and constrains lake planning as well.
    /// The predicate must be exactly representable by the shared wire format.
    pub async fn capture_read_context_with_partition_scope(
        &self,
        scope: &Predicate,
    ) -> Result<FlussLakeReadContext> {
        self.capture(Some(scope)).await
    }

    /// Resolves an engine-transmitted boundary without capturing new offsets or snapshots.
    /// A newer live schema is allowed: the schema named in the boundary is loaded.
    /// A table drop/recreate or incompatible source mapping fails the import. This
    /// resolves metadata once; retain the returned context for subsequent reads.
    pub async fn import_read_context(
        &self,
        boundary: &PbUnionReadContext,
    ) -> Result<FlussLakeReadContext> {
        use crate::read_context::protocol::{required, validate_wire_identity};
        let admin = self
            .connection
            .get_admin()
            .map_err(|e| planning_client_error("create Fluss admin client", e))?;
        let live = admin
            .get_table_info(&self.table_path)
            .await
            .map_err(|e| planning_client_error("resolve boundary table", e))?;
        validate_wire_identity(boundary, &live)?;
        let schema_id = required(boundary.schema_id, "schema_id")?;
        let schema = if schema_id == live.schema_id {
            SchemaInfo::new(live.schema.clone(), live.schema_id)
        } else {
            admin
                .get_table_schema(&self.table_path, Some(schema_id))
                .await
                .map_err(|e| planning_client_error("resolve captured schema", e))?
        };
        let context = FlussLakeReadContext::from_protobuf(boundary, &live, &schema)?;
        if schema_id != live.schema_id {
            // Schema lookup is name-based too. Never accept a replacement table's schema.
            let current = admin
                .get_table_info(&self.table_path)
                .await
                .map_err(|e| planning_client_error("recheck boundary table", e))?;
            context.validate_table(&current)?;
        }
        Ok(context)
    }

    async fn capture(&self, scope: Option<&Predicate>) -> Result<FlussLakeReadContext> {
        let admin = self
            .connection
            .get_admin()
            .map_err(|error| planning_client_error("create Fluss admin client", error))?;
        let table_info = admin
            .get_table_info(&self.table_path)
            .await
            .map_err(|error| planning_client_error("get table metadata", error))?;
        validate_readable_table(&table_info)?;
        crate::boundary::freeze_read_context(&admin, &table_info, scope).await
    }
}

impl Debug for FlussLakeTable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FlussLakeTable")
            .field("table_path", &self.table_path)
            .finish_non_exhaustive()
    }
}

/// Resolve the physical lake name without changing the logical Fluss identity.
pub(crate) fn resolve_lake_table_path(table_info: &TableInfo) -> Result<TablePath> {
    let database = table_info
        .properties
        .get("table.datalake.database-name")
        .map(String::as_str)
        .unwrap_or(table_info.table_path.database());
    let table = table_info
        .properties
        .get("table.datalake.table-name")
        .map(String::as_str)
        .unwrap_or(table_info.table_path.table());
    if database.is_empty() || table.is_empty() {
        return Err(FlussLakeError::PlanningFailed(
            "lake database and table names must not be empty".to_string(),
        ));
    }
    Ok(TablePath::new(database, table))
}

pub(crate) fn validate_readable_table(table_info: &TableInfo) -> Result<()> {
    validate_supported_table(table_info)?;
    match table_info.table_config.is_datalake_enabled() {
        Ok(true) => {}
        Ok(false) => return Ok(()),
        Err(error) => {
            return Err(FlussLakeError::NotLakeReadable(format!(
                "table {} has an invalid lake configuration: {error}",
                table_info.table_path
            )));
        }
    }
    match table_info.table_config.get_datalake_format() {
        Ok(Some(_)) => Ok(()),
        Ok(None) => Err(FlussLakeError::NotLakeReadable(format!(
            "table {} does not define table.datalake.format",
            table_info.table_path
        ))),
        Err(error) => Err(FlussLakeError::NotLakeReadable(format!(
            "table {} has an invalid lake configuration: {error}",
            table_info.table_path
        ))),
    }
}

// Ordinary partition listings omit the shared historical log. Until its ranges
// participate in capture and reconciliation, accepting this table can lose ACKed writes.
pub(crate) fn validate_supported_table(table: &TableInfo) -> Result<()> {
    const OPTION: &str = "table.datalake.historical-partition.enabled";
    match table.table_config.properties.get(OPTION) {
        None => Ok(()),
        Some(value) if value.eq_ignore_ascii_case("false") => Ok(()),
        Some(value) if value.eq_ignore_ascii_case("true") => {
            Err(FlussLakeError::NotLakeReadable(format!(
                "table {} enables {OPTION}; shared historical-log reads are not supported",
                table.table_path
            )))
        }
        Some(value) => Err(FlussLakeError::NotLakeReadable(format!(
            "table {} has invalid {OPTION} {value:?}: expected true or false",
            table.table_path
        ))),
    }
}
