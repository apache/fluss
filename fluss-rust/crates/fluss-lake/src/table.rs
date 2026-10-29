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
use fluss::metadata::{TableInfo, TablePath};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Entry point for source preparation over a lake-enabled Fluss table.
#[derive(Clone)]
pub struct FlussLakeTable {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
}

impl FlussLakeTable {
    /// Opens a table and validates that it is configured for lake reads.
    pub async fn open(connection: Arc<FlussConnection>, table_path: &TablePath) -> Result<Self> {
        let admin = connection
            .get_admin()
            .map_err(|error| planning_client_error("create Fluss admin client", error))?;
        let table_info = admin
            .get_table_info(table_path)
            .await
            .map_err(|error| planning_client_error("get table metadata", error))?;
        Self::try_from_table_info(connection, &table_info)
    }

    /// Creates a table from metadata already resolved by the caller.
    /// Preparation fetches fresh metadata before capturing source boundaries.
    pub fn try_from_table_info(
        connection: Arc<FlussConnection>,
        table_info: &TableInfo,
    ) -> Result<Self> {
        validate_lake_readable(table_info)?;
        Ok(Self {
            connection,
            table_path: table_info.table_path.clone(),
        })
    }

    /// Captures table-wide source boundaries without planning lake files.
    ///
    /// The result may be shared by scans with different projections and
    /// predicates, including separate references to the table in one query.
    /// This does not open a lake catalog or acquire a retention lease.
    pub async fn prepare(&self) -> Result<FlussLakeReadContext> {
        let admin = self
            .connection
            .get_admin()
            .map_err(|error| planning_client_error("create Fluss admin client", error))?;
        let table_info = admin
            .get_table_info(&self.table_path)
            .await
            .map_err(|error| planning_client_error("get table metadata", error))?;
        validate_lake_readable(&table_info)?;
        crate::boundary::freeze_read_context(&admin, &table_info).await
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

pub(crate) fn validate_lake_readable(table_info: &TableInfo) -> Result<()> {
    match table_info.table_config.is_datalake_enabled() {
        Ok(true) => {}
        Ok(false) => {
            return Err(FlussLakeError::NotLakeReadable(format!(
                "table {} does not enable table.datalake.enabled",
                table_info.table_path
            )));
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataTypes, Schema};
    use std::collections::HashMap;

    #[test]
    fn preconfigured_lake_format_is_not_readable_until_enabled() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .build()
            .unwrap();
        let table_info = TableInfo::new(
            TablePath::new("fluss", "orders"),
            7,
            1,
            schema,
            Vec::new(),
            Vec::<String>::new().into(),
            1,
            HashMap::from([("table.datalake.format".to_string(), "paimon".to_string())]),
            HashMap::new(),
            None,
            0,
            0,
        );

        assert!(matches!(
            validate_lake_readable(&table_info),
            Err(FlussLakeError::NotLakeReadable(_))
        ));
    }
}
