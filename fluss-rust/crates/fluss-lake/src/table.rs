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

//! Table and scan APIs for bounded UnionRead.

use crate::error::planning_client_error;
use crate::planner::{plan_union_read, plan_with_context};
use crate::{
    FlussLakeError, FlussLakeReadContext, FlussLakeReadPlan, FlussLakeReader, LakeSource, Result,
};
use fluss::client::FlussConnection;
use fluss::metadata::{PartitionSpec, RowType, TableInfo, TablePath};
use fluss::predicate::{BoundPredicate, Predicate};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Projection requested by a scan before it is resolved against fresh metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
enum FlussLakeProjection {
    Indices(Vec<usize>),
    Names(Vec<String>),
}

/// Schema-bound scan settings shared by the planner and executor.
pub(crate) struct BoundScan {
    pub(crate) projection: Option<Vec<usize>>,
    pub(crate) filter: BoundPredicate,
}

/// Entry point for bounded reads over a Fluss table.
#[derive(Clone)]
pub struct FlussLakeTable {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    catalog_property_overrides: HashMap<String, String>,
}

impl FlussLakeTable {
    /// Opens a table for bounded Fluss-only or lake and log reads.
    pub async fn open(connection: Arc<FlussConnection>, table_path: &TablePath) -> Result<Self> {
        Self::open_with_properties(connection, table_path, HashMap::new()).await
    }

    /// Opens a table with runtime lake properties such as storage credentials.
    ///
    /// Runtime properties are held by the table and reader. They are never
    /// serialized into a read split.
    pub async fn open_with_properties(
        connection: Arc<FlussConnection>,
        table_path: &TablePath,
        catalog_property_overrides: HashMap<String, String>,
    ) -> Result<Self> {
        let admin = connection
            .get_admin()
            .map_err(|error| planning_client_error("create Fluss admin client", error))?;
        let table_info = admin
            .get_table_info(table_path)
            .await
            .map_err(|error| planning_client_error("get table metadata", error))?;
        validate_readable_table(&table_info)?;
        Ok(Self {
            connection,
            table_path: table_path.clone(),
            catalog_property_overrides,
        })
    }

    /// Creates a table from metadata already resolved by the caller.
    pub fn try_from_table_info(
        connection: Arc<FlussConnection>,
        table_info: &TableInfo,
    ) -> Result<Self> {
        validate_readable_table(table_info)?;
        Ok(Self {
            connection,
            table_path: table_info.table_path.clone(),
            catalog_property_overrides: HashMap::new(),
        })
    }

    /// Creates a bounded read scan.
    pub fn new_scan(&self) -> FlussLakeScan {
        FlussLakeScan {
            connection: self.connection.clone(),
            table_path: self.table_path.clone(),
            projection: None,
            filter: None,
            batch_size: None,
            lake_only: false,
            lake_source: None,
            catalog_property_overrides: self.catalog_property_overrides.clone(),
        }
    }

    /// Captures table-wide source boundaries without planning lake files.
    ///
    /// The result may be shared by scans with different projections and
    /// predicates, including separate references to the table in one query.
    /// This does not require a lake reader or acquire a retention lease.
    pub async fn capture_read_context(&self) -> Result<FlussLakeReadContext> {
        self.capture_read_context_with_partition_scope(&PartitionSpec::new(
            HashMap::<String, String>::new(),
        ))
        .await
    }

    /// Captures boundaries within an exact, possibly partial partition spec.
    /// The scope constrains both lake and log planning, including lake-only partitions.
    /// It is an input restriction, not an optional pruning hint.
    pub async fn capture_read_context_with_partition_scope(
        &self,
        scope: &PartitionSpec,
    ) -> Result<FlussLakeReadContext> {
        let admin = self
            .connection
            .get_admin()
            .map_err(|e| planning_client_error("create Fluss admin client", e))?;
        let info = admin
            .get_table_info(&self.table_path)
            .await
            .map_err(|e| planning_client_error("get table metadata", e))?;
        validate_readable_table(&info)?;
        crate::boundary::freeze_read_context(&admin, &info, scope).await
    }
}

impl Debug for FlussLakeTable {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FlussLakeTable")
            .field("table_path", &self.table_path)
            .field(
                "catalog_property_override_count",
                &self.catalog_property_overrides.len(),
            )
            .finish_non_exhaustive()
    }
}

/// Immutable configuration for one bounded UnionRead.
///
/// Planning freezes source inputs into tasks. Both local readers and distributed
/// workers must use the same scan configuration that produced those tasks.
#[derive(Clone)]
pub struct FlussLakeScan {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    projection: Option<FlussLakeProjection>,
    filter: Option<Predicate>,
    batch_size: Option<usize>,
    lake_only: bool,
    lake_source: Option<Arc<dyn crate::LakeSource>>,
    catalog_property_overrides: HashMap<String, String>,
}

impl FlussLakeScan {
    /// Restricts output to table field indexes, in the requested order.
    /// An empty projection preserves row counts in zero-column output batches.
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(FlussLakeProjection::Indices(projection));
        self
    }

    /// Restricts output to table field names, in the requested order.
    pub fn with_projection_by_names(mut self, projection: Vec<String>) -> Self {
        self.projection = Some(FlussLakeProjection::Names(projection));
        self
    }

    /// Adds an exact filter. Repeated calls are combined with `AND`.
    /// Unsupported predicates fail during schema binding; they are not silently
    /// downgraded to pruning hints. PK filters run after reconciliation.
    pub fn with_filter(mut self, filter: Predicate) -> Self {
        self.filter = Some(match self.filter.take() {
            Some(existing) => existing.and(filter),
            None => filter,
        });
        self
    }

    /// Sets the target output batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Enables or disables lake-only execution.
    /// Requires a lake-enabled table; before its first snapshot the result is empty.
    pub fn with_lake_only(mut self, lake_only: bool) -> Self {
        self.lake_only = lake_only;
        self
    }

    /// Prepares source boundaries and builds the complete default read plan.
    ///
    /// Safe string partition equalities restrict offset capture before lake planning.
    /// Other predicates still participate in planning and exact result filtering.
    /// Use `FlussLakeTable::capture_read_context` for a table-wide boundary shared by scans.
    ///
    /// Create the reader using this scan's `new_reader()`. Lake planning and reading
    /// use the injected LakeSource.
    pub async fn plan(&self) -> Result<FlussLakeReadPlan> {
        plan_union_read(self).await
    }

    /// Creates a reader without planning or refreshing read boundaries.
    ///
    /// The caller must use the planner's projection, filter, read mode and batch
    /// size, plus a compatible lake backend and runtime catalog mapping. Workers
    /// restore these settings using the engine's own transport. They are not
    /// carried in splits or checked against a plan. Mismatches are caller errors
    /// and can produce incomplete results or the wrong schema.
    ///
    /// Splits must come from a trusted coordinator. Version, table/schema,
    /// layout and payload checks still apply, but do not authenticate a task.
    /// Credentials stay local. Configuration and metadata validation is
    /// deferred until reading; constructing this reader performs no I/O.
    pub fn new_reader(&self) -> FlussLakeReader {
        FlussLakeReader::from_scan(self.clone())
    }

    /// Builds a default plan without choosing new snapshots or log offsets.
    ///
    /// The context must refer to this table. Metadata drift is rejected.
    /// Different scans may share a context within its captured partition scope
    /// without sharing predicates or projection. The supplied context is never
    /// narrowed or recaptured from this scan's filter. Engines implementing their
    /// own UnionRead can consume the public context and skip this default planner.
    pub async fn plan_with_context(
        &self,
        context: &FlussLakeReadContext,
    ) -> Result<FlussLakeReadPlan> {
        plan_with_context(self, context).await
    }

    /// Use a lake backend for task planning and baseline reading.
    /// The source owns both task planning and reading; it may delegate either
    /// operation to another compatible source. It must match the table format.
    pub fn with_lake_source(mut self, source: Arc<dyn crate::LakeSource>) -> Self {
        self.lake_source = Some(source);
        self
    }

    pub(crate) fn resolve_lake_source(&self, info: &TableInfo) -> Result<Arc<dyn LakeSource>> {
        let format = info
            .table_config
            .get_datalake_format()
            .map_err(|e| FlussLakeError::PlanningFailed(e.to_string()))?
            .ok_or_else(|| FlussLakeError::NotLakeReadable("missing lake format".to_string()))?
            .to_string();
        if let Some(source) = &self.lake_source {
            if source.format() != format {
                return Err(FlussLakeError::PlanningFailed(
                    "LakeSource format does not match the table".to_string(),
                ));
            }
            return Ok(source.clone());
        }
        Err(FlussLakeError::PlanningFailed(format!(
            "no LakeSource for {format}; supply one with with_lake_source"
        )))
    }

    /// Resolves metadata and scan settings once before freezing the source inputs.
    pub(crate) async fn prepare_bound(
        &self,
    ) -> Result<(TableInfo, FlussLakeReadContext, BoundScan)> {
        self.validate_configuration()?;
        let admin = self
            .connection
            .get_admin()
            .map_err(|error| planning_client_error("create Fluss admin client", error))?;
        let table_info = admin
            .get_table_info(&self.table_path)
            .await
            .map_err(|error| planning_client_error("get table metadata", error))?;
        validate_readable_table(&table_info)?;
        let bound = self.bind(table_info.row_type())?;
        let scope = crate::pruning::PartitionPruner::new(
            table_info.row_type(),
            &table_info.partition_keys,
            &bound.filter,
        )
        .partition_scope();
        let context = crate::boundary::freeze_read_context(&admin, &table_info, &scope).await?;
        Ok((table_info, context, bound))
    }

    /// Shares predicate coercion and projection resolution between planning and reading.
    pub(crate) fn bind(&self, row_type: &RowType) -> Result<BoundScan> {
        Ok(BoundScan {
            projection: self.resolve_projection(row_type)?,
            filter: BoundPredicate::bind(self.filter(), row_type).map_err(|error| {
                FlussLakeError::PlanningFailed(format!("failed to bind filter predicate: {error}"))
            })?,
        })
    }

    pub(crate) fn connection(&self) -> &Arc<FlussConnection> {
        &self.connection
    }

    pub(crate) fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    fn resolve_projection(&self, row_type: &RowType) -> Result<Option<Vec<usize>>> {
        let projection = match &self.projection {
            Some(FlussLakeProjection::Indices(projection)) => projection.clone(),
            Some(FlussLakeProjection::Names(names)) => {
                let mut projection = Vec::with_capacity(names.len());
                for name in names {
                    let field_index = row_type.get_field_index(name).ok_or_else(|| {
                        FlussLakeError::PlanningFailed(format!(
                            "output projection references unknown field '{name}'"
                        ))
                    })?;
                    projection.push(field_index);
                }
                projection
            }
            None => return Ok(None),
        };

        let mut seen = std::collections::HashSet::with_capacity(projection.len());
        for field_index in &projection {
            if *field_index >= row_type.fields().len() {
                return Err(FlussLakeError::PlanningFailed(format!(
                    "output projection field index {field_index} exceeds table width {}",
                    row_type.fields().len()
                )));
            }
            if !seen.insert(*field_index) {
                return Err(FlussLakeError::PlanningFailed(format!(
                    "output projection contains duplicate field index {field_index}"
                )));
            }
        }
        Ok(Some(projection))
    }

    pub(crate) fn filter(&self) -> Option<&Predicate> {
        self.filter.as_ref()
    }

    pub(crate) fn batch_size(&self) -> Option<usize> {
        self.batch_size
    }

    pub(crate) fn lake_only(&self) -> bool {
        self.lake_only
    }

    pub(crate) fn validate_configuration(&self) -> Result<()> {
        if self.table_path.database().is_empty() || self.table_path.table().is_empty() {
            return Err(FlussLakeError::PlanningFailed(
                "database and table names must not be empty".to_string(),
            ));
        }
        if self.batch_size == Some(0) {
            return Err(FlussLakeError::PlanningFailed(
                "batch size must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

impl Debug for FlussLakeScan {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FlussLakeScan")
            .field("table_path", &self.table_path)
            .field("projection", &self.projection)
            .field("filter", &self.filter)
            .field("batch_size", &self.batch_size)
            .field("lake_only", &self.lake_only)
            .field(
                "catalog_property_override_count",
                &self.catalog_property_overrides.len(),
            )
            .finish()
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

pub(crate) fn validate_pk_union_merge_engine(table_info: &TableInfo) -> Result<()> {
    let merge_engine = table_info
        .table_config
        .get_merge_engine_type()
        .map_err(|error| {
            FlussLakeError::PlanningFailed(format!(
                "failed to resolve the merge engine of {}: {error}",
                table_info.table_path
            ))
        })?;
    if let Some(merge_engine) = merge_engine {
        return Err(FlussLakeError::UnsupportedMergeEngine(format!(
            "primary-key UnionRead only supports the default deduplicate semantics, but table {} uses table.merge-engine={merge_engine}",
            table_info.table_path
        )));
    }
    Ok(())
}

pub(crate) fn physical_primary_key_indexes(table_info: &TableInfo) -> Result<Vec<usize>> {
    table_info
        .get_physical_primary_keys()
        .iter()
        .map(|name| {
            table_info.row_type().get_field_index(name).ok_or_else(|| {
                FlussLakeError::PlanningFailed(format!(
                    "physical primary-key column '{name}' is missing from table {}",
                    table_info.table_path
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataTypes, Schema};

    #[test]
    fn disabled_lake_configuration_does_not_require_a_provider() {
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
            HashMap::from([("table.datalake.format".to_string(), "unused".to_string())]),
            HashMap::new(),
            None,
            0,
            0,
        );

        validate_readable_table(&table_info).unwrap();
    }
}
