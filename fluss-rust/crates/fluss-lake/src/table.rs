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
use crate::{FlussLakeError, FlussLakeReadContext, FlussLakeReadPlan, LakeSource, Result};
use fluss::client::FlussConnection;
use fluss::metadata::{RowType, SchemaInfo, TableInfo, TablePath};
use fluss::predicate::{BoundPredicate, Predicate};
use fluss::protocol::PbUnionReadContext;
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
    /// Creates a reusable table handle without fetching metadata or capturing boundaries.
    ///
    /// Each [`FlussLakeScan::plan`] call captures fresh source boundaries. Use
    /// [`Self::capture_read_context`] to explicitly share boundaries across scans.
    /// Table existence and configuration are checked when planning or capturing.
    pub fn new(connection: Arc<FlussConnection>, table_path: &TablePath) -> Self {
        Self {
            connection,
            table_path: table_path.clone(),
            catalog_property_overrides: HashMap::new(),
        }
    }

    /// Sets runtime lake properties, replacing any previous overrides.
    ///
    /// Properties such as storage credentials are copied into subsequently
    /// created scans. They are not part of the read context or split payloads.
    pub fn with_catalog_properties(mut self, properties: HashMap<String, String>) -> Self {
        self.catalog_property_overrides = properties;
        self
    }

    /// Creates a bounded read scan.
    pub fn new_scan(&self) -> FlussLakeScan {
        FlussLakeScan {
            connection: self.connection.clone(),
            table_path: self.table_path.clone(),
            projection: None,
            filter: None,
            lake_only: false,
            lake_source: None,
            catalog_property_overrides: self.catalog_property_overrides.clone(),
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
        self.resolve_read_context(boundary)
            .await
            .map(|(context, _)| context)
    }

    async fn resolve_read_context(
        &self,
        boundary: &PbUnionReadContext,
    ) -> Result<(FlussLakeReadContext, TableInfo)> {
        use crate::read_context::protocol::{required, validate_wire_identity};
        let admin = self
            .connection
            .get_admin()
            .map_err(|e| planning_client_error("create Fluss admin client", e))?;
        let mut live = admin
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
            live = current;
        }
        Ok((context, live))
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
            .field(
                "catalog_property_override_count",
                &self.catalog_property_overrides.len(),
            )
            .finish_non_exhaustive()
    }
}

/// Immutable configuration for one bounded UnionRead.
///
/// Planning freezes source inputs into local tasks. Readers must use the same
/// scan configuration that produced those tasks.
#[derive(Clone)]
pub struct FlussLakeScan {
    connection: Arc<FlussConnection>,
    table_path: TablePath,
    projection: Option<FlussLakeProjection>,
    filter: Option<Predicate>,
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

    /// Enables or disables lake-only execution.
    /// Requires a lake-enabled table; before its first snapshot the result is empty.
    pub fn with_lake_only(mut self, lake_only: bool) -> Self {
        self.lake_only = lake_only;
        self
    }

    /// Captures fresh source boundaries and builds the complete default read plan.
    ///
    /// Safe string partition equalities restrict offset capture before lake planning.
    /// Other predicates still participate in planning and exact result filtering.
    /// Use `FlussLakeTable::capture_read_context` for a table-wide boundary shared by scans.
    ///
    /// Lake planning uses the source supplied through `with_lake_source`.
    pub async fn plan(&self) -> Result<FlussLakeReadPlan> {
        plan_union_read(self).await
    }

    /// Builds a default plan without choosing new snapshots or log offsets.
    ///
    /// The context must refer to this table. Source identity and lake mapping
    /// are checked; later schema additions do not replace the captured schema.
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

    /// Use a lake backend instead of the feature-selected default implementation.
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
        #[cfg(feature = "paimon")]
        if format == "paimon" {
            return Ok(Arc::new(crate::paimon::PaimonLakeSource::new(
                info,
                &self.catalog_property_overrides,
            )?));
        }
        Err(FlussLakeError::PlanningFailed(format!(
            "no LakeSource for {format}; enable its feature or supply a custom source"
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
        let context = if self.lake_only {
            crate::boundary::freeze_lake_read_context(&admin, &table_info, scope.as_ref()).await?
        } else {
            crate::boundary::freeze_read_context(&admin, &table_info, scope.as_ref()).await?
        };
        Ok((table_info, context, bound))
    }

    /// Restores source-owned tasks under a shared boundary without lake replanning.
    ///
    /// Resolves the captured schema and current source identity once for this import.
    /// The caller owns transport and assignment, must preserve the planning scan
    /// settings and provide catalog credentials locally. Returned splits use the
    /// ordinary reader APIs; each read attempt still checks live source identity.
    pub async fn import_splits(
        &self,
        boundary: &PbUnionReadContext,
        descriptors: &[crate::FlussLakeSplitDescriptor],
    ) -> Result<Vec<crate::FlussLakeReadSplit>> {
        self.validate_configuration()?;
        let (context, live) = FlussLakeTable::new(self.connection.clone(), &self.table_path)
            .resolve_read_context(boundary)
            .await?;
        if context.is_lake_only() && !self.lake_only {
            return Err(FlussLakeError::InvalidReadContext(
                "lake-only boundary cannot execute a union read".into(),
            ));
        }
        let info = Arc::new(context.table_info(&live)?);
        let keys = physical_primary_key_indexes(&info)?;
        let scope = crate::partition::PartitionScope::bind(
            context.partition_scope(),
            context.table_schema().row_type(),
            context.partition_keys(),
        )?;
        let ranges: HashMap<_, _> = context
            .log_ranges()
            .iter()
            .map(|range| (range.table_bucket(), range))
            .collect();
        let mut ids = std::collections::HashSet::with_capacity(descriptors.len());
        descriptors
            .iter()
            .map(|descriptor| {
                if !ids.insert(descriptor.split_id()) {
                    return Err(FlussLakeError::InvalidReadContext(
                        "duplicate imported split identity".into(),
                    ));
                }
                descriptor.validate(
                    &context,
                    ranges.get(descriptor.table_bucket()).copied(),
                    &scope,
                )?;
                crate::FlussLakeReadSplit::try_new(info.clone(), descriptor.clone(), keys.clone())
                    .map_err(|error| FlussLakeError::InvalidReadContext(error.to_string()))
            })
            .collect()
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

    pub(crate) fn lake_only(&self) -> bool {
        self.lake_only
    }

    pub(crate) fn validate_configuration(&self) -> Result<()> {
        if self.table_path.database().is_empty() || self.table_path.table().is_empty() {
            return Err(FlussLakeError::PlanningFailed(
                "database and table names must not be empty".to_string(),
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
