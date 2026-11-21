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

//! Paimon lake snapshot planning and reading.
//!
//! The physical lake database and table use `table.datalake.database-name`
//! and `table.datalake.table-name`, falling back to the logical Fluss names.
//! Catalog settings come from `table.datalake.paimon.*` properties and runtime
//! overrides. Planning pins a readable snapshot into immutable lake tasks.
//! Append tasks are independently readable, including bucket-unaware layouts.
//! Primary-key tasks are read as a partition/bucket group so Paimon can apply
//! its merge and deletion rules before Fluss tail reconciliation.
//!
//! Parquet reads are covered by the backend integration tests. Other Paimon
//! file formats have not been validated for UnionRead.

use crate::error::error_message;
use crate::{FlussLakeError, FlussLakePartitionIdentity, RecordBatchStream, Result};
use arrow::record_batch::RecordBatch;
use fluss::metadata::{DataType, RowType, TableInfo, TablePath};
use fluss::predicate::{BoundPredicate, CompoundFunction, LeafFunction, Literal};
use fluss::row::Datum;
use futures::StreamExt;
use opendal_core::ErrorKind as StorageErrorKind;
use paimon::api::RestError;
use paimon::catalog::{Catalog, Identifier};
use paimon::spec::{
    BucketFunctionType, CoreOptions, Datum as PaimonDatum, Predicate as PaimonPredicate,
    PredicateBuilder,
};
use paimon::table::Table;
use paimon::{CatalogFactory, CatalogOptions, DataSplit, Options};
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{Mutex, OnceCell};

mod arrow_conversion;
#[cfg(test)]
mod reader_tests;

/// Pinned snapshots a shared source keeps open; older ones are reopened on demand.
const MAX_PINNED_TABLES: usize = 4;

const PAIMON_PAYLOAD_VERSION: u32 = 1;

/// Default Paimon LakeSource. Planning and reading share the same catalog mapping.
/// Credentials stay in this runtime object, never in the split payload.
#[derive(Clone)]
pub struct PaimonLakeSource {
    catalog_options: PaimonCatalogOptions,
    catalog_overrides: HashMap<String, String>,
    baseline_reader: Option<Arc<dyn PaimonBaselineReader>>,
    catalog: Arc<OnceCell<Arc<dyn Catalog>>>,
    pinned_tables: Arc<PinnedTables>,
}

#[derive(PartialEq, Eq)]
struct PinnedTableKey {
    table_id: i64,
    path: TablePath,
    snapshot_id: i64,
}

type PinnedTableCell = Arc<OnceCell<Arc<Table>>>;

/// The most recently pinned tables, each opened once however many tasks ask for it.
#[derive(Default)]
struct PinnedTables {
    entries: Mutex<VecDeque<(PinnedTableKey, PinnedTableCell)>>,
}

impl PinnedTables {
    async fn get_or_open<F, Fut>(&self, key: PinnedTableKey, open: F) -> Result<Arc<Table>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Table>>,
    {
        // Open outside the lock so a slow snapshot never delays the others.
        self.cell(key)
            .await
            .get_or_try_init(|| async { open().await.map(Arc::new) })
            .await
            .cloned()
    }

    /// Returns the key's cell as the most recently used entry.
    async fn cell(&self, key: PinnedTableKey) -> PinnedTableCell {
        let mut entries = self.entries.lock().await;
        let cached = entries
            .iter()
            .position(|(cached, _)| *cached == key)
            .and_then(|index| entries.remove(index));
        let cell = match cached {
            Some((_, cell)) => cell,
            None => {
                // A cell held by a caller has an open in flight; evicting it would open twice.
                while entries.len() >= MAX_PINNED_TABLES {
                    let Some(idle) = entries
                        .iter()
                        .position(|(_, cell)| Arc::strong_count(cell) == 1)
                    else {
                        break;
                    };
                    entries.remove(idle);
                }
                PinnedTableCell::default()
            }
        };
        entries.push_back((key, Arc::clone(&cell)));
        cell
    }
}

impl PaimonLakeSource {
    /// Resolve the runtime catalog from table properties and explicit overrides.
    pub fn new(table_info: &TableInfo, overrides: &HashMap<String, String>) -> Result<Self> {
        Ok(Self {
            catalog_options: PaimonCatalogOptions::from_table_info_with_overrides(
                table_info, overrides,
            )?,
            catalog_overrides: overrides.clone(),
            baseline_reader: None,
            catalog: Arc::new(OnceCell::new()),
            pinned_tables: Arc::default(),
        })
    }

    async fn catalog(&self) -> Result<&Arc<dyn Catalog>> {
        self.catalog
            .get_or_try_init(|| async {
                let mut options = Options::default();
                for (key, value) in self.catalog_options.as_map() {
                    options.set(key, value);
                }
                CatalogFactory::create(options)
                    .await
                    .map_err(|error| paimon_error("create Paimon catalog", error))
            })
            .await
    }

    /// Rejects a table whose properties resolve to another catalog than this source's.
    fn ensure_same_catalog(&self, info: &TableInfo) -> Result<()> {
        let resolved =
            PaimonCatalogOptions::from_table_info_with_overrides(info, &self.catalog_overrides)?;
        if resolved.identity() != self.catalog_options.identity() {
            return Err(FlussLakeError::InvalidReadContext(format!(
                "table {} uses a different Paimon catalog than this source; create a source per catalog",
                info.table_path
            )));
        }
        Ok(())
    }

    async fn pinned_table(
        &self,
        info: &TableInfo,
        snapshot_id: i64,
        reconcile_primary_key: bool,
    ) -> Result<Arc<Table>> {
        self.ensure_same_catalog(info)?;
        let path = crate::table::resolve_lake_table_path(info)?;
        let key = PinnedTableKey {
            table_id: info.table_id,
            path: path.clone(),
            snapshot_id,
        };
        let table = self
            .pinned_tables
            .get_or_open(key, || async {
                open_pinned_table(self.catalog().await?.as_ref(), &path, snapshot_id).await
            })
            .await?;
        // Callers can use the same snapshot with different capture metadata or
        // lake-only/UnionRead semantics. These checks are not cached with the table.
        if reconcile_primary_key {
            ensure_deduplicate_merge_engine(table.schema().options(), &path)?;
            ensure_last_write_wins(table.schema().options(), info, &path)?;
        }
        validate_paimon_layout(
            &path,
            table.schema().partition_keys(),
            table.schema().primary_keys(),
            table.schema().trimmed_primary_keys(),
            table.schema().options(),
            info,
        )?;
        Ok(table)
    }

    /// Overrides baseline execution while retaining Paimon planning and task validation.
    /// The reader receives decoded, snapshot-bound Paimon tasks, never private payloads.
    /// Unsupported task shapes may fall back to the default reader before any rows
    /// are emitted. Read failures must propagate instead of switching readers.
    pub fn with_baseline_reader(mut self, reader: Arc<dyn PaimonBaselineReader>) -> Self {
        self.baseline_reader = Some(reader);
        self
    }
}

/// A format-specific execution extension. Planning, snapshot pinning and payload
/// decoding remain owned by PaimonLakeSource; this is not a cross-language ABI.
///
/// A reader must preserve the supplied splits' version and deletion semantics,
/// return the requested columns in order, and release resources on stream drop.
/// Return `None` only when the task shape is unsupported, before producing data.
/// A Parquet reader alone must reject tasks requiring Paimon merge or DV handling.
pub trait PaimonBaselineReader: Send + Sync {
    /// Attempts native baseline execution, or declines the task for default execution.
    fn read<'a>(
        &'a self,
        context: PaimonReadContext<'a>,
    ) -> futures::future::BoxFuture<'a, Result<Option<RecordBatchStream>>>;
}

/// Validated Paimon tasks for one default UnionRead task. These references are
/// borrowed only during initialization; the returned stream must own its inputs.
#[non_exhaustive]
pub struct PaimonReadContext<'a> {
    /// Table pinned to the captured snapshot, with runtime catalog credentials.
    pub table: &'a Table,
    /// Decoded tasks, including data-file/deletion-file associations and row ranges.
    pub splits: &'a [DataSplit],
    /// Fields present in the pinned schema, including hidden PK/filter fields,
    /// in output order. The source fills newly added nullable columns afterward.
    pub projection: &'a [String],
    /// Arrow schema of the requested physical fields.
    pub schema: arrow::datatypes::SchemaRef,
    /// Required append or PK current-view semantics across the supplied group.
    pub semantics: crate::LakeReadSemantics,
}

impl crate::LakeSource for PaimonLakeSource {
    fn format(&self) -> &str {
        "paimon"
    }

    fn plan<'a>(
        &'a self,
        context: crate::LakePlannerContext<'a>,
    ) -> futures::future::BoxFuture<'a, Result<Vec<crate::LakeSplit>>> {
        Box::pin(async move { plan_snapshot_splits(self, context).await })
    }

    fn read<'a>(
        &'a self,
        context: crate::LakeReaderContext<'a>,
    ) -> futures::future::BoxFuture<'a, Result<RecordBatchStream>> {
        Box::pin(async move { read_snapshot_splits(self, context).await })
    }
}

/// Property prefix carrying the Paimon catalog configuration of a table.
const PAIMON_PROPERTY_PREFIX: &str = "table.datalake.paimon.";

/// Paimon table option that pins a scan to one snapshot id, never to a tag.
const PAIMON_SCAN_SNAPSHOT_ID_OPTION: &str = "scan.snapshot-id";

const FLUSS_DELETE_BEHAVIOR_OPTION: &str = "table.delete.behavior";

const FLUSS_DELETE_BEHAVIORS_WITHOUT_DELETES: [&str; 2] = ["IGNORE", "DISABLE"];

const DEFAULT_PAIMON_METASTORE: &str = "filesystem";

/// Catalog configuration needed to reopen one Paimon table.
///
/// It remains reader-local and is never serialized into a
/// [`crate::FlussLakeReadSplit`].
#[derive(Clone)]
struct PaimonCatalogOptions {
    options: HashMap<String, String>,
}

impl Debug for PaimonCatalogOptions {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PaimonCatalogOptions")
            .field("option_count", &self.options.len())
            .finish_non_exhaustive()
    }
}

impl PaimonCatalogOptions {
    /// Merges caller-provided catalog properties over server table metadata.
    ///
    /// Full `table.datalake.paimon.*` keys are preferred. Bare Paimon option
    /// names remain accepted for execution-context compatibility.
    fn from_table_info_with_overrides(
        table_info: &TableInfo,
        overrides: &HashMap<String, String>,
    ) -> Result<Self> {
        let mut options: HashMap<String, String> = table_info
            .properties
            .iter()
            .chain(table_info.custom_properties.iter())
            .filter_map(|(key, value)| {
                key.strip_prefix(PAIMON_PROPERTY_PREFIX)
                    .map(|suffix| (suffix.to_string(), value.clone()))
            })
            .collect();
        for (key, value) in overrides {
            if let Some(suffix) = key.strip_prefix(PAIMON_PROPERTY_PREFIX) {
                options.insert(suffix.to_string(), value.clone());
            } else if !key.starts_with("table.datalake.") {
                options.insert(key.clone(), value.clone());
            }
        }
        if !options.contains_key("warehouse") {
            return Err(FlussLakeError::PlanningFailed(format!(
                "table {} has a readable lake snapshot but no {PAIMON_PROPERTY_PREFIX}warehouse property",
                table_info.table_path
            )));
        }
        Ok(Self { options })
    }

    /// The metastore, URI and warehouse that name a catalog, ignoring how a path is spelled.
    fn identity(&self) -> (String, Option<&str>, Option<&str>) {
        let metastore = self
            .options
            .get(CatalogOptions::METASTORE)
            .map_or(DEFAULT_PAIMON_METASTORE, String::as_str)
            .to_ascii_lowercase();
        let location = |key| {
            self.options
                .get(key)
                .map(|value: &String| value.trim_end_matches('/'))
        };
        let warehouse = location(CatalogOptions::WAREHOUSE)
            .map(|path| path.strip_prefix("file://").unwrap_or(path));
        (metastore, location(CatalogOptions::URI), warehouse)
    }

    fn as_map(&self) -> &HashMap<String, String> {
        &self.options
    }
}

/// Opens a Paimon table at the captured snapshot and its historical schema.
///
/// Pinning uses the Paimon `scan.snapshot-id` option so that both planning and
/// execution observe exactly the snapshot frozen by the planner, regardless of
/// later lake commits.
async fn open_pinned_table(
    catalog: &dyn Catalog,
    table_path: &TablePath,
    snapshot_id: i64,
) -> Result<Table> {
    let identifier = Identifier::new(table_path.database(), table_path.table());
    let table = catalog
        .get_table(&identifier)
        .await
        .map_err(|error| paimon_error("open Paimon table", error))?;

    // A UnionRead log tail cannot enforce Paimon's row filters or column masks.
    table
        .schema()
        .core_options()
        .ensure_read_authorized()
        .map_err(|error| FlussLakeError::NotLakeReadable(format!("{table_path}: {error}")))?;

    let mut pinned = HashMap::with_capacity(1);
    pinned.insert(
        PAIMON_SCAN_SNAPSHOT_ID_OPTION.to_string(),
        snapshot_id.to_string(),
    );
    let table = table
        .copy_with_time_travel_strict(pinned)
        .await
        .map_err(|error| paimon_error("pin Paimon snapshot schema", error))?;
    if table.travel_snapshot().map(|snapshot| snapshot.id()) != Some(snapshot_id) {
        return Err(FlussLakeError::DataUnavailable(format!(
            "cannot resolve frozen Paimon snapshot {snapshot_id} for {table_path}"
        )));
    }
    Ok(table)
}

/// Converts a schema-bound core predicate into Paimon's predicate model.
///
/// Conversion failure deliberately disables this optimization instead of
/// failing the query. The Arrow-level evaluator still applies the original
/// bound predicate after reading and remains the source of exact semantics.
fn to_paimon_predicate(
    predicate: &BoundPredicate,
    fields: &[paimon::spec::DataField],
) -> Option<PaimonPredicate> {
    let builder = PredicateBuilder::new(fields);
    convert_paimon_predicate(predicate, &builder, fields).ok()
}

fn convert_paimon_predicate(
    predicate: &BoundPredicate,
    builder: &PredicateBuilder,
    fields: &[paimon::spec::DataField],
) -> paimon::Result<PaimonPredicate> {
    match predicate {
        BoundPredicate::AlwaysTrue => Ok(PaimonPredicate::AlwaysTrue),
        BoundPredicate::Compound { function, children } => {
            let converted = children
                .iter()
                .map(|child| convert_paimon_predicate(child, builder, fields));
            Ok(match function {
                // Dropping a conjunct only broadens the candidate set. Dropping
                // an OR branch could instead discard matching rows.
                CompoundFunction::And => {
                    PaimonPredicate::and(converted.filter_map(|result| result.ok()).collect())
                }
                CompoundFunction::Or => {
                    PaimonPredicate::or(converted.collect::<paimon::Result<Vec<_>>>()?)
                }
            })
        }
        BoundPredicate::Leaf {
            field_name,
            data_type,
            function,
            literals,
            ..
        } => {
            // Fluss orders signed bytes; Paimon orders unsigned bytes.
            if matches!(data_type, DataType::Binary(_) | DataType::Bytes(_))
                && matches!(
                    function,
                    LeafFunction::LessThan
                        | LeafFunction::LessOrEqual
                        | LeafFunction::GreaterThan
                        | LeafFunction::GreaterOrEqual
                )
            {
                return Err(paimon::Error::ConfigInvalid {
                    message: "binary ordering requires exact Fluss evaluation".into(),
                });
            }
            // Paimon's exact residual filter narrows timestamp constants to its
            // Arrow unit. A final Fluss filter cannot recover rows lost there.
            let field = fields.iter().find(|field| field.name() == field_name);
            if field.is_some_and(|field| !timestamp_literals_fit(literals, field.data_type())) {
                return Err(paimon::Error::ConfigInvalid {
                    message: "timestamp predicate requires exact Fluss evaluation".into(),
                });
            }
            let non_null_literals = || {
                literals
                    .iter()
                    .filter(|literal| !literal.is_null())
                    .map(|literal| to_paimon_datum(literal, data_type))
                    .collect::<Option<Vec<_>>>()
                    .ok_or_else(|| paimon::Error::ConfigInvalid {
                        message: format!(
                            "cannot translate the bound predicate on '{field_name}' to Paimon"
                        ),
                    })
            };
            match function {
                LeafFunction::IsNull => builder.is_null(field_name),
                LeafFunction::IsNotNull => builder.is_not_null(field_name),
                LeafFunction::In => {
                    let literals = non_null_literals()?;
                    if literals.is_empty() {
                        Ok(PaimonPredicate::AlwaysFalse)
                    } else {
                        builder.is_in(field_name, literals)
                    }
                }
                LeafFunction::NotIn if literals.iter().any(Literal::is_null) => {
                    Ok(PaimonPredicate::AlwaysFalse)
                }
                LeafFunction::NotIn => builder.is_not_in(field_name, non_null_literals()?),
                LeafFunction::StartsWith => {
                    builder.starts_with(field_name, required_paimon_literal(literals, data_type)?)
                }
                LeafFunction::EndsWith => {
                    builder.ends_with(field_name, required_paimon_literal(literals, data_type)?)
                }
                LeafFunction::Contains => {
                    builder.contains(field_name, required_paimon_literal(literals, data_type)?)
                }
                LeafFunction::Equal => {
                    builder.equal(field_name, required_paimon_literal(literals, data_type)?)
                }
                LeafFunction::NotEqual => {
                    builder.not_equal(field_name, required_paimon_literal(literals, data_type)?)
                }
                LeafFunction::LessThan => {
                    builder.less_than(field_name, required_paimon_literal(literals, data_type)?)
                }
                LeafFunction::LessOrEqual => {
                    builder.less_or_equal(field_name, required_paimon_literal(literals, data_type)?)
                }
                LeafFunction::GreaterThan => {
                    builder.greater_than(field_name, required_paimon_literal(literals, data_type)?)
                }
                LeafFunction::GreaterOrEqual => builder
                    .greater_or_equal(field_name, required_paimon_literal(literals, data_type)?),
            }
        }
    }
}

fn timestamp_literals_fit(literals: &[Literal], data_type: &paimon::spec::DataType) -> bool {
    let precision = match data_type {
        paimon::spec::DataType::Timestamp(ty) => ty.precision(),
        paimon::spec::DataType::LocalZonedTimestamp(ty) => ty.precision(),
        _ => return true,
    };
    let (factor, divisor) = match precision {
        0..=3 => (1_i64, 1_000_000),
        4..=6 => (1_000, 1_000),
        _ => (1_000_000, 1),
    };
    literals.iter().all(|literal| {
        let (millis, nanos) = match literal {
            Literal::TimestampNtz(v) => (v.get_millisecond(), v.get_nano_of_millisecond()),
            Literal::TimestampLtz(v) => (v.get_epoch_millisecond(), v.get_nano_of_millisecond()),
            _ => return true,
        };
        nanos % divisor == 0
            && millis
                .checked_mul(factor)
                .and_then(|value| value.checked_add(i64::from(nanos / divisor)))
                .is_some()
    })
}

fn required_paimon_literal(
    literals: &[Literal],
    data_type: &DataType,
) -> paimon::Result<PaimonDatum> {
    literals
        .first()
        .and_then(|literal| to_paimon_datum(literal, data_type))
        .ok_or_else(|| paimon::Error::ConfigInvalid {
            message: format!("cannot translate bound literal for Fluss type {data_type}"),
        })
}

fn to_paimon_datum(literal: &Literal, data_type: &DataType) -> Option<PaimonDatum> {
    if let Literal::Decimal(value) = literal {
        let DataType::Decimal(decimal_type) = data_type else {
            return None;
        };
        return Some(PaimonDatum::Decimal {
            unscaled: decimal_to_i128(value)?,
            precision: decimal_type.precision(),
            scale: decimal_type.scale(),
        });
    }
    to_paimon_scalar(&literal.to_datum())
}

fn decimal_to_i128(value: &fluss::row::Decimal) -> Option<i128> {
    value
        .to_big_decimal()
        .as_bigint_and_exponent()
        .0
        .try_into()
        .ok()
}

/// Plans the immutable Paimon splits of one readable lake snapshot.
///
/// Backend payloads preserve Paimon's native file locations and deletion metadata.
async fn plan_snapshot_splits(
    source: &PaimonLakeSource,
    context: crate::LakePlannerContext<'_>,
) -> Result<Vec<crate::LakeSplit>> {
    let info = context.table_info;
    let snapshot_id = context.snapshot_id;
    let table = source
        .pinned_table(info, snapshot_id, context.reconcile_primary_key)
        .await?;
    validate_additive_schema(info.row_type(), table.schema().fields())?;
    let partition_keys = table.schema().partition_keys();
    let mut read_builder = table.new_read_builder();
    let scope = BoundPredicate::bind(context.partition_scope, info.row_type())
        .map_err(|error| crate::error::planning_client_error("bind lake partition scope", error))?;
    let mut filters: Vec<_> = [context.filter, &scope]
        .into_iter()
        .filter_map(|filter| to_paimon_predicate(filter, table.schema().fields()))
        .collect();
    if let Some(range) = context.selected_bucket {
        let crate::FlussLakeLogPartition::Ordinary(identity) = range.partition();
        if let FlussLakePartitionIdentity::KeyValues(values) = identity {
            let builder = PredicateBuilder::new(table.schema().fields());
            for (key, value) in values {
                let index = info.row_type().get_field_index(key).ok_or_else(|| {
                    FlussLakeError::PlanningFailed(format!("unknown partition key {key}"))
                })?;
                let value =
                    fluss::parse_value_of_type(value, info.row_type().fields()[index].data_type())
                        .map_err(|error| {
                            crate::error::planning_client_error("decode partition value", error)
                        })?;
                if let Some(value) = to_paimon_scalar(&value) {
                    filters.push(
                        builder
                            .equal(key, value)
                            .map_err(|error| paimon_error("select lake partition", error))?,
                    );
                }
            }
        }
    }
    if !filters.is_empty() {
        read_builder.with_filter(PaimonPredicate::and(filters));
    }
    let plan = read_builder
        .new_scan()
        .plan()
        .await
        .map_err(|error| paimon_error("plan Paimon snapshot splits", error))?;

    let mut tasks = Vec::new();
    for split in plan.splits() {
        // paimon-rust has no public bucket-ID scan filter. Select before
        // encoding payloads; partition predicates already prune manifests.
        let partition = split_partition_identity(
            split,
            partition_keys,
            table.schema().fields(),
            info.row_type(),
        )?;
        if let Some(range) = context.selected_bucket {
            let crate::FlussLakeLogPartition::Ordinary(selected) = range.partition();
            if &partition != selected {
                continue;
            }
            if split.total_buckets() != range.bucket_count() {
                return Err(FlussLakeError::PlanningFailed(
                    "bucket-scoped read requires a Fluss-aligned lake layout".into(),
                ));
            }
            if split.bucket() != range.table_bucket().bucket_id() {
                continue;
            }
        }
        validate_split_bucket_count(
            split,
            if partition_keys.is_empty() && !info.primary_keys.is_empty() {
                info.num_buckets
            } else {
                split.total_buckets()
            },
        )?;
        tasks.push(crate::LakeSplit {
            format: "paimon".to_string(),
            snapshot_id: split.snapshot_id(),
            partition,
            bucket_id: lake_bucket_id(split),
            bucket_count: split.total_buckets(),
            estimated_rows: split
                .row_counts_known()
                .then(|| split.row_count())
                .and_then(|rows| usize::try_from(rows).ok()),
            estimated_size: split.data_files().iter().try_fold(0usize, |total, file| {
                total.checked_add(usize::try_from(file.file_size).ok()?)
            }),
            payload_version: PAIMON_PAYLOAD_VERSION,
            payload: serde_json::to_vec(split).map_err(|error| {
                FlussLakeError::PlanningFailed(format!("failed to encode Paimon split: {error}"))
            })?,
        });
    }
    Ok(tasks)
}

fn validate_split_bucket_count(split: &DataSplit, expected: i32) -> Result<()> {
    if expected == -1 && split.total_buckets() == -1 && matches!(split.bucket(), -1 | 0) {
        return Ok(());
    }
    if expected <= 0
        || split.total_buckets() != expected
        || split.bucket() < 0
        || split.bucket() >= expected
    {
        return Err(FlussLakeError::PlanningFailed(format!(
            "Paimon split has {} buckets or an invalid bucket id; expected partition bucket count {expected}",
            split.total_buckets()
        )));
    }
    Ok(())
}

fn lake_bucket_id(split: &DataSplit) -> i32 {
    // Physical bucket 0 in a bucket-unaware Paimon table is not ownership
    // of Fluss log bucket 0. The public lake identity remains -1.
    if split.total_buckets() == -1 {
        -1
    } else {
        split.bucket()
    }
}

fn validate_paimon_layout(
    table_path: &TablePath,
    actual_partition_keys: &[String],
    actual_primary_keys: &[String],
    default_bucket_keys: Vec<String>,
    table_options: &HashMap<String, String>,
    expected: &TableInfo,
) -> Result<()> {
    if actual_partition_keys != expected.partition_keys.as_ref() {
        return Err(FlussLakeError::PlanningFailed(format!(
            "Paimon partition keys {:?} do not match Fluss partition keys {:?} for {table_path}",
            actual_partition_keys, expected.partition_keys
        )));
    }
    if actual_primary_keys != expected.primary_keys {
        return Err(FlussLakeError::PlanningFailed(format!(
            "Paimon primary keys {:?} do not match Fluss primary keys {:?} for {table_path}",
            actual_primary_keys, expected.primary_keys
        )));
    }
    // Append tasks are not reconciled with a Fluss bucket. Their own lake
    // layout is validated by Paimon and the task envelope, not by Fluss hashing.
    if expected.primary_keys.is_empty() {
        return Ok(());
    }
    let core_options = CoreOptions::new(table_options);
    let bucket_keys = core_options.bucket_key().unwrap_or(default_bucket_keys);
    if bucket_keys != expected.bucket_keys {
        return Err(FlussLakeError::PlanningFailed(format!(
            "Paimon bucket keys {:?} do not match Fluss bucket keys {:?} for {table_path}",
            bucket_keys, expected.bucket_keys
        )));
    }
    if core_options.bucket() <= 0
        || (expected.partition_keys.is_empty() && core_options.bucket() != expected.num_buckets)
    {
        return Err(FlussLakeError::PlanningFailed(format!(
            "Paimon bucket count {} does not match Fluss bucket count {} for {table_path}",
            core_options.bucket(),
            expected.num_buckets
        )));
    }
    let bucket_function = core_options.bucket_function_type().map_err(|error| {
        FlussLakeError::PlanningFailed(format!(
            "failed to resolve the Paimon bucket function of {table_path}: {error}"
        ))
    })?;
    if bucket_function != BucketFunctionType::Default {
        return Err(FlussLakeError::PlanningFailed(format!(
            "Paimon bucket function {bucket_function:?} is incompatible with Fluss bucket assignment for {table_path}"
        )));
    }
    Ok(())
}

/// Requires deduplicate semantics for the default primary-key tail reconciliation.
///
/// The Fluss overlay keeps each key's latest changelog row or tombstone.
/// Applying that overlay to a lake baseline is not correct for merge engines
/// such as aggregation, partial-update or first-row. Lake-only reads use
/// Paimon's native current-view semantics and do not require this restriction.
fn ensure_deduplicate_merge_engine(
    table_options: &HashMap<String, String>,
    table_path: &TablePath,
) -> Result<()> {
    let merge_engine = paimon::spec::CoreOptions::new(table_options)
        .merge_engine()
        .map_err(|error| {
            FlussLakeError::UnsupportedMergeEngine(format!(
                "failed to resolve the Paimon merge engine of {table_path}: {error}"
            ))
        })?;
    if merge_engine != paimon::spec::MergeEngine::Deduplicate {
        return Err(FlussLakeError::UnsupportedMergeEngine(format!(
            "primary-key UnionRead only supports the deduplicate merge engine, but the Paimon table for {table_path} uses {merge_engine:?}; refusing to plan a read that would silently produce an incorrect current view"
        )));
    }
    Ok(())
}

/// Requires Paimon to keep each key's last write, as the Fluss log tail does.
fn ensure_last_write_wins(
    table_options: &HashMap<String, String>,
    info: &TableInfo,
    table_path: &TablePath,
) -> Result<()> {
    let core_options = CoreOptions::new(table_options);
    let sequence_fields = core_options.sequence_fields();
    if !sequence_fields.is_empty() {
        return Err(FlussLakeError::NotLakeReadable(format!(
            "the Paimon table for {table_path} orders versions of a key by sequence.field '{}'; matching Fluss needs the VERSIONED merge engine on that column, which UnionRead does not support yet",
            sequence_fields.join(",")
        )));
    }
    // Dropped deletes match Fluss only when Fluss drops or rejects them too.
    let fluss_applies_deletes = !info
        .properties
        .get(FLUSS_DELETE_BEHAVIOR_OPTION)
        .is_some_and(|behavior| {
            FLUSS_DELETE_BEHAVIORS_WITHOUT_DELETES
                .iter()
                .any(|without_deletes| behavior.eq_ignore_ascii_case(without_deletes))
        });
    if core_options.ignore_delete() && fluss_applies_deletes {
        return Err(FlussLakeError::NotLakeReadable(format!(
            "the Paimon table for {table_path} drops deletes ('ignore-delete=true') but the Fluss table applies them; set '{FLUSS_DELETE_BEHAVIOR_OPTION}' to IGNORE or DISABLE, or remove 'paimon.ignore-delete'"
        )));
    }
    Ok(())
}

fn split_partition_identity(
    split: &DataSplit,
    partition_keys: &[String],
    fields: &[paimon::spec::DataField],
    row_type: &RowType,
) -> Result<FlussLakePartitionIdentity> {
    if partition_keys.is_empty() {
        return Ok(FlussLakePartitionIdentity::Unpartitioned);
    }
    if split.partition().arity() as usize != partition_keys.len() {
        return Err(FlussLakeError::PlanningFailed(
            "Paimon partition arity differs from Fluss".into(),
        ));
    }
    let values = partition_keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            let field = fields
                .iter()
                .find(|field| field.name() == key)
                .ok_or_else(|| {
                    FlussLakeError::PlanningFailed(format!("missing Paimon partition key {key}"))
                })?;
            let fluss_index = row_type.get_field_index(key).ok_or_else(|| {
                FlussLakeError::PlanningFailed(format!("missing Fluss partition key {key}"))
            })?;
            let datum = split
                .partition()
                .get_datum(index, field.data_type())
                .map_err(|error| paimon_error("decode Paimon partition", error))?
                .ok_or_else(|| {
                    FlussLakeError::PlanningFailed("Fluss partition values cannot be null".into())
                })?;
            let literal = partition_literal(datum)?;
            let value = fluss::convert_value_of_type(
                &literal.to_datum(),
                row_type.fields()[fluss_index].data_type(),
            )
            .map_err(|error| {
                crate::error::planning_client_error("encode Fluss partition", error)
            })?;
            Ok((key.clone(), value))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(FlussLakePartitionIdentity::KeyValues(values))
}

fn partition_literal(datum: PaimonDatum) -> Result<Literal> {
    Ok(match datum {
        PaimonDatum::Bool(v) => Literal::Bool(v),
        PaimonDatum::TinyInt(v) => Literal::Int8(v),
        PaimonDatum::SmallInt(v) => Literal::Int16(v),
        PaimonDatum::Int(v) => Literal::Int32(v),
        PaimonDatum::Long(v) => Literal::Int64(v),
        PaimonDatum::Float(v) => Literal::Float32(v),
        PaimonDatum::Double(v) => Literal::Float64(v),
        PaimonDatum::String(v) => Literal::String(v),
        PaimonDatum::Bytes(v) => Literal::Bytes(v),
        PaimonDatum::Date(v) => Literal::Date(v),
        PaimonDatum::Time(v) => Literal::Time(v),
        PaimonDatum::Timestamp { millis, nanos } => Literal::TimestampNtz(
            fluss::row::TimestampNtz::from_millis_nanos(millis, nanos).map_err(|error| {
                crate::error::planning_client_error("decode partition timestamp", error)
            })?,
        ),
        PaimonDatum::LocalZonedTimestamp { millis, nanos } => Literal::TimestampLtz(
            fluss::row::TimestampLtz::from_millis_nanos(millis, nanos).map_err(|error| {
                crate::error::planning_client_error("decode partition timestamp", error)
            })?,
        ),
        other => {
            return Err(FlussLakeError::PlanningFailed(format!(
                "unsupported Fluss partition value {other:?}"
            )));
        }
    })
}

fn to_paimon_scalar(datum: &Datum<'_>) -> Option<PaimonDatum> {
    Some(match datum {
        Datum::Bool(v) => PaimonDatum::Bool(*v),
        Datum::Int8(v) => PaimonDatum::TinyInt(*v),
        Datum::Int16(v) => PaimonDatum::SmallInt(*v),
        Datum::Int32(v) => PaimonDatum::Int(*v),
        Datum::Int64(v) => PaimonDatum::Long(*v),
        Datum::String(v) => PaimonDatum::String(v.to_string()),
        Datum::Blob(v) => PaimonDatum::Bytes(v.to_vec()),
        Datum::Date(v) => PaimonDatum::Date(v.get_inner()),
        Datum::Time(v) => PaimonDatum::Time(v.get_inner()),
        Datum::TimestampNtz(v) => PaimonDatum::Timestamp {
            millis: v.get_millisecond(),
            nanos: v.get_nano_of_millisecond(),
        },
        Datum::TimestampLtz(v) => PaimonDatum::LocalZonedTimestamp {
            millis: v.get_epoch_millisecond(),
            nanos: v.get_nano_of_millisecond(),
        },
        // Floating equality has different NaN/zero semantics in Paimon.
        _ => return None,
    })
}

/// The authorization marker of a serialized split, which `DataSplit` keeps private.
#[derive(serde::Deserialize)]
struct SplitAuthorization {
    query_auth_required: bool,
}

/// Reads several frozen Paimon splits as one finite Arrow batch stream.
///
/// All splits must come from the same pinned snapshot and partition/bucket.
/// For primary-key reads, Paimon planning keeps key-overlapping files together
/// so each split can apply lake version and deletion rules independently.
/// The default UnionRead executor still supplies the whole bucket group to
/// reconcile its baseline with the Fluss tail in one task.
async fn read_snapshot_splits(
    source: &PaimonLakeSource,
    context: crate::LakeReaderContext<'_>,
) -> Result<RecordBatchStream> {
    let info = context.table_info;
    let snapshot_id = context.snapshot_id;
    let Some(first) = context.splits.first() else {
        return Ok(Box::pin(futures::stream::empty()));
    };
    let expected_bucket_id = first.bucket_id;
    let table = source
        .pinned_table(info, snapshot_id, context.reconcile_primary_key)
        .await?;
    let splits = context
        .splits
        .iter()
        .map(|task| {
            task.validate("paimon", snapshot_id)?;
            if task.payload_version != PAIMON_PAYLOAD_VERSION {
                return Err(FlussLakeError::IncompatibleSplitVersion(format!(
                    "Paimon requires V{PAIMON_PAYLOAD_VERSION} lake task payloads"
                )));
            }
            if task.bucket_id != expected_bucket_id
                || (info.has_primary_key() && task.bucket_id < 0)
                || task.partition != first.partition
                || task.bucket_count != first.bucket_count
                || (info.has_primary_key()
                    && info.partition_keys.is_empty()
                    && task.bucket_count != info.num_buckets)
            {
                return Err(FlussLakeError::PlanningFailed(
                    "Paimon read tasks must belong to one partition/bucket".to_string(),
                ));
            }
            if source.baseline_reader.is_some() {
                // TableRead checks this transported marker itself. A replacement
                // reader does not pass through TableRead, and rules can be empty
                // even when a grant is required.
                let auth: SplitAuthorization =
                    serde_json::from_slice(&task.payload).map_err(|error| {
                        FlussLakeError::PlanningFailed(format!(
                            "invalid Paimon split authorization marker: {error}"
                        ))
                    })?;
                if auth.query_auth_required {
                    return Err(FlussLakeError::NotLakeReadable(
                        "UnionRead does not support Paimon splits requiring query authorization"
                            .into(),
                    ));
                }
            }
            let split: DataSplit = serde_json::from_slice(&task.payload).map_err(|error| {
                FlussLakeError::PlanningFailed(format!("failed to decode Paimon split: {error}"))
            })?;
            validate_split_bucket_count(&split, task.bucket_count)?;
            if split_partition_identity(
                &split,
                table.schema().partition_keys(),
                table.schema().fields(),
                info.row_type(),
            )? != task.partition
            {
                return Err(FlussLakeError::PlanningFailed(
                    "Paimon payload partition does not match its envelope".to_string(),
                ));
            }
            Ok(split)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut expected_bucket_path: Option<&str> = None;
    for split in &splits {
        if split.snapshot_id() != snapshot_id {
            return Err(FlussLakeError::Internal(format!(
                "Paimon split snapshot id {} does not match frozen snapshot id {snapshot_id}",
                split.snapshot_id()
            )));
        }
        if lake_bucket_id(split) != expected_bucket_id {
            return Err(FlussLakeError::Internal(format!(
                "Paimon split bucket id {} does not match logical split bucket id {expected_bucket_id}",
                split.bucket()
            )));
        }
        match expected_bucket_path {
            Some(path) if path != split.bucket_path() => {
                return Err(FlussLakeError::Internal(format!(
                    "one logical bucket split contains multiple Paimon bucket paths: '{path}' and '{}'",
                    split.bucket_path()
                )));
            }
            None => expected_bucket_path = Some(split.bucket_path()),
            _ => {}
        }
    }
    let projection =
        PaimonProjection::resolve(info.row_type(), table.schema().fields(), context.schema)?;
    let field_names = &projection.names;
    if let Some(reader) = source.baseline_reader.as_deref()
        && let Some(stream) = reader
            .read(PaimonReadContext {
                table: &table,
                splits: &splits,
                projection: field_names,
                schema: projection.physical_schema.clone(),
                semantics: context.semantics,
            })
            .await?
    {
        return Ok(Box::pin(stream.map(move |batch| {
            batch.and_then(|batch| projection.normalize(batch))
        })));
    }
    let mut read_builder = table.new_read_builder();
    let borrowed: Vec<&str> = field_names.iter().map(String::as_str).collect();
    read_builder
        .with_projection(&borrowed)
        .map_err(|e| paimon_error("apply Paimon read projection", e))?;
    if let Some(filter) = to_paimon_predicate(context.filter, table.schema().fields()) {
        read_builder.with_filter(filter);
    }
    let stream = read_builder
        .new_read()
        .map_err(|error| paimon_error("create Paimon reader", error))?
        .to_arrow(&splits)
        .map_err(|error| paimon_error("read Paimon splits", error))?;

    Ok(Box::pin(stream.map(move |result| {
        result
            .map_err(|error| paimon_error("read Paimon split batch", error))
            .and_then(|batch| projection.normalize(batch))
    })))
}

// Fluss lake schema evolution currently appends nullable business columns.
// Lake field IDs belong to Paimon (legacy system fields also consume IDs), so
// never reinterpret a Fluss field ID as a Paimon field ID.
fn validate_additive_schema(row_type: &RowType, fields: &[paimon::spec::DataField]) -> Result<()> {
    let business_fields: Vec<_> = fields
        .iter()
        .filter(|field| !matches!(field.name(), "__bucket" | "__offset" | "__timestamp"))
        .collect();
    if business_fields.len() > row_type.fields().len()
        || business_fields
            .iter()
            .zip(row_type.fields())
            .any(|(lake, fluss)| lake.name() != fluss.name())
        || row_type.fields()[business_fields.len()..]
            .iter()
            .any(|field| !field.data_type().is_nullable())
    {
        return Err(FlussLakeError::SchemaIncompatible(
            "Paimon snapshot schema must match the captured schema or lack only newly added nullable columns".into(),
        ));
    }
    Ok(())
}

struct PaimonProjection {
    names: Vec<String>,
    physical_schema: arrow::datatypes::SchemaRef,
    output_schema: arrow::datatypes::SchemaRef,
    alignment: Option<Vec<i32>>,
}

impl PaimonProjection {
    fn resolve(
        row_type: &RowType,
        fields: &[paimon::spec::DataField],
        output_schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self> {
        validate_additive_schema(row_type, fields)?;
        let mut names = Vec::new();
        let mut physical_fields = Vec::new();
        let alignment: Vec<i32> = output_schema
            .fields()
            .iter()
            .map(|field| {
                if fields.iter().any(|lake| lake.name() == field.name()) {
                    let index = names.len() as i32;
                    names.push(field.name().clone());
                    physical_fields.push(field.clone());
                    index
                } else {
                    -1
                }
            })
            .collect();
        let alignment = (alignment.is_empty() || alignment.contains(&-1)).then_some(alignment);
        // A projection consisting only of added columns still needs the row
        // count from the old files. Read one existing column and discard it.
        if names.is_empty() {
            let schema = fluss::record::to_arrow_schema(row_type).map_err(|error| {
                crate::error::planning_client_error("resolve Paimon projection", error)
            })?;
            names.push(schema.field(0).name().clone());
            physical_fields.push(schema.fields()[0].clone());
        }
        Ok(Self {
            names,
            physical_schema: Arc::new(arrow::datatypes::Schema::new(physical_fields)),
            output_schema,
            alignment,
        })
    }

    fn normalize(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let batch = arrow_conversion::normalize_batch(batch, self.physical_schema.clone())?;
        match &self.alignment {
            Some(alignment) => fluss::record::align_record_batch_to_schema(
                batch,
                self.output_schema.clone(),
                alignment,
            )
            .map_err(|error| FlussLakeError::SchemaIncompatible(error.to_string())),
            None => Ok(batch),
        }
    }
}

type FailureKind = fn(String) -> FlussLakeError;

/// Classifies a Paimon failure by its first typed cause; message text is not stable.
fn paimon_error(action: &str, error: paimon::Error) -> FlussLakeError {
    let kind = std::iter::successors(Some(&error as &(dyn Error + 'static)), |&cause| {
        next_cause(cause)
    })
    .find_map(paimon_failure_kind)
    .unwrap_or(FlussLakeError::Internal);
    kind(error_message(action, &error))
}

/// Follows `Error::source`, and also the causes that `UnexpectedError` and
/// `io::Error` keep out of it.
fn next_cause<'a>(cause: &'a (dyn Error + 'static)) -> Option<&'a (dyn Error + 'static)> {
    if let Some(paimon::Error::UnexpectedError {
        source: Some(source),
        ..
    }) = cause.downcast_ref::<paimon::Error>()
    {
        return Some(source.as_ref());
    }
    if let Some(inner) = cause
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::get_ref)
    {
        return Some(inner);
    }
    cause.source()
}

fn paimon_failure_kind(cause: &(dyn Error + 'static)) -> Option<FailureKind> {
    if cause
        .downcast_ref::<reqwest::Error>()
        .is_some_and(|error| error.is_connect() || error.is_timeout() || error.is_request())
    {
        return Some(FlussLakeError::ConnectionError);
    }
    match cause.downcast_ref::<paimon::Error>()? {
        paimon::Error::ColumnNotExist { .. } => Some(FlussLakeError::SchemaIncompatible),
        paimon::Error::DatabaseNotExist { .. }
        | paimon::Error::TableNotExist { .. }
        | paimon::Error::SnapshotNotExist { .. }
        | paimon::Error::RestApi {
            source: RestError::NoSuchResource { .. },
        } => Some(FlussLakeError::DataUnavailable),
        paimon::Error::IoUnexpected { source, .. }
            if source.kind() == StorageErrorKind::NotFound =>
        {
            Some(FlussLakeError::DataUnavailable)
        }
        paimon::Error::IoUnexpected { .. } | paimon::Error::RestApi { .. } => {
            Some(FlussLakeError::ConnectionError)
        }
        paimon::Error::Unsupported { .. } => Some(FlussLakeError::NotLakeReadable),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataField, DataTypes, Schema};
    use fluss::predicate::{Literal, Predicate, col};
    use opendal_core::Error as StorageError;
    use paimon::spec::{
        BinaryRow, DataField as PaimonField, DataType as PaimonType, DecimalType, DoubleType,
        IntType, TimestampType, VarCharType,
    };
    use std::mem::discriminant;

    // Model a backend wrapper that hides its cause in both Display and Debug.
    #[derive(thiserror::Error)]
    #[error("storage read failed")]
    struct ReadFailure(#[source] paimon::Error);

    impl Debug for ReadFailure {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.write_str("ReadFailure")
        }
    }

    fn storage_error(kind: StorageErrorKind, detail: &str) -> paimon::Error {
        StorageError::new(kind, detail).into()
    }

    fn wrapped(cause: paimon::Error) -> paimon::Error {
        paimon::Error::DataInvalid {
            message: "wrapped read failure".into(),
            source: Some(Box::new(ReadFailure(cause))),
        }
    }

    #[test]
    fn paimon_failures_are_classified_by_typed_cause() {
        let cases: Vec<(paimon::Error, FailureKind)> = vec![
            (
                paimon::Error::SnapshotNotExist { snapshot_id: 7 },
                FlussLakeError::DataUnavailable,
            ),
            (
                paimon::Error::TableNotExist {
                    full_name: "fluss.orders".into(),
                },
                FlussLakeError::DataUnavailable,
            ),
            (
                storage_error(StorageErrorKind::NotFound, "frozen.parquet"),
                FlussLakeError::DataUnavailable,
            ),
            (
                wrapped(storage_error(StorageErrorKind::NotFound, "frozen.parquet")),
                FlussLakeError::DataUnavailable,
            ),
            (
                storage_error(StorageErrorKind::Unexpected, "connection reset"),
                FlussLakeError::ConnectionError,
            ),
            (
                wrapped(storage_error(
                    StorageErrorKind::PermissionDenied,
                    "frozen.parquet",
                )),
                FlussLakeError::ConnectionError,
            ),
            (
                paimon::Error::RestApi {
                    source: RestError::NoSuchResource {
                        resource_type: Some("TABLE".into()),
                        resource_name: Some("orders".into()),
                        message: "dropped".into(),
                    },
                },
                FlussLakeError::DataUnavailable,
            ),
            (
                paimon::Error::RestApi {
                    source: RestError::ServiceUnavailable {
                        message: "retry later".into(),
                    },
                },
                FlussLakeError::ConnectionError,
            ),
            (
                paimon::Error::ColumnNotExist {
                    full_name: "fluss.orders".into(),
                    column: "price".into(),
                },
                FlussLakeError::SchemaIncompatible,
            ),
            (
                paimon::Error::Unsupported {
                    message: "query-auth.enabled".into(),
                },
                FlussLakeError::NotLakeReadable,
            ),
            (
                paimon::Error::UnexpectedError {
                    message: "orc read failed".into(),
                    source: Some(Box::new(storage_error(
                        StorageErrorKind::NotFound,
                        "frozen.orc",
                    ))),
                },
                FlussLakeError::DataUnavailable,
            ),
            (
                wrapped(paimon::Error::UnexpectedError {
                    message: "orc read failed".into(),
                    source: Some(Box::new(std::io::Error::other(storage_error(
                        StorageErrorKind::Unexpected,
                        "connection reset",
                    )))),
                }),
                FlussLakeError::ConnectionError,
            ),
            (
                paimon::Error::DataInvalid {
                    message: "orc read failed".into(),
                    source: Some(Box::new(std::io::Error::other(storage_error(
                        StorageErrorKind::NotFound,
                        "frozen.orc",
                    )))),
                },
                FlussLakeError::DataUnavailable,
            ),
            // Wording that looks like a missing file is still an internal failure.
            (
                paimon::Error::UnexpectedError {
                    message: "Partition key 'p' not found in schema fields".into(),
                    source: None,
                },
                FlussLakeError::Internal,
            ),
            (
                wrapped(paimon::Error::DataTypeInvalid {
                    message: "connection timed out".into(),
                }),
                FlussLakeError::Internal,
            ),
        ];
        for (error, expected) in cases {
            let description = error.to_string();
            let actual = paimon_error("read frozen Paimon task", error);
            assert_eq!(
                discriminant(&actual),
                discriminant(&expected(String::new())),
                "{description} was classified as {actual:?}"
            );
        }
    }

    #[test]
    fn diagnostics_keep_causes_hidden_by_wrappers() {
        let source = wrapped(storage_error(StorageErrorKind::NotFound, "frozen.parquet"));
        assert!(!source.to_string().contains("frozen.parquet"));
        let error = paimon_error("read frozen Paimon task", source);
        assert!(error.to_string().contains("frozen.parquet"));
    }

    #[test]
    fn decimal_predicates_preserve_signed_unscaled_values() {
        for unscaled in [
            0,
            128,
            -129,
            i128::from(i64::MAX) + 1,
            -(10_i128.pow(38) - 1),
        ] {
            let literal = Literal::Decimal(
                fluss::row::Decimal::from_unscaled_bytes(&unscaled.to_be_bytes(), 38, 3).unwrap(),
            );
            assert_eq!(
                to_paimon_datum(&literal, &DataTypes::decimal(38, 3)),
                Some(PaimonDatum::Decimal {
                    unscaled,
                    precision: 38,
                    scale: 3
                })
            );
        }
    }

    fn table_info(
        properties: HashMap<String, String>,
        custom_properties: HashMap<String, String>,
    ) -> TableInfo {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()
            .unwrap();
        TableInfo::new(
            TablePath::new("fluss", "orders"),
            7,
            1,
            schema,
            vec!["id".to_string()],
            Vec::<String>::new().into(),
            1,
            properties,
            custom_properties,
            None,
            0,
            0,
        )
    }

    #[test]
    fn translates_core_string_membership_and_null_semantics_to_paimon() {
        let fields = vec![
            PaimonField::new(0, "id".to_string(), PaimonType::Int(IntType::new())),
            PaimonField::new(
                1,
                "name".to_string(),
                PaimonType::VarChar(VarCharType::string_type()),
            ),
        ];
        let row_type = RowType::new(vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("name", DataTypes::string(), None),
        ]);
        let predicate = col("id")
            .is_in([1_i32, 2_i32])
            .and(col("name").contains("a"));
        let bound = BoundPredicate::bind(Some(&predicate), &row_type).unwrap();
        assert!(matches!(
            to_paimon_predicate(&bound, &fields),
            Some(PaimonPredicate::And(children)) if children.len() == 2
        ));

        let not_in_with_null = Predicate::Leaf {
            field: "id".to_string(),
            function: LeafFunction::NotIn,
            literals: vec![Literal::Int32(1), Literal::Null],
        };
        let bound = BoundPredicate::bind(Some(&not_in_with_null), &row_type).unwrap();
        assert_eq!(
            to_paimon_predicate(&bound, &fields),
            Some(PaimonPredicate::AlwaysFalse)
        );
    }

    #[test]
    fn numeric_pushdown_uses_exact_bounds_and_keeps_floats_residual() {
        let row_type = RowType::new(vec![
            DataField::new("i", DataTypes::int(), None),
            DataField::new("d", DataTypes::decimal(5, 2), None),
            DataField::new("f", DataTypes::double(), None),
        ]);
        let fields = vec![
            PaimonField::new(0, "i".into(), PaimonType::Int(IntType::new())),
            PaimonField::new(
                1,
                "d".into(),
                PaimonType::Decimal(DecimalType::new(5, 2).unwrap()),
            ),
            PaimonField::new(2, "f".into(), PaimonType::Double(DoubleType::new())),
        ];
        let builder = PredicateBuilder::new(&fields);
        let convert = |predicate| {
            to_paimon_predicate(
                &BoundPredicate::bind(Some(&predicate), &row_type).unwrap(),
                &fields,
            )
        };
        let decimal = fluss::row::Decimal::from_unscaled_long(1234, 5, 3).unwrap();
        assert_eq!(
            convert(col("i").ge(1.3)),
            Some(builder.greater_than("i", PaimonDatum::Int(1)).unwrap())
        );
        assert_eq!(
            convert(col("i").eq(1.3)),
            Some(PaimonPredicate::AlwaysFalse)
        );
        assert_eq!(
            convert(col("d").ge(decimal)),
            Some(
                builder
                    .greater_than(
                        "d",
                        PaimonDatum::Decimal {
                            unscaled: 123,
                            precision: 5,
                            scale: 2
                        }
                    )
                    .unwrap()
            )
        );
        for value in [1.3, -0.0, f64::INFINITY, f64::NAN] {
            assert!(convert(col("f").ge(value)).is_none());
        }
        assert!(convert(col("i").eq(1).or(col("f").ge(1.3))).is_none());
        assert_eq!(
            convert(col("i").eq(1).and(col("f").ge(1.3))),
            convert(col("i").eq(1))
        );
        assert_eq!(
            convert(col("i").eq(1).and(col("f").ge(1.3)).or(col("i").eq(2)),),
            convert(col("i").eq(1).or(col("i").eq(2)))
        );
        assert!(convert(col("f").is_not_null()).is_some());
    }

    #[test]
    fn caller_catalog_properties_override_server_metadata() {
        let properties = HashMap::from([
            (
                "table.datalake.paimon.warehouse".into(),
                "s3://server/warehouse".into(),
            ),
            (
                "table.datalake.paimon.s3.endpoint".into(),
                "http://server:9000".into(),
            ),
        ]);
        let overrides = HashMap::from([
            (
                "table.datalake.paimon.warehouse".into(),
                "s3://caller/warehouse".into(),
            ),
            (
                "table.datalake.paimon.s3.secret-key".into(),
                "CALLER-SECRET".into(),
            ),
        ]);

        let options = PaimonCatalogOptions::from_table_info_with_overrides(
            &table_info(properties, HashMap::new()),
            &overrides,
        )
        .unwrap();

        assert_eq!(
            options.as_map().get("warehouse"),
            Some(&"s3://caller/warehouse".to_string())
        );
        assert_eq!(
            options.as_map().get("s3.endpoint"),
            Some(&"http://server:9000".to_string())
        );
        assert_eq!(
            options.as_map().get("s3.secret-key"),
            Some(&"CALLER-SECRET".to_string())
        );
        let debug = format!("{options:?}");
        assert!(!debug.contains("CALLER-SECRET"));
        assert!(!debug.contains("s3.secret-key"));
    }

    #[test]
    fn paimon_layout_must_match_fluss_partition_primary_and_bucket_contract() {
        let table_path = TablePath::new("fluss", "pk_orders");
        let partition_keys = vec!["region".to_string()];
        let primary_keys = vec!["id".to_string(), "region".to_string()];
        let bucket_keys = vec!["id".to_string()];
        let mut expected = table_info(HashMap::new(), HashMap::new());
        expected.partition_keys = partition_keys.clone().into();
        expected.primary_keys = primary_keys.clone();
        expected.bucket_keys = bucket_keys.clone();
        expected.num_buckets = 4;
        let matching_options = HashMap::from([
            ("bucket".to_string(), "4".to_string()),
            ("bucket-key".to_string(), "id".to_string()),
        ]);

        validate_paimon_layout(
            &table_path,
            &partition_keys,
            &primary_keys,
            bucket_keys.clone(),
            &matching_options,
            &expected,
        )
        .unwrap();

        for (actual_primary_keys, options) in [
            (
                vec!["other".to_string(), "region".to_string()],
                matching_options.clone(),
            ),
            (
                primary_keys.clone(),
                HashMap::from([
                    ("bucket".to_string(), "0".to_string()),
                    ("bucket-key".to_string(), "id".to_string()),
                ]),
            ),
            (
                primary_keys.clone(),
                HashMap::from([
                    ("bucket".to_string(), "4".to_string()),
                    ("bucket-key".to_string(), "other".to_string()),
                ]),
            ),
            (
                primary_keys.clone(),
                HashMap::from([
                    ("bucket".to_string(), "4".to_string()),
                    ("bucket-key".to_string(), "id".to_string()),
                    ("bucket-function.type".to_string(), "mod".to_string()),
                ]),
            ),
        ] {
            assert!(matches!(
                validate_paimon_layout(
                    &table_path,
                    &partition_keys,
                    &actual_primary_keys,
                    bucket_keys.clone(),
                    &options,
                    &expected,
                ),
                Err(FlussLakeError::PlanningFailed(_))
            ));
        }
    }

    #[test]
    fn partitioned_paimon_default_does_not_override_existing_partition_layouts() {
        let path = TablePath::new("fluss", "orders");
        let partitions = vec!["region".into()];
        let keys = vec!["id".into()];
        let options = HashMap::from([
            ("bucket".into(), "2".into()),
            ("bucket-key".into(), "id".into()),
        ]);
        let mut expected = table_info(HashMap::new(), HashMap::new());
        expected.partition_keys = partitions.clone().into();
        expected.primary_keys = keys.clone();
        expected.bucket_keys = keys.clone();
        expected.num_buckets = 4;
        validate_paimon_layout(&path, &partitions, &keys, keys.clone(), &options, &expected)
            .unwrap();
        expected.partition_keys = vec![].into();
        assert!(
            validate_paimon_layout(&path, &[], &keys, keys.clone(), &options, &expected).is_err()
        );
    }

    #[test]
    fn typed_lake_partitions_match_fluss_identity_and_scope() {
        let fields = vec![
            PaimonField::new(
                0,
                "region".into(),
                PaimonType::VarChar(VarCharType::string_type()),
            ),
            PaimonField::new(
                1,
                "ts".into(),
                PaimonType::Timestamp(TimestampType::new(9).unwrap()),
            ),
        ];
        let row_type = RowType::new(vec![
            DataField::new("region", DataTypes::string(), None),
            DataField::with_field_id("ts", DataTypes::timestamp_with_precision(9), None, 1),
        ]);
        let millis = 1_704_110_400_500;
        let region = PaimonDatum::String("U/S".into());
        let timestamp = PaimonDatum::Timestamp {
            millis,
            nanos: 123_000,
        };
        let partition = BinaryRow::from_datums(&[
            (Some(&region), fields[0].data_type()),
            (Some(&timestamp), fields[1].data_type()),
        ]);
        let split = DataSplit::builder()
            .with_snapshot(42)
            .with_partition(partition)
            .with_bucket(3)
            .with_total_buckets(4)
            .with_bucket_path(
                "s3://warehouse/region=U%2FS/ts=2024-01-01 12:00:00.500123/bucket-3".into(),
            )
            .with_data_files(Vec::new())
            .build()
            .unwrap();
        let keys = vec!["region".into(), "ts".into()];
        let identity = split_partition_identity(&split, &keys, &fields, &row_type).unwrap();
        assert_eq!(
            identity,
            FlussLakePartitionIdentity::KeyValues(vec![
                ("region".into(), "U/S".into()),
                ("ts".into(), "2024-01-01-12-00-00_500123".into()),
            ])
        );
        let scope =
            col("ts").eq(fluss::row::TimestampNtz::from_millis_nanos(millis, 123_000).unwrap());
        assert!(
            crate::partition::PartitionScope::bind(Some(&scope), &row_type, &keys)
                .unwrap()
                .matches(&identity)
                .unwrap()
        );
    }
}
