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

//! Source boundaries shared by the default reader and engine-native readers.

use crate::FlussLakeLogPartition;
use crate::{FlussLakeError, FlussLakePartitionIdentity, Result};
use fluss::metadata::{
    DataLakeFormat, JsonSerde, MergeEngineType, PartitionSpec, Schema, TableBucket, TableInfo,
    TablePath,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

const CONTEXT_VERSION: u32 = 1;

/// Baseline represented by a bucket's frozen log start.
///
/// KV snapshots are not supported yet. Unknown baseline kinds are rejected
/// during import rather than interpreted as an empty baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "kind")]
pub enum FlussLakeBaseline {
    /// No baseline: read the log retained at preparation time.
    /// For PK tables this cannot recover keys absent from the retained history.
    #[serde(rename = "none")]
    None,
    /// The context's pinned lake snapshot, with `start_offset` as its seam.
    /// A seam of zero is valid and does not mean the baseline is absent.
    #[serde(rename = "lake")]
    Lake,
}

/// One frozen, half-open Fluss log range, not an engine scheduling unit.
///
/// Offsets are comparable only within `table_bucket`. Engines must preserve
/// change types and offset order when reconciling primary-key tails. Do not
/// infer the lake file layout from the bucket id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlussLakeLogRange {
    pub(crate) table_bucket: TableBucket,
    pub(crate) partition: FlussLakeLogPartition,
    pub(crate) bucket_count: i32,
    pub(crate) baseline: FlussLakeBaseline,
    pub(crate) start_offset: i64,
    pub(crate) stop_offset: i64,
    pub(crate) earliest_offset: i64,
}

impl FlussLakeLogRange {
    /// Creates an externally captured log range without querying Fluss.
    ///
    /// `snapshot_offset` is the readable lake snapshot's seam for this bucket,
    /// or `None` when there is no lake baseline. `Some(0)` still denotes a
    /// lake baseline. With no seam, the start is the captured earliest offset.
    /// `stop_offset` and `earliest_offset` must
    /// already be resolved to concrete server offsets. Table identity,
    /// partition layout and complete bucket coverage are checked by
    /// [`FlussLakeReadContext::from_boundary`].
    pub fn new(
        table_bucket: TableBucket,
        partition: FlussLakeLogPartition,
        bucket_count: i32,
        snapshot_offset: Option<i64>,
        stop_offset: i64,
        earliest_offset: i64,
    ) -> Self {
        Self {
            table_bucket,
            partition,
            bucket_count,
            baseline: if snapshot_offset.is_some() {
                FlussLakeBaseline::Lake
            } else {
                FlussLakeBaseline::None
            },
            start_offset: snapshot_offset.unwrap_or(earliest_offset),
            stop_offset,
            earliest_offset,
        }
    }

    /// Server table, partition, and bucket identity used to subscribe.
    pub fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    /// Association with business partitions, distinct from the physical bucket.
    /// Consumers must handle this kind or reject the read before emitting results.
    pub fn partition(&self) -> &FlussLakeLogPartition {
        &self.partition
    }

    /// Actual bucket count of this physical log partition.
    ///
    /// This may differ from the table default. All ranges of the same partition
    /// carry the same count and cover exactly `0..bucket_count`. Bucket pruning
    /// also requires the routing rules for this log partition kind.
    pub fn bucket_count(&self) -> i32 {
        self.bucket_count
    }

    /// Baseline to read before applying this range's changelog.
    pub fn baseline(&self) -> FlussLakeBaseline {
        self.baseline
    }

    /// Inclusive seam offset, or the captured earliest offset without a baseline.
    pub fn start_offset(&self) -> i64 {
        self.start_offset
    }

    /// Exclusive, server-issued end offset captured during preparation.
    pub fn stop_offset(&self) -> i64 {
        self.stop_offset
    }

    /// Earliest retained offset observed during preparation.
    ///
    /// Retention may advance after preparation. This is not a lease.
    pub fn earliest_offset(&self) -> i64 {
        self.earliest_offset
    }

    /// Whether this range contains no changelog records.
    pub fn is_empty(&self) -> bool {
        self.start_offset == self.stop_offset
    }

    /// Checks whether the required range was already unavailable at preparation.
    ///
    /// Call this for every range an engine-native union read needs, after safe
    /// partition/bucket pruning. Lake-only reads do not need this check. A
    /// reader must still detect retention or truncation during execution.
    pub fn validate_available(&self) -> Result<()> {
        if self.start_offset < self.earliest_offset {
            return Err(FlussLakeError::DataUnavailable(format!(
                "required log range [{}, {}) for {} starts before retained offset {}; the frozen view cannot be reconstructed",
                self.start_offset, self.stop_offset, self.table_bucket, self.earliest_offset
            )));
        }
        Ok(())
    }
}

/// Immutable source state for one table, independent of physical execution.
///
/// A context fixes the readable lake snapshot and live Fluss bucket boundaries
/// within its partition scope. It is not a global transactional
/// snapshot, a retention lease, or a file/split plan. It carries neither
/// credentials nor projection or row-filter configuration. Scans can share
/// this boundary only within its captured partition scope.
///
/// The lake snapshot may also contain partitions no longer present in Fluss.
/// Engines must plan the entire selected lake baseline, including those
/// partitions; `log_ranges()` is not a complete lake partition inventory.
/// Engine-native readers own physical planning and reconciliation. Without a
/// baseline, PK results cover only keys reconstructible from the retained log.
/// Workers must not refresh boundaries or silently skip missing inputs.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "ContextDescriptor", into = "ContextDescriptor")]
pub struct FlussLakeReadContext {
    descriptor: ContextDescriptor,
    schema: Schema,
}

/// Uses the public Fluss schema JSON, not Rust's internal Schema serde.
/// No arbitrary catalog/table property map is allowed in this descriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContextDescriptor {
    context_version: u32,
    table_path: TablePath,
    table_id: i64,
    table_modified_time: i64,
    schema_id: i32,
    schema: serde_json::Value,
    partition_keys: Vec<String>,
    partition_filter: BTreeMap<String, String>,
    bucket_keys: Vec<String>,
    num_buckets: i32,
    merge_engine: Option<String>,
    #[serde(deserialize_with = "Option::deserialize")]
    lake: Option<LakeDescriptor>,
    log_ranges: Vec<FlussLakeLogRange>,
}

/// Lake configuration is present only for lake-enabled tables. A configured
/// lake without a readable snapshot is distinct from a Fluss-only table.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LakeDescriptor {
    table_path: TablePath,
    format: String,
    snapshot_id: Option<i64>,
}

impl LakeDescriptor {
    fn capture(table: &TableInfo, snapshot_id: Option<i64>) -> Result<Option<Self>> {
        if !table
            .table_config
            .is_datalake_enabled()
            .map_err(invalid_context)?
        {
            if snapshot_id.is_some() {
                return Err(invalid_context(
                    "a non-lake table cannot have a lake snapshot",
                ));
            }
            return Ok(None);
        }
        Ok(Some(Self {
            table_path: crate::table::resolve_lake_table_path(table)?,
            format: table
                .table_config
                .get_datalake_format()
                .map_err(invalid_context)?
                .ok_or_else(|| invalid_context("missing lake format"))?
                .to_string(),
            snapshot_id,
        }))
    }
}

impl FlussLakeReadContext {
    /// Imports boundaries already captured by a trusted query coordinator.
    ///
    /// This performs the same validation as [`Self::from_json`] without
    /// querying metadata or refreshing any offsets. The caller must supply
    /// the readable snapshot id and its matching seam offsets in `log_ranges`,
    /// including every bucket of each captured live partition. Lake-only
    /// partitions remain discoverable through the pinned lake snapshot.
    ///
    /// Runtime credentials and scan configuration are supplied separately.
    /// Retention availability must still be checked for the required ranges.
    pub fn from_boundary(
        table: &TableInfo,
        lake_snapshot_id: Option<i64>,
        log_ranges: Vec<FlussLakeLogRange>,
    ) -> Result<Self> {
        Self::from_boundary_with_partition_filter(
            table,
            lake_snapshot_id,
            log_ranges,
            &PartitionSpec::new(HashMap::<String, String>::new()),
        )
    }

    /// Imports a boundary restricted by an exact, possibly partial partition spec.
    ///
    /// Ordinary log partitions must match the filter. Consumers must apply the
    /// same business-partition filter to the pinned lake snapshot, including lake-only partitions,
    /// and must not reuse this context for a scan outside that scope.
    pub fn from_boundary_with_partition_filter(
        table: &TableInfo,
        lake_snapshot_id: Option<i64>,
        log_ranges: Vec<FlussLakeLogRange>,
        partition_filter: &PartitionSpec,
    ) -> Result<Self> {
        ContextDescriptor {
            context_version: CONTEXT_VERSION,
            table_path: table.table_path.clone(),
            table_id: table.table_id,
            table_modified_time: table.modified_time,
            schema_id: table.schema_id,
            schema: table.schema.serialize_json().map_err(invalid_context)?,
            partition_keys: table.partition_keys.to_vec(),
            partition_filter: partition_filter
                .get_spec_map()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            bucket_keys: table.bucket_keys.clone(),
            num_buckets: table.num_buckets,
            merge_engine: table
                .table_config
                .get_merge_engine_type()
                .map_err(invalid_context)?
                .map(|engine| engine.to_string()),
            lake: LakeDescriptor::capture(table, lake_snapshot_id)?,
            log_ranges,
        }
        .try_into()
    }

    /// Context transport version, independent of default split versions.
    pub fn context_version(&self) -> u32 {
        self.descriptor.context_version
    }

    /// Logical Fluss table path. Catalog credentials are supplied separately.
    pub fn table_path(&self) -> &TablePath {
        &self.descriptor.table_path
    }

    /// Physical lake database/table name, or `None` for a non-lake table.
    /// Catalog credentials and runtime catalog mapping are supplied separately.
    pub fn lake_table_path(&self) -> Option<&TablePath> {
        self.descriptor.lake.as_ref().map(|lake| &lake.table_path)
    }

    /// Server-assigned identity, used to reject a dropped/recreated table.
    pub fn table_id(&self) -> i64 {
        self.descriptor.table_id
    }

    /// Source metadata modification time, not a query snapshot timestamp.
    ///
    /// Informational only: unrelated property or comment changes do not
    /// invalidate the frozen read identity.
    pub fn table_modified_time(&self) -> i64 {
        self.descriptor.table_modified_time
    }

    /// Frozen Fluss schema identity.
    pub fn schema_id(&self) -> i32 {
        self.descriptor.schema_id
    }

    /// Full, unprojected source schema, including stable field identities.
    pub fn table_schema(&self) -> &Schema {
        &self.schema
    }

    /// Table partition-key names in canonical order.
    pub fn partition_keys(&self) -> &[String] {
        &self.descriptor.partition_keys
    }

    /// Table bucket-key names. Pruning also requires the routing rules for each
    /// log partition kind; these keys alone do not establish those rules.
    pub fn bucket_keys(&self) -> &[String] {
        &self.descriptor.bucket_keys
    }

    /// Exact partition scope shared by lake planning and log capture.
    /// An empty map means the whole table. Values use Fluss metadata encoding.
    pub fn partition_filter(&self) -> &BTreeMap<String, String> {
        &self.descriptor.partition_filter
    }

    /// Frozen table default for new partitions, not execution parallelism.
    ///
    /// Use each log range's `bucket_count()` for live partitions and the lake
    /// source's layout for lake-only partitions, not this default.
    pub fn num_buckets(&self) -> i32 {
        self.descriptor.num_buckets
    }

    /// Lake format identifier, such as `paimon`, or `None` for a non-lake table.
    pub fn lake_format(&self) -> Option<&str> {
        self.descriptor
            .lake
            .as_ref()
            .map(|lake| lake.format.as_str())
    }

    /// Explicit Fluss merge engine, or `None` for the default semantics.
    ///
    /// Default semantics are append for log tables and deduplicate for PK
    /// tables. The lake adapter must separately validate its baseline merge
    /// engine and layout before executing a union read.
    pub fn merge_engine(&self) -> Option<&str> {
        self.descriptor.merge_engine.as_deref()
    }

    /// Readable lake snapshot to pass to the lake planner without refreshing it.
    pub fn lake_snapshot_id(&self) -> Option<i64> {
        self.descriptor
            .lake
            .as_ref()
            .and_then(|lake| lake.snapshot_id)
    }

    /// Physical log ranges required within the business `partition_filter()` scope.
    /// Consumers must handle every log partition kind or reject the read; ranges
    /// must not be skipped merely because their kind is unsupported.
    /// Lake-only partitions must be planned separately within the same scope.
    pub fn log_ranges(&self) -> &[FlussLakeLogRange] {
        &self.descriptor.log_ranges
    }

    /// Serializes the public, credential-free context as UTF-8 JSON.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&self.descriptor).map_err(invalid_context)
    }

    /// Decodes and validates a public context, including version and coverage.
    ///
    /// Transport is not authentication: receive contexts only from a trusted
    /// query coordinator and supply credentials outside the context.
    pub fn from_json(bytes: &[u8]) -> Result<Self> {
        let value: serde_json::Value = serde_json::from_slice(bytes).map_err(invalid_context)?;
        let version = value
            .get("context_version")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| invalid_context("missing or invalid context_version"))?;
        validate_version(version)?;
        serde_json::from_value::<ContextDescriptor>(value)
            .map_err(invalid_context)?
            .try_into()
    }

    /// Checks that live metadata still identifies the same source table.
    ///
    /// Schema and read semantics come from this context, not live metadata.
    /// Readers must map data by column ID into `table_schema()`, even when
    /// columns were added during or after preparation. This identity check
    /// does not certify that a reader supports every schema evolution.
    ///
    /// This does not refresh the snapshot, add newly created partitions, or
    /// verify that data is still retained. An engine-native adapter must also
    /// validate its lake layout and enforce bounded log availability.
    pub fn validate_table(&self, table: &TableInfo) -> Result<()> {
        let lake_changed = if let Some(lake) = &self.descriptor.lake {
            let live = LakeDescriptor::capture(table, None)?;
            live.as_ref()
                .is_none_or(|live| live.table_path != lake.table_path || live.format != lake.format)
        } else {
            // Enabling tiering later does not change a frozen Fluss-only read.
            false
        };
        if self.table_path() != &table.table_path
            || self.table_id() != table.table_id
            || lake_changed
        {
            return Err(FlussLakeError::SchemaIncompatible(format!(
                "table metadata no longer matches the frozen read context for {} (table id {}, schema id {})",
                self.table_path(),
                self.table_id(),
                self.schema_id()
            )));
        }
        Ok(())
    }
}

impl From<FlussLakeReadContext> for ContextDescriptor {
    fn from(context: FlussLakeReadContext) -> Self {
        context.descriptor
    }
}

impl TryFrom<ContextDescriptor> for FlussLakeReadContext {
    type Error = FlussLakeError;

    fn try_from(descriptor: ContextDescriptor) -> Result<Self> {
        validate_version(u64::from(descriptor.context_version))?;
        if descriptor.table_path.database().is_empty()
            || descriptor.table_path.table().is_empty()
            || descriptor.table_id < 0
            || descriptor.table_modified_time < 0
            || descriptor.schema_id < 0
            || descriptor.num_buckets <= 0
        {
            return Err(invalid_context(
                "invalid table, schema, snapshot, or bucket identity",
            ));
        }
        if let Some(lake) = &descriptor.lake {
            if lake.table_path.database().is_empty()
                || lake.table_path.table().is_empty()
                || lake.snapshot_id.is_some_and(|id| id < 0)
            {
                return Err(invalid_context("invalid lake identity"));
            }
            lake.format
                .parse::<DataLakeFormat>()
                .map_err(invalid_context)?;
        }
        if let Some(engine) = &descriptor.merge_engine {
            engine.parse::<MergeEngineType>().map_err(invalid_context)?;
        }
        let schema = Schema::deserialize_json(&descriptor.schema).map_err(invalid_context)?;
        // Optional schema attributes may evolve independently, but a new
        // schema format version must not be interpreted with old semantics.
        if descriptor.schema.get("version")
            != schema
                .serialize_json()
                .map_err(invalid_context)?
                .get("version")
        {
            return Err(invalid_context("unsupported Fluss schema JSON version"));
        }
        crate::partition::validate_partition_filter(
            descriptor.partition_filter.keys(),
            &descriptor.partition_keys,
        )?;
        let columns: HashSet<_> = schema.column_names().into_iter().collect();
        for keys in [&descriptor.partition_keys, &descriptor.bucket_keys] {
            let mut seen = HashSet::new();
            if keys
                .iter()
                .any(|key| !columns.contains(key.as_str()) || !seen.insert(key))
            {
                return Err(invalid_context("unknown or duplicate partition/bucket key"));
            }
        }
        if descriptor
            .bucket_keys
            .iter()
            .any(|key| descriptor.partition_keys.contains(key))
        {
            return Err(invalid_context(
                "bucket keys must not contain partition keys",
            ));
        }
        if schema.primary_key().is_some() {
            let primary_keys = schema.primary_key_column_names();
            if descriptor.bucket_keys.is_empty()
                || descriptor
                    .partition_keys
                    .iter()
                    .chain(&descriptor.bucket_keys)
                    .any(|key| !primary_keys.contains(&key.as_str()))
            {
                return Err(invalid_context(
                    "PK partition and bucket keys must belong to the primary key",
                ));
            }
        }
        let mut groups = HashMap::new();
        let mut partition_ids = HashMap::new();
        for range in &descriptor.log_ranges {
            let bucket = range.table_bucket();
            let FlussLakeLogPartition::Ordinary(identity) = range.partition();
            if bucket.table_id() != descriptor.table_id
                || bucket.bucket_id() < 0
                || range.bucket_count <= 0
                || bucket.bucket_id() >= range.bucket_count
                || bucket.partition_id().is_some_and(|id| id < 0)
                || !identity.matches_filter(&descriptor.partition_filter)
                || range.start_offset < 0
                || range.earliest_offset < 0
                || range.start_offset > range.stop_offset
                || range.earliest_offset > range.stop_offset
                || match range.baseline {
                    FlussLakeBaseline::Lake => descriptor
                        .lake
                        .as_ref()
                        .and_then(|lake| lake.snapshot_id)
                        .is_none(),
                    FlussLakeBaseline::None => range.start_offset != range.earliest_offset,
                }
            {
                return Err(invalid_context("invalid bucket identity or log boundary"));
            }
            match identity {
                FlussLakePartitionIdentity::Unpartitioned => {
                    if !descriptor.partition_keys.is_empty()
                        || bucket.partition_id().is_some()
                        || range.bucket_count != descriptor.num_buckets
                    {
                        return Err(invalid_context(
                            "unpartitioned range in a partitioned context",
                        ));
                    }
                }
                FlussLakePartitionIdentity::KeyValues(_) => {
                    if bucket.partition_id().is_none()
                        || !identity.matches_keys(&descriptor.partition_keys)
                    {
                        return Err(invalid_context(
                            "partition values do not match the frozen schema",
                        ));
                    }
                }
            }
            if let Some(existing) = partition_ids.insert(bucket.partition_id(), identity)
                && existing != identity
            {
                return Err(invalid_context(
                    "partition id has conflicting logical identities",
                ));
            }
            let (partition_id, bucket_count, buckets) = groups
                .entry(identity)
                .or_insert_with(|| (bucket.partition_id(), range.bucket_count, HashSet::new()));
            if *partition_id != bucket.partition_id()
                || *bucket_count != range.bucket_count
                || !buckets.insert(bucket.bucket_id())
            {
                return Err(invalid_context(
                    "duplicate or conflicting partition/bucket range",
                ));
            }
        }
        if (descriptor.partition_keys.is_empty() && groups.len() != 1)
            || groups
                .values()
                .any(|(_, count, buckets)| buckets.len() != *count as usize)
        {
            return Err(invalid_context(
                "context must cover every bucket of each captured live partition",
            ));
        }
        Ok(Self { descriptor, schema })
    }
}

fn validate_version(version: u64) -> Result<()> {
    if version != u64::from(CONTEXT_VERSION) {
        return Err(FlussLakeError::IncompatibleReadContextVersion(format!(
            "context version {version}; supported version is {CONTEXT_VERSION}"
        )));
    }
    Ok(())
}

fn invalid_context(error: impl std::fmt::Display) -> FlussLakeError {
    FlussLakeError::InvalidReadContext(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::DataTypes;
    use serde_json::{Value, json};

    include!("../tests/fixtures/java_read_context.rs");

    fn table(partitioned: bool) -> TableInfo {
        TableInfo::new(
            TablePath::new("fluss", "orders"),
            7,
            2,
            Schema::builder()
                .column("id", DataTypes::int())
                .column("region", DataTypes::string())
                .column("amount", DataTypes::bigint())
                .primary_key_named("orders_pk", vec!["id", "region"])
                .unwrap()
                .build()
                .unwrap(),
            vec!["id".to_string()],
            if partitioned {
                vec!["region".to_string()]
            } else {
                vec![]
            }
            .into(),
            2,
            HashMap::from([
                ("table.datalake.enabled".to_string(), "true".to_string()),
                ("table.datalake.format".to_string(), "paimon".to_string()),
                (
                    "table.datalake.paimon.s3.secret-key".to_string(),
                    "SECRET".to_string(),
                ),
                (
                    "table.datalake.paimon.warehouse".to_string(),
                    "s3://private/warehouse".to_string(),
                ),
            ]),
            HashMap::new(),
            None,
            0,
            0,
        )
    }

    fn context(partitioned: bool) -> FlussLakeReadContext {
        let partition = if partitioned {
            FlussLakePartitionIdentity::KeyValues(vec![("region".to_string(), "US".to_string())])
        } else {
            FlussLakePartitionIdentity::Unpartitioned
        };
        FlussLakeReadContext::from_boundary(
            &table(partitioned),
            Some(42),
            (0..2)
                .map(|bucket| {
                    FlussLakeLogRange::new(
                        TableBucket::new_with_partition(7, partitioned.then_some(9), bucket),
                        FlussLakeLogPartition::Ordinary(partition.clone()),
                        2,
                        Some(12),
                        20,
                        8,
                    )
                })
                .collect(),
        )
        .unwrap()
    }

    fn value(partitioned: bool) -> serde_json::Value {
        serde_json::from_slice(&context(partitioned).to_json().unwrap()).unwrap()
    }

    fn assert_invalid(value: serde_json::Value) {
        assert!(
            matches!(
                FlussLakeReadContext::from_json(&serde_json::to_vec(&value).unwrap()),
                Err(FlussLakeError::InvalidReadContext(_))
            ),
            "unexpectedly accepted {value}"
        );
    }

    #[test]
    fn public_context_round_trips_without_a_lake_backend() {
        for partitioned in [false, true] {
            let original = context(partitioned);
            let bytes = original.to_json().unwrap();
            let decoded = FlussLakeReadContext::from_json(&bytes).unwrap();
            assert_eq!(decoded.to_json().unwrap(), bytes);
            assert_eq!(decoded.log_ranges(), original.log_ranges());
            assert_eq!(decoded.table_schema(), original.table_schema());
            // The canonical schema intentionally omits constraint names.
            // A named PK must not be mistaken for schema drift.
            decoded.validate_table(&table(partitioned)).unwrap();
            let via_serde: FlussLakeReadContext = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(serde_json::to_vec(&via_serde).unwrap(), bytes);
        }
    }

    #[test]
    fn typed_import_reuses_context_validation() {
        let original = context(true);
        assert!(matches!(
            FlussLakeReadContext::from_boundary(
                &table(true),
                Some(42),
                vec![original.log_ranges()[0].clone()],
            ),
            Err(FlussLakeError::InvalidReadContext(_))
        ));
        let mut disabled = table(true);
        disabled
            .table_config
            .properties
            .insert("table.datalake.enabled".into(), "false".into());
        assert!(matches!(
            FlussLakeReadContext::from_boundary(
                &disabled,
                Some(42),
                original.log_ranges().to_vec()
            ),
            Err(FlussLakeError::InvalidReadContext(_))
        ));
    }

    #[test]
    fn fluss_only_context_freezes_retained_offsets_without_lake_metadata() {
        let mut info = table(false);
        info.table_config
            .properties
            .insert("table.datalake.enabled".into(), "false".into());
        let ranges = (0..2)
            .map(|bucket| {
                FlussLakeLogRange::new(
                    TableBucket::new(7, bucket),
                    FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
                    2,
                    None,
                    20,
                    8,
                )
            })
            .collect();
        let context = FlussLakeReadContext::from_boundary(&info, None, ranges).unwrap();
        assert_eq!(context.lake_format(), None);
        assert_eq!(context.lake_table_path(), None);
        context.validate_table(&info).unwrap();
        let bytes = context.to_json().unwrap();
        let decoded = FlussLakeReadContext::from_json(&bytes).unwrap();
        assert!(decoded.log_ranges().iter().all(|r| r.start_offset() == 8));
        assert_eq!(decoded.to_json().unwrap(), bytes);
        let mut missing: Value = serde_json::from_slice(&bytes).unwrap();
        missing.as_object_mut().unwrap().remove("lake");
        assert_invalid(missing);
        // A later tiering configuration must not refresh this log-only boundary.
        context.validate_table(&table(false)).unwrap();
    }

    #[test]
    fn java_boundary_transport_round_trips_without_changing_offsets() {
        let decoded = FlussLakeReadContext::from_json(JAVA_READ_CONTEXT.as_bytes()).unwrap();
        assert_eq!(decoded.context_version(), 1);
        assert_eq!(decoded.table_id(), 7);
        assert_eq!(decoded.schema_id(), 2);
        assert_eq!(
            decoded.table_schema().primary_key_column_names(),
            vec!["id", "region"]
        );
        assert_eq!(
            decoded.lake_table_path(),
            Some(&TablePath::new("lake_db", "orders_lake"))
        );
        assert_eq!(decoded.lake_snapshot_id(), Some(42));
        assert_eq!(decoded.log_ranges().len(), 6);
        assert_eq!(decoded.log_ranges()[0].stop_offset(), 9_007_199_254_740_993);
        assert_eq!(decoded.log_ranges()[2].bucket_count(), 4);
        assert_eq!(
            decoded.log_ranges()[2].partition(),
            &FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::KeyValues(vec![(
                "region".into(),
                "欧洲\"\n".into()
            )]))
        );
        let untiered = &decoded.log_ranges()[5];
        assert_eq!(decoded.log_ranges()[0].baseline(), FlussLakeBaseline::Lake);
        assert_eq!(untiered.baseline(), FlussLakeBaseline::None);
        assert_eq!(untiered.start_offset(), 8);
        assert_eq!(untiered.earliest_offset(), 8);
        untiered.validate_available().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&decoded.to_json().unwrap()).unwrap(),
            serde_json::from_str::<Value>(JAVA_READ_CONTEXT).unwrap()
        );
    }

    #[test]
    fn mixed_partition_counts_require_complete_coverage() {
        let original: Value = serde_json::from_str(JAVA_READ_CONTEXT).unwrap();
        for count in [0, -1, 2, 5] {
            let mut invalid = original.clone();
            invalid["log_ranges"][2]["bucket_count"] = json!(count);
            assert_invalid(invalid);
        }
        let mut missing = original.clone();
        missing["log_ranges"].as_array_mut().unwrap().pop();
        assert_invalid(missing);
        let mut missing_count = original;
        missing_count["log_ranges"][0]
            .as_object_mut()
            .unwrap()
            .remove("bucket_count");
        assert_invalid(missing_count);
        let mut root = value(false);
        for range in root["log_ranges"].as_array_mut().unwrap() {
            range["bucket_count"] = json!(4);
        }
        assert_invalid(root);
    }

    #[test]
    fn transport_contains_only_source_contract_fields() {
        let encoded = String::from_utf8(context(true).to_json().unwrap()).unwrap();
        for excluded in [
            "SECRET",
            "secret-key",
            "warehouse",
            "properties",
            "projection",
            "\"filter\"",
            "batch_size",
            "encoded_split",
            "catalog",
        ] {
            assert!(!encoded.contains(excluded), "context leaked {excluded}");
        }
    }

    #[test]
    fn optional_transport_fields_do_not_require_a_version_bump() {
        let mut extended = value(false);
        extended["estimated_rows"] = json!(100);
        extended["log_ranges"][0]["estimated_bytes"] = json!(1000);
        extended["log_ranges"][0]["table_bucket"]["future_hint"] = json!(true);
        extended["log_ranges"][0]["baseline"]["future_hint"] = json!(true);
        extended["schema"]["columns"][0]["future_hint"] = json!(true);
        let decoded =
            FlussLakeReadContext::from_json(&serde_json::to_vec(&extended).unwrap()).unwrap();
        assert_eq!(decoded.log_ranges(), context(false).log_ranges());
        assert_eq!(decoded.table_schema(), context(false).table_schema());
        let round_trip = serde_json::to_value(decoded).unwrap();
        assert!(round_trip.get("estimated_rows").is_none());
        // Keep the original core schema JSON, including optional attributes
        // a producer understands but this Rust version does not expose.
        assert_eq!(round_trip["schema"], extended["schema"]);
    }

    #[test]
    fn incompatible_versions_are_explicit_errors() {
        for version in [0, 2, u64::MAX] {
            let mut value = value(false);
            value["context_version"] = json!(version);
            assert!(matches!(
                FlussLakeReadContext::from_json(&serde_json::to_vec(&value).unwrap()),
                Err(FlussLakeError::IncompatibleReadContextVersion(_))
            ));
        }
        for version in [json!(null), json!("1"), json!(-1)] {
            let mut value = value(false);
            value["context_version"] = version;
            assert_invalid(value);
        }
    }

    #[test]
    fn rejects_invalid_source_identities_and_schema_shapes() {
        for (key, invalid) in [
            ("table_id", json!(-1)),
            ("table_modified_time", json!(-1)),
            ("schema_id", json!(-1)),
            ("num_buckets", json!(0)),
            ("merge_engine", json!("deduplicate")),
            ("bucket_keys", json!(["missing"])),
            ("bucket_keys", json!(["id", "id"])),
            ("bucket_keys", json!([])),
            ("bucket_keys", json!(["amount"])),
            ("partition_keys", json!(["amount"])),
        ] {
            let mut value = value(false);
            value[key] = invalid;
            assert_invalid(value);
        }
        for (key, invalid) in [("snapshot_id", json!(-1)), ("format", json!("unknown"))] {
            let mut invalid_context = value(false);
            invalid_context["lake"][key] = invalid;
            assert_invalid(invalid_context);
        }
        let mut value = value(false);
        value["schema"]["version"] = json!(2);
        assert_invalid(value);
    }

    #[test]
    fn rejects_incomplete_duplicate_or_conflicting_buckets() {
        let mut missing = value(false);
        missing["log_ranges"].as_array_mut().unwrap().pop();
        assert_invalid(missing);
        let mut duplicate = value(true);
        duplicate["log_ranges"][1] = duplicate["log_ranges"][0].clone();
        assert!(serde_json::from_value::<FlussLakeReadContext>(duplicate.clone()).is_err());
        assert_invalid(duplicate);
        let mut conflicting = value(true);
        conflicting["log_ranges"][1]["partition"]["identity"] = serde_json::to_value(
            FlussLakePartitionIdentity::KeyValues(vec![("region".to_string(), "EU".to_string())]),
        )
        .unwrap();
        assert_invalid(conflicting);
        let mut empty = value(false);
        empty["log_ranges"] = json!([]);
        assert_invalid(empty);
    }

    #[test]
    fn rejects_wrong_table_bucket_or_partition_identity() {
        let mut unsupported = value(true);
        unsupported["log_ranges"][0]["partition"]["kind"] = json!("historical");
        assert_invalid(unsupported);
        for (key, invalid) in [
            ("table_id", json!(8)),
            ("bucket", json!(-1)),
            ("bucket", json!(2)),
            ("partition_id", json!(-1)),
            ("partition_id", json!(null)),
        ] {
            let mut value = value(true);
            value["log_ranges"][0]["table_bucket"][key] = invalid;
            assert_invalid(value);
        }
        let mut value = value(true);
        value["log_ranges"][0]["partition"]["identity"] =
            json!({"key_values": [["other_key", "US"]]});
        assert_invalid(value);
    }

    #[test]
    fn empty_partitioned_table_is_valid_but_not_a_lake_inventory() {
        let mut value = value(true);
        value["log_ranges"] = json!([]);
        let decoded =
            FlussLakeReadContext::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(decoded.log_ranges().is_empty());
        assert_eq!(decoded.lake_snapshot_id(), Some(42));
    }

    #[test]
    fn rejects_malformed_bounds_and_missing_baseline_seams() {
        for (key, invalid) in [
            ("start_offset", -1),
            ("stop_offset", 11),
            ("earliest_offset", -1),
            ("earliest_offset", 21),
        ] {
            let mut value = value(false);
            value["log_ranges"][0][key] = json!(invalid);
            assert_invalid(value);
        }
        let mut no_baseline = value(false);
        no_baseline["lake"]["snapshot_id"] = json!(null);
        assert_invalid(no_baseline);
    }

    #[test]
    fn baseline_is_explicit_even_at_offset_zero() {
        let mut ranges = context(false).log_ranges().to_vec();
        for (range, seam) in ranges.iter_mut().zip([Some(0), None]) {
            *range = FlussLakeLogRange::new(
                range.table_bucket.clone(),
                range.partition.clone(),
                range.bucket_count,
                seam,
                20,
                0,
            );
        }
        let context = FlussLakeReadContext::from_boundary(&table(false), Some(42), ranges).unwrap();
        let decoded = FlussLakeReadContext::from_json(&context.to_json().unwrap()).unwrap();
        assert_eq!(decoded.log_ranges()[0].baseline(), FlussLakeBaseline::Lake);
        assert_eq!(decoded.log_ranges()[1].baseline(), FlussLakeBaseline::None);
        for range in decoded.log_ranges() {
            assert_eq!(range.start_offset(), 0);
            range.validate_available().unwrap();
        }

        // Unknown baselines cannot become log-only reads; no-baseline ranges
        // must start at the captured earliest offset even with a lake snapshot.
        for baseline in [
            json!(null),
            json!({"kind": "kv_snapshot", "snapshot_id": 9}),
            json!({"kind": "none"}),
        ] {
            let mut invalid = value(false);
            invalid["log_ranges"][0]["baseline"] = baseline;
            assert_invalid(invalid);
        }
        let mut missing = value(false);
        missing["log_ranges"][0]
            .as_object_mut()
            .unwrap()
            .remove("baseline");
        assert_invalid(missing);
    }

    #[test]
    fn retained_gap_is_rejected_only_when_range_is_required() {
        let mut value = value(false);
        for range in value["log_ranges"].as_array_mut().unwrap() {
            range["earliest_offset"] = json!(15);
        }
        let context =
            FlussLakeReadContext::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
        // Lake-only reads and safely pruned ranges do not consume the tail.
        // Native union execution must check each required range.
        assert!(matches!(
            context.log_ranges()[0].validate_available(),
            Err(FlussLakeError::DataUnavailable(_))
        ));
    }

    #[test]
    fn validates_source_identity_without_refreshing_schema_or_bounds() {
        let context = context(true);
        let mut recreated = table(true);
        recreated.table_id += 1;
        let mut evolved = table(true);
        evolved.schema_id += 1;
        let mut rebucketed = table(false);
        rebucketed.num_buckets += 1;
        let mut moved = table(true);
        moved.table_path = TablePath::new("fluss", "another");
        for table in [recreated, moved] {
            assert!(matches!(
                context.validate_table(&table),
                Err(FlussLakeError::SchemaIncompatible(_))
            ));
        }
        evolved.schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("region", DataTypes::string())
            .column("amount", DataTypes::bigint())
            .column("added", DataTypes::string())
            .primary_key_named("orders_pk", vec!["id", "region"])
            .unwrap()
            .build()
            .unwrap();
        context.validate_table(&evolved).unwrap();
        assert_eq!(context.table_schema().columns().len(), 3);
        assert_eq!(context.schema_id(), 2);
        self::context(false).validate_table(&rebucketed).unwrap();
        assert_eq!(context.lake_snapshot_id(), Some(42));
        assert_eq!(context.log_ranges()[0].stop_offset(), 20);
    }

    #[test]
    fn unrelated_metadata_and_new_partition_defaults_preserve_frozen_bounds() {
        let context = context(true);
        let mut changed = table(true);
        changed.modified_time += 1;
        changed.comment = Some("new description".into());
        changed.num_buckets = 4;
        changed
            .properties
            .insert("unrelated".into(), "value".into());
        context.validate_table(&changed).unwrap();
        assert_eq!(context.num_buckets(), 2);
        assert!(
            context
                .log_ranges()
                .iter()
                .all(|range| range.bucket_count() == 2)
        );
        assert_eq!(context.log_ranges()[0].stop_offset(), 20);
    }

    #[test]
    fn merge_engine_is_frozen_independently_of_live_metadata() {
        let mut table = table(true);
        for value in ["VERSIONED", "versioned"] {
            table
                .properties
                .insert("table.merge-engine".into(), value.into());
            table.table_config =
                fluss::metadata::TableConfig::from_properties(table.properties.clone());
            let context = FlussLakeReadContext::from_boundary(
                &table,
                Some(42),
                context(true).log_ranges().to_vec(),
            )
            .unwrap();
            assert_eq!(context.merge_engine(), Some("versioned"));
            context.validate_table(&table).unwrap();
            table.table_config.properties.remove("table.merge-engine");
            context.validate_table(&table).unwrap();
            assert_eq!(context.merge_engine(), Some("versioned"));
        }
    }

    #[test]
    fn partition_scope_survives_transport_and_includes_lake_only_partitions() {
        let filter = PartitionSpec::new(HashMap::from([("region", "US")]));
        let context = FlussLakeReadContext::from_boundary_with_partition_filter(
            &table(true),
            Some(42),
            context(true).log_ranges().to_vec(),
            &filter,
        )
        .unwrap();
        let mut value = serde_json::to_value(context).unwrap();
        assert_eq!(value["partition_filter"], json!({"region": "US"}));
        value["log_ranges"] = json!([]);
        let lake_only =
            FlussLakeReadContext::from_json(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert_eq!(lake_only.lake_snapshot_id(), Some(42));
        let identity = FlussLakePartitionIdentity::KeyValues(vec![("region".into(), "US".into())]);
        assert!(identity.matches_filter(lake_only.partition_filter()));
        let mut invalid = self::value(true);
        invalid["partition_filter"] = json!({"region": "EU"});
        assert_invalid(invalid);
        value["partition_filter"] = json!({"amount": "1"});
        assert_invalid(value.clone());
        value.as_object_mut().unwrap().remove("partition_filter");
        assert_invalid(value);
    }

    #[test]
    fn physical_lake_mapping_is_frozen_and_not_a_runtime_catalog() {
        let context = context(true);
        let mut changed = table(true);
        for key in ["table.datalake.database-name", "table.datalake.table-name"] {
            changed.properties.insert(key.into(), "physical".into());
            assert!(matches!(
                context.validate_table(&changed),
                Err(FlussLakeError::SchemaIncompatible(_))
            ));
            changed.properties.remove(key);
        }
        let mut malformed = value(true);
        malformed["lake"]["table_path"]["table"] = json!("");
        assert_invalid(malformed);
    }
}
