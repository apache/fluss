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

pub(crate) mod protocol;

use crate::FlussLakeLogPartition;
use crate::partition::PartitionScope;
use crate::{FlussLakeError, FlussLakePartitionIdentity, Result};
use fluss::metadata::{Schema, TableBucket, TableInfo, TablePath};
use fluss::predicate::Predicate;
use std::collections::{HashMap, HashSet};

/// Baseline represented by a bucket's frozen log start.
///
/// KV snapshots are not supported yet. Unknown baseline kinds are rejected
/// by consumers rather than interpreted as an empty baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlussLakeBaseline {
    /// No baseline: read the log retained at preparation time.
    /// For PK tables this cannot recover keys absent from the retained history.
    None,
    /// The context's pinned lake snapshot, with `start_offset` as its seam.
    /// A seam of zero is valid and does not mean the baseline is absent.
    Lake,
}

/// One frozen, half-open Fluss log range, not an engine scheduling unit.
///
/// Offsets are comparable only within `table_bucket`. Engines must preserve
/// change types and offset order when reconciling primary-key tails. Do not
/// infer the lake file layout from the bucket id.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// carry the same count. Complete partition captures cover `0..bucket_count`;
    /// a bucket-scoped context carries only its selected range. Bucket pruning
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
/// baseline, PK reads require complete logs; truncated history is rejected.
/// Workers must not refresh boundaries or silently skip missing inputs.
/// Engines own task serialization, distribution and placement.
/// [`Self::to_protobuf`] exposes the shared boundary message;
/// [`Self::from_protobuf`] restores it with the schema identified by the message.
#[derive(Debug, Clone)]
pub struct FlussLakeReadContext {
    table_path: TablePath,
    table_id: i64,
    schema_id: i32,
    schema: Schema,
    partition_keys: Vec<String>,
    partition_scope: Option<Predicate>,
    bucket_scoped: bool,
    bucket_keys: Vec<String>,
    num_buckets: i32,
    merge_engine: Option<String>,
    lake: Option<LakeDescriptor>,
    log_ranges: Vec<FlussLakeLogRange>,
}

/// Lake configuration is present only for lake-enabled tables. A configured
/// lake without a readable snapshot is distinct from a Fluss-only table.
#[derive(Debug, Clone)]
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
    /// Validates identity, layout and complete bucket coverage without
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
        Self::from_boundary_with_partition_scope(table, lake_snapshot_id, log_ranges, None)
    }

    /// Imports complete partition boundaries restricted by a partition predicate.
    ///
    /// Ordinary log partitions must match the predicate. Consumers must apply the
    /// same business-partition scope to the pinned lake snapshot, including lake-only partitions,
    /// and must not reuse this context for a scan outside that scope.
    pub fn from_boundary_with_partition_scope(
        table: &TableInfo,
        lake_snapshot_id: Option<i64>,
        log_ranges: Vec<FlussLakeLogRange>,
        partition_scope: Option<&Predicate>,
    ) -> Result<Self> {
        let context = Self::from_inputs(table, lake_snapshot_id, log_ranges, partition_scope)?;
        context.validate()?;
        Ok(context)
    }

    fn from_inputs(
        table: &TableInfo,
        lake_snapshot_id: Option<i64>,
        log_ranges: Vec<FlussLakeLogRange>,
        partition_scope: Option<&Predicate>,
    ) -> Result<Self> {
        Ok(Self {
            table_path: table.table_path.clone(),
            table_id: table.table_id,
            schema_id: table.schema_id,
            schema: table.schema.clone(),
            partition_keys: table.partition_keys.to_vec(),
            partition_scope: partition_scope.cloned(),
            bucket_scoped: false,
            bucket_keys: table.bucket_keys.clone(),
            num_buckets: table.num_buckets,
            merge_engine: table
                .table_config
                .get_merge_engine_type()
                .map_err(invalid_context)?
                .map(|engine| engine.to_string()),
            lake: LakeDescriptor::capture(table, lake_snapshot_id)?,
            log_ranges,
        })
    }

    /// Logical Fluss table path. Catalog credentials are supplied separately.
    pub fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    /// Physical lake database/table name, or `None` for a non-lake table.
    /// Catalog credentials and runtime catalog mapping are supplied separately.
    pub fn lake_table_path(&self) -> Option<&TablePath> {
        self.lake.as_ref().map(|lake| &lake.table_path)
    }

    /// Server-assigned identity, used to reject a dropped/recreated table.
    pub fn table_id(&self) -> i64 {
        self.table_id
    }

    /// Frozen Fluss schema identity.
    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }

    /// Full, unprojected source schema, including stable field identities.
    pub fn table_schema(&self) -> &Schema {
        &self.schema
    }

    /// Table partition-key names in canonical order.
    pub fn partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    /// Table bucket-key names. Pruning also requires the routing rules for each
    /// log partition kind; these keys alone do not establish those rules.
    pub fn bucket_keys(&self) -> &[String] {
        &self.bucket_keys
    }

    /// Exact partition predicate shared by lake planning and log capture.
    /// None means all partitions; this is not a conservative pushdown hint.
    pub fn partition_scope(&self) -> Option<&Predicate> {
        self.partition_scope.as_ref()
    }

    /// Whether this boundary selects one bucket rather than complete partitions.
    /// Lake planning must select only that bucket; this cannot seed a full-table scan.
    pub fn is_bucket_scoped(&self) -> bool {
        self.bucket_scoped
    }

    /// Selects a bucket boundary without changing its frozen bounds.
    /// The task owns that bucket's complete baseline and tail. The coordinator
    /// remains responsible for assigning all selected buckets to the query.
    pub fn for_bucket(&self, bucket: &TableBucket) -> Result<Self> {
        let range = self
            .log_ranges
            .iter()
            .find(|range| range.table_bucket() == bucket)
            .ok_or_else(|| invalid_context("bucket is outside the captured boundary"))?;
        Ok(Self {
            table_path: self.table_path.clone(),
            table_id: self.table_id,
            schema_id: self.schema_id,
            schema: self.schema.clone(),
            partition_keys: self.partition_keys.clone(),
            partition_scope: self.partition_scope.clone(),
            bucket_scoped: true,
            bucket_keys: self.bucket_keys.clone(),
            num_buckets: self.num_buckets,
            merge_engine: self.merge_engine.clone(),
            lake: self.lake.clone(),
            log_ranges: vec![range.clone()],
        })
    }

    /// Imports a single bucket task. It is validated independently of other buckets.
    pub fn from_bucket_boundary(
        table: &TableInfo,
        snapshot_id: Option<i64>,
        range: FlussLakeLogRange,
    ) -> Result<Self> {
        let mut context = Self::from_inputs(table, snapshot_id, vec![range], None)?;
        context.bucket_scoped = true;
        context.validate()?;
        Ok(context)
    }

    /// Frozen table default for new partitions, not execution parallelism.
    ///
    /// Use each log range's `bucket_count()` for live partitions and the lake
    /// source's layout for lake-only partitions, not this default.
    pub fn num_buckets(&self) -> i32 {
        self.num_buckets
    }

    /// Lake format identifier, such as `paimon`, or `None` for a non-lake table.
    pub fn lake_format(&self) -> Option<&str> {
        self.lake.as_ref().map(|lake| lake.format.as_str())
    }

    /// Explicit Fluss merge engine, or `None` for the default semantics.
    ///
    /// Default semantics are append for log tables and deduplicate for PK
    /// tables. The lake adapter must separately validate its baseline merge
    /// engine and layout before executing a union read.
    pub fn merge_engine(&self) -> Option<&str> {
        self.merge_engine.as_deref()
    }

    /// Readable lake snapshot to pass to the lake planner without refreshing it.
    pub fn lake_snapshot_id(&self) -> Option<i64> {
        self.lake.as_ref().and_then(|lake| lake.snapshot_id)
    }

    /// Physical log ranges required within the business `partition_scope()` scope.
    /// Consumers must handle every log partition kind or reject the read; ranges
    /// must not be skipped merely because their kind is unsupported.
    /// Lake-only partitions must be planned separately within the same scope.
    pub fn log_ranges(&self) -> &[FlussLakeLogRange] {
        &self.log_ranges
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
        let lake_changed = if let Some(lake) = &self.lake {
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
            || self.partition_keys.as_slice() != table.partition_keys.as_ref()
            || self.bucket_keys != table.bucket_keys
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

impl FlussLakeReadContext {
    fn validate(&self) -> Result<()> {
        if self.table_path.database().is_empty()
            || self.table_path.table().is_empty()
            || self.table_id < 0
            || self.schema_id < 0
            || self.num_buckets <= 0
        {
            return Err(invalid_context(
                "invalid table, schema, snapshot, or bucket identity",
            ));
        }
        if self.lake_snapshot_id().is_some_and(|id| id < 0) {
            return Err(invalid_context("invalid lake identity"));
        }
        let schema = &self.schema;
        let scope = PartitionScope::bind(
            self.partition_scope.as_ref(),
            schema.row_type(),
            &self.partition_keys,
        )?;
        let primary_keys = schema.primary_key_column_names();
        let mut keys = HashSet::new();
        for key in self.partition_keys.iter().chain(&self.bucket_keys) {
            if schema.row_type().get_field_index(key).is_none() || !keys.insert(key) {
                return Err(invalid_context(
                    "partition and bucket keys must be distinct schema columns",
                ));
            }
            if schema.primary_key().is_some() && !primary_keys.contains(&key.as_str()) {
                return Err(invalid_context(
                    "PK partition and bucket keys must belong to the primary key",
                ));
            }
        }
        if schema.primary_key().is_some() && self.bucket_keys.is_empty() {
            return Err(invalid_context("PK tables must have bucket keys"));
        }
        let mut groups = HashMap::new();
        let mut partition_ids = HashMap::new();
        for range in &self.log_ranges {
            let bucket = range.table_bucket();
            let FlussLakeLogPartition::Ordinary(identity) = range.partition();
            if bucket.table_id() != self.table_id
                || bucket.bucket_id() < 0
                || range.bucket_count <= 0
                || bucket.bucket_id() >= range.bucket_count
                || bucket.partition_id().is_some_and(|id| id < 0)
                || !scope.matches(identity)?
                || range.start_offset < 0
                || range.earliest_offset < 0
                || range.start_offset > range.stop_offset
                || range.earliest_offset > range.stop_offset
                || match range.baseline {
                    FlussLakeBaseline::Lake => self
                        .lake
                        .as_ref()
                        .and_then(|lake| lake.snapshot_id)
                        .is_none(),
                    FlussLakeBaseline::None => range.start_offset != range.earliest_offset,
                }
            {
                return Err(invalid_context("invalid bucket identity or log boundary"));
            }
            if schema.primary_key().is_some()
                && range.baseline == FlussLakeBaseline::None
                && range.earliest_offset > 0
            {
                return Err(FlussLakeError::DataUnavailable(format!(
                    "PK bucket {bucket} has no baseline and its log begins at {}; KV snapshot fallback is required",
                    range.earliest_offset
                )));
            }
            match identity {
                FlussLakePartitionIdentity::Unpartitioned => {
                    if !self.partition_keys.is_empty()
                        || bucket.partition_id().is_some()
                        || range.bucket_count != self.num_buckets
                    {
                        return Err(invalid_context(
                            "unpartitioned range in a partitioned context",
                        ));
                    }
                }
                FlussLakePartitionIdentity::KeyValues(_) => {
                    if bucket.partition_id().is_none()
                        || !identity.matches_keys(&self.partition_keys)
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
        if self.bucket_scoped {
            if self.log_ranges.len() != 1 {
                return Err(invalid_context(
                    "bucket task must contain exactly one range",
                ));
            }
        } else if (self.partition_keys.is_empty() && groups.len() != 1)
            || groups
                .values()
                .any(|(_, count, buckets)| buckets.len() != *count as usize)
        {
            return Err(invalid_context(
                "context must cover every bucket of each captured live partition",
            ));
        }
        Ok(())
    }
}

fn invalid_context(error: impl std::fmt::Display) -> FlussLakeError {
    FlussLakeError::InvalidReadContext(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataTypes, SchemaInfo};
    use fluss::predicate::col;

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

    #[test]
    fn typed_boundaries_preserve_schema_and_offsets() {
        for partitioned in [false, true] {
            let original = context(partitioned);
            let range = original.log_ranges()[0].clone();
            let selected = original.for_bucket(range.table_bucket()).unwrap();
            let wire = selected.to_protobuf().unwrap();
            assert_eq!(wire.log_ranges.len(), 1);
            let mut live = table(partitioned);
            let schema = SchemaInfo::new(live.schema.clone(), live.schema_id);
            live.schema_id += 1;
            live.modified_time += 1;
            let imported = FlussLakeReadContext::from_protobuf(&wire, &live, &schema).unwrap();
            assert!(imported.is_bucket_scoped());
            assert_eq!(imported.log_ranges(), &[range]);
            assert_eq!(imported.table_schema(), schema.schema());
            assert_eq!(imported.schema_id(), schema.schema_id());
            let mut unknown = wire.clone();
            unknown.log_ranges[0].baseline = Some(2);
            assert!(FlussLakeReadContext::from_protobuf(&unknown, &live, &schema).is_err());
            let mut version = wire.clone();
            version.version = Some(2);
            assert!(FlussLakeReadContext::from_protobuf(&version, &live, &schema).is_err());
            let mut wrong_schema = wire.clone();
            wrong_schema.schema_id = Some(live.schema_id);
            assert!(FlussLakeReadContext::from_protobuf(&wrong_schema, &live, &schema).is_err());
            let mut full = wire.clone();
            full.coverage = Some(0);
            assert!(FlussLakeReadContext::from_protobuf(&full, &live, &schema).is_err());
            live.table_id += 1;
            assert!(FlussLakeReadContext::from_protobuf(&wire, &live, &schema).is_err());
        }
    }

    #[test]
    fn imported_layout_rejects_invalid_keys() {
        let original = context(true);
        for (partition_keys, bucket_keys) in [
            (vec!["region"], vec!["missing"]),
            (vec!["region"], vec!["id", "id"]),
            (vec!["region"], vec!["region"]),
            (vec!["region"], vec!["amount"]),
            (vec!["region"], vec![]),
        ] {
            let mut info = table(true);
            info.partition_keys = partition_keys.into_iter().map(str::to_owned).collect();
            info.bucket_keys = bucket_keys.into_iter().map(str::to_owned).collect();
            assert!(
                FlussLakeReadContext::from_boundary(
                    &info,
                    Some(42),
                    original.log_ranges().to_vec(),
                )
                .is_err()
            );
        }
    }

    #[test]
    fn validates_imported_bucket_coverage_and_bounds() {
        let original = context(true).log_ranges().to_vec();
        let mut duplicate = original.clone();
        duplicate[1] = duplicate[0].clone();
        let mut wrong_table = original.clone();
        wrong_table[0].table_bucket = TableBucket::new_with_partition(8, Some(9), 0);
        let mut wrong_bounds = original.clone();
        wrong_bounds[0].stop_offset = 11;
        let mut wrong_count = original.clone();
        wrong_count[0].bucket_count = 4;
        for ranges in [
            vec![original[0].clone()],
            duplicate,
            wrong_table,
            wrong_bounds,
            wrong_count,
        ] {
            assert!(matches!(
                FlussLakeReadContext::from_boundary(&table(true), Some(42), ranges),
                Err(FlussLakeError::InvalidReadContext(_))
            ));
        }
        assert!(FlussLakeReadContext::from_boundary(&table(true), None, original).is_err());
    }

    #[test]
    fn no_baseline_starts_at_earliest_and_zero_seam_is_still_a_baseline() {
        let mut info = table(false);
        info.table_config
            .properties
            .insert("table.datalake.enabled".into(), "false".into());
        for stop in [8, 20] {
            let ranges = (0..2)
                .map(|bucket| {
                    FlussLakeLogRange::new(
                        TableBucket::new(7, bucket),
                        FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
                        2,
                        None,
                        stop,
                        8,
                    )
                })
                .collect();
            assert!(matches!(
                FlussLakeReadContext::from_boundary(&info, None, ranges),
                Err(FlussLakeError::DataUnavailable(_))
            ));
        }
        info.schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("region", DataTypes::string())
            .column("amount", DataTypes::bigint())
            .build()
            .unwrap();
        let ranges = (0..2)
            .map(|bucket| {
                FlussLakeLogRange::new(
                    TableBucket::new(7, bucket),
                    FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
                    2,
                    None,
                    8,
                    8,
                )
            })
            .collect();
        let plain = FlussLakeReadContext::from_boundary(&info, None, ranges).unwrap();
        assert_eq!(plain.lake_format(), None);
        assert!(
            plain
                .log_ranges()
                .iter()
                .all(|r| r.start_offset() == 8 && r.baseline() == FlussLakeBaseline::None)
        );
        plain.validate_table(&table(false)).unwrap();
        assert!(
            FlussLakeReadContext::from_boundary(&info, Some(42), plain.log_ranges().to_vec())
                .is_err()
        );
        let ranges = (0..2)
            .map(|bucket| {
                FlussLakeLogRange::new(
                    TableBucket::new(7, bucket),
                    FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
                    2,
                    (bucket == 0).then_some(0),
                    20,
                    0,
                )
            })
            .collect();
        let mixed = FlussLakeReadContext::from_boundary(&table(false), Some(42), ranges).unwrap();
        assert_eq!(mixed.log_ranges()[0].baseline(), FlussLakeBaseline::Lake);
        assert_eq!(mixed.log_ranges()[1].baseline(), FlussLakeBaseline::None);
        assert!(mixed.log_ranges().iter().all(|r| r.start_offset() == 0));
        let info = table(false);
        let imported = FlussLakeReadContext::from_protobuf(
            &mixed.to_protobuf().unwrap(),
            &info,
            &SchemaInfo::new(info.schema.clone(), info.schema_id),
        )
        .unwrap();
        assert_eq!(imported.log_ranges(), mixed.log_ranges());
    }

    #[test]
    fn retained_gap_is_rejected_only_when_range_is_required() {
        let mut ranges = context(false).log_ranges().to_vec();
        ranges[0].earliest_offset = 15;
        let context = FlussLakeReadContext::from_boundary(&table(false), Some(42), ranges).unwrap();
        assert!(matches!(
            context.log_ranges()[0].validate_available(),
            Err(FlussLakeError::DataUnavailable(_))
        ));
    }

    #[test]
    fn scope_covers_lake_only_partitions_and_rejects_unrelated_ranges() {
        let filter = col("region").ge("US");
        let context = FlussLakeReadContext::from_boundary_with_partition_scope(
            &table(true),
            Some(42),
            vec![],
            Some(&filter),
        )
        .unwrap();
        assert!(context.log_ranges().is_empty());
        assert_eq!(context.lake_snapshot_id(), Some(42));
        let info = table(true);
        let imported = FlussLakeReadContext::from_protobuf(
            &context.to_protobuf().unwrap(),
            &info,
            &SchemaInfo::new(info.schema.clone(), info.schema_id),
        )
        .unwrap();
        assert_eq!(imported.partition_scope(), Some(&filter));
        for filter in [col("region").lt("US"), col("amount").eq(1i64)] {
            assert!(
                FlussLakeReadContext::from_boundary_with_partition_scope(
                    &table(true),
                    Some(42),
                    self::context(true).log_ranges().to_vec(),
                    Some(&filter),
                )
                .is_err()
            );
        }
    }
    #[test]
    fn validates_source_identity_without_refreshing_schema_or_bounds() {
        let context = context(true);
        let mut recreated = table(true);
        recreated.table_id += 1;
        let mut evolved = table(true);
        evolved.schema_id += 1;
        evolved.num_buckets = 4;
        evolved.modified_time += 1;
        evolved.comment = Some("new description".into());
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
        assert_eq!(context.num_buckets(), 2);
        assert!(
            context
                .log_ranges()
                .iter()
                .all(|range| range.bucket_count() == 2)
        );
        self::context(false).validate_table(&rebucketed).unwrap();
        assert_eq!(context.lake_snapshot_id(), Some(42));
        assert_eq!(context.log_ranges()[0].stop_offset(), 20);
    }

    #[test]
    fn merge_engine_is_frozen_independently_of_live_metadata() {
        let mut table = table(true);
        table
            .properties
            .insert("table.merge-engine".into(), "versioned".into());
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

    #[test]
    fn rejects_changed_lake_mapping() {
        let context = context(true);
        let mut changed = table(true);
        changed
            .properties
            .insert("table.datalake.table-name".into(), "other".into());
        assert!(matches!(
            context.validate_table(&changed),
            Err(FlussLakeError::SchemaIncompatible(_))
        ));
    }
}
