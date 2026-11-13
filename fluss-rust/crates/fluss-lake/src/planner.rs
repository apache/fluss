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

//! Default Fluss-Rust planner for bounded UnionRead requests.

use crate::FlussLakeSplitDescriptor;
use crate::bucket_pruning::BucketPruner;
use crate::error::planning_client_error;
use crate::pruning::PartitionPruner;
use crate::split::SplitStatistics;
use crate::table::{
    BoundScan, FlussLakeScan, physical_primary_key_indexes, validate_pk_union_merge_engine,
};
use crate::{
    FlussLakeError, FlussLakeLogPartition, FlussLakeLogRange, FlussLakePartitionIdentity,
    FlussLakeReadContext, FlussLakeReadPlan, FlussLakeReadSplit, LakePlannerContext,
    LakeReadSemantics, Result,
};
use fluss::metadata::{RowType, TableInfo};
use fluss::predicate::BoundPredicate;
use fluss::record::to_arrow_schema;
use std::sync::Arc;

pub(crate) async fn plan_union_read(scan: &FlussLakeScan) -> Result<FlussLakeReadPlan> {
    let (table_info, context, bound) = scan.prepare_bound().await?;
    plan_prepared(scan, &context, &table_info, bound).await
}

/// Binds scan-specific pruning and the default lake backend to fixed inputs.
/// Metadata is checked, but neither snapshots nor offset bounds are refreshed.
pub(crate) async fn plan_with_context(
    scan: &FlussLakeScan,
    context: &FlussLakeReadContext,
) -> Result<FlussLakeReadPlan> {
    scan.validate_configuration()?;
    if context.is_lake_only() && !scan.lake_only() {
        return Err(FlussLakeError::InvalidReadContext(
            "lake-only context has no log boundaries for a union read".into(),
        ));
    }
    if scan.table_path() != context.table_path() {
        return Err(FlussLakeError::InvalidReadContext(
            "scan and read context refer to different tables".to_string(),
        ));
    }
    let admin = scan
        .connection()
        .get_admin()
        .map_err(|error| planning_client_error("create Fluss admin client", error))?;
    let table_info = admin
        .get_table_info(scan.table_path())
        .await
        .map_err(|error| {
            if error.api_error() == Some(fluss::error::FlussError::TableNotExist) {
                FlussLakeError::DataUnavailable(format!(
                    "table {} from the frozen context no longer exists",
                    context.table_path()
                ))
            } else {
                planning_client_error("get table metadata", error)
            }
        })?;
    let table_info = context.table_info(&table_info)?;
    let bound = scan.bind(table_info.row_type())?;
    plan_prepared(scan, context, &table_info, bound).await
}

async fn plan_prepared(
    scan: &FlussLakeScan,
    context: &FlussLakeReadContext,
    table_info: &TableInfo,
    bound: BoundScan,
) -> Result<FlussLakeReadPlan> {
    if scan.lake_only() && context.lake_format().is_none() {
        return Err(FlussLakeError::NotLakeReadable(
            "lake-only read requires a lake-enabled table".into(),
        ));
    }
    let BoundScan {
        projection: output_projection,
        filter,
    } = bound;
    let output_schema = projected_arrow_schema(
        scan.table_path(),
        output_projection.as_deref(),
        context.table_schema().row_type(),
    )?;
    let partition_pruner = PartitionPruner::new(
        context.table_schema().row_type(),
        context.partition_keys(),
        &filter,
    );
    let lake_format = context
        .lake_format()
        .map(str::parse)
        .transpose()
        .map_err(|error| FlussLakeError::PlanningFailed(format!("invalid lake format: {error}")))?;
    // Partitions retain their own hash modulus after a table-default change.
    // Cache once per distinct layout, rather than re-encoding keys per bucket.
    let mut bucket_pruners = std::collections::HashMap::new();
    let mut bucket_may_match = |bucket_id, bucket_count| {
        // A Fluss-only context does not carry a lake hash-format contract.
        if context.lake_format().is_none() {
            return true;
        }
        bucket_pruners
            .entry(bucket_count)
            .or_insert_with(|| {
                BucketPruner::new(
                    context.table_schema().row_type(),
                    context.bucket_keys(),
                    bucket_count,
                    &lake_format,
                    &filter,
                )
            })
            .bucket_may_match(bucket_id)
    };
    let is_primary_key = table_info.has_primary_key();
    if is_primary_key && !scan.lake_only() {
        validate_pk_union_merge_engine(table_info)?;
    }
    let primary_key_indexes = if is_primary_key {
        let indexes = physical_primary_key_indexes(table_info)?;
        if indexes.is_empty() {
            return Err(FlussLakeError::PlanningFailed(format!(
                "table {} reports a primary key but resolves no physical key field indexes",
                scan.table_path()
            )));
        }
        indexes
    } else {
        Vec::new()
    };

    let lake_source = if context.lake_snapshot_id().is_some() {
        Some(scan.resolve_lake_source(table_info)?)
    } else {
        None
    };
    let snapshot_id = context.lake_snapshot_id();
    let mut tasks = match (&lake_source, snapshot_id) {
        (Some(source), Some(snapshot_id)) => {
            let safe_filter = crate::source::lake_pushdown_filter(
                &filter,
                table_info,
                is_primary_key && !scan.lake_only(),
            )
            .unwrap_or(BoundPredicate::AlwaysTrue);
            source
                .plan(LakePlannerContext {
                    table_info,
                    snapshot_id,
                    semantics: if is_primary_key {
                        LakeReadSemantics::PrimaryKey
                    } else {
                        LakeReadSemantics::Append
                    },
                    reconcile_primary_key: is_primary_key && !scan.lake_only(),
                    filter: &safe_filter,
                })
                .await?
        }
        _ => Vec::new(),
    };
    tasks = select_context_tasks(context, tasks)?;
    let task_table = Arc::new(table_info.clone());
    let mut splits = Vec::new();
    if !is_primary_key && !tasks.is_empty() {
        if let (Some(source), Some(snapshot_id)) = (&lake_source, snapshot_id) {
            validate_lake_tasks(&tasks, source.format(), snapshot_id, table_info)?;
        }
        let live_partitions: std::collections::HashMap<_, _> = context
            .log_ranges()
            .iter()
            .map(|range| {
                let FlussLakeLogPartition::Ordinary(identity) = range.partition();
                (identity, range.table_bucket().partition_id())
            })
            .collect();
        // Append baseline tasks need no Fluss bucket ownership. LakeSource applies
        // its own safe file/bucket pruning; Fluss bucket pruning applies only to logs.
        for (index, task) in tasks.drain(..).enumerate() {
            if !partition_pruner.partition_identity_may_match(&task.partition) {
                continue;
            }
            let mut range = FlussLakeLogRange::lake_only(
                table_info.table_id,
                task.bucket_id,
                task.bucket_count,
                task.partition.clone(),
            );
            range.table_bucket = fluss::metadata::TableBucket::new_with_partition(
                table_info.table_id,
                live_partitions.get(&task.partition).copied().flatten(),
                task.bucket_id,
            );
            splits.push(append_lake_split(
                &task_table,
                &range,
                snapshot_id,
                task,
                index,
            )?);
        }
    }
    let mut lake_splits = match (&lake_source, snapshot_id, is_primary_key) {
        (Some(source), Some(snapshot_id), true) => group_lake_splits(
            tasks,
            source.format(),
            snapshot_id,
            table_info,
            context.log_ranges(),
        )?,
        _ => std::collections::HashMap::new(),
    };
    for bucket_range in context.log_ranges() {
        let FlussLakeLogPartition::Ordinary(identity) = bucket_range.partition();
        let bucket_id = bucket_range.table_bucket().bucket_id();
        if !bucket_may_match(bucket_id, bucket_range.bucket_count())
            || !partition_pruner.partition_identity_may_match(identity)
        {
            continue;
        }
        if !scan.lake_only() {
            bucket_range.validate_available()?;
        }
        let planned_lake_bucket = lake_splits
            .remove(&(identity.clone(), bucket_id))
            .unwrap_or_default();
        splits.extend(plan_bucket_splits(
            &task_table,
            bucket_range,
            snapshot_id,
            planned_lake_bucket,
            &primary_key_indexes,
            scan.lake_only(),
        )?);
    }
    let mut lake_only_buckets: Vec<_> = lake_splits.into_iter().collect();
    lake_only_buckets.sort_by(
        |((left_partition, left_bucket), _), ((right_partition, right_bucket), _)| {
            partition_sort_key(left_partition)
                .cmp(partition_sort_key(right_partition))
                .then_with(|| left_bucket.cmp(right_bucket))
        },
    );
    for ((partition, bucket_id), planned_lake_bucket) in lake_only_buckets {
        // Nonempty groups have a validated, uniform lake-partition layout.
        let bucket_count = planned_lake_bucket.splits[0].bucket_count;
        if !bucket_may_match(bucket_id, bucket_count)
            || !partition_pruner.partition_identity_may_match(&partition)
        {
            continue;
        }
        if table_info.partition_keys.is_empty() && !scan.lake_only() {
            return Err(FlussLakeError::PlanningFailed(format!(
                "lake snapshot {} of the unpartitioned table {} contains an unmatched bucket {bucket_id}",
                snapshot_id.unwrap_or_default(),
                scan.table_path()
            )));
        }
        if !table_info.partition_keys.is_empty()
            && matches!(partition, crate::FlussLakePartitionIdentity::Unpartitioned)
        {
            return Err(FlussLakeError::PlanningFailed(format!(
                "lake snapshot {} of the partitioned table {} contains an unpartitioned split",
                snapshot_id.unwrap_or_default(),
                scan.table_path()
            )));
        }
        let lake_only_range =
            FlussLakeLogRange::lake_only(table_info.table_id, bucket_id, bucket_count, partition);
        splits.extend(plan_bucket_splits(
            &task_table,
            &lake_only_range,
            snapshot_id,
            planned_lake_bucket,
            &primary_key_indexes,
            true,
        )?);
    }
    FlussLakeReadPlan::new(context.clone(), output_schema, splits)
}

fn select_context_tasks(
    context: &FlussLakeReadContext,
    tasks: Vec<crate::LakeSplit>,
) -> Result<Vec<crate::LakeSplit>> {
    let scope = crate::partition::PartitionScope::bind(
        context.partition_scope(),
        context.table_schema().row_type(),
        context.partition_keys(),
    )?;
    let selected = context.is_bucket_scoped().then(|| &context.log_ranges()[0]);
    tasks
        .into_iter()
        .filter_map(|task| {
            let keep = (|| -> Result<bool> {
                if !scope.matches(&task.partition)? {
                    return Ok(false);
                }
                if let Some(range) = selected {
                    let FlussLakeLogPartition::Ordinary(partition) = range.partition();
                    if &task.partition != partition {
                        return Ok(false);
                    }
                    // An append backend may use a different layout. Such tasks cannot
                    // be assigned by Fluss bucket without rereading or dropping data.
                    if task.bucket_id < 0 || task.bucket_count != range.bucket_count() {
                        return Err(FlussLakeError::PlanningFailed(
                            "bucket-scoped reads require a Fluss-aligned lake bucket layout".into(),
                        ));
                    }
                    return Ok(task.bucket_id == range.table_bucket().bucket_id());
                }
                Ok(true)
            })();
            match keep {
                Ok(true) => Some(Ok(task)),
                Ok(false) => None,
                Err(error) => Some(Err(error)),
            }
        })
        .collect::<Result<Vec<_>>>()
}

/// Append lake tasks and log tails can be scheduled independently. Primary-key
/// reads keep the whole baseline and tail together for bucket-local reconciliation.
fn plan_bucket_splits(
    table_info: &Arc<TableInfo>,
    bucket_range: &FlussLakeLogRange,
    snapshot_id: Option<i64>,
    lake_bucket: PlannedLakeBucket,
    primary_key_indexes: &[usize],
    lake_only: bool,
) -> Result<Vec<FlussLakeReadSplit>> {
    // A lake file is not a usable PK baseline without its matching log seam.
    // Keep the frozen KV/log fallback, even if lake planning finds files here.
    let lake_bucket = if !primary_key_indexes.is_empty()
        && !lake_only
        && bucket_range.baseline() != crate::FlussLakeBaseline::Lake
    {
        PlannedLakeBucket::default()
    } else {
        lake_bucket
    };
    let include_log_tail = !lake_only && !bucket_range.is_empty();
    let kv_snapshot = matches!(
        bucket_range.baseline(),
        crate::FlussLakeBaseline::KvSnapshot { .. }
    ) && !lake_only;
    if lake_bucket.splits.is_empty() && !include_log_tail && !kv_snapshot {
        return Ok(Vec::new());
    }
    if !primary_key_indexes.is_empty() {
        let statistics = split_statistics(bucket_range, include_log_tail, &lake_bucket);
        return Ok(vec![create_logical_split(
            table_info,
            bucket_range,
            snapshot_id,
            lake_bucket.splits,
            primary_key_indexes.to_vec(),
            statistics,
        )?]);
    }

    let mut splits = Vec::with_capacity(lake_bucket.splits.len() + usize::from(include_log_tail));
    // Preserve live partition identity, but never attach its tail to a lake task.
    let mut lake_range = bucket_range.clone();
    lake_range.stop_offset = lake_range.start_offset;
    for (index, task) in lake_bucket.splits.into_iter().enumerate() {
        splits.push(append_lake_split(
            table_info,
            &lake_range,
            snapshot_id,
            task,
            index,
        )?);
    }
    if include_log_tail {
        let mut split = create_logical_split(
            table_info,
            bucket_range,
            snapshot_id,
            Vec::new(),
            Vec::new(),
            SplitStatistics::new(bucket_range.estimated_log_rows(), None),
        )?;
        split.append_id(":log");
        splits.push(split);
    }
    Ok(splits)
}

fn append_lake_split(
    table_info: &Arc<TableInfo>,
    range: &FlussLakeLogRange,
    snapshot_id: Option<i64>,
    task: crate::LakeSplit,
    index: usize,
) -> Result<FlussLakeReadSplit> {
    let statistics = SplitStatistics::new(task.estimated_rows, task.estimated_size);
    let mut split = create_logical_split(
        table_info,
        range,
        snapshot_id,
        vec![task],
        Vec::new(),
        statistics,
    )?;
    // Indexes distinguish lake tasks within this plan, not across replanning.
    split.append_id(&format!(":lake:{index}"));
    Ok(split)
}

/// Creates one opaque logical `(partition, bucket)` split.
#[allow(clippy::too_many_arguments)]
fn create_logical_split(
    table_info: &Arc<TableInfo>,
    bucket_range: &FlussLakeLogRange,
    snapshot_id: Option<i64>,
    lake_splits: Vec<crate::LakeSplit>,
    primary_key_indexes: Vec<usize>,
    statistics: SplitStatistics,
) -> Result<FlussLakeReadSplit> {
    let table_path = &table_info.table_path;
    let FlussLakeLogPartition::Ordinary(identity) = bucket_range.partition();
    let partition = match (identity, bucket_range.table_bucket.partition_id()) {
        (FlussLakePartitionIdentity::Unpartitioned, None) => "root".to_string(),
        (FlussLakePartitionIdentity::KeyValues(_), Some(partition_id)) => partition_id.to_string(),
        (FlussLakePartitionIdentity::KeyValues(key_values), None) => {
            format!("lake-only({key_values:?})")
        }
        (FlussLakePartitionIdentity::Unpartitioned, Some(partition_id)) => {
            return Err(FlussLakeError::Internal(format!(
                "unpartitioned logical split unexpectedly carries partition id {partition_id}"
            )));
        }
    };
    let split_id = format!(
        "{table_path}:{partition}:{}",
        bucket_range.table_bucket.bucket_id()
    );
    FlussLakeReadSplit::try_new(
        table_info.clone(),
        FlussLakeSplitDescriptor {
            kv_snapshot_id: match bucket_range.baseline() {
                crate::FlussLakeBaseline::KvSnapshot { snapshot_id } if lake_splits.is_empty() => {
                    Some(snapshot_id)
                }
                _ => None,
            },
            split_id,
            table_bucket: bucket_range.table_bucket.clone(),
            partition: identity.clone(),
            start_offset: bucket_range.start_offset,
            stop_offset: bucket_range.stop_offset,
            snapshot_id,
            lake_splits,
            estimated_rows: statistics.estimated_rows,
            estimated_size: statistics.estimated_size,
        },
        primary_key_indexes,
    )
}

fn partition_sort_key(partition: &crate::FlussLakePartitionIdentity) -> &[(String, String)] {
    match partition {
        crate::FlussLakePartitionIdentity::Unpartitioned => &[],
        crate::FlussLakePartitionIdentity::KeyValues(key_values) => key_values,
    }
}

#[derive(Debug)]
struct PlannedLakeBucket {
    splits: Vec<crate::LakeSplit>,
    estimated_rows: Option<usize>,
    estimated_size: Option<usize>,
}

impl Default for PlannedLakeBucket {
    fn default() -> Self {
        Self {
            splits: Vec::new(),
            estimated_rows: Some(0),
            estimated_size: Some(0),
        }
    }
}

fn group_lake_splits(
    tasks: Vec<crate::LakeSplit>,
    format: &str,
    snapshot_id: i64,
    info: &TableInfo,
    log_ranges: &[crate::FlussLakeLogRange],
) -> Result<std::collections::HashMap<(crate::FlussLakePartitionIdentity, i32), PlannedLakeBucket>>
{
    validate_lake_tasks(&tasks, format, snapshot_id, info)?;
    let mut grouped = std::collections::HashMap::new();
    let mut bucket_counts: std::collections::HashMap<_, _> = log_ranges
        .iter()
        .map(|range| {
            let FlussLakeLogPartition::Ordinary(identity) = range.partition();
            (identity, range.bucket_count())
        })
        .collect();
    if info.partition_keys.is_empty() {
        bucket_counts.insert(
            &crate::FlussLakePartitionIdentity::Unpartitioned,
            info.num_buckets,
        );
    }
    for task in &tasks {
        if task.bucket_id < 0 {
            return Err(FlussLakeError::PlanningFailed(
                "primary-key lake tasks require fixed Fluss-aligned buckets".to_string(),
            ));
        }
        if let Some(count) = bucket_counts.insert(&task.partition, task.bucket_count)
            && count != task.bucket_count
        {
            return Err(FlussLakeError::PlanningFailed(
                "lake partition bucket count conflicts with its frozen Fluss layout or another lake task".to_string()
            ));
        }
    }
    for task in tasks {
        let bucket = grouped
            .entry((task.partition.clone(), task.bucket_id))
            .or_insert_with(PlannedLakeBucket::default);
        bucket.estimated_rows = add_estimates(bucket.estimated_rows, task.estimated_rows);
        bucket.estimated_size = add_estimates(bucket.estimated_size, task.estimated_size);
        bucket.splits.push(task);
    }
    Ok(grouped)
}

fn validate_lake_tasks(
    tasks: &[crate::LakeSplit],
    format: &str,
    snapshot_id: i64,
    info: &TableInfo,
) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for task in tasks {
        task.validate(format, snapshot_id)?;
        if !seen.insert((
            &task.partition,
            task.bucket_id,
            task.payload_version,
            &task.payload,
        )) {
            return Err(FlussLakeError::PlanningFailed(
                "lake planner returned a duplicate task".to_string(),
            ));
        }
        if !task.partition.matches_keys(&info.partition_keys) {
            return Err(FlussLakeError::PlanningFailed(
                "lake split partition identity does not match the table layout".to_string(),
            ));
        }
    }
    Ok(())
}

fn split_statistics(
    bucket_range: &FlussLakeLogRange,
    include_log_tail: bool,
    lake_bucket: &PlannedLakeBucket,
) -> SplitStatistics {
    // Snapshot file sizes do not establish the number of decoded current rows.
    if matches!(
        bucket_range.baseline(),
        crate::FlussLakeBaseline::KvSnapshot { .. }
    ) {
        return SplitStatistics::new(None, None);
    }
    let log_rows = if include_log_tail {
        bucket_range.estimated_log_rows()
    } else {
        Some(0)
    };
    let log_size = if include_log_tail && !bucket_range.is_empty() {
        None
    } else {
        Some(0)
    };
    SplitStatistics::new(
        add_estimates(lake_bucket.estimated_rows, log_rows),
        add_estimates(lake_bucket.estimated_size, log_size),
    )
}

fn add_estimates(left: Option<usize>, right: Option<usize>) -> Option<usize> {
    left?.checked_add(right?)
}

fn projected_arrow_schema(
    table_path: &fluss::metadata::TablePath,
    projection: Option<&[usize]>,
    row_type: &RowType,
) -> Result<arrow::datatypes::SchemaRef> {
    let schema = to_arrow_schema(row_type).map_err(|error| {
        FlussLakeError::PlanningFailed(format!(
            "failed to convert schema for {} to Arrow: {error}",
            table_path
        ))
    })?;
    match projection {
        Some(projection) => schema.project(projection).map(Arc::new).map_err(|error| {
            FlussLakeError::PlanningFailed(format!(
                "failed to project output schema for {}: {error}",
                table_path
            ))
        }),
        None => Ok(schema),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataTypes, Schema, TableBucket, TablePath};
    use std::collections::{HashMap, HashSet};

    fn lake_bucket(range: &FlussLakeLogRange) -> PlannedLakeBucket {
        let FlussLakeLogPartition::Ordinary(identity) = range.partition();
        let splits = (0..2)
            .map(|index| {
                let mut task = crate::source::testing_split();
                task.partition = identity.clone();
                task.bucket_id = range.table_bucket.bucket_id();
                task.bucket_count = range.bucket_count;
                task.payload = vec![index];
                task.estimated_rows = Some(3);
                task.estimated_size = Some(100);
                task
            })
            .collect();
        PlannedLakeBucket {
            splits,
            estimated_rows: Some(6),
            estimated_size: Some(200),
        }
    }

    #[test]
    fn append_tasks_cover_lake_and_tail_independently() {
        let info = table_info(false, vec![], HashMap::new());
        let range = FlussLakeLogRange {
            baseline: crate::FlussLakeBaseline::Lake,
            table_bucket: TableBucket::new(info.table_id, 0),
            partition: FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
            bucket_count: 4,
            start_offset: 6,
            stop_offset: 10,
            earliest_offset: 0,
        };
        let splits =
            plan_bucket_splits(&info, &range, Some(42), lake_bucket(&range), &[], false).unwrap();
        assert_eq!(splits.len(), 3);
        assert_eq!(
            splits
                .iter()
                .map(|split| split.split_id())
                .collect::<HashSet<_>>()
                .len(),
            3
        );
        for (index, split) in splits.iter().enumerate() {
            let descriptor = split.descriptor();
            assert_eq!(descriptor.table_bucket(), range.table_bucket());
            assert_eq!(descriptor.snapshot_id(), Some(42));
            assert!(!split.is_primary_key());
            if index < 2 {
                assert_eq!(descriptor.start_offset(), 6);
                assert_eq!(descriptor.stop_offset(), 6);
                assert_eq!(descriptor.lake_splits().len(), 1);
                assert_eq!(descriptor.lake_splits()[0].payload, vec![index as u8]);
                assert_eq!(split.estimated_rows(), Some(3));
                assert_eq!(split.estimated_size(), Some(100));
            } else {
                assert_eq!(descriptor.start_offset(), 6);
                assert_eq!(descriptor.stop_offset(), 10);
                assert!(descriptor.lake_splits().is_empty());
                assert_eq!(split.estimated_rows(), Some(4));
                assert_eq!(split.estimated_size(), None);
            }
        }
        let stats = crate::FlussLakePlanStatistics::from_splits(&splits);
        assert_eq!(stats.estimated_rows, Some(10));
        assert_eq!(stats.estimated_size, None);
    }

    #[test]
    fn append_planning_omits_empty_work_and_respects_lake_only_mode() {
        let info = table_info(false, vec![], HashMap::new());
        for (has_lake, stop, lake_only, expected) in [
            (false, 0, false, 0),
            (false, 4, false, 1),
            (true, 0, false, 2),
            (true, 4, true, 2),
            (false, 4, true, 0),
        ] {
            let range = FlussLakeLogRange::new(
                TableBucket::new(info.table_id, 0),
                FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
                4,
                has_lake.then_some(0),
                stop,
                0,
            );
            let lake = if has_lake {
                lake_bucket(&range)
            } else {
                PlannedLakeBucket::default()
            };
            let splits =
                plan_bucket_splits(&info, &range, has_lake.then_some(42), lake, &[], lake_only)
                    .unwrap();
            assert_eq!(splits.len(), expected);
            if lake_only {
                assert!(
                    splits.iter().all(|split| split.descriptor().start_offset()
                        == split.descriptor().stop_offset())
                );
            }
        }
    }

    #[test]
    fn append_lake_tasks_preserve_live_and_expired_partition_identity() {
        let info = table_info(false, vec!["region".into()], HashMap::new());
        let mut all_ids = HashSet::new();
        for (partition_id, name) in [(Some(9), "live"), (None, "expired")] {
            let range = FlussLakeLogRange {
                baseline: crate::FlussLakeBaseline::Lake,
                table_bucket: TableBucket::new_with_partition(info.table_id, partition_id, 7),
                partition: FlussLakeLogPartition::Ordinary(
                    crate::FlussLakePartitionIdentity::KeyValues(vec![(
                        "region".into(),
                        name.into(),
                    )]),
                ),
                bucket_count: 8,
                start_offset: 0,
                stop_offset: 0,
                earliest_offset: 0,
            };
            let splits =
                plan_bucket_splits(&info, &range, Some(42), lake_bucket(&range), &[], false)
                    .unwrap();
            assert_eq!(splits.len(), 2);
            for split in splits {
                assert!(all_ids.insert(split.split_id().to_owned()));
                assert_eq!(
                    FlussLakeLogPartition::Ordinary(split.partition().clone()),
                    *range.partition()
                );
                let descriptor = split.descriptor();
                assert_eq!(descriptor.table_bucket().partition_id(), partition_id);
                assert_eq!(descriptor.lake_splits()[0].bucket_count, 8);
                assert_eq!(descriptor.start_offset(), descriptor.stop_offset());
            }
        }
    }

    #[test]
    fn kv_baseline_survives_empty_tail_and_task_import() {
        let info = pk_table_info(vec![], HashMap::new());
        for stop in [6, 10] {
            let range = FlussLakeLogRange::from_kv_snapshot(
                TableBucket::new(info.table_id, 0),
                FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
                4,
                &fluss::metadata::KvSnapshot {
                    bucket_id: 0,
                    snapshot_id: Some(12),
                    log_offset: Some(6),
                },
                stop,
                6,
            )
            .unwrap();
            let context =
                FlussLakeReadContext::from_bucket_boundary(&info, None, range.clone()).unwrap();
            let range = &context.log_ranges()[0];
            let splits = plan_bucket_splits(
                &info,
                range,
                None,
                PlannedLakeBucket::default(),
                &[0, 1],
                false,
            )
            .unwrap();
            assert_eq!(splits.len(), 1);
            let descriptor = splits[0].descriptor();
            assert_eq!(descriptor.kv_snapshot_id(), Some(12));
            assert_eq!(
                (descriptor.start_offset(), descriptor.stop_offset()),
                (6, stop)
            );
            assert_eq!(splits[0].estimated_rows(), None);
            let scope =
                crate::partition::PartitionScope::bind(None, info.row_type(), &info.partition_keys)
                    .unwrap();
            descriptor
                .validate(&context, Some(range), &scope, false)
                .unwrap();
            for wrong in [None, Some(13)] {
                let mut altered = descriptor.clone();
                altered.kv_snapshot_id = wrong;
                assert!(
                    altered
                        .validate(&context, Some(range), &scope, false)
                        .is_err()
                );
            }
            assert!(
                plan_bucket_splits(
                    &info,
                    range,
                    None,
                    PlannedLakeBucket::default(),
                    &[0, 1],
                    true
                )
                .unwrap()
                .is_empty()
            );
            let fallback =
                plan_bucket_splits(&info, range, Some(42), lake_bucket(range), &[0, 1], false)
                    .unwrap();
            assert!(fallback[0].descriptor().lake_splits().is_empty());
            assert_eq!(fallback[0].descriptor().kv_snapshot_id(), Some(12));
        }
    }

    #[test]
    fn imported_tasks_preserve_required_logs() {
        for primary_key in [false, true] {
            let info = table_info(primary_key, vec![], HashMap::new());
            let keys = physical_primary_key_indexes(&info).unwrap();
            let range = FlussLakeLogRange::new(
                TableBucket::new(info.table_id, 0),
                FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
                4,
                Some(6),
                10,
                0,
            );
            let context =
                FlussLakeReadContext::from_bucket_boundary(&info, Some(42), range.clone()).unwrap();
            let scope =
                crate::partition::PartitionScope::bind(None, info.row_type(), &info.partition_keys)
                    .unwrap();
            let splits =
                plan_bucket_splits(&info, &range, Some(42), lake_bucket(&range), &keys, false)
                    .unwrap();
            let validate = |task: &FlussLakeSplitDescriptor, lake_only| {
                task.validate(&context, Some(&range), &scope, lake_only)
            };
            for split in &splits {
                validate(split.descriptor(), false).unwrap();
            }
            let mut altered = splits.last().unwrap().descriptor().clone();
            altered.stop_offset = altered.start_offset;
            assert!(validate(&altered, false).is_err());
            if primary_key {
                validate(&altered, true).unwrap();
                altered.lake_splits.clear();
                assert!(validate(&altered, false).is_err());
            } else {
                let mut lake = splits[0].descriptor().clone();
                lake.start_offset = 0;
                lake.stop_offset = 0;
                validate(&lake, false).unwrap();
            }
        }
    }

    #[test]
    fn pk_lake_files_without_a_seam_do_not_replace_complete_logs() {
        let info = pk_table_info(vec![], HashMap::new());
        let range = FlussLakeLogRange::new(
            TableBucket::new(info.table_id, 0),
            FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
            4,
            None,
            10,
            0,
        );
        let splits =
            plan_bucket_splits(&info, &range, Some(42), lake_bucket(&range), &[0, 1], false)
                .unwrap();
        assert_eq!(splits.len(), 1);
        let descriptor = splits[0].descriptor();
        assert!(descriptor.lake_splits().is_empty());
        assert_eq!(
            (descriptor.start_offset(), descriptor.stop_offset()),
            (0, 10)
        );
        let lake_only =
            plan_bucket_splits(&info, &range, Some(42), lake_bucket(&range), &[0, 1], true)
                .unwrap();
        assert_eq!(lake_only[0].descriptor().lake_splits().len(), 2);
        let context =
            FlussLakeReadContext::from_bucket_boundary(&info, Some(42), range.clone()).unwrap();
        let scope =
            crate::partition::PartitionScope::bind(None, info.row_type(), &info.partition_keys)
                .unwrap();
        descriptor
            .validate(&context, Some(&range), &scope, false)
            .unwrap();
        assert!(
            lake_only[0]
                .descriptor()
                .validate(&context, Some(&range), &scope, false)
                .is_err()
        );
    }

    #[test]
    fn primary_key_planning_keeps_bucket_baseline_and_tail_together() {
        let info = pk_table_info(vec![], HashMap::new());
        let range = FlussLakeLogRange {
            baseline: crate::FlussLakeBaseline::Lake,
            table_bucket: TableBucket::new(info.table_id, 0),
            partition: FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
            bucket_count: 4,
            start_offset: 6,
            stop_offset: 10,
            earliest_offset: 0,
        };
        for lake_only in [false, true] {
            let splits = plan_bucket_splits(
                &info,
                &range,
                Some(42),
                lake_bucket(&range),
                &[0],
                lake_only,
            )
            .unwrap();
            assert_eq!(splits.len(), 1);
            let descriptor = splits[0].descriptor();
            assert!(splits[0].is_primary_key());
            assert_eq!(descriptor.lake_splits().len(), 2);
            assert_eq!(descriptor.start_offset(), 6);
            assert_eq!(descriptor.stop_offset(), 10);
            assert_eq!(
                splits[0].estimated_rows(),
                Some(if lake_only { 6 } else { 10 })
            );
            assert_eq!(
                splits[0].estimated_size(),
                if lake_only { Some(200) } else { None }
            );
        }
    }

    #[test]
    fn backend_tasks_are_validated_before_grouping_or_pruning() {
        let info = pk_table_info(vec![], HashMap::new());
        let task = crate::source::testing_split();
        let mut second = task.clone();
        second.payload = b"another-task".to_vec();
        let groups =
            group_lake_splits(vec![task.clone(), second], "paimon", 42, &info, &[]).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups.values().next().unwrap().splits.len(), 2);
        assert!(
            group_lake_splits(vec![task.clone(), task.clone()], "paimon", 42, &info, &[]).is_err()
        );
        assert!(group_lake_splits(vec![task.clone()], "iceberg", 42, &info, &[]).is_err());
        assert!(group_lake_splits(vec![task.clone()], "paimon", 43, &info, &[]).is_err());
        let mut invalid = task.clone();
        invalid.bucket_id = info.num_buckets;
        assert!(group_lake_splits(vec![invalid], "paimon", 42, &info, &[]).is_err());
        let mut invalid = task;
        invalid.partition =
            crate::FlussLakePartitionIdentity::KeyValues(vec![("unknown".into(), "v".into())]);
        assert!(group_lake_splits(vec![invalid], "paimon", 42, &info, &[]).is_err());
    }

    #[test]
    fn bucket_unaware_tasks_are_append_only() {
        let info = table_info(false, vec![], HashMap::new());
        let mut task = crate::source::testing_split();
        task.bucket_id = -1;
        task.bucket_count = -1;
        validate_lake_tasks(&[task.clone()], "paimon", 42, &info).unwrap();
        assert!(group_lake_splits(vec![task.clone()], "paimon", 42, &info, &[]).is_err());
        assert!(validate_lake_tasks(&[task.clone(), task.clone()], "paimon", 42, &info).is_err());
        for count in [0, 4] {
            task.bucket_count = count;
            assert!(validate_lake_tasks(&[task.clone()], "paimon", 42, &info).is_err());
        }
    }

    #[test]
    fn lake_layout_uses_actual_live_and_expired_partition_counts() {
        let info = pk_table_info(vec!["region".into()], HashMap::new());
        let identity = |value: &str| {
            crate::FlussLakePartitionIdentity::KeyValues(vec![("region".into(), value.into())])
        };
        // The table default is 4, but this old live partition retains 8 buckets.
        let ranges = (0..8)
            .map(|bucket| crate::FlussLakeLogRange {
                baseline: crate::FlussLakeBaseline::Lake,
                table_bucket: fluss::metadata::TableBucket::new_with_partition(7, Some(9), bucket),
                partition: FlussLakeLogPartition::Ordinary(identity("old")),
                bucket_count: 8,
                start_offset: 0,
                stop_offset: 1,
                earliest_offset: 0,
            })
            .collect::<Vec<_>>();
        let mut live = crate::source::testing_split();
        live.partition = identity("old");
        live.bucket_count = 8;
        live.bucket_id = 7;
        let mut expired = live.clone();
        expired.partition = identity("expired");
        let groups = group_lake_splits(
            vec![live.clone(), expired.clone()],
            "paimon",
            42,
            &info,
            &ranges,
        )
        .unwrap();
        assert_eq!(groups.len(), 2);
        // Neither a valid id nor a query filter excuses a wrong live layout.
        let mut wrong = live;
        wrong.bucket_count = 4;
        wrong.bucket_id = 0;
        assert!(group_lake_splits(vec![wrong], "paimon", 42, &info, &ranges).is_err());
        let mut conflicting = expired.clone();
        conflicting.bucket_count = 4;
        conflicting.bucket_id = 0;
        assert!(
            group_lake_splits(vec![expired, conflicting], "paimon", 42, &info, &ranges).is_err()
        );
    }

    fn pk_table_info(
        partition_keys: Vec<String>,
        properties: HashMap<String, String>,
    ) -> Arc<TableInfo> {
        table_info(true, partition_keys, properties)
    }

    fn table_info(
        primary_key: bool,
        partition_keys: Vec<String>,
        mut properties: HashMap<String, String>,
    ) -> Arc<TableInfo> {
        properties
            .entry("table.datalake.enabled".into())
            .or_insert_with(|| "true".into());
        properties
            .entry("table.datalake.format".into())
            .or_insert_with(|| "paimon".into());
        let builder = Schema::builder()
            .column("id", DataTypes::int())
            .column("region", DataTypes::string())
            .column("amount", DataTypes::bigint());
        let schema = if primary_key {
            builder.primary_key(["id", "region"]).unwrap()
        } else {
            builder
        }
        .build()
        .unwrap();
        Arc::new(TableInfo::new(
            TablePath::new("fluss", "pk_orders"),
            7,
            1,
            schema,
            vec!["id".to_string()],
            partition_keys.into(),
            4,
            properties,
            HashMap::new(),
            None,
            0,
            0,
        ))
    }

    #[test]
    fn bucket_scope_limits_lake_tasks_and_rejects_unaligned_layouts() {
        let info = table_info(false, vec!["region".into()], HashMap::new());
        let partition = FlussLakePartitionIdentity::KeyValues(vec![("region".into(), "US".into())]);
        let context = FlussLakeReadContext::from_bucket_boundary(
            &info,
            Some(42),
            FlussLakeLogRange::new(
                TableBucket::new_with_partition(7, Some(9), 1),
                FlussLakeLogPartition::Ordinary(partition.clone()),
                4,
                Some(12),
                20,
                8,
            ),
        )
        .unwrap();
        let tasks = (0..4)
            .map(|bucket| crate::LakeSplit {
                bucket_id: bucket,
                partition: partition.clone(),
                ..crate::source::testing_split()
            })
            .collect::<Vec<_>>();
        let mut input = tasks.clone();
        input.push(crate::LakeSplit {
            partition: FlussLakePartitionIdentity::KeyValues(vec![(
                "region".into(),
                "expired".into(),
            )]),
            ..tasks[1].clone()
        });
        assert_eq!(
            select_context_tasks(&context, input).unwrap(),
            vec![tasks[1].clone()]
        );
        for (bucket_id, bucket_count) in [(-1, -1), (1, 2)] {
            let task = crate::LakeSplit {
                bucket_id,
                bucket_count,
                ..tasks[1].clone()
            };
            assert!(select_context_tasks(&context, vec![task]).is_err());
        }
    }

    #[test]
    fn unsupported_merge_engine_is_rejected() {
        let info = pk_table_info(
            Vec::new(),
            HashMap::from([("table.merge-engine".into(), "first_row".into())]),
        );
        assert!(matches!(
            validate_pk_union_merge_engine(&info),
            Err(FlussLakeError::UnsupportedMergeEngine(_))
        ));
    }

    #[test]
    fn lake_only_split_ids_do_not_collide_on_partition_delimiters() {
        let mut info = crate::split::testing_table(TablePath::new("fluss", "orders"), 3, true, 5);
        info.partition_keys = vec!["a".into(), "b".into()].into();
        let info = Arc::new(info);
        let split = |a: &str, b: &str| {
            let range = FlussLakeLogRange::lake_only(
                5,
                0,
                4,
                FlussLakePartitionIdentity::KeyValues(vec![
                    ("a".into(), a.into()),
                    ("b".into(), b.into()),
                ]),
            );
            let FlussLakeLogPartition::Ordinary(identity) = range.partition();
            create_logical_split(
                &info,
                &range,
                Some(42),
                vec![crate::LakeSplit {
                    partition: identity.clone(),
                    ..crate::source::testing_split()
                }],
                Vec::new(),
                SplitStatistics::default(),
            )
            .unwrap()
        };
        // Both used to produce "a=x/b=y/b=z" with delimiter concatenation.
        assert_ne!(
            split("x/b=y", "z").split_id(),
            split("x", "y/b=z").split_id()
        );
    }
}
