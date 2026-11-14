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

//! Freezes baseline references and server-issued log ranges within a partition scope.
//!
//! This module does not open a lake catalog, generate splits or schedule reads.

use crate::error::planning_client_error;
use crate::partition::PartitionScope;
use crate::{
    FlussLakeError, FlussLakeLogRange, FlussLakePartitionIdentity, FlussLakeReadContext, Result,
};
use fluss::client::FlussAdmin;
use fluss::metadata::{
    KvSnapshot, LakeSnapshotInfo, LatestKvSnapshots, PartitionInfo, TableBucket, TableInfo,
    TablePath,
};
use fluss::predicate::Predicate;
use fluss::rpc::message::OffsetSpec;
use futures::StreamExt;
use std::collections::{HashMap, HashSet};

/// Freezes live boundaries after applying the requested partition predicate.
pub(crate) async fn freeze_read_context(
    admin: &FlussAdmin,
    table_info: &TableInfo,
    partition_scope: Option<&Predicate>,
) -> Result<FlussLakeReadContext> {
    let scope = PartitionScope::bind(
        partition_scope,
        table_info.row_type(),
        &table_info.partition_keys,
    )?;
    let listing_spec = scope.listing_spec();
    let table_path = &table_info.table_path;
    if table_info.num_buckets <= 0 {
        return Err(FlussLakeError::PlanningFailed(format!(
            "table {table_path} has invalid bucket count {}",
            table_info.num_buckets
        )));
    }

    let readable_snapshot = load_readable_snapshot(admin, table_info).await?;

    let snapshot_offsets =
        collect_snapshot_offsets(table_info.table_id, readable_snapshot.as_ref())?;
    let partitions = if table_info.partition_keys.is_empty() {
        vec![(
            None,
            None,
            FlussLakePartitionIdentity::Unpartitioned,
            table_info.num_buckets,
        )]
    } else {
        let mut partition_infos = admin
            .list_partition_infos_with_spec(table_path, Some(&listing_spec))
            .await
            .map_err(|error| planning_client_error("list table partitions", error))?;
        partition_infos.sort_by(|left, right| {
            left.get_partition_name()
                .cmp(&right.get_partition_name())
                .then_with(|| left.get_partition_id().cmp(&right.get_partition_id()))
        });
        partition_infos
            .into_iter()
            .map(|partition| {
                let count = partition_bucket_count(&partition)?;
                let (id, name, identity) = partition_identity(partition);
                Ok(scope
                    .matches(&identity)?
                    .then_some((id, name, identity, count)))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect()
    };

    // Bound metadata fan-out and preserve the canonical partition order.
    let captured =
        futures::stream::iter(partitions.into_iter().map(|(id, name, identity, count)| {
            let snapshot_offsets = &snapshot_offsets;
            async move {
                let result = async {
                    let bucket_ids: Vec<i32> = (0..count).collect();
                    let needs_kv = table_info.has_primary_key()
                        && bucket_ids.iter().any(|bucket_id| {
                            !snapshot_offsets.contains_key(&TableBucket::new_with_partition(
                                table_info.table_id,
                                id,
                                *bucket_id,
                            ))
                        });
                    // Select the snapshot and its seam before querying stop offsets. Looking
                    // up a newer snapshot afterward could put the baseline beyond the stop.
                    let kv_snapshots = if needs_kv {
                        let snapshots = admin
                            .get_latest_kv_snapshots(table_path, name.as_deref())
                            .await
                            .map_err(|error| {
                                planning_client_error("get latest KV snapshots", error)
                            })?;
                        collect_kv_snapshots(snapshots, table_info.table_id, id, count)?
                    } else {
                        HashMap::new()
                    };
                    let (earliest_offsets, latest_offsets) =
                        load_server_offsets(admin, table_path, name.as_deref(), &bucket_ids)
                            .await?;
                    (0..count)
                        .map(|bucket_id| {
                            let bucket =
                                TableBucket::new_with_partition(table_info.table_id, id, bucket_id);
                            let earliest = required_offset(&earliest_offsets, &bucket, "earliest")?;
                            let stop = required_offset(&latest_offsets, &bucket, "latest")?;
                            let seam = snapshot_offsets.get(&bucket).copied();
                            if seam.is_none()
                                && let Some(snapshot) = kv_snapshots.get(&bucket_id)
                            {
                                return FlussLakeLogRange::from_kv_snapshot(
                                    bucket,
                                    crate::FlussLakeLogPartition::Ordinary(identity.clone()),
                                    count,
                                    snapshot,
                                    stop,
                                    earliest,
                                );
                            }
                            freeze_bucket_range(
                                bucket,
                                identity.clone(),
                                count,
                                seam,
                                earliest,
                                stop,
                            )
                        })
                        .collect::<Result<Vec<_>>>()
                }
                .await;
                (id, result)
            }
        }))
        .buffered(8)
        .collect::<Vec<_>>()
        .await;

    let bucket_ranges = if table_info.partition_keys.is_empty() {
        captured
            .into_iter()
            .map(|(_, result)| result)
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect()
    } else {
        // Offset lookup resolves partition names internally. Recheck IDs even
        // after successful lookups: a name may now refer to a new partition.
        // One recheck covers all captures, including failures of any kind.
        let live_ids = admin
            .list_partition_infos_with_spec(table_path, Some(&listing_spec))
            .await
            .map(|partitions| {
                partitions
                    .into_iter()
                    .map(|p| p.get_partition_id())
                    .collect()
            })
            .map_err(|error| planning_client_error("recheck table partitions", error));
        retain_live_ranges(captured, live_ids)?
    };

    let context = FlussLakeReadContext::from_boundary_with_partition_scope(
        table_info,
        readable_snapshot.map(|snapshot| snapshot.snapshot_id),
        bucket_ranges,
        partition_scope,
    )?;
    validate_source_identity(admin, table_info, &context).await?;
    Ok(context)
}

pub(crate) async fn freeze_lake_read_context(
    admin: &FlussAdmin,
    table_info: &TableInfo,
    scope: Option<&Predicate>,
) -> Result<FlussLakeReadContext> {
    if !table_info
        .table_config
        .is_datalake_enabled()
        .map_err(|error| planning_client_error("check lake configuration", error))?
    {
        return Err(FlussLakeError::NotLakeReadable(
            "lake-only read requires a lake-enabled table".into(),
        ));
    }
    PartitionScope::bind(scope, table_info.row_type(), &table_info.partition_keys)?;
    let snapshot = load_readable_snapshot(admin, table_info).await?;
    let context = FlussLakeReadContext::from_lake_snapshot(
        table_info,
        snapshot.map(|snapshot| snapshot.snapshot_id),
        scope,
    )?;
    validate_source_identity(admin, table_info, &context).await?;
    Ok(context)
}

async fn load_readable_snapshot(
    admin: &FlussAdmin,
    table_info: &TableInfo,
) -> Result<Option<LakeSnapshotInfo>> {
    let table_path = &table_info.table_path;
    let snapshot = if table_info
        .table_config
        .is_datalake_enabled()
        .map_err(|error| FlussLakeError::PlanningFailed(error.to_string()))?
    {
        match admin.get_lake_snapshot(table_path, None, Some(true)).await {
            Ok(snapshot) => Some(snapshot),
            Err(error)
                if error.api_error() == Some(fluss::error::FlussError::LakeSnapshotNotExist) =>
            {
                None
            }
            Err(error) => {
                return Err(planning_client_error("get readable lake snapshot", error));
            }
        }
    } else {
        None
    };

    if let Some(snapshot) = &snapshot
        && snapshot.table_id != table_info.table_id
    {
        return Err(FlussLakeError::PlanningFailed(format!(
            "readable snapshot {} belongs to table id {}, but {table_path} resolved to table id {}",
            snapshot.snapshot_id, snapshot.table_id, table_info.table_id
        )));
    }
    Ok(snapshot)
}

async fn validate_source_identity(
    admin: &FlussAdmin,
    table_info: &TableInfo,
    context: &FlussLakeReadContext,
) -> Result<()> {
    // Name-based metadata calls may cross a table drop/recreate. Recheck the
    // source identity without replacing the captured schema or boundaries.
    let live_table = admin
        .get_table_info(&table_info.table_path)
        .await
        .map_err(|error| planning_client_error("recheck source table identity", error))?;
    context.validate_table(&live_table)
}

fn retain_live_ranges(
    captured: Vec<(Option<i64>, Result<Vec<FlussLakeLogRange>>)>,
    live_ids: Result<HashSet<i64>>,
) -> Result<Vec<FlussLakeLogRange>> {
    let live_ids = match live_ids {
        Ok(ids) => ids,
        Err(recheck_error) => {
            // A failed recheck does not prove deletion. Preserve the lookup
            // error when there was one, rather than hiding it with a new error.
            return Err(captured
                .into_iter()
                .find_map(|(_, result)| result.err())
                .unwrap_or(recheck_error));
        }
    };
    captured
        .into_iter()
        .filter(|(id, _)| id.is_some_and(|id| live_ids.contains(&id)))
        .map(|(_, result)| result)
        .collect::<Result<Vec<_>>>()
        .map(|ranges| ranges.into_iter().flatten().collect())
}

/// Admin resolves legacy counts; never substitute the table default here.
fn partition_bucket_count(partition: &PartitionInfo) -> Result<i32> {
    let count = partition.get_bucket_count();
    if count <= 0 {
        return Err(FlussLakeError::PlanningFailed(format!(
            "partition {} has invalid bucket count {count}",
            partition.get_partition_id()
        )));
    }
    Ok(count)
}

fn partition_identity(
    partition_info: PartitionInfo,
) -> (Option<i64>, Option<String>, FlussLakePartitionIdentity) {
    let resolved = partition_info.get_resolved_partition_spec();
    let key_values = resolved
        .get_partition_keys()
        .iter()
        .cloned()
        .zip(resolved.get_partition_values().iter().cloned())
        .collect();
    (
        Some(partition_info.get_partition_id()),
        Some(partition_info.get_partition_name()),
        FlussLakePartitionIdentity::KeyValues(key_values),
    )
}

fn collect_snapshot_offsets(
    table_id: i64,
    readable_snapshot: Option<&LakeSnapshotInfo>,
) -> Result<HashMap<TableBucket, i64>> {
    let Some(snapshot) = readable_snapshot else {
        return Ok(HashMap::new());
    };
    let mut offsets = HashMap::new();
    for bucket_snapshot in &snapshot.bucket_snapshots {
        let Some(offset) = bucket_snapshot.log_offset else {
            continue;
        };
        if offset < 0 {
            return Err(FlussLakeError::PlanningFailed(format!(
                "readable snapshot {} returned negative log offset {offset} for bucket {}",
                snapshot.snapshot_id, bucket_snapshot.bucket_id
            )));
        }
        let table_bucket = TableBucket::new_with_partition(
            table_id,
            bucket_snapshot.partition_id,
            bucket_snapshot.bucket_id,
        );
        if offsets.insert(table_bucket.clone(), offset).is_some() {
            return Err(FlussLakeError::PlanningFailed(format!(
                "readable snapshot {} contains duplicate boundary for {table_bucket}",
                snapshot.snapshot_id
            )));
        }
    }
    Ok(offsets)
}

fn collect_kv_snapshots(
    snapshots: LatestKvSnapshots,
    table_id: i64,
    partition_id: Option<i64>,
    bucket_count: i32,
) -> Result<HashMap<i32, KvSnapshot>> {
    if snapshots.table_id != table_id || snapshots.partition_id != partition_id {
        return Err(FlussLakeError::DataUnavailable(
            "table or partition was replaced while selecting KV snapshots".into(),
        ));
    }
    let mut selected = HashMap::new();
    for snapshot in snapshots.latest_snapshots {
        match (snapshot.snapshot_id, snapshot.log_offset) {
            (None, None) => continue,
            (Some(id), Some(offset)) if id >= 0 && offset >= 0 => {}
            _ => {
                return Err(FlussLakeError::PlanningFailed(
                    "KV snapshot has no valid ID and matching log seam".into(),
                ));
            }
        }
        let bucket_id = snapshot.bucket_id;
        if bucket_id < 0
            || bucket_id >= bucket_count
            || selected.insert(bucket_id, snapshot).is_some()
        {
            return Err(FlussLakeError::PlanningFailed(
                "KV snapshots contain an invalid or duplicate bucket".into(),
            ));
        }
    }
    Ok(selected)
}

async fn load_server_offsets(
    admin: &FlussAdmin,
    table_path: &TablePath,
    partition_name: Option<&str>,
    bucket_ids: &[i32],
) -> Result<(HashMap<i32, i64>, HashMap<i32, i64>)> {
    let earliest = list_server_offsets(
        admin,
        table_path,
        partition_name,
        bucket_ids,
        OffsetSpec::Earliest,
    );
    let latest = list_server_offsets(
        admin,
        table_path,
        partition_name,
        bucket_ids,
        OffsetSpec::Latest,
    );
    futures::try_join!(earliest, latest)
}

async fn list_server_offsets(
    admin: &FlussAdmin,
    table_path: &TablePath,
    partition_name: Option<&str>,
    bucket_ids: &[i32],
    offset_spec: OffsetSpec,
) -> Result<HashMap<i32, i64>> {
    let offset_name = match offset_spec {
        OffsetSpec::Earliest => "earliest",
        OffsetSpec::Latest => "latest",
        OffsetSpec::Timestamp(_) => "timestamp",
    };
    let result = match partition_name {
        Some(partition_name) => {
            admin
                .list_partition_offsets(table_path, partition_name, bucket_ids, offset_spec)
                .await
        }
        None => {
            admin
                .list_offsets(table_path, bucket_ids, offset_spec)
                .await
        }
    };
    result.map_err(|error| {
        planning_client_error(&format!("get server-issued {offset_name} offsets"), error)
    })
}

fn required_offset(
    offsets: &HashMap<i32, i64>,
    table_bucket: &TableBucket,
    offset_name: &str,
) -> Result<i64> {
    offsets
        .get(&table_bucket.bucket_id())
        .copied()
        .ok_or_else(|| {
            FlussLakeError::PlanningFailed(format!(
                "server did not return the {offset_name} offset for {table_bucket}"
            ))
        })
}

fn freeze_bucket_range(
    table_bucket: TableBucket,
    partition_identity: FlussLakePartitionIdentity,
    bucket_count: i32,
    snapshot_offset: Option<i64>,
    earliest_offset: i64,
    stop_offset: i64,
) -> Result<FlussLakeLogRange> {
    if earliest_offset < 0 || stop_offset < 0 {
        return Err(FlussLakeError::PlanningFailed(format!(
            "server returned a negative log boundary [{earliest_offset}, {stop_offset}) for {table_bucket}"
        )));
    }

    if earliest_offset > stop_offset {
        return Err(FlussLakeError::PlanningFailed(format!(
            "server earliest offset {earliest_offset} exceeds latest offset {stop_offset} for {table_bucket}"
        )));
    }
    // Check retention availability after pruning: lake-only and pruned reads
    // do not require the log tail.
    let start_offset = snapshot_offset.unwrap_or(earliest_offset);
    if start_offset > stop_offset {
        return Err(FlussLakeError::PlanningFailed(format!(
            "read start offset {start_offset} exceeds server latest offset {stop_offset} for {table_bucket}"
        )));
    }

    Ok(FlussLakeLogRange::new(
        table_bucket,
        crate::FlussLakeLogPartition::Ordinary(partition_identity),
        bucket_count,
        snapshot_offset,
        stop_offset,
        earliest_offset,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kv_selection_requires_matching_identity_and_seam() {
        let snapshots = LatestKvSnapshots {
            table_id: 5,
            partition_id: Some(9),
            latest_snapshots: vec![
                KvSnapshot {
                    bucket_id: 0,
                    snapshot_id: Some(3),
                    log_offset: Some(12),
                },
                KvSnapshot {
                    bucket_id: 1,
                    snapshot_id: None,
                    log_offset: None,
                },
            ],
        };
        let selected = collect_kv_snapshots(snapshots.clone(), 5, Some(9), 2).unwrap();
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[&0].log_offset, Some(12));
        assert!(collect_kv_snapshots(snapshots.clone(), 5, Some(10), 2).is_err());
        let mut incomplete = snapshots;
        incomplete.latest_snapshots[0].log_offset = None;
        assert!(collect_kv_snapshots(incomplete, 5, Some(9), 2).is_err());
    }

    #[test]
    fn partition_recheck_discards_only_confirmed_deleted_ids() {
        let error = || FlussLakeError::PlanningFailed("original offset failure".into());
        let captured = || {
            vec![
                (Some(9), Err(error())),
                (Some(10), Ok(vec![root_range(Some(12), 8, 20).unwrap()])),
            ]
        };
        // ID 11 may be a replacement with the same name as ID 9.
        let ranges = retain_live_ranges(captured(), Ok(HashSet::from([10, 11]))).unwrap();
        assert_eq!(ranges.len(), 1);
        assert_eq!(
            retain_live_ranges(captured(), Ok(HashSet::from([9, 10])))
                .unwrap_err()
                .to_string(),
            error().to_string()
        );
        assert_eq!(
            retain_live_ranges(
                captured(),
                Err(FlussLakeError::PlanningFailed("recheck failed".into()))
            )
            .unwrap_err()
            .to_string(),
            error().to_string()
        );
        // Successful name-based offsets are unsafe too when the original ID disappeared.
        let successful = vec![(Some(9), Ok(vec![root_range(Some(12), 8, 20).unwrap()]))];
        assert!(
            retain_live_ranges(successful, Ok(HashSet::from([11])))
                .unwrap()
                .is_empty()
        );
        assert!(retain_live_ranges(vec![], Err(error())).is_err());
    }

    #[test]
    fn uses_resolved_partition_bucket_counts_and_rejects_missing_counts() {
        use fluss::metadata::ResolvedPartitionSpec;
        use std::sync::Arc;

        let spec =
            ResolvedPartitionSpec::new(Arc::from(["region".to_string()]), vec!["US".to_string()])
                .unwrap();
        let partition = PartitionInfo::new(42, spec);
        assert!(partition_bucket_count(&partition).is_err());
        for count in [4, 0, -1] {
            let result = partition_bucket_count(&partition.clone().with_bucket_count(count));
            assert_eq!(result.is_ok(), count > 0);
            if count > 0 {
                assert_eq!(result.unwrap(), count);
            }
        }
    }

    fn root_range(
        snapshot_offset: Option<i64>,
        earliest_offset: i64,
        stop_offset: i64,
    ) -> Result<FlussLakeLogRange> {
        freeze_bucket_range(
            TableBucket::new(5, 2),
            FlussLakePartitionIdentity::Unpartitioned,
            4,
            snapshot_offset,
            earliest_offset,
            stop_offset,
        )
    }

    #[test]
    fn freezes_lake_seam_or_earliest_with_server_stop() {
        use crate::FlussLakeBaseline::{Lake, None as NoBaseline};

        for (seam, start, baseline) in [
            (Some(12), 12, Lake),
            (Some(7), 7, Lake),
            (None, 8, NoBaseline),
            (Some(20), 20, Lake),
        ] {
            let range = root_range(seam, 8, 20).unwrap();
            assert_eq!(range.baseline(), baseline);
            assert_eq!(range.start_offset(), start);
            assert_eq!(range.stop_offset(), 20);
            assert_eq!(range.is_empty(), start == 20);
            assert_eq!(range.validate_available().is_ok(), start >= 8);
        }
    }

    #[test]
    fn rejects_start_after_server_latest() {
        let result = root_range(Some(21), 8, 20);

        assert!(matches!(result, Err(FlussLakeError::PlanningFailed(_))));
    }
}
