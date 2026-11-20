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

#![cfg(feature = "integration_tests")]

mod support;

use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use fluss::metadata::{
    AddColumn, AlterTableChanges, ColumnPositionType, DataTypes, JsonSerde, Schema, TableBucket,
    TableDescriptor, TableInfo, TablePath,
};
use fluss::predicate::col;
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
use fluss_lake::{
    FlussLakeError, FlussLakeLogPartition, FlussLakeLogRange, FlussLakePartitionIdentity,
    FlussLakeReadContext, FlussLakeTable, LakePlannerContext, LakeReaderContext, LakeSource,
    LakeSplit, RecordBatchStream,
};
use futures::TryStreamExt;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::time::Duration;

fn append_schema() -> Schema {
    Schema::builder()
        .column("id", DataTypes::int())
        .column("name", DataTypes::string())
        .build()
        .unwrap()
}

fn pk_schema() -> Schema {
    Schema::builder()
        .column("id", DataTypes::int())
        .column("name", DataTypes::string())
        .primary_key(vec!["id"])
        .unwrap()
        .build()
        .unwrap()
}

fn sorted_ids(batches: &[RecordBatch]) -> Vec<i32> {
    let mut ids: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect();
    ids.sort_unstable();
    ids
}

fn append_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("name", Arc::new(StringArray::from(names)) as ArrayRef),
    ])
    .expect("Failed to build append record batch")
}

#[tokio::test]
async fn append_log_plan_uses_frozen_stop_offset_after_planning() {
    let connection = Arc::new(support::get_shared_cluster().get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "test_union_read_frozen_append_log");
    let descriptor = TableDescriptor::builder()
        .schema(append_schema())
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    let table = connection.get_table(&path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    writer
        .append_arrow_batch(append_batch(vec![1, 2, 3], vec!["one", "two", "three"]))
        .unwrap();
    writer.flush().await.unwrap();

    let lake_table = FlussLakeTable::new(connection.clone(), &path);
    let scan = lake_table.new_scan().with_projection(vec![0]);
    let plan = scan.plan().await.unwrap();
    let context = plan.read_context().clone();
    drop(plan);

    writer
        .append_arrow_batch(append_batch(vec![4, 5], vec!["four", "five"]))
        .unwrap();
    writer.flush().await.unwrap();

    let splits = scan
        .plan_with_context(&context)
        .await
        .unwrap()
        .splits()
        .to_vec();
    let batches = tokio::time::timeout(
        Duration::from_secs(10),
        scan.new_reader()
            .read_splits(&splits)
            .unwrap()
            .try_collect::<Vec<_>>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(sorted_ids(&batches), [1, 2, 3]);
    // A lake-only reader must reject the log range instead of returning no rows.
    assert!(matches!(
        lake_table
            .new_scan()
            .with_lake_only(true)
            .new_reader()
            .read_split(&splits[0]),
        Err(FlussLakeError::PlanningFailed(_))
    ));
    // Neither prefix is pushed into Fluss log statistics. The task must still
    // distinguish their exact row filters when the worker rebuilds the scan.
    let filtered = scan.clone().with_filter(col("name").starts_with("t"));
    let plan = filtered.plan_with_context(&context).await.unwrap();
    let task = &plan.splits()[0];
    let wire = context.to_protobuf().unwrap();
    let descriptors = std::slice::from_ref(task.descriptor());
    let imported = filtered.import_splits(&wire, descriptors).await.unwrap();
    assert_eq!(
        sorted_ids(
            &filtered
                .new_reader()
                .read_splits(&imported)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
        ),
        [2, 3]
    );
    assert!(matches!(
        scan.new_reader().read_split(task),
        Err(FlussLakeError::InvalidReadContext(_))
    ));
    let wrong = scan.clone().with_filter(col("name").starts_with("o"));
    assert!(matches!(
        wrong.import_splits(&wire, descriptors).await,
        Err(FlussLakeError::InvalidReadContext(_))
    ));
    drop(writer);
    admin.drop_table(&path, false).await.unwrap();
    assert!(matches!(
        scan.plan_with_context(&context).await,
        Err(FlussLakeError::DataUnavailable(_))
    ));
}

#[tokio::test]
async fn frozen_schema_survives_alter_table_after_planning() {
    let cluster = support::get_shared_cluster();
    let connection = Arc::new(cluster.get_fluss_connection().await);
    let admin = connection.get_admin().expect("Failed to get Fluss admin");
    let table_path = TablePath::new("fluss", "test_union_read_stale_schema");
    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()
                .expect("Failed to build UnionRead integration test schema"),
        )
        .property("table.datalake.enabled", "true")
        .property("table.datalake.format", "paimon")
        .build()
        .expect("Failed to build UnionRead integration test table descriptor");
    support::create_table(&admin, &table_path, &table_descriptor).await;

    let table = connection
        .get_table(&table_path)
        .await
        .expect("Failed to open UnionRead integration test table");
    let frozen_info = table.get_table_info().clone();
    let writer = table
        .new_append()
        .expect("Failed to create append operation")
        .create_writer()
        .expect("Failed to create append writer");
    writer
        .append_arrow_batch(append_batch(vec![1, 2], vec!["a", "b"]))
        .expect("Failed to append pre-plan batch");
    writer
        .flush()
        .await
        .expect("Failed to flush pre-plan batch");

    let lake_table = FlussLakeTable::new(connection.clone(), &table_path);
    let scan = lake_table.new_scan();
    let plan = scan
        .plan()
        .await
        .expect("Failed to plan append-log UnionRead");
    assert_eq!(plan.splits().len(), 1);
    let stale_split = plan.splits()[0].clone();

    let age_type_json = serde_json::to_vec(
        &DataTypes::int()
            .serialize_json()
            .expect("Failed to serialize INT type"),
    )
    .expect("Failed to encode INT type json");
    admin
        .alter_table(
            &table_path,
            false,
            AlterTableChanges {
                add_columns: vec![AddColumn {
                    column_name: "age".to_string(),
                    data_type_json: age_type_json,
                    comment: None,
                    position: ColumnPositionType::Last,
                }],
                ..Default::default()
            },
        )
        .await
        .expect("Failed to alter UnionRead integration test table");

    let replanned = scan.plan_with_context(plan.read_context()).await.unwrap();
    assert_eq!(replanned.schema(), plan.schema());
    let retained = stale_split.clone();
    let expected_schema = plan.schema().clone();
    drop(plan);
    drop(scan);
    let reopened_table = FlussLakeTable::new(connection.clone(), &table_path);
    let batches = tokio::time::timeout(Duration::from_secs(10), async {
        reopened_table
            .new_scan()
            .new_reader()
            .read_split(&retained)?
            .try_collect::<Vec<_>>()
            .await
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(
        batches
            .iter()
            .all(|batch| batch.schema() == expected_schema)
    );

    // The schema may also change during capture: old schema, newer stop.
    let live_table = connection.get_table(&table_path).await.unwrap();
    let live_writer = live_table.new_append().unwrap().create_writer().unwrap();
    live_writer
        .append_arrow_batch(
            RecordBatch::try_from_iter(vec![
                ("id", Arc::new(Int32Array::from(vec![3])) as ArrayRef),
                ("name", Arc::new(StringArray::from(vec!["c"])) as ArrayRef),
                ("age", Arc::new(Int32Array::from(vec![30])) as ArrayRef),
            ])
            .unwrap(),
        )
        .unwrap();
    live_writer.flush().await.unwrap();
    let later = reopened_table.capture_read_context().await.unwrap();
    let mixed =
        FlussLakeReadContext::from_boundary(&frozen_info, None, later.log_ranges().to_vec())
            .unwrap();
    let reopened_scan = reopened_table.new_scan().with_filter(col("id").eq(3_i32));
    let mixed_plan = reopened_scan.plan_with_context(&mixed).await.unwrap();
    let batches: Vec<_> = reopened_scan
        .new_reader()
        .read_splits(mixed_plan.splits())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    assert_eq!(batches[0].schema(), expected_schema);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0),
        3
    );
    assert_eq!(
        batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "c"
    );
    drop(live_writer);
    drop(writer);
    admin
        .drop_table(&table_path, false)
        .await
        .expect("Failed to drop UnionRead integration test table");
}

#[tokio::test]
async fn shared_read_metadata_does_not_outlive_the_read_attempt() {
    let cluster = support::get_shared_cluster();
    let connection = Arc::new(cluster.get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "test_union_read_metadata_session");
    let descriptor = TableDescriptor::builder()
        .schema(append_schema())
        .distributed_by(Some(2), vec!["id".into()])
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    let lake_table = FlussLakeTable::new(connection.clone(), &path);
    let scan = lake_table.new_scan();
    let mut previous_splits = None;

    for generation in 0..2 {
        let reader = scan.new_reader();
        if generation > 0 {
            admin.drop_table(&path, false).await.unwrap();
            support::create_table(&admin, &path, &descriptor).await;
            // A fresh attempt must reject tasks for the dropped table.
            let result = reader
                .read_splits(previous_splits.as_deref().unwrap())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await;
            assert!(matches!(result, Err(FlussLakeError::DataUnavailable(_))));
        }
        let table = connection.get_table(&path).await.unwrap();
        let writer = table.new_append().unwrap().create_writer().unwrap();
        let expected: Vec<i32> = (generation * 100..generation * 100 + 16).collect();
        writer
            .append_arrow_batch(append_batch(expected.clone(), vec!["value"; 16]))
            .unwrap();
        writer.flush().await.unwrap();
        drop(writer);
        let plan = scan.plan().await.unwrap();
        assert_eq!(plan.splits().len(), 2);
        let batches = tokio::time::timeout(
            Duration::from_secs(10),
            reader
                .read_splits(plan.splits())
                .unwrap()
                .try_collect::<Vec<_>>(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(sorted_ids(&batches), expected);
        previous_splits = Some(plan.splits().to_vec());
    }
    admin.drop_table(&path, false).await.unwrap();
}

fn lake_context(info: &TableInfo, seam: i64, stop: i64) -> FlussLakeReadContext {
    FlussLakeReadContext::from_boundary(
        info,
        Some(1),
        vec![FlussLakeLogRange::new(
            TableBucket::new(info.table_id, 0),
            FlussLakeLogPartition::Ordinary(FlussLakePartitionIdentity::Unpartitioned),
            1,
            Some(seam),
            stop,
            0,
        )],
    )
    .unwrap()
}

struct Baseline(RecordBatch);
impl LakeSource for Baseline {
    fn format(&self) -> &str {
        "paimon"
    }
    fn plan<'a>(
        &'a self,
        ctx: LakePlannerContext<'a>,
    ) -> BoxFuture<'a, fluss_lake::Result<Vec<LakeSplit>>> {
        Box::pin(async move {
            Ok(vec![LakeSplit {
                format: self.format().into(),
                snapshot_id: ctx.snapshot_id,
                partition: FlussLakePartitionIdentity::Unpartitioned,
                bucket_id: 0,
                bucket_count: 1,
                estimated_rows: None,
                estimated_size: None,
                payload_version: 1,
                payload: vec![1],
            }])
        })
    }
    fn read<'a>(
        &'a self,
        ctx: LakeReaderContext<'a>,
    ) -> BoxFuture<'a, fluss_lake::Result<RecordBatchStream>> {
        Box::pin(async move {
            let batch = self.0.project(ctx.projection).unwrap();
            Ok(Box::pin(futures::stream::iter([Ok(batch)])) as RecordBatchStream)
        })
    }
}

#[tokio::test]
async fn append_reads_lake_baseline_and_only_the_log_tail() {
    let connection = Arc::new(support::get_shared_cluster().get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "test_union_read_stub_lake_and_tail");
    let descriptor = TableDescriptor::builder()
        .schema(append_schema())
        .property("table.datalake.enabled", "true")
        .distributed_by(Some(1), vec!["id".into()])
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    let table = connection.get_table(&path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    writer
        .append_arrow_batch(append_batch(vec![1, 2], vec!["baseline", "tail"]))
        .unwrap();
    writer.flush().await.unwrap();
    let info = table.get_table_info();
    // Supply an engine-captured boundary: offset 0 is already in the lake.
    let context = lake_context(info, 1, 2);
    let scan = FlussLakeTable::new(connection.clone(), &path)
        .new_scan()
        .with_lake_source(Arc::new(Baseline(append_batch(vec![1], vec!["baseline"]))))
        .with_projection(vec![0]);
    let plan = scan.plan_with_context(&context).await.unwrap();
    let batches = scan
        .new_reader()
        .read_splits(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(sorted_ids(&batches), [1, 2]);
    admin.drop_table(&path, false).await.unwrap();
}

#[tokio::test]
async fn pk_lake_baseline_with_an_empty_tail_needs_no_overlay_budget() {
    let connection = Arc::new(support::get_shared_cluster().get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "union_read_pk_empty_tail");
    let descriptor = TableDescriptor::builder()
        .schema(pk_schema())
        .property("table.datalake.enabled", "true")
        .distributed_by(Some(1), vec!["id".into()])
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    let info = admin.get_table_info(&path).await.unwrap();
    let context = lake_context(&info, 0, 0);
    let scan = FlussLakeTable::new(connection, &path)
        .new_scan()
        .with_lake_source(Arc::new(Baseline(append_batch(vec![1], vec!["baseline"]))))
        .with_projection(vec![1]);
    let plan = scan.plan_with_context(&context).await.unwrap();
    let batches = scan
        .new_reader()
        .with_memory_budget(Arc::new(fluss::client::LimitedReadMemoryBudget::new(0)))
        .read_splits(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        batches,
        [RecordBatch::try_new(
            plan.schema(),
            vec![Arc::new(StringArray::from(vec!["baseline"]))]
        )
        .unwrap()]
    );
    admin.drop_table(&path, false).await.unwrap();
}

#[tokio::test]
async fn pk_lake_and_sparse_tail_filter_the_reconciled_rows() {
    let connection = Arc::new(support::get_shared_cluster().get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "union_read_pk_lake_and_sparse_tail");
    let descriptor = TableDescriptor::builder()
        .schema(pk_schema())
        .property("table.datalake.enabled", "true")
        .distributed_by(Some(1), vec!["id".into()])
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    let table = connection.get_table(&path).await.unwrap();
    let writer = table.new_upsert().unwrap().create_writer().unwrap();
    for row in [
        pk_row(1, "keep"),
        pk_row(2, "keep"),
        pk_row(3, "old"),
        pk_row(4, "keep"),
    ] {
        writer.upsert(&row).unwrap().await.unwrap();
    }
    let seam = admin
        .list_offsets(&path, &[0], OffsetSpec::Latest)
        .await
        .unwrap()[&0];
    writer.upsert(&pk_row(1, "old")).unwrap().await.unwrap();
    // Each missing-key delete is acknowledged separately so it forms a zero-row CDC batch.
    writer.delete(&pk_row(99, "")).unwrap().await.unwrap();
    writer.delete(&pk_row(2, "keep")).unwrap().await.unwrap();
    writer.upsert(&pk_row(3, "keep")).unwrap().await.unwrap();
    writer.delete(&pk_row(99, "")).unwrap().await.unwrap();
    let stop = admin
        .list_offsets(&path, &[0], OffsetSpec::Latest)
        .await
        .unwrap()[&0];
    let context = lake_context(table.get_table_info(), seam, stop);
    let baseline = Baseline(append_batch(
        vec![1, 2, 3, 4],
        vec!["keep", "keep", "old", "keep"],
    ));
    let scan = FlussLakeTable::new(connection.clone(), &path)
        .new_scan()
        .with_lake_source(Arc::new(baseline))
        .with_filter(col("name").eq("keep"))
        .with_projection(vec![0]);
    let plan = scan.plan_with_context(&context).await.unwrap();
    let rows = scan.new_reader().read_splits(plan.splits()).unwrap();
    let batches = tokio::time::timeout(Duration::from_secs(30), rows.try_collect::<Vec<_>>())
        .await
        .expect("PK read did not finish after consuming the final empty batch")
        .unwrap();
    assert_eq!(sorted_ids(&batches), [3, 4]);
    admin.drop_table(&path, false).await.unwrap();
}

#[tokio::test]
async fn dropped_partition_fails_a_frozen_log_read() {
    use fluss::metadata::PartitionSpec;
    use std::collections::HashMap;

    let connection = Arc::new(support::get_shared_cluster().get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "test_union_read_expired_partition");
    let descriptor = TableDescriptor::builder()
        .schema(append_schema())
        .partitioned_by(vec!["name"])
        .distributed_by(Some(1), vec!["id".into()])
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    let partition = PartitionSpec::new(HashMap::from([("name", "expired")]));
    admin
        .create_partition(&path, &partition, false)
        .await
        .unwrap();
    let table = connection.get_table(&path).await.unwrap();
    let writer = table.new_append().unwrap().create_writer().unwrap();
    writer
        .append_arrow_batch(append_batch(vec![1], vec!["expired"]))
        .unwrap();
    writer.flush().await.unwrap();
    let scan = FlussLakeTable::new(connection.clone(), &path).new_scan();
    let plan = scan.plan().await.unwrap();
    // Keep a core reader subscribed across deletion; checking only when a split
    // opens would miss a partition disappearing after its first batch.
    let scanner = table.new_scan().create_record_batch_log_scanner().unwrap();
    let mut active = fluss::client::RecordBatchLogReader::new_from_ranges(
        scanner,
        vec![fluss::client::BoundedLogReadRange {
            bucket: plan.read_context().log_ranges()[0].table_bucket().clone(),
            starting_offset: 0,
            stopping_offset: 2,
        }],
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(15), active.next_batch())
            .await
            .unwrap()
            .unwrap()
            .is_some()
    );
    admin
        .drop_partition(&path, &partition, false)
        .await
        .unwrap();
    let error = tokio::time::timeout(Duration::from_secs(15), active.next_batch())
        .await
        .expect("bounded reader must observe deletion")
        .unwrap_err();
    assert_eq!(
        error.api_error(),
        Some(fluss::error::FlussError::PartitionNotExists)
    );
    let stream = scan.new_reader().read_splits(plan.splits()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(15), stream.try_collect::<Vec<_>>())
        .await
        .expect("bounded read must terminate");
    assert!(
        matches!(result, Err(FlussLakeError::DataUnavailable(_))),
        "{result:?}"
    );
    admin.drop_table(&path, false).await.unwrap();
}

fn pk_row(id: i32, name: &'static str) -> GenericRow<'static> {
    let mut row = GenericRow::new(2);
    row.set_field(0, id);
    row.set_field(1, name);
    row
}

#[tokio::test]
async fn complete_log_pk_read_applies_changes_only_up_to_the_frozen_stop() {
    let connection = Arc::new(support::get_shared_cluster().get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "union_read_pk_complete_log");
    let descriptor = TableDescriptor::builder()
        .schema(pk_schema())
        .distributed_by(Some(1), vec!["id".into()])
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    let table = connection.get_table(&path).await.unwrap();
    let writer = table.new_upsert().unwrap().create_writer().unwrap();
    for value in [pk_row(1, "old"), pk_row(2, "remove"), pk_row(1, "kept")] {
        writer.upsert(&value).unwrap().await.unwrap();
    }
    writer.delete(&pk_row(2, "remove")).unwrap().await.unwrap();
    let scan = FlussLakeTable::new(connection.clone(), &path)
        .new_scan()
        .with_projection(vec![1]);
    // The cluster periodically creates KV snapshots. Pin the complete-log path
    // explicitly so this read does not depend on snapshot timing.
    let info = table.get_table_info();
    let stop = admin
        .list_offsets(&path, &[0], fluss::rpc::message::OffsetSpec::Latest)
        .await
        .unwrap()[&0];
    let context = FlussLakeReadContext::from_boundary(
        info,
        None,
        vec![fluss_lake::FlussLakeLogRange::new(
            fluss::metadata::TableBucket::new(info.table_id, 0),
            fluss_lake::FlussLakeLogPartition::Ordinary(
                fluss_lake::FlussLakePartitionIdentity::Unpartitioned,
            ),
            1,
            None,
            stop,
            0,
        )],
    )
    .unwrap();
    let plan = scan.plan_with_context(&context).await.unwrap();
    writer
        .upsert(&pk_row(1, "too late"))
        .unwrap()
        .await
        .unwrap();
    let batches = scan
        .new_reader()
        .read_splits(plan.splits())
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let names: Vec<_> = batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
        })
        .collect();
    assert_eq!(names, [Some("kept")]);
    admin.drop_table(&path, false).await.unwrap();
}
