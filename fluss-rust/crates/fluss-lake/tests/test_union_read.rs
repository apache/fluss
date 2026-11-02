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
    AddColumn, AlterTableChanges, ColumnPositionType, DataTypes, JsonSerde, Schema,
    TableDescriptor, TablePath,
};
use fluss::predicate::col;
use fluss_lake::{FlussLakeError, FlussLakeReadContext, FlussLakeReadSplit, FlussLakeTable};
use futures::TryStreamExt;
use std::sync::Arc;
use std::time::Duration;

fn append_batch(ids: Vec<i32>, names: Vec<&str>) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
        ("name", Arc::new(StringArray::from(names)) as ArrayRef),
    ])
    .expect("Failed to build append record batch")
}

#[tokio::test]
async fn append_log_plan_uses_frozen_stop_offset_after_transport() {
    let cluster = support::get_shared_cluster();
    let connection = Arc::new(cluster.get_fluss_connection().await);
    let admin = connection.get_admin().expect("Failed to get Fluss admin");
    let table_path = TablePath::new("fluss", "test_union_read_frozen_append_log");
    let table_descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("name", DataTypes::string())
                .build()
                .expect("Failed to build UnionRead integration test schema"),
        )
        .build()
        .expect("Failed to build UnionRead integration test table descriptor");
    support::create_table(&admin, &table_path, &table_descriptor).await;

    let table = connection
        .get_table(&table_path)
        .await
        .expect("Failed to open UnionRead integration test table");
    let writer = table
        .new_append()
        .expect("Failed to create append operation")
        .create_writer()
        .expect("Failed to create append writer");
    writer
        .append_arrow_batch(append_batch(
            vec![1, 2, 3],
            vec!["before-1", "before-2", "before-3"],
        ))
        .expect("Failed to append pre-plan batch");
    writer
        .flush()
        .await
        .expect("Failed to flush pre-plan batch");

    let lake_table = FlussLakeTable::open(connection.clone(), &table_path)
        .await
        .expect("Failed to open append-log lake table");
    let scan = lake_table
        .new_scan()
        .with_projection(vec![1])
        .with_filter(col("id").eq(2_i32));
    let plan = scan
        .plan()
        .await
        .expect("Failed to plan append-log UnionRead");
    assert_eq!(plan.schema().fields().len(), 1);
    assert_eq!(plan.schema().field(0).name(), "name");
    assert_eq!(plan.splits().len(), 1);
    let transported_split: FlussLakeReadSplit = serde_json::from_slice(
        &serde_json::to_vec(&plan.splits()[0]).expect("Failed to encode UnionRead split"),
    )
    .expect("Failed to decode transported UnionRead split");
    let context = FlussLakeReadContext::from_json(&plan.read_context().to_json().unwrap()).unwrap();

    writer
        .append_arrow_batch(append_batch(vec![4, 5], vec!["after-4", "after-5"]))
        .expect("Failed to append post-plan batch");
    writer
        .flush()
        .await
        .expect("Failed to flush post-plan batch");

    // A different table reference can use a different projection and no
    // predicate without observing the rows appended after preparation.
    // This path does not require the optional Paimon dependency.
    let ids_scan = lake_table.new_scan().with_projection(vec![0]);
    let reused_plan = ids_scan.plan_with_context(&context).await.unwrap();
    assert_eq!(
        reused_plan.read_context().to_json().unwrap(),
        context.to_json().unwrap()
    );
    let ids_splits: Vec<FlussLakeReadSplit> =
        serde_json::from_slice(&serde_json::to_vec(reused_plan.splits()).unwrap()).unwrap();
    drop(reused_plan);
    // A worker has only the original tasks and matching scan settings. It must
    // not replan and include the rows written after the frozen stop offset.
    let ids_reader = lake_table.new_scan().with_projection(vec![0]).new_reader();
    assert!(
        ids_reader
            .read_splits(&[ids_splits[0].clone(), ids_splits[0].clone()])
            .await
            .is_err()
    );
    assert!(
        ids_reader
            .read_splits_with_concurrency(&ids_splits, 0)
            .await
            .is_err()
    );
    let mut incompatible = ids_splits[0].clone();
    incompatible.descriptor_version += 1;
    assert!(matches!(
        ids_reader.read_split(&incompatible).await,
        Err(FlussLakeError::IncompatibleSplitVersion(_))
    ));
    assert!(
        lake_table
            .new_scan()
            .with_batch_size(0)
            .new_reader()
            .read_splits(&[])
            .await
            .is_err()
    );
    let mut ids: Vec<i32> = Vec::new();
    for split in &ids_splits {
        let batches = tokio::time::timeout(
            Duration::from_secs(10),
            ids_reader
                .read_split(split)
                .await
                .unwrap()
                .try_collect::<Vec<_>>(),
        )
        .await
        .unwrap()
        .unwrap();
        for batch in batches {
            assert_eq!(batch.num_columns(), 1);
            ids.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values(),
            );
        }
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3]);
    let later_context = lake_table.prepare().await.unwrap();
    assert!(
        later_context
            .log_ranges()
            .iter()
            .map(|range| range.stop_offset())
            .sum::<i64>()
            > context
                .log_ranges()
                .iter()
                .map(|range| range.stop_offset())
                .sum::<i64>()
    );
    let mut other_info = admin.get_table_info(&table_path).await.unwrap();
    other_info.table_path = TablePath::new("fluss", "different_table");
    let other = FlussLakeTable::try_from_table_info(connection.clone(), &other_info).unwrap();
    assert!(matches!(
        other.new_scan().plan_with_context(&context).await,
        Err(FlussLakeError::InvalidReadContext(_))
    ));
    assert!(matches!(
        other
            .new_scan()
            .new_reader()
            .read_split(&ids_splits[0])
            .await,
        Err(FlussLakeError::PlanningFailed(_))
    ));
    assert!(matches!(
        lake_table
            .new_scan()
            .with_lake_only(true)
            .plan_with_context(&context)
            .await,
        Err(FlussLakeError::NotLakeReadable(_))
    ));

    // A reader captures immutable settings. Configuring another clone must not
    // change its filter or projection, even after dropping the plan.
    let local_reader = scan.new_reader();
    let _other_scan = scan
        .clone()
        .with_projection(vec![0])
        .with_filter(col("id").eq(3_i32));
    drop(plan);
    drop(scan);
    let local_batches = tokio::time::timeout(
        Duration::from_secs(10),
        local_reader
            .read_split(&transported_split)
            .await
            .unwrap()
            .try_collect::<Vec<_>>(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        local_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        1
    );
    assert!(
        local_batches
            .iter()
            .all(|batch| batch.schema().field(0).name() == "name")
    );
    let local_names = local_batches
        .iter()
        .flat_map(|batch| {
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..names.len()).map(|index| names.value(index).to_owned())
        })
        .collect::<Vec<_>>();
    assert_eq!(local_names, vec!["before-2"]);
    // A worker can independently restore the same scan settings.
    let read = lake_table
        .new_scan()
        .with_projection(vec![1])
        .with_filter(col("id").eq(2_i32))
        .new_reader();
    let stream = read
        .read_split(&transported_split)
        .await
        .expect("Failed to execute append-log UnionRead split");
    let batches = tokio::time::timeout(Duration::from_secs(10), stream.try_collect::<Vec<_>>())
        .await
        .expect("Timed out waiting for bounded UnionRead stream to finish")
        .expect("Failed to collect bounded UnionRead output");

    assert!(!batches.is_empty());
    assert!(batches.iter().all(|batch| batch.num_columns() == 1));
    let names: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let names = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Projected name column should be a StringArray");
            (0..names.len()).map(|row| names.value(row).to_string())
        })
        .collect();
    assert_eq!(names, vec!["before-2"]);
    assert!(batches.iter().all(|batch| {
        batch
            .columns()
            .iter()
            .all(|column| !column.as_any().is::<Int32Array>())
    }));

    drop(writer);
    admin
        .drop_table(&table_path, false)
        .await
        .expect("Failed to drop UnionRead integration test table");
    assert!(matches!(
        ids_scan.plan_with_context(&context).await,
        Err(FlussLakeError::DataUnavailable(_))
    ));
}

#[tokio::test]
async fn frozen_schema_survives_alter_table_on_coordinator_and_worker() {
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

    let lake_table = FlussLakeTable::open(connection.clone(), &table_path)
        .await
        .expect("Failed to open stale-schema lake table");
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
    let transported: FlussLakeReadSplit =
        serde_json::from_slice(&serde_json::to_vec(&stale_split).unwrap()).unwrap();
    let expected_schema = plan.schema().clone();
    drop(plan);
    drop(scan);
    let worker_table = FlussLakeTable::open(connection.clone(), &table_path)
        .await
        .unwrap();
    let batches = tokio::time::timeout(Duration::from_secs(10), async {
        worker_table
            .new_scan()
            .new_reader()
            .read_split(&transported)
            .await?
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
    let later = worker_table.prepare().await.unwrap();
    let mixed =
        FlussLakeReadContext::from_boundary(&frozen_info, None, later.log_ranges().to_vec())
            .unwrap();
    let worker_scan = worker_table.new_scan().with_filter(col("id").eq(3_i32));
    let mixed_plan = worker_scan.plan_with_context(&mixed).await.unwrap();
    let batches: Vec<_> = worker_scan
        .new_reader()
        .read_splits(mixed_plan.splits())
        .await
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
