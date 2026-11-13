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

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::record_batch::RecordBatch;
use fluss::metadata::{
    AddColumn, AlterTableChanges, ColumnPositionType, DataTypes, JsonSerde, PartitionSpec, Schema,
    TableDescriptor, TablePath,
};
use fluss::predicate::col;
use fluss_lake::{FlussLakeBaseline, FlussLakeTable};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod support;

// Source preparation needs only Fluss: no lake reader, tiering job or S3 fixture.
#[tokio::test]
async fn prepare_freezes_fluss_only_ranges_and_schema() {
    let cluster = support::get_shared_cluster();
    let connection = Arc::new(cluster.get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "prepare_only");
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    let table = FlussLakeTable::new(connection.clone(), &path);
    assert!(table.capture_read_context().await.is_err());
    support::create_table(&admin, &path, &descriptor).await;
    let source = connection.get_table(&path).await.unwrap();
    let writer = source.new_append().unwrap().create_writer().unwrap();
    let batch = |values| {
        RecordBatch::try_from_iter(vec![("id", Arc::new(Int32Array::from(values)) as ArrayRef)])
            .unwrap()
    };
    writer.append_arrow_batch(batch(vec![1, 2, 3])).unwrap();
    writer.flush().await.unwrap();

    let plain = table.capture_read_context().await.unwrap();
    assert_eq!(plain.lake_snapshot_id(), None);
    assert_eq!(plain.lake_format(), None);
    assert_eq!(plain.log_ranges()[0].baseline(), FlussLakeBaseline::None);
    assert_eq!(plain.log_ranges().len(), 1);
    assert_eq!(plain.log_ranges()[0].start_offset(), 0);
    assert_eq!(plain.log_ranges()[0].stop_offset(), 3);
    plain.log_ranges()[0].validate_available().unwrap();
    writer.append_arrow_batch(batch(vec![4, 5])).unwrap();
    writer.flush().await.unwrap();
    assert_eq!(plain.log_ranges()[0].stop_offset(), 3);
    assert_eq!(
        table.capture_read_context().await.unwrap().log_ranges()[0].stop_offset(),
        5
    );
    drop(writer);

    admin
        .alter_table(
            &path,
            false,
            AlterTableChanges {
                add_columns: vec![AddColumn {
                    column_name: "added".into(),
                    data_type_json: serde_json::to_vec(&DataTypes::int().serialize_json().unwrap())
                        .unwrap(),
                    comment: None,
                    position: ColumnPositionType::Last,
                }],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let evolved = admin.get_table_info(&path).await.unwrap();
    assert_ne!(evolved.schema_id, plain.schema_id());
    let restored = table
        .import_read_context(&plain.to_protobuf().unwrap())
        .await
        .unwrap();
    assert_eq!(restored.schema_id(), plain.schema_id());
    assert_eq!(restored.table_schema(), plain.table_schema());
    assert_eq!(restored.log_ranges(), plain.log_ranges());
    admin.drop_table(&path, false).await.unwrap();
}

#[tokio::test]
async fn kv_snapshot_boundaries_plan_and_import_without_lake() {
    let cluster = support::get_shared_cluster();
    let connection = Arc::new(cluster.get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "prepare_kv_snapshot");
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("region", DataTypes::string())
                .primary_key(vec!["region", "id"])
                .unwrap()
                .build()
                .unwrap(),
        )
        .partitioned_by(vec!["region"])
        .distributed_by(Some(1), vec!["id".into()])
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    admin
        .create_partition(
            &path,
            &PartitionSpec::new(HashMap::from([("region", "US")])),
            false,
        )
        .await
        .unwrap();
    let core_table = connection.get_table(&path).await.unwrap();
    let writer = core_table.new_upsert().unwrap().create_writer().unwrap();
    let mut row = fluss::row::GenericRow::new(2);
    row.set_field(0, 1);
    row.set_field(1, "US");
    let write = writer.upsert(&row).unwrap();
    writer.flush().await.unwrap();
    write.await.unwrap();

    let table = FlussLakeTable::new(connection.clone(), &path);
    let context = tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let context = table.capture_read_context().await.unwrap();
            if context.log_ranges().iter().any(|range| {
                matches!(range.baseline(), FlussLakeBaseline::KvSnapshot { .. })
                    && range.start_offset() >= 1
            }) {
                break context;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("persisted KV snapshot was not captured");
    let range = &context.log_ranges()[0];
    let FlussLakeBaseline::KvSnapshot { snapshot_id } = range.baseline() else {
        unreachable!()
    };
    let bucket = range.table_bucket();
    let metadata = admin
        .get_kv_snapshot_metadata(
            bucket.table_id(),
            bucket.partition_id(),
            bucket.bucket_id(),
            snapshot_id,
        )
        .await
        .unwrap();
    assert_eq!(range.start_offset(), metadata.log_offset);
    assert_eq!(range.stop_offset(), 1);
    assert_eq!(context.lake_snapshot_id(), None);

    // Later writes must not change the baseline or stop of assigned work.
    row.set_field(0, 2);
    let write = writer.upsert(&row).unwrap();
    writer.flush().await.unwrap();
    write.await.unwrap();
    let scan = table.new_scan();
    let plan = scan.plan_with_context(&context).await.unwrap();
    assert_eq!(plan.split_count(), 1);
    let tasks = vec![plan.splits()[0].descriptor().clone()];
    assert_eq!(tasks[0].kv_snapshot_id(), Some(snapshot_id));
    assert_eq!(tasks[0].stop_offset(), 1);
    let imported = scan
        .import_splits(&context.to_protobuf().unwrap(), &tasks)
        .await
        .unwrap();
    assert_eq!(imported[0].descriptor(), &tasks[0]);
    drop(writer);
    admin.drop_table(&path, false).await.unwrap();
}

#[tokio::test]
async fn partition_filter_preserves_scope_and_resolved_bucket_counts() {
    let cluster = support::get_shared_cluster();
    let connection = Arc::new(cluster.get_fluss_connection().await);
    let admin = connection.get_admin().unwrap();
    let path = TablePath::new("fluss", "prepare_partitions");
    let descriptor = TableDescriptor::builder()
        .schema(
            Schema::builder()
                .column("id", DataTypes::int())
                .column("region", DataTypes::string())
                .build()
                .unwrap(),
        )
        .partitioned_by(vec!["region"])
        .distributed_by(Some(1), vec![])
        .property("table.datalake.enabled", "true")
        .property("table.datalake.format", "paimon")
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    for (region, count) in [("US", 1), ("EU", 2)] {
        admin
            .alter_table(
                &path,
                false,
                AlterTableChanges {
                    new_bucket_count: Some(count),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        admin
            .create_partition(
                &path,
                &PartitionSpec::new(HashMap::from([("region", region)])),
                false,
            )
            .await
            .unwrap();
        let started = Instant::now();
        loop {
            match admin
                .list_partition_offsets(
                    &path,
                    region,
                    &(0..count).collect::<Vec<_>>(),
                    fluss::rpc::message::OffsetSpec::Latest,
                )
                .await
            {
                Ok(_) => break,
                Err(error) => {
                    assert!(
                        started.elapsed() < Duration::from_secs(30),
                        "partition not ready: {error}"
                    );
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
    }
    let source = connection.get_table(&path).await.unwrap();
    let writer = source.new_append().unwrap().create_writer().unwrap();
    // append_arrow_batch routes a batch using its first row's partition.
    for (region, ids) in [("US", vec![1]), ("EU", vec![2, 3])] {
        let count = ids.len();
        writer
            .append_arrow_batch(
                RecordBatch::try_from_iter(vec![
                    ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
                    (
                        "region",
                        Arc::new(StringArray::from(vec![region; count])) as ArrayRef,
                    ),
                ])
                .unwrap(),
            )
            .unwrap();
    }
    writer.flush().await.unwrap();
    drop(writer);
    let table = FlussLakeTable::new(connection.clone(), &path);
    let all = table.capture_read_context().await.unwrap();
    assert_eq!(all.log_ranges().len(), 3);
    let filter = col("region").ge("US");
    let selected = table
        .capture_read_context_with_partition_scope(&filter)
        .await
        .unwrap();
    assert_eq!(selected.log_ranges().len(), 1);
    assert_eq!(selected.log_ranges()[0].bucket_count(), 1);
    assert_eq!(selected.log_ranges()[0].stop_offset(), 1);
    assert_eq!(selected.partition_scope(), Some(&filter));
    admin
        .drop_partition(
            &path,
            &PartitionSpec::new(HashMap::from([("region", "US")])),
            false,
        )
        .await
        .unwrap();
    let dropped = table
        .capture_read_context_with_partition_scope(&filter)
        .await
        .unwrap();
    assert!(dropped.log_ranges().is_empty());
    assert_eq!(dropped.partition_scope(), selected.partition_scope());
    admin.drop_table(&path, false).await.unwrap();
}
