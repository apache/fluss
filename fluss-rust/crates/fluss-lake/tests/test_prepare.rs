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
use fluss_lake::{FlussLakeBaseline, FlussLakeReadContext, FlussLakeTable};
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
    let imported = FlussLakeReadContext::from_boundary(
        source.get_table_info(),
        plain.lake_snapshot_id(),
        plain.log_ranges().to_vec(),
    )
    .unwrap();
    imported
        .validate_table(&admin.get_table_info(&path).await.unwrap())
        .unwrap();

    writer.append_arrow_batch(batch(vec![4, 5])).unwrap();
    writer.flush().await.unwrap();
    assert_eq!(imported.log_ranges(), plain.log_ranges());
    assert_eq!(imported.table_schema(), plain.table_schema());
    assert_eq!(imported.log_ranges()[0].stop_offset(), 3);
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
    let evolved = connection.get_table(&path).await.unwrap();
    imported.validate_table(evolved.get_table_info()).unwrap();
    let restored = table
        .import_read_context(&plain.to_protobuf().unwrap())
        .await
        .unwrap();
    assert_eq!(restored.schema_id(), plain.schema_id());
    assert_eq!(restored.table_schema(), plain.table_schema());
    assert_eq!(restored.log_ranges(), plain.log_ranges());
    assert_eq!(imported.log_ranges(), plain.log_ranges());
    assert_eq!(imported.table_schema(), plain.table_schema());
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
    let imported = FlussLakeReadContext::from_boundary_with_partition_scope(
        source.get_table_info(),
        selected.lake_snapshot_id(),
        selected.log_ranges().to_vec(),
        Some(&filter),
    )
    .unwrap();
    assert_eq!(imported.partition_scope(), Some(&filter));
    assert_eq!(imported.log_ranges(), selected.log_ranges());
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
