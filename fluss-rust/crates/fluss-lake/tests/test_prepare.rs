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

use arrow::array::{ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use fluss::metadata::{DataTypes, Schema, TableDescriptor, TablePath};
use fluss::predicate::col;
use fluss_lake::{FlussLakeReadContext, FlussLakeTable};
use std::sync::Arc;

mod support;

// Source preparation needs only Fluss: no lake reader, tiering job or S3 fixture.
#[tokio::test]
async fn prepare_freezes_table_wide_ranges_without_a_lake_backend() {
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
        .property("table.datalake.enabled", "true")
        .property("table.datalake.format", "paimon")
        .build()
        .unwrap();
    support::create_table(&admin, &path, &descriptor).await;
    let source = connection.get_table(&path).await.unwrap();
    let writer = source.new_append().unwrap().create_writer().unwrap();
    let batch = |values| {
        RecordBatch::try_from_iter(vec![("id", Arc::new(Int32Array::from(values)) as ArrayRef)])
            .unwrap()
    };
    writer.append_arrow_batch(batch(vec![1, 2, 3])).unwrap();
    writer.flush().await.unwrap();

    let table = FlussLakeTable::open(connection.clone(), &path)
        .await
        .unwrap();
    let plain = table.prepare().await.unwrap();
    let filtered = table
        .new_scan()
        .with_filter(col("id").eq(99_i32))
        .with_projection(vec![0])
        .prepare()
        .await
        .unwrap();
    assert_eq!(plain.to_json().unwrap(), filtered.to_json().unwrap());
    assert_eq!(plain.lake_snapshot_id(), None);
    assert_eq!(plain.log_ranges().len(), 1);
    assert_eq!(plain.log_ranges()[0].start_offset(), 0);
    assert_eq!(plain.log_ranges()[0].stop_offset(), 3);
    plain.log_ranges()[0].validate_available().unwrap();
    let bytes = plain.to_json().unwrap();
    let transported = FlussLakeReadContext::from_json(&bytes).unwrap();
    transported
        .validate_table(&admin.get_table_info(&path).await.unwrap())
        .unwrap();

    writer.append_arrow_batch(batch(vec![4, 5])).unwrap();
    writer.flush().await.unwrap();
    assert_eq!(transported.to_json().unwrap(), bytes);
    assert_eq!(transported.log_ranges()[0].stop_offset(), 3);
    assert_eq!(
        table.prepare().await.unwrap().log_ranges()[0].stop_offset(),
        5
    );
    drop(writer);
    admin.drop_table(&path, false).await.unwrap();
}
