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

use arrow::array::{Array, Int32Array, StringArray};
use fluss::ServerType;
use fluss::client::{FlussAdmin, FlussConnection, KvSnapshotLeaseMode, KvSnapshotReaderOptions};
use fluss::config::Config;
use fluss::metadata::{
    AddColumn, AlterTableChanges, ColumnPositionType, DataTypes, JsonSerde,
    KvSnapshotLeaseForBucket, KvSnapshotLeaseForTable, Schema, TableBucket, TableDescriptor,
    TablePath,
};
use fluss::row::GenericRow;
use fluss::rpc::message::OffsetSpec;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::Duration;

fn row(id: i32, name: &str, age: Option<i32>, evolved: bool, partitioned: bool) -> GenericRow<'_> {
    let age_index = if partitioned { 3 } else { 2 };
    let mut row = GenericRow::new(age_index + usize::from(evolved));
    row.set_field(0, id);
    row.set_field(1, name);
    if partitioned {
        row.set_field(2, "us");
    }
    if let Some(age) = age {
        row.set_field(age_index, age);
    }
    row
}

async fn snapshot_after(
    admin: &FlussAdmin,
    path: &TablePath,
    partition: Option<&str>,
    offset: i64,
) -> (i64, i64) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let latest = admin
                .get_latest_kv_snapshots(path, partition)
                .await
                .unwrap();
            if let Some(snapshot) = latest.latest_snapshots.iter().find(|s| {
                s.bucket_id == 0
                    && s.snapshot_id.is_some()
                    && s.log_offset.is_some_and(|s| s >= offset)
            }) {
                return (snapshot.snapshot_id.unwrap(), snapshot.log_offset.unwrap());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("persisted snapshot did not reach the acknowledged writes")
}

/// Waits until a tablet server has registered, because the coordinator rejects table creation
/// before then.
async fn wait_for_tablet_server(admin: &FlussAdmin) {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            match admin.get_server_nodes().await {
                Ok(nodes)
                    if nodes
                        .iter()
                        .any(|node| *node.server_type() == ServerType::TabletServer) =>
                {
                    return;
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .expect("no tablet server registered with the coordinator")
}

async fn end_offset(admin: &FlussAdmin, path: &TablePath, partition: Option<&str>) -> i64 {
    match partition {
        Some(partition) => admin
            .list_partition_offsets(path, partition, &[0], OffsetSpec::Latest)
            .await
            .unwrap()[&0],
        None => admin
            .list_offsets(path, &[0], OffsetSpec::Latest)
            .await
            .unwrap()[&0],
    }
}

async fn rename_snapshot_file(
    cluster: Option<&fluss_test_cluster::FlussTestingCluster>,
    from: &Path,
    to: &Path,
) {
    if let Some(cluster) = cluster {
        // On Linux, snapshot directories belong to the container user. The host runner
        // can read the files but cannot rename them in those directories.
        cluster
            .exec_in_tablet_server(
                0,
                vec![
                    "mv".into(),
                    "--".into(),
                    from.to_str().unwrap().into(),
                    to.to_str().unwrap().into(),
                ],
            )
            .await;
    } else {
        std::fs::rename(from, to).unwrap();
    }
}

/// Set FLUSS_KV_SNAPSHOT_BOOTSTRAP to exercise a locally built Java test cluster.
/// Otherwise this uses the same container harness as the other integration tests.
#[tokio::test]
async fn persisted_snapshot_current_view_and_schema() {
    let target = std::env::current_dir().unwrap().join("target");
    std::fs::create_dir_all(&target).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("snapshot-it-")
        .tempdir_in(target)
        .unwrap();
    let cluster = if std::env::var("FLUSS_KV_SNAPSHOT_BOOTSTRAP").is_ok() {
        None
    } else {
        Some(
            fluss_test_cluster::FlussTestingClusterBuilder::new_with_cluster_conf(
                format!(
                    "snapshot-{}",
                    &uuid::Uuid::new_v4().simple().to_string()[..12]
                ),
                &HashMap::from([
                    ("kv.snapshot.interval".into(), "1s".into()),
                    ("kv.snapshot.num-retained".into(), "2".into()),
                ]),
            )
            .with_port(19323)
            .with_remote_data_dir(directory.path().join("remote"))
            .build()
            .await,
        )
    };
    let bootstrap_servers = match &cluster {
        Some(cluster) => {
            // Wait for the server using the existing cluster harness.
            let _ = cluster.get_fluss_connection().await;
            cluster.plaintext_bootstrap_servers().to_string()
        }
        None => std::env::var("FLUSS_KV_SNAPSHOT_BOOTSTRAP").unwrap(),
    };
    let downloads = directory.path().join("downloads");
    let connection = FlussConnection::new(Config {
        bootstrap_servers,
        scanner_io_tmp_dir: Some(downloads.to_string_lossy().into_owned()),
        ..Default::default()
    })
    .await
    .unwrap();
    let admin = connection.get_admin().unwrap();
    wait_for_tablet_server(&admin).await;

    for (format, tagged) in [
        ("COMPACTED", false),
        ("INDEXED", false),
        ("COMPACTED", true),
    ] {
        let path = TablePath::new(
            "fluss",
            format!("snapshot_{}", uuid::Uuid::new_v4().simple()),
        );
        let mut schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string());
        if tagged {
            schema = schema.column("region", DataTypes::string());
        }
        let schema = schema
            .primary_key(if tagged {
                vec!["region", "id"]
            } else {
                vec!["id"]
            })
            .unwrap()
            .build()
            .unwrap();
        let mut descriptor = TableDescriptor::builder()
            .schema(schema)
            .distributed_by(Some(1), vec!["id".into()])
            .property("table.kv.format", format);
        if tagged {
            descriptor = descriptor
                .property("table.kv.ttl", "1 d")
                .partitioned_by(vec!["region"]);
        }
        admin
            .create_table(&path, &descriptor.build().unwrap(), false)
            .await
            .unwrap();
        let table = connection.get_table(&path).await.unwrap();
        if format == "INDEXED" {
            let bucket = TableBucket::new(table.get_table_info().table_id, 0);
            let error = table
                .create_kv_snapshot_reader(bucket, 0)
                .await
                .err()
                .expect("unsupported decoder");
            assert!(
                error
                    .to_string()
                    .contains("INDEXED format is not yet supported"),
                "{error}"
            );
            admin.drop_table(&path, false).await.unwrap();
            continue;
        }
        let writer = table.new_upsert().unwrap().create_writer().unwrap();
        let mut acks = Vec::new();
        for (id, name) in [(1, "old"), (2, "deleted"), (3, "unchanged")] {
            acks.push(writer.upsert(&row(id, name, None, false, tagged)).unwrap());
        }
        writer.flush().await.unwrap();
        for ack in acks {
            ack.await.expect("upsert ack");
        }
        let partition = if tagged {
            Some(admin.list_partition_infos(&path).await.unwrap().remove(0))
        } else {
            None
        };
        let partition_name = partition.as_ref().map(|p| p.get_partition_name());
        let first_end = end_offset(&admin, &path, partition_name.as_deref()).await;
        let (first_snapshot, _) =
            snapshot_after(&admin, &path, partition_name.as_deref(), first_end).await;

        admin
            .alter_table(
                &path,
                false,
                AlterTableChanges {
                    add_columns: vec![AddColumn {
                        column_name: "age".into(),
                        data_type_json: serde_json::to_vec(
                            &DataTypes::int().serialize_json().unwrap(),
                        )
                        .unwrap(),
                        comment: None,
                        position: ColumnPositionType::Last,
                    }],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let fixed_table = connection.get_table(&path).await.unwrap();
        if tagged {
            assert_eq!(
                fixed_table.get_table_info().get_properties()["table.kv.value-layout-version"],
                "2"
            );
        }
        let writer = fixed_table.new_upsert().unwrap().create_writer().unwrap();
        let ack1 = writer
            .upsert(&row(1, "updated", Some(7), true, tagged))
            .unwrap();
        let ack2 = writer
            .delete(&row(2, "deleted", None, true, tagged))
            .unwrap();
        writer.flush().await.unwrap();
        ack1.await.expect("upsert ack");
        ack2.await.expect("delete ack");
        let end = end_offset(&admin, &path, partition_name.as_deref()).await;
        let (snapshot_id, seam) =
            snapshot_after(&admin, &path, partition_name.as_deref(), end).await;
        assert!(snapshot_id > first_snapshot);
        let bucket = TableBucket::new_with_partition(
            fixed_table.get_table_info().table_id,
            partition.as_ref().map(|p| p.get_partition_id()),
            0,
        );
        let lease_id = format!("snapshot-plan-{}", uuid::Uuid::new_v4());
        let leased = admin
            .create_kv_snapshot_lease(
                &lease_id,
                120_000,
                vec![KvSnapshotLeaseForTable {
                    table_id: bucket.table_id(),
                    bucket_snapshots: vec![KvSnapshotLeaseForBucket {
                        partition_id: bucket.partition_id(),
                        bucket_id: 0,
                        snapshot_id,
                    }],
                }],
            )
            .await
            .unwrap();
        assert!(leased.unavailable_snapshots.is_empty());
        let ack1 = writer
            .upsert(&row(1, "after-snapshot", Some(99), true, tagged))
            .unwrap();
        let ack2 = writer
            .upsert(&row(4, "after-snapshot", Some(99), true, tagged))
            .unwrap();
        writer.flush().await.unwrap();
        for ack in [ack1, ack2] {
            ack.await.expect("upsert ack");
        }

        let mut reader = fixed_table
            .create_kv_snapshot_reader_with_lease_duration(
                bucket.clone(),
                snapshot_id,
                Duration::from_secs(120),
            )
            .await
            .unwrap();
        assert_eq!(reader.log_offset(), seam);
        assert_eq!(std::fs::read_dir(&downloads).unwrap().count(), 1);
        if !tagged {
            assert!(reader.next_batch(0).await.is_err());
        }
        let mut actual = BTreeMap::new();
        while let Some(batch) = reader.next_batch(1).await.unwrap() {
            assert_eq!(batch.num_rows(), 1);
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ages = batch
                .column(if tagged { 3 } else { 2 })
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            actual.insert(
                ids.value(0),
                (
                    names.value(0).to_string(),
                    if ages.is_null(0) {
                        None
                    } else {
                        Some(ages.value(0))
                    },
                ),
            );
        }
        assert_eq!(
            actual,
            BTreeMap::from([
                (1, ("updated".into(), Some(7))),
                (3, ("unchanged".into(), None)),
            ])
        );
        assert_eq!(std::fs::read_dir(&downloads).unwrap().count(), 0);
        // Lifecycle and download failures are independent of the persisted value layout.
        if tagged {
            admin.drop_kv_snapshot_lease(&lease_id).await.unwrap();
            admin.drop_table(&path, false).await.unwrap();
            continue;
        }

        let mut reader = fixed_table
            .create_kv_snapshot_reader(bucket.clone(), snapshot_id)
            .await
            .unwrap();
        reader.close();
        reader.close();
        assert!(reader.next_batch(1).await.unwrap().is_none());
        assert_eq!(std::fs::read_dir(&downloads).unwrap().count(), 0);
        let reader = fixed_table
            .create_kv_snapshot_reader(bucket.clone(), snapshot_id)
            .await
            .unwrap();
        drop(reader);
        assert_eq!(std::fs::read_dir(&downloads).unwrap().count(), 0);
        assert!(
            fixed_table
                .create_kv_snapshot_reader(bucket.clone(), i64::MAX)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read_dir(&downloads).unwrap().count(), 0);
        let metadata = admin
            .get_kv_snapshot_metadata(bucket.table_id(), bucket.partition_id(), 0, snapshot_id)
            .await
            .unwrap();
        let current = metadata
            .snapshot_files
            .iter()
            .find(|file| file.local_file_name == "CURRENT")
            .unwrap();
        let current = if current.remote_path.contains("://") {
            url::Url::parse(&current.remote_path)
                .unwrap()
                .to_file_path()
                .expect("snapshot failure injection requires local storage")
        } else {
            std::path::PathBuf::from(&current.remote_path)
        };
        let hidden = current.with_extension("unavailable");
        rename_snapshot_file(cluster.as_ref(), &current, &hidden).await;
        let failed = fixed_table
            .create_kv_snapshot_reader(bucket.clone(), snapshot_id)
            .await;
        rename_snapshot_file(cluster.as_ref(), &hidden, &current).await;
        assert!(
            failed.is_err(),
            "a missing snapshot file must fail without substituting a newer snapshot"
        );
        assert_eq!(std::fs::read_dir(&downloads).unwrap().count(), 0);
        // Planning protects this snapshot while newer snapshots exceed ordinary retention.
        // Repeat after reader close to verify that it did not release the shared planning lease.
        for _ in 0..2 {
            for age in 100..103 {
                let ack = writer
                    .upsert(&row(1, "newer", Some(age), true, false))
                    .unwrap();
                writer.flush().await.unwrap();
                ack.await.expect("upsert ack");
                let end = end_offset(&admin, &path, None).await;
                snapshot_after(&admin, &path, None, end).await;
            }
            let mut reader = table
                .create_kv_snapshot_reader_with_options(
                    bucket.clone(),
                    snapshot_id,
                    KvSnapshotReaderOptions {
                        target_schema_id: Some(fixed_table.get_table_info().get_schema_id()),
                        projected_fields: Some(vec![2, 0]),
                        lease_mode: KvSnapshotLeaseMode::CallerManaged,
                    },
                )
                .await
                .unwrap();
            let mut actual = BTreeMap::new();
            while let Some(batch) = reader.next_batch(10).await.unwrap() {
                assert_eq!(batch.schema().field(0).name(), "age");
                assert_eq!(batch.schema().field(1).name(), "id");
                let ages = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                let ids = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap();
                for index in 0..batch.num_rows() {
                    assert!(
                        actual
                            .insert(
                                ids.value(index),
                                (!ages.is_null(index)).then(|| ages.value(index)),
                            )
                            .is_none(),
                        "schema-boundary resume duplicated a row"
                    );
                }
            }
            assert_eq!(actual, BTreeMap::from([(1, Some(7)), (3, None)]));
        }
        admin.drop_kv_snapshot_lease(&lease_id).await.unwrap();
        admin.drop_table(&path, false).await.unwrap();
    }
    connection.close(Duration::from_secs(5)).await.unwrap();
    if let Some(cluster) = cluster {
        cluster.stop();
    }
}
