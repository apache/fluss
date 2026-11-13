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

use crate::bucketing::BucketingFunction;
use crate::client::{FlussConnection, Lookuper, PrefixKeyLookuper};
use crate::cluster::ServerType;
use crate::config::Config;
use crate::metadata::{
    DataTypes, PartitionSpec, PhysicalTablePath, Schema, TableBucket, TableDescriptor, TablePath,
};
use crate::row::encode::KeyEncoderFactory;
use crate::row::{DataGetters, GenericRow};
use fluss_test_cluster::FlussTestingClusterBuilder;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(60);

async fn wait_until<F, Fut>(mut ready: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    tokio::time::timeout(TIMEOUT, async {
        while !ready().await {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the cluster reached the expected state");
}

fn key(id: i32, prefix: bool, partitioned: bool) -> GenericRow<'static> {
    let mut key = GenericRow::new(1 + usize::from(!prefix) + usize::from(partitioned));
    let offset = usize::from(partitioned);
    if partitioned {
        key.set_field(0, "p1");
    }
    key.set_field(offset, id);
    if !prefix {
        key.set_field(offset + 1, 0);
    }
    key
}

async fn assert_found(
    primary: &Lookuper,
    prefix: &PrefixKeyLookuper,
    id: i32,
    partitioned: bool,
    replicas: i32,
) {
    let primary_key = key(id, false, partitioned);
    let prefix_key = key(id, true, partitioned);
    let (primary, prefix) = tokio::join!(primary.lookup(&primary_key), prefix.lookup(&prefix_key));
    for (operation, result) in [("primary key", primary), ("prefix", prefix)] {
        let context =
            format!("{operation} lookup: replicas={replicas}, partitioned={partitioned}, id={id}");
        let result = result.unwrap_or_else(|error| panic!("{context}: {error}"));
        let rows = result.get_rows().unwrap();
        assert_eq!(rows.len(), 1, "{context}");
        assert_eq!(rows[0].get_int(0).unwrap(), id, "{context}");
        assert_eq!(rows[0].get_int(1).unwrap(), 0, "{context}");
    }
}

#[tokio::test]
async fn lookups_recover_after_a_tablet_server_failure_in_a_real_cluster() {
    for replicas in [2, 1] {
        let cluster_conf = HashMap::from([
            (
                "zookeeper.client.session-timeout".to_string(),
                "3s".to_string(),
            ),
            (
                "log.replica.min-in-sync-replicas-number".to_string(),
                replicas.to_string(),
            ),
        ]);
        let cluster = FlussTestingClusterBuilder::new_with_cluster_conf(
            format!("lookup-failover-{replicas}"),
            &cluster_conf,
        )
        .with_port(10123)
        .with_tablet_servers(2)
        .build()
        .await;
        let config = Config {
            bootstrap_servers: cluster.plaintext_bootstrap_servers().to_string(),
            writer_acks: "all".to_string(),
            lookup_batch_timeout_ms: 0,
            connect_timeout_ms: 1000,
            ..Default::default()
        };
        drop(cluster.get_fluss_connection().await);
        let control = FlussConnection::new(config.clone()).await.unwrap();
        let admin = control.get_admin().unwrap();
        wait_until(|| async {
            admin.get_server_nodes().await.is_ok_and(|nodes| {
                nodes
                    .iter()
                    .filter(|node| *node.server_type() == ServerType::TabletServer)
                    .count()
                    == 2
            })
        })
        .await;

        for partitioned in [false, true] {
            let table_path = TablePath::new("fluss", format!("failover_{partitioned}"));
            let mut schema = Schema::builder()
                .column("id", DataTypes::int())
                .column("seq", DataTypes::int());
            if partitioned {
                schema = schema.column("p", DataTypes::string());
            }
            let schema = schema
                .primary_key(if partitioned {
                    vec!["p", "id", "seq"]
                } else {
                    vec!["id", "seq"]
                })
                .unwrap()
                .build()
                .unwrap();
            let descriptor = TableDescriptor::builder()
                .schema(schema.clone())
                .partitioned_by(if partitioned { vec!["p"] } else { vec![] })
                .distributed_by(Some(2), vec!["id".to_string()])
                .property("table.replication.factor", replicas.to_string())
                .build()
                .unwrap();
            admin
                .create_table(&table_path, &descriptor, false)
                .await
                .unwrap();
            if partitioned {
                admin
                    .create_partition(
                        &table_path,
                        &PartitionSpec::new(HashMap::from([("p".to_string(), "p1".to_string())])),
                        false,
                    )
                    .await
                    .unwrap();
            }
            let table = control.get_table(&table_path).await.unwrap();
            let writer = table.new_upsert().unwrap().create_writer().unwrap();
            for id in 0..32 {
                let mut row = GenericRow::new(if partitioned { 3 } else { 2 });
                row.set_field(0, id);
                row.set_field(1, 0);
                if partitioned {
                    row.set_field(2, "p1");
                }
                writer.upsert(&row).unwrap();
            }
            tokio::time::timeout(TIMEOUT, writer.flush())
                .await
                .unwrap()
                .unwrap();

            let lookup_connection = FlussConnection::new(config.clone()).await.unwrap();
            let table = lookup_connection.get_table(&table_path).await.unwrap();
            let primary = table.new_lookup().unwrap().create_lookuper().unwrap();
            let prefix = table
                .new_lookup()
                .unwrap()
                .lookup_by(
                    if partitioned {
                        vec!["p", "id"]
                    } else {
                        vec!["id"]
                    }
                    .into_iter()
                    .map(str::to_string)
                    .collect(),
                )
                .create_lookuper()
                .unwrap();
            let row_type = schema.row_type();
            let mut encoder =
                KeyEncoderFactory::of_bucket_key_encoder(row_type, &["id".to_string()], &None)
                    .unwrap();
            let bucketing = <dyn BucketingFunction>::of(None);
            let mut id_for_bucket = |bucket| {
                (0..32)
                    .find(|&id| {
                        let mut row = GenericRow::new(if partitioned { 3 } else { 2 });
                        row.set_field(0, id);
                        bucketing
                            .bucketing(&encoder.encode_key(&row).unwrap(), 2)
                            .unwrap()
                            == bucket
                    })
                    .unwrap()
            };
            let (affected_id, healthy_id) = (id_for_bucket(0), id_for_bucket(1));
            for id in [affected_id, healthy_id] {
                tokio::time::timeout(
                    TIMEOUT,
                    assert_found(&primary, &prefix, id, partitioned, replicas),
                )
                .await
                .unwrap();
            }
            let metadata = lookup_connection.get_metadata();
            let cached = metadata.get_cluster();
            let info = cached.get_table(&table_path).unwrap();
            let partition_id = if partitioned {
                Some(
                    cached
                        .get_partition_id(&PhysicalTablePath::of_partitioned(
                            Arc::new(table_path.clone()),
                            Some("p1".to_string()),
                        ))
                        .unwrap(),
                )
            } else {
                None
            };
            let affected_bucket = TableBucket::new_with_partition(info.table_id, partition_id, 0);
            let healthy_bucket = TableBucket::new_with_partition(info.table_id, partition_id, 1);
            let stopped = cached.leader_for(&affected_bucket).unwrap().id();
            assert_ne!(stopped, cached.leader_for(&healthy_bucket).unwrap().id());
            let stopped_server = u16::try_from(stopped).unwrap();
            if replicas == 1 {
                cluster.stop_tablet_server(stopped_server).await;
            } else {
                cluster.kill_tablet_server(stopped_server).await;
            }
            wait_until(|| async {
                admin.get_server_nodes().await.is_ok_and(|nodes| {
                    !nodes.iter().any(|node| {
                        node.id() == stopped && *node.server_type() == ServerType::TabletServer
                    })
                })
            })
            .await;
            assert_eq!(
                metadata
                    .get_cluster()
                    .leader_for(&affected_bucket)
                    .unwrap()
                    .id(),
                stopped
            );

            let waiting = assert_found(&primary, &prefix, affected_id, partitioned, replicas);
            tokio::pin!(waiting);
            if replicas == 1 {
                assert!(futures::poll!(&mut waiting).is_pending());
                wait_until(|| async {
                    metadata
                        .get_cluster()
                        .leader_for(&affected_bucket)
                        .is_none()
                })
                .await;
                for _ in 0..5 {
                    tokio::time::timeout(
                        TIMEOUT,
                        assert_found(&primary, &prefix, healthy_id, partitioned, replicas),
                    )
                    .await
                    .unwrap();
                }
                assert!(futures::poll!(&mut waiting).is_pending());
                cluster.start_tablet_server(stopped_server).await;
                tokio::time::timeout(TIMEOUT, &mut waiting).await.unwrap();
            } else {
                tokio::time::timeout(TIMEOUT, &mut waiting).await.unwrap();
                assert_ne!(
                    metadata
                        .get_cluster()
                        .leader_for(&affected_bucket)
                        .unwrap()
                        .id(),
                    stopped
                );
                cluster.start_tablet_server(stopped_server).await;
            }
            wait_until(|| async {
                admin.get_server_nodes().await.is_ok_and(|nodes| {
                    nodes
                        .iter()
                        .filter(|node| *node.server_type() == ServerType::TabletServer)
                        .count()
                        == 2
                })
            })
            .await;
        }
        cluster.stop();
    }
}
