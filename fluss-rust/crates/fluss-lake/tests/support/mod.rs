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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use fluss::ServerType;
use fluss::client::FlussAdmin;
use fluss::error::{Error, FlussError};
use fluss::metadata::{TableDescriptor, TablePath};
use fluss::rpc::message::OffsetSpec;
use fluss_test_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

const READINESS_TIMEOUT: Duration = Duration::from_secs(30);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(200);
extern "C" fn cleanup_on_exit() {
    SHARED_CLUSTER.stop();
}

static CLUSTER_PORT: LazyLock<u16> =
    LazyLock::new(|| 20_000 + (std::process::id() % 20_000) as u16);

// These tests read only the Fluss log; the warehouse stays inside Docker.
// Real lake files are exercised by the Java-owned RustUnionReadITCase.
static SHARED_CLUSTER: LazyLock<FlussTestingCluster> = LazyLock::new(|| {
    std::thread::spawn(|| {
        let runtime = tokio::runtime::Runtime::new()
            .expect("Failed to create UnionRead integration test runtime");
        runtime.block_on(async {
            let cluster_conf = HashMap::from([
                ("datalake.enabled".to_string(), "true".to_string()),
                ("datalake.format".to_string(), "paimon".to_string()),
                (
                    "datalake.paimon.metastore".to_string(),
                    "filesystem".to_string(),
                ),
                (
                    "datalake.paimon.warehouse".to_string(),
                    "/tmp/union-read-warehouse".to_string(),
                ),
            ]);
            let cluster = FlussTestingClusterBuilder::new_with_cluster_conf(
                format!("rust-union-read-{}", std::process::id()),
                &cluster_conf,
            )
            .with_port(*CLUSTER_PORT)
            .build()
            .await;
            wait_for_cluster_ready(&cluster).await;

            unsafe {
                unsafe extern "C" {
                    fn atexit(callback: extern "C" fn()) -> std::os::raw::c_int;
                }
                atexit(cleanup_on_exit);
            }

            cluster
        })
    })
    .join()
    .expect("Failed to initialize UnionRead integration test cluster")
});

pub fn get_shared_cluster() -> Arc<FlussTestingCluster> {
    Arc::new(SHARED_CLUSTER.clone())
}

async fn wait_for_cluster_ready(cluster: &FlussTestingCluster) {
    let connection = cluster.get_fluss_connection().await;
    let admin = connection
        .get_admin()
        .expect("Failed to get admin while waiting for UnionRead test cluster");
    let start = Instant::now();
    loop {
        match admin.get_server_nodes().await {
            Ok(nodes)
                if nodes
                    .iter()
                    .any(|node| *node.server_type() == ServerType::TabletServer) =>
            {
                return;
            }
            result if start.elapsed() >= READINESS_TIMEOUT => {
                panic!(
                    "Timed out waiting for a registered tablet server in the UnionRead test \
                     cluster. Last metadata result: {result:?}"
                );
            }
            _ => tokio::time::sleep(READINESS_POLL_INTERVAL).await,
        }
    }
}

pub async fn create_table(
    admin: &FlussAdmin,
    table_path: &TablePath,
    table_descriptor: &TableDescriptor,
) {
    let create_start = Instant::now();
    loop {
        match admin
            .create_table(table_path, table_descriptor, false)
            .await
        {
            Ok(()) => break,
            Err(Error::FlussAPIError { api_error })
                if FlussError::for_code(api_error.code) == FlussError::InvalidReplicationFactor
                    && create_start.elapsed() < READINESS_TIMEOUT =>
            {
                tokio::time::sleep(READINESS_POLL_INTERVAL).await;
            }
            Err(error) => {
                panic!("Failed to create UnionRead integration test table: {error}");
            }
        }
    }

    let start = Instant::now();
    loop {
        let readiness = if table_descriptor.is_partitioned() {
            admin
                .get_table_info(table_path)
                .await
                .map(|_| HashMap::new())
        } else {
            admin
                .list_offsets(table_path, &[0], OffsetSpec::Latest)
                .await
        };
        match readiness {
            Ok(_) => return,
            Err(error) if start.elapsed() < READINESS_TIMEOUT => {
                tokio::time::sleep(READINESS_POLL_INTERVAL).await;
                if start.elapsed() >= READINESS_TIMEOUT {
                    panic!(
                        "Timed out waiting for UnionRead integration test table {table_path}: {error}"
                    );
                }
            }
            Err(error) => {
                panic!(
                    "Timed out waiting for UnionRead integration test table {table_path}: {error}"
                );
            }
        }
    }
}
