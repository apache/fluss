/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
use crate::integration::fluss_cluster::{FlussTestingCluster, FlussTestingClusterBuilder};
use arrow::array::Int32Array;
use fluss::client::FlussAdmin;
use fluss::metadata::{
    DataField, DataType, DataTypes, PartitionSpec, RowType, Schema, TableDescriptor, TablePath,
};
use fluss::record::ScanBatch;
use fluss::row::FlussArray;
use fluss::row::binary_array::FlussArrayWriter;
use fluss::rpc::message::OffsetSpec;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

extern "C" fn cleanup_on_exit() {
    SHARED_CLUSTER.stop();
}

/// Shared cluster with dual listeners: PLAIN_CLIENT (plaintext) on port 9223
/// and CLIENT (SASL) on port 9123. Includes remote storage config so
/// table_remote_scan can also use this cluster.
static SHARED_CLUSTER: LazyLock<FlussTestingCluster> = LazyLock::new(|| {
    std::thread::spawn(|| {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let temp_dir = std::env::current_dir()
                .unwrap_or_else(|_| std::path::PathBuf::from("."))
                .join("target")
                .join(format!("test-remote-data-{}", uuid::Uuid::new_v4()));
            let _ = std::fs::remove_dir_all(&temp_dir);
            std::fs::create_dir_all(&temp_dir)
                .expect("Failed to create temporary directory for remote data");
            let temp_dir = temp_dir
                .canonicalize()
                .expect("Failed to canonicalize remote data directory path");

            let mut cluster_conf = HashMap::new();
            cluster_conf.insert("log.segment.file-size".to_string(), "120b".to_string());
            cluster_conf.insert(
                "remote.log.task-interval-duration".to_string(),
                "1s".to_string(),
            );

            let cluster =
                FlussTestingClusterBuilder::new_with_cluster_conf("rust-test", &cluster_conf)
                    .with_sasl(vec![
                        ("admin".to_string(), "admin-secret".to_string()),
                        ("alice".to_string(), "alice-secret".to_string()),
                    ])
                    .with_remote_data_dir(temp_dir)
                    .build()
                    .await;
            wait_for_cluster_ready_with_sasl(&cluster).await;

            // Register cleanup so containers are removed on process exit.
            unsafe {
                unsafe extern "C" {
                    fn atexit(f: extern "C" fn()) -> std::os::raw::c_int;
                }
                atexit(cleanup_on_exit);
            }

            cluster
        })
    })
    .join()
    .expect("Failed to initialize shared cluster")
});

/// Returns an `Arc` to the shared test cluster.
pub fn get_shared_cluster() -> Arc<FlussTestingCluster> {
    Arc::new(SHARED_CLUSTER.clone())
}

pub async fn create_table(
    admin: &FlussAdmin,
    table_path: &TablePath,
    table_descriptor: &TableDescriptor,
) {
    admin
        .create_table(table_path, table_descriptor, false)
        .await
        .expect("Failed to create table");
}

const READINESS_TIMEOUT: Duration = Duration::from_secs(30);
const READINESS_POLL_INTERVAL: Duration = Duration::from_millis(200);

async fn poll_until<F, Fut>(
    timeout: Duration,
    interval: Duration,
    timeout_message: String,
    mut probe: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let start = std::time::Instant::now();

    loop {
        match probe().await {
            Ok(()) => return,
            Err(err) => {
                if start.elapsed() >= timeout {
                    panic!(
                        "{timeout_message} after {} seconds. Last error: {err}",
                        timeout.as_secs()
                    );
                }
            }
        }

        tokio::time::sleep(interval).await;
    }
}

/// Waits until the default bucket of a non-partitioned table can serve offset requests.
///
/// Newly-created tables may not have bucket leaders immediately. Polling list offsets avoids
/// fixed sleeps that are either flaky on slow CI or waste time when the cluster is ready sooner.
pub async fn wait_for_table_ready(admin: &FlussAdmin, table_path: &TablePath) {
    wait_for_table_buckets_ready(admin, table_path, &[0]).await;
}

/// Waits until the specified buckets of a non-partitioned table can serve offset requests.
pub async fn wait_for_table_buckets_ready(
    admin: &FlussAdmin,
    table_path: &TablePath,
    buckets: &[i32],
) {
    poll_until(
        READINESS_TIMEOUT,
        READINESS_POLL_INTERVAL,
        format!("Timed out waiting for table '{table_path}' buckets {buckets:?} to become ready"),
        || async {
            admin
                .list_offsets(table_path, buckets, OffsetSpec::Latest)
                .await
                .map(|_| ())
                .map_err(|err| format!("{err:?}"))
        },
    )
    .await;
}

/// Waits until all listed partition values can serve offset requests for the default bucket.
pub async fn wait_for_partitions_ready(
    admin: &FlussAdmin,
    table_path: &TablePath,
    partition_values: &[&str],
) {
    for partition_value in partition_values {
        wait_for_partition_ready(admin, table_path, partition_value).await;
    }
}

/// Waits until one partition value can serve offset requests for the default bucket.
pub async fn wait_for_partition_ready(
    admin: &FlussAdmin,
    table_path: &TablePath,
    partition_value: &str,
) {
    wait_for_partition_buckets_ready(admin, table_path, partition_value, &[0]).await;
}

/// Waits until the specified buckets of a partition can serve offset requests.
pub async fn wait_for_partition_buckets_ready(
    admin: &FlussAdmin,
    table_path: &TablePath,
    partition_value: &str,
    buckets: &[i32],
) {
    poll_until(
        READINESS_TIMEOUT,
        READINESS_POLL_INTERVAL,
        format!(
            "Timed out waiting for table '{table_path}' partition '{partition_value}' buckets {buckets:?} to become ready"
        ),
        || async {
            admin
                .list_partition_offsets(table_path, partition_value, buckets, OffsetSpec::Latest)
                .await
                .map(|_| ())
                .map_err(|err| format!("{err:?}"))
        },
    )
    .await;
}

pub fn make_string_array(values: &[Option<&str>]) -> FlussArray {
    let mut writer = FlussArrayWriter::new(values.len(), &DataTypes::string());
    for (idx, value) in values.iter().enumerate() {
        match value {
            Some(v) => writer.write_string(idx, v),
            None => writer.set_null_at(idx),
        }
    }
    writer.complete().expect("Failed to build string array")
}

pub fn make_int_array(values: &[Option<i32>]) -> FlussArray {
    let mut writer = FlussArrayWriter::new(values.len(), &DataTypes::int());
    for (idx, value) in values.iter().enumerate() {
        match value {
            Some(v) => writer.write_int(idx, *v),
            None => writer.set_null_at(idx),
        }
    }
    writer.complete().expect("Failed to build int array")
}

pub fn extract_ids_from_batches(batches: &[ScanBatch]) -> Vec<i32> {
    batches
        .iter()
        .flat_map(|scan_batch| {
            let batch = scan_batch.batch();
            (0..batch.num_rows()).map(move |row| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id column should be Int32")
                    .value(row)
            })
        })
        .collect()
}

/// Similar to wait_for_cluster_ready but connects with SASL credentials.
pub async fn wait_for_cluster_ready_with_sasl(cluster: &FlussTestingCluster) {
    let (username, password) = cluster
        .sasl_users()
        .first()
        .expect("SASL cluster must have at least one user");

    poll_until(
        Duration::from_secs(30),
        Duration::from_millis(500),
        "SASL server readiness check timed out".to_string(),
        || async {
            let connection = cluster
                .get_fluss_connection_with_sasl(username, password)
                .await;
            if connection
                .get_metadata()
                .get_cluster()
                .get_one_available_server()
                .is_some()
            {
                Ok(())
            } else {
                Err(
                    "CoordinatorEventProcessor may not be initialized or TabletServer may not be available"
                        .to_string(),
                )
            }
        },
    )
    .await;
}

/// Creates partitions for a partitioned table.
///
/// # Arguments
/// * `admin` - The FlussAdmin instance
/// * `table_path` - The table path
/// * `partition_column` - The partition column name
/// * `partition_values` - The partition values to create
pub async fn create_partitions(
    admin: &FlussAdmin,
    table_path: &TablePath,
    partition_column: &str,
    partition_values: &[&str],
) {
    for value in partition_values {
        let mut partition_map = HashMap::new();
        partition_map.insert(partition_column, *value);
        admin
            .create_partition(table_path, &PartitionSpec::new(partition_map), true)
            .await
            .expect("Failed to create partition");
    }
}

pub fn dt_array_int() -> DataType {
    DataTypes::array(DataTypes::int())
}

pub fn dt_map_string_int() -> DataType {
    DataTypes::map(DataTypes::string(), DataTypes::int())
}

pub fn dt_row_seq_label() -> DataType {
    DataTypes::row(vec![
        DataField::new("seq", DataTypes::int(), None),
        DataField::new("label", DataTypes::string(), None),
    ])
}

pub fn as_row_type(dt: &DataType) -> RowType {
    match dt {
        DataType::Row(rt) => rt.clone(),
        other => panic!("expected DataType::Row, got {other:?}"),
    }
}

pub fn dt_row_deep() -> DataType {
    let inner = DataTypes::row(vec![DataField::new("n", DataTypes::int(), None)]);
    DataTypes::row(vec![DataField::new("inner", inner, None)])
}

pub fn dt_row_rich() -> DataType {
    DataTypes::row(vec![
        DataField::new("f_bool", DataTypes::boolean(), None),
        DataField::new("f_int", DataTypes::int(), None),
        DataField::new("f_long", DataTypes::bigint(), None),
        DataField::new("f_float", DataTypes::float(), None),
        DataField::new("f_double", DataTypes::double(), None),
        DataField::new("f_str", DataTypes::string(), None),
        DataField::new("f_bytes", DataTypes::bytes(), None),
        DataField::new("f_decimal", DataTypes::decimal(10, 2), None),
        DataField::new("f_date", DataTypes::date(), None),
        DataField::new("f_time", DataTypes::time_with_precision(3), None),
        DataField::new("f_ts_ntz", DataTypes::timestamp_with_precision(6), None),
        DataField::new("f_ts_ltz", DataTypes::timestamp_ltz_with_precision(6), None),
        DataField::new("f_binary_fixed", DataTypes::binary(4), None),
        DataField::new("f_array_int", DataTypes::array(DataTypes::int()), None),
    ])
}

pub fn array_dt_basics_columns() -> Vec<(&'static str, DataType)> {
    vec![
        ("arr_int", DataTypes::array(DataTypes::int())),
        ("arr_string", DataTypes::array(DataTypes::string())),
        ("arr_of_arr", DataTypes::array(dt_array_int())),
        ("arr_of_row", DataTypes::array(dt_row_seq_label())),
    ]
}

pub fn row_dt_basics_columns() -> Vec<(&'static str, DataType)> {
    vec![
        ("row_basic", dt_row_seq_label()),
        ("row_deep", dt_row_deep()),
        ("row_rich", dt_row_rich()),
    ]
}

pub fn map_dt_basics_columns() -> Vec<(&'static str, DataType)> {
    vec![
        ("map_string_int", dt_map_string_int()),
        (
            "map_of_row",
            DataTypes::map(DataTypes::string(), dt_row_seq_label()),
        ),
        (
            "map_of_map",
            DataTypes::map(DataTypes::string(), dt_map_string_int()),
        ),
        (
            "map_of_array",
            DataTypes::map(DataTypes::string(), dt_array_int()),
        ),
        ("array_of_map", DataTypes::array(dt_map_string_int())),
    ]
}

pub fn scalar_dt_columns() -> Vec<(&'static str, DataType)> {
    vec![
        ("col_tinyint", DataTypes::tinyint()),
        ("col_smallint", DataTypes::smallint()),
        ("col_bigint", DataTypes::bigint()),
        ("col_float", DataTypes::float()),
        ("col_double", DataTypes::double()),
        ("col_boolean", DataTypes::boolean()),
        ("col_char", DataTypes::char(10)),
        ("col_string", DataTypes::string()),
        ("col_decimal", DataTypes::decimal(10, 2)),
        ("col_date", DataTypes::date()),
        ("col_time_s", DataTypes::time_with_precision(0)),
        ("col_time_ms", DataTypes::time_with_precision(3)),
        ("col_time_us", DataTypes::time_with_precision(6)),
        ("col_time_ns", DataTypes::time_with_precision(9)),
        ("col_ts_s", DataTypes::timestamp_with_precision(0)),
        ("col_ts_ms", DataTypes::timestamp_with_precision(3)),
        ("col_ts_us", DataTypes::timestamp_with_precision(6)),
        ("col_ts_ns", DataTypes::timestamp_with_precision(9)),
        ("col_ts_ltz_s", DataTypes::timestamp_ltz_with_precision(0)),
        ("col_ts_ltz_ms", DataTypes::timestamp_ltz_with_precision(3)),
        ("col_ts_ltz_us", DataTypes::timestamp_ltz_with_precision(6)),
        ("col_ts_ltz_ns", DataTypes::timestamp_ltz_with_precision(9)),
        ("col_bytes_top", DataTypes::bytes()),
        ("col_binary_top", DataTypes::binary(4)),
        ("col_ts_us_neg", DataTypes::timestamp_with_precision(6)),
        ("col_ts_ns_neg", DataTypes::timestamp_with_precision(9)),
        (
            "col_ts_ltz_us_neg",
            DataTypes::timestamp_ltz_with_precision(6),
        ),
        (
            "col_ts_ltz_ns_neg",
            DataTypes::timestamp_ltz_with_precision(9),
        ),
    ]
}

#[derive(Default)]
pub struct ColumnPlan {
    cols: Vec<(&'static str, DataType)>,
    index: HashMap<&'static str, usize>,
    sections: Vec<(&'static str, usize)>,
}

impl ColumnPlan {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(mut self, name: &'static str, dt: DataType) -> Self {
        let prev = self.index.insert(name, self.cols.len());
        assert!(prev.is_none(), "duplicate column in plan: {name}");
        self.cols.push((name, dt));
        self
    }

    pub fn extend<I: IntoIterator<Item = (&'static str, DataType)>>(mut self, it: I) -> Self {
        for (n, dt) in it {
            self = self.add(n, dt);
        }
        self
    }

    /// Marks the next column added as the start of a named section. Each call
    /// closes the previous section; the last section runs to the end of the plan.
    pub fn start_section(mut self, name: &'static str) -> Self {
        assert!(
            !self.sections.iter().any(|(n, _)| *n == name),
            "duplicate section: {name}"
        );
        self.sections.push((name, self.cols.len()));
        self
    }

    pub fn build_schema(&self, pk: Option<&[&str]>) -> Schema {
        let mut sb = Schema::builder();
        for (n, dt) in &self.cols {
            sb = sb.column(*n, dt.clone());
        }
        if let Some(keys) = pk {
            sb = sb.primary_key(keys.iter().copied());
        }
        sb.build().expect("schema build")
    }

    pub fn idx(&self, name: &str) -> usize {
        *self
            .index
            .get(name)
            .unwrap_or_else(|| panic!("unknown column in plan: {name}"))
    }

    pub fn len(&self) -> usize {
        self.cols.len()
    }

    /// Half-open range of the named section: `[its start, next section's start or plan end)`.
    pub fn section_range(&self, name: &str) -> std::ops::Range<usize> {
        let pos = self
            .sections
            .iter()
            .position(|(n, _)| *n == name)
            .unwrap_or_else(|| panic!("unknown section: {name}"));
        let start = self.sections[pos].1;
        let end = self
            .sections
            .get(pos + 1)
            .map(|(_, s)| *s)
            .unwrap_or(self.cols.len());
        start..end
    }
}
