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

use crate::client::WriteRecord;
use crate::cluster::{BucketLocation, Cluster, ServerNode, ServerType};
use crate::compression::{
    ArrowCompressionInfo, ArrowCompressionRatioEstimator, ArrowCompressionType,
    DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
};
use crate::metadata::{
    DataField, DataTypes, PhysicalTablePath, RowType, Schema, TableBucket, TableDescriptor,
    TableInfo, TablePath,
};
use crate::metrics::{LABEL_DATABASE, LABEL_TABLE, ScannerMetrics};
use crate::record::{
    APPEND_ONLY_FLAG_MASK, ATTRIBUTES_OFFSET, ArrowBatchConfig, CRC_LENGTH, CRC_OFFSET, ChangeType,
    LENGTH_LENGTH, LENGTH_OFFSET, LOG_MAGIC_VALUE_V1, LOG_OVERHEAD, MAGIC_OFFSET,
    MemoryLogRecordsArrowBuilder, RECORDS_OFFSET, SCHEMA_ID_OFFSET,
};
use crate::row::GenericRow;
use crc32c::crc32c;
use std::collections::HashMap;
use std::sync::Arc;

/// An uncompressed [`ArrowBatchConfig`] with no statistics and a fresh
/// compression ratio estimator.
pub(crate) fn uncompressed_arrow_batch_config(
    schema_id: i32,
    row_type: &RowType,
    write_limit: usize,
) -> ArrowBatchConfig {
    ArrowBatchConfig {
        schema_id,
        row_type: row_type.clone(),
        stats_index_mapping: None,
        compression: ArrowCompressionInfo {
            compression_type: ArrowCompressionType::None,
            compression_level: DEFAULT_NON_ZSTD_COMPRESSION_LEVEL,
        },
        write_limit,
        compression_ratio_estimator: Arc::new(ArrowCompressionRatioEstimator::default()),
    }
}

pub(crate) fn build_table_info(table_path: TablePath, table_id: i64, buckets: i32) -> TableInfo {
    build_table_info_with_columns(
        table_path,
        table_id,
        buckets,
        vec![DataField::new("id", DataTypes::int(), None)],
    )
}

pub(crate) fn build_table_info_with_columns(
    table_path: TablePath,
    table_id: i64,
    buckets: i32,
    columns: Vec<DataField>,
) -> TableInfo {
    let row_type = DataTypes::row(columns);
    let schema = Schema::builder()
        .with_row_type(&row_type)
        .build()
        .expect("schema build");
    let table_descriptor = TableDescriptor::builder()
        .schema(schema)
        .distributed_by(Some(buckets), vec![])
        .build()
        .expect("descriptor build");
    TableInfo::of(table_path, table_id, 1, table_descriptor, 0, 0)
}

pub(crate) fn build_cluster(table_path: &TablePath, table_id: i64, buckets: i32) -> Cluster {
    build_cluster_with_port(table_path, table_id, buckets, 9092)
}

pub(crate) fn build_cluster_with_port(
    table_path: &TablePath,
    table_id: i64,
    buckets: i32,
    port: u32,
) -> Cluster {
    let server = ServerNode::new(1, "127.0.0.1".to_string(), port, ServerType::TabletServer);

    let mut servers = HashMap::new();
    servers.insert(server.id(), server.clone());

    let mut locations_by_path = HashMap::new();
    let mut locations_by_bucket = HashMap::new();
    let mut bucket_locations = Vec::new();

    for bucket_id in 0..buckets {
        let table_bucket = TableBucket::new(table_id, bucket_id);
        let bucket_location = BucketLocation::new(
            table_bucket.clone(),
            Some(server.clone()),
            Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone()))),
        );
        bucket_locations.push(bucket_location.clone());
        locations_by_bucket.insert(table_bucket, bucket_location);
    }
    locations_by_path.insert(
        Arc::new(PhysicalTablePath::of(Arc::new(table_path.clone()))),
        bucket_locations,
    );

    let mut table_id_by_path = HashMap::new();
    table_id_by_path.insert(table_path.clone(), table_id);

    let mut table_info_by_path = HashMap::new();
    table_info_by_path.insert(
        table_path.clone(),
        build_table_info(table_path.clone(), table_id, buckets),
    );

    Cluster::new(
        None,
        servers,
        locations_by_path,
        locations_by_bucket,
        table_id_by_path,
        table_info_by_path,
        HashMap::new(),
    )
}

pub(crate) fn build_cluster_arc(
    table_path: &TablePath,
    table_id: i64,
    buckets: i32,
) -> Arc<Cluster> {
    Arc::new(build_cluster(table_path, table_id, buckets))
}

pub(crate) fn build_cluster_arc_with_port(
    table_path: &TablePath,
    table_id: i64,
    buckets: i32,
    port: u32,
) -> Arc<Cluster> {
    Arc::new(build_cluster_with_port(table_path, table_id, buckets, port))
}

/// Build an `Arc<ScannerMetrics>` for tests. Most callers don't install
/// a recorder, so the cached handles are no-ops; tests that *do* install
/// `metrics::with_local_recorder(...)` must call this *inside* the
/// recorder closure for the cached handles to bind to that recorder.
pub(crate) fn test_scanner_metrics(table_path: &TablePath) -> Arc<ScannerMetrics> {
    Arc::new(ScannerMetrics::new(table_path))
}

/// Asserts that every entry whose name starts with `fluss.client.scanner.`
/// carries both the `database` and `table` labels matching the expected
/// values. Use after a `Snapshotter::snapshot().into_vec()` to verify all
/// emitted scanner metrics in one shot — protects against future scanner
/// metrics that bypass [`ScannerMetrics`].
pub(crate) fn assert_scanner_entries_labeled(
    entries: &[(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        metrics_util::debugging::DebugValue,
    )],
    expected_database: &str,
    expected_table: &str,
) {
    for (key, _, _, _) in entries {
        let name = key.key().name();
        if !name.starts_with("fluss.client.scanner.") {
            continue;
        }
        let labels: Vec<_> = key
            .key()
            .labels()
            .map(|l| (l.key().to_string(), l.value().to_string()))
            .collect();
        let database = labels
            .iter()
            .find(|(k, _)| k == LABEL_DATABASE)
            .unwrap_or_else(|| {
                panic!("scanner metric `{name}` is missing the database label; labels={labels:?}")
            });
        let table = labels
            .iter()
            .find(|(k, _)| k == LABEL_TABLE)
            .unwrap_or_else(|| {
                panic!("scanner metric `{name}` is missing the table label; labels={labels:?}")
            });
        assert_eq!(
            database.1, expected_database,
            "scanner metric `{name}` has unexpected database label"
        );
        assert_eq!(
            table.1, expected_table,
            "scanner metric `{name}` has unexpected table label"
        );
    }
}

/// Builds an append-only `(id INT, name STRING)` Arrow log batch from `rows`.
/// The writer always emits append-only batches, so changelog tests derive
/// their bytes from this with [`splice_change_type_vector`].
pub(crate) fn build_append_only_batch(rows: &[(i32, &str)]) -> (RowType, Vec<u8>) {
    let row_type = RowType::new(vec![
        DataField::new("id".to_string(), DataTypes::int(), None),
        DataField::new("name".to_string(), DataTypes::string(), None),
    ]);
    let table_path = TablePath::new("db".to_string(), "tbl".to_string());
    let table_info = Arc::new(build_table_info(table_path.clone(), 1, 1));
    let physical_table_path = Arc::new(PhysicalTablePath::of(Arc::new(table_path)));

    let mut builder = MemoryLogRecordsArrowBuilder::new(
        uncompressed_arrow_batch_config(1, &row_type, usize::MAX),
        false,
    )
    .unwrap();

    for (id, name) in rows {
        let mut row = GenericRow::new(2);
        row.set_field(0, *id);
        row.set_field(1, *name);
        let record = WriteRecord::for_append(
            Arc::clone(&table_info),
            physical_table_path.clone(),
            1,
            &row,
        );
        builder.append(&record).unwrap();
    }

    (row_type, builder.build().unwrap())
}

/// Turns a V0 batch into a wire-valid V1 batch by splicing a
/// length-prefixed statistics section before the records data and fixing
/// up the magic, length field and CRC.
pub(crate) fn splice_statistics_section(v0_batch: &[u8], statistics: &[u8]) -> Vec<u8> {
    let mut data = v0_batch.to_vec();
    data[MAGIC_OFFSET] = LOG_MAGIC_VALUE_V1;
    let mut section = (statistics.len() as i32).to_le_bytes().to_vec();
    section.extend_from_slice(statistics);
    data.splice(RECORDS_OFFSET..RECORDS_OFFSET, section);

    let new_length = (data.len() - LOG_OVERHEAD) as i32;
    data[LENGTH_OFFSET..LENGTH_OFFSET + LENGTH_LENGTH].copy_from_slice(&new_length.to_le_bytes());
    let crc = crc32c(&data[SCHEMA_ID_OFFSET..]);
    data[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH].copy_from_slice(&crc.to_le_bytes());
    data
}

/// Turns an append-only batch into a wire-valid changelog batch: clears the
/// append-only flag, splices one change-type byte per record between the
/// header and the Arrow payload, then fixes up the length field and CRC.
pub(crate) fn splice_change_type_vector(
    append_only: &[u8],
    change_types: &[ChangeType],
) -> Vec<u8> {
    let mut data = append_only.to_vec();
    data[ATTRIBUTES_OFFSET] &= !APPEND_ONLY_FLAG_MASK;
    let change_bytes = change_types.iter().map(|ct| ct.to_byte_value());
    data.splice(RECORDS_OFFSET..RECORDS_OFFSET, change_bytes);

    let new_length = (data.len() - LOG_OVERHEAD) as i32;
    data[LENGTH_OFFSET..LENGTH_OFFSET + LENGTH_LENGTH].copy_from_slice(&new_length.to_le_bytes());

    let crc = crc32c(&data[SCHEMA_ID_OFFSET..]);
    data[CRC_OFFSET..CRC_OFFSET + CRC_LENGTH].copy_from_slice(&crc.to_le_bytes());
    data
}
