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

mod types;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use fluss as fcore;
use fluss::PartitionId;
use fluss::client::PrefixKeyLookuper;
use fluss::error::Error;
use fluss::metadata::{Column, DataType, TableInfo};
use fluss::row::{Datum, GenericRow};
use fluss::rpc::FlussError as CoreFlussError;
use fluss::rpc::message::OffsetSpec;

static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
});

#[cxx::bridge(namespace = "fluss::ffi")]
mod ffi {
    struct HashMapValue {
        key: String,
        value: String,
    }

    struct FfiConfig {
        bootstrap_servers: String,
        writer_request_max_size: i32,
        writer_acks: String,
        writer_retries: i32,
        writer_batch_size: i32,
        writer_dynamic_batch_size_enabled: bool,
        writer_dynamic_batch_size_min: i32,
        writer_bucket_no_key_assigner: String,
        scanner_remote_log_prefetch_num: usize,
        remote_file_download_thread_num: usize,
        scanner_remote_log_read_concurrency: usize,
        scanner_log_max_poll_records: usize,
        scanner_log_fetch_max_bytes: i32,
        scanner_log_fetch_min_bytes: i32,
        scanner_log_fetch_wait_max_time_ms: i32,
        scanner_log_fetch_max_bytes_for_bucket: i32,
        writer_batch_timeout_ms: i64,
        writer_enable_idempotence: bool,
        writer_max_inflight_requests_per_bucket: usize,
        writer_buffer_memory_size: usize,
        writer_buffer_wait_timeout_ms: u64,
        connect_timeout_ms: u64,
        security_protocol: String,
        security_sasl_mechanism: String,
        security_sasl_username: String,
        security_sasl_password: String,
        lookup_queue_size: usize,
        lookup_max_batch_size: usize,
        lookup_batch_timeout_ms: u64,
        lookup_max_inflight_requests: usize,
        lookup_max_retries: i32,
    }

    struct FfiResult {
        error_code: i32,
        error_message: String,
    }

    struct FfiTablePath {
        database_name: String,
        table_name: String,
    }

    // One node of a type tree, serialized in preorder. A column's type is the
    // sequence of nodes starting at its root; ARRAY is followed by its element
    // subtree, MAP by its key then value subtrees, ROW by its `child_count`
    // field subtrees (each field node carries its `field_name`). This carries
    // the full recursive type losslessly — exact precision/scale/length,
    // nullability at every level, and ROW field names.
    struct FfiTypeNode {
        type_id: i32,
        nullable: bool,
        // Decimal/Timestamp/TimestampLtz precision, or Char/Binary length; 0 otherwise.
        precision: i32,
        // Decimal scale; 0 otherwise.
        scale: i32,
        // Field name when this node is a ROW field; empty otherwise.
        field_name: String,
        // Immediate children: 0 scalar, 1 ARRAY, 2 MAP, N ROW fields.
        child_count: u32,
    }

    struct FfiColumn {
        name: String,
        comment: String,
        type_nodes: Vec<FfiTypeNode>,
    }

    struct FfiSchema {
        columns: Vec<FfiColumn>,
        primary_keys: Vec<String>,
    }

    struct FfiTableDescriptor {
        schema: FfiSchema,
        partition_keys: Vec<String>,
        bucket_count: i32,
        bucket_keys: Vec<String>,
        properties: Vec<HashMapValue>,
        custom_properties: Vec<HashMapValue>,
        comment: String,
    }

    struct FfiTableInfo {
        table_id: i64,
        schema_id: i32,
        table_path: FfiTablePath,
        created_time: i64,
        modified_time: i64,
        primary_keys: Vec<String>,
        bucket_keys: Vec<String>,
        partition_keys: Vec<String>,
        num_buckets: i32,
        has_primary_key: bool,
        is_partitioned: bool,
        properties: Vec<HashMapValue>,
        custom_properties: Vec<HashMapValue>,
        comment: String,
        schema: FfiSchema,
    }

    struct FfiTableInfoResult {
        result: FfiResult,
        table_info: FfiTableInfo,
    }

    // NOTE: FfiDatum, FfiGenericRow, FfiScanRecord, FfiScanRecords, FfiScanRecordsResult
    // have been replaced by opaque types below (ScanResultInner, GenericRowInner, LookupResultInner).

    struct FfiArrowRecordBatch {
        array_ptr: usize,
        schema_ptr: usize,
        table_id: i64,
        partition_id: i64,
        bucket_id: i32,
        base_offset: i64,
    }

    struct FfiArrowRecordBatches {
        batches: Vec<FfiArrowRecordBatch>,
    }

    struct FfiArrowRecordBatchesResult {
        result: FfiResult,
        arrow_batches: FfiArrowRecordBatches,
    }

    struct FfiLakeSnapshot {
        snapshot_id: i64,
        bucket_offsets: Vec<FfiBucketOffset>,
    }

    struct FfiBucketOffset {
        table_id: i64,
        partition_id: i64,
        bucket_id: i32,
        offset: i64,
    }

    struct FfiOffsetQuery {
        offset_type: i32,
        timestamp: i64,
    }

    struct FfiBucketInfo {
        table_id: i64,
        bucket_id: i32,
        has_partition_id: bool,
        partition_id: i64,
        record_count: usize,
    }

    struct FfiBucketSubscription {
        bucket_id: i32,
        offset: i64,
    }

    struct FfiPartitionBucketSubscription {
        partition_id: i64,
        bucket_id: i32,
        offset: i64,
    }

    struct FfiBucketOffsetPair {
        bucket_id: i32,
        offset: i64,
    }

    struct FfiListOffsetsResult {
        result: FfiResult,
        bucket_offsets: Vec<FfiBucketOffsetPair>,
    }

    // NOTE: FfiLookupResult replaced by opaque LookupResultInner below.

    struct FfiLakeSnapshotResult {
        result: FfiResult,
        lake_snapshot: FfiLakeSnapshot,
    }

    struct FfiPartitionKeyValue {
        key: String,
        value: String,
    }

    struct FfiPartitionInfo {
        partition_id: i64,
        partition_name: String,
    }

    struct FfiListPartitionInfosResult {
        result: FfiResult,
        partition_infos: Vec<FfiPartitionInfo>,
    }

    struct FfiDatabaseDescriptor {
        comment: String,
        properties: Vec<HashMapValue>,
    }

    struct FfiDatabaseInfo {
        database_name: String,
        comment: String,
        properties: Vec<HashMapValue>,
        created_time: i64,
        modified_time: i64,
    }

    struct FfiDatabaseInfoResult {
        result: FfiResult,
        database_info: FfiDatabaseInfo,
    }

    struct FfiListDatabasesResult {
        result: FfiResult,
        database_names: Vec<String>,
    }

    struct FfiListTablesResult {
        result: FfiResult,
        table_names: Vec<String>,
    }

    struct FfiBoolResult {
        result: FfiResult,
        value: bool,
    }

    struct FfiServerNode {
        node_id: i32,
        host: String,
        port: u32,
        server_type: String,
        uid: String,
    }

    struct FfiServerNodesResult {
        result: FfiResult,
        server_nodes: Vec<FfiServerNode>,
    }

    struct FfiPtrResult {
        result: FfiResult,
        ptr: usize,
    }

    extern "Rust" {
        type Connection;
        type Admin;
        type Table;
        type AppendWriter;
        type WriteResult;
        type LogScanner;
        type BatchScanner;
        type UpsertWriter;
        type Lookuper;
        type PrefixLookuper;

        // Opaque types for optimized FFI
        type ScanResultInner;
        type GenericRowInner;
        type LookupResultInner;
        type PrefixLookupResultInner;
        type ArrayWriterInner;
        type MapWriterInner;
        type ValueInner;

        // Connection
        fn new_connection(config: &FfiConfig) -> FfiPtrResult;
        unsafe fn delete_connection(conn: *mut Connection);
        fn get_admin(self: &Connection) -> FfiPtrResult;
        fn get_table(self: &Connection, table_path: &FfiTablePath) -> FfiPtrResult;

        // Admin
        unsafe fn delete_admin(admin: *mut Admin);
        fn create_table(
            self: &Admin,
            table_path: &FfiTablePath,
            descriptor: &FfiTableDescriptor,
            ignore_if_exists: bool,
        ) -> FfiResult;
        fn drop_table(
            self: &Admin,
            table_path: &FfiTablePath,
            ignore_if_not_exists: bool,
        ) -> FfiResult;
        fn get_table_info(self: &Admin, table_path: &FfiTablePath) -> FfiTableInfoResult;
        fn get_latest_lake_snapshot(
            self: &Admin,
            table_path: &FfiTablePath,
        ) -> FfiLakeSnapshotResult;
        fn list_offsets(
            self: &Admin,
            table_path: &FfiTablePath,
            bucket_ids: Vec<i32>,
            offset_query: &FfiOffsetQuery,
        ) -> FfiListOffsetsResult;
        fn list_partition_offsets(
            self: &Admin,
            table_path: &FfiTablePath,
            partition_name: String,
            bucket_ids: Vec<i32>,
            offset_query: &FfiOffsetQuery,
        ) -> FfiListOffsetsResult;
        fn list_partition_infos(
            self: &Admin,
            table_path: &FfiTablePath,
        ) -> FfiListPartitionInfosResult;
        fn list_partition_infos_with_spec(
            self: &Admin,
            table_path: &FfiTablePath,
            partition_spec: Vec<FfiPartitionKeyValue>,
        ) -> FfiListPartitionInfosResult;
        fn create_partition(
            self: &Admin,
            table_path: &FfiTablePath,
            partition_spec: Vec<FfiPartitionKeyValue>,
            ignore_if_exists: bool,
        ) -> FfiResult;
        fn drop_partition(
            self: &Admin,
            table_path: &FfiTablePath,
            partition_spec: Vec<FfiPartitionKeyValue>,
            ignore_if_not_exists: bool,
        ) -> FfiResult;
        fn create_database(
            self: &Admin,
            database_name: &str,
            descriptor: &FfiDatabaseDescriptor,
            ignore_if_exists: bool,
        ) -> FfiResult;
        fn drop_database(
            self: &Admin,
            database_name: &str,
            ignore_if_not_exists: bool,
            cascade: bool,
        ) -> FfiResult;
        fn list_databases(self: &Admin) -> FfiListDatabasesResult;
        fn database_exists(self: &Admin, database_name: &str) -> FfiBoolResult;
        fn get_database_info(self: &Admin, database_name: &str) -> FfiDatabaseInfoResult;
        fn list_tables(self: &Admin, database_name: &str) -> FfiListTablesResult;
        fn table_exists(self: &Admin, table_path: &FfiTablePath) -> FfiBoolResult;
        fn get_server_nodes(self: &Admin) -> FfiServerNodesResult;

        // Table
        unsafe fn delete_table(table: *mut Table);
        fn new_append_writer(self: &Table) -> FfiPtrResult;
        fn create_scanner(self: &Table, column_indices: Vec<usize>, batch: bool) -> FfiPtrResult;
        fn create_bucket_batch_scanner(
            self: &Table,
            column_indices: Vec<usize>,
            limit: i32,
            table_id: i64,
            partition_id: i64,
            bucket_id: i32,
        ) -> FfiPtrResult;
        fn get_table_info_from_table(self: &Table) -> FfiTableInfo;
        fn get_table_path(self: &Table) -> FfiTablePath;
        fn has_primary_key(self: &Table) -> bool;
        fn create_upsert_writer(self: &Table, column_indices: Vec<usize>) -> FfiPtrResult;
        fn new_lookuper(self: &Table) -> FfiPtrResult;
        fn new_prefix_lookuper(self: &Table, lookup_column_names: Vec<String>) -> FfiPtrResult;

        // GenericRowInner — opaque row for writes
        fn new_generic_row(field_count: usize) -> Box<GenericRowInner>;
        fn gr_reset(self: &mut GenericRowInner);
        fn gr_set_null(self: &mut GenericRowInner, idx: usize);
        fn gr_set_bool(self: &mut GenericRowInner, idx: usize, val: bool);
        fn gr_set_i32(self: &mut GenericRowInner, idx: usize, val: i32);
        fn gr_set_i64(self: &mut GenericRowInner, idx: usize, val: i64);
        fn gr_set_f32(self: &mut GenericRowInner, idx: usize, val: f32);
        fn gr_set_f64(self: &mut GenericRowInner, idx: usize, val: f64);
        fn gr_set_str(self: &mut GenericRowInner, idx: usize, val: &str);
        fn gr_set_bytes(self: &mut GenericRowInner, idx: usize, val: &[u8]);
        fn gr_set_date(self: &mut GenericRowInner, idx: usize, days: i32);
        fn gr_set_time(self: &mut GenericRowInner, idx: usize, millis: i32);
        fn gr_set_ts_ntz(self: &mut GenericRowInner, idx: usize, millis: i64, nanos: i32);
        fn gr_set_ts_ltz(self: &mut GenericRowInner, idx: usize, millis: i64, nanos: i32);
        fn gr_set_decimal_str(self: &mut GenericRowInner, idx: usize, val: &str);
        fn gr_set_array(
            self: &mut GenericRowInner,
            idx: usize,
            writer: &mut ArrayWriterInner,
        ) -> Result<()>;
        fn gr_set_map(
            self: &mut GenericRowInner,
            idx: usize,
            writer: &mut MapWriterInner,
        ) -> Result<()>;
        fn gr_set_row(
            self: &mut GenericRowInner,
            idx: usize,
            row: &mut GenericRowInner,
        ) -> Result<()>;

        // ArrayWriterInner — opaque array builder for writes
        fn new_array_writer(
            size: usize,
            element_leaf_type_id: i32,
            precision: u32,
            scale: u32,
            array_nesting: u32,
        ) -> Result<Box<ArrayWriterInner>>;
        // ROW/MAP element types: a one-field Arrow schema (the flat encoding can't carry them).
        fn new_array_writer_arrow(
            size: usize,
            element_schema_ptr: usize,
        ) -> Result<Box<ArrayWriterInner>>;
        fn aw_size(self: &ArrayWriterInner) -> usize;
        fn aw_set_null(self: &mut ArrayWriterInner, idx: usize) -> Result<()>;
        fn aw_set_bool(self: &mut ArrayWriterInner, idx: usize, val: bool) -> Result<()>;
        fn aw_set_i32(self: &mut ArrayWriterInner, idx: usize, val: i32) -> Result<()>;
        fn aw_set_i64(self: &mut ArrayWriterInner, idx: usize, val: i64) -> Result<()>;
        fn aw_set_f32(self: &mut ArrayWriterInner, idx: usize, val: f32) -> Result<()>;
        fn aw_set_f64(self: &mut ArrayWriterInner, idx: usize, val: f64) -> Result<()>;
        fn aw_set_str(self: &mut ArrayWriterInner, idx: usize, val: &str) -> Result<()>;
        fn aw_set_bytes(self: &mut ArrayWriterInner, idx: usize, val: &[u8]) -> Result<()>;
        fn aw_set_date(self: &mut ArrayWriterInner, idx: usize, days: i32) -> Result<()>;
        fn aw_set_time(self: &mut ArrayWriterInner, idx: usize, millis: i32) -> Result<()>;
        fn aw_set_ts_ntz(
            self: &mut ArrayWriterInner,
            idx: usize,
            millis: i64,
            nanos: i32,
        ) -> Result<()>;
        fn aw_set_ts_ltz(
            self: &mut ArrayWriterInner,
            idx: usize,
            millis: i64,
            nanos: i32,
        ) -> Result<()>;
        fn aw_set_decimal_str(self: &mut ArrayWriterInner, idx: usize, val: &str) -> Result<()>;
        fn aw_set_array(
            self: &mut ArrayWriterInner,
            idx: usize,
            nested: &mut ArrayWriterInner,
        ) -> Result<()>;
        fn aw_set_row(
            self: &mut ArrayWriterInner,
            idx: usize,
            row: &mut GenericRowInner,
        ) -> Result<()>;
        fn aw_set_map(
            self: &mut ArrayWriterInner,
            idx: usize,
            map: &mut MapWriterInner,
        ) -> Result<()>;

        // MapWriterInner — opaque map builder for writes.
        #[allow(clippy::too_many_arguments)]
        fn new_map_writer(
            capacity: usize,
            key_leaf_type_id: i32,
            key_precision: u32,
            key_scale: u32,
            value_leaf_type_id: i32,
            value_precision: u32,
            value_scale: u32,
            value_array_nesting: u32,
        ) -> Result<Box<MapWriterInner>>;
        // ROW/MAP key/value types: a [key, value] Arrow schema (the flat encoding can't).
        fn new_map_writer_arrow(
            capacity: usize,
            kv_schema_ptr: usize,
        ) -> Result<Box<MapWriterInner>>;
        fn mw_key_bool(self: &mut MapWriterInner, val: bool) -> Result<()>;
        fn mw_key_i32(self: &mut MapWriterInner, val: i32) -> Result<()>;
        fn mw_key_i64(self: &mut MapWriterInner, val: i64) -> Result<()>;
        fn mw_key_f32(self: &mut MapWriterInner, val: f32) -> Result<()>;
        fn mw_key_f64(self: &mut MapWriterInner, val: f64) -> Result<()>;
        fn mw_key_str(self: &mut MapWriterInner, val: &str) -> Result<()>;
        fn mw_key_bytes(self: &mut MapWriterInner, val: &[u8]) -> Result<()>;
        fn mw_key_date(self: &mut MapWriterInner, days: i32) -> Result<()>;
        fn mw_key_time(self: &mut MapWriterInner, millis: i32) -> Result<()>;
        fn mw_key_timestamp(self: &mut MapWriterInner, millis: i64, nanos: i32) -> Result<()>;
        fn mw_key_decimal_str(self: &mut MapWriterInner, val: &str) -> Result<()>;
        fn mw_value_null(self: &mut MapWriterInner) -> Result<()>;
        fn mw_value_bool(self: &mut MapWriterInner, val: bool) -> Result<()>;
        fn mw_value_i32(self: &mut MapWriterInner, val: i32) -> Result<()>;
        fn mw_value_i64(self: &mut MapWriterInner, val: i64) -> Result<()>;
        fn mw_value_f32(self: &mut MapWriterInner, val: f32) -> Result<()>;
        fn mw_value_f64(self: &mut MapWriterInner, val: f64) -> Result<()>;
        fn mw_value_str(self: &mut MapWriterInner, val: &str) -> Result<()>;
        fn mw_value_bytes(self: &mut MapWriterInner, val: &[u8]) -> Result<()>;
        fn mw_value_date(self: &mut MapWriterInner, days: i32) -> Result<()>;
        fn mw_value_time(self: &mut MapWriterInner, millis: i32) -> Result<()>;
        fn mw_value_timestamp(self: &mut MapWriterInner, millis: i64, nanos: i32) -> Result<()>;
        fn mw_value_decimal_str(self: &mut MapWriterInner, val: &str) -> Result<()>;
        fn mw_value_row(self: &mut MapWriterInner, row: &mut GenericRowInner) -> Result<()>;
        fn mw_value_map(self: &mut MapWriterInner, map: &mut MapWriterInner) -> Result<()>;
        fn mw_value_array(self: &mut MapWriterInner, array: &mut ArrayWriterInner) -> Result<()>;
        fn mw_commit(self: &mut MapWriterInner) -> Result<()>;

        // AppendWriter
        unsafe fn delete_append_writer(writer: *mut AppendWriter);
        fn append(self: &mut AppendWriter, row: &GenericRowInner) -> FfiPtrResult;
        fn append_arrow_batch(
            self: &mut AppendWriter,
            array_ptr: usize,
            schema_ptr: usize,
        ) -> FfiPtrResult;
        fn flush(self: &mut AppendWriter) -> FfiResult;

        // WriteResult
        unsafe fn delete_write_result(wr: *mut WriteResult);
        fn wait(self: &mut WriteResult) -> FfiResult;

        // UpsertWriter
        unsafe fn delete_upsert_writer(writer: *mut UpsertWriter);
        fn upsert(self: &mut UpsertWriter, row: &GenericRowInner) -> FfiPtrResult;
        fn delete_row(self: &mut UpsertWriter, row: &GenericRowInner) -> FfiPtrResult;
        fn upsert_flush(self: &mut UpsertWriter) -> FfiResult;

        // Lookuper
        unsafe fn delete_lookuper(lookuper: *mut Lookuper);
        fn lookup(self: &mut Lookuper, pk_row: &GenericRowInner) -> Box<LookupResultInner>;

        // LookupResultInner accessors
        fn lv_has_error(self: &LookupResultInner) -> bool;
        fn lv_error_code(self: &LookupResultInner) -> i32;
        fn lv_error_message(self: &LookupResultInner) -> &str;
        fn lv_found(self: &LookupResultInner) -> bool;
        fn lv_field_count(self: &LookupResultInner) -> usize;
        fn lv_column_name(self: &LookupResultInner, field: usize) -> Result<&str>;
        fn lv_column_type(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_is_null(self: &LookupResultInner, field: usize) -> Result<bool>;
        fn lv_get_bool(self: &LookupResultInner, field: usize) -> Result<bool>;
        fn lv_get_i32(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_get_i64(self: &LookupResultInner, field: usize) -> Result<i64>;
        fn lv_get_f32(self: &LookupResultInner, field: usize) -> Result<f32>;
        fn lv_get_f64(self: &LookupResultInner, field: usize) -> Result<f64>;
        fn lv_get_str(self: &LookupResultInner, field: usize) -> Result<&str>;
        fn lv_get_bytes(self: &LookupResultInner, field: usize) -> Result<&[u8]>;
        fn lv_get_date_days(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_get_time_millis(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_get_ts_millis(self: &LookupResultInner, field: usize) -> Result<i64>;
        fn lv_get_ts_nanos(self: &LookupResultInner, field: usize) -> Result<i32>;
        fn lv_is_ts_ltz(self: &LookupResultInner, field: usize) -> Result<bool>;
        fn lv_get_decimal_str(self: &LookupResultInner, field: usize) -> Result<String>;

        // ValueInner — recursive value handle for complex column reads.
        fn lv_get_value(self: &LookupResultInner, field: usize) -> Result<Box<ValueInner>>;
        fn sv_get_value(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<Box<ValueInner>>;
        fn v_type(self: &ValueInner) -> i32;
        fn v_is_null(self: &ValueInner) -> bool;
        fn v_get_bool(self: &ValueInner) -> Result<bool>;
        fn v_get_i32(self: &ValueInner) -> Result<i32>;
        fn v_get_i64(self: &ValueInner) -> Result<i64>;
        fn v_get_f32(self: &ValueInner) -> Result<f32>;
        fn v_get_f64(self: &ValueInner) -> Result<f64>;
        fn v_get_str(self: &ValueInner) -> Result<String>;
        fn v_get_bytes(self: &ValueInner) -> Result<Vec<u8>>;
        fn v_get_date_days(self: &ValueInner) -> Result<i32>;
        fn v_get_time_millis(self: &ValueInner) -> Result<i32>;
        fn v_get_ts_millis(self: &ValueInner) -> Result<i64>;
        fn v_get_ts_nanos(self: &ValueInner) -> Result<i32>;
        fn v_get_decimal_str(self: &ValueInner) -> Result<String>;
        fn v_size(self: &ValueInner) -> Result<usize>;
        fn v_field_count(self: &ValueInner) -> Result<usize>;
        fn v_at(self: &ValueInner, i: usize) -> Result<Box<ValueInner>>;
        fn v_key_at(self: &ValueInner, i: usize) -> Result<Box<ValueInner>>;
        fn v_value_at(self: &ValueInner, i: usize) -> Result<Box<ValueInner>>;
        fn v_field(self: &ValueInner, i: usize) -> Result<Box<ValueInner>>;
        fn v_field_by_name(self: &ValueInner, name: &str) -> Result<Box<ValueInner>>;

        // PrefixLookuper
        unsafe fn delete_prefix_lookuper(lookuper: *mut PrefixLookuper);
        fn prefix_lookup(
            self: &mut PrefixLookuper,
            prefix_row: &GenericRowInner,
        ) -> Box<PrefixLookupResultInner>;

        // PrefixLookupResultInner accessors — like LookupResultInner but indexed
        // by record, since a prefix lookup returns zero-or-more rows.
        fn plv_has_error(self: &PrefixLookupResultInner) -> bool;
        fn plv_error_code(self: &PrefixLookupResultInner) -> i32;
        fn plv_error_message(self: &PrefixLookupResultInner) -> &str;
        fn plv_row_count(self: &PrefixLookupResultInner) -> usize;
        fn plv_field_count(self: &PrefixLookupResultInner) -> usize;
        fn plv_column_name(self: &PrefixLookupResultInner, field: usize) -> Result<&str>;
        fn plv_column_type(self: &PrefixLookupResultInner, field: usize) -> Result<i32>;
        fn plv_is_null(self: &PrefixLookupResultInner, rec: usize, field: usize) -> Result<bool>;
        fn plv_get_bool(self: &PrefixLookupResultInner, rec: usize, field: usize) -> Result<bool>;
        fn plv_get_i32(self: &PrefixLookupResultInner, rec: usize, field: usize) -> Result<i32>;
        fn plv_get_i64(self: &PrefixLookupResultInner, rec: usize, field: usize) -> Result<i64>;
        fn plv_get_f32(self: &PrefixLookupResultInner, rec: usize, field: usize) -> Result<f32>;
        fn plv_get_f64(self: &PrefixLookupResultInner, rec: usize, field: usize) -> Result<f64>;
        fn plv_get_str(self: &PrefixLookupResultInner, rec: usize, field: usize) -> Result<&str>;
        fn plv_get_bytes(self: &PrefixLookupResultInner, rec: usize, field: usize)
        -> Result<&[u8]>;
        fn plv_get_date_days(
            self: &PrefixLookupResultInner,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn plv_get_time_millis(
            self: &PrefixLookupResultInner,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn plv_get_ts_millis(
            self: &PrefixLookupResultInner,
            rec: usize,
            field: usize,
        ) -> Result<i64>;
        fn plv_get_ts_nanos(
            self: &PrefixLookupResultInner,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn plv_is_ts_ltz(self: &PrefixLookupResultInner, rec: usize, field: usize) -> Result<bool>;
        fn plv_get_decimal_str(
            self: &PrefixLookupResultInner,
            rec: usize,
            field: usize,
        ) -> Result<String>;

        fn plv_get_value(
            self: &PrefixLookupResultInner,
            rec: usize,
            field: usize,
        ) -> Result<Box<ValueInner>>;

        // LogScanner
        unsafe fn delete_log_scanner(scanner: *mut LogScanner);
        fn subscribe(self: &LogScanner, bucket_id: i32, start_offset: i64) -> FfiResult;
        fn subscribe_buckets(
            self: &LogScanner,
            subscriptions: Vec<FfiBucketSubscription>,
        ) -> FfiResult;
        fn subscribe_partition(
            self: &LogScanner,
            partition_id: i64,
            bucket_id: i32,
            start_offset: i64,
        ) -> FfiResult;
        fn subscribe_partition_buckets(
            self: &LogScanner,
            subscriptions: Vec<FfiPartitionBucketSubscription>,
        ) -> FfiResult;
        fn unsubscribe(self: &LogScanner, bucket_id: i32) -> FfiResult;
        fn unsubscribe_partition(self: &LogScanner, partition_id: i64, bucket_id: i32)
        -> FfiResult;
        fn poll(self: &LogScanner, timeout_ms: i64) -> Box<ScanResultInner>;
        fn poll_record_batch(self: &LogScanner, timeout_ms: i64) -> FfiArrowRecordBatchesResult;
        fn free_arrow_ffi_structures(array_ptr: usize, schema_ptr: usize);

        // BatchScanner
        unsafe fn delete_batch_scanner(scanner: *mut BatchScanner);
        fn next_batch(self: &BatchScanner) -> FfiArrowRecordBatchesResult;
        fn collect_all_batches(self: &BatchScanner) -> FfiArrowRecordBatchesResult;

        // ScanResultInner accessors
        fn sv_has_error(self: &ScanResultInner) -> bool;
        fn sv_error_code(self: &ScanResultInner) -> i32;
        fn sv_error_message(self: &ScanResultInner) -> &str;
        fn sv_record_count(self: &ScanResultInner) -> usize;
        fn sv_column_count(self: &ScanResultInner) -> usize;
        fn sv_column_name(self: &ScanResultInner, field: usize) -> Result<&str>;
        fn sv_column_type(self: &ScanResultInner, field: usize) -> Result<i32>;
        fn sv_offset(self: &ScanResultInner, bucket: usize, rec: usize) -> i64;
        fn sv_timestamp(self: &ScanResultInner, bucket: usize, rec: usize) -> i64;
        fn sv_change_type(self: &ScanResultInner, bucket: usize, rec: usize) -> i32;
        fn sv_field_count(self: &ScanResultInner) -> usize;
        fn sv_is_null(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<bool>;
        fn sv_get_bool(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<bool>;
        fn sv_get_i32(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn sv_get_i64(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i64>;
        fn sv_get_f32(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<f32>;
        fn sv_get_f64(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<f64>;
        fn sv_get_str(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<&str>;
        fn sv_get_bytes(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<&[u8]>;
        fn sv_get_date_days(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn sv_get_time_millis(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn sv_get_ts_millis(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i64>;
        fn sv_get_ts_nanos(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<i32>;
        fn sv_is_ts_ltz(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<bool>;
        fn sv_get_decimal_str(
            self: &ScanResultInner,
            bucket: usize,
            rec: usize,
            field: usize,
        ) -> Result<String>;

        fn sv_bucket_infos(self: &ScanResultInner) -> &Vec<FfiBucketInfo>;
    }
}

pub struct Connection {
    inner: Arc<fcore::client::FlussConnection>,
}

pub struct Admin {
    inner: Arc<fcore::client::FlussAdmin>,
}

pub struct Table {
    connection: Arc<fcore::client::FlussConnection>,
    metadata: Arc<fcore::client::Metadata>,
    table_info: fcore::metadata::TableInfo,
    table_path: fcore::metadata::TablePath,
    has_pk: bool,
}

pub struct AppendWriter {
    inner: fcore::client::AppendWriter,
    table_info: fcore::metadata::TableInfo,
}

pub struct WriteResult {
    inner: Option<fcore::client::WriteResultFuture>,
}

enum ScannerKind {
    Record(fcore::client::LogScanner),
    Batch(fcore::client::RecordBatchLogScanner),
}

pub struct LogScanner {
    scanner: ScannerKind,
    /// Fluss columns matching the projected Arrow fields (1:1 by index).
    /// For non-projected scanners this is the full table schema columns.
    projected_columns: Vec<fcore::metadata::Column>,
}

pub struct BatchScanner {
    inner: Mutex<fcore::client::LimitBatchScanner>,
}

pub struct UpsertWriter {
    inner: fcore::client::UpsertWriter,
    table_info: fcore::metadata::TableInfo,
}

pub struct Lookuper {
    inner: fcore::client::Lookuper,
    table_info: fcore::metadata::TableInfo,
}

pub struct PrefixLookuper {
    inner: PrefixKeyLookuper,
    table_info: TableInfo,
    /// Full-schema indices of the lookup columns, used to compact the input row.
    lookup_column_indices: Vec<usize>,
}

/// Error code for client-side errors that did not originate from the server API protocol.
/// Must be non-zero so that CPP `Result::Ok()` (which checks `error_code == 0`) correctly
/// detects client-side errors as failures. The value -2 is outside the server API error
/// code range (-1 .. 57+), so it will never collide with current or future API codes.
const CLIENT_ERROR_CODE: i32 = -2;

fn ok_result() -> ffi::FfiResult {
    ffi::FfiResult {
        error_code: 0,
        error_message: String::new(),
    }
}

fn err_result(code: i32, msg: String) -> ffi::FfiResult {
    ffi::FfiResult {
        error_code: code,
        error_message: msg,
    }
}

/// Create a client-side error result (not from server API).
fn client_err(msg: String) -> ffi::FfiResult {
    err_result(CLIENT_ERROR_CODE, msg)
}

fn err_from_core_error(e: &Error) -> ffi::FfiResult {
    // Transport failures map to `NetworkException` (Java parity,
    // retriable).
    match e {
        Error::FlussAPIError { api_error } => err_result(api_error.code, api_error.message.clone()),
        Error::RpcError { .. } => {
            err_result(CoreFlussError::NetworkException.code(), e.to_string())
        }
        _ => client_err(e.to_string()),
    }
}

fn ok_ptr(ptr: usize) -> ffi::FfiPtrResult {
    ffi::FfiPtrResult {
        result: ok_result(),
        ptr,
    }
}

fn client_err_ptr(msg: String) -> ffi::FfiPtrResult {
    ffi::FfiPtrResult {
        result: client_err(msg),
        ptr: 0usize,
    }
}

fn err_ptr_from_core(e: &fcore::error::Error) -> ffi::FfiPtrResult {
    ffi::FfiPtrResult {
        result: err_from_core_error(e),
        ptr: 0usize,
    }
}

fn empty_arrow_batches_result(result: ffi::FfiResult) -> ffi::FfiArrowRecordBatchesResult {
    ffi::FfiArrowRecordBatchesResult {
        result,
        arrow_batches: ffi::FfiArrowRecordBatches { batches: vec![] },
    }
}

/// Wrap the result of `core_scan_batches_to_ffi` in an `FfiArrowRecordBatchesResult`.
fn arrow_batches_result(
    converted: Result<ffi::FfiArrowRecordBatches, String>,
) -> ffi::FfiArrowRecordBatchesResult {
    match converted {
        Ok(arrow_batches) => ffi::FfiArrowRecordBatchesResult {
            result: ok_result(),
            arrow_batches,
        },
        Err(e) => empty_arrow_batches_result(client_err(e)),
    }
}

// Connection implementation
fn new_connection(config: &ffi::FfiConfig) -> ffi::FfiPtrResult {
    let assigner_type = match config
        .writer_bucket_no_key_assigner
        .parse::<fluss::config::NoKeyAssigner>()
    {
        Ok(v) => v,
        Err(e) => return client_err_ptr(format!("Invalid bucket assigner type: {e}")),
    };
    let config_core = fluss::config::Config {
        bootstrap_servers: config.bootstrap_servers.to_string(),
        writer_request_max_size: config.writer_request_max_size,
        writer_acks: config.writer_acks.to_string(),
        writer_retries: config.writer_retries,
        writer_batch_size: config.writer_batch_size,
        writer_dynamic_batch_size_enabled: config.writer_dynamic_batch_size_enabled,
        writer_dynamic_batch_size_min: config.writer_dynamic_batch_size_min,
        writer_batch_timeout_ms: config.writer_batch_timeout_ms,
        writer_bucket_no_key_assigner: assigner_type,
        scanner_remote_log_prefetch_num: config.scanner_remote_log_prefetch_num,
        remote_file_download_thread_num: config.remote_file_download_thread_num,
        scanner_remote_log_read_concurrency: config.scanner_remote_log_read_concurrency,
        scanner_log_max_poll_records: config.scanner_log_max_poll_records,
        scanner_log_fetch_max_bytes: config.scanner_log_fetch_max_bytes,
        scanner_log_fetch_min_bytes: config.scanner_log_fetch_min_bytes,
        scanner_log_fetch_wait_max_time_ms: config.scanner_log_fetch_wait_max_time_ms,
        scanner_log_fetch_max_bytes_for_bucket: config.scanner_log_fetch_max_bytes_for_bucket,
        writer_enable_idempotence: config.writer_enable_idempotence,
        writer_max_inflight_requests_per_bucket: config.writer_max_inflight_requests_per_bucket,
        writer_buffer_memory_size: config.writer_buffer_memory_size,
        writer_buffer_wait_timeout_ms: config.writer_buffer_wait_timeout_ms,
        connect_timeout_ms: config.connect_timeout_ms,
        security_protocol: config.security_protocol.to_string(),
        security_sasl_mechanism: config.security_sasl_mechanism.to_string(),
        security_sasl_username: config.security_sasl_username.to_string(),
        security_sasl_password: config.security_sasl_password.to_string(),
        lookup_queue_size: config.lookup_queue_size,
        lookup_max_batch_size: config.lookup_max_batch_size,
        lookup_batch_timeout_ms: config.lookup_batch_timeout_ms,
        lookup_max_inflight_requests: config.lookup_max_inflight_requests,
        lookup_max_retries: config.lookup_max_retries,
    };

    let conn = RUNTIME.block_on(async { fcore::client::FlussConnection::new(config_core).await });

    match conn {
        Ok(c) => {
            let ptr = Box::into_raw(Box::new(Connection { inner: Arc::new(c) }));
            ok_ptr(ptr as usize)
        }
        Err(e) => err_ptr_from_core(&e),
    }
}

unsafe fn delete_connection(conn: *mut Connection) {
    if !conn.is_null() {
        unsafe {
            drop(Box::from_raw(conn));
        }
    }
}

impl Connection {
    fn get_admin(&self) -> ffi::FfiPtrResult {
        let admin_result = self.inner.get_admin();

        match admin_result {
            Ok(admin) => {
                let ptr = Box::into_raw(Box::new(Admin { inner: admin }));
                ok_ptr(ptr as usize)
            }
            Err(e) => err_ptr_from_core(&e),
        }
    }

    fn get_table(&self, table_path: &ffi::FfiTablePath) -> ffi::FfiPtrResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let table_result = RUNTIME.block_on(async { self.inner.get_table(&path).await });

        match table_result {
            Ok(t) => {
                let ptr = Box::into_raw(Box::new(Table {
                    connection: self.inner.clone(),
                    metadata: t.metadata().clone(),
                    table_info: t.get_table_info().clone(),
                    table_path: t.table_path().clone(),
                    has_pk: t.has_primary_key(),
                }));
                ok_ptr(ptr as usize)
            }
            Err(e) => err_ptr_from_core(&e),
        }
    }
}

// Admin implementation
unsafe fn delete_admin(admin: *mut Admin) {
    if !admin.is_null() {
        unsafe {
            drop(Box::from_raw(admin));
        }
    }
}

impl Admin {
    fn create_table(
        &self,
        table_path: &ffi::FfiTablePath,
        descriptor: &ffi::FfiTableDescriptor,
        ignore_if_exists: bool,
    ) -> ffi::FfiResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let core_descriptor = match types::ffi_descriptor_to_core(descriptor) {
            Ok(d) => d,
            Err(e) => return client_err(e.to_string()),
        };

        let result = RUNTIME.block_on(async {
            self.inner
                .create_table(&path, &core_descriptor, ignore_if_exists)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn drop_table(
        &self,
        table_path: &ffi::FfiTablePath,
        ignore_if_not_exists: bool,
    ) -> ffi::FfiResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let result =
            RUNTIME.block_on(async { self.inner.drop_table(&path, ignore_if_not_exists).await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn get_table_info(&self, table_path: &ffi::FfiTablePath) -> ffi::FfiTableInfoResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let result = RUNTIME.block_on(async { self.inner.get_table_info(&path).await });

        match result {
            Ok(info) => ffi::FfiTableInfoResult {
                result: ok_result(),
                table_info: types::core_table_info_to_ffi(&info),
            },
            Err(e) => ffi::FfiTableInfoResult {
                result: err_from_core_error(&e),
                table_info: types::empty_table_info(),
            },
        }
    }

    fn get_latest_lake_snapshot(
        &self,
        table_path: &ffi::FfiTablePath,
    ) -> ffi::FfiLakeSnapshotResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let result = RUNTIME.block_on(async { self.inner.get_latest_lake_snapshot(&path).await });

        match result {
            Ok(snapshot) => ffi::FfiLakeSnapshotResult {
                result: ok_result(),
                lake_snapshot: types::core_lake_snapshot_to_ffi(&snapshot),
            },
            Err(e) => ffi::FfiLakeSnapshotResult {
                result: err_from_core_error(&e),
                lake_snapshot: ffi::FfiLakeSnapshot {
                    snapshot_id: -1,
                    bucket_offsets: vec![],
                },
            },
        }
    }

    // Helper function for common list offsets functionality
    fn do_list_offsets(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_name: Option<&str>,
        bucket_ids: Vec<i32>,
        offset_query: &ffi::FfiOffsetQuery,
    ) -> ffi::FfiListOffsetsResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let offset_spec = match offset_query.offset_type {
            0 => OffsetSpec::Earliest,
            1 => OffsetSpec::Latest,
            2 => OffsetSpec::Timestamp(offset_query.timestamp),
            _ => {
                return ffi::FfiListOffsetsResult {
                    result: client_err(format!(
                        "Invalid offset_type: {}",
                        offset_query.offset_type
                    )),
                    bucket_offsets: vec![],
                };
            }
        };

        let result = RUNTIME.block_on(async {
            if let Some(part_name) = partition_name {
                self.inner
                    .list_partition_offsets(&path, part_name, &bucket_ids, offset_spec)
                    .await
            } else {
                self.inner
                    .list_offsets(&path, &bucket_ids, offset_spec)
                    .await
            }
        });

        match result {
            Ok(offsets) => {
                let bucket_offsets: Vec<ffi::FfiBucketOffsetPair> = offsets
                    .into_iter()
                    .map(|(bucket_id, offset)| ffi::FfiBucketOffsetPair { bucket_id, offset })
                    .collect();
                ffi::FfiListOffsetsResult {
                    result: ok_result(),
                    bucket_offsets,
                }
            }
            Err(e) => ffi::FfiListOffsetsResult {
                result: err_from_core_error(&e),
                bucket_offsets: vec![],
            },
        }
    }

    fn list_offsets(
        &self,
        table_path: &ffi::FfiTablePath,
        bucket_ids: Vec<i32>,
        offset_query: &ffi::FfiOffsetQuery,
    ) -> ffi::FfiListOffsetsResult {
        self.do_list_offsets(table_path, None, bucket_ids, offset_query)
    }

    fn list_partition_offsets(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_name: String,
        bucket_ids: Vec<i32>,
        offset_query: &ffi::FfiOffsetQuery,
    ) -> ffi::FfiListOffsetsResult {
        self.do_list_offsets(table_path, Some(&partition_name), bucket_ids, offset_query)
    }

    fn list_partition_infos(
        &self,
        table_path: &ffi::FfiTablePath,
    ) -> ffi::FfiListPartitionInfosResult {
        self.do_list_partition_infos(table_path, None)
    }

    fn list_partition_infos_with_spec(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_spec: Vec<ffi::FfiPartitionKeyValue>,
    ) -> ffi::FfiListPartitionInfosResult {
        let spec_map: std::collections::HashMap<String, String> = partition_spec
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();
        let spec = fcore::metadata::PartitionSpec::new(spec_map);
        self.do_list_partition_infos(table_path, Some(&spec))
    }
    fn create_partition(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_spec: Vec<ffi::FfiPartitionKeyValue>,
        ignore_if_exists: bool,
    ) -> ffi::FfiResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );
        let spec_map: std::collections::HashMap<String, String> = partition_spec
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();
        let partition_spec = fcore::metadata::PartitionSpec::new(spec_map);

        let result = RUNTIME.block_on(async {
            self.inner
                .create_partition(&path, &partition_spec, ignore_if_exists)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn drop_partition(
        &self,
        table_path: &ffi::FfiTablePath,
        partition_spec: Vec<ffi::FfiPartitionKeyValue>,
        ignore_if_not_exists: bool,
    ) -> ffi::FfiResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );
        let spec_map: std::collections::HashMap<String, String> = partition_spec
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect();
        let partition_spec = fcore::metadata::PartitionSpec::new(spec_map);

        let result = RUNTIME.block_on(async {
            self.inner
                .drop_partition(&path, &partition_spec, ignore_if_not_exists)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn create_database(
        &self,
        database_name: &str,
        descriptor: &ffi::FfiDatabaseDescriptor,
        ignore_if_exists: bool,
    ) -> ffi::FfiResult {
        let descriptor_opt = types::ffi_database_descriptor_to_core(descriptor);

        let result = RUNTIME.block_on(async {
            self.inner
                .create_database(database_name, descriptor_opt.as_ref(), ignore_if_exists)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn drop_database(
        &self,
        database_name: &str,
        ignore_if_not_exists: bool,
        cascade: bool,
    ) -> ffi::FfiResult {
        let result = RUNTIME.block_on(async {
            self.inner
                .drop_database(database_name, ignore_if_not_exists, cascade)
                .await
        });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }

    fn list_databases(&self) -> ffi::FfiListDatabasesResult {
        let result = RUNTIME.block_on(async { self.inner.list_databases().await });

        match result {
            Ok(names) => ffi::FfiListDatabasesResult {
                result: ok_result(),
                database_names: names,
            },
            Err(e) => ffi::FfiListDatabasesResult {
                result: err_from_core_error(&e),
                database_names: vec![],
            },
        }
    }

    fn database_exists(&self, database_name: &str) -> ffi::FfiBoolResult {
        let result = RUNTIME.block_on(async { self.inner.database_exists(database_name).await });

        match result {
            Ok(exists) => ffi::FfiBoolResult {
                result: ok_result(),
                value: exists,
            },
            Err(e) => ffi::FfiBoolResult {
                result: err_from_core_error(&e),
                value: false,
            },
        }
    }

    fn get_database_info(&self, database_name: &str) -> ffi::FfiDatabaseInfoResult {
        let result = RUNTIME.block_on(async { self.inner.get_database_info(database_name).await });

        match result {
            Ok(info) => ffi::FfiDatabaseInfoResult {
                result: ok_result(),
                database_info: types::core_database_info_to_ffi(&info),
            },
            Err(e) => ffi::FfiDatabaseInfoResult {
                result: err_from_core_error(&e),
                database_info: ffi::FfiDatabaseInfo {
                    database_name: String::new(),
                    comment: String::new(),
                    properties: vec![],
                    created_time: 0,
                    modified_time: 0,
                },
            },
        }
    }

    fn list_tables(&self, database_name: &str) -> ffi::FfiListTablesResult {
        let result = RUNTIME.block_on(async { self.inner.list_tables(database_name).await });

        match result {
            Ok(names) => ffi::FfiListTablesResult {
                result: ok_result(),
                table_names: names,
            },
            Err(e) => ffi::FfiListTablesResult {
                result: err_from_core_error(&e),
                table_names: vec![],
            },
        }
    }

    fn table_exists(&self, table_path: &ffi::FfiTablePath) -> ffi::FfiBoolResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );

        let result = RUNTIME.block_on(async { self.inner.table_exists(&path).await });

        match result {
            Ok(exists) => ffi::FfiBoolResult {
                result: ok_result(),
                value: exists,
            },
            Err(e) => ffi::FfiBoolResult {
                result: err_from_core_error(&e),
                value: false,
            },
        }
    }

    fn do_list_partition_infos(
        &self,
        table_path: &ffi::FfiTablePath,
        partial_partition_spec: Option<&fcore::metadata::PartitionSpec>,
    ) -> ffi::FfiListPartitionInfosResult {
        let path = fcore::metadata::TablePath::new(
            table_path.database_name.clone(),
            table_path.table_name.clone(),
        );
        let result = RUNTIME.block_on(async {
            self.inner
                .list_partition_infos_with_spec(&path, partial_partition_spec)
                .await
        });
        match result {
            Ok(infos) => {
                let partition_infos: Vec<ffi::FfiPartitionInfo> = infos
                    .into_iter()
                    .map(|info| ffi::FfiPartitionInfo {
                        partition_id: info.get_partition_id(),
                        partition_name: info.get_partition_name(),
                    })
                    .collect();
                ffi::FfiListPartitionInfosResult {
                    result: ok_result(),
                    partition_infos,
                }
            }
            Err(e) => ffi::FfiListPartitionInfosResult {
                result: err_from_core_error(&e),
                partition_infos: vec![],
            },
        }
    }

    fn get_server_nodes(&self) -> ffi::FfiServerNodesResult {
        let result = RUNTIME.block_on(async { self.inner.get_server_nodes().await });

        match result {
            Ok(nodes) => {
                let server_nodes: Vec<ffi::FfiServerNode> = nodes
                    .into_iter()
                    .map(|node| ffi::FfiServerNode {
                        node_id: node.id(),
                        host: node.host().to_string(),
                        port: node.port(),
                        server_type: node.server_type().to_string(),
                        uid: node.uid().to_string(),
                    })
                    .collect();
                ffi::FfiServerNodesResult {
                    result: ok_result(),
                    server_nodes,
                }
            }
            Err(e) => ffi::FfiServerNodesResult {
                result: err_from_core_error(&e),
                server_nodes: vec![],
            },
        }
    }
}

// Table implementation
unsafe fn delete_table(table: *mut Table) {
    if !table.is_null() {
        unsafe {
            drop(Box::from_raw(table));
        }
    }
}

impl Table {
    fn fluss_table(&self) -> fcore::client::FlussTable<'_> {
        fcore::client::FlussTable::new(
            &self.connection,
            self.metadata.clone(),
            self.table_info.clone(),
        )
    }

    fn resolve_projected_columns(
        &self,
        indices: &[usize],
    ) -> Result<Vec<fcore::metadata::Column>, String> {
        let all_columns = self.table_info.get_schema().columns();
        indices
            .iter()
            .map(|&i| {
                all_columns.get(i).cloned().ok_or_else(|| {
                    format!(
                        "Invalid column index {i}: schema has {} columns",
                        all_columns.len()
                    )
                })
            })
            .collect()
    }

    fn new_append_writer(&self) -> ffi::FfiPtrResult {
        let _enter = RUNTIME.enter();

        let table_append = match self.fluss_table().new_append() {
            Ok(a) => a,
            Err(e) => return err_ptr_from_core(&e),
        };

        let writer = match table_append.create_writer() {
            Ok(w) => w,
            Err(e) => return err_ptr_from_core(&e),
        };

        let ptr = Box::into_raw(Box::new(AppendWriter {
            inner: writer,
            table_info: self.table_info.clone(),
        }));
        ok_ptr(ptr as usize)
    }

    fn create_scanner(&self, column_indices: Vec<usize>, batch: bool) -> ffi::FfiPtrResult {
        RUNTIME.block_on(async {
            let fluss_table = self.fluss_table();
            let scan = fluss_table.new_scan();

            let (projected_columns, scan) = if column_indices.is_empty() {
                (self.table_info.get_schema().columns().to_vec(), scan)
            } else {
                let cols = match self.resolve_projected_columns(&column_indices) {
                    Ok(c) => c,
                    Err(e) => return client_err_ptr(e),
                };
                let scan = match scan.project(&column_indices) {
                    Ok(s) => s,
                    Err(e) => return err_ptr_from_core(&e),
                };
                (cols, scan)
            };

            let scanner = if batch {
                match scan.create_record_batch_log_scanner() {
                    Ok(s) => ScannerKind::Batch(s),
                    Err(e) => return err_ptr_from_core(&e),
                }
            } else {
                match scan.create_log_scanner() {
                    Ok(s) => ScannerKind::Record(s),
                    Err(e) => return err_ptr_from_core(&e),
                }
            };

            let ptr = Box::into_raw(Box::new(LogScanner {
                scanner,
                projected_columns,
            }));
            ok_ptr(ptr as usize)
        })
    }

    fn create_bucket_batch_scanner(
        &self,
        column_indices: Vec<usize>,
        limit: i32,
        table_id: i64,
        partition_id: i64,
        bucket_id: i32,
    ) -> ffi::FfiPtrResult {
        RUNTIME.block_on(async {
            let fluss_table = self.fluss_table();
            let scan = fluss_table.new_scan();

            let scan = if column_indices.is_empty() {
                scan
            } else {
                match scan.project(&column_indices) {
                    Ok(s) => s,
                    Err(e) => return err_ptr_from_core(&e),
                }
            };

            let scan = match scan.limit(limit) {
                Ok(s) => s,
                Err(e) => return err_ptr_from_core(&e),
            };

            // A negative partition id encodes "no partition" across the FFI boundary.
            let partition = (partition_id >= 0).then_some(partition_id);
            let bucket =
                fcore::metadata::TableBucket::new_with_partition(table_id, partition, bucket_id);

            let scanner = match scan.create_bucket_batch_scanner(bucket) {
                Ok(s) => s,
                Err(e) => return err_ptr_from_core(&e),
            };

            let ptr = Box::into_raw(Box::new(BatchScanner {
                inner: Mutex::new(scanner),
            }));
            ok_ptr(ptr as usize)
        })
    }

    fn get_table_info_from_table(&self) -> ffi::FfiTableInfo {
        types::core_table_info_to_ffi(&self.table_info)
    }

    fn get_table_path(&self) -> ffi::FfiTablePath {
        ffi::FfiTablePath {
            database_name: self.table_path.database().to_string(),
            table_name: self.table_path.table().to_string(),
        }
    }

    fn has_primary_key(&self) -> bool {
        self.has_pk
    }

    fn create_upsert_writer(&self, column_indices: Vec<usize>) -> ffi::FfiPtrResult {
        let _enter = RUNTIME.enter();

        let table_upsert = match self.fluss_table().new_upsert() {
            Ok(u) => u,
            Err(e) => return err_ptr_from_core(&e),
        };

        let table_upsert = if column_indices.is_empty() {
            table_upsert
        } else {
            match table_upsert.partial_update(Some(column_indices)) {
                Ok(u) => u,
                Err(e) => return err_ptr_from_core(&e),
            }
        };

        let writer = match table_upsert.create_writer() {
            Ok(w) => w,
            Err(e) => return err_ptr_from_core(&e),
        };

        let ptr = Box::into_raw(Box::new(UpsertWriter {
            inner: writer,
            table_info: self.table_info.clone(),
        }));
        ok_ptr(ptr as usize)
    }

    fn new_lookuper(&self) -> ffi::FfiPtrResult {
        let _enter = RUNTIME.enter();

        let table_lookup = match self.fluss_table().new_lookup() {
            Ok(l) => l,
            Err(e) => return err_ptr_from_core(&e),
        };

        let lookuper = match table_lookup.create_lookuper() {
            Ok(l) => l,
            Err(e) => return err_ptr_from_core(&e),
        };

        let ptr = Box::into_raw(Box::new(Lookuper {
            inner: lookuper,
            table_info: self.table_info.clone(),
        }));
        ok_ptr(ptr as usize)
    }

    fn new_prefix_lookuper(&self, lookup_column_names: Vec<String>) -> ffi::FfiPtrResult {
        let _enter = RUNTIME.enter();

        let table_lookup = match self.fluss_table().new_lookup() {
            Ok(l) => l,
            Err(e) => return err_ptr_from_core(&e),
        };

        // `create_lookuper` validates the column names (rejecting unknown ones),
        // so by the time it succeeds every name resolves to an index.
        let row_type = self.table_info.row_type();
        let lookup_column_indices: Vec<usize> = lookup_column_names
            .iter()
            .filter_map(|name| row_type.get_field_index(name))
            .collect();

        let lookuper = match table_lookup
            .lookup_by(lookup_column_names)
            .create_lookuper()
        {
            Ok(l) => l,
            Err(e) => return err_ptr_from_core(&e),
        };

        let ptr = Box::into_raw(Box::new(PrefixLookuper {
            inner: lookuper,
            table_info: self.table_info.clone(),
            lookup_column_indices,
        }));
        ok_ptr(ptr as usize)
    }
}

// AppendWriter implementation
unsafe fn delete_append_writer(writer: *mut AppendWriter) {
    if !writer.is_null() {
        unsafe {
            drop(Box::from_raw(writer));
        }
    }
}

impl AppendWriter {
    fn append(&mut self, row: &GenericRowInner) -> ffi::FfiPtrResult {
        let schema = self.table_info.get_schema();
        let generic_row = match types::resolve_row_types(&row.row, Some(schema)) {
            Ok(r) => r,
            Err(e) => return client_err_ptr(e.to_string()),
        };

        let result_future = match self.inner.append(&generic_row) {
            Ok(f) => f,
            Err(e) => return err_ptr_from_core(&e),
        };

        let ptr = Box::into_raw(Box::new(WriteResult {
            inner: Some(result_future),
        }));
        ok_ptr(ptr as usize)
    }

    fn append_arrow_batch(&mut self, array_ptr: usize, schema_ptr: usize) -> ffi::FfiPtrResult {
        // Safety: C++ allocates these via `new ArrowArray/ArrowSchema` after a
        // successful `ExportRecordBatch`, so both pointers are valid heap
        // allocations that we take ownership of here.
        let ffi_array = unsafe { *Box::from_raw(array_ptr as *mut FFI_ArrowArray) };
        let ffi_schema = unsafe { Box::from_raw(schema_ptr as *mut FFI_ArrowSchema) };

        // Safety: `from_ffi` requires that the array and schema conform to the
        // Arrow C Data Interface, which is guaranteed by C++'s ExportRecordBatch.
        let array_data = match unsafe { arrow::ffi::from_ffi(ffi_array, &ffi_schema) } {
            Ok(d) => d,
            Err(e) => return client_err_ptr(format!("Failed to import Arrow batch: {e}")),
        };
        // ffi_array is consumed by from_ffi; ffi_schema is dropped here (Box goes out of scope)

        // Reconstruct RecordBatch from the imported StructArray data
        let struct_array = arrow::array::StructArray::from(array_data);
        let batch = arrow::record_batch::RecordBatch::from(struct_array);

        let result_future = match self.inner.append_arrow_batch(batch) {
            Ok(f) => f,
            Err(e) => return err_ptr_from_core(&e),
        };

        let ptr = Box::into_raw(Box::new(WriteResult {
            inner: Some(result_future),
        }));
        ok_ptr(ptr as usize)
    }

    fn flush(&mut self) -> ffi::FfiResult {
        let result = RUNTIME.block_on(async { self.inner.flush().await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }
}

unsafe fn delete_write_result(wr: *mut WriteResult) {
    if !wr.is_null() {
        unsafe {
            drop(Box::from_raw(wr));
        }
    }
}

impl WriteResult {
    fn wait(&mut self) -> ffi::FfiResult {
        if let Some(future) = self.inner.take() {
            let result = RUNTIME.block_on(future);
            match result {
                Ok(_) => ok_result(),
                Err(e) => err_from_core_error(&e),
            }
        } else {
            client_err("WriteResult already consumed".to_string())
        }
    }
}

// UpsertWriter implementation
unsafe fn delete_upsert_writer(writer: *mut UpsertWriter) {
    if !writer.is_null() {
        unsafe {
            drop(Box::from_raw(writer));
        }
    }
}

impl UpsertWriter {
    /// Pad row with Null to full schema width.
    /// This allows callers to only set the fields they care about.
    fn pad_row<'a>(&self, mut row: fcore::row::GenericRow<'a>) -> fcore::row::GenericRow<'a> {
        let num_columns = self.table_info.get_schema().columns().len();
        if row.values.len() < num_columns {
            row.values.resize(num_columns, fcore::row::Datum::Null);
        }
        row
    }

    fn upsert(&mut self, row: &GenericRowInner) -> ffi::FfiPtrResult {
        let schema = self.table_info.get_schema();
        let generic_row = match types::resolve_row_types(&row.row, Some(schema)) {
            Ok(r) => r,
            Err(e) => return client_err_ptr(e.to_string()),
        };
        let generic_row = self.pad_row(generic_row);

        let result_future = match self.inner.upsert(&generic_row) {
            Ok(f) => f,
            Err(e) => return err_ptr_from_core(&e),
        };

        let ptr = Box::into_raw(Box::new(WriteResult {
            inner: Some(result_future),
        }));
        ok_ptr(ptr as usize)
    }

    fn delete_row(&mut self, row: &GenericRowInner) -> ffi::FfiPtrResult {
        let schema = self.table_info.get_schema();
        let generic_row = match types::resolve_row_types(&row.row, Some(schema)) {
            Ok(r) => r,
            Err(e) => return client_err_ptr(e.to_string()),
        };
        let generic_row = self.pad_row(generic_row);

        let result_future = match self.inner.delete(&generic_row) {
            Ok(f) => f,
            Err(e) => return err_ptr_from_core(&e),
        };

        let ptr = Box::into_raw(Box::new(WriteResult {
            inner: Some(result_future),
        }));
        ok_ptr(ptr as usize)
    }

    fn upsert_flush(&mut self) -> ffi::FfiResult {
        let result = RUNTIME.block_on(async { self.inner.flush().await });

        match result {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    }
}

// Lookuper implementation
unsafe fn delete_lookuper(lookuper: *mut Lookuper) {
    if !lookuper.is_null() {
        unsafe {
            drop(Box::from_raw(lookuper));
        }
    }
}

impl Lookuper {
    /// Build a dense PK-only row from a (possibly sparse) input row.
    /// The user may set PK values at their full schema positions (e.g. [0, 2])
    /// via name-based Set(). We compact them into [0, 1, …] to match
    /// the lookup_row_type the core KeyEncoder expects.
    fn dense_pk_row<'a>(&self, mut row: fcore::row::GenericRow<'a>) -> fcore::row::GenericRow<'a> {
        let pk_indices = self.table_info.get_schema().primary_key_indexes();
        let mut dense = fcore::row::GenericRow::new(pk_indices.len());
        for (dense_idx, &schema_idx) in pk_indices.iter().enumerate() {
            if schema_idx < row.values.len() {
                dense.values[dense_idx] =
                    std::mem::replace(&mut row.values[schema_idx], fcore::row::Datum::Null);
            }
        }
        dense
    }

    fn lookup(&mut self, pk_row: &GenericRowInner) -> Box<LookupResultInner> {
        let schema = self.table_info.get_schema();
        let generic_row = match types::resolve_row_types(&pk_row.row, Some(schema)) {
            Ok(r) => self.dense_pk_row(r),
            Err(e) => {
                return Box::new(LookupResultInner::from_error(
                    CLIENT_ERROR_CODE,
                    e.to_string(),
                ));
            }
        };

        let lookup_result = match RUNTIME.block_on(self.inner.lookup(&generic_row)) {
            Ok(r) => r,
            Err(e) => {
                let ffi_err = err_from_core_error(&e);
                return Box::new(LookupResultInner::from_error(
                    ffi_err.error_code,
                    ffi_err.error_message,
                ));
            }
        };

        let columns = self.table_info.get_schema().columns().to_vec();
        match lookup_result.get_single_row() {
            Ok(Some(row)) => match types::compacted_row_to_owned(&row, &self.table_info) {
                Ok(owned_row) => Box::new(LookupResultInner {
                    error: None,
                    found: true,
                    row: Some(owned_row),
                    columns,
                }),
                Err(e) => Box::new(LookupResultInner::from_error(
                    CLIENT_ERROR_CODE,
                    e.to_string(),
                )),
            },
            Ok(None) => Box::new(LookupResultInner {
                error: None,
                found: false,
                row: None,
                columns,
            }),
            Err(e) => {
                let ffi_err = err_from_core_error(&e);
                Box::new(LookupResultInner::from_error(
                    ffi_err.error_code,
                    ffi_err.error_message,
                ))
            }
        }
    }
}

// PrefixLookuper implementation
unsafe fn delete_prefix_lookuper(lookuper: *mut PrefixLookuper) {
    if !lookuper.is_null() {
        unsafe {
            drop(Box::from_raw(lookuper));
        }
    }
}

impl PrefixLookuper {
    /// Compact a sparse input row (prefix columns set at their schema positions)
    /// into the dense, lookup-column-ordered row the core prefix encoder expects.
    fn dense_prefix_row<'a>(&self, mut row: GenericRow<'a>) -> GenericRow<'a> {
        let mut dense = GenericRow::new(self.lookup_column_indices.len());
        for (dense_idx, &schema_idx) in self.lookup_column_indices.iter().enumerate() {
            if schema_idx < row.values.len() {
                dense.values[dense_idx] =
                    std::mem::replace(&mut row.values[schema_idx], Datum::Null);
            }
        }
        dense
    }

    fn prefix_lookup(&mut self, prefix_row: &GenericRowInner) -> Box<PrefixLookupResultInner> {
        let schema = self.table_info.get_schema();
        let generic_row = match types::resolve_row_types(&prefix_row.row, Some(schema)) {
            Ok(r) => self.dense_prefix_row(r),
            Err(e) => {
                return Box::new(PrefixLookupResultInner::from_error(
                    CLIENT_ERROR_CODE,
                    e.to_string(),
                ));
            }
        };

        let lookup_result = match RUNTIME.block_on(self.inner.lookup(&generic_row)) {
            Ok(r) => r,
            Err(e) => {
                let ffi_err = err_from_core_error(&e);
                return Box::new(PrefixLookupResultInner::from_error(
                    ffi_err.error_code,
                    ffi_err.error_message,
                ));
            }
        };

        let lookup_rows = match lookup_result.get_rows() {
            Ok(rows) => rows,
            Err(e) => {
                let ffi_err = err_from_core_error(&e);
                return Box::new(PrefixLookupResultInner::from_error(
                    ffi_err.error_code,
                    ffi_err.error_message,
                ));
            }
        };

        let mut rows = Vec::with_capacity(lookup_rows.len());
        for row in &lookup_rows {
            match types::compacted_row_to_owned(row, &self.table_info) {
                Ok(owned_row) => rows.push(owned_row),
                Err(e) => {
                    return Box::new(PrefixLookupResultInner::from_error(
                        CLIENT_ERROR_CODE,
                        e.to_string(),
                    ));
                }
            }
        }

        let columns = self.table_info.get_schema().columns().to_vec();
        Box::new(PrefixLookupResultInner {
            error: None,
            rows,
            columns,
        })
    }
}

// LogScanner implementation
unsafe fn delete_log_scanner(scanner: *mut LogScanner) {
    if !scanner.is_null() {
        unsafe {
            drop(Box::from_raw(scanner));
        }
    }
}

// Helper function to free the Arrow FFI structures separately (for use after ImportRecordBatch)
pub extern "C" fn free_arrow_ffi_structures(array_ptr: usize, schema_ptr: usize) {
    if array_ptr != 0 {
        let _array = unsafe { Box::from_raw(array_ptr as *mut FFI_ArrowArray) };
    }
    if schema_ptr != 0 {
        let _schema = unsafe { Box::from_raw(schema_ptr as *mut FFI_ArrowSchema) };
    }
}

/// Dispatch a method call to whichever scanner variant is active.
/// Both LogScanner and RecordBatchLogScanner share the same subscribe/unsubscribe interface.
macro_rules! dispatch_scanner {
    ($self:expr, $method:ident($($arg:expr),*)) => {
        match RUNTIME.block_on(async {
            match &$self.scanner {
                ScannerKind::Record(s) => s.$method($($arg),*).await,
                ScannerKind::Batch(s) => s.$method($($arg),*).await,
            }
        }) {
            Ok(_) => ok_result(),
            Err(e) => err_from_core_error(&e),
        }
    };
}

impl LogScanner {
    fn subscribe(&self, bucket_id: i32, start_offset: i64) -> ffi::FfiResult {
        dispatch_scanner!(self, subscribe(bucket_id, start_offset))
    }

    fn subscribe_buckets(&self, subscriptions: Vec<ffi::FfiBucketSubscription>) -> ffi::FfiResult {
        let bucket_offsets: HashMap<i32, i64> = subscriptions
            .into_iter()
            .map(|s| (s.bucket_id, s.offset))
            .collect();
        dispatch_scanner!(self, subscribe_buckets(&bucket_offsets))
    }

    fn subscribe_partition(
        &self,
        partition_id: PartitionId,
        bucket_id: i32,
        start_offset: i64,
    ) -> ffi::FfiResult {
        dispatch_scanner!(
            self,
            subscribe_partition(partition_id, bucket_id, start_offset)
        )
    }

    fn subscribe_partition_buckets(
        &self,
        subscriptions: Vec<ffi::FfiPartitionBucketSubscription>,
    ) -> ffi::FfiResult {
        let offsets: HashMap<(PartitionId, i32), i64> = subscriptions
            .into_iter()
            .map(|s| ((s.partition_id, s.bucket_id), s.offset))
            .collect();
        dispatch_scanner!(self, subscribe_partition_buckets(&offsets))
    }

    fn unsubscribe(&self, bucket_id: i32) -> ffi::FfiResult {
        dispatch_scanner!(self, unsubscribe(bucket_id))
    }

    fn unsubscribe_partition(&self, partition_id: PartitionId, bucket_id: i32) -> ffi::FfiResult {
        dispatch_scanner!(self, unsubscribe_partition(partition_id, bucket_id))
    }

    fn poll(&self, timeout_ms: i64) -> Box<ScanResultInner> {
        let ScannerKind::Record(ref inner) = self.scanner else {
            return Box::new(ScanResultInner::from_error(
                CLIENT_ERROR_CODE,
                "Record-based scanner not available".to_string(),
            ));
        };

        let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
        let result = RUNTIME.block_on(async { inner.poll(timeout).await });

        match result {
            Ok(records) => {
                let columns = self.projected_columns.clone();
                let mut total_count = 0usize;
                let mut buckets = Vec::new();
                let mut bucket_infos = Vec::new();
                for (table_bucket, bucket_records) in records.into_records_by_buckets() {
                    let count = bucket_records.len();
                    total_count += count;
                    bucket_infos.push(ffi::FfiBucketInfo {
                        table_id: table_bucket.table_id(),
                        bucket_id: table_bucket.bucket_id(),
                        has_partition_id: table_bucket.partition_id().is_some(),
                        partition_id: table_bucket.partition_id().unwrap_or(0),
                        record_count: count,
                    });
                    buckets.push((table_bucket, bucket_records));
                }
                Box::new(ScanResultInner {
                    error: None,
                    buckets,
                    columns,
                    bucket_infos,
                    total_count,
                })
            }
            Err(e) => {
                let ffi_err = err_from_core_error(&e);
                Box::new(ScanResultInner::from_error(
                    ffi_err.error_code,
                    ffi_err.error_message,
                ))
            }
        }
    }

    fn poll_record_batch(&self, timeout_ms: i64) -> ffi::FfiArrowRecordBatchesResult {
        let ScannerKind::Batch(ref inner_batch) = self.scanner else {
            return ffi::FfiArrowRecordBatchesResult {
                result: client_err("Batch-based scanner not available".to_string()),
                arrow_batches: ffi::FfiArrowRecordBatches { batches: vec![] },
            };
        };

        let timeout = Duration::from_millis(timeout_ms.max(0) as u64);
        let result = RUNTIME.block_on(async { inner_batch.poll(timeout).await });

        match result {
            Ok(batches) => arrow_batches_result(types::core_scan_batches_to_ffi(&batches)),
            Err(e) => empty_arrow_batches_result(err_from_core_error(&e)),
        }
    }
}

// BatchScanner implementation
unsafe fn delete_batch_scanner(scanner: *mut BatchScanner) {
    if !scanner.is_null() {
        unsafe {
            drop(Box::from_raw(scanner));
        }
    }
}

impl BatchScanner {
    fn next_batch(&self) -> ffi::FfiArrowRecordBatchesResult {
        let mut scanner = self.inner.lock().unwrap();
        match RUNTIME.block_on(scanner.next_batch()) {
            Ok(Some(batch)) => arrow_batches_result(types::core_scan_batches_to_ffi(
                std::slice::from_ref(&batch),
            )),
            Ok(None) => empty_arrow_batches_result(ok_result()),
            Err(e) => empty_arrow_batches_result(err_from_core_error(&e)),
        }
    }

    fn collect_all_batches(&self) -> ffi::FfiArrowRecordBatchesResult {
        let mut scanner = self.inner.lock().unwrap();
        match RUNTIME.block_on(scanner.collect_all_batches()) {
            Ok(batches) => arrow_batches_result(types::core_scan_batches_to_ffi(&batches)),
            Err(e) => empty_arrow_batches_result(err_from_core_error(&e)),
        }
    }
}

// ============================================================================
// Opaque types: GenericRowInner (write path)
// ============================================================================

pub struct GenericRowInner {
    row: fcore::row::GenericRow<'static>,
}

fn new_generic_row(field_count: usize) -> Box<GenericRowInner> {
    Box::new(GenericRowInner {
        row: fcore::row::GenericRow::new(field_count),
    })
}

impl GenericRowInner {
    fn gr_reset(&mut self) {
        let len = self.row.values.len();
        self.row = fcore::row::GenericRow::new(len);
    }

    fn gr_set_null(&mut self, idx: usize) {
        self.ensure_size(idx);
        self.row.set_field(idx, fcore::row::Datum::Null);
    }

    fn gr_set_bool(&mut self, idx: usize, val: bool) {
        self.ensure_size(idx);
        self.row.set_field(idx, fcore::row::Datum::Bool(val));
    }

    fn gr_set_i32(&mut self, idx: usize, val: i32) {
        self.ensure_size(idx);
        self.row.set_field(idx, fcore::row::Datum::Int32(val));
    }

    fn gr_set_i64(&mut self, idx: usize, val: i64) {
        self.ensure_size(idx);
        self.row.set_field(idx, fcore::row::Datum::Int64(val));
    }

    fn gr_set_f32(&mut self, idx: usize, val: f32) {
        self.ensure_size(idx);
        self.row
            .set_field(idx, fcore::row::Datum::Float32(val.into()));
    }

    fn gr_set_f64(&mut self, idx: usize, val: f64) {
        self.ensure_size(idx);
        self.row
            .set_field(idx, fcore::row::Datum::Float64(val.into()));
    }

    fn gr_set_str(&mut self, idx: usize, val: &str) {
        self.ensure_size(idx);
        self.row.set_field(
            idx,
            fcore::row::Datum::String(std::borrow::Cow::Owned(val.to_string())),
        );
    }

    fn gr_set_bytes(&mut self, idx: usize, val: &[u8]) {
        self.ensure_size(idx);
        self.row.set_field(
            idx,
            fcore::row::Datum::Blob(std::borrow::Cow::Owned(val.to_vec())),
        );
    }

    fn gr_set_date(&mut self, idx: usize, days: i32) {
        self.ensure_size(idx);
        self.row
            .set_field(idx, fcore::row::Datum::Date(fcore::row::Date::new(days)));
    }

    fn gr_set_time(&mut self, idx: usize, millis: i32) {
        self.ensure_size(idx);
        self.row
            .set_field(idx, fcore::row::Datum::Time(fcore::row::Time::new(millis)));
    }

    fn gr_set_ts_ntz(&mut self, idx: usize, millis: i64, nanos: i32) {
        self.ensure_size(idx);
        // Use from_millis_nanos, falling back to millis-only on error
        let ts = fcore::row::TimestampNtz::from_millis_nanos(millis, nanos)
            .unwrap_or_else(|_| fcore::row::TimestampNtz::new(millis));
        self.row.set_field(idx, fcore::row::Datum::TimestampNtz(ts));
    }

    fn gr_set_ts_ltz(&mut self, idx: usize, millis: i64, nanos: i32) {
        self.ensure_size(idx);
        let ts = fcore::row::TimestampLtz::from_millis_nanos(millis, nanos)
            .unwrap_or_else(|_| fcore::row::TimestampLtz::new(millis));
        self.row.set_field(idx, fcore::row::Datum::TimestampLtz(ts));
    }

    fn gr_set_decimal_str(&mut self, idx: usize, val: &str) {
        self.ensure_size(idx);
        // Store as string; resolve_row_types() will parse and validate against schema
        self.row.set_field(
            idx,
            fcore::row::Datum::String(std::borrow::Cow::Owned(val.to_string())),
        );
    }

    fn gr_set_array(&mut self, idx: usize, writer: &mut ArrayWriterInner) -> Result<(), String> {
        self.ensure_size(idx);
        writer.complete_if_needed()?;
        let arr = writer.completed.take().ok_or_else(|| {
            "ArrayWriter invariant violation: completed array missing after finalize".to_string()
        })?;
        self.row.set_field(idx, fcore::row::Datum::Array(arr));
        Ok(())
    }

    fn gr_set_map(&mut self, idx: usize, writer: &mut MapWriterInner) -> Result<(), String> {
        self.ensure_size(idx);
        writer.complete_if_needed()?;
        let map = writer.completed.take().ok_or_else(|| {
            "MapWriter invariant violation: completed map missing after finalize".to_string()
        })?;
        self.row.set_field(idx, fcore::row::Datum::Map(map));
        Ok(())
    }

    fn gr_set_row(&mut self, idx: usize, row: &mut GenericRowInner) -> Result<(), String> {
        self.ensure_size(idx);
        // Move the nested row out; the C++ side discards its writer afterwards.
        let nested = std::mem::replace(&mut row.row, fcore::row::GenericRow::new(0));
        self.row
            .set_field(idx, fcore::row::Datum::Row(Box::new(nested)));
        Ok(())
    }

    fn ensure_size(&mut self, idx: usize) {
        if self.row.values.len() <= idx {
            self.row.values.resize(idx + 1, fcore::row::Datum::Null);
        }
    }
}

// ============================================================================
// Shared row-reading helpers (used by both ScanResultInner and LookupResultInner)
// ============================================================================

mod row_reader {
    use fcore::row::InternalRow;
    use fluss as fcore;

    use crate::types;

    /// Get column at `field`, or error if out of bounds.
    fn get_column(
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<&fcore::metadata::Column, String> {
        columns.get(field).ok_or_else(|| {
            format!(
                "field index {field} out of range ({} columns)",
                columns.len()
            )
        })
    }

    /// Validate bounds, null, and type compatibility in a single pass.
    /// Returns the data type on success for callers that need to dispatch on it.
    fn validate<'a>(
        row: &dyn InternalRow,
        columns: &'a [fcore::metadata::Column],
        field: usize,
        getter: &str,
        allowed: impl FnOnce(&fcore::metadata::DataType) -> bool,
    ) -> Result<&'a fcore::metadata::DataType, String> {
        let col = get_column(columns, field)?;
        if row.is_null_at(field).map_err(|e| e.to_string())? {
            return Err(format!("field {field} is null"));
        }
        let dt = col.data_type();
        if !allowed(dt) {
            return Err(format!(
                "{getter}: column {field} has incompatible type {dt}"
            ));
        }
        Ok(dt)
    }

    pub fn column_type(columns: &[fcore::metadata::Column], field: usize) -> Result<i32, String> {
        Ok(types::core_data_type_to_ffi(
            get_column(columns, field)?.data_type(),
        ))
    }

    pub fn column_name(columns: &[fcore::metadata::Column], field: usize) -> Result<&str, String> {
        Ok(get_column(columns, field)?.name())
    }

    pub fn is_null(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<bool, String> {
        get_column(columns, field)?;
        row.is_null_at(field).map_err(|e| e.to_string())
    }

    pub fn get_bool(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<bool, String> {
        validate(row, columns, field, "get_bool", |dt| {
            matches!(dt, fcore::metadata::DataType::Boolean(_))
        })?;
        row.get_boolean(field).map_err(|e| e.to_string())
    }

    pub fn get_i32(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i32, String> {
        let dt = validate(row, columns, field, "get_i32", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::TinyInt(_)
                    | fcore::metadata::DataType::SmallInt(_)
                    | fcore::metadata::DataType::Int(_)
            )
        })?;
        match dt {
            fcore::metadata::DataType::TinyInt(_) => row
                .get_byte(field)
                .map(|v| v as i32)
                .map_err(|e| e.to_string()),
            fcore::metadata::DataType::SmallInt(_) => row
                .get_short(field)
                .map(|v| v as i32)
                .map_err(|e| e.to_string()),
            _ => row.get_int(field).map_err(|e| e.to_string()),
        }
    }

    pub fn get_i64(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i64, String> {
        validate(row, columns, field, "get_i64", |dt| {
            matches!(dt, fcore::metadata::DataType::BigInt(_))
        })?;
        row.get_long(field).map_err(|e| e.to_string())
    }

    pub fn get_f32(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<f32, String> {
        validate(row, columns, field, "get_f32", |dt| {
            matches!(dt, fcore::metadata::DataType::Float(_))
        })?;
        row.get_float(field).map_err(|e| e.to_string())
    }

    pub fn get_f64(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<f64, String> {
        validate(row, columns, field, "get_f64", |dt| {
            matches!(dt, fcore::metadata::DataType::Double(_))
        })?;
        row.get_double(field).map_err(|e| e.to_string())
    }

    pub fn get_str<'a>(
        row: &'a dyn InternalRow,
        columns: &'a [fcore::metadata::Column],
        field: usize,
    ) -> Result<&'a str, String> {
        let dt = validate(row, columns, field, "get_str", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::Char(_) | fcore::metadata::DataType::String(_)
            )
        })?;
        match dt {
            fcore::metadata::DataType::Char(ct) => row
                .get_char(field, ct.length() as usize)
                .map_err(|e| e.to_string()),
            _ => row.get_string(field).map_err(|e| e.to_string()),
        }
    }

    pub fn get_bytes<'a>(
        row: &'a dyn InternalRow,
        columns: &'a [fcore::metadata::Column],
        field: usize,
    ) -> Result<&'a [u8], String> {
        let dt = validate(row, columns, field, "get_bytes", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::Binary(_) | fcore::metadata::DataType::Bytes(_)
            )
        })?;
        match dt {
            fcore::metadata::DataType::Binary(bt) => row
                .get_binary(field, bt.length())
                .map_err(|e| e.to_string()),
            _ => row.get_bytes(field).map_err(|e| e.to_string()),
        }
    }

    pub fn get_date_days(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i32, String> {
        validate(row, columns, field, "get_date_days", |dt| {
            matches!(dt, fcore::metadata::DataType::Date(_))
        })?;
        row.get_date(field)
            .map(|d| d.get_inner())
            .map_err(|e| e.to_string())
    }

    pub fn get_time_millis(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i32, String> {
        validate(row, columns, field, "get_time_millis", |dt| {
            matches!(dt, fcore::metadata::DataType::Time(_))
        })?;
        row.get_time(field)
            .map(|t| t.get_inner())
            .map_err(|e| e.to_string())
    }

    pub fn get_ts_millis(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i64, String> {
        let dt = validate(row, columns, field, "get_ts_millis", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::Timestamp(_)
                    | fcore::metadata::DataType::TimestampLTz(_)
            )
        })?;
        match dt {
            fcore::metadata::DataType::TimestampLTz(ts) => row
                .get_timestamp_ltz(field, ts.precision())
                .map(|v| v.get_epoch_millisecond())
                .map_err(|e| e.to_string()),
            fcore::metadata::DataType::Timestamp(ts) => row
                .get_timestamp_ntz(field, ts.precision())
                .map(|v| v.get_millisecond())
                .map_err(|e| e.to_string()),
            dt => Err(format!("get_ts_millis: unexpected type {dt}")),
        }
    }

    pub fn get_ts_nanos(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<i32, String> {
        let dt = validate(row, columns, field, "get_ts_nanos", |dt| {
            matches!(
                dt,
                fcore::metadata::DataType::Timestamp(_)
                    | fcore::metadata::DataType::TimestampLTz(_)
            )
        })?;
        match dt {
            fcore::metadata::DataType::TimestampLTz(ts) => row
                .get_timestamp_ltz(field, ts.precision())
                .map(|v| v.get_nano_of_millisecond())
                .map_err(|e| e.to_string()),
            fcore::metadata::DataType::Timestamp(ts) => row
                .get_timestamp_ntz(field, ts.precision())
                .map(|v| v.get_nano_of_millisecond())
                .map_err(|e| e.to_string()),
            dt => Err(format!("get_ts_nanos: unexpected type {dt}")),
        }
    }

    pub fn is_ts_ltz(columns: &[fcore::metadata::Column], field: usize) -> Result<bool, String> {
        Ok(matches!(
            get_column(columns, field)?.data_type(),
            fcore::metadata::DataType::TimestampLTz(_)
        ))
    }

    pub fn get_decimal_str(
        row: &dyn InternalRow,
        columns: &[fcore::metadata::Column],
        field: usize,
    ) -> Result<String, String> {
        let dt = validate(row, columns, field, "get_decimal_str", |dt| {
            matches!(dt, fcore::metadata::DataType::Decimal(_))
        })?;
        match dt {
            fcore::metadata::DataType::Decimal(dd) => {
                let decimal = row
                    .get_decimal(field, dd.precision() as usize, dd.scale() as usize)
                    .map_err(|e| e.to_string())?;
                Ok(decimal.to_big_decimal().to_string())
            }
            dt => Err(format!("get_decimal_str: unexpected type {dt}")),
        }
    }
}

// ============================================================================
// array_reader — low-level accessors over an already-resolved FlussArray.
//
// `get_datum` is the single dispatch that turns any array/map element into an
// owned `Datum` for the recursive `ValueInner` handle (bounds-checked, null
// and type validated, recursing into nested ROW/MAP/ARRAY).
// ============================================================================

mod array_reader {
    use super::fcore;
    use fcore::metadata::DataType as DT;
    use fcore::row::Datum;

    fn validate_index(
        arr: &fcore::row::binary_array::FlussArray,
        element: usize,
        op: &str,
    ) -> Result<(), String> {
        if element < arr.size() {
            Ok(())
        } else {
            Err(format!(
                "{op}: element index out of bounds: element={element}, size={}",
                arr.size()
            ))
        }
    }

    pub fn is_null(
        arr: &fcore::row::binary_array::FlussArray,
        element: usize,
    ) -> Result<bool, String> {
        validate_index(arr, element, "array_is_null")?;
        Ok(arr.is_null_at(element))
    }

    pub fn get_datum(
        arr: &fcore::row::binary_array::FlussArray,
        elem_type: &fcore::metadata::DataType,
        element: usize,
    ) -> Result<fcore::row::Datum<'static>, String> {
        if is_null(arr, element)? {
            return Ok(Datum::Null);
        }
        let map_err = |e: fcore::error::Error| e.to_string();
        Ok(match elem_type {
            DT::Boolean(_) => Datum::Bool(arr.get_boolean(element).map_err(map_err)?),
            DT::TinyInt(_) => Datum::Int8(arr.get_byte(element).map_err(map_err)?),
            DT::SmallInt(_) => Datum::Int16(arr.get_short(element).map_err(map_err)?),
            DT::Int(_) => Datum::Int32(arr.get_int(element).map_err(map_err)?),
            DT::BigInt(_) => Datum::Int64(arr.get_long(element).map_err(map_err)?),
            DT::Float(_) => Datum::Float32(arr.get_float(element).map_err(map_err)?.into()),
            DT::Double(_) => Datum::Float64(arr.get_double(element).map_err(map_err)?.into()),
            DT::String(_) | DT::Char(_) => Datum::String(std::borrow::Cow::Owned(
                arr.get_string(element).map_err(map_err)?.to_string(),
            )),
            DT::Bytes(_) | DT::Binary(_) => Datum::Blob(std::borrow::Cow::Owned(
                arr.get_binary(element).map_err(map_err)?.to_vec(),
            )),
            DT::Decimal(d) => Datum::Decimal(
                arr.get_decimal(element, d.precision(), d.scale())
                    .map_err(map_err)?,
            ),
            DT::Date(_) => Datum::Date(arr.get_date(element).map_err(map_err)?),
            DT::Time(_) => Datum::Time(arr.get_time(element).map_err(map_err)?),
            DT::Timestamp(t) => Datum::TimestampNtz(
                arr.get_timestamp_ntz(element, t.precision())
                    .map_err(map_err)?,
            ),
            DT::TimestampLTz(t) => Datum::TimestampLtz(
                arr.get_timestamp_ltz(element, t.precision())
                    .map_err(map_err)?,
            ),
            DT::Array(_) => Datum::Array(arr.get_array(element).map_err(map_err)?),
            DT::Map(mt) => Datum::Map(
                arr.get_map(element, mt.key_type(), mt.value_type())
                    .map_err(map_err)?,
            ),
            DT::Row(rt) => {
                let cr = arr.get_row(element, rt).map_err(map_err)?;
                let cols: Vec<fcore::metadata::Column> = rt
                    .fields()
                    .iter()
                    .map(|f| fcore::metadata::Column::new(f.name(), f.data_type().clone()))
                    .collect();
                Datum::Row(Box::new(
                    crate::types::internal_row_to_owned_generic(&cr, &cols)
                        .map_err(|e| e.to_string())?,
                ))
            }
        })
    }
}

// ============================================================================
// Opaque types: ScanResultInner (scan read path)
// ============================================================================

pub struct ScanResultInner {
    error: Option<(i32, String)>,
    buckets: Vec<(fcore::metadata::TableBucket, Vec<fcore::record::ScanRecord>)>,
    columns: Vec<fcore::metadata::Column>,
    bucket_infos: Vec<ffi::FfiBucketInfo>,
    total_count: usize,
}

impl ScanResultInner {
    fn from_error(code: i32, msg: String) -> Self {
        Self {
            error: Some((code, msg)),
            buckets: Vec::new(),
            columns: Vec::new(),
            bucket_infos: Vec::new(),
            total_count: 0,
        }
    }

    fn resolve(&self, bucket: usize, rec: usize) -> &fcore::record::ScanRecord {
        &self.buckets[bucket].1[rec]
    }

    fn sv_has_error(&self) -> bool {
        self.error.is_some()
    }

    fn sv_error_code(&self) -> i32 {
        self.error.as_ref().map_or(0, |e| e.0)
    }

    fn sv_error_message(&self) -> &str {
        self.error.as_ref().map_or("", |e| e.1.as_str())
    }

    fn sv_record_count(&self) -> usize {
        self.total_count
    }

    fn sv_column_count(&self) -> usize {
        self.columns.len()
    }
    fn sv_column_name(&self, field: usize) -> Result<&str, String> {
        row_reader::column_name(&self.columns, field)
    }
    fn sv_column_type(&self, field: usize) -> Result<i32, String> {
        row_reader::column_type(&self.columns, field)
    }

    fn sv_offset(&self, bucket: usize, rec: usize) -> i64 {
        self.resolve(bucket, rec).offset()
    }
    fn sv_timestamp(&self, bucket: usize, rec: usize) -> i64 {
        self.resolve(bucket, rec).timestamp()
    }
    fn sv_change_type(&self, bucket: usize, rec: usize) -> i32 {
        self.resolve(bucket, rec).change_type().to_byte_value() as i32
    }
    fn sv_field_count(&self) -> usize {
        self.columns.len()
    }

    // Field accessors — C++ validates bounds in BucketRecords/RecordAt, validate() checks field.
    fn sv_is_null(&self, bucket: usize, rec: usize, field: usize) -> Result<bool, String> {
        row_reader::is_null(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_bool(&self, bucket: usize, rec: usize, field: usize) -> Result<bool, String> {
        row_reader::get_bool(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_i32(&self, bucket: usize, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_i32(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_i64(&self, bucket: usize, rec: usize, field: usize) -> Result<i64, String> {
        row_reader::get_i64(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_f32(&self, bucket: usize, rec: usize, field: usize) -> Result<f32, String> {
        row_reader::get_f32(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_f64(&self, bucket: usize, rec: usize, field: usize) -> Result<f64, String> {
        row_reader::get_f64(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_str(&self, bucket: usize, rec: usize, field: usize) -> Result<&str, String> {
        row_reader::get_str(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_bytes(&self, bucket: usize, rec: usize, field: usize) -> Result<&[u8], String> {
        row_reader::get_bytes(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_date_days(&self, bucket: usize, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_date_days(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_time_millis(&self, bucket: usize, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_time_millis(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_ts_millis(&self, bucket: usize, rec: usize, field: usize) -> Result<i64, String> {
        row_reader::get_ts_millis(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_get_ts_nanos(&self, bucket: usize, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_ts_nanos(self.resolve(bucket, rec).row(), &self.columns, field)
    }
    fn sv_is_ts_ltz(&self, _bucket: usize, _rec: usize, field: usize) -> Result<bool, String> {
        row_reader::is_ts_ltz(&self.columns, field)
    }
    fn sv_get_decimal_str(
        &self,
        bucket: usize,
        rec: usize,
        field: usize,
    ) -> Result<String, String> {
        row_reader::get_decimal_str(self.resolve(bucket, rec).row(), &self.columns, field)
    }

    fn sv_get_value(
        &self,
        bucket: usize,
        rec: usize,
        field: usize,
    ) -> Result<Box<ValueInner>, String> {
        // Scan rows are borrowed Arrow cursors; materialize only the requested
        // field's owned datum (not the whole row). Top-level scalar scan reads
        // keep their zero-copy index fast-path.
        let datum = crate::types::field_to_owned_datum(
            self.resolve(bucket, rec).row(),
            &self.columns,
            field,
        )
        .map_err(|e| e.to_string())?;
        let data_type = self
            .columns
            .get(field)
            .ok_or_else(|| format!("field index {field} out of range"))?
            .data_type()
            .clone();
        Ok(Box::new(ValueInner { datum, data_type }))
    }

    fn sv_bucket_infos(&self) -> &Vec<ffi::FfiBucketInfo> {
        &self.bucket_infos
    }
}

// ============================================================================
// Opaque types: LookupResultInner (lookup read path)
// ============================================================================

pub struct LookupResultInner {
    error: Option<(i32, String)>,
    found: bool,
    row: Option<fcore::row::GenericRow<'static>>,
    columns: Vec<fcore::metadata::Column>,
}

impl LookupResultInner {
    fn from_error(code: i32, msg: String) -> Self {
        Self {
            error: Some((code, msg)),
            found: false,
            row: None,
            columns: Vec::new(),
        }
    }

    fn lv_has_error(&self) -> bool {
        self.error.is_some()
    }

    fn lv_error_code(&self) -> i32 {
        self.error.as_ref().map_or(0, |e| e.0)
    }

    fn lv_error_message(&self) -> &str {
        self.error.as_ref().map_or("", |e| e.1.as_str())
    }

    fn lv_found(&self) -> bool {
        self.found
    }

    fn lv_field_count(&self) -> usize {
        self.columns.len()
    }

    fn lv_column_type(&self, field: usize) -> Result<i32, String> {
        row_reader::column_type(&self.columns, field)
    }

    fn lv_column_name(&self, field: usize) -> Result<&str, String> {
        row_reader::column_name(&self.columns, field)
    }

    fn lv_row(&self) -> Result<&fcore::row::GenericRow<'static>, String> {
        self.row
            .as_ref()
            .ok_or_else(|| "no row available (not found or error)".to_string())
    }

    // Field accessors — delegate to shared row_reader helpers.
    fn lv_is_null(&self, field: usize) -> Result<bool, String> {
        let r = self.lv_row()?;
        row_reader::is_null(r, &self.columns, field)
    }
    fn lv_get_bool(&self, field: usize) -> Result<bool, String> {
        let r = self.lv_row()?;
        row_reader::get_bool(r, &self.columns, field)
    }
    fn lv_get_i32(&self, field: usize) -> Result<i32, String> {
        let r = self.lv_row()?;
        row_reader::get_i32(r, &self.columns, field)
    }
    fn lv_get_i64(&self, field: usize) -> Result<i64, String> {
        let r = self.lv_row()?;
        row_reader::get_i64(r, &self.columns, field)
    }
    fn lv_get_f32(&self, field: usize) -> Result<f32, String> {
        let r = self.lv_row()?;
        row_reader::get_f32(r, &self.columns, field)
    }
    fn lv_get_f64(&self, field: usize) -> Result<f64, String> {
        let r = self.lv_row()?;
        row_reader::get_f64(r, &self.columns, field)
    }
    fn lv_get_str(&self, field: usize) -> Result<&str, String> {
        let r = self.lv_row()?;
        row_reader::get_str(r, &self.columns, field)
    }
    fn lv_get_bytes(&self, field: usize) -> Result<&[u8], String> {
        let r = self.lv_row()?;
        row_reader::get_bytes(r, &self.columns, field)
    }
    fn lv_get_date_days(&self, field: usize) -> Result<i32, String> {
        let r = self.lv_row()?;
        row_reader::get_date_days(r, &self.columns, field)
    }
    fn lv_get_time_millis(&self, field: usize) -> Result<i32, String> {
        let r = self.lv_row()?;
        row_reader::get_time_millis(r, &self.columns, field)
    }
    fn lv_get_ts_millis(&self, field: usize) -> Result<i64, String> {
        let r = self.lv_row()?;
        row_reader::get_ts_millis(r, &self.columns, field)
    }
    fn lv_get_ts_nanos(&self, field: usize) -> Result<i32, String> {
        let r = self.lv_row()?;
        row_reader::get_ts_nanos(r, &self.columns, field)
    }
    fn lv_is_ts_ltz(&self, field: usize) -> Result<bool, String> {
        row_reader::is_ts_ltz(&self.columns, field)
    }
    fn lv_get_decimal_str(&self, field: usize) -> Result<String, String> {
        let r = self.lv_row()?;
        row_reader::get_decimal_str(r, &self.columns, field)
    }
    fn lv_get_value(&self, field: usize) -> Result<Box<ValueInner>, String> {
        value_from_owned_row(self.lv_row()?, &self.columns, field)
    }
}

// ============================================================================
// Opaque types: PrefixLookupResultInner (prefix lookup read path)
//
// Like LookupResultInner but holds 0..n rows; accessors take a record index.
// ============================================================================

pub struct PrefixLookupResultInner {
    error: Option<(i32, String)>,
    rows: Vec<GenericRow<'static>>,
    columns: Vec<Column>,
}

impl PrefixLookupResultInner {
    fn from_error(code: i32, msg: String) -> Self {
        Self {
            error: Some((code, msg)),
            rows: Vec::new(),
            columns: Vec::new(),
        }
    }

    fn plv_has_error(&self) -> bool {
        self.error.is_some()
    }

    fn plv_error_code(&self) -> i32 {
        self.error.as_ref().map_or(0, |e| e.0)
    }

    fn plv_error_message(&self) -> &str {
        self.error.as_ref().map_or("", |e| e.1.as_str())
    }

    fn plv_row_count(&self) -> usize {
        self.rows.len()
    }

    fn plv_field_count(&self) -> usize {
        self.columns.len()
    }

    fn plv_column_type(&self, field: usize) -> Result<i32, String> {
        row_reader::column_type(&self.columns, field)
    }

    fn plv_column_name(&self, field: usize) -> Result<&str, String> {
        row_reader::column_name(&self.columns, field)
    }

    fn plv_row(&self, rec: usize) -> Result<&GenericRow<'static>, String> {
        self.rows
            .get(rec)
            .ok_or_else(|| format!("record index {rec} out of range ({} rows)", self.rows.len()))
    }

    // Field accessors — delegate to shared row_reader helpers.
    fn plv_is_null(&self, rec: usize, field: usize) -> Result<bool, String> {
        row_reader::is_null(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_bool(&self, rec: usize, field: usize) -> Result<bool, String> {
        row_reader::get_bool(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_i32(&self, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_i32(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_i64(&self, rec: usize, field: usize) -> Result<i64, String> {
        row_reader::get_i64(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_f32(&self, rec: usize, field: usize) -> Result<f32, String> {
        row_reader::get_f32(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_f64(&self, rec: usize, field: usize) -> Result<f64, String> {
        row_reader::get_f64(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_str(&self, rec: usize, field: usize) -> Result<&str, String> {
        row_reader::get_str(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_bytes(&self, rec: usize, field: usize) -> Result<&[u8], String> {
        row_reader::get_bytes(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_date_days(&self, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_date_days(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_time_millis(&self, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_time_millis(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_ts_millis(&self, rec: usize, field: usize) -> Result<i64, String> {
        row_reader::get_ts_millis(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_ts_nanos(&self, rec: usize, field: usize) -> Result<i32, String> {
        row_reader::get_ts_nanos(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_is_ts_ltz(&self, _rec: usize, field: usize) -> Result<bool, String> {
        row_reader::is_ts_ltz(&self.columns, field)
    }
    fn plv_get_decimal_str(&self, rec: usize, field: usize) -> Result<String, String> {
        row_reader::get_decimal_str(self.plv_row(rec)?, &self.columns, field)
    }
    fn plv_get_value(&self, rec: usize, field: usize) -> Result<Box<ValueInner>, String> {
        value_from_owned_row(self.plv_row(rec)?, &self.columns, field)
    }
}

// ============================================================================
// ValueInner — recursive value handle for complex reads. Holds an owned Datum
// + its DataType: leaf getters match on the datum, navigators (v_at/v_key_at/
// v_value_at/v_field) descend into a child node.
// ============================================================================

pub struct ValueInner {
    datum: fcore::row::Datum<'static>,
    data_type: fcore::metadata::DataType,
}

/// Builds a `ValueInner` from an owned row; shared by the lookup and prefix-lookup paths.
fn value_from_owned_row(
    row: &GenericRow<'static>,
    columns: &[Column],
    field: usize,
) -> Result<Box<ValueInner>, String> {
    let datum = row
        .values
        .get(field)
        .ok_or_else(|| format!("field index {field} out of range"))?
        .clone();
    let data_type = columns
        .get(field)
        .ok_or_else(|| format!("field index {field} out of range"))?
        .data_type()
        .clone();
    Ok(Box::new(ValueInner { datum, data_type }))
}

impl ValueInner {
    fn type_err(&self, expected: &str) -> String {
        format!("value of type {} is not {expected}", self.data_type)
    }

    fn v_type(&self) -> i32 {
        crate::types::core_data_type_to_ffi(&self.data_type)
    }
    fn v_is_null(&self) -> bool {
        matches!(self.datum, fcore::row::Datum::Null)
    }

    fn v_get_bool(&self) -> Result<bool, String> {
        match &self.datum {
            fcore::row::Datum::Bool(v) => Ok(*v),
            _ => Err(self.type_err("BOOLEAN")),
        }
    }
    fn v_get_i32(&self) -> Result<i32, String> {
        match &self.datum {
            fcore::row::Datum::Int8(v) => Ok(i32::from(*v)),
            fcore::row::Datum::Int16(v) => Ok(i32::from(*v)),
            fcore::row::Datum::Int32(v) => Ok(*v),
            _ => Err(self.type_err("INT")),
        }
    }
    fn v_get_i64(&self) -> Result<i64, String> {
        match &self.datum {
            fcore::row::Datum::Int64(v) => Ok(*v),
            _ => Err(self.type_err("BIGINT")),
        }
    }
    fn v_get_f32(&self) -> Result<f32, String> {
        match &self.datum {
            fcore::row::Datum::Float32(v) => Ok(v.0),
            _ => Err(self.type_err("FLOAT")),
        }
    }
    fn v_get_f64(&self) -> Result<f64, String> {
        match &self.datum {
            fcore::row::Datum::Float64(v) => Ok(v.0),
            _ => Err(self.type_err("DOUBLE")),
        }
    }
    fn v_get_str(&self) -> Result<String, String> {
        match &self.datum {
            fcore::row::Datum::String(v) => Ok(v.to_string()),
            _ => Err(self.type_err("STRING")),
        }
    }
    fn v_get_bytes(&self) -> Result<Vec<u8>, String> {
        match &self.datum {
            fcore::row::Datum::Blob(v) => Ok(v.to_vec()),
            _ => Err(self.type_err("BYTES")),
        }
    }
    fn v_get_date_days(&self) -> Result<i32, String> {
        match &self.datum {
            fcore::row::Datum::Date(d) => Ok(d.get_inner()),
            _ => Err(self.type_err("DATE")),
        }
    }
    fn v_get_time_millis(&self) -> Result<i32, String> {
        match &self.datum {
            fcore::row::Datum::Time(t) => Ok(t.get_inner()),
            _ => Err(self.type_err("TIME")),
        }
    }
    fn v_get_ts_millis(&self) -> Result<i64, String> {
        match &self.datum {
            fcore::row::Datum::TimestampNtz(t) => Ok(t.get_millisecond()),
            fcore::row::Datum::TimestampLtz(t) => Ok(t.get_epoch_millisecond()),
            _ => Err(self.type_err("TIMESTAMP")),
        }
    }
    fn v_get_ts_nanos(&self) -> Result<i32, String> {
        match &self.datum {
            fcore::row::Datum::TimestampNtz(t) => Ok(t.get_nano_of_millisecond()),
            fcore::row::Datum::TimestampLtz(t) => Ok(t.get_nano_of_millisecond()),
            _ => Err(self.type_err("TIMESTAMP")),
        }
    }
    fn v_get_decimal_str(&self) -> Result<String, String> {
        match &self.datum {
            fcore::row::Datum::Decimal(d) => Ok(d.to_big_decimal().to_string()),
            _ => Err(self.type_err("DECIMAL")),
        }
    }

    fn v_size(&self) -> Result<usize, String> {
        match &self.datum {
            fcore::row::Datum::Array(a) => Ok(a.size()),
            fcore::row::Datum::Map(m) => Ok(m.size()),
            _ => Err(self.type_err("ARRAY/MAP")),
        }
    }
    fn v_field_count(&self) -> Result<usize, String> {
        match &self.datum {
            fcore::row::Datum::Row(r) => Ok(r.values.len()),
            _ => Err(self.type_err("ROW")),
        }
    }
    fn v_at(&self, i: usize) -> Result<Box<ValueInner>, String> {
        match &self.datum {
            fcore::row::Datum::Array(a) => {
                let elem_type = match &self.data_type {
                    fcore::metadata::DataType::Array(at) => at.get_element_type().clone(),
                    _ => return Err("internal: ARRAY datum without ARRAY type".to_string()),
                };
                let datum = array_reader::get_datum(a, &elem_type, i)?;
                Ok(Box::new(ValueInner {
                    datum,
                    data_type: elem_type,
                }))
            }
            _ => Err(self.type_err("ARRAY")),
        }
    }
    fn v_key_at(&self, i: usize) -> Result<Box<ValueInner>, String> {
        match &self.datum {
            fcore::row::Datum::Map(m) => {
                let kt = m.key_type().clone();
                let datum = array_reader::get_datum(m.key_array(), &kt, i)?;
                Ok(Box::new(ValueInner {
                    datum,
                    data_type: kt,
                }))
            }
            _ => Err(self.type_err("MAP")),
        }
    }
    fn v_value_at(&self, i: usize) -> Result<Box<ValueInner>, String> {
        match &self.datum {
            fcore::row::Datum::Map(m) => {
                let vt = m.value_type().clone();
                let datum = array_reader::get_datum(m.value_array(), &vt, i)?;
                Ok(Box::new(ValueInner {
                    datum,
                    data_type: vt,
                }))
            }
            _ => Err(self.type_err("MAP")),
        }
    }
    fn v_field(&self, i: usize) -> Result<Box<ValueInner>, String> {
        match &self.datum {
            fcore::row::Datum::Row(r) => {
                let ft = match &self.data_type {
                    fcore::metadata::DataType::Row(rt) => rt
                        .fields()
                        .get(i)
                        .ok_or_else(|| format!("field index {i} out of range"))?
                        .data_type()
                        .clone(),
                    _ => return Err("internal: ROW datum without ROW type".to_string()),
                };
                let datum = r
                    .values
                    .get(i)
                    .ok_or_else(|| format!("field index {i} out of range"))?
                    .clone();
                Ok(Box::new(ValueInner {
                    datum,
                    data_type: ft,
                }))
            }
            _ => Err(self.type_err("ROW")),
        }
    }

    fn v_field_by_name(&self, name: &str) -> Result<Box<ValueInner>, String> {
        match &self.data_type {
            fcore::metadata::DataType::Row(rt) => {
                let idx = rt
                    .fields()
                    .iter()
                    .position(|f| f.name() == name)
                    .ok_or_else(|| format!("no field named '{name}' in {}", self.data_type))?;
                self.v_field(idx)
            }
            _ => Err(self.type_err("ROW")),
        }
    }
}

// ============================================================================
// Opaque types: ArrayWriterInner (array builder for writes)
// ============================================================================

pub struct ArrayWriterInner {
    writer: Option<fcore::row::binary_array::FlussArrayWriter>,
    completed: Option<fcore::row::binary_array::FlussArray>,
    element_type: fcore::metadata::DataType,
    num_elements: usize,
}

// ============================================================================
// MapWriterInner — opaque map builder (mirrors ArrayWriterInner)
// ============================================================================

pub struct MapWriterInner {
    writer: Option<fcore::row::binary_map::FlussMapWriter>,
    completed: Option<fcore::row::binary_map::FlussMap>,
    key_type: fcore::metadata::DataType,
    value_type: fcore::metadata::DataType,
    pending_key: Option<fcore::row::Datum<'static>>,
    pending_value: Option<fcore::row::Datum<'static>>,
}

fn ts_ntz_from(millis: i64, nanos: i32) -> fcore::row::TimestampNtz {
    fcore::row::TimestampNtz::from_millis_nanos(millis, nanos)
        .unwrap_or_else(|_| fcore::row::TimestampNtz::new(millis))
}

fn ts_ltz_from(millis: i64, nanos: i32) -> fcore::row::TimestampLtz {
    fcore::row::TimestampLtz::from_millis_nanos(millis, nanos)
        .unwrap_or_else(|_| fcore::row::TimestampLtz::new(millis))
}

/// Parse a decimal string into a `Datum::Decimal` matching `dt`'s precision and
/// scale. Used by the map key/value decimal setters, which carry no schema.
fn parse_decimal_datum(
    dt: &fcore::metadata::DataType,
    val: &str,
) -> Result<fcore::row::Datum<'static>, String> {
    match dt {
        fcore::metadata::DataType::Decimal(d) => {
            let bd = bigdecimal::BigDecimal::from_str(val).map_err(|e| e.to_string())?;
            let dec = fcore::row::Decimal::from_big_decimal(bd, d.precision(), d.scale())
                .map_err(|e| e.to_string())?;
            Ok(fcore::row::Datum::Decimal(dec))
        }
        other => Err(format!(
            "decimal setter used on non-DECIMAL map type {other}"
        )),
    }
}

fn timestamp_datum(
    dt: &fcore::metadata::DataType,
    millis: i64,
    nanos: i32,
) -> Result<fcore::row::Datum<'static>, String> {
    match dt {
        fcore::metadata::DataType::Timestamp(_) => {
            Ok(fcore::row::Datum::TimestampNtz(ts_ntz_from(millis, nanos)))
        }
        fcore::metadata::DataType::TimestampLTz(_) => {
            Ok(fcore::row::Datum::TimestampLtz(ts_ltz_from(millis, nanos)))
        }
        other => Err(format!(
            "timestamp setter used on non-TIMESTAMP map type {other}"
        )),
    }
}

/// Narrow a C++ `int32` into the `Datum` a map key/value expects, erroring on
/// overflow. Core needs `Int8`/`Int16` for TINYINT/SMALLINT; a widened `Int32`
/// is rejected. Mirrors `ArrayWriterInner::aw_set_i32`.
fn int_datum(dt: &DataType, val: i32, role: &str) -> Result<Datum<'static>, String> {
    match dt {
        DataType::TinyInt(_) => i8::try_from(val)
            .map(Datum::Int8)
            .map_err(|_| format!("Value {val} does not fit TINYINT map {role}")),
        DataType::SmallInt(_) => i16::try_from(val)
            .map(Datum::Int16)
            .map_err(|_| format!("Value {val} does not fit SMALLINT map {role}")),
        _ => Ok(Datum::Int32(val)),
    }
}

#[allow(clippy::too_many_arguments)]
fn new_map_writer(
    capacity: usize,
    key_leaf_type_id: i32,
    key_precision: u32,
    key_scale: u32,
    value_leaf_type_id: i32,
    value_precision: u32,
    value_scale: u32,
    value_array_nesting: u32,
) -> Result<Box<MapWriterInner>, String> {
    let key_type = types::element_type_from_ffi(key_leaf_type_id, key_precision, key_scale, 0)
        .map_err(|e| e.to_string())?;
    let value_type = types::element_type_from_ffi(
        value_leaf_type_id,
        value_precision,
        value_scale,
        value_array_nesting,
    )
    .map_err(|e| e.to_string())?;
    let writer = fcore::row::binary_map::FlussMapWriter::new(capacity, &key_type, &value_type);
    Ok(Box::new(MapWriterInner {
        writer: Some(writer),
        completed: None,
        key_type,
        value_type,
        pending_key: None,
        pending_value: None,
    }))
}

fn new_map_writer_arrow(
    capacity: usize,
    kv_schema_ptr: usize,
) -> Result<Box<MapWriterInner>, String> {
    let mut types =
        unsafe { types::arrow_ffi_to_data_types(kv_schema_ptr) }.map_err(|e| e.to_string())?;
    if types.len() != 2 {
        return Err(format!(
            "map writer Arrow schema must have exactly 2 fields (key, value), got {}",
            types.len()
        ));
    }
    let value_type = types.swap_remove(1);
    let key_type = types.swap_remove(0);
    let writer = fcore::row::binary_map::FlussMapWriter::new(capacity, &key_type, &value_type);
    Ok(Box::new(MapWriterInner {
        writer: Some(writer),
        completed: None,
        key_type,
        value_type,
        pending_key: None,
        pending_value: None,
    }))
}

impl MapWriterInner {
    fn writer_mut(&mut self) -> Result<&mut fcore::row::binary_map::FlussMapWriter, String> {
        self.writer
            .as_mut()
            .ok_or_else(|| "MapWriter is already finalized".to_string())
    }

    fn mw_key_bool(&mut self, val: bool) -> Result<(), String> {
        self.pending_key = Some(fcore::row::Datum::Bool(val));
        Ok(())
    }
    fn mw_key_i32(&mut self, val: i32) -> Result<(), String> {
        self.pending_key = Some(int_datum(&self.key_type, val, "key")?);
        Ok(())
    }
    fn mw_key_i64(&mut self, val: i64) -> Result<(), String> {
        self.pending_key = Some(fcore::row::Datum::Int64(val));
        Ok(())
    }
    fn mw_key_f32(&mut self, val: f32) -> Result<(), String> {
        self.pending_key = Some(fcore::row::Datum::Float32(val.into()));
        Ok(())
    }
    fn mw_key_f64(&mut self, val: f64) -> Result<(), String> {
        self.pending_key = Some(fcore::row::Datum::Float64(val.into()));
        Ok(())
    }
    fn mw_key_str(&mut self, val: &str) -> Result<(), String> {
        self.pending_key = Some(fcore::row::Datum::String(std::borrow::Cow::Owned(
            val.to_string(),
        )));
        Ok(())
    }
    fn mw_key_bytes(&mut self, val: &[u8]) -> Result<(), String> {
        self.pending_key = Some(fcore::row::Datum::Blob(std::borrow::Cow::Owned(
            val.to_vec(),
        )));
        Ok(())
    }
    fn mw_key_date(&mut self, days: i32) -> Result<(), String> {
        self.pending_key = Some(fcore::row::Datum::Date(fcore::row::Date::new(days)));
        Ok(())
    }
    fn mw_key_time(&mut self, millis: i32) -> Result<(), String> {
        self.pending_key = Some(fcore::row::Datum::Time(fcore::row::Time::new(millis)));
        Ok(())
    }
    fn mw_key_timestamp(&mut self, millis: i64, nanos: i32) -> Result<(), String> {
        self.pending_key = Some(timestamp_datum(&self.key_type, millis, nanos)?);
        Ok(())
    }
    fn mw_key_decimal_str(&mut self, val: &str) -> Result<(), String> {
        self.pending_key = Some(parse_decimal_datum(&self.key_type, val)?);
        Ok(())
    }

    fn mw_value_null(&mut self) -> Result<(), String> {
        self.pending_value = Some(fcore::row::Datum::Null);
        Ok(())
    }
    fn mw_value_bool(&mut self, val: bool) -> Result<(), String> {
        self.pending_value = Some(fcore::row::Datum::Bool(val));
        Ok(())
    }
    fn mw_value_i32(&mut self, val: i32) -> Result<(), String> {
        self.pending_value = Some(int_datum(&self.value_type, val, "value")?);
        Ok(())
    }
    fn mw_value_i64(&mut self, val: i64) -> Result<(), String> {
        self.pending_value = Some(fcore::row::Datum::Int64(val));
        Ok(())
    }
    fn mw_value_f32(&mut self, val: f32) -> Result<(), String> {
        self.pending_value = Some(fcore::row::Datum::Float32(val.into()));
        Ok(())
    }
    fn mw_value_f64(&mut self, val: f64) -> Result<(), String> {
        self.pending_value = Some(fcore::row::Datum::Float64(val.into()));
        Ok(())
    }
    fn mw_value_str(&mut self, val: &str) -> Result<(), String> {
        self.pending_value = Some(fcore::row::Datum::String(std::borrow::Cow::Owned(
            val.to_string(),
        )));
        Ok(())
    }
    fn mw_value_bytes(&mut self, val: &[u8]) -> Result<(), String> {
        self.pending_value = Some(fcore::row::Datum::Blob(std::borrow::Cow::Owned(
            val.to_vec(),
        )));
        Ok(())
    }
    fn mw_value_date(&mut self, days: i32) -> Result<(), String> {
        self.pending_value = Some(fcore::row::Datum::Date(fcore::row::Date::new(days)));
        Ok(())
    }
    fn mw_value_time(&mut self, millis: i32) -> Result<(), String> {
        self.pending_value = Some(fcore::row::Datum::Time(fcore::row::Time::new(millis)));
        Ok(())
    }
    fn mw_value_timestamp(&mut self, millis: i64, nanos: i32) -> Result<(), String> {
        self.pending_value = Some(timestamp_datum(&self.value_type, millis, nanos)?);
        Ok(())
    }
    fn mw_value_decimal_str(&mut self, val: &str) -> Result<(), String> {
        self.pending_value = Some(parse_decimal_datum(&self.value_type, val)?);
        Ok(())
    }
    fn mw_value_row(&mut self, row: &mut GenericRowInner) -> Result<(), String> {
        let nested = std::mem::replace(&mut row.row, fcore::row::GenericRow::new(0));
        self.pending_value = Some(fcore::row::Datum::Row(Box::new(nested)));
        Ok(())
    }
    fn mw_value_map(&mut self, map: &mut MapWriterInner) -> Result<(), String> {
        map.complete_if_needed()?;
        let completed = map
            .completed
            .take()
            .ok_or_else(|| "MapWriter invariant violation: nested map missing".to_string())?;
        self.pending_value = Some(fcore::row::Datum::Map(completed));
        Ok(())
    }
    fn mw_value_array(&mut self, array: &mut ArrayWriterInner) -> Result<(), String> {
        array.complete_if_needed()?;
        let completed = array
            .completed
            .take()
            .ok_or_else(|| "ArrayWriter invariant violation: nested array missing".to_string())?;
        self.pending_value = Some(fcore::row::Datum::Array(completed));
        Ok(())
    }

    /// Commit the pending key/value as one map entry. Keys cannot be null;
    /// an unset value defaults to null.
    fn mw_commit(&mut self) -> Result<(), String> {
        let key = self
            .pending_key
            .take()
            .ok_or_else(|| "MapWriter: entry key not set before commit".to_string())?;
        let value = self.pending_value.take().unwrap_or(fcore::row::Datum::Null);
        self.writer_mut()?
            .write_entry(key, value)
            .map_err(|e| e.to_string())
    }

    fn complete_if_needed(&mut self) -> Result<(), String> {
        if self.completed.is_none() {
            let w = self
                .writer
                .take()
                .ok_or_else(|| "MapWriter has already been finalized".to_string())?;
            self.completed = Some(w.complete().map_err(|e| e.to_string())?);
        }
        Ok(())
    }
}

fn new_array_writer(
    size: usize,
    element_leaf_type_id: i32,
    precision: u32,
    scale: u32,
    array_nesting: u32,
) -> Result<Box<ArrayWriterInner>, String> {
    let element_type =
        types::element_type_from_ffi(element_leaf_type_id, precision, scale, array_nesting)
            .map_err(|e| e.to_string())?;
    let writer = fcore::row::binary_array::FlussArrayWriter::new(size, &element_type);
    Ok(Box::new(ArrayWriterInner {
        writer: Some(writer),
        completed: None,
        element_type,
        num_elements: size,
    }))
}

fn new_array_writer_arrow(
    size: usize,
    element_schema_ptr: usize,
) -> Result<Box<ArrayWriterInner>, String> {
    let mut types =
        unsafe { types::arrow_ffi_to_data_types(element_schema_ptr) }.map_err(|e| e.to_string())?;
    let element_type = if types.is_empty() {
        return Err("array element Arrow schema must have one field".to_string());
    } else {
        types.swap_remove(0)
    };
    let writer = fcore::row::binary_array::FlussArrayWriter::new(size, &element_type);
    Ok(Box::new(ArrayWriterInner {
        writer: Some(writer),
        completed: None,
        element_type,
        num_elements: size,
    }))
}

impl ArrayWriterInner {
    fn writer_mut(&mut self) -> Result<&mut fcore::row::binary_array::FlussArrayWriter, String> {
        self.writer
            .as_mut()
            .ok_or_else(|| "ArrayWriter is already finalized".to_string())
    }

    fn validate_index(&self, idx: usize) -> Result<(), String> {
        if idx < self.num_elements {
            Ok(())
        } else {
            Err(format!(
                "ArrayWriter index out of bounds: idx={idx}, size={}",
                self.num_elements
            ))
        }
    }

    fn complete_if_needed(&mut self) -> Result<(), String> {
        if self.completed.is_none() {
            let w = self
                .writer
                .take()
                .ok_or_else(|| "ArrayWriter has already been finalized".to_string())?;
            self.completed = Some(w.complete().map_err(|e| e.to_string())?);
        }
        Ok(())
    }

    /// Checks writer liveness first, then the element index. Returning the
    /// clearest finalization error before a bounds error keeps diagnostics
    /// aligned with the caller's intent when a writer is misused after
    /// completion.
    fn ensure_writable(&self, idx: usize) -> Result<(), String> {
        if self.writer.is_none() {
            return Err("ArrayWriter is already finalized".to_string());
        }
        self.validate_index(idx)
    }

    fn aw_size(&self) -> usize {
        self.num_elements
    }

    fn aw_set_null(&mut self, idx: usize) -> Result<(), String> {
        self.ensure_writable(idx)?;
        self.writer_mut()?.set_null_at(idx);
        Ok(())
    }

    fn aw_set_bool(&mut self, idx: usize, val: bool) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(self.element_type, fcore::metadata::DataType::Boolean(_)) {
            return Err(format!(
                "ArrayWriter type mismatch: expected BOOLEAN element, got {}",
                self.element_type
            ));
        }
        self.writer_mut()?.write_boolean(idx, val);
        Ok(())
    }

    fn aw_set_i32(&mut self, idx: usize, val: i32) -> Result<(), String> {
        self.ensure_writable(idx)?;
        match &self.element_type {
            fcore::metadata::DataType::TinyInt(_) => {
                let v = i8::try_from(val)
                    .map_err(|_| format!("Value {val} does not fit TINYINT element"))?;
                self.writer_mut()?.write_byte(idx, v);
            }
            fcore::metadata::DataType::SmallInt(_) => {
                let v = i16::try_from(val)
                    .map_err(|_| format!("Value {val} does not fit SMALLINT element"))?;
                self.writer_mut()?.write_short(idx, v);
            }
            fcore::metadata::DataType::Int(_) => {
                self.writer_mut()?.write_int(idx, val);
            }
            _ => {
                return Err(format!(
                    "ArrayWriter type mismatch: expected TINYINT/SMALLINT/INT element, got {}",
                    self.element_type
                ));
            }
        }
        Ok(())
    }

    fn aw_set_i64(&mut self, idx: usize, val: i64) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(self.element_type, fcore::metadata::DataType::BigInt(_)) {
            return Err(format!(
                "ArrayWriter type mismatch: expected BIGINT element, got {}",
                self.element_type
            ));
        }
        self.writer_mut()?.write_long(idx, val);
        Ok(())
    }

    fn aw_set_f32(&mut self, idx: usize, val: f32) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(self.element_type, fcore::metadata::DataType::Float(_)) {
            return Err(format!(
                "ArrayWriter type mismatch: expected FLOAT element, got {}",
                self.element_type
            ));
        }
        self.writer_mut()?.write_float(idx, val);
        Ok(())
    }

    fn aw_set_f64(&mut self, idx: usize, val: f64) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(self.element_type, fcore::metadata::DataType::Double(_)) {
            return Err(format!(
                "ArrayWriter type mismatch: expected DOUBLE element, got {}",
                self.element_type
            ));
        }
        self.writer_mut()?.write_double(idx, val);
        Ok(())
    }

    fn aw_set_str(&mut self, idx: usize, val: &str) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(
            self.element_type,
            fcore::metadata::DataType::String(_) | fcore::metadata::DataType::Char(_)
        ) {
            return Err(format!(
                "ArrayWriter type mismatch: expected STRING/CHAR element, got {}",
                self.element_type
            ));
        }
        self.writer_mut()?.write_string(idx, val);
        Ok(())
    }

    fn aw_set_bytes(&mut self, idx: usize, val: &[u8]) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(
            self.element_type,
            fcore::metadata::DataType::Bytes(_) | fcore::metadata::DataType::Binary(_)
        ) {
            return Err(format!(
                "ArrayWriter type mismatch: expected BYTES/BINARY element, got {}",
                self.element_type
            ));
        }
        self.writer_mut()?.write_binary_bytes(idx, val);
        Ok(())
    }

    fn aw_set_date(&mut self, idx: usize, days: i32) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(self.element_type, fcore::metadata::DataType::Date(_)) {
            return Err(format!(
                "ArrayWriter type mismatch: expected DATE element, got {}",
                self.element_type
            ));
        }
        self.writer_mut()?
            .write_date(idx, fcore::row::Date::new(days));
        Ok(())
    }

    fn aw_set_time(&mut self, idx: usize, millis: i32) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(self.element_type, fcore::metadata::DataType::Time(_)) {
            return Err(format!(
                "ArrayWriter type mismatch: expected TIME element, got {}",
                self.element_type
            ));
        }
        self.writer_mut()?
            .write_time(idx, fcore::row::Time::new(millis));
        Ok(())
    }

    fn aw_set_ts_ntz(&mut self, idx: usize, millis: i64, nanos: i32) -> Result<(), String> {
        self.ensure_writable(idx)?;
        let precision = match &self.element_type {
            fcore::metadata::DataType::Timestamp(ts) => ts.precision(),
            _ => {
                return Err(format!(
                    "ArrayWriter type mismatch: expected TIMESTAMP element, got {}",
                    self.element_type
                ));
            }
        };
        let ts = fcore::row::TimestampNtz::from_millis_nanos(millis, nanos)
            .map_err(|e| e.to_string())?;
        self.writer_mut()?.write_timestamp_ntz(idx, &ts, precision);
        Ok(())
    }

    fn aw_set_ts_ltz(&mut self, idx: usize, millis: i64, nanos: i32) -> Result<(), String> {
        self.ensure_writable(idx)?;
        let precision = match &self.element_type {
            fcore::metadata::DataType::TimestampLTz(ts) => ts.precision(),
            _ => {
                return Err(format!(
                    "ArrayWriter type mismatch: expected TIMESTAMP_LTZ element, got {}",
                    self.element_type
                ));
            }
        };
        let ts = fcore::row::TimestampLtz::from_millis_nanos(millis, nanos)
            .map_err(|e| e.to_string())?;
        self.writer_mut()?.write_timestamp_ltz(idx, &ts, precision);
        Ok(())
    }

    fn aw_set_decimal_str(&mut self, idx: usize, val: &str) -> Result<(), String> {
        self.ensure_writable(idx)?;
        let (precision, scale) = match &self.element_type {
            fcore::metadata::DataType::Decimal(d) => (d.precision(), d.scale()),
            _ => {
                return Err(format!(
                    "ArrayWriter type mismatch: expected DECIMAL element, got {}",
                    self.element_type
                ));
            }
        };
        let bd = bigdecimal::BigDecimal::from_str(val).map_err(|e| e.to_string())?;
        let decimal = fcore::row::Decimal::from_big_decimal(bd, precision, scale)
            .map_err(|e| e.to_string())?;
        self.writer_mut()?.write_decimal(idx, &decimal, precision);
        Ok(())
    }

    fn aw_set_array(&mut self, idx: usize, nested: &mut ArrayWriterInner) -> Result<(), String> {
        self.ensure_writable(idx)?;
        let expected_inner = match &self.element_type {
            fcore::metadata::DataType::Array(at) => at.get_element_type(),
            _ => {
                return Err(format!(
                    "ArrayWriter type mismatch: expected ARRAY element, got {}",
                    self.element_type
                ));
            }
        };
        if !structurally_compatible(expected_inner, &nested.element_type) {
            return Err(format!(
                "Nested ArrayWriter type mismatch: expected nested element type {}, got {}",
                expected_inner, nested.element_type
            ));
        }
        nested.complete_if_needed()?;
        let arr = nested.completed.as_ref().ok_or_else(|| {
            "ArrayWriter invariant violation: nested completed array missing after finalize"
                .to_string()
        })?;
        self.writer_mut()?.write_array(idx, arr);
        Ok(())
    }

    fn aw_set_row(&mut self, idx: usize, row: &mut GenericRowInner) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(self.element_type, fcore::metadata::DataType::Row(_)) {
            return Err(format!(
                "ArrayWriter type mismatch: expected ROW element, got {}",
                self.element_type
            ));
        }
        self.writer_mut()?
            .write_row(idx, &row.row)
            .map_err(|e| e.to_string())
    }

    fn aw_set_map(&mut self, idx: usize, map: &mut MapWriterInner) -> Result<(), String> {
        self.ensure_writable(idx)?;
        if !matches!(self.element_type, fcore::metadata::DataType::Map(_)) {
            return Err(format!(
                "ArrayWriter type mismatch: expected MAP element, got {}",
                self.element_type
            ));
        }
        map.complete_if_needed()?;
        let completed = map.completed.as_ref().ok_or_else(|| {
            "ArrayWriter invariant violation: nested completed map missing after finalize"
                .to_string()
        })?;
        self.writer_mut()?.write_map(idx, completed);
        Ok(())
    }
}

/// Structural type equivalence that ignores nullability flags but preserves
/// variant and precision/scale semantics. Used to compare ArrayWriter element
/// types on the binding boundary. Nullability is ignored in structural comparison
/// because the Rust-side element type is always reconstructed as nullable
/// (encoding doesn't depend on it).
fn structurally_compatible(a: &fcore::metadata::DataType, b: &fcore::metadata::DataType) -> bool {
    match (a, b) {
        (DataType::Boolean(_), DataType::Boolean(_))
        | (DataType::TinyInt(_), DataType::TinyInt(_))
        | (DataType::SmallInt(_), DataType::SmallInt(_))
        | (DataType::Int(_), DataType::Int(_))
        | (DataType::BigInt(_), DataType::BigInt(_))
        | (DataType::Float(_), DataType::Float(_))
        | (DataType::Double(_), DataType::Double(_))
        | (DataType::String(_), DataType::String(_))
        | (DataType::Bytes(_), DataType::Bytes(_))
        | (DataType::Date(_), DataType::Date(_))
        | (DataType::Time(_), DataType::Time(_)) => true,
        (DataType::Timestamp(x), DataType::Timestamp(y)) => x.precision() == y.precision(),
        (DataType::TimestampLTz(x), DataType::TimestampLTz(y)) => x.precision() == y.precision(),
        (DataType::Char(x), DataType::Char(y)) => x.length() == y.length(),
        (DataType::Binary(x), DataType::Binary(y)) => x.length() == y.length(),
        (DataType::Decimal(x), DataType::Decimal(y)) => {
            x.precision() == y.precision() && x.scale() == y.scale()
        }
        (DataType::Array(x), DataType::Array(y)) => {
            structurally_compatible(x.get_element_type(), y.get_element_type())
        }
        _ => false,
    }
}
