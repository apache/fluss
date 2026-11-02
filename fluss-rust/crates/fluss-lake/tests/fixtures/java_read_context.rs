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

// Captured from Java UnionReadContextEncoder.encode(...).
const JAVA_READ_CONTEXT: &str = r#"{
  "context_version": 1,
  "table_path": {"database": "fluss", "table": "orders"},
  "lake": {"table_path": {"database": "lake_db", "table": "orders_lake"}, "format": "paimon", "snapshot_id": 42},
  "table_id": 7,
  "table_modified_time": 0,
  "schema_id": 2,
  "schema": {"version": 1, "columns": [{"name": "id", "data_type": {"type": "INTEGER", "nullable": false}, "id": 0}, {"name": "region", "data_type": {"type": "STRING", "nullable": false}, "id": 1}, {"name": "amount", "data_type": {"type": "BIGINT"}, "id": 2}], "primary_key": ["id", "region"], "highest_field_id": 2},
  "partition_keys": ["region"],
  "partition_filter": {},
  "bucket_keys": ["id"],
  "num_buckets": 2,
  "merge_engine": null,
  "log_ranges": [
    {"table_bucket": {"table_id": 7, "bucket": 0, "partition_id": 9}, "partition": {"kind": "ordinary", "identity": {"key_values": [["region", "US"]]}}, "bucket_count": 2, "baseline": {"kind": "lake"}, "start_offset": 12, "stop_offset": 9007199254740993, "earliest_offset": 8},
    {"table_bucket": {"table_id": 7, "bucket": 1, "partition_id": 9}, "partition": {"kind": "ordinary", "identity": {"key_values": [["region", "US"]]}}, "bucket_count": 2, "baseline": {"kind": "lake"}, "start_offset": 12, "stop_offset": 9007199254740993, "earliest_offset": 8},
    {"table_bucket": {"table_id": 7, "bucket": 0, "partition_id": 10}, "partition": {"kind": "ordinary", "identity": {"key_values": [["region", "欧洲\"\n"]]}}, "bucket_count": 4, "baseline": {"kind": "lake"}, "start_offset": 12, "stop_offset": 9007199254740993, "earliest_offset": 8},
    {"table_bucket": {"table_id": 7, "bucket": 1, "partition_id": 10}, "partition": {"kind": "ordinary", "identity": {"key_values": [["region", "欧洲\"\n"]]}}, "bucket_count": 4, "baseline": {"kind": "lake"}, "start_offset": 12, "stop_offset": 9007199254740993, "earliest_offset": 8},
    {"table_bucket": {"table_id": 7, "bucket": 2, "partition_id": 10}, "partition": {"kind": "ordinary", "identity": {"key_values": [["region", "欧洲\"\n"]]}}, "bucket_count": 4, "baseline": {"kind": "lake"}, "start_offset": 12, "stop_offset": 9007199254740993, "earliest_offset": 8},
    {"table_bucket": {"table_id": 7, "bucket": 3, "partition_id": 10}, "partition": {"kind": "ordinary", "identity": {"key_values": [["region", "欧洲\"\n"]]}}, "bucket_count": 4, "baseline": {"kind": "none"}, "start_offset": 8, "stop_offset": 9007199254740993, "earliest_offset": 8}
  ]
}"#;
