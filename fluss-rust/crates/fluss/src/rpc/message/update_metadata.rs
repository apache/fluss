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

use crate::metadata::{PhysicalTablePath, TablePath};
use crate::proto::{MetadataResponse, PbPhysicalTablePath, PbTablePath};
use crate::rpc::api_key::ApiKey;
use crate::rpc::frame::ReadError;
use crate::rpc::frame::WriteError;
use crate::rpc::message::{ReadType, RequestBody, WriteType};
use std::collections::HashSet;
use std::sync::Arc;

use crate::{impl_read_type, impl_write_type, proto};
use bytes::{Buf, BufMut};
use prost::Message;

pub struct UpdateMetadataRequest {
    pub(crate) inner_request: proto::MetadataRequest,
}

impl UpdateMetadataRequest {
    pub fn new(
        table_paths: &HashSet<&TablePath>,
        physical_table_paths: &HashSet<&Arc<PhysicalTablePath>>,
        partition_ids: Vec<i64>,
    ) -> Self {
        // Table paths destined for the `table_path` field, deduplicated so a
        // path present both here and as a non-partitioned physical path below
        // is only sent once.
        let mut seen_tables: HashSet<(&str, &str)> = HashSet::new();
        let mut pb_table_paths: Vec<PbTablePath> = Vec::new();
        for path in table_paths {
            if seen_tables.insert((path.database(), path.table())) {
                pb_table_paths.push(PbTablePath {
                    database_name: path.database().to_string(),
                    table_name: path.table().to_string(),
                });
            }
        }

        let mut pb_partition_paths: Vec<PbPhysicalTablePath> = Vec::new();
        for path in physical_table_paths {
            match path.get_partition_name() {
                // A real partition path belongs in `partitions_path`.
                Some(partition_name) => pb_partition_paths.push(PbPhysicalTablePath {
                    database_name: path.get_database_name().to_string(),
                    table_name: path.get_table_name().to_string(),
                    partition_name: Some(partition_name.to_string()),
                }),
                // A non-partitioned table has no partition name. Sending it in
                // `partitions_path` makes the server resolve it as a partition and
                // hit a NullPointerException in ZooKeeperClient.getPartitionIds,
                // which fails the whole metadata response. Route it to
                // `table_path` instead.
                None => {
                    if seen_tables.insert((path.get_database_name(), path.get_table_name())) {
                        pb_table_paths.push(PbTablePath {
                            database_name: path.get_database_name().to_string(),
                            table_name: path.get_table_name().to_string(),
                        });
                    }
                }
            }
        }

        UpdateMetadataRequest {
            inner_request: proto::MetadataRequest {
                table_path: pb_table_paths,
                partitions_path: pb_partition_paths,
                partitions_id: partition_ids,
            },
        }
    }
}

impl RequestBody for UpdateMetadataRequest {
    type ResponseBody = MetadataResponse;

    const API_KEY: ApiKey = ApiKey::MetaData;
}

impl_write_type!(UpdateMetadataRequest);
impl_read_type!(MetadataResponse);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_partitioned_physical_path_is_sent_as_table_not_partition() {
        let physical = Arc::new(PhysicalTablePath::of(Arc::new(TablePath::new(
            "db",
            "non_partitioned",
        ))));
        let physical_set: HashSet<&Arc<PhysicalTablePath>> = std::iter::once(&physical).collect();

        let inner =
            UpdateMetadataRequest::new(&HashSet::new(), &physical_set, vec![]).inner_request;

        assert!(
            inner.partitions_path.is_empty(),
            "a non-partitioned path must never be sent as a partition"
        );
        assert_eq!(inner.table_path.len(), 1);
        assert_eq!(inner.table_path[0].database_name, "db");
        assert_eq!(inner.table_path[0].table_name, "non_partitioned");
    }

    #[test]
    fn partitioned_physical_path_is_sent_as_partition() {
        let physical = Arc::new(PhysicalTablePath::of_with_names(
            "db",
            "partitioned",
            Some("dt=2026"),
        ));
        let physical_set: HashSet<&Arc<PhysicalTablePath>> = std::iter::once(&physical).collect();

        let inner =
            UpdateMetadataRequest::new(&HashSet::new(), &physical_set, vec![]).inner_request;

        assert_eq!(inner.partitions_path.len(), 1);
        assert_eq!(
            inner.partitions_path[0].partition_name.as_deref(),
            Some("dt=2026")
        );
        assert!(inner.table_path.is_empty());
    }

    #[test]
    fn table_is_not_duplicated_when_present_as_both_table_and_physical() {
        let table_path = Arc::new(TablePath::new("db", "t"));
        let table_set: HashSet<&TablePath> = std::iter::once(table_path.as_ref()).collect();
        let physical = Arc::new(PhysicalTablePath::of(Arc::clone(&table_path)));
        let physical_set: HashSet<&Arc<PhysicalTablePath>> = std::iter::once(&physical).collect();

        let inner = UpdateMetadataRequest::new(&table_set, &physical_set, vec![]).inner_request;

        assert_eq!(inner.table_path.len(), 1);
        assert!(inner.partitions_path.is_empty());
    }
}
