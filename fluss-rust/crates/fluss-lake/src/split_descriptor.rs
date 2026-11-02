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

use crate::read_context::SourceDescriptor;
use crate::{FlussLakeError, Result};
use fluss::metadata::{PartitionSpec, TableInfo};
use fluss::metadata::{TableBucket, TablePath};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Frozen execution state for one logical `(partition, bucket)` split.
///
/// Projection, filtering and read mode belong to the immutable reader configuration.
/// Lake task payloads are encoded and interpreted only by their LakeSource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SplitDescriptor {
    source: SourceDescriptor,
    table_bucket: TableBucket,
    start_offset: i64,
    stop_offset: i64,
    lake_splits: Vec<crate::LakeSplit>,
    primary_key_indexes: Vec<usize>,
}

impl SplitDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        table_info: &TableInfo,
        table_bucket: TableBucket,
        start_offset: i64,
        stop_offset: i64,
        snapshot_id: Option<i64>,
        lake_splits: Vec<crate::LakeSplit>,
        primary_key_indexes: Vec<usize>,
    ) -> Result<Self> {
        let descriptor = Self {
            source: SourceDescriptor::capture(
                table_info,
                snapshot_id,
                &PartitionSpec::new(std::collections::HashMap::<String, String>::new()),
            )?,
            table_bucket,
            start_offset,
            stop_offset,
            lake_splits,
            primary_key_indexes,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub(crate) fn table_info(&self, live: &TableInfo) -> Result<TableInfo> {
        self.source.table_info(live)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let Self {
            source,
            table_bucket,
            start_offset,
            stop_offset,
            lake_splits,
            primary_key_indexes,
        } = self;
        source.validate()?;
        let partitioned = source.is_partitioned();
        let snapshot_id = source.snapshot_id();
        if source.table_id() != table_bucket.table_id() {
            return Err(invalid_descriptor(
                "frozen source does not match task table",
            ));
        }
        if let Some(partition_id) = table_bucket.partition_id() {
            if !partitioned {
                return Err(invalid_descriptor(
                    "an unpartitioned split must not carry a live partition id",
                ));
            }
            if partition_id < 0 {
                return Err(invalid_descriptor(format!(
                    "partition id must be non-negative, got {partition_id}"
                )));
            }
        }
        if table_bucket.bucket_id() < 0
            && !(self.is_append_lake() && table_bucket.bucket_id() == -1)
        {
            return Err(invalid_descriptor(format!(
                "bucket id must be non-negative, got {}",
                table_bucket.bucket_id()
            )));
        }
        if *start_offset < 0 || *stop_offset < 0 || start_offset > stop_offset {
            return Err(invalid_descriptor(format!(
                "logical changelog range is invalid: [{start_offset}, {stop_offset})"
            )));
        }
        if partitioned && table_bucket.partition_id().is_none() && start_offset != stop_offset {
            return Err(invalid_descriptor(
                "a partition that no longer exists in Fluss cannot carry a log range",
            ));
        }
        if !lake_splits.is_empty() && snapshot_id.is_none() {
            return Err(invalid_descriptor(
                "lake splits require a pinned snapshot id",
            ));
        }
        for task in lake_splits {
            if task.format.is_empty()
                || Some(task.snapshot_id) != snapshot_id
                || task.bucket_id != table_bucket.bucket_id()
                || !task.has_valid_bucket()
                || (self.is_primary_key() && task.bucket_id < 0)
                || task.bucket_count != lake_splits[0].bucket_count
                || task.payload_version == 0
                || task.payload.is_empty()
            {
                return Err(invalid_descriptor(
                    "lake task identity or payload does not match the logical split",
                ));
            }
        }
        let mut seen = HashSet::with_capacity(primary_key_indexes.len());
        if primary_key_indexes.iter().any(|index| !seen.insert(*index)) {
            return Err(invalid_descriptor(
                "primary-key indexes must not contain duplicates",
            ));
        }

        if !self.is_primary_key() && !lake_splits.is_empty() && !self.is_append_lake() {
            return Err(invalid_descriptor(
                "append lake tasks must be independent of log ranges",
            ));
        }
        Ok(())
    }

    pub(crate) fn table_path(&self) -> &TablePath {
        self.source.table_path()
    }

    pub(crate) fn schema_id(&self) -> i32 {
        self.source.schema_id()
    }

    pub(crate) fn is_partitioned(&self) -> bool {
        self.source.is_partitioned()
    }

    pub(crate) fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    pub(crate) fn start_offset(&self) -> i64 {
        self.start_offset
    }

    pub(crate) fn stop_offset(&self) -> i64 {
        self.stop_offset
    }

    pub(crate) fn snapshot_id(&self) -> Option<i64> {
        self.source.snapshot_id()
    }

    pub(crate) fn lake_splits(&self) -> &[crate::LakeSplit] {
        &self.lake_splits
    }

    pub(crate) fn primary_key_indexes(&self) -> &[usize] {
        &self.primary_key_indexes
    }

    pub(crate) fn is_primary_key(&self) -> bool {
        !self.primary_key_indexes.is_empty()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.start_offset == self.stop_offset && self.lake_splits.is_empty()
    }

    pub(crate) fn is_append_lake(&self) -> bool {
        !self.is_primary_key()
            && self.lake_splits.len() == 1
            && self.start_offset == self.stop_offset
    }

    #[cfg(test)]
    fn decode(encoded: &[u8]) -> Result<Self> {
        let value: Self =
            serde_json::from_slice(encoded).map_err(|e| invalid_descriptor(e.to_string()))?;
        value.validate()?;
        Ok(value)
    }
}

fn invalid_descriptor(message: impl Into<String>) -> FlussLakeError {
    FlussLakeError::Internal(message.into())
}

#[cfg(test)]
pub(crate) fn testing_table(
    path: TablePath,
    schema_id: i32,
    partitioned: bool,
    table_id: i64,
) -> TableInfo {
    use fluss::metadata::{DataTypes, Schema};
    use std::collections::HashMap;
    let schema = Schema::builder()
        .column("id", DataTypes::int())
        .column("region", DataTypes::string())
        .build()
        .unwrap();
    TableInfo::new(
        path,
        table_id,
        schema_id,
        schema,
        vec![],
        if partitioned {
            vec!["region".into()]
        } else {
            vec![]
        }
        .into(),
        4,
        HashMap::from([
            ("table.datalake.enabled".into(), "true".into()),
            ("table.datalake.format".into(), "paimon".into()),
        ]),
        HashMap::new(),
        None,
        0,
        0,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn descriptor() -> SplitDescriptor {
        SplitDescriptor::try_new(
            &crate::split_descriptor::testing_table(
                TablePath::new("fluss", "orders"),
                3,
                false,
                (TableBucket::new(7, 0)).table_id(),
            ),
            TableBucket::new(7, 0),
            12,
            20,
            Some(42),
            vec![crate::source::testing_split()],
            vec![0],
        )
        .unwrap()
    }

    #[test]
    fn descriptor_revalidates_identity_ranges_payloads_and_keys() {
        for (field, invalid) in [
            ("schema_id", json!(-1)),
            ("start_offset", json!(21)),
            ("stop_offset", json!(-1)),
            ("snapshot_id", json!(null)),
            ("primary_key_indexes", json!([0, 0])),
            ("unknown", json!(true)),
            ("lake_splits", json!([{}])),
        ] {
            let mut value = serde_json::to_value(descriptor()).unwrap();
            if field == "schema_id" {
                value["source"][field] = invalid;
            } else if field == "snapshot_id" {
                value["source"]["lake"]["snapshot_id"] = invalid;
            } else {
                value[field] = invalid;
            }
            assert!(
                SplitDescriptor::decode(&serde_json::to_vec(&value).unwrap()).is_err(),
                "{field}"
            );
        }
        for (field, invalid) in [
            ("bucket_id", json!(1)),
            ("bucket_count", json!(0)),
            ("bucket_count", json!(-1)),
            ("snapshot_id", json!(43)),
            ("payload", json!([])),
            ("payload_version", json!(0)),
        ] {
            let mut value = serde_json::to_value(descriptor()).unwrap();
            value["lake_splits"][0][field] = invalid;
            assert!(
                SplitDescriptor::decode(&serde_json::to_vec(&value).unwrap()).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn expired_partition_cannot_carry_a_tail() {
        assert!(
            SplitDescriptor::try_new(
                &crate::split_descriptor::testing_table(
                    TablePath::new("fluss", "orders"),
                    1,
                    true,
                    (TableBucket::new(7, 0)).table_id()
                ),
                TableBucket::new(7, 0),
                1,
                2,
                None,
                vec![],
                vec![],
            )
            .is_err()
        );
        assert!(
            SplitDescriptor::try_new(
                &crate::split_descriptor::testing_table(
                    TablePath::new("fluss", "orders"),
                    1,
                    true,
                    (TableBucket::new(7, 0)).table_id()
                ),
                TableBucket::new(7, 0),
                0,
                0,
                Some(42),
                vec![crate::source::testing_split()],
                vec![],
            )
            .is_ok()
        );
    }

    #[test]
    fn bucket_unaware_append_task_never_becomes_a_log_or_pk_task() {
        let mut value = serde_json::to_value(descriptor()).unwrap();
        value["primary_key_indexes"] = json!([]);
        value["start_offset"] = json!(0);
        value["stop_offset"] = json!(0);
        value["table_bucket"]["bucket"] = json!(-1);
        value["lake_splits"][0]["bucket_id"] = json!(-1);
        value["lake_splits"][0]["bucket_count"] = json!(-1);
        let decoded = SplitDescriptor::decode(&serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(decoded.is_append_lake());
        for (key, invalid) in [
            ("stop_offset", json!(1)),
            ("primary_key_indexes", json!([0])),
            ("lake_splits", json!([])),
        ] {
            let mut invalid_value = value.clone();
            invalid_value[key] = invalid;
            assert!(SplitDescriptor::decode(&serde_json::to_vec(&invalid_value).unwrap()).is_err());
        }
        for count in [0, 4] {
            let mut invalid = value.clone();
            invalid["lake_splits"][0]["bucket_count"] = json!(count);
            assert!(SplitDescriptor::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
    }
}
