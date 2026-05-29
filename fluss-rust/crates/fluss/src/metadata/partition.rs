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

use crate::error::{Error, Result};
use crate::proto::{PbKeyValue, PbPartitionInfo, PbPartitionSpec};
use crate::{PartitionId, TableId};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Represents a partition spec in fluss. Partition columns and values are NOT of strict order, and
/// they need to be re-arranged to the correct order by comparing with a list of strictly ordered
/// partition keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionSpec {
    partition_spec: HashMap<String, String>,
}

impl PartitionSpec {
    pub fn new<K: Into<String>, V: Into<String>>(partition_spec: HashMap<K, V>) -> Self {
        let mut new_map = HashMap::new();
        for (k, v) in partition_spec {
            new_map.insert(k.into(), v.into());
        }
        Self {
            partition_spec: new_map,
        }
    }

    pub fn get_spec_map(&self) -> &HashMap<String, String> {
        &self.partition_spec
    }

    pub fn to_pb(&self) -> PbPartitionSpec {
        PbPartitionSpec {
            partition_key_values: self
                .partition_spec
                .iter()
                .map(|(k, v)| PbKeyValue {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
        }
    }

    pub fn from_pb(pb: &PbPartitionSpec) -> Self {
        let partition_spec = pb
            .partition_key_values
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        Self { partition_spec }
    }
}

impl Display for PartitionSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionSpec{{{:?}}}", self.partition_spec)
    }
}

/// Represents a partition, which is the resolved version of PartitionSpec. The partition
/// spec is re-arranged into the correct order by comparing it with a list of strictly ordered
/// partition keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResolvedPartitionSpec {
    partition_keys: Arc<[String]>,
    partition_values: Vec<String>,
}

pub const PARTITION_SPEC_SEPARATOR: &str = "$";

impl ResolvedPartitionSpec {
    pub fn new(partition_keys: Arc<[String]>, partition_values: Vec<String>) -> Result<Self> {
        if partition_keys.len() != partition_values.len() {
            return Err(Error::IllegalArgument {
                message: "The number of partition keys and partition values should be the same."
                    .to_string(),
            });
        }

        Ok(Self {
            partition_keys,
            partition_values,
        })
    }

    pub fn from_partition_spec(
        partition_keys: Arc<[String]>,
        partition_spec: &PartitionSpec,
    ) -> Self {
        let partition_values =
            Self::get_reordered_partition_values(&partition_keys, partition_spec);
        Self {
            partition_keys,
            partition_values,
        }
    }

    pub fn from_partition_name(partition_keys: Arc<[String]>, partition_name: &str) -> Self {
        let partition_values: Vec<String> = partition_name
            .split(PARTITION_SPEC_SEPARATOR)
            .map(|s| s.to_string())
            .collect();
        Self {
            partition_keys,
            partition_values,
        }
    }

    pub fn from_partition_qualified_name(qualified_partition_name: &str) -> Result<Self> {
        let mut keys = Vec::new();
        let mut values = Vec::new();

        for pair in qualified_partition_name.split('/') {
            let parts: Vec<&str> = pair.splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(Error::IllegalArgument {
                    message: format!(
                        "Invalid partition name format. Expected key=value, got: {pair}"
                    ),
                });
            }
            keys.push(parts[0].to_string());
            values.push(parts[1].to_string());
        }

        Ok(Self {
            partition_keys: Arc::from(keys),
            partition_values: values,
        })
    }

    pub fn get_partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    pub fn get_partition_values(&self) -> &[String] {
        &self.partition_values
    }

    pub fn to_partition_spec(&self) -> PartitionSpec {
        let mut spec_map = HashMap::new();
        for (i, key) in self.partition_keys.iter().enumerate() {
            spec_map.insert(key.clone(), self.partition_values[i].clone());
        }
        PartitionSpec::new(spec_map)
    }

    /// Generate the partition name for a partition table with specified partition values.
    ///
    /// The partition name is in the following format: value1$value2$...$valueN
    pub fn get_partition_name(&self) -> String {
        self.partition_values.join(PARTITION_SPEC_SEPARATOR)
    }

    /// Returns the qualified partition name for a partition spec.
    /// The format is: key1=value1/key2=value2/.../keyN=valueN
    pub fn get_partition_qualified_name(&self) -> String {
        let mut sb = String::new();
        for (i, key) in self.partition_keys.iter().enumerate() {
            sb.push_str(key);
            sb.push('=');
            sb.push_str(&self.partition_values[i]);
            if i != self.partition_keys.len() - 1 {
                sb.push('/');
            }
        }
        sb
    }

    pub fn contains(&self, other: &ResolvedPartitionSpec) -> Result<bool> {
        let other_partition_keys = other.get_partition_keys();
        let other_partition_values = other.get_partition_values();

        let mut expected_partition_values = Vec::new();
        for other_partition_key in other_partition_keys {
            let key_index = self
                .partition_keys
                .iter()
                .position(|k| k == other_partition_key);
            match key_index {
                Some(idx) => expected_partition_values.push(self.partition_values[idx].clone()),
                None => {
                    return Err(Error::IllegalArgument {
                        message: format!(
                            "table does not contain partitionKey: {other_partition_key}"
                        ),
                    });
                }
            }
        }

        let expected_partition_name = expected_partition_values.join(PARTITION_SPEC_SEPARATOR);
        let other_partition_name = other_partition_values.join(PARTITION_SPEC_SEPARATOR);

        Ok(expected_partition_name == other_partition_name)
    }

    pub fn to_pb(&self) -> PbPartitionSpec {
        PbPartitionSpec {
            partition_key_values: self
                .partition_keys
                .iter()
                .zip(self.partition_values.iter())
                .map(|(k, v)| PbKeyValue {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
        }
    }

    pub fn from_pb(pb: &PbPartitionSpec) -> Self {
        let partition_keys = pb
            .partition_key_values
            .iter()
            .map(|kv| kv.key.clone())
            .collect();
        let partition_values = pb
            .partition_key_values
            .iter()
            .map(|kv| kv.value.clone())
            .collect();

        Self {
            partition_keys,
            partition_values,
        }
    }

    fn get_reordered_partition_values(
        partition_keys: &Arc<[String]>,
        partition_spec: &PartitionSpec,
    ) -> Vec<String> {
        let partition_spec_map = partition_spec.get_spec_map();
        partition_keys
            .iter()
            .map(|key| partition_spec_map.get(key).cloned().unwrap_or_default())
            .collect()
    }
}

impl Display for ResolvedPartitionSpec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get_partition_qualified_name())
    }
}

/// Information of a partition metadata, includes the partition's name and the partition id that
/// represents the unique identifier of the partition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PartitionInfo {
    partition_id: PartitionId,
    partition_spec: ResolvedPartitionSpec,
}

impl PartitionInfo {
    pub fn new(partition_id: PartitionId, partition_spec: ResolvedPartitionSpec) -> Self {
        Self {
            partition_id,
            partition_spec,
        }
    }

    /// Get the partition id. The id is globally unique in the Fluss cluster.
    pub fn get_partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Get the partition name.
    pub fn get_partition_name(&self) -> String {
        self.partition_spec.get_partition_name()
    }

    pub fn get_resolved_partition_spec(&self) -> &ResolvedPartitionSpec {
        &self.partition_spec
    }

    pub fn get_partition_spec(&self) -> PartitionSpec {
        self.partition_spec.to_partition_spec()
    }

    pub fn to_pb(&self) -> PbPartitionInfo {
        PbPartitionInfo {
            partition_id: self.partition_id,
            partition_spec: self.partition_spec.to_pb(),
        }
    }

    pub fn from_pb(pb: &PbPartitionInfo) -> Self {
        Self {
            partition_id: pb.partition_id,
            partition_spec: ResolvedPartitionSpec::from_pb(&pb.partition_spec),
        }
    }
}

impl Display for PartitionInfo {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "Partition{{name='{}', id={}}}",
            self.get_partition_name(),
            self.partition_id
        )
    }
}

/// A class to identify a table partition, containing the table id and the partition id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TablePartition {
    table_id: TableId,
    partition_id: PartitionId,
}

impl TablePartition {
    pub fn new(table_id: TableId, partition_id: PartitionId) -> Self {
        Self {
            table_id,
            partition_id,
        }
    }

    pub fn get_table_id(&self) -> i64 {
        self.table_id
    }

    pub fn get_partition_id(&self) -> PartitionId {
        self.partition_id
    }
}

impl Display for TablePartition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TablePartition{{tableId={}, partitionId={}}}",
            self.table_id, self.partition_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolved_partition_spec_name() {
        let spec = ResolvedPartitionSpec::new(
            Arc::from(["date".to_string(), "region".to_string()]),
            vec!["2024-01-15".to_string(), "US".to_string()],
        )
        .unwrap();

        assert_eq!(spec.get_partition_name(), "2024-01-15$US");
        assert_eq!(
            spec.get_partition_qualified_name(),
            "date=2024-01-15/region=US"
        );
    }

    #[test]
    fn test_resolved_partition_spec_from_partition_name() {
        let spec = ResolvedPartitionSpec::from_partition_name(
            Arc::from(["date".to_string(), "region".to_string()]),
            "2024-01-15$US",
        );

        assert_eq!(spec.get_partition_values(), &["2024-01-15", "US"]);
    }

    #[test]
    fn test_resolved_partition_spec_from_qualified_name() {
        let spec =
            ResolvedPartitionSpec::from_partition_qualified_name("date=2024-01-15/region=US")
                .unwrap();

        assert_eq!(spec.get_partition_keys(), &["date", "region"]);
        assert_eq!(spec.get_partition_values(), &["2024-01-15", "US"]);
    }

    #[test]
    fn test_resolved_partition_spec_mismatched_lengths() {
        let result = ResolvedPartitionSpec::new(
            Arc::from(["date".to_string(), "region".to_string()]),
            vec!["2024-01-15".to_string()],
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_partition_info() {
        let spec = ResolvedPartitionSpec::new(
            Arc::from(["date".to_string()]),
            vec!["2024-01-15".to_string()],
        )
        .unwrap();

        let info = PartitionInfo::new(42, spec);
        assert_eq!(info.get_partition_id(), 42);
        assert_eq!(info.get_partition_name(), "2024-01-15");
    }

    #[test]
    fn test_table_partition() {
        let tp = TablePartition::new(100, 42);
        assert_eq!(tp.get_table_id(), 100);
        assert_eq!(tp.get_partition_id(), 42);
    }

    #[test]
    fn test_partition_spec_pb_roundtrip() {
        let mut map = HashMap::new();
        map.insert("date".to_string(), "2024-01-15".to_string());
        let spec = PartitionSpec::new(map);

        let pb = spec.to_pb();
        let restored = PartitionSpec::from_pb(&pb);

        assert_eq!(
            spec.get_spec_map().get("date"),
            restored.get_spec_map().get("date")
        );
    }

    #[test]
    fn test_partition_info_pb_roundtrip() {
        let spec = ResolvedPartitionSpec::new(
            Arc::from(["date".to_string()]),
            vec!["2024-01-15".to_string()],
        )
        .unwrap();
        let info = PartitionInfo::new(42, spec);

        let pb = info.to_pb();
        let restored = PartitionInfo::from_pb(&pb);

        assert_eq!(info.get_partition_id(), restored.get_partition_id());
        assert_eq!(info.get_partition_name(), restored.get_partition_name());
    }

    #[test]
    fn test_contains() {
        let full_spec = ResolvedPartitionSpec::new(
            Arc::from(["date".to_string(), "region".to_string()]),
            vec!["2024-01-15".to_string(), "US".to_string()],
        )
        .unwrap();

        let partial_spec = ResolvedPartitionSpec::new(
            Arc::from(["date".to_string()]),
            vec!["2024-01-15".to_string()],
        )
        .unwrap();

        assert!(full_spec.contains(&partial_spec).unwrap());
    }
}
