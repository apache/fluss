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

//! Source partition identity shared by preparation and default planning.

use crate::{FlussLakeError, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub(crate) fn validate_partition_filter<'a>(
    keys: impl IntoIterator<Item = &'a String>,
    partition_keys: &[String],
) -> Result<()> {
    if keys.into_iter().any(|key| !partition_keys.contains(key)) {
        return Err(FlussLakeError::InvalidReadContext(
            "partition filter must reference only partition keys".into(),
        ));
    }
    Ok(())
}

/// Association between a physical log partition and business partition values.
///
/// Consumers must reject unsupported variants, not skip their log ranges.
/// Shared historical logs may cover multiple business partitions; support for
/// them will require a separate variant rather than a synthetic business value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
#[serde(tag = "kind", content = "identity")]
pub enum FlussLakeLogPartition {
    /// A regular Fluss partition, including the root of an unpartitioned table.
    #[serde(rename = "ordinary")]
    Ordinary(FlussLakePartitionIdentity),
}

/// Stable partition identity exposed for scheduling and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FlussLakePartitionIdentity {
    /// Root identity for an unpartitioned table.
    #[serde(rename = "unpartitioned")]
    Unpartitioned,
    /// Logical partition-key/value pairs in table partition-key order.
    #[serde(rename = "key_values")]
    KeyValues(Vec<(String, String)>),
}

impl FlussLakePartitionIdentity {
    /// Whether this partition matches an exact, possibly partial partition spec.
    /// Values use Fluss partition metadata encoding, not SQL literal coercion.
    pub fn matches_filter(&self, filter: &BTreeMap<String, String>) -> bool {
        match self {
            Self::Unpartitioned => filter.is_empty(),
            Self::KeyValues(values) => filter.iter().all(|pair| {
                values
                    .iter()
                    .any(|(key, value)| key == pair.0 && value == pair.1)
            }),
        }
    }

    /// Checks the identity's shape and key order against a resolved table layout.
    pub(crate) fn matches_keys(&self, partition_keys: &[String]) -> bool {
        match self {
            Self::Unpartitioned => partition_keys.is_empty(),
            Self::KeyValues(values) => {
                !partition_keys.is_empty()
                    && values.iter().map(|(key, _)| key).eq(partition_keys.iter())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_identity_requires_the_complete_ordered_key_layout() {
        let keys = vec!["region".to_string(), "day".to_string()];
        assert!(FlussLakePartitionIdentity::Unpartitioned.matches_keys(&[]));
        assert!(!FlussLakePartitionIdentity::Unpartitioned.matches_keys(&keys));
        for names in [
            vec!["region", "day"],
            vec!["day", "region"],
            vec!["region"],
            vec!["region", "region"],
            vec![],
        ] {
            let identity = FlussLakePartitionIdentity::KeyValues(
                names
                    .iter()
                    .map(|key| ((*key).into(), String::new()))
                    .collect(),
            );
            assert_eq!(identity.matches_keys(&keys), names == ["region", "day"]);
            assert!(!identity.matches_keys(&[]));
        }
    }
}
