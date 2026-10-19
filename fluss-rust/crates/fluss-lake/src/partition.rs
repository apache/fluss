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

use serde::{Deserialize, Serialize};

/// Stable partition identity exposed for scheduling and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FlussLakePartitionIdentity {
    /// Root identity for an unpartitioned table.
    Unpartitioned,
    /// Logical partition-key/value pairs in table partition-key order.
    KeyValues(Vec<(String, String)>),
}

impl FlussLakePartitionIdentity {
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
