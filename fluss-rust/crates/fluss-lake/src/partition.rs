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
use fluss::metadata::{DataType, PartitionSpec, RowType};
use fluss::predicate::{BoundPredicate, CompoundFunction, LeafFunction, Literal, Predicate};
use std::collections::HashMap;

pub(crate) struct PartitionScope(BoundPredicate);

impl PartitionScope {
    pub(crate) fn bind(
        scope: Option<&Predicate>,
        row_type: &RowType,
        partition_keys: &[String],
    ) -> Result<Self> {
        // The scope is transported exactly, unlike a conservative pushdown filter.
        if let Some(scope) = scope {
            scope.to_protobuf(row_type).map_err(scope_error)?;
        }
        let bound = BoundPredicate::bind(scope, row_type).map_err(scope_error)?;
        if bound.referenced_field_indexes().iter().any(|&index| {
            !partition_keys
                .iter()
                .any(|key| key == row_type.fields()[index].name())
        }) {
            return Err(scope_error(
                "partition scope must reference only partition columns",
            ));
        }
        Ok(Self(bound))
    }

    /// Narrow the Admin listing using equalities it can express, then evaluate
    /// the complete scope locally before issuing any offset queries.
    pub(crate) fn listing_spec(&self) -> PartitionSpec {
        let mut spec = HashMap::new();
        let mut pending = vec![&self.0];
        while let Some(predicate) = pending.pop() {
            match predicate {
                BoundPredicate::Compound {
                    function: CompoundFunction::And,
                    children,
                } => pending.extend(children),
                BoundPredicate::Leaf {
                    field_name,
                    data_type: DataType::String(_),
                    function: LeafFunction::Equal,
                    literals,
                    ..
                } => {
                    if let [Literal::String(value)] = literals.as_slice() {
                        spec.insert(field_name.clone(), value.clone());
                    }
                }
                _ => {}
            }
        }
        PartitionSpec::new(spec)
    }

    pub(crate) fn matches(&self, identity: &FlussLakePartitionIdentity) -> Result<bool> {
        fn evaluate(
            predicate: &BoundPredicate,
            identity: &FlussLakePartitionIdentity,
        ) -> Result<bool> {
            match predicate {
                BoundPredicate::AlwaysTrue => Ok(true),
                BoundPredicate::Leaf { field_name, .. } => {
                    let FlussLakePartitionIdentity::KeyValues(values) = identity else {
                        return Err(scope_error(
                            "partition scope applied to an unpartitioned range",
                        ));
                    };
                    let value = values
                        .iter()
                        .find(|(key, _)| key == field_name)
                        .ok_or_else(|| scope_error("partition scope column is missing"))?;
                    predicate
                        .evaluate_partition_value(&value.1)
                        .map_err(scope_error)
                }
                BoundPredicate::Compound { function, children } => {
                    let and = *function == CompoundFunction::And;
                    for child in children {
                        if evaluate(child, identity)? != and {
                            return Ok(!and);
                        }
                    }
                    Ok(and)
                }
            }
        }
        evaluate(&self.0, identity)
    }
}

fn scope_error(error: impl std::fmt::Display) -> FlussLakeError {
    FlussLakeError::InvalidReadContext(error.to_string())
}

/// Association between a physical log partition and business partition values.
///
/// Consumers must reject unsupported variants, not skip their log ranges.
/// Shared historical logs may cover multiple business partitions; support for
/// them will require a separate variant rather than a synthetic business value.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlussLakeLogPartition {
    /// A regular Fluss partition, including the root of an unpartitioned table.
    Ordinary(FlussLakePartitionIdentity),
}

/// Stable partition identity exposed for scheduling and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
