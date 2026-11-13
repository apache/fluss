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

//! Conservative partition pruning over a schema-bound core predicate.

use crate::FlussLakePartitionIdentity;
use fluss::metadata::{DataType, RowType};
use fluss::predicate::{BoundPredicate, CompoundFunction, LeafFunction, Literal, Predicate};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TruthValue {
    True,
    False,
    Unknown,
}

/// Evaluates predicates only against fields represented by partition values.
pub(crate) struct PartitionPruner {
    partition_fields: HashMap<usize, String>,
    predicate: BoundPredicate,
}

impl PartitionPruner {
    pub(crate) fn new(
        row_type: &RowType,
        partition_keys: &[String],
        predicate: &BoundPredicate,
    ) -> Self {
        let partition_fields = partition_keys
            .iter()
            .filter_map(|partition_key| {
                row_type
                    .get_field_index(partition_key)
                    .map(|field_index| (field_index, partition_key.clone()))
            })
            .collect();
        Self {
            partition_fields,
            predicate: predicate.clone(),
        }
    }

    /// Extracts a scope implied by the filter, not a replacement for that filter.
    /// String equality uses metadata values verbatim. Other types may have
    /// multiple encodings of an equal value; keep them for metadata-aware pruning.
    pub(crate) fn partition_scope(&self) -> Option<Predicate> {
        let mut scope = HashMap::new();
        let mut pending = vec![&self.predicate];
        while let Some(predicate) = pending.pop() {
            match predicate {
                BoundPredicate::Compound {
                    function: CompoundFunction::And,
                    children,
                } => pending.extend(children),
                BoundPredicate::Leaf {
                    field_index,
                    field_name,
                    data_type: DataType::String(_),
                    function: LeafFunction::Equal,
                    literals,
                    ..
                } if self.partition_fields.get(field_index) == Some(field_name) => {
                    if let [Literal::String(value)] = literals.as_slice() {
                        scope.insert(field_name.clone(), value.clone());
                    }
                }
                _ => {}
            }
        }
        scope
            .into_iter()
            .map(|(name, value)| fluss::predicate::col(name).eq(value))
            .reduce(Predicate::and)
    }

    pub(crate) fn partition_identity_may_match(
        &self,
        partition: &FlussLakePartitionIdentity,
    ) -> bool {
        let FlussLakePartitionIdentity::KeyValues(key_values) = partition else {
            return true;
        };
        let values: HashMap<&str, &str> = key_values
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        self.partition_values_may_match(&values)
    }

    fn partition_values_may_match(&self, values: &HashMap<&str, &str>) -> bool {
        self.evaluate(&self.predicate, values) != TruthValue::False
    }

    fn evaluate(
        &self,
        predicate: &BoundPredicate,
        partition_values: &HashMap<&str, &str>,
    ) -> TruthValue {
        match predicate {
            BoundPredicate::AlwaysTrue => TruthValue::True,
            BoundPredicate::Compound { function, children } => match function {
                CompoundFunction::And => {
                    let mut result = TruthValue::True;
                    for child in children {
                        match self.evaluate(child, partition_values) {
                            TruthValue::False => return TruthValue::False,
                            TruthValue::Unknown => result = TruthValue::Unknown,
                            TruthValue::True => {}
                        }
                    }
                    result
                }
                CompoundFunction::Or => {
                    let mut result = TruthValue::False;
                    for child in children {
                        match self.evaluate(child, partition_values) {
                            TruthValue::True => return TruthValue::True,
                            TruthValue::Unknown => result = TruthValue::Unknown,
                            TruthValue::False => {}
                        }
                    }
                    result
                }
            },
            BoundPredicate::Leaf {
                field_index,
                field_name,
                ..
            } => {
                let Some(partition_key) = self.partition_fields.get(field_index) else {
                    return TruthValue::Unknown;
                };
                if partition_key != field_name {
                    return TruthValue::Unknown;
                }
                let Some(value) = partition_values.get(partition_key.as_str()).copied() else {
                    return TruthValue::Unknown;
                };
                match predicate.evaluate_partition_value(value) {
                    Ok(true) => TruthValue::True,
                    Ok(false) => TruthValue::False,
                    Err(_) => TruthValue::Unknown,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluss::metadata::{DataField, DataTypes};
    use fluss::predicate::{Literal, Predicate, col};

    fn row_type() -> RowType {
        RowType::new(vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("region", DataTypes::string(), None),
            DataField::new("day", DataTypes::int(), None),
        ])
    }

    fn pruner(predicate: Predicate) -> PartitionPruner {
        let row_type = row_type();
        let bound = BoundPredicate::bind(Some(&predicate), &row_type).unwrap();
        PartitionPruner::new(
            &row_type,
            &["region".to_string(), "day".to_string()],
            &bound,
        )
    }

    fn partition(region: &str, day: &str) -> FlussLakePartitionIdentity {
        FlussLakePartitionIdentity::KeyValues(vec![
            ("region".into(), region.into()),
            ("day".into(), day.into()),
        ])
    }

    #[test]
    fn capture_scope_uses_only_implied_string_partition_equalities() {
        for (predicate, region) in [
            (col("region").eq("US").and(col("id").eq(42_i32)), Some("US")),
            (col("region").eq("US").or(col("id").eq(42_i32)), None),
            (col("region").ge("US").and(col("day").eq(2_i32)), None),
            (
                col("region")
                    .eq("US")
                    .and(col("day").eq(1_i32).or(col("day").eq(2_i32))),
                Some("US"),
            ),
        ] {
            let scope = pruner(predicate).partition_scope();
            assert_eq!(scope, region.map(|value| col("region").eq(value)));
        }
    }

    #[test]
    fn prunes_equal_range_and_in_predicates() {
        assert!(
            !pruner(col("region").eq("US")).partition_identity_may_match(&partition("EU", "2"))
        );
        assert!(!pruner(col("day").ge(10_i32)).partition_identity_may_match(&partition("US", "9")));
        assert!(
            !pruner(col("region").is_in(["US", "CN"]))
                .partition_identity_may_match(&partition("EU", "2"))
        );
    }

    #[test]
    fn fractional_numeric_bounds_do_not_prune_matching_partitions() {
        let filter = pruner(col("day").lt(1.3));
        assert!(filter.partition_identity_may_match(&partition("EU", "1")));
        assert!(!filter.partition_identity_may_match(&partition("EU", "2")));
        assert!(!pruner(col("day").eq(1.3)).partition_identity_may_match(&partition("EU", "1")));
        assert!(pruner(col("day").ge(-1.3)).partition_identity_may_match(&partition("EU", "-1")));
    }

    #[test]
    fn timestamp_partitions_preserve_sub_millisecond_precision() {
        for ltz in [false, true] {
            let data_type = if ltz {
                DataTypes::timestamp_ltz_with_precision(9)
            } else {
                DataTypes::timestamp_with_precision(9)
            };
            let literal = if ltz {
                Literal::TimestampLtz(
                    fluss::row::TimestampLtz::from_millis_nanos(1, 900_000).unwrap(),
                )
            } else {
                Literal::TimestampNtz(
                    fluss::row::TimestampNtz::from_millis_nanos(1, 900_000).unwrap(),
                )
            };
            let row_type = RowType::new(vec![DataField::new("ts", data_type, None)]);
            for (predicate, expected) in [
                (col("ts").lt(literal.clone()), [true, false, false]),
                (col("ts").eq(literal.clone()), [false, true, false]),
            ] {
                let bound = BoundPredicate::bind(Some(&predicate), &row_type).unwrap();
                let pruner = PartitionPruner::new(&row_type, &["ts".into()], &bound);
                for (fraction, expected) in ["001", "0019", "002"].into_iter().zip(expected) {
                    let partition = FlussLakePartitionIdentity::KeyValues(vec![(
                        "ts".into(),
                        format!("1970-01-01-00-00-00_{fraction}"),
                    )]);
                    assert_eq!(pruner.partition_identity_may_match(&partition), expected);
                }
            }
        }
    }

    #[test]
    fn non_partition_fields_and_unparsable_values_are_kept() {
        assert!(pruner(col("id").eq(1_i32)).partition_identity_may_match(&partition("EU", "2")));
        assert!(
            pruner(col("day").eq(1_i32))
                .partition_identity_may_match(&partition("EU", "not-a-number"))
        );
    }

    #[test]
    fn mixed_boolean_predicates_remain_conservative() {
        let mixed = pruner(col("region").eq("US").or(col("id").eq(1_i32)));
        assert!(mixed.partition_identity_may_match(&partition("EU", "2")));
        assert!(
            !pruner(col("region").eq("US").and(col("id").eq(1_i32)))
                .partition_identity_may_match(&partition("EU", "2"))
        );
    }
}
