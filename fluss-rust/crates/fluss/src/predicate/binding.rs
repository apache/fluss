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

//! Schema-bound predicates for local evaluation and conservative pruning.

use super::coercion::{compound_name, mismatch, resolve_field, unsupported_column, validate_leaf};
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataField, DataType, RowType};
use crate::predicate::{CompoundFunction, LeafFunction, Literal, Predicate};
use std::collections::HashSet;

/// A core predicate resolved against a concrete Fluss row type.
///
/// This type is public only so sibling workspace crates can share binding,
/// coercion, pruning, and exact evaluation semantics.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq)]
pub enum BoundPredicate {
    AlwaysTrue,
    Leaf {
        field_index: usize,
        field_name: String,
        field_id: i32,
        data_type: DataType,
        function: LeafFunction,
        literals: Vec<Literal>,
    },
    Compound {
        function: CompoundFunction,
        children: Vec<BoundPredicate>,
    },
}

impl BoundPredicate {
    /// Binds an optional predicate to `row_type`, rewriting numeric bounds exactly.
    /// Numeric comparisons use the literal's value, not a lossy cast to the column
    /// type. Floating comparisons follow Fluss: all NaNs compare equal and above
    /// infinity, while signed zeros remain distinct. Other physical representations
    /// are checked by the evaluator.
    #[doc(hidden)]
    pub fn bind(predicate: Option<&Predicate>, row_type: &RowType) -> Result<Self> {
        match predicate {
            Some(predicate) => Self::bind_predicate(predicate, row_type),
            None => Ok(Self::AlwaysTrue),
        }
    }

    fn bind_predicate(predicate: &Predicate, row_type: &RowType) -> Result<Self> {
        match predicate {
            Predicate::Leaf {
                field,
                function,
                literals,
            } => {
                let (field_index, data_field) = resolve_field(row_type, field)?;
                validate_leaf(*function, data_field, literals)?;
                let (function, literals) =
                    match super::numeric::bind(data_field, *function, literals)? {
                        Some(rewritten) => rewritten,
                        None => (
                            *function,
                            literals
                                .iter()
                                .map(|literal| bind_literal(data_field, literal))
                                .collect::<Result<Vec<_>>>()?,
                        ),
                    };
                Ok(Self::Leaf {
                    field_index,
                    field_name: field.clone(),
                    field_id: data_field.field_id(),
                    data_type: data_field.data_type().clone(),
                    function,
                    literals,
                })
            }
            Predicate::Compound { function, children } => {
                if children.is_empty() {
                    return Err(IllegalArgument {
                        message: format!("{} predicate has no children", compound_name(*function)),
                    });
                }
                Ok(Self::Compound {
                    function: *function,
                    children: children
                        .iter()
                        .map(|child| Self::bind_predicate(child, row_type))
                        .collect::<Result<Vec<_>>>()?,
                })
            }
        }
    }

    /// Returns a conservative predicate for Fluss log-batch pruning.
    ///
    /// Uses the rewritten bounds, not the original literals, and excludes filters
    /// whose server-side statistics semantics may discard matching rows. This
    /// method does not negotiate backend capabilities. `None` means no pruning;
    /// callers must still apply this bound predicate to the returned rows.
    /// A partially supported `OR` is never pushed down.
    #[doc(hidden)]
    pub fn to_pushdown_predicate(&self) -> Option<Predicate> {
        match self {
            Self::AlwaysTrue => None,
            Self::Leaf {
                field_name,
                data_type,
                function,
                literals,
                ..
            } => {
                // TIME statistics store milliseconds even for finer-precision columns.
                let null_checks_only =
                    matches!(data_type, DataType::Float(_) | DataType::Double(_))
                        || matches!(data_type, DataType::Time(time) if time.precision() > 3);
                if null_checks_only
                    && !matches!(function, LeafFunction::IsNull | LeafFunction::IsNotNull)
                {
                    return None;
                }
                // Server prefix pruning uses UTF-16 ordering, unlike UTF-8 min/max.
                if matches!(function, LeafFunction::StartsWith) {
                    return None;
                }
                Some(Predicate::Leaf {
                    field: field_name.clone(),
                    function: *function,
                    literals: literals.clone(),
                })
            }
            Self::Compound {
                function: CompoundFunction::And,
                children,
            } => Predicate::and_all(children.iter().filter_map(Self::to_pushdown_predicate)),
            Self::Compound {
                function: CompoundFunction::Or,
                children,
            } => Predicate::or_all(
                children
                    .iter()
                    .map(Self::to_pushdown_predicate)
                    .collect::<Option<Vec<_>>>()?,
            ),
        }
    }

    /// Returns referenced table field indexes in first-seen order.
    #[doc(hidden)]
    pub fn referenced_field_indexes(&self) -> Vec<usize> {
        let mut indexes = Vec::new();
        let mut seen = HashSet::new();
        self.collect_referenced_field_indexes(&mut indexes, &mut seen);
        indexes
    }

    fn collect_referenced_field_indexes(
        &self,
        indexes: &mut Vec<usize>,
        seen: &mut HashSet<usize>,
    ) {
        match self {
            Self::AlwaysTrue => {}
            Self::Leaf { field_index, .. } => {
                if seen.insert(*field_index) {
                    indexes.push(*field_index);
                }
            }
            Self::Compound { children, .. } => {
                for child in children {
                    child.collect_referenced_field_indexes(indexes, seen);
                }
            }
        }
    }
}

fn bind_literal(field: &DataField, literal: &Literal) -> Result<Literal> {
    if matches!(literal, Literal::Null) {
        return Ok(Literal::Null);
    }

    // Numeric bounds are normalized before this point. Temporal bounds keep
    // their precision for comparison using the core timestamp types.
    Ok(match (field.data_type(), literal) {
        (DataType::Boolean(_), Literal::Bool(_))
        | (DataType::Char(_) | DataType::String(_), Literal::String(_))
        | (DataType::Binary(_) | DataType::Bytes(_), Literal::Bytes(_))
        | (DataType::Date(_), Literal::Date(_))
        | (DataType::Time(_), Literal::Time(_))
        | (DataType::Timestamp(_), Literal::TimestampNtz(_))
        | (DataType::TimestampLTz(_), Literal::TimestampLtz(_)) => literal.clone(),
        (DataType::Array(_) | DataType::Map(_) | DataType::Row(_), _) => {
            return Err(unsupported_column(field));
        }
        _ => return Err(mismatch(field, literal)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes};
    use crate::predicate::col;

    fn row_type() -> RowType {
        RowType::new(vec![
            DataField::with_field_id("id", DataTypes::int(), None, 3),
            DataField::with_field_id("name", DataTypes::string(), None, 5),
        ])
    }

    #[test]
    fn binds_field_identity_and_coerced_literals_once() {
        let bound = BoundPredicate::bind(Some(&col("id").eq(1_i8)), &row_type()).unwrap();

        assert_eq!(
            bound,
            BoundPredicate::Leaf {
                field_index: 0,
                field_name: "id".to_string(),
                field_id: 3,
                data_type: DataTypes::int(),
                function: LeafFunction::Equal,
                literals: vec![Literal::Int32(1)],
            }
        );
    }

    #[test]
    fn discovers_referenced_fields_in_first_seen_order() {
        let predicate = col("name").starts_with("A").and(col("id").gt(1_i32));
        let bound = BoundPredicate::bind(Some(&predicate), &row_type()).unwrap();

        assert_eq!(bound.referenced_field_indexes(), vec![1, 0]);
    }

    #[test]
    fn pushdown_keeps_only_statistics_safe_conditions() {
        for (data_type, functions, literal, safe) in [
            (
                DataTypes::time_with_precision(3),
                vec![LeafFunction::GreaterThan],
                Literal::Time(0),
                true,
            ),
            (
                DataTypes::time_with_precision(6),
                vec![LeafFunction::GreaterThan],
                Literal::Time(0),
                false,
            ),
            (
                DataTypes::time_with_precision(9),
                vec![
                    LeafFunction::Equal,
                    LeafFunction::NotEqual,
                    LeafFunction::LessThan,
                    LeafFunction::LessOrEqual,
                    LeafFunction::GreaterThan,
                    LeafFunction::GreaterOrEqual,
                    LeafFunction::In,
                    LeafFunction::NotIn,
                ],
                Literal::Time(0),
                false,
            ),
            (
                DataTypes::float(),
                vec![LeafFunction::Equal],
                Literal::Float32(0.0),
                false,
            ),
            (
                DataTypes::double(),
                vec![LeafFunction::Equal],
                Literal::Float64(0.0),
                false,
            ),
            (
                DataTypes::string(),
                vec![LeafFunction::StartsWith],
                Literal::String("\u{e000}".into()),
                false,
            ),
            (
                DataTypes::char(4),
                vec![LeafFunction::StartsWith],
                Literal::String("A".into()),
                false,
            ),
            (
                DataTypes::string(),
                vec![LeafFunction::Equal, LeafFunction::LessThan],
                Literal::String("A".into()),
                true,
            ),
        ] {
            let row_type = RowType::new(vec![DataField::new("c", data_type, None)]);
            let pushdown = |predicate: &Predicate| {
                BoundPredicate::bind(Some(predicate), &row_type)
                    .unwrap()
                    .to_pushdown_predicate()
            };
            let not_null = col("c").is_not_null();
            assert_eq!(pushdown(&not_null), Some(not_null.clone()));
            assert_eq!(pushdown(&col("c").is_null()), Some(col("c").is_null()));
            for function in functions {
                let predicate = Predicate::Leaf {
                    field: "c".into(),
                    function,
                    literals: vec![literal.clone()],
                };
                assert_eq!(
                    pushdown(&predicate),
                    safe.then(|| predicate.clone()),
                    "{row_type:?}, {function:?}"
                );
                if !safe {
                    assert_eq!(
                        pushdown(&predicate.clone().and(not_null.clone())),
                        Some(not_null.clone())
                    );
                    assert_eq!(pushdown(&predicate.or(not_null.clone())), None);
                }
            }
        }

        let row_type = RowType::new(vec![DataField::new("c", DataTypes::string(), None)]);
        let batch = arrow::record_batch::RecordBatch::try_from_iter([(
            "c",
            std::sync::Arc::new(arrow::array::StringArray::from(vec![
                Some("\u{e000}"),
                Some("\u{10000}"),
                None,
            ])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let bound =
            BoundPredicate::bind(Some(&col("c").starts_with("\u{e000}")), &row_type).unwrap();
        assert_eq!(
            bound
                .into_evaluator()
                .unwrap()
                .evaluate_batch(&batch)
                .unwrap()
                .values()
                .iter()
                .collect::<Vec<_>>(),
            vec![true, false, false]
        );
    }

    #[test]
    fn rejects_invalid_shape_before_any_consumer_uses_the_predicate() {
        let predicate = Predicate::Leaf {
            field: "id".to_string(),
            function: LeafFunction::IsNull,
            literals: vec![Literal::Int32(1)],
        };

        let error = BoundPredicate::bind(Some(&predicate), &row_type()).unwrap_err();

        assert!(error.to_string().contains("expects 0 literal(s), got 1"));
    }

    #[test]
    fn local_binding_does_not_require_protocol_field_ids() {
        let row_type = RowType::new(vec![DataField::new("id", DataTypes::int(), None)]);
        let bound = BoundPredicate::bind(Some(&col("id").eq(1_i8)), &row_type).unwrap();

        assert!(matches!(
            bound,
            BoundPredicate::Leaf { field_index: 0, ref literals, .. }
                if literals == &[Literal::Int32(1)]
        ));
    }

    #[test]
    fn folds_unrepresentable_integer_equality_without_rounding() {
        let row_type = RowType::new(vec![DataField::new("value", DataTypes::double(), None)]);

        let bound = BoundPredicate::bind(Some(&col("value").eq(i64::MAX)), &row_type).unwrap();
        assert!(
            matches!(bound, BoundPredicate::Leaf { function: LeafFunction::In, literals, .. } if literals.is_empty())
        );
        BoundPredicate::bind(Some(&col("value").eq(9_007_199_254_740_994_i64)), &row_type).unwrap();
    }
}
