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

//! Predicates for server-side filter pushdown on log scans.
//!
//! ```rust
//! use fluss::predicate::col;
//!
//! let p = col("age").gt(30i64).and(col("name").starts_with("A"));
//! ```
//!
//! The server prunes whole Arrow batches by their statistics, so a filtered scan
//! returns a superset of the matching records and callers must still filter
//! exactly. The protocol has no negation node; negate with [`ColumnRef::ne`] and
//! [`ColumnRef::not_in`].
//!
//! Setting a predicate on a scan is only supported on log scans over tables
//! with the ARROW log format.

mod pb;

pub(crate) use pb::to_pb_predicate;

use crate::row::{Decimal, TimestampLtz, TimestampNtz};

/// Comparison applied by a [`Predicate::Leaf`] to one column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LeafFunction {
    Equal,
    NotEqual,
    LessThan,
    LessOrEqual,
    GreaterThan,
    GreaterOrEqual,
    IsNull,
    IsNotNull,
    StartsWith,
    Contains,
    EndsWith,
    In,
    NotIn,
}

/// Boolean connective applied by a [`Predicate::Compound`] to its children.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CompoundFunction {
    And,
    Or,
}

/// A constant compared against a column, coerced to the column's declared type
/// when the predicate is bound to a schema. [`Literal::Date`] holds epoch days
/// and [`Literal::Time`] milliseconds of day, as Fluss encodes them internally.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Decimal(Decimal),
    Date(i32),
    Time(i32),
    TimestampNtz(TimestampNtz),
    TimestampLtz(TimestampLtz),
}

macro_rules! impl_from_literal {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(
            impl From<$ty> for Literal {
                fn from(value: $ty) -> Self {
                    Literal::$variant(value.into())
                }
            }
        )*
    };
}

impl_from_literal! {
    bool => Bool,
    i8 => Int8,
    i16 => Int16,
    i32 => Int32,
    i64 => Int64,
    f32 => Float32,
    f64 => Float64,
    String => String,
    Decimal => Decimal,
    TimestampNtz => TimestampNtz,
    TimestampLtz => TimestampLtz,
}

impl From<&str> for Literal {
    fn from(value: &str) -> Self {
        Literal::String(value.to_string())
    }
}

impl From<Vec<u8>> for Literal {
    fn from(value: Vec<u8>) -> Self {
        Literal::Bytes(value)
    }
}

impl From<&[u8]> for Literal {
    fn from(value: &[u8]) -> Self {
        Literal::Bytes(value.to_vec())
    }
}

impl<T: Into<Literal>> From<Option<T>> for Literal {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => Literal::Null,
        }
    }
}

/// A filter expression pushed down to the server for a log scan, built with
/// [`col`]. Column names and literal types are checked when it is attached to a
/// scan, not here.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// A comparison against a single column.
    Leaf {
        field: String,
        function: LeafFunction,
        /// Empty for `IS [NOT] NULL`, one element for the comparisons, one or
        /// more for `IN`/`NOT IN`.
        literals: Vec<Literal>,
    },
    /// A boolean combination of child predicates.
    Compound {
        function: CompoundFunction,
        children: Vec<Predicate>,
    },
}

impl Predicate {
    /// Returns a predicate matching rows matched by both `self` and `other`.
    pub fn and(self, other: Predicate) -> Predicate {
        self.combine(CompoundFunction::And, other)
    }

    /// Returns a predicate matching rows matched by either `self` or `other`.
    pub fn or(self, other: Predicate) -> Predicate {
        self.combine(CompoundFunction::Or, other)
    }

    /// Combines all `predicates` with `AND`, or returns `None` if empty.
    pub fn and_all(predicates: impl IntoIterator<Item = Predicate>) -> Option<Predicate> {
        Self::combine_all(CompoundFunction::And, predicates)
    }

    /// Combines all `predicates` with `OR`, or returns `None` if empty.
    pub fn or_all(predicates: impl IntoIterator<Item = Predicate>) -> Option<Predicate> {
        Self::combine_all(CompoundFunction::Or, predicates)
    }

    /// Flattens into an existing node of the same function so that chained
    /// combinators produce one n-ary node instead of a left-leaning tree.
    fn combine(self, function: CompoundFunction, other: Predicate) -> Predicate {
        match self {
            // Same kind of node: absorb `other` as one more child.
            Predicate::Compound {
                function: existing,
                mut children,
            } if existing == function => {
                children.push(other);
                Predicate::Compound { function, children }
            }
            // A leaf, or a node of the other kind: both operands become children.
            lhs => Predicate::Compound {
                function,
                children: vec![lhs, other],
            },
        }
    }

    fn combine_all(
        function: CompoundFunction,
        predicates: impl IntoIterator<Item = Predicate>,
    ) -> Option<Predicate> {
        predicates
            .into_iter()
            .reduce(|acc, next| acc.combine(function, next))
    }
}

/// Starts a predicate on the column named `name`, which is validated only once
/// the predicate is attached to a scan.
pub fn col(name: impl Into<String>) -> ColumnRef {
    ColumnRef { name: name.into() }
}

/// A column reference awaiting a comparison, produced by [`col`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    name: String,
}

// The `is_*` builders consume `self` and return a predicate, like every other
// combinator here, rather than asking a question about the column.
#[allow(clippy::wrong_self_convention)]
impl ColumnRef {
    /// Name of the referenced column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `column = value`.
    pub fn eq(self, value: impl Into<Literal>) -> Predicate {
        self.binary(LeafFunction::Equal, value)
    }

    /// `column <> value`.
    pub fn ne(self, value: impl Into<Literal>) -> Predicate {
        self.binary(LeafFunction::NotEqual, value)
    }

    /// `column < value`.
    pub fn lt(self, value: impl Into<Literal>) -> Predicate {
        self.binary(LeafFunction::LessThan, value)
    }

    /// `column <= value`.
    pub fn le(self, value: impl Into<Literal>) -> Predicate {
        self.binary(LeafFunction::LessOrEqual, value)
    }

    /// `column > value`.
    pub fn gt(self, value: impl Into<Literal>) -> Predicate {
        self.binary(LeafFunction::GreaterThan, value)
    }

    /// `column >= value`.
    pub fn ge(self, value: impl Into<Literal>) -> Predicate {
        self.binary(LeafFunction::GreaterOrEqual, value)
    }

    /// `column IS NULL`.
    pub fn is_null(self) -> Predicate {
        self.leaf(LeafFunction::IsNull, vec![])
    }

    /// `column IS NOT NULL`.
    pub fn is_not_null(self) -> Predicate {
        self.leaf(LeafFunction::IsNotNull, vec![])
    }

    /// `column IN (values...)`. An empty `values` matches nothing.
    pub fn is_in<V: Into<Literal>>(self, values: impl IntoIterator<Item = V>) -> Predicate {
        self.leaf(
            LeafFunction::In,
            values.into_iter().map(Into::into).collect(),
        )
    }

    /// `column NOT IN (values...)`. An empty `values` matches everything.
    pub fn not_in<V: Into<Literal>>(self, values: impl IntoIterator<Item = V>) -> Predicate {
        self.leaf(
            LeafFunction::NotIn,
            values.into_iter().map(Into::into).collect(),
        )
    }

    /// `column LIKE 'prefix%'`, for character-string columns.
    pub fn starts_with(self, prefix: impl Into<String>) -> Predicate {
        self.binary(LeafFunction::StartsWith, prefix.into())
    }

    /// `column LIKE '%suffix'`, for character-string columns.
    pub fn ends_with(self, suffix: impl Into<String>) -> Predicate {
        self.binary(LeafFunction::EndsWith, suffix.into())
    }

    /// `column LIKE '%infix%'`, for character-string columns.
    pub fn contains(self, infix: impl Into<String>) -> Predicate {
        self.binary(LeafFunction::Contains, infix.into())
    }

    fn binary(self, function: LeafFunction, value: impl Into<Literal>) -> Predicate {
        self.leaf(function, vec![value.into()])
    }

    fn leaf(self, function: LeafFunction, literals: Vec<Literal>) -> Predicate {
        Predicate::Leaf {
            field: self.name,
            function,
            literals,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(field: &str, function: LeafFunction, literals: Vec<Literal>) -> Predicate {
        Predicate::Leaf {
            field: field.to_string(),
            function,
            literals,
        }
    }

    #[test]
    fn builds_leaf_predicates() {
        assert_eq!(
            col("age").gt(30i64),
            leaf("age", LeafFunction::GreaterThan, vec![Literal::Int64(30)])
        );
        assert_eq!(
            col("name").starts_with("A"),
            leaf(
                "name",
                LeafFunction::StartsWith,
                vec![Literal::String("A".to_string())]
            )
        );
        assert_eq!(
            col("deleted_at").is_null(),
            leaf("deleted_at", LeafFunction::IsNull, vec![])
        );
        assert_eq!(
            col("region").is_in(vec!["eu", "us"]),
            leaf(
                "region",
                LeafFunction::In,
                vec![
                    Literal::String("eu".to_string()),
                    Literal::String("us".to_string())
                ]
            )
        );
    }

    #[test]
    fn converts_rust_values_to_literals() {
        assert_eq!(Literal::from(true), Literal::Bool(true));
        assert_eq!(Literal::from(7i8), Literal::Int8(7));
        assert_eq!(Literal::from(7i32), Literal::Int32(7));
        assert_eq!(Literal::from(1.5f64), Literal::Float64(1.5));
        assert_eq!(
            Literal::from("hi".to_string()),
            Literal::String("hi".to_string())
        );
        assert_eq!(
            Literal::from(vec![1u8, 2].as_slice()),
            Literal::Bytes(vec![1, 2])
        );
        assert_eq!(Literal::from(None::<i32>), Literal::Null);
        assert_eq!(Literal::from(Some(3i32)), Literal::Int32(3));
    }

    #[test]
    fn chained_combinators_produce_one_n_ary_node() {
        let predicate = col("a")
            .eq(1i32)
            .and(col("b").eq(2i32))
            .and(col("c").eq(3i32));

        assert_eq!(
            predicate,
            Predicate::Compound {
                function: CompoundFunction::And,
                children: vec![
                    leaf("a", LeafFunction::Equal, vec![Literal::Int32(1)]),
                    leaf("b", LeafFunction::Equal, vec![Literal::Int32(2)]),
                    leaf("c", LeafFunction::Equal, vec![Literal::Int32(3)]),
                ],
            }
        );
    }

    #[test]
    fn mixed_combinators_stay_nested() {
        let predicate = col("a")
            .eq(1i32)
            .and(col("b").eq(2i32))
            .or(col("c").eq(3i32));

        assert_eq!(
            predicate,
            Predicate::Compound {
                function: CompoundFunction::Or,
                children: vec![
                    Predicate::Compound {
                        function: CompoundFunction::And,
                        children: vec![
                            leaf("a", LeafFunction::Equal, vec![Literal::Int32(1)]),
                            leaf("b", LeafFunction::Equal, vec![Literal::Int32(2)]),
                        ],
                    },
                    leaf("c", LeafFunction::Equal, vec![Literal::Int32(3)]),
                ],
            }
        );
    }

    #[test]
    fn combine_all_folds_and_handles_empty_input() {
        assert_eq!(Predicate::and_all(Vec::new()), None);
        assert_eq!(
            Predicate::or_all(vec![col("a").eq(1i32)]),
            Some(col("a").eq(1i32))
        );

        let folded = Predicate::and_all(vec![col("a").eq(1i32), col("b").eq(2i32)]).unwrap();
        assert_eq!(folded, col("a").eq(1i32).and(col("b").eq(2i32)));
    }
}
