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

//! Exact Arrow evaluation of schema-bound core predicates.

use super::coercion::{validate_leaf, validate_leaf_shape};
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataField, DataType as FlussDataType};
use crate::predicate::{BoundPredicate, CompoundFunction, LeafFunction, Literal};
use crate::record::to_arrow_type;
use crate::row::column_vector::{TimestampColumn, TypedColumn};
use crate::row::{Datum, TimestampLtz, TimestampNtz};
use crate::util::partition::parse_value_of_type;
use crate::util::{canonicalize_nan, compare_floats};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, make_builder};
use arrow::compute::kernels::boolean::{and, is_not_null, is_null, or};
use arrow::datatypes::{DataType as ArrowDataType, Float32Type, Float64Type};
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use std::cmp::Ordering;
use std::collections::HashSet;

impl BoundPredicate {
    /// Prepares an owned evaluator, converting constants and building membership sets once.
    ///
    /// Rejects malformed leaf shapes and errors while preparing constants. The evaluator is
    /// immutable and can be shared across batch readers; its buffers
    /// are released when it is dropped. Batch column names and types are checked
    /// on each evaluation. Prefer this over `evaluate_batch` for a stream of batches.
    #[doc(hidden)]
    pub fn into_evaluator(self) -> Result<PredicateEvaluator> {
        Ok(PredicateEvaluator {
            root: PreparedPredicate::new(&self)?,
        })
    }

    /// Evaluates a leaf against a non-null, encoded Fluss partition value.
    ///
    /// Uses the same exact filter semantics as [`Self::evaluate_batch`]. Returns
    /// an error for non-leaves, invalid encodings, or unsupported types.
    /// Temporal values retain their core precision, independently of Arrow units.
    /// A pruning caller must retain the partition on error.
    #[doc(hidden)]
    pub fn evaluate_partition_value(&self, value: &str) -> Result<bool> {
        let Self::Leaf {
            field_name,
            data_type,
            function,
            literals,
            ..
        } = self
        else {
            return Err(IllegalArgument {
                message: "partition value evaluation requires a leaf predicate".into(),
            });
        };
        let datum = parse_value_of_type(value, data_type)?;
        validate_leaf_shape(*function, field_name, literals)?;
        // Partition strings are core values, not Arrow rows. In particular a
        // timestamp must not be narrowed to the column's physical unit here.
        match &datum {
            Datum::TimestampNtz(value) => {
                return evaluate_value(*value, *function, literals, |literal| match literal {
                    Literal::TimestampNtz(value) => Ok(*value),
                    _ => Err(literal_mismatch()),
                });
            }
            Datum::TimestampLtz(value) => {
                return evaluate_value(*value, *function, literals, |literal| match literal {
                    Literal::TimestampLtz(value) => Ok(*value),
                    _ => Err(literal_mismatch()),
                });
            }
            Datum::Time(value) => {
                return evaluate_value(value.get_inner(), *function, literals, |literal| {
                    match literal {
                        Literal::Time(value) => Ok(*value),
                        _ => Err(literal_mismatch()),
                    }
                });
            }
            _ => {}
        }
        let array = datum_array(&datum, data_type)?;
        let batch = RecordBatch::try_from_iter([(field_name.as_str(), array)])?;
        Ok(self.evaluate_batch(&batch)?.value(0))
    }

    /// Evaluates this predicate exactly against an Arrow record batch.
    ///
    /// Returns a non-null filter mask. Except for `IS NULL`, null input values
    /// do not match; `NOT IN` with a null literal matches no rows.
    /// Prepares a temporary evaluator per call; use [`Self::into_evaluator`]
    /// to reuse converted constants and membership sets across batches.
    #[doc(hidden)]
    pub fn evaluate_batch(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        PreparedPredicate::new(self)?.evaluate_batch(batch)
    }
}

fn predicate_column<'a>(
    batch: &'a RecordBatch,
    field_name: &str,
    data_type: &FlussDataType,
) -> Result<&'a ArrayRef> {
    let array = batch
        .column_by_name(field_name)
        .ok_or_else(|| IllegalArgument {
            message: format!("Arrow batch is missing predicate column '{field_name}'"),
        })?;
    let expected_arrow_type = to_arrow_type(data_type)?;
    if array.data_type() != &expected_arrow_type {
        return Err(IllegalArgument {
            message: format!(
                "predicate column '{field_name}' has Arrow type {}, expected {expected_arrow_type}",
                array.data_type()
            ),
        });
    }

    Ok(array)
}

fn compare(
    array: &ArrayRef,
    function: LeafFunction,
    operand: &Operand,
    data_type: &FlussDataType,
) -> Result<BooleanArray> {
    let column = TypedColumn::build(array.as_ref(), data_type)?;
    match (&column, operand) {
        (TypedColumn::Timestamp(_), Operand::Literal(Literal::TimestampNtz(bound))) => {
            timestamp_mask(array, &column, TypedColumn::get_timestamp_ntz, |value| {
                matches_order(
                    value.map_or_else(|side| side, |value| value.cmp(bound)),
                    function,
                )
            })
        }
        (TypedColumn::TimestampLtz(_), Operand::Literal(Literal::TimestampLtz(bound))) => {
            timestamp_mask(array, &column, TypedColumn::get_timestamp_ltz, |value| {
                matches_order(
                    value.map_or_else(|side| side, |value| value.cmp(bound)),
                    function,
                )
            })
        }
        (TypedColumn::Time(time), Operand::Literal(Literal::Time(bound))) => Ok(compare_values(
            (0..array.len()).map(|i| array.is_valid(i).then(|| time.get_nanos(i))),
            i128::from(*bound) * 1_000_000,
            function,
            |a, b| a.cmp(&b),
        )),
        (TypedColumn::Binary(values), Operand::Literal(Literal::Bytes(bound))) => Ok(
            compare_values(values.iter(), bound.as_slice(), function, compare_binary),
        ),
        (TypedColumn::Bytes(values), Operand::Literal(Literal::Bytes(bound))) => Ok(
            compare_values(values.iter(), bound.as_slice(), function, compare_binary),
        ),
        (_, Operand::Scalar(constant)) => compare_scalar(&column, function, constant),
        _ => Err(literal_mismatch()),
    }
}

fn literal_mismatch() -> crate::error::Error {
    IllegalArgument {
        message: "predicate literal does not match its column type".into(),
    }
}

fn compare_scalar(
    column: &TypedColumn,
    function: LeafFunction,
    constant: &TypedColumn,
) -> Result<BooleanArray> {
    macro_rules! compare {
        ($array:expr, $bound:expr, $cmp:expr) => {
            compare_values($array.iter(), $bound.value(0), function, $cmp)
        };
        ($array:expr, $bound:expr) => {
            compare!($array, $bound, |a, b| a.cmp(&b))
        };
    }
    Ok(match (column, constant) {
        (TypedColumn::Boolean(a), TypedColumn::Boolean(b)) => compare!(a, b),
        (TypedColumn::TinyInt(a), TypedColumn::TinyInt(b)) => compare!(a, b),
        (TypedColumn::SmallInt(a), TypedColumn::SmallInt(b)) => compare!(a, b),
        (TypedColumn::Int(a), TypedColumn::Int(b)) => compare!(a, b),
        (TypedColumn::BigInt(a), TypedColumn::BigInt(b)) => compare!(a, b),
        (TypedColumn::Date(a), TypedColumn::Date(b)) => compare!(a, b),
        (TypedColumn::Float(a), TypedColumn::Float(b)) => {
            compare!(a, b, |a, b| compare_floats(f64::from(a), f64::from(b)))
        }
        (TypedColumn::Double(a), TypedColumn::Double(b)) => compare!(a, b, compare_floats),
        (TypedColumn::String(a), TypedColumn::String(b)) => match function {
            LeafFunction::StartsWith | LeafFunction::EndsWith | LeafFunction::Contains => a
                .iter()
                .map(|value| value.is_some_and(|value| matches_string(value, b.value(0), function)))
                .collect(),
            _ => compare!(a, b),
        },
        // Binding has already aligned the scale. Compare unscaled values rather
        // than allocating a core Decimal/BigDecimal for every row.
        (TypedColumn::Decimal(a, _), TypedColumn::Decimal(b, _)) => compare!(a, b),
        _ => return Err(literal_mismatch()),
    })
}

// Arrow seconds span a wider range than core timestamps. Classify only those
// out-of-range rows before using the existing core timestamp accessors.
fn timestamp_mask<T>(
    array: &ArrayRef,
    column: &TypedColumn,
    read: impl Fn(&TypedColumn, usize) -> Result<T>,
    matches: impl Fn(std::result::Result<T, Ordering>) -> bool,
) -> Result<BooleanArray> {
    let mut mask = BooleanBuilder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            mask.append_value(false);
            continue;
        }
        if let TypedColumn::Timestamp(TimestampColumn::Second(seconds))
        | TypedColumn::TimestampLtz(TimestampColumn::Second(seconds)) = column
        {
            let value = seconds.value(i);
            if value.checked_mul(1_000).is_none() {
                mask.append_value(matches(Err(value.cmp(&0))));
                continue;
            }
        }
        mask.append_value(matches(Ok(read(column, i)?)));
    }
    Ok(mask.finish())
}

fn evaluate_value<T: Ord + Copy>(
    value: T,
    function: LeafFunction,
    literals: &[Literal],
    extract: impl Fn(&Literal) -> Result<T>,
) -> Result<bool> {
    match function {
        LeafFunction::IsNull => Ok(false),
        LeafFunction::IsNotNull => Ok(true),
        LeafFunction::In | LeafFunction::NotIn => {
            if function == LeafFunction::NotIn && literals.iter().any(Literal::is_null) {
                return Ok(false);
            }
            let mut found = false;
            for literal in literals.iter().filter(|literal| !literal.is_null()) {
                found |= value == extract(literal)?;
            }
            Ok(found != (function == LeafFunction::NotIn))
        }
        LeafFunction::StartsWith | LeafFunction::EndsWith | LeafFunction::Contains => {
            Err(literal_mismatch())
        }
        _ => Ok(matches_order(value.cmp(&extract(&literals[0])?), function)),
    }
}

fn matches_order(ordering: Ordering, function: LeafFunction) -> bool {
    match function {
        LeafFunction::Equal => ordering.is_eq(),
        LeafFunction::NotEqual => !ordering.is_eq(),
        LeafFunction::LessThan => ordering.is_lt(),
        LeafFunction::LessOrEqual => !ordering.is_gt(),
        LeafFunction::GreaterThan => ordering.is_gt(),
        LeafFunction::GreaterOrEqual => !ordering.is_lt(),
        _ => unreachable!("non-comparison leaf reached compare"),
    }
}

fn compare_values<T: Copy>(
    values: impl Iterator<Item = Option<T>>,
    constant: T,
    function: LeafFunction,
    compare: impl Fn(T, T) -> Ordering,
) -> BooleanArray {
    values
        .map(|value| value.is_some_and(|value| matches_order(compare(value, constant), function)))
        .collect()
}

fn compare_binary(left: &[u8], right: &[u8]) -> Ordering {
    // Java byte[] comparison is signed, unlike Arrow's binary ordering.
    left.iter()
        .map(|b| *b as i8)
        .cmp(right.iter().map(|b| *b as i8))
}

fn matches_string(value: &str, expected: &str, function: LeafFunction) -> bool {
    match function {
        LeafFunction::StartsWith => value.starts_with(expected),
        LeafFunction::EndsWith => value.ends_with(expected),
        LeafFunction::Contains => value.contains(expected),
        _ => unreachable!("non-string function reached string evaluator"),
    }
}

/// Builds one scalar value, rather than repeating the literal for every input row.
fn literal_array(literal: &Literal, data_type: &FlussDataType) -> Result<ArrayRef> {
    datum_array(&literal.to_datum(), data_type)
}

fn datum_array(datum: &Datum<'_>, data_type: &FlussDataType) -> Result<ArrayRef> {
    let arrow_type = to_arrow_type(data_type)?;
    // Temporal predicates use core values directly, not the row writer's
    // narrowing conversions.
    let mut builder = make_builder(&arrow_type, 1);
    datum.append_to(builder.as_mut(), data_type, &arrow_type)?;
    Ok(builder.finish())
}

// Constant folding here is for a filter mask, not a nullable SQL expression.
fn constant_filter(array: &ArrayRef, matches: bool) -> Result<BooleanArray> {
    if matches {
        is_not_null(array.as_ref()).map_err(Into::into)
    } else {
        Ok(BooleanArray::from(vec![false; array.len()]))
    }
}

/// An immutable evaluator with constants and membership sets prepared for repeated batches.
#[doc(hidden)]
#[derive(Debug)]
pub struct PredicateEvaluator {
    root: PreparedPredicate,
}

impl PredicateEvaluator {
    /// Returns a non-null filter mask, using the same semantics and batch schema
    /// checks as [`BoundPredicate::evaluate_batch`].
    pub fn evaluate_batch(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        self.root.evaluate_batch(batch)
    }
}

#[derive(Debug)]
enum PreparedPredicate {
    AlwaysTrue,
    Leaf {
        field_name: String,
        data_type: FlussDataType,
        function: LeafFunction,
        operand: Operand,
    },
    Compound {
        function: CompoundFunction,
        children: Vec<PreparedPredicate>,
    },
}

impl PreparedPredicate {
    fn new(predicate: &BoundPredicate) -> Result<Self> {
        Ok(match predicate {
            BoundPredicate::AlwaysTrue => Self::AlwaysTrue,
            BoundPredicate::Compound { function, children } => Self::Compound {
                function: *function,
                children: children.iter().map(Self::new).collect::<Result<_>>()?,
            },
            BoundPredicate::Leaf {
                field_name,
                data_type,
                function,
                literals,
                ..
            } => {
                validate_leaf(
                    *function,
                    &DataField::new(field_name, data_type.clone(), None),
                    literals,
                )?;
                Self::Leaf {
                    field_name: field_name.clone(),
                    data_type: data_type.clone(),
                    function: *function,
                    operand: Operand::new(*function, literals, data_type)?,
                }
            }
        })
    }

    fn evaluate_batch(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        match self {
            Self::AlwaysTrue => Ok(BooleanArray::from(vec![true; batch.num_rows()])),
            Self::Leaf {
                field_name,
                data_type,
                function,
                operand,
            } => {
                let array = predicate_column(batch, field_name, data_type)?;
                match function {
                    LeafFunction::IsNull => is_null(array.as_ref()).map_err(Into::into),
                    LeafFunction::IsNotNull => is_not_null(array.as_ref()).map_err(Into::into),
                    _ => match operand {
                        Operand::Constant(matches) => constant_filter(array, *matches),
                        Operand::Membership(set) => set.evaluate(array, data_type, *function),
                        _ => compare(array, *function, operand, data_type),
                    },
                }
            }
            Self::Compound { function, children } => combine_masks(
                *function,
                batch.num_rows(),
                children.iter().map(|child| child.evaluate_batch(batch)),
            ),
        }
    }
}

#[derive(Debug)]
enum Operand {
    None,
    Constant(bool),
    Literal(Literal),
    Scalar(Box<TypedColumn>),
    Membership(MembershipSet),
}

impl Operand {
    fn new(
        function: LeafFunction,
        literals: &[Literal],
        data_type: &FlussDataType,
    ) -> Result<Self> {
        match function {
            LeafFunction::IsNull | LeafFunction::IsNotNull => return Ok(Self::None),
            LeafFunction::In | LeafFunction::NotIn => {
                return Ok(if literals.is_empty() {
                    Self::Constant(function == LeafFunction::NotIn)
                } else if function == LeafFunction::NotIn && literals.iter().any(Literal::is_null) {
                    Self::Constant(false)
                } else {
                    Self::Membership(MembershipSet::new(literals, data_type)?)
                });
            }
            _ => {}
        }
        let literal = &literals[0];
        match (data_type, literal) {
            (FlussDataType::Binary(_) | FlussDataType::Bytes(_), Literal::Bytes(_))
            | (FlussDataType::Time(_), Literal::Time(_))
            | (FlussDataType::Timestamp(_), Literal::TimestampNtz(_))
            | (FlussDataType::TimestampLTz(_), Literal::TimestampLtz(_)) => {
                // Comparison bounds keep their precision and length; row-writer
                // narrowing and fixed-width checks apply only to stored values.
                Ok(Self::Literal(literal.clone()))
            }
            (
                FlussDataType::Binary(_)
                | FlussDataType::Bytes(_)
                | FlussDataType::Time(_)
                | FlussDataType::Timestamp(_)
                | FlussDataType::TimestampLTz(_),
                _,
            ) => Err(literal_mismatch()),
            _ => Ok(Self::Scalar(Box::new(TypedColumn::build(
                literal_array(literal, data_type)?.as_ref(),
                data_type,
            )?))),
        }
    }
}

fn combine_masks(
    function: CompoundFunction,
    len: usize,
    masks: impl Iterator<Item = Result<BooleanArray>>,
) -> Result<BooleanArray> {
    let mut result = BooleanArray::from(vec![function == CompoundFunction::And; len]);
    for mask in masks {
        let mask = mask?;
        result = match function {
            CompoundFunction::And => and(&result, &mask),
            CompoundFunction::Or => or(&result, &mask),
        }?;
    }
    Ok(result)
}

#[derive(Debug)]
enum MembershipSet {
    Bytes(HashSet<Vec<u8>>),
    Timestamps(HashSet<TimestampNtz>),
    TimestampsLtz(HashSet<TimestampLtz>),
    Times(HashSet<i128>),
    Floats(HashSet<u64>),
    Rows {
        converter: RowConverter,
        values: HashSet<Box<[u8]>>,
    },
}

impl MembershipSet {
    fn new(literals: &[Literal], data_type: &FlussDataType) -> Result<Self> {
        macro_rules! core_set {
            ($literal:ident, $variant:ident, $value:ident => $convert:expr) => {{
                let values = literals
                    .iter()
                    .filter(|literal| !literal.is_null())
                    .map(|literal| match literal {
                        Literal::$literal($value) => Ok($convert),
                        _ => Err(literal_mismatch()),
                    })
                    .collect::<Result<HashSet<_>>>()?;
                return Ok(Self::$variant(values));
            }};
        }
        match data_type {
            FlussDataType::Binary(_) | FlussDataType::Bytes(_) => {
                core_set!(Bytes, Bytes, value => value.clone())
            }
            FlussDataType::Timestamp(_) => core_set!(TimestampNtz, Timestamps, value => *value),
            FlussDataType::TimestampLTz(_) => {
                core_set!(TimestampLtz, TimestampsLtz, value => *value)
            }
            FlussDataType::Time(_) => {
                core_set!(Time, Times, value => i128::from(*value) * 1_000_000)
            }
            _ => {}
        }
        let arrow_type = to_arrow_type(data_type)?;
        if matches!(arrow_type, ArrowDataType::Float32 | ArrowDataType::Float64) {
            let mut values = HashSet::with_capacity(literals.len());
            for literal in literals.iter().filter(|literal| !literal.is_null()) {
                let scalar = literal_array(literal, data_type)?;
                let value = match arrow_type {
                    ArrowDataType::Float32 => {
                        f64::from(scalar.as_primitive::<Float32Type>().value(0))
                    }
                    _ => scalar.as_primitive::<Float64Type>().value(0),
                };
                values.insert(canonicalize_nan(value).to_bits());
            }
            return Ok(Self::Floats(values));
        }
        let converter = RowConverter::new(vec![SortField::new(arrow_type)])?;
        let mut values = HashSet::with_capacity(literals.len());
        for literal in literals.iter().filter(|literal| !literal.is_null()) {
            let scalar = literal_array(literal, data_type)?;
            let rows = converter.convert_columns(&[scalar])?;
            values.insert(rows.row(0).as_ref().into());
        }
        Ok(Self::Rows { converter, values })
    }

    fn evaluate(
        &self,
        array: &ArrayRef,
        data_type: &FlussDataType,
        function: LeafFunction,
    ) -> Result<BooleanArray> {
        let negate = function == LeafFunction::NotIn;
        let (converter, values) = match self {
            Self::Bytes(values) => {
                let matches = |value: Option<&[u8]>| {
                    value.is_some_and(|value| values.contains(value) != negate)
                };
                return Ok(match TypedColumn::build(array.as_ref(), data_type)? {
                    TypedColumn::Binary(array) => array.iter().map(matches).collect(),
                    TypedColumn::Bytes(array) => array.iter().map(matches).collect(),
                    _ => return Err(literal_mismatch()),
                });
            }
            Self::Timestamps(values) => {
                let column = TypedColumn::build(array.as_ref(), data_type)?;
                return timestamp_mask(array, &column, TypedColumn::get_timestamp_ntz, |value| {
                    value.is_ok_and(|value| values.contains(&value)) != negate
                });
            }
            Self::TimestampsLtz(values) => {
                let column = TypedColumn::build(array.as_ref(), data_type)?;
                return timestamp_mask(array, &column, TypedColumn::get_timestamp_ltz, |value| {
                    value.is_ok_and(|value| values.contains(&value)) != negate
                });
            }
            Self::Times(values) => {
                let TypedColumn::Time(time) = TypedColumn::build(array.as_ref(), data_type)? else {
                    return Err(literal_mismatch());
                };
                return Ok((0..array.len())
                    .map(|i| array.is_valid(i) && (values.contains(&time.get_nanos(i)) != negate))
                    .collect());
            }
            Self::Floats(values) => {
                // Canonicalize only NaNs; signed-zero keys remain distinct.
                // Probe fixed-size keys without allocating per row or copying a batch.
                let matches = |value: Option<f64>| {
                    value.is_some_and(|value| {
                        values.contains(&canonicalize_nan(value).to_bits()) != negate
                    })
                };
                return Ok(match array.data_type() {
                    ArrowDataType::Float32 => array
                        .as_primitive::<Float32Type>()
                        .iter()
                        .map(|value| matches(value.map(f64::from)))
                        .collect(),
                    ArrowDataType::Float64 => array
                        .as_primitive::<Float64Type>()
                        .iter()
                        .map(matches)
                        .collect(),
                    _ => unreachable!("membership column type was validated"),
                });
            }
            Self::Rows { converter, values } => (converter, values),
        };
        if values.is_empty() {
            return constant_filter(array, negate);
        }
        // For the remaining scalar types, Arrow row encoding agrees with Fluss equality.
        let rows = converter.convert_columns(std::slice::from_ref(array))?;
        Ok(rows
            .iter()
            .enumerate()
            .map(|(index, row)| array.is_valid(index) && (values.contains(row.as_ref()) != negate))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes, RowType};
    use crate::predicate::tests::assert_comparisons;
    use crate::predicate::{Literal, Predicate, col};
    use crate::row::{TimestampLtz, TimestampNtz};
    use arrow::array::{
        ArrayRef, Decimal128Array, FixedSizeBinaryArray, Int32Array, Int64Array, StringArray,
        new_empty_array,
    };
    use arrow::compute::cast;
    use arrow::compute::kernels::boolean::not;
    use std::sync::Arc;

    #[test]
    fn partition_values_use_typed_parsing_and_exact_filters() {
        use crate::util::partition::convert_value_of_type;
        let cases = [
            (DataTypes::tinyint(), Literal::Int8(-1), "-1"),
            (DataTypes::smallint(), Literal::Int16(256), "256"),
            (DataTypes::int(), Literal::Int32(-1000), "-1000"),
            (
                DataTypes::bigint(),
                Literal::Int64(i64::MAX),
                "9223372036854775807",
            ),
            (DataTypes::boolean(), Literal::Bool(true), "true"),
            (DataTypes::float(), Literal::Float32(1.5), "1_5"),
            (DataTypes::double(), Literal::Float64(-1.5), "-1_5"),
            (DataTypes::double(), Literal::Float64(-0.0), "-0_0"),
            (DataTypes::float(), Literal::Float32(f32::INFINITY), "Inf"),
            (DataTypes::float(), Literal::Float32(-f32::NAN), "NaN"),
            (DataTypes::double(), Literal::Float64(-f64::NAN), "NaN"),
            (
                DataTypes::double(),
                Literal::Float64(f64::NEG_INFINITY),
                "-Inf",
            ),
            (DataTypes::string(), Literal::String("US".into()), "US"),
            (DataTypes::char(2), Literal::String("US".into()), "US"),
            (DataTypes::bytes(), Literal::Bytes(vec![0x00, 0xff]), "00ff"),
            (
                DataTypes::binary(2),
                Literal::Bytes(vec![0x00, 0xff]),
                "00ff",
            ),
            (DataTypes::date(), Literal::Date(-1), "1969-12-31"),
            (DataTypes::date(), Literal::Date(19_782), "2024-02-29"),
            (
                DataTypes::time_with_precision(3),
                Literal::Time(3_723_004),
                "01-02-03_004",
            ),
            (
                DataTypes::timestamp_with_precision(9),
                Literal::TimestampNtz(TimestampNtz::from_millis_nanos(-1, 900_000).unwrap()),
                "1969-12-31-23-59-59_9999",
            ),
            (
                DataTypes::timestamp_ltz_with_precision(9),
                Literal::TimestampLtz(TimestampLtz::from_millis_nanos(1, 900_000).unwrap()),
                "1970-01-01-00-00-00_0019",
            ),
        ];
        for (data_type, literal, encoded) in cases {
            let datum = literal.to_datum();
            assert_eq!(
                parse_value_of_type(encoded, &data_type).unwrap(),
                datum,
                "{encoded}"
            );
            let round_trip = convert_value_of_type(&datum, &data_type).unwrap();
            let row_type = RowType::new(vec![DataField::new("p", data_type, None)]);
            for (predicate, expected) in [
                (col("p").eq(literal.clone()), true),
                (col("p").ne(literal.clone()), false),
                (col("p").is_in([literal.clone()]), true),
                (col("p").not_in([literal.clone(), Literal::Null]), false),
                (col("p").is_null(), false),
                (col("p").is_not_null(), true),
            ] {
                let bound = BoundPredicate::bind(Some(&predicate), &row_type).unwrap();
                for value in [encoded, round_trip.as_str()] {
                    assert_eq!(
                        bound.evaluate_partition_value(value).unwrap(),
                        expected,
                        "{bound:?}: {value}"
                    );
                }
            }
        }
    }

    #[test]
    fn partition_evaluation_rejects_invalid_values_and_preserves_temporal_precision() {
        for (data_type, value) in [
            (DataTypes::tinyint(), "128"),
            (DataTypes::boolean(), "invalid"),
            (DataTypes::bytes(), "0ff"),
            (DataTypes::binary(2), "ff"),
            (DataTypes::bytes(), "é"),
            (DataTypes::date(), "2026-02-30"),
            (DataTypes::time(), "24-00-00_000"),
            (DataTypes::timestamp(), "1970-01-01-00-00-00_1234567890"),
        ] {
            let row_type = RowType::new(vec![DataField::new("p", data_type, None)]);
            let bound = BoundPredicate::bind(Some(&col("p").is_not_null()), &row_type).unwrap();
            assert!(bound.evaluate_partition_value(value).is_err(), "{value}");
        }
        for precision in [0, 3, 6, 9] {
            for (data_type, literal) in [
                (
                    DataTypes::timestamp_with_precision(precision),
                    Literal::TimestampNtz(TimestampNtz::from_millis_nanos(1, 900_000).unwrap()),
                ),
                (
                    DataTypes::timestamp_ltz_with_precision(precision),
                    Literal::TimestampLtz(TimestampLtz::from_millis_nanos(1, 900_000).unwrap()),
                ),
            ] {
                let row_type = RowType::new(vec![DataField::new("p", data_type, None)]);
                for (predicate, expected) in [
                    (col("p").lt(literal.clone()), [true, false, false, false]),
                    (col("p").eq(literal.clone()), [false, true, false, false]),
                    (
                        col("p").is_in([literal.clone()]),
                        [false, true, false, false],
                    ),
                    (col("p").not_in([literal]), [true, false, true, true]),
                ] {
                    let bound = BoundPredicate::bind(Some(&predicate), &row_type).unwrap();
                    for (value, expected) in [
                        "1970-01-01-00-00-00_001",
                        "1970-01-01-00-00-00_0019",
                        "1970-01-01-00-00-00_002",
                        "3000-01-01",
                    ]
                    .into_iter()
                    .zip(expected)
                    {
                        assert_eq!(
                            bound.evaluate_partition_value(value).unwrap(),
                            expected,
                            "{bound:?}: {value}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn binary_comparisons_use_signed_bytes() {
        use Ordering::{Equal, Greater, Less};
        use arrow::array::BinaryArray;
        let bytes: &[Option<&[u8]>] = &[
            Some(&[0x80]),
            Some(&[0xff]),
            Some(&[0]),
            Some(&[0x7f]),
            None,
        ];
        for (data_type, array) in [
            (
                DataTypes::bytes(),
                std::sync::Arc::new(BinaryArray::from(bytes.to_vec())) as ArrayRef,
            ),
            (
                DataTypes::binary(1),
                std::sync::Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(bytes.iter().copied(), 1)
                        .unwrap(),
                ) as ArrayRef,
            ),
        ] {
            assert_comparisons(
                data_type,
                array,
                Literal::Bytes(vec![0]),
                vec![Some(Less), Some(Less), Some(Equal), Some(Greater), None],
            );
        }
        assert_comparisons(
            DataTypes::bytes(),
            std::sync::Arc::new(BinaryArray::from(vec![
                Some(&[][..]),
                Some(&[0][..]),
                Some(&[0, 0x80][..]),
                Some(&[0, 0][..]),
                None,
            ])),
            Literal::Bytes(vec![0, 0]),
            vec![Some(Less), Some(Less), Some(Less), Some(Equal), None],
        );
    }

    #[test]
    fn nan_signs_and_payloads_share_one_equality_class() {
        use arrow::array::{Float32Array, Float64Array};
        // Explicit Java expectations, independent of Arrow/total_cmp ordering.
        let bits = [
            0x7ff8_0000_0000_0000,
            0xfff8_0000_0000_0000,
            0x7ff8_0000_2000_0000,
            0xfff8_0000_2000_0000,
            0x7ff0_0000_2000_0000,
        ];
        for single in [false, true] {
            let (data_type, array): (_, ArrayRef) = if single {
                (
                    DataTypes::float(),
                    std::sync::Arc::new(Float32Array::from(
                        bits.iter()
                            .map(|bits| Some(f64::from_bits(*bits) as f32))
                            .chain([None])
                            .collect::<Vec<_>>(),
                    )),
                )
            } else {
                (
                    DataTypes::double(),
                    std::sync::Arc::new(Float64Array::from(
                        bits.iter()
                            .map(|bits| Some(f64::from_bits(*bits)))
                            .chain([None])
                            .collect::<Vec<_>>(),
                    )),
                )
            };
            for literal in [f64::NAN, f64::from_bits(bits[3])] {
                assert_comparisons(
                    data_type.clone(),
                    array.clone(),
                    Literal::Float64(literal),
                    vec![
                        Some(Ordering::Equal),
                        Some(Ordering::Equal),
                        Some(Ordering::Equal),
                        Some(Ordering::Equal),
                        Some(Ordering::Equal),
                        None,
                    ],
                );
            }
        }
    }

    // Independent comparison path for membership differential tests.
    fn evaluate_membership_linear(
        array: &ArrayRef,
        data_type: &FlussDataType,
        function: LeafFunction,
        literals: &[Literal],
        len: usize,
    ) -> Result<BooleanArray> {
        let mut result = BooleanArray::from(vec![false; len]);
        for literal in literals.iter().filter(|literal| !literal.is_null()) {
            let operand = Operand::new(
                LeafFunction::Equal,
                std::slice::from_ref(literal),
                data_type,
            )?;
            let mask = compare(array, LeafFunction::Equal, &operand, data_type)?;
            result = or(&result, &mask)?;
        }

        if function == LeafFunction::In {
            return Ok(result);
        }

        let non_null = is_not_null(array.as_ref())?;
        and(&non_null, &not(&result)?).map_err(Into::into)
    }

    fn row_type() -> RowType {
        RowType::new(vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("name", DataTypes::string(), None),
        ])
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            ),
            (
                "name",
                Arc::new(StringArray::from(vec![
                    Some("alpha"),
                    None,
                    Some("alphabet"),
                ])) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn evaluate(predicate: Predicate) -> Vec<bool> {
        BoundPredicate::bind(Some(&predicate), &row_type())
            .unwrap()
            .evaluate_batch(&batch())
            .unwrap()
            .values()
            .iter()
            .collect()
    }

    #[test]
    fn evaluates_string_functions_and_null_inputs_exactly() {
        assert_eq!(
            evaluate(col("name").starts_with("alpha")),
            vec![true, false, true]
        );
        assert_eq!(
            evaluate(col("name").ends_with("bet")),
            vec![false, false, true]
        );
        assert_eq!(
            evaluate(col("name").contains("ph")),
            vec![true, false, true]
        );
    }

    #[test]
    fn compound_predicates_treat_null_leaf_results_as_false() {
        assert_eq!(
            evaluate(col("id").eq(1_i32).or(col("name").is_null())),
            vec![true, true, false]
        );
    }

    #[test]
    fn in_and_not_in_match_core_null_semantics() {
        for (predicate, expected) in [
            (
                col("id").is_in(vec![Some(1_i32), None]),
                [true, false, false],
            ),
            (
                col("id").not_in(vec![Some(1_i32), None]),
                [false, false, false],
            ),
            (col("id").is_in(Vec::<i32>::new()), [false, false, false]),
            (col("id").not_in(Vec::<i32>::new()), [true, false, true]),
        ] {
            assert_eq!(evaluate(predicate), expected);
        }
    }

    #[test]
    fn manually_constructed_leaves_reject_invalid_shapes_and_types() {
        let batch = RecordBatch::try_from_iter([(
            "c",
            Arc::new(StringArray::from(vec![Some("a"), None])) as ArrayRef,
        )])
        .unwrap();
        let leaf = |function, literals| BoundPredicate::Leaf {
            field_index: 0,
            field_id: 0,
            field_name: "c".into(),
            data_type: DataTypes::string(),
            function,
            literals,
        };
        for function in [
            LeafFunction::Equal,
            LeafFunction::NotEqual,
            LeafFunction::LessThan,
            LeafFunction::LessOrEqual,
            LeafFunction::GreaterThan,
            LeafFunction::GreaterOrEqual,
            LeafFunction::StartsWith,
            LeafFunction::EndsWith,
            LeafFunction::Contains,
        ] {
            for literals in [
                vec![],
                vec![Literal::String("a".into()); 2],
                vec![Literal::Null],
            ] {
                let predicate = leaf(function, literals);
                assert!(matches!(
                    predicate.evaluate_batch(&batch),
                    Err(IllegalArgument { .. })
                ));
                assert!(matches!(
                    predicate.into_evaluator(),
                    Err(IllegalArgument { .. })
                ));
            }
        }
        for function in [LeafFunction::IsNull, LeafFunction::IsNotNull] {
            assert!(
                leaf(function, vec![Literal::Null])
                    .evaluate_batch(&batch)
                    .is_err()
            );
        }
        let mut predicate = leaf(LeafFunction::StartsWith, vec![Literal::Int32(1)]);
        if let BoundPredicate::Leaf { data_type, .. } = &mut predicate {
            *data_type = DataTypes::int();
        }
        assert!(predicate.into_evaluator().is_err());
    }

    fn assert_membership_strategies(
        data_type: FlussDataType,
        array: ArrayRef,
        literals: Vec<Literal>,
    ) {
        let row_type = RowType::new(vec![DataField::new("c", data_type.clone(), None)]);
        let batch = RecordBatch::try_from_iter([("c", array.clone())]).unwrap();
        for function in [LeafFunction::In, LeafFunction::NotIn] {
            for with_null in [false, true] {
                let mut literals = literals.clone();
                literals.extend(literals.clone());
                if with_null {
                    literals.push(Literal::Null);
                }
                let predicate = Predicate::Leaf {
                    field: "c".into(),
                    function,
                    literals,
                };
                let bound = BoundPredicate::bind(Some(&predicate), &row_type).unwrap();
                let BoundPredicate::Leaf { literals, .. } = &bound else {
                    unreachable!()
                };
                let expected = if function == LeafFunction::NotIn && with_null {
                    BooleanArray::from(vec![false; array.len()])
                } else {
                    evaluate_membership_linear(&array, &data_type, function, literals, array.len())
                        .unwrap()
                };
                assert_eq!(bound.evaluate_batch(&batch).unwrap(), expected);
                let evaluator = bound.into_evaluator().unwrap();
                assert_eq!(evaluator.evaluate_batch(&batch).unwrap(), expected);
                assert_eq!(
                    evaluator.evaluate_batch(&batch.slice(0, 0)).unwrap().len(),
                    0
                );
            }
        }
    }

    #[test]
    fn hashed_membership_matches_linear_comparison_for_scalar_types() {
        use arrow::array::{BinaryArray, Float64Array};
        let nan = f64::from_bits(f64::NAN.to_bits() + (1 << 30));
        for single in [false, true] {
            let values = vec![
                Some(-0.0),
                Some(0.0),
                Some(f64::NAN),
                Some(-f64::NAN),
                Some(nan),
                Some(1.0),
                Some(2.0),
                None,
            ];
            let data_type = if single {
                DataTypes::float()
            } else {
                DataTypes::double()
            };
            let array = cast(
                &Float64Array::from(values),
                &to_arrow_type(&data_type).unwrap(),
            )
            .unwrap();
            let literals = [-0.0, nan, 1.0]
                .into_iter()
                .map(|v| {
                    if single {
                        Literal::Float32(v as f32)
                    } else {
                        Literal::Float64(v)
                    }
                })
                .collect();
            assert_membership_strategies(data_type, array, literals);
        }
        assert_membership_strategies(
            DataTypes::int(),
            Arc::new(Int32Array::from(vec![Some(-1), Some(0), Some(1), None])),
            vec![Literal::Int32(-1), Literal::Int32(0)],
        );
        assert_membership_strategies(
            DataTypes::string(),
            Arc::new(StringArray::from(vec![
                Some(""),
                Some("a"),
                Some("ab"),
                Some("你好"),
                None,
            ])),
            vec![Literal::String("a".into()), Literal::String("你好".into())],
        );
        assert_membership_strategies(
            DataTypes::bytes(),
            Arc::new(BinaryArray::from(vec![
                Some(b"a".as_slice()),
                Some(b"ab".as_slice()),
                None,
            ])),
            vec![Literal::Bytes(b"a".to_vec())],
        );
        assert_membership_strategies(
            DataTypes::binary(2),
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    [Some(b"ab".as_slice()), Some(b"cd".as_slice()), None].into_iter(),
                    2,
                )
                .unwrap(),
            ),
            vec![Literal::Bytes(b"ab".to_vec())],
        );
        let values = vec![Some(-(10_i128.pow(38) - 1)), Some(123), Some(124), None];
        assert_membership_strategies(
            DataTypes::decimal(38, 2),
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            vec![Literal::Decimal(
                crate::row::Decimal::from_unscaled_bytes(&123_i128.to_be_bytes(), 38, 2).unwrap(),
            )],
        );
    }

    #[test]
    fn prepared_evaluator_reuses_constants_and_validates_each_batch() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<PredicateEvaluator>();
        let predicate = col("id")
            .is_in(0..128)
            .and(col("id").ge(1))
            .and(col("name").starts_with("alpha"))
            .and(col("name").is_not_null());
        let bound = BoundPredicate::bind(Some(&predicate), &row_type()).unwrap();
        let evaluator = bound.clone().into_evaluator().unwrap();
        for (input, expected) in [
            (batch(), vec![true, false, true]),
            (batch().slice(1, 2), vec![false, true]),
            (batch().slice(0, 0), vec![]),
            (
                RecordBatch::try_from_iter([
                    ("id", Arc::new(Int32Array::from(vec![0, 2, 3])) as ArrayRef),
                    (
                        "name",
                        Arc::new(StringArray::from(vec!["alpha", "beta", "alpha"])) as ArrayRef,
                    ),
                ])
                .unwrap(),
                vec![false, false, true],
            ),
        ] {
            let expected = BooleanArray::from(expected);
            assert_eq!(evaluator.evaluate_batch(&input).unwrap(), expected);
            assert_eq!(bound.evaluate_batch(&input).unwrap(), expected);
        }
        let missing = RecordBatch::try_from_iter([("name", batch().column(1).clone())]).unwrap();
        let wrong = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("name", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
        ])
        .unwrap();
        // Constant folding must not bypass validation of the referenced column.
        for predicate in [
            predicate,
            col("id").is_in(Vec::<i32>::new()),
            col("id").not_in([Literal::Null]),
        ] {
            let bound = BoundPredicate::bind(Some(&predicate), &row_type()).unwrap();
            let evaluator = bound.clone().into_evaluator().unwrap();
            for invalid in [&missing, &wrong] {
                assert!(bound.evaluate_batch(invalid).is_err());
                assert!(evaluator.evaluate_batch(invalid).is_err());
            }
        }
    }

    #[test]
    fn binary_literals_allow_different_lengths_but_columns_keep_their_width() {
        use Ordering::{Equal, Greater, Less};
        use arrow::array::BinaryArray;
        let values = [Some(&[1, 2, 3, 4][..]), None];
        for (data_type, array) in [
            (
                DataTypes::bytes(),
                Arc::new(BinaryArray::from(values.to_vec())) as ArrayRef,
            ),
            (
                DataTypes::binary(4),
                Arc::new(
                    FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 4)
                        .unwrap(),
                ) as ArrayRef,
            ),
        ] {
            for (literal, ordering) in [
                (vec![], Greater),
                (vec![1, 2, 3], Greater),
                (vec![1, 2, 3, 4], Equal),
                (vec![1, 2, 3, 4, 0], Less),
                (vec![1, 2, 4], Less),
                (vec![1, 2, 3, 0x80], Greater),
            ] {
                assert_comparisons(
                    data_type.clone(),
                    array.clone(),
                    Literal::Bytes(literal),
                    vec![Some(ordering), None],
                );
            }
            assert_membership_strategies(
                data_type,
                array,
                vec![vec![], vec![1, 2, 3], vec![1, 2, 3, 4], vec![1, 2, 3, 4, 0]]
                    .into_iter()
                    .map(Literal::Bytes)
                    .collect(),
            );
        }
        let row_type = RowType::new(vec![DataField::new("c", DataTypes::binary(4), None)]);
        let bound =
            BoundPredicate::bind(Some(&col("c").ne(Literal::Bytes(vec![1, 2, 3]))), &row_type)
                .unwrap();
        assert!(bound.evaluate_partition_value("01020304").unwrap());
        assert!(bound.evaluate_partition_value("010203").is_err());
        let evaluator = bound.clone().into_evaluator().unwrap();
        for wrong_type in [DataTypes::bytes(), DataTypes::binary(3)] {
            let batch = RecordBatch::try_from_iter([(
                "c",
                new_empty_array(&to_arrow_type(&wrong_type).unwrap()),
            )])
            .unwrap();
            assert!(bound.evaluate_batch(&batch).is_err());
            assert!(evaluator.evaluate_batch(&batch).is_err());
        }
    }

    #[test]
    fn scalar_comparisons_preserve_nulls_and_operators() {
        assert_comparisons(
            DataTypes::int(),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(2), Some(3)])),
            Literal::Int32(2),
            vec![
                Some(Ordering::Less),
                None,
                Some(Ordering::Equal),
                Some(Ordering::Greater),
            ],
        );
    }

    #[test]
    fn core_converted_scalars_preserve_comparison_semantics() {
        for (data_type, literal) in [
            (DataTypes::boolean(), Literal::Bool(true)),
            (DataTypes::int(), Literal::Int32(-3)),
            (DataTypes::double(), Literal::Float64(-2.5)),
            (DataTypes::string(), Literal::String("hello".into())),
            (DataTypes::bytes(), Literal::Bytes(vec![0, 255])),
            (DataTypes::binary(4), Literal::Bytes(vec![1, 2, 3, 4])),
            (DataTypes::date(), Literal::Date(-1)),
            (DataTypes::time_with_precision(0), Literal::Time(1_000)),
            (DataTypes::time_with_precision(9), Literal::Time(1)),
        ] {
            let scalar = literal_array(&literal, &data_type).unwrap();
            assert_eq!(scalar.len(), 1);
            assert_eq!(scalar.data_type(), &to_arrow_type(&data_type).unwrap());
            let indices = arrow::array::UInt32Array::from(vec![Some(0), None, Some(0)]);
            let array = arrow::compute::take(scalar.as_ref(), &indices, None).unwrap();
            assert_comparisons(
                data_type,
                array,
                literal,
                vec![Some(Ordering::Equal), None, Some(Ordering::Equal)],
            );
        }
    }

    #[test]
    fn temporal_comparisons_match_exact_nanosecond_ordering() {
        for (precision, millis, nanos) in [
            (0_u32, 1_500, 0),
            (0, -1_500, 0),
            (1, 1, 1),
            (3, -1, 999_999),
            (3, i64::from(i32::MIN), 0),
            (6, 1, 1),
            (6, -9_223_372_036_854_776, 192_000),
            (6, i64::MAX, 0),
            (9, 1, 1),
            (9, i64::from(i32::MAX), 0),
            (9, -9_223_372_036_855, 224_191),
            (9, -9_223_372_036_855, 224_192),
            (9, 9_223_372_036_854, 775_807),
            (9, 9_223_372_036_854, 775_808),
        ] {
            let tick_nanos = 10_i128.pow(9 - precision.div_ceil(3) * 3);
            let mut cases = vec![
                (
                    DataTypes::timestamp_with_precision(precision),
                    Literal::TimestampNtz(TimestampNtz::from_millis_nanos(millis, nanos).unwrap()),
                ),
                (
                    DataTypes::timestamp_ltz_with_precision(precision),
                    Literal::TimestampLtz(TimestampLtz::from_millis_nanos(millis, nanos).unwrap()),
                ),
            ];
            if nanos == 0 {
                if let Ok(millis) = i32::try_from(millis) {
                    cases.push((
                        DataTypes::time_with_precision(precision),
                        Literal::Time(millis),
                    ));
                }
            }
            let literal_nanos = i128::from(millis) * 1_000_000 + i128::from(nanos);
            for (data_type, literal) in cases {
                let arrow_type = to_arrow_type(&data_type).unwrap();
                let time32 = matches!(arrow_type, ArrowDataType::Time32(_));
                let (min, max) = if time32 {
                    (i128::from(i32::MIN), i128::from(i32::MAX))
                } else {
                    (i128::from(i64::MIN), i128::from(i64::MAX))
                };
                let near = literal_nanos / tick_nanos;
                let values: Vec<_> = [min, max, -2, -1, 0, 1, 2, near - 1, near, near + 1]
                    .into_iter()
                    .filter(|v| (min..=max).contains(v))
                    .map(|v| Some(v as i64))
                    .chain([None])
                    .collect();
                let integers: ArrayRef = if time32 {
                    Arc::new(Int32Array::from(
                        values
                            .iter()
                            .map(|v| v.map(|v| v as i32))
                            .collect::<Vec<_>>(),
                    ))
                } else {
                    Arc::new(Int64Array::from(values.clone()))
                };
                let array = cast(integers.as_ref(), &arrow_type).unwrap();
                // Compare in the exact nanosecond domain, independently of the core timestamp accessors.
                let orderings = values
                    .iter()
                    .map(|v| v.map(|v| (i128::from(v) * tick_nanos).cmp(&literal_nanos)))
                    .collect();
                assert_comparisons(data_type, array, literal, orderings);
            }
        }
    }

    #[test]
    fn temporal_membership_keeps_exact_literals() {
        let data_type = DataTypes::timestamp_with_precision(0);
        let row_type = RowType::new(vec![DataField::new("c", data_type.clone(), None)]);
        let array = cast(
            &Int64Array::from(vec![Some(1), None, Some(2)]),
            &to_arrow_type(&data_type).unwrap(),
        )
        .unwrap();
        let batch = RecordBatch::try_from_iter([("c", array)]).unwrap();
        let literals = vec![
            Literal::TimestampNtz(TimestampNtz::new(1_500)),
            Literal::TimestampNtz(TimestampNtz::new(2_000)),
        ];
        let mut with_null = literals.clone();
        with_null.push(Literal::Null);
        for (predicate, expected) in [
            (col("c").is_in(literals.clone()), [false, false, true]),
            (col("c").not_in(literals), [true, false, false]),
            (col("c").is_in(with_null.clone()), [false, false, true]),
            (col("c").not_in(with_null), [false, false, false]),
        ] {
            let mask = BoundPredicate::bind(Some(&predicate), &row_type)
                .unwrap()
                .evaluate_batch(&batch)
                .unwrap();
            assert_eq!(mask.iter().collect::<Vec<_>>(), expected.map(Some));
        }
    }

    #[test]
    fn decimal_literals_preserve_signed_unscaled_values() {
        for unscaled in [0, i128::from(i64::MAX) + 1, -(10_i128.pow(38) - 1)] {
            let value =
                crate::row::Decimal::from_unscaled_bytes(&unscaled.to_be_bytes(), 38, 3).unwrap();
            let array =
                literal_array(&Literal::Decimal(value), &DataTypes::decimal(38, 3)).unwrap();
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            assert_eq!(array.values().as_ref(), &[unscaled]);
            assert_eq!(array.data_type(), &ArrowDataType::Decimal128(38, 3));
        }
    }
}
