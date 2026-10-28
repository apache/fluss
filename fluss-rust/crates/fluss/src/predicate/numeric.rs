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

//! Exact numeric boundary rewriting at bind time; wire coercion remains unchanged.

use super::coercion::{float32, float64, integer_in_range, integer_value, mismatch, rescale};
use crate::error::Result;
use crate::metadata::{DataField, DataType};
use crate::predicate::{LeafFunction, Literal};
use crate::row::Decimal;
use bigdecimal::num_traits::ToPrimitive;
use bigdecimal::{BigDecimal, RoundingMode};
use std::cmp::Ordering;

// Value compares the converted boundary to the original literal; it must be
// an adjacent representable value. Outside records the literal's side of the range.
enum Bound {
    Value(Literal, Ordering),
    Outside(Ordering),
}

pub(super) fn bind(
    field: &DataField,
    function: LeafFunction,
    literals: &[Literal],
) -> Result<Option<(LeafFunction, Vec<Literal>)>> {
    if !matches!(
        field.data_type(),
        DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::Decimal(_)
    ) {
        return Ok(None);
    }
    let rewritten = match function {
        LeafFunction::IsNull | LeafFunction::IsNotNull => (function, vec![]),
        LeafFunction::In | LeafFunction::NotIn => {
            let mut values = Vec::with_capacity(literals.len());
            for literal in literals {
                if literal.is_null() {
                    values.push(Literal::Null);
                } else if let Bound::Value(value, Ordering::Equal) = convert(field, literal)? {
                    values.push(value);
                }
            }
            (function, values)
        }
        _ => rewrite(function, convert(field, &literals[0])?),
    };
    Ok(Some(rewritten))
}

fn rewrite(function: LeafFunction, bound: Bound) -> (LeafFunction, Vec<Literal>) {
    use LeafFunction::*;
    let (value, direction) = match bound {
        Bound::Outside(side) => {
            return constant(match function {
                Equal => false,
                NotEqual => true,
                LessThan | LessOrEqual => side == Ordering::Greater,
                GreaterThan | GreaterOrEqual => side == Ordering::Less,
                _ => unreachable!("non-comparison in numeric rewrite"),
            });
        }
        Bound::Value(value, direction) => (value, direction),
    };
    let function = match (function, direction) {
        (_, Ordering::Equal) => function,
        (Equal, _) => return constant(false),
        (NotEqual, _) => return constant(true),
        (LessThan | LessOrEqual, Ordering::Less) => LessOrEqual,
        (LessThan | LessOrEqual, Ordering::Greater) => LessThan,
        (GreaterThan | GreaterOrEqual, Ordering::Less) => GreaterThan,
        (GreaterThan | GreaterOrEqual, Ordering::Greater) => GreaterOrEqual,
        _ => unreachable!("non-comparison in numeric rewrite"),
    };
    (function, vec![value])
}

// Keep field identity and null filtering without adding new predicate node kinds.
fn constant(matches: bool) -> (LeafFunction, Vec<Literal>) {
    (
        if matches {
            LeafFunction::IsNotNull
        } else {
            LeafFunction::In
        },
        vec![],
    )
}

fn convert(field: &DataField, literal: &Literal) -> Result<Bound> {
    // Preserve existing coercions for exact values and special floats.
    if let Ok(value) = exact_coercion(field, literal) {
        return Ok(Bound::Value(value, Ordering::Equal));
    }
    let value = match literal {
        Literal::Int8(v) => BigDecimal::from(*v),
        Literal::Int16(v) => BigDecimal::from(*v),
        Literal::Int32(v) => BigDecimal::from(*v),
        Literal::Int64(v) => BigDecimal::from(*v),
        Literal::Decimal(v) => v.to_big_decimal(),
        Literal::Float32(v) => return convert_float_literal(field, literal, f64::from(*v)),
        Literal::Float64(v) => return convert_float_literal(field, literal, *v),
        _ => return Err(mismatch(field, literal)),
    };
    convert_finite(field, literal, &value)
}

fn convert_float_literal(field: &DataField, literal: &Literal, value: f64) -> Result<Bound> {
    if value.is_infinite() {
        return Ok(Bound::Outside(if value.is_sign_negative() {
            Ordering::Less
        } else {
            Ordering::Greater
        }));
    }
    // Decode the binary value exactly: formatting the float itself as decimal
    // text would hide its rounding error. NaN has no integer/decimal value.
    let value = BigDecimal::try_from(value).map_err(|_| mismatch(field, literal))?;
    convert_finite(field, literal, &value)
}

fn convert_finite(field: &DataField, original: &Literal, value: &BigDecimal) -> Result<Bound> {
    let data_type = field.data_type();
    if matches!(data_type, DataType::Float(_) | DataType::Double(_)) {
        // Parse the exact decimal expansion directly into the target width;
        // an intermediate f64 could double-round a boundary intended for f32.
        let text = value.to_string();
        let (literal, rounded) = if matches!(data_type, DataType::Float(_)) {
            let narrowed = text.parse::<f32>().map_err(|_| mismatch(field, original))?;
            (Literal::Float32(narrowed), f64::from(narrowed))
        } else {
            let rounded = text.parse::<f64>().map_err(|_| mismatch(field, original))?;
            (Literal::Float64(rounded), rounded)
        };
        // Infinity is a representable float boundary, not an out-of-domain bound:
        // the column may itself contain infinities or NaNs ordered beyond them.
        let direction = if rounded.is_infinite() {
            if rounded.is_sign_negative() {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        } else {
            BigDecimal::try_from(rounded)
                .map_err(|_| mismatch(field, original))?
                .cmp(value)
        };
        return Ok(Bound::Value(literal, direction));
    }
    let (min, max, scale) = match data_type {
        DataType::TinyInt(_) => (i128::from(i8::MIN), i128::from(i8::MAX), 0),
        DataType::SmallInt(_) => (i128::from(i16::MIN), i128::from(i16::MAX), 0),
        DataType::Int(_) => (i128::from(i32::MIN), i128::from(i32::MAX), 0),
        DataType::BigInt(_) => (i128::from(i64::MIN), i128::from(i64::MAX), 0),
        DataType::Decimal(t) => {
            let max = 10_i128.pow(t.precision()) - 1;
            (-max, max, i64::from(t.scale()))
        }
        _ => unreachable!("non-numeric field in numeric conversion"),
    };
    for (limit, side) in [(min, Ordering::Less), (max, Ordering::Greater)] {
        if value.cmp(&BigDecimal::new(limit.into(), scale)) == side {
            return Ok(Bound::Outside(side));
        }
    }
    let rounded = value.with_scale_round(scale, RoundingMode::Floor);
    let direction = rounded.cmp(value);
    let literal = match data_type {
        DataType::Decimal(t) => Literal::Decimal(Decimal::from_big_decimal(
            rounded,
            t.precision(),
            t.scale(),
        )?),
        _ => {
            let integer = rounded.to_i64().ok_or_else(|| mismatch(field, original))?;
            exact_coercion(field, &Literal::Int64(integer))?
        }
    };
    Ok(Bound::Value(literal, direction))
}

fn exact_coercion(field: &DataField, literal: &Literal) -> Result<Literal> {
    Ok(match field.data_type() {
        DataType::TinyInt(_) => {
            Literal::Int8(integer_in_range(field, literal, i8::MAX as i64)? as i8)
        }
        DataType::SmallInt(_) => {
            Literal::Int16(integer_in_range(field, literal, i16::MAX as i64)? as i16)
        }
        DataType::Int(_) => {
            Literal::Int32(integer_in_range(field, literal, i32::MAX as i64)? as i32)
        }
        DataType::BigInt(_) => Literal::Int64(integer_value(field, literal)?),
        DataType::Float(_) => Literal::Float32(float32(field, literal)?),
        DataType::Double(_) => Literal::Float64(float64(field, literal)?),
        DataType::Decimal(t) => match literal {
            Literal::Decimal(v) => Literal::Decimal(rescale(field, v, t.precision(), t.scale())?),
            _ => return Err(mismatch(field, literal)),
        },
        _ => unreachable!("non-numeric field in numeric conversion"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypes, RowType};
    use crate::predicate::tests::assert_comparisons;
    use crate::predicate::{BoundPredicate, Predicate, col, to_pb_predicate};
    use arrow::array::{ArrayRef, Decimal128Array, Float64Array, Int64Array};
    use arrow::compute::cast;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn decimal(text: &str) -> Literal {
        let value: BigDecimal = text.parse().unwrap();
        let (_, scale) = value.as_bigint_and_exponent();
        Literal::Decimal(Decimal::from_big_decimal(value, 38, scale as u32).unwrap())
    }

    #[test]
    fn numeric_boundaries_preserve_comparison_results() {
        use Ordering::{Equal as E, Greater as G, Less as L};
        let decimal_max = 10_i128.pow(38) - 1;
        // Decimal samples are unscaled; other samples are exact integer values.
        // Expected orderings compare each sample to the original literal.
        for (data_type, values, literal, expected) in [
            (
                DataTypes::tinyint(),
                [-128, -1, 127],
                Literal::Float64(-1.3),
                [L, G, G],
            ),
            (
                DataTypes::smallint(),
                [-32768, 1, 32767],
                Literal::Float64(1.3),
                [L, L, G],
            ),
            (
                DataTypes::int(),
                [i32::MIN as i128, 0, i32::MAX as i128],
                Literal::Int64(i64::MAX),
                [L, L, L],
            ),
            (
                DataTypes::tinyint(),
                [-128, 0, 127],
                Literal::Int64(-129),
                [G, G, G],
            ),
            (
                DataTypes::bigint(),
                [i64::MIN as i128, i64::MAX as i128 - 1, i64::MAX as i128],
                decimal("9223372036854775806.9"),
                [L, L, G],
            ),
            (
                DataTypes::decimal(3, 2),
                [-124, 123, 124],
                decimal("1.234"),
                [L, L, G],
            ),
            (
                DataTypes::decimal(3, 2),
                [-124, -123, 0],
                decimal("-1.234"),
                [L, G, G],
            ),
            (
                DataTypes::decimal(38, 0),
                [-decimal_max, 0, decimal_max],
                decimal("99999999999999999999999999999999999999"),
                [L, L, E],
            ),
            (
                DataTypes::decimal(38, 38),
                [-decimal_max, 0, decimal_max],
                decimal("0.000000000000000000000000000000000000001"),
                [L, L, G],
            ),
            (
                DataTypes::float(),
                [16_777_216, 16_777_218, 16_777_220],
                Literal::Int64(16_777_219),
                [L, L, G],
            ),
            (
                DataTypes::float(),
                [16_777_216, 16_777_218, 16_777_220],
                Literal::Int64(16_777_217),
                [L, G, G],
            ),
            (
                DataTypes::double(),
                [
                    9_007_199_254_740_992,
                    9_007_199_254_740_994,
                    9_007_199_254_740_996,
                ],
                Literal::Int64(9_007_199_254_740_995),
                [L, L, G],
            ),
            (
                DataTypes::double(),
                [
                    9_007_199_254_740_992,
                    9_007_199_254_740_994,
                    9_007_199_254_740_996,
                ],
                Literal::Int64(9_007_199_254_740_993),
                [L, G, G],
            ),
        ] {
            let array = if let DataType::Decimal(t) = &data_type {
                Arc::new(
                    Decimal128Array::from(
                        values
                            .into_iter()
                            .map(Some)
                            .chain([None])
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(t.precision() as u8, t.scale() as i8)
                    .unwrap(),
                ) as ArrayRef
            } else {
                cast(
                    &Int64Array::from(
                        values
                            .into_iter()
                            .map(|v| Some(i64::try_from(v).unwrap()))
                            .chain([None])
                            .collect::<Vec<_>>(),
                    ),
                    &crate::record::to_arrow_type(&data_type).unwrap(),
                )
                .unwrap()
            };
            assert_comparisons(
                data_type,
                array,
                literal,
                expected.into_iter().map(Some).chain([None]).collect(),
            );
        }
    }

    #[test]
    fn floating_special_values_follow_fluss_ordering() {
        use Ordering::{Equal as E, Greater as G, Less as L};
        let values = Float64Array::from(vec![
            Some(-f64::NAN),
            Some(f64::NEG_INFINITY),
            Some(-0.0),
            Some(0.0),
            Some(f64::INFINITY),
            Some(f64::NAN),
            None,
        ]);
        for data_type in [DataTypes::float(), DataTypes::double()] {
            let array = cast(&values, &crate::record::to_arrow_type(&data_type).unwrap()).unwrap();
            for (literal, expected) in [
                (-0.0, [G, L, E, G, G, G]),
                (0.0, [G, L, L, E, G, G]),
                (f64::NAN, [E, L, L, L, L, E]),
                (f64::MAX, [G, L, L, L, G, G]),
                (-f64::MAX, [G, L, G, G, G, G]),
                (f64::from_bits(1), [G, L, L, L, G, G]),
                (-f64::from_bits(1), [G, L, G, G, G, G]),
            ] {
                assert_comparisons(
                    data_type.clone(),
                    array.clone(),
                    Literal::Float64(literal),
                    expected.into_iter().map(Some).chain([None]).collect(),
                );
            }
        }
    }

    #[test]
    fn membership_preserves_null_and_drops_unrepresentable_values() {
        let row_type = RowType::new(vec![DataField::new("c", DataTypes::int(), None)]);
        let batch = RecordBatch::try_from_iter([(
            "c",
            cast(
                &Int64Array::from(vec![Some(1), Some(2), None]),
                &arrow::datatypes::DataType::Int32,
            )
            .unwrap(),
        )])
        .unwrap();
        let values = vec![
            Literal::Float64(1.3),
            Literal::Int64(2),
            Literal::Int64(i64::MAX),
            Literal::Null,
        ];
        for (predicate, expected) in [
            (col("c").is_in(values.clone()), [false, true, false]),
            (col("c").not_in(values), [false, false, false]),
        ] {
            let mask = BoundPredicate::bind(Some(&predicate), &row_type)
                .unwrap()
                .evaluate_batch(&batch)
                .unwrap();
            assert_eq!(mask.iter().collect::<Vec<_>>(), expected.map(Some));
        }
        assert!(BoundPredicate::bind(Some(&col("c").eq(f64::NAN)), &row_type).is_err());
        assert!(BoundPredicate::bind(Some(&col("c").eq("1.3")), &row_type).is_err());
    }

    #[test]
    fn pushdown_uses_rewritten_bounds_without_changing_wire_coercion() {
        let row_type = RowType::new(vec![
            DataField::with_field_id("i", DataTypes::int(), None, 0),
            DataField::with_field_id("f", DataTypes::float(), None, 1),
        ]);
        let predicate = col("i").ge(1.3);
        assert!(to_pb_predicate(&predicate, &row_type).is_err());
        let bind = |p: Predicate| {
            BoundPredicate::bind(Some(&p), &row_type)
                .unwrap()
                .to_pushdown_predicate()
        };
        assert_eq!(bind(predicate.clone()), Some(col("i").gt(1_i32)));
        assert_eq!(
            bind(predicate.clone().and(col("f").ge(1.3))),
            Some(col("i").gt(1_i32))
        );
        assert_eq!(bind(predicate.or(col("f").ge(1.3))), None);
    }
}
