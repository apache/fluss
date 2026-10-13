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

//! Encodes a [`Predicate`] into its protocol representation, mirroring Java's
//! `PredicateMessageUtils.toPbPredicate`: columns are resolved by name against
//! the table's row type and sent as schema field ids, and every literal is
//! coerced to the declared type of its column, since the server reads the log
//! unfiltered when a literal type root does not match the field's.

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataField, DataType, RowType, UNASSIGNED_FIELD_ID};
use crate::predicate::{CompoundFunction, LeafFunction, Literal, Predicate};
use crate::proto::{PbCompoundPredicate, PbLeafPredicate, PbLiteralValue, PbPredicate};
use crate::row::Decimal;

/// Wire codes for `PbPredicate.type`.
const PREDICATE_TYPE_LEAF: i32 = 0;
const PREDICATE_TYPE_COMPOUND: i32 = 1;

/// Wire code for `PbLeafPredicate.function`.
fn leaf_function_code(function: LeafFunction) -> i32 {
    match function {
        LeafFunction::Equal => 0,
        LeafFunction::NotEqual => 1,
        LeafFunction::LessThan => 2,
        LeafFunction::LessOrEqual => 3,
        LeafFunction::GreaterThan => 4,
        LeafFunction::GreaterOrEqual => 5,
        LeafFunction::IsNull => 6,
        LeafFunction::IsNotNull => 7,
        LeafFunction::StartsWith => 8,
        LeafFunction::Contains => 9,
        LeafFunction::EndsWith => 10,
        LeafFunction::In => 11,
        LeafFunction::NotIn => 12,
    }
}

/// Wire code for `PbCompoundPredicate.function`.
fn compound_function_code(function: CompoundFunction) -> i32 {
    match function {
        CompoundFunction::And => 0,
        CompoundFunction::Or => 1,
    }
}

/// Wire code for `PbLiteralValue.literal_type`, telling the server which value
/// field is populated.
fn literal_type_code(field: &DataField) -> Result<i32> {
    Ok(match field.data_type() {
        DataType::Boolean(_) => 0,
        DataType::TinyInt(_) => 1,
        DataType::SmallInt(_) => 2,
        DataType::Int(_) => 3,
        DataType::BigInt(_) => 4,
        DataType::Float(_) => 5,
        DataType::Double(_) => 6,
        DataType::Char(_) => 7,
        DataType::String(_) => 8,
        DataType::Decimal(_) => 9,
        DataType::Date(_) => 10,
        DataType::Time(_) => 11,
        DataType::Timestamp(_) => 12,
        DataType::TimestampLTz(_) => 13,
        DataType::Binary(_) => 14,
        DataType::Bytes(_) => 15,
        DataType::Array(_) | DataType::Map(_) | DataType::Row(_) => {
            return Err(unsupported_column(field));
        }
    })
}

/// Encodes `predicate` against `row_type`, which must be the table's full row
/// type: the server evaluates the filter on unprojected batches, so resolving
/// against a projected row type would send field ids for the wrong columns.
pub(crate) fn to_pb_predicate(predicate: &Predicate, row_type: &RowType) -> Result<PbPredicate> {
    match predicate {
        Predicate::Leaf {
            field,
            function,
            literals,
        } => {
            let data_field = resolve_field(row_type, field)?;
            validate_leaf(*function, data_field, literals)?;
            Ok(PbPredicate {
                r#type: PREDICATE_TYPE_LEAF,
                leaf: Some(PbLeafPredicate {
                    function: leaf_function_code(*function),
                    field_id: data_field.field_id(),
                    literals: literals
                        .iter()
                        .map(|literal| to_pb_literal(data_field, literal))
                        .collect::<Result<Vec<_>>>()?,
                }),
                compound: None,
            })
        }
        Predicate::Compound { function, children } => {
            if children.is_empty() {
                return Err(IllegalArgument {
                    message: format!("{} predicate has no children", compound_name(*function)),
                });
            }
            Ok(PbPredicate {
                r#type: PREDICATE_TYPE_COMPOUND,
                leaf: None,
                compound: Some(PbCompoundPredicate {
                    function: compound_function_code(*function),
                    children: children
                        .iter()
                        .map(|child| to_pb_predicate(child, row_type))
                        .collect::<Result<Vec<_>>>()?,
                }),
            })
        }
    }
}

fn resolve_field<'a>(row_type: &'a RowType, name: &str) -> Result<&'a DataField> {
    let field = row_type
        .fields()
        .iter()
        .find(|field| field.name() == name)
        .ok_or_else(|| IllegalArgument {
            message: format!(
                "filter column '{}' does not exist in the table schema, available columns: {:?}",
                name,
                row_type.get_field_names()
            ),
        })?;

    // The server resolves the column by field id, so an unassigned one would be
    // unresolvable there.
    if field.field_id() == UNASSIGNED_FIELD_ID {
        return Err(IllegalArgument {
            message: format!("filter column '{name}' has no schema field id assigned"),
        });
    }
    Ok(field)
}

/// Checks the literal count and kind a function requires, so that a predicate
/// the server could only evaluate to garbage is rejected before it is sent.
fn validate_leaf(function: LeafFunction, field: &DataField, literals: &[Literal]) -> Result<()> {
    let expected = match function {
        // `IN`/`NOT IN` accept any count: empty ones match nothing/everything.
        LeafFunction::In | LeafFunction::NotIn => return Ok(()),
        LeafFunction::IsNull | LeafFunction::IsNotNull => 0,
        _ => 1,
    };
    if literals.len() != expected {
        return Err(IllegalArgument {
            message: format!(
                "{function:?} on column '{}' expects {expected} literal(s), got {}",
                field.name(),
                literals.len()
            ),
        });
    }

    // A null literal is only meaningful to `IN`/`NOT IN`, which skip it; every
    // other function compares against it and the server errors out.
    if literals.first() == Some(&Literal::Null) {
        return Err(IllegalArgument {
            message: format!(
                "{function:?} on column '{}' cannot take a null literal, use is_null()/is_not_null()",
                field.name()
            ),
        });
    }

    if matches!(
        function,
        LeafFunction::StartsWith | LeafFunction::EndsWith | LeafFunction::Contains
    ) && !matches!(field.data_type(), DataType::Char(_) | DataType::String(_))
    {
        return Err(IllegalArgument {
            message: format!(
                "{function:?} requires a character string column, but column '{}' is {}",
                field.name(),
                field.data_type()
            ),
        });
    }
    Ok(())
}

fn to_pb_literal(field: &DataField, literal: &Literal) -> Result<PbLiteralValue> {
    let data_type = field.data_type();
    let mut pb = PbLiteralValue {
        literal_type: literal_type_code(field)?,
        is_null: false,
        ..Default::default()
    };
    if matches!(literal, Literal::Null) {
        pb.is_null = true;
        return Ok(pb);
    }

    match data_type {
        DataType::Boolean(_) => match literal {
            Literal::Bool(value) => pb.boolean_value = Some(*value),
            _ => return Err(mismatch(field, literal)),
        },
        DataType::TinyInt(_) => {
            pb.int_value = Some(integer_in_range(field, literal, i8::MAX as i64)? as i32);
        }
        DataType::SmallInt(_) => {
            pb.int_value = Some(integer_in_range(field, literal, i16::MAX as i64)? as i32);
        }
        DataType::Int(_) => {
            pb.int_value = Some(integer_in_range(field, literal, i32::MAX as i64)? as i32);
        }
        DataType::BigInt(_) => pb.bigint_value = Some(integer_value(field, literal)?),
        DataType::Float(_) => pb.float_value = Some(float32(field, literal)?),
        DataType::Double(_) => pb.double_value = Some(float64(field, literal)?),
        DataType::Char(_) | DataType::String(_) => match literal {
            Literal::String(value) => pb.string_value = Some(value.clone()),
            _ => return Err(mismatch(field, literal)),
        },
        DataType::Binary(_) | DataType::Bytes(_) => match literal {
            Literal::Bytes(value) => pb.binary_value = Some(value.clone()),
            _ => return Err(mismatch(field, literal)),
        },
        DataType::Decimal(decimal_type) => {
            let Literal::Decimal(value) = literal else {
                return Err(mismatch(field, literal));
            };
            // The server rebuilds the decimal with the *field's* precision and
            // scale, so the unscaled value has to be expressed in those.
            let rescaled = rescale(field, value, decimal_type.precision(), decimal_type.scale())?;
            if rescaled.is_compact() {
                pb.decimal_value = Some(rescaled.to_unscaled_long()?);
            } else {
                pb.decimal_bytes = Some(rescaled.to_unscaled_bytes());
            }
        }
        // Dates travel as `bigint_value`, unlike every other 32-bit value.
        DataType::Date(_) => match literal {
            Literal::Date(days) => pb.bigint_value = Some(*days as i64),
            _ => return Err(mismatch(field, literal)),
        },
        DataType::Time(_) => match literal {
            Literal::Time(millis) => pb.int_value = Some(*millis),
            _ => return Err(mismatch(field, literal)),
        },
        DataType::Timestamp(_) => match literal {
            Literal::TimestampNtz(value) => {
                pb.timestamp_millis_value = Some(value.get_millisecond());
                pb.timestamp_nano_of_millis_value = Some(value.get_nano_of_millisecond());
            }
            _ => return Err(mismatch(field, literal)),
        },
        DataType::TimestampLTz(_) => match literal {
            Literal::TimestampLtz(value) => {
                pb.timestamp_millis_value = Some(value.get_epoch_millisecond());
                pb.timestamp_nano_of_millis_value = Some(value.get_nano_of_millisecond());
            }
            _ => return Err(mismatch(field, literal)),
        },
        DataType::Array(_) | DataType::Map(_) | DataType::Row(_) => {
            return Err(unsupported_column(field));
        }
    }
    Ok(pb)
}

/// Reads an integer literal of any width, so that a column can accept the one
/// Rust inferred for it.
fn integer_value(field: &DataField, literal: &Literal) -> Result<i64> {
    match literal {
        Literal::Int8(value) => Ok(*value as i64),
        Literal::Int16(value) => Ok(*value as i64),
        Literal::Int32(value) => Ok(*value as i64),
        Literal::Int64(value) => Ok(*value),
        _ => Err(mismatch(field, literal)),
    }
}

/// Reads an integer literal the column's type must hold, where `max` is its
/// upper bound and `-max - 1` its lower one.
fn integer_in_range(field: &DataField, literal: &Literal, max: i64) -> Result<i64> {
    let value = integer_value(field, literal)?;
    if value > max || value < -max - 1 {
        return Err(IllegalArgument {
            message: format!(
                "filter literal {value} is out of range for column '{}' of type {}",
                field.name(),
                field.data_type()
            ),
        });
    }
    Ok(value)
}

fn float32(field: &DataField, literal: &Literal) -> Result<f32> {
    if let Literal::Float32(value) = literal {
        return Ok(*value);
    }
    let value = float64(field, literal)?;
    let narrowed = value as f32;
    if narrowed as f64 != value && !value.is_nan() {
        return Err(inexact(field, value));
    }
    Ok(narrowed)
}

fn float64(field: &DataField, literal: &Literal) -> Result<f64> {
    match literal {
        Literal::Float64(value) => Ok(*value),
        Literal::Float32(value) => Ok(*value as f64),
        Literal::Int8(_) | Literal::Int16(_) | Literal::Int32(_) | Literal::Int64(_) => {
            let value = integer_value(field, literal)?;
            let widened = value as f64;
            if widened as i64 != value {
                return Err(inexact(field, widened));
            }
            Ok(widened)
        }
        _ => Err(mismatch(field, literal)),
    }
}

/// Rejects a conversion that would move the comparison bound, since the server
/// prunes batches by that bound and a shifted one drops matching rows.
fn inexact(field: &DataField, value: f64) -> crate::error::Error {
    IllegalArgument {
        message: format!(
            "filter literal {value} cannot be represented exactly by column '{}' of type {}",
            field.name(),
            field.data_type()
        ),
    }
}

fn rescale(field: &DataField, value: &Decimal, precision: u32, scale: u32) -> Result<Decimal> {
    let big_decimal = value.to_big_decimal();
    let rescaled = Decimal::from_big_decimal(big_decimal.clone(), precision, scale)?;
    // `from_big_decimal` rounds, which would move a comparison bound; only an
    // exact change of scale is safe.
    if rescaled.to_big_decimal() != big_decimal {
        return Err(IllegalArgument {
            message: format!(
                "filter literal {big_decimal} does not fit column '{}' of type {}",
                field.name(),
                field.data_type()
            ),
        });
    }
    Ok(rescaled)
}

fn mismatch(field: &DataField, literal: &Literal) -> crate::error::Error {
    IllegalArgument {
        message: format!(
            "filter literal {literal:?} does not match column '{}' of type {}",
            field.name(),
            field.data_type()
        ),
    }
}

fn unsupported_column(field: &DataField) -> crate::error::Error {
    IllegalArgument {
        message: format!(
            "filter on column '{}' of type {} is not supported",
            field.name(),
            field.data_type()
        ),
    }
}

fn compound_name(function: CompoundFunction) -> &'static str {
    match function {
        CompoundFunction::And => "AND",
        CompoundFunction::Or => "OR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        BigIntType, BinaryType, BooleanType, BytesType, CharType, DataField, DateType, DecimalType,
        DoubleType, FloatType, IntType, RowType, SmallIntType, StringType, TimeType,
        TimestampLTzType, TimestampType, TinyIntType,
    };
    use crate::predicate::col;
    use crate::row::{TimestampLtz, TimestampNtz};
    use bigdecimal::BigDecimal;
    use std::str::FromStr;

    /// A row type holding one column of `data_type` named `c`, with field id 7.
    fn row_type_of(data_type: DataType) -> RowType {
        RowType::new(vec![DataField::with_field_id("c", data_type, None, 7)])
    }

    /// Encodes a single-leaf predicate over one column and returns its literal.
    fn encode_literal(data_type: DataType, predicate: Predicate) -> Result<PbLiteralValue> {
        let pb = to_pb_predicate(&predicate, &row_type_of(data_type))?;
        Ok(pb.leaf.unwrap().literals.into_iter().next().unwrap())
    }

    fn literal_of(data_type: DataType, value: impl Into<Literal>) -> PbLiteralValue {
        encode_literal(data_type.clone(), col("c").eq(value)).unwrap()
    }

    #[test]
    fn wire_codes_match_the_protocol() {
        assert_eq!(leaf_function_code(LeafFunction::Equal), 0);
        assert_eq!(leaf_function_code(LeafFunction::NotEqual), 1);
        assert_eq!(leaf_function_code(LeafFunction::LessThan), 2);
        assert_eq!(leaf_function_code(LeafFunction::LessOrEqual), 3);
        assert_eq!(leaf_function_code(LeafFunction::GreaterThan), 4);
        assert_eq!(leaf_function_code(LeafFunction::GreaterOrEqual), 5);
        assert_eq!(leaf_function_code(LeafFunction::IsNull), 6);
        assert_eq!(leaf_function_code(LeafFunction::IsNotNull), 7);
        assert_eq!(leaf_function_code(LeafFunction::StartsWith), 8);
        assert_eq!(leaf_function_code(LeafFunction::Contains), 9);
        assert_eq!(leaf_function_code(LeafFunction::EndsWith), 10);
        assert_eq!(leaf_function_code(LeafFunction::In), 11);
        assert_eq!(leaf_function_code(LeafFunction::NotIn), 12);
        assert_eq!(compound_function_code(CompoundFunction::And), 0);
        assert_eq!(compound_function_code(CompoundFunction::Or), 1);
    }

    #[test]
    fn encodes_leaf_with_field_id_and_function() {
        let row_type = RowType::new(vec![
            DataField::with_field_id("a", DataType::Int(IntType::new()), None, 3),
            DataField::with_field_id("b", DataType::BigInt(BigIntType::new()), None, 5),
        ]);

        let pb = to_pb_predicate(&col("b").gt(30i32), &row_type).unwrap();

        assert_eq!(pb.r#type, PREDICATE_TYPE_LEAF);
        assert!(pb.compound.is_none());
        let leaf = pb.leaf.unwrap();
        assert_eq!(leaf.function, leaf_function_code(LeafFunction::GreaterThan));
        assert_eq!(leaf.field_id, 5);
        // An i32 literal against a BIGINT column travels as a BIGINT.
        assert_eq!(leaf.literals[0].literal_type, 4);
        assert_eq!(leaf.literals[0].bigint_value, Some(30));
        assert_eq!(leaf.literals[0].int_value, None);
    }

    #[test]
    fn encodes_nested_compound() {
        let row_type = RowType::new(vec![
            DataField::with_field_id("a", DataType::Int(IntType::new()), None, 1),
            DataField::with_field_id("b", DataType::Int(IntType::new()), None, 2),
            DataField::with_field_id("c", DataType::Int(IntType::new()), None, 3),
        ]);

        let pb = to_pb_predicate(
            &col("a")
                .eq(1i32)
                .and(col("b").eq(2i32))
                .or(col("c").eq(3i32)),
            &row_type,
        )
        .unwrap();

        assert_eq!(pb.r#type, PREDICATE_TYPE_COMPOUND);
        assert!(pb.leaf.is_none());
        let or = pb.compound.unwrap();
        assert_eq!(or.function, compound_function_code(CompoundFunction::Or));
        assert_eq!(or.children.len(), 2);

        let and = or.children[0].compound.as_ref().unwrap();
        assert_eq!(and.function, compound_function_code(CompoundFunction::And));
        let and_field_ids: Vec<i32> = and
            .children
            .iter()
            .map(|child| child.leaf.as_ref().unwrap().field_id)
            .collect();
        assert_eq!(and_field_ids, vec![1, 2]);
        assert_eq!(or.children[1].leaf.as_ref().unwrap().field_id, 3);
    }

    /// Locks each literal type against the `PbDataTypeRoot` codes in the proto.
    #[test]
    fn encodes_every_supported_type_root() {
        let boolean = literal_of(DataType::Boolean(BooleanType::new()), true);
        assert_eq!(boolean.literal_type, 0);
        assert_eq!(boolean.boolean_value, Some(true));

        let tinyint = literal_of(DataType::TinyInt(TinyIntType::new()), 7i8);
        assert_eq!(tinyint.literal_type, 1);
        assert_eq!(tinyint.int_value, Some(7));

        let smallint = literal_of(DataType::SmallInt(SmallIntType::new()), 7i16);
        assert_eq!(smallint.literal_type, 2);
        assert_eq!(smallint.int_value, Some(7));

        let int = literal_of(DataType::Int(IntType::new()), 7i32);
        assert_eq!(int.literal_type, 3);
        assert_eq!(int.int_value, Some(7));

        let bigint = literal_of(DataType::BigInt(BigIntType::new()), 7i64);
        assert_eq!(bigint.literal_type, 4);
        assert_eq!(bigint.bigint_value, Some(7));

        let float = literal_of(DataType::Float(FloatType::new()), 1.5f32);
        assert_eq!(float.literal_type, 5);
        assert_eq!(float.float_value, Some(1.5));

        let double = literal_of(DataType::Double(DoubleType::new()), 1.5f64);
        assert_eq!(double.literal_type, 6);
        assert_eq!(double.double_value, Some(1.5));

        let char_value = literal_of(DataType::Char(CharType::new(2)), "hi");
        assert_eq!(char_value.literal_type, 7);
        assert_eq!(char_value.string_value.as_deref(), Some("hi"));

        let string = literal_of(DataType::String(StringType::new()), "hi");
        assert_eq!(string.literal_type, 8);
        assert_eq!(string.string_value.as_deref(), Some("hi"));

        let binary = literal_of(DataType::Binary(BinaryType::new(2)), vec![1u8, 2]);
        assert_eq!(binary.literal_type, 14);
        assert_eq!(binary.binary_value, Some(vec![1, 2]));

        let bytes = literal_of(DataType::Bytes(BytesType::new()), vec![1u8, 2]);
        assert_eq!(bytes.literal_type, 15);
        assert_eq!(bytes.binary_value, Some(vec![1, 2]));

        let time = encode_literal(
            DataType::Time(TimeType::new(3).unwrap()),
            col("c").eq(Literal::Time(3_600_000)),
        )
        .unwrap();
        assert_eq!(time.literal_type, 11);
        assert_eq!(time.int_value, Some(3_600_000));
    }

    #[test]
    fn encodes_date_as_bigint() {
        let date = encode_literal(
            DataType::Date(DateType::new()),
            col("c").eq(Literal::Date(19_000)),
        )
        .unwrap();

        assert_eq!(date.literal_type, 10);
        assert_eq!(date.bigint_value, Some(19_000));
        assert_eq!(date.int_value, None);
    }

    #[test]
    fn encodes_both_timestamp_kinds() {
        let ntz = encode_literal(
            DataType::Timestamp(TimestampType::new(6).unwrap()),
            col("c").eq(TimestampNtz::from_millis_nanos(1_700_000_000_000, 123_456).unwrap()),
        )
        .unwrap();
        assert_eq!(ntz.literal_type, 12);
        assert_eq!(ntz.timestamp_millis_value, Some(1_700_000_000_000));
        assert_eq!(ntz.timestamp_nano_of_millis_value, Some(123_456));

        let ltz = encode_literal(
            DataType::TimestampLTz(TimestampLTzType::new(6).unwrap()),
            col("c").eq(TimestampLtz::from_millis_nanos(1_700_000_000_000, 123_456).unwrap()),
        )
        .unwrap();
        assert_eq!(ltz.literal_type, 13);
        assert_eq!(ltz.timestamp_millis_value, Some(1_700_000_000_000));
        assert_eq!(ltz.timestamp_nano_of_millis_value, Some(123_456));
    }

    #[test]
    fn encodes_decimal_in_both_modes() {
        let compact = encode_literal(
            DataType::Decimal(DecimalType::new(10, 2).unwrap()),
            col("c").eq(Decimal::from_unscaled_long(12_345, 10, 2).unwrap()),
        )
        .unwrap();
        assert_eq!(compact.literal_type, 9);
        assert_eq!(compact.decimal_value, Some(12_345));
        assert_eq!(compact.decimal_bytes, None);

        // A precision above 18 does not fit an i64 and travels as bytes.
        let non_compact = encode_literal(
            DataType::Decimal(DecimalType::new(20, 2).unwrap()),
            col("c").eq(
                Decimal::from_big_decimal(BigDecimal::from_str("123.45").unwrap(), 20, 2).unwrap(),
            ),
        )
        .unwrap();
        assert_eq!(non_compact.literal_type, 9);
        assert_eq!(non_compact.decimal_value, None);
        assert_eq!(
            non_compact.decimal_bytes,
            Some(12_345i32.to_be_bytes()[2..].to_vec())
        );
    }

    #[test]
    fn rescales_decimal_to_the_column_scale() {
        // 123.4 declared with scale 1, encoded for a scale-3 column.
        let rescaled = encode_literal(
            DataType::Decimal(DecimalType::new(10, 3).unwrap()),
            col("c").eq(Decimal::from_unscaled_long(1_234, 10, 1).unwrap()),
        )
        .unwrap();
        assert_eq!(rescaled.decimal_value, Some(123_400));
    }

    #[test]
    fn rejects_decimal_that_would_be_rounded() {
        // 1.234 cannot be a scale-1 literal without moving the comparison bound.
        let error = encode_literal(
            DataType::Decimal(DecimalType::new(10, 1).unwrap()),
            col("c").eq(Decimal::from_unscaled_long(1_234, 10, 3).unwrap()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not fit column 'c'"));
    }

    #[test]
    fn encodes_null_literal_in_a_list() {
        let pb = to_pb_predicate(
            &col("c").is_in(vec![Some(1i32), None]),
            &row_type_of(DataType::Int(IntType::new())),
        )
        .unwrap();

        let literals = pb.leaf.unwrap().literals;
        assert!(!literals[0].is_null);
        assert_eq!(literals[0].int_value, Some(1));
        assert!(literals[1].is_null);
        assert_eq!(literals[1].literal_type, 3);
        assert_eq!(literals[1].int_value, None);
    }

    #[test]
    fn encodes_null_checks_without_literals() {
        let pb = to_pb_predicate(
            &col("c").is_null(),
            &row_type_of(DataType::Int(IntType::new())),
        )
        .unwrap();

        let leaf = pb.leaf.unwrap();
        assert_eq!(leaf.function, leaf_function_code(LeafFunction::IsNull));
        assert!(leaf.literals.is_empty());
    }

    #[test]
    fn rejects_unknown_column() {
        let error = to_pb_predicate(
            &col("missing").eq(1i32),
            &row_type_of(DataType::Int(IntType::new())),
        )
        .unwrap_err();

        assert!(error.to_string().contains("filter column 'missing'"));
    }

    #[test]
    fn rejects_column_without_field_id() {
        let row_type = RowType::new(vec![DataField::new(
            "c",
            DataType::Int(IntType::new()),
            None,
        )]);

        let error = to_pb_predicate(&col("c").eq(1i32), &row_type).unwrap_err();

        assert!(error.to_string().contains("no schema field id"));
    }

    #[test]
    fn rejects_literal_of_the_wrong_type() {
        let error = encode_literal(DataType::Int(IntType::new()), col("c").eq("hi")).unwrap_err();
        assert!(error.to_string().contains("does not match column 'c'"));

        let error =
            encode_literal(DataType::String(StringType::new()), col("c").eq(1i32)).unwrap_err();
        assert!(error.to_string().contains("does not match column 'c'"));
    }

    #[test]
    fn rejects_integer_literal_out_of_the_column_range() {
        let error =
            encode_literal(DataType::TinyInt(TinyIntType::new()), col("c").eq(200i32)).unwrap_err();
        assert!(error.to_string().contains("out of range"));

        // The column's minimum is still in range.
        let min = literal_of(DataType::TinyInt(TinyIntType::new()), i8::MIN);
        assert_eq!(min.int_value, Some(i8::MIN as i32));
    }

    #[test]
    fn coerces_between_numeric_widths_when_exact() {
        assert_eq!(
            literal_of(DataType::Double(DoubleType::new()), 2i32).double_value,
            Some(2.0)
        );
        assert_eq!(
            literal_of(DataType::Float(FloatType::new()), 1.5f64).float_value,
            Some(1.5)
        );

        // 0.1 is not representable as an f32, so the bound would shift.
        let error =
            encode_literal(DataType::Float(FloatType::new()), col("c").eq(0.1f64)).unwrap_err();
        assert!(error.to_string().contains("cannot be represented exactly"));

        // Neither is an integer above 2^53, for either float width.
        let too_precise = (1i64 << 53) + 1;
        for data_type in [
            DataType::Double(DoubleType::new()),
            DataType::Float(FloatType::new()),
        ] {
            let error = encode_literal(data_type, col("c").eq(too_precise)).unwrap_err();
            assert!(error.to_string().contains("cannot be represented exactly"));
        }
    }

    #[test]
    fn rejects_wrong_literal_count() {
        let predicate = Predicate::Leaf {
            field: "c".to_string(),
            function: LeafFunction::IsNull,
            literals: vec![Literal::Int32(1)],
        };

        let error =
            to_pb_predicate(&predicate, &row_type_of(DataType::Int(IntType::new()))).unwrap_err();

        assert!(error.to_string().contains("expects 0 literal(s), got 1"));
    }

    #[test]
    fn rejects_null_literal_for_comparisons() {
        let error =
            encode_literal(DataType::Int(IntType::new()), col("c").eq(None::<i32>)).unwrap_err();

        assert!(error.to_string().contains("cannot take a null literal"));
    }

    #[test]
    fn rejects_string_matching_on_a_non_string_column() {
        let error =
            encode_literal(DataType::Int(IntType::new()), col("c").starts_with("a")).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("requires a character string column")
        );
    }

    #[test]
    fn rejects_unsupported_column_type() {
        let error = encode_literal(
            DataType::Array(crate::metadata::ArrayType::new(DataType::Int(
                IntType::new(),
            ))),
            col("c").eq(1i32),
        )
        .unwrap_err();

        assert!(error.to_string().contains("is not supported"));
    }
}
