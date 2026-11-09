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

//! Field validation and literal coercion shared by wire encoding and local binding.

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::{DataField, DataType, RowType};
use crate::predicate::{CompoundFunction, LeafFunction, Literal};
use crate::row::Decimal;

pub(super) fn resolve_field<'a>(
    row_type: &'a RowType,
    name: &str,
) -> Result<(usize, &'a DataField)> {
    row_type
        .get_field_index(name)
        .map(|index| (index, &row_type.fields()[index]))
        .ok_or_else(|| IllegalArgument {
            message: format!(
                "filter column '{}' does not exist in the table schema, available columns: {:?}",
                name,
                row_type.get_field_names()
            ),
        })
}

pub(super) fn validate_leaf(
    function: LeafFunction,
    field: &DataField,
    literals: &[Literal],
) -> Result<()> {
    validate_leaf_shape(function, field.name(), literals)?;

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

pub(super) fn validate_leaf_shape(
    function: LeafFunction,
    field_name: &str,
    literals: &[Literal],
) -> Result<()> {
    let expected = match function {
        LeafFunction::In | LeafFunction::NotIn => return Ok(()),
        LeafFunction::IsNull | LeafFunction::IsNotNull => 0,
        _ => 1,
    };
    if literals.len() != expected {
        return Err(IllegalArgument {
            message: format!(
                "{function:?} on column '{}' expects {expected} literal(s), got {}",
                field_name,
                literals.len()
            ),
        });
    }

    if literals.first() == Some(&Literal::Null) {
        return Err(IllegalArgument {
            message: format!(
                "{function:?} on column '{}' cannot take a null literal, use is_null()/is_not_null()",
                field_name
            ),
        });
    }

    Ok(())
}

pub(super) fn integer_value(field: &DataField, literal: &Literal) -> Result<i64> {
    match literal {
        Literal::Int8(value) => Ok(i64::from(*value)),
        Literal::Int16(value) => Ok(i64::from(*value)),
        Literal::Int32(value) => Ok(i64::from(*value)),
        Literal::Int64(value) => Ok(*value),
        _ => Err(mismatch(field, literal)),
    }
}

pub(super) fn integer_in_range(field: &DataField, literal: &Literal, max: i64) -> Result<i64> {
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

pub(super) fn float32(field: &DataField, literal: &Literal) -> Result<f32> {
    if let Literal::Float32(value) = literal {
        return Ok(*value);
    }
    let value = float64(field, literal)?;
    let narrowed = value as f32;
    if f64::from(narrowed) != value && !value.is_nan() {
        return Err(inexact(field, value));
    }
    Ok(narrowed)
}

pub(super) fn float64(field: &DataField, literal: &Literal) -> Result<f64> {
    match literal {
        Literal::Float64(value) => Ok(*value),
        Literal::Float32(value) => Ok(f64::from(*value)),
        Literal::Int8(_) | Literal::Int16(_) | Literal::Int32(_) | Literal::Int64(_) => {
            let value = integer_value(field, literal)?;
            let widened = value as f64;
            if widened as i128 != i128::from(value) {
                return Err(inexact(field, widened));
            }
            Ok(widened)
        }
        _ => Err(mismatch(field, literal)),
    }
}

fn inexact(field: &DataField, value: f64) -> crate::error::Error {
    IllegalArgument {
        message: format!(
            "filter literal {value} cannot be represented exactly by column '{}' of type {}",
            field.name(),
            field.data_type()
        ),
    }
}

pub(super) fn rescale(
    field: &DataField,
    value: &Decimal,
    precision: u32,
    scale: u32,
) -> Result<Decimal> {
    let big_decimal = value.to_big_decimal();
    let rescaled = Decimal::from_big_decimal(big_decimal.clone(), precision, scale)?;
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

pub(super) fn mismatch(field: &DataField, literal: &Literal) -> crate::error::Error {
    IllegalArgument {
        message: format!(
            "filter literal {literal:?} does not match column '{}' of type {}",
            field.name(),
            field.data_type()
        ),
    }
}

pub(super) fn unsupported_column(field: &DataField) -> crate::error::Error {
    IllegalArgument {
        message: format!(
            "filter on column '{}' of type {} is not supported",
            field.name(),
            field.data_type()
        ),
    }
}

pub(super) fn compound_name(function: CompoundFunction) -> &'static str {
    match function {
        CompoundFunction::And => "AND",
        CompoundFunction::Or => "OR",
    }
}
