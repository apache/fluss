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

use std::str::FromStr;

use fluss::metadata::{Column, DataType};
use fluss::row::{Date, Decimal, GenericRow, InternalRow, Time, TimestampLtz, TimestampNtz};
use rustler::types::binary::NewBinary;
use rustler::{Encoder, Env, Term};

use crate::atoms;

/// Convert column names to BEAM atoms for use as map keys.
///
/// Note: BEAM atoms are never garbage-collected. This is safe because column
/// names come from server-defined table schemas (bounded set), not arbitrary
/// user input. The BEAM deduplicates atoms, so repeated calls with the same
/// column names do not grow the atom table.
pub fn intern_column_atoms<'a>(env: Env<'a>, columns: &[Column]) -> Vec<rustler::Atom> {
    columns
        .iter()
        .map(|col| rustler::Atom::from_str(env, col.name()).expect("valid atom"))
        .collect()
}

pub fn row_to_term<'a>(
    env: Env<'a>,
    row: &dyn InternalRow,
    columns: &[Column],
    column_atoms: &[rustler::Atom],
) -> Result<Term<'a>, String> {
    let pairs: Vec<(Term<'a>, Term<'a>)> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            let key = column_atoms[i].encode(env);
            let value = field_to_term(env, row, i, col.data_type())?;
            Ok((key, value))
        })
        .collect::<Result<_, String>>()?;
    Term::map_from_pairs(env, &pairs).map_err(|_| "failed to create map".to_string())
}

fn field_to_term<'a>(
    env: Env<'a>,
    row: &dyn InternalRow,
    pos: usize,
    data_type: &DataType,
) -> Result<Term<'a>, String> {
    if row.is_null_at(pos).map_err(|e| e.to_string())? {
        return Ok(atoms::nil().encode(env));
    }

    match data_type {
        DataType::Boolean(_) => {
            let v = row.get_boolean(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::TinyInt(_) => {
            let v = row.get_byte(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::SmallInt(_) => {
            let v = row.get_short(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Int(_) => {
            let v = row.get_int(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::BigInt(_) => {
            let v = row.get_long(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Float(_) => {
            let v = row.get_float(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Double(_) => {
            let v = row.get_double(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::String(_) => {
            let v = row.get_string(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Char(ct) => {
            let v = row
                .get_char(pos, ct.length() as usize)
                .map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Bytes(_) => {
            let v = row.get_bytes(pos).map_err(|e| e.to_string())?;
            let mut bin = NewBinary::new(env, v.len());
            bin.as_mut_slice().copy_from_slice(v);
            let binary: rustler::Binary = bin.into();
            Ok(binary.encode(env))
        }
        DataType::Binary(bt) => {
            let v = row
                .get_binary(pos, bt.length())
                .map_err(|e| e.to_string())?;
            let mut bin = NewBinary::new(env, v.len());
            bin.as_mut_slice().copy_from_slice(v);
            let binary: rustler::Binary = bin.into();
            Ok(binary.encode(env))
        }
        DataType::Date(_) => {
            let v = row.get_date(pos).map_err(|e| e.to_string())?;
            Ok(v.get_inner().encode(env))
        }
        DataType::Time(_) => {
            let v = row.get_time(pos).map_err(|e| e.to_string())?;
            Ok(v.get_inner().encode(env))
        }
        DataType::Timestamp(ts) => {
            let v = row
                .get_timestamp_ntz(pos, ts.precision())
                .map_err(|e| e.to_string())?;
            Ok((v.get_millisecond(), v.get_nano_of_millisecond()).encode(env))
        }
        DataType::TimestampLTz(ts) => {
            let v = row
                .get_timestamp_ltz(pos, ts.precision())
                .map_err(|e| e.to_string())?;
            Ok((v.get_epoch_millisecond(), v.get_nano_of_millisecond()).encode(env))
        }
        DataType::Decimal(dt) => {
            let v = row
                .get_decimal(pos, dt.precision() as usize, dt.scale() as usize)
                .map_err(|e| e.to_string())?;
            Ok(v.to_string().encode(env))
        }
        _ => Err(format!("unsupported data type: {data_type:?}")),
    }
}

pub fn term_to_row<'a>(
    env: Env<'a>,
    values: Term<'a>,
    columns: &[Column],
) -> Result<GenericRow<'static>, String> {
    let list: Vec<Term<'a>> = values
        .decode()
        .map_err(|_| "expected a list of values".to_string())?;
    if list.len() != columns.len() {
        return Err(format!(
            "expected {} values, got {}",
            columns.len(),
            list.len()
        ));
    }

    let mut row = GenericRow::new(columns.len());
    for (i, (term, col)) in list.iter().zip(columns.iter()).enumerate() {
        if term.is_atom()
            && let Ok(atom) = term.decode::<rustler::Atom>()
            && atom == atoms::nil()
        {
            continue; // leave as null
        }
        set_field_from_term(env, &mut row, i, *term, col.data_type())?;
    }
    Ok(row)
}

fn set_field_from_term<'a>(
    _env: Env<'a>,
    row: &mut GenericRow<'static>,
    pos: usize,
    term: Term<'a>,
    data_type: &DataType,
) -> Result<(), String> {
    match data_type {
        DataType::Boolean(_) => {
            let v: bool = term.decode().map_err(|_| "expected boolean")?;
            row.set_field(pos, v);
        }
        DataType::TinyInt(_) => {
            let v: i8 = term
                .decode()
                .map_err(|_| "expected integer in range -128..127 for tinyint")?;
            row.set_field(pos, v);
        }
        DataType::SmallInt(_) => {
            let v: i16 = term
                .decode()
                .map_err(|_| "expected integer in range -32768..32767 for smallint")?;
            row.set_field(pos, v);
        }
        DataType::Int(_) => {
            let v: i32 = term.decode().map_err(|_| "expected integer")?;
            row.set_field(pos, v);
        }
        DataType::BigInt(_) => {
            let v: i64 = term.decode().map_err(|_| "expected integer")?;
            row.set_field(pos, v);
        }
        DataType::Date(_) => {
            let v: i32 = term
                .decode()
                .map_err(|_| "expected integer (days since epoch)")?;
            row.set_field(pos, Date::new(v));
        }
        DataType::Time(_) => {
            let v: i32 = term
                .decode()
                .map_err(|_| "expected integer (millis since midnight)")?;
            row.set_field(pos, Time::new(v));
        }
        DataType::Timestamp(_) => {
            let (millis, nanos): (i64, i32) = term
                .decode()
                .map_err(|_| "expected {millis, nanos} tuple for timestamp")?;
            let ts = TimestampNtz::from_millis_nanos(millis, nanos).map_err(|e| e.to_string())?;
            row.set_field(pos, ts);
        }
        DataType::TimestampLTz(_) => {
            let (millis, nanos): (i64, i32) = term
                .decode()
                .map_err(|_| "expected {millis, nanos} tuple for timestamp_ltz")?;
            let ts = TimestampLtz::from_millis_nanos(millis, nanos).map_err(|e| e.to_string())?;
            row.set_field(pos, ts);
        }
        DataType::Float(_) => {
            let v: f64 = term.decode().map_err(|_| "expected number for float")?;
            row.set_field(pos, v as f32);
        }
        DataType::Double(_) => {
            let v: f64 = term.decode().map_err(|_| "expected number for double")?;
            row.set_field(pos, v);
        }
        DataType::String(_) | DataType::Char(_) => {
            let v: String = term.decode().map_err(|_| "expected string")?;
            row.set_field(pos, v);
        }
        DataType::Decimal(dt) => {
            let v: String = term.decode().map_err(|_| "expected string for decimal")?;
            let bd = bigdecimal::BigDecimal::from_str(&v)
                .map_err(|e| format!("failed to parse decimal '{v}': {e}"))?;
            let decimal = Decimal::from_big_decimal(bd, dt.precision(), dt.scale())
                .map_err(|e| e.to_string())?;
            row.set_field(pos, decimal);
        }
        DataType::Bytes(_) | DataType::Binary(_) => {
            let bin: rustler::Binary = term.decode().map_err(|_| "expected binary")?;
            row.set_field(pos, bin.as_slice().to_vec());
        }
        _ => return Err(format!("unsupported data type for writing: {data_type:?}")),
    }
    Ok(())
}
