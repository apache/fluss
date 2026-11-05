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

use fluss::metadata::{Column, DataType, MapType, RowType};
use fluss::row::{
    DataGetters, Date, Datum, Decimal, FlussArray, FlussArrayWriter, FlussMap, FlussMapWriter,
    GenericRow, InternalRow, Time, TimestampLtz, TimestampNtz,
};
use rustler::types::binary::NewBinary;
use rustler::{Encoder, Env, MapIterator, Term};

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
    getters: &dyn DataGetters,
    pos: usize,
    data_type: &DataType,
) -> Result<Term<'a>, String> {
    if getters.is_null_at(pos).map_err(|e| e.to_string())? {
        return Ok(atoms::nil().encode(env));
    }

    match data_type {
        DataType::Boolean(_) => {
            let v = getters.get_boolean(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::TinyInt(_) => {
            let v = getters.get_byte(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::SmallInt(_) => {
            let v = getters.get_short(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Int(_) => {
            let v = getters.get_int(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::BigInt(_) => {
            let v = getters.get_long(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Float(_) => {
            let v = getters.get_float(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Double(_) => {
            let v = getters.get_double(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::String(_) => {
            let v = getters.get_string(pos).map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Char(ct) => {
            let v = getters
                .get_char(pos, ct.length() as usize)
                .map_err(|e| e.to_string())?;
            Ok(v.encode(env))
        }
        DataType::Bytes(_) => {
            let v = getters.get_bytes(pos).map_err(|e| e.to_string())?;
            let mut bin = NewBinary::new(env, v.len());
            bin.as_mut_slice().copy_from_slice(v);
            let binary: rustler::Binary = bin.into();
            Ok(binary.encode(env))
        }
        DataType::Binary(bt) => {
            let v = getters
                .get_binary(pos, bt.length())
                .map_err(|e| e.to_string())?;
            let mut bin = NewBinary::new(env, v.len());
            bin.as_mut_slice().copy_from_slice(v);
            let binary: rustler::Binary = bin.into();
            Ok(binary.encode(env))
        }
        DataType::Date(_) => {
            let v = getters.get_date(pos).map_err(|e| e.to_string())?;
            Ok(v.get_inner().encode(env))
        }
        DataType::Time(_) => {
            let v = getters.get_time(pos).map_err(|e| e.to_string())?;
            Ok(v.get_inner().encode(env))
        }
        DataType::Timestamp(ts) => {
            let v = getters
                .get_timestamp_ntz(pos, ts.precision())
                .map_err(|e| e.to_string())?;
            Ok((v.get_millisecond(), v.get_nano_of_millisecond()).encode(env))
        }
        DataType::TimestampLTz(ts) => {
            let v = getters
                .get_timestamp_ltz(pos, ts.precision())
                .map_err(|e| e.to_string())?;
            Ok((v.get_epoch_millisecond(), v.get_nano_of_millisecond()).encode(env))
        }
        DataType::Decimal(dt) => {
            let v = getters
                .get_decimal(pos, dt.precision() as usize, dt.scale() as usize)
                .map_err(|e| e.to_string())?;
            Ok(v.to_string().encode(env))
        }
        DataType::Array(dt) => {
            let arr = getters
                .get_array(pos)
                .map_err(|e| e.to_string())?
                .try_into_binary()
                .map_err(|e| e.to_string())?;
            array_to_term(env, &arr, dt.get_element_type())
        }
        DataType::Map(dt) => {
            let map = getters
                .get_map(pos)
                .map_err(|e| e.to_string())?
                .try_into_binary()
                .map_err(|e| e.to_string())?; // → FlussMap
            map_to_term(env, &map, dt)
        }
        DataType::Row(dt) => {
            let nested = getters
                .get_row(pos)
                .map_err(|e| e.to_string())?
                .try_into_generic(dt)
                .map_err(|e| e.to_string())?; // → GenericRow
            row_fields_to_term(env, &nested, dt)
        }
    }
}

pub fn array_to_term<'a>(
    env: Env<'a>,
    arr: &FlussArray,
    element_type: &DataType,
) -> Result<Term<'a>, String> {
    let items = (0..arr.size())
        .map(|i| array_elem_to_term(env, arr, i, element_type))
        .collect::<Result<Vec<Term<'a>>, String>>()?;
    Ok(items.encode(env))
}

fn array_elem_to_term<'a>(
    env: Env<'a>,
    arr: &FlussArray,
    i: usize,
    element_type: &DataType,
) -> Result<Term<'a>, String> {
    if arr.is_null_at(i) {
        return Ok(atoms::nil().encode(env));
    }
    match element_type {
        DataType::Array(at) => {
            let nested = arr.get_array(i).map_err(|e| e.to_string())?;
            array_to_term(env, &nested, at.get_element_type())
        }
        DataType::Map(mt) => {
            let nested = arr
                .get_map(i, mt.key_type(), mt.value_type())
                .map_err(|e| e.to_string())?;
            map_to_term(env, &nested, mt)
        }
        DataType::Row(rt) => {
            let nested = arr.get_row(i, rt).map_err(|e| e.to_string())?;
            row_fields_to_term(env, &nested, rt)
        }
        _ => field_to_term(env, arr, i, element_type),
    }
}

pub fn map_to_term<'a>(env: Env<'a>, map: &FlussMap, mt: &MapType) -> Result<Term<'a>, String> {
    let keys = map.key_array();
    let values = map.value_array();

    let pairs = (0..map.size())
        .map(|i| {
            let k = array_elem_to_term(env, keys, i, mt.key_type())?;
            let v = array_elem_to_term(env, values, i, mt.value_type())?;
            Ok((k, v))
        })
        .collect::<Result<Vec<(Term<'a>, Term<'a>)>, String>>()?;

    Term::map_from_pairs(env, &pairs).map_err(|_| "failed to build map".to_string())
}

pub fn row_fields_to_term<'a>(
    env: Env<'a>,
    row: &dyn DataGetters,
    rt: &RowType,
) -> Result<Term<'a>, String> {
    let pairs: Vec<(Term<'a>, Term<'a>)> = rt
        .fields()
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let key = rustler::Atom::from_str(env, field.name())
                .map_err(|_| "invalid field name".to_string())?
                .encode(env);
            let value = field_to_term(env, row, i, field.data_type())?;
            Ok((key, value))
        })
        .collect::<Result<_, String>>()?;
    Term::map_from_pairs(env, &pairs).map_err(|_| "failed to build row map".to_string())
}
pub fn term_to_row<'a>(
    env: Env<'a>,
    values: Term<'a>,
    columns: &[Column],
) -> Result<GenericRow<'static>, String> {
    let list = decode_list(values, "expected a list of values")?;
    if list.len() != columns.len() {
        return Err(format!(
            "expected {} values, got {}",
            columns.len(),
            list.len()
        ));
    }

    let mut row = GenericRow::new(columns.len());
    for (i, (term, col)) in list.iter().zip(columns.iter()).enumerate() {
        if is_nil(*term) {
            continue; // leave as null
        }
        row.set_field(i, term_to_datum(env, *term, col.data_type())?);
    }
    check_not_null(&row, columns, 0..columns.len())?;
    Ok(row)
}

/// Build a full-width row from values for `indices` only, in that order; other fields stay
/// null. Full width because the key encoder addresses columns by table position.
pub fn term_to_sparse_row<'a>(
    env: Env<'a>,
    values: Term<'a>,
    columns: &[Column],
    indices: &[usize],
) -> Result<GenericRow<'static>, String> {
    let list = decode_list(values, "expected a list of values")?;
    if list.len() != indices.len() {
        return Err(format!(
            "expected {} values for the target columns, got {}",
            indices.len(),
            list.len()
        ));
    }

    let mut row = GenericRow::new(columns.len());
    for (term, idx) in list.iter().zip(indices.iter().copied()) {
        if is_nil(*term) {
            continue; // leave as null
        }
        row.set_field(idx, term_to_datum(env, *term, columns[idx].data_type())?);
    }
    check_not_null(&row, columns, indices.iter().copied())?;
    Ok(row)
}

/// Build a full-width row from a map keyed by column name. Keys must be among `indices`
/// (every column if `None`), and NOT NULL targets need a non-nil value.
pub fn map_to_row<'a>(
    env: Env<'a>,
    values: Term<'a>,
    columns: &[Column],
    indices: Option<&[usize]>,
) -> Result<GenericRow<'static>, String> {
    let entries: MapIterator<'a> = values
        .decode()
        .map_err(|_| "expected a map of column names to values".to_string())?;

    let all_indices: Vec<usize>;
    let targets: &[usize] = match indices {
        Some(indices) => indices,
        None => {
            all_indices = (0..columns.len()).collect();
            &all_indices
        }
    };

    let mut row = GenericRow::new(columns.len());
    // Duplicate keys only; nullness is checked on the row below.
    let mut seen = vec![false; columns.len()];

    for (key, value) in entries {
        let name = term_to_column_name(key)?;
        let idx = targets
            .iter()
            .copied()
            .find(|&idx| columns[idx].name() == name)
            .ok_or_else(|| {
                let problem = if columns.iter().any(|column| column.name() == name) {
                    "is not accepted here"
                } else {
                    "is unknown"
                };
                format!(
                    "column `{}` {}; expected one of: {}",
                    name,
                    problem,
                    target_column_names(columns, targets).join(", ")
                )
            })?;
        if std::mem::replace(&mut seen[idx], true) {
            return Err(format!("column `{name}` given more than once"));
        }
        row.set_field(idx, term_to_datum(env, value, columns[idx].data_type())?);
    }

    // Catches both an omitted key and an explicit nil: the row starts all-null.
    check_not_null(&row, columns, targets.iter().copied())?;
    Ok(row)
}

/// Core would reject the null later, with a type mismatch that doesn't name the column.
fn check_not_null(
    row: &GenericRow<'_>,
    columns: &[Column],
    indices: impl IntoIterator<Item = usize>,
) -> Result<(), String> {
    for idx in indices {
        if row.values[idx].is_null() && !columns[idx].data_type().is_nullable() {
            return Err(format!(
                "NOT NULL column `{}` must not be nil or omitted",
                columns[idx].name()
            ));
        }
    }
    Ok(())
}

/// Rustler's own list decode panics on an improper list such as `[1 | 2]`.
pub fn decode_list<'a>(term: Term<'a>, error: &str) -> Result<Vec<Term<'a>>, String> {
    term.list_length().map_err(|_| error.to_string())?;
    term.decode().map_err(|_| error.to_string())
}

fn is_nil(term: Term<'_>) -> bool {
    term.is_atom()
        && term
            .decode::<rustler::Atom>()
            .is_ok_and(|atom| atom == atoms::nil())
}

/// Decode a column name given as either an atom or a string.
pub fn term_to_column_name(term: Term<'_>) -> Result<String, String> {
    if term.is_atom() {
        term.atom_to_string()
            .map_err(|_| "invalid atom column name".to_string())
    } else {
        term.decode::<String>()
            .map_err(|_| "column names must be atoms or strings".to_string())
    }
}

/// Core accepts a repeated column; `[0, 0]` would then expect two values for one column.
/// Indices must be in range.
pub fn check_distinct_columns(columns: &[Column], indices: &[usize]) -> Result<(), String> {
    let mut seen = vec![false; columns.len()];
    for &idx in indices {
        if std::mem::replace(&mut seen[idx], true) {
            return Err(format!("duplicate target column `{}`", columns[idx].name()));
        }
    }
    Ok(())
}

fn target_column_names(columns: &[Column], targets: &[usize]) -> Vec<String> {
    targets
        .iter()
        .map(|&idx| columns[idx].name().to_string())
        .collect()
}

fn term_to_datum<'a>(
    env: Env<'a>,
    term: Term<'a>,
    data_type: &DataType,
) -> Result<Datum<'static>, String> {
    // A nil term maps to Null regardless of the declared type. This is what lets
    // the Array/Map/Row arms recurse without special-casing null elements.
    if is_nil(term) {
        return Ok(Datum::Null);
    }

    match data_type {
        DataType::Boolean(_) => {
            let v: bool = term.decode().map_err(|_| "expected boolean")?;
            Ok(v.into())
        }
        DataType::TinyInt(_) => {
            let v: i8 = term
                .decode()
                .map_err(|_| "expected integer in range -128..127 for tinyint")?;
            Ok(v.into())
        }
        DataType::SmallInt(_) => {
            let v: i16 = term
                .decode()
                .map_err(|_| "expected integer in range -32768..32767 for smallint")?;
            Ok(v.into())
        }
        DataType::Int(_) => {
            let v: i32 = term.decode().map_err(|_| "expected integer")?;
            Ok(v.into())
        }
        DataType::BigInt(_) => {
            let v: i64 = term.decode().map_err(|_| "expected integer")?;
            Ok(v.into())
        }
        DataType::Date(_) => {
            let v: i32 = term
                .decode()
                .map_err(|_| "expected integer (days since epoch)")?;
            Ok(Date::new(v).into())
        }
        DataType::Time(_) => {
            let v: i32 = term
                .decode()
                .map_err(|_| "expected integer (millis since midnight)")?;
            Ok(Time::new(v).into())
        }
        DataType::Timestamp(_) => {
            let (millis, nanos): (i64, i32) = term
                .decode()
                .map_err(|_| "expected {millis, nanos} tuple for timestamp")?;
            let ts = TimestampNtz::from_millis_nanos(millis, nanos).map_err(|e| e.to_string())?;
            Ok(ts.into())
        }
        DataType::TimestampLTz(_) => {
            let (millis, nanos): (i64, i32) = term
                .decode()
                .map_err(|_| "expected {millis, nanos} tuple for timestamp_ltz")?;
            let ts = TimestampLtz::from_millis_nanos(millis, nanos).map_err(|e| e.to_string())?;
            Ok(ts.into())
        }
        DataType::Float(_) => {
            let v: f64 = term.decode().map_err(|_| "expected number for float")?;
            Ok((v as f32).into())
        }
        DataType::Double(_) => {
            let v: f64 = term.decode().map_err(|_| "expected number for double")?;
            Ok(v.into())
        }
        DataType::String(_) | DataType::Char(_) => {
            let v: String = term.decode().map_err(|_| "expected string")?;
            Ok(v.into())
        }
        DataType::Decimal(dt) => {
            let v: String = term.decode().map_err(|_| "expected string for decimal")?;
            let bd = bigdecimal::BigDecimal::from_str(&v)
                .map_err(|e| format!("failed to parse decimal '{v}': {e}"))?;
            let decimal = Decimal::from_big_decimal(bd, dt.precision(), dt.scale())
                .map_err(|e| e.to_string())?;
            Ok(decimal.into())
        }
        DataType::Bytes(_) | DataType::Binary(_) => {
            let bin: rustler::Binary = term.decode().map_err(|_| "expected binary")?;
            Ok(bin.as_slice().to_vec().into())
        }
        DataType::Array(at) => {
            let et = at.get_element_type();
            let list = decode_list(term, "expected a list for array")?;
            let mut writer = FlussArrayWriter::new(list.len(), et);
            for (i, elem) in list.iter().enumerate() {
                let datum = term_to_datum(env, *elem, et)?;
                write_datum_into_array(&mut writer, i, datum, et)?;
            }
            Ok(Datum::Array(writer.complete().map_err(|e| e.to_string())?))
        }
        DataType::Map(mt) => {
            let kt = mt.key_type();
            let vt = mt.value_type();
            let size = term
                .map_size()
                .map_err(|_| "expected a map for map column".to_string())?;
            let iter = MapIterator::new(term)
                .ok_or_else(|| "expected a map for map column".to_string())?;
            let mut writer = FlussMapWriter::new(size, kt, vt);
            for (k_term, v_term) in iter {
                let key = term_to_datum(env, k_term, kt)?;
                let value = term_to_datum(env, v_term, vt)?;
                writer.write_entry(key, value).map_err(|e| e.to_string())?;
            }
            Ok(Datum::Map(writer.complete().map_err(|e| e.to_string())?))
        }
        DataType::Row(rt) => {
            let fields = rt.fields();
            let mut nested = GenericRow::new(fields.len());
            for (i, field) in fields.iter().enumerate() {
                let key = rustler::Atom::from_str(env, field.name())
                    .map_err(|_| "invalid field name".to_string())?;
                let value_term = term
                    .map_get(key)
                    .map_err(|_| format!("row is missing field '{}'", field.name()))?;
                nested.set_field(i, term_to_datum(env, value_term, field.data_type())?);
            }
            Ok(Datum::Row(Box::new(nested)))
        }
    }
}

pub fn write_datum_into_array(
    writer: &mut FlussArrayWriter,
    i: usize,
    datum: Datum<'static>,
    element_type: &DataType,
) -> Result<(), String> {
    match datum {
        Datum::Null => writer.set_null_at(i),
        Datum::Bool(v) => writer.write_boolean(i, v),
        Datum::Int8(v) => writer.write_byte(i, v),
        Datum::Int16(v) => writer.write_short(i, v),
        Datum::Int32(v) => writer.write_int(i, v),
        Datum::Int64(v) => writer.write_long(i, v),
        Datum::Float32(v) => writer.write_float(i, v.into_inner()),
        Datum::Float64(v) => writer.write_double(i, v.into_inner()),
        Datum::String(v) => writer.write_string(i, &v),
        Datum::Blob(v) => writer.write_binary_bytes(i, v.as_ref()),
        Datum::Decimal(v) => {
            if let DataType::Decimal(dt) = element_type {
                writer.write_decimal(i, &v, dt.precision());
            }
        }
        Datum::Date(v) => writer.write_date(i, v),
        Datum::Time(v) => writer.write_time(i, v),
        Datum::TimestampNtz(v) => {
            if let DataType::Timestamp(dt) = element_type {
                writer.write_timestamp_ntz(i, &v, dt.precision());
            }
        }
        Datum::TimestampLtz(v) => {
            if let DataType::TimestampLTz(dt) = element_type {
                writer.write_timestamp_ltz(i, &v, dt.precision());
            }
        }
        Datum::Array(v) => writer.write_array(i, &v),
        Datum::Map(v) => writer.write_map(i, &v),
        Datum::Row(v) => writer.write_row(i, v.as_ref()).map_err(|e| e.to_string())?,
    }
    Ok(())
}
