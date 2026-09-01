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

//! Predicates for log-scan filter pushdown, mirroring `fluss::predicate`.

use crate::error::FlussError;
use fcore::predicate::Literal;
use fluss as fcore;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::pyclass::CompareOp;
use pyo3::types::{
    PyBool, PyBytes, PyDate, PyDateTime, PyDelta, PyDeltaAccess, PyInt, PyTime, PyTimeAccess,
    PyTzInfoAccess,
};

/// Python's ordinal for 1970-01-01, the DATE epoch.
const UNIX_EPOCH_ORDINAL: i64 = 719_163;

/// A filter expression for a log scan, combined with `&` and `|`.
#[pyclass(module = "fluss", from_py_object)]
#[derive(Clone)]
pub struct Predicate {
    inner: fcore::predicate::Predicate,
}

impl Predicate {
    pub(crate) fn to_core(&self) -> fcore::predicate::Predicate {
        self.inner.clone()
    }
}

#[pymethods]
impl Predicate {
    /// Match rows satisfying both predicates.
    fn and_(&self, other: &Predicate) -> Predicate {
        Predicate {
            inner: self.inner.clone().and(other.inner.clone()),
        }
    }

    /// Match rows satisfying either predicate.
    fn or_(&self, other: &Predicate) -> Predicate {
        Predicate {
            inner: self.inner.clone().or(other.inner.clone()),
        }
    }

    fn __and__(&self, other: &Predicate) -> Predicate {
        self.and_(other)
    }

    fn __or__(&self, other: &Predicate) -> Predicate {
        self.or_(other)
    }

    /// `and`/`or` would silently return one side, so refuse a truth value.
    fn __bool__(&self) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "A Predicate has no truth value; combine them with & and |, not and/or",
        ))
    }

    fn __repr__(&self) -> String {
        format!("Predicate({:?})", self.inner)
    }
}

/// A column reference, where `col("id") >= 200` and
/// `col("id").greater_or_equal(200)` build the same [`Predicate`].
#[pyclass(module = "fluss")]
pub struct ColumnRef {
    name: String,
}

#[pymethods]
impl ColumnRef {
    #[new]
    fn new(name: String) -> Self {
        ColumnRef { name }
    }

    /// The column name.
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    fn __richcmp__(&self, other: &Bound<'_, PyAny>, op: CompareOp) -> PyResult<Predicate> {
        let literal = literal_from_py(other)?;
        let column = self.column();
        let inner = match op {
            CompareOp::Eq => column.eq(literal),
            CompareOp::Ne => column.ne(literal),
            CompareOp::Lt => column.lt(literal),
            CompareOp::Le => column.le(literal),
            CompareOp::Gt => column.gt(literal),
            CompareOp::Ge => column.ge(literal),
        };
        Ok(Predicate { inner })
    }

    /// Match rows where the column equals `value`.
    fn equal(&self, value: &Bound<'_, PyAny>) -> PyResult<Predicate> {
        Ok(Predicate {
            inner: self.column().eq(literal_from_py(value)?),
        })
    }

    /// Match rows where the column differs from `value`.
    fn not_equal(&self, value: &Bound<'_, PyAny>) -> PyResult<Predicate> {
        Ok(Predicate {
            inner: self.column().ne(literal_from_py(value)?),
        })
    }

    /// Match rows where the column is less than `value`.
    fn less_than(&self, value: &Bound<'_, PyAny>) -> PyResult<Predicate> {
        Ok(Predicate {
            inner: self.column().lt(literal_from_py(value)?),
        })
    }

    /// Match rows where the column is less than or equal to `value`.
    fn less_or_equal(&self, value: &Bound<'_, PyAny>) -> PyResult<Predicate> {
        Ok(Predicate {
            inner: self.column().le(literal_from_py(value)?),
        })
    }

    /// Match rows where the column is greater than `value`.
    fn greater_than(&self, value: &Bound<'_, PyAny>) -> PyResult<Predicate> {
        Ok(Predicate {
            inner: self.column().gt(literal_from_py(value)?),
        })
    }

    /// Match rows where the column is greater than or equal to `value`.
    fn greater_or_equal(&self, value: &Bound<'_, PyAny>) -> PyResult<Predicate> {
        Ok(Predicate {
            inner: self.column().ge(literal_from_py(value)?),
        })
    }

    /// Match rows where the column is null.
    fn is_null(&self) -> Predicate {
        Predicate {
            inner: self.column().is_null(),
        }
    }

    /// Match rows where the column is not null.
    fn is_not_null(&self) -> Predicate {
        Predicate {
            inner: self.column().is_not_null(),
        }
    }

    /// Match rows where the string column starts with `prefix`.
    fn starts_with(&self, prefix: String) -> Predicate {
        Predicate {
            inner: self.column().starts_with(prefix),
        }
    }

    /// Match rows where the string column ends with `suffix`.
    fn ends_with(&self, suffix: String) -> Predicate {
        Predicate {
            inner: self.column().ends_with(suffix),
        }
    }

    /// Match rows where the string column contains `infix`.
    fn contains(&self, infix: String) -> Predicate {
        Predicate {
            inner: self.column().contains(infix),
        }
    }

    /// Match rows where the column equals any of `values`.
    fn is_in(&self, values: Vec<Bound<'_, PyAny>>) -> PyResult<Predicate> {
        Ok(Predicate {
            inner: self.column().is_in(literals_from_py(&values)?),
        })
    }

    /// Match rows where the column equals none of `values`.
    fn not_in(&self, values: Vec<Bound<'_, PyAny>>) -> PyResult<Predicate> {
        Ok(Predicate {
            inner: self.column().not_in(literals_from_py(&values)?),
        })
    }

    fn __bool__(&self) -> PyResult<bool> {
        Err(PyTypeError::new_err(
            "A ColumnRef has no truth value; compare it first, then combine with & and |",
        ))
    }

    fn __repr__(&self) -> String {
        format!("ColumnRef({})", self.name)
    }
}

impl ColumnRef {
    fn column(&self) -> fcore::predicate::ColumnRef {
        fcore::predicate::col(self.name.clone())
    }
}

/// Reference a column by name when building a filter.
#[pyfunction]
pub fn col(name: String) -> ColumnRef {
    ColumnRef::new(name)
}

fn literals_from_py(values: &[Bound<'_, PyAny>]) -> PyResult<Vec<Literal>> {
    values.iter().map(literal_from_py).collect()
}

/// `bool` before `int` and `datetime` before `date`: each is a subclass of the other.
fn literal_from_py(value: &Bound<'_, PyAny>) -> PyResult<Literal> {
    if value.is_none() {
        return Ok(Literal::Null);
    }
    if let Ok(flag) = value.cast_exact::<PyBool>() {
        return Ok(Literal::Bool(flag.is_true()));
    }
    if let Ok(datetime) = value.cast::<PyDateTime>() {
        return timestamp_literal(datetime);
    }
    if let Ok(date) = value.cast::<PyDate>() {
        return date_literal(date);
    }
    if let Ok(time) = value.cast::<PyTime>() {
        return Ok(Literal::Time(time_millis_of_day(time)));
    }
    let decimal_type = value.py().import("decimal")?.getattr("Decimal")?;
    if value.is_instance(&decimal_type)? {
        return decimal_literal(value);
    }
    match value.extract::<i64>() {
        Ok(integer) => return Ok(Literal::Int64(integer)),
        // Falling through would turn an out-of-range int into a float literal.
        Err(err) if value.is_instance_of::<PyInt>() => return Err(err),
        Err(_) => {}
    }
    if let Ok(floating) = value.extract::<f64>() {
        return Ok(Literal::Float64(floating));
    }
    if let Ok(text) = value.extract::<String>() {
        return Ok(Literal::String(text));
    }
    if let Ok(bytes) = value.cast::<PyBytes>() {
        return Ok(Literal::Bytes(bytes.as_bytes().to_vec()));
    }

    Err(PyTypeError::new_err(format!(
        "Unsupported filter literal of type '{}'. Supported: bool, int, float, \
         str, bytes, decimal.Decimal, datetime.date, datetime.time, \
         datetime.datetime; use is_null()/is_not_null() for nulls",
        value
            .get_type()
            .name()
            .map(|name| name.to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    )))
}

/// Kept at the caller's scale; the scan rescales it and rejects an inexact one.
fn decimal_literal(value: &Bound<'_, PyAny>) -> PyResult<Literal> {
    let text = value.str()?.to_string();
    let big_decimal: bigdecimal::BigDecimal = text
        .parse()
        .map_err(|e| FlussError::new_err(format!("Invalid decimal literal '{text}': {e}")))?;
    let scale = i64::max(big_decimal.fractional_digit_count(), 0) as u32;
    let digits = big_decimal.digits() as u32;
    let precision = u32::max(digits, scale).max(1);
    let decimal = fcore::row::Decimal::from_big_decimal(big_decimal, precision, scale)
        .map_err(|e| FlussError::from_core_error(&e))?;
    Ok(Literal::Decimal(decimal))
}

/// Naive datetimes filter TIMESTAMP, aware ones TIMESTAMP_LTZ.
///
/// Subtracting a matching epoch, not `datetime.timestamp()`, which reads a
/// naive value as local time and would shift the literal by the UTC offset.
fn timestamp_literal(value: &Bound<'_, PyDateTime>) -> PyResult<Literal> {
    let datetime_module = value.py().import("datetime")?;
    let datetime_type = datetime_module.getattr("datetime")?;
    let aware = value.get_tzinfo().is_some();
    let epoch = if aware {
        let utc = datetime_module.getattr("timezone")?.getattr("utc")?;
        datetime_type.call1((1970, 1, 1, 0, 0, 0, 0, utc))?
    } else {
        datetime_type.call1((1970, 1, 1))?
    };

    let delta = value.sub(epoch)?;
    let delta = delta.cast::<PyDelta>()?;
    // A timedelta carries the sign in `days`, so this holds before 1970 too.
    let micros = delta.get_days() as i64 * 86_400_000_000
        + delta.get_seconds() as i64 * 1_000_000
        + delta.get_microseconds() as i64;
    let millis = micros.div_euclid(1_000);
    let nanos = (micros.rem_euclid(1_000) * 1_000) as i32;

    if aware {
        let timestamp = fcore::row::TimestampLtz::from_millis_nanos(millis, nanos)
            .map_err(|e| FlussError::from_core_error(&e))?;
        Ok(Literal::TimestampLtz(timestamp))
    } else {
        let timestamp = fcore::row::TimestampNtz::from_millis_nanos(millis, nanos)
            .map_err(|e| FlussError::from_core_error(&e))?;
        Ok(Literal::TimestampNtz(timestamp))
    }
}

/// Fluss stores DATE as days since the Unix epoch.
fn date_literal(value: &Bound<'_, PyDate>) -> PyResult<Literal> {
    let ordinal: i64 = value.call_method0("toordinal")?.extract()?;
    let days = i32::try_from(ordinal - UNIX_EPOCH_ORDINAL).map_err(|_| {
        FlussError::new_err(format!(
            "Date literal {value} is out of range for a DATE column"
        ))
    })?;
    Ok(Literal::Date(days))
}

/// Fluss stores TIME as milliseconds of day.
fn time_millis_of_day(value: &Bound<'_, PyTime>) -> i32 {
    let hours = value.get_hour() as i32;
    let minutes = value.get_minute() as i32;
    let seconds = value.get_second() as i32;
    let micros = value.get_microsecond() as i32;
    ((hours * 3_600 + minutes * 60 + seconds) * 1_000) + micros / 1_000
}
