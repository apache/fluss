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

/// Utils for partition.
use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::DataType;
use crate::row::{Date, Datum, Time, TimestampLtz, TimestampNtz};
use jiff::ToSpan;
use std::fmt::Write;

fn hex_string(bytes: &[u8]) -> String {
    let mut hex = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        write!(hex, "{b:02x}").unwrap();
    }
    hex
}

fn reformat_float(value: f32) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        value.to_string().replace('.', "_")
    }
}

fn reformat_double(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value > 0.0 {
            "Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        value.to_string().replace('.', "_")
    }
}

const UNIX_EPOCH_DATE: jiff::civil::Date = jiff::civil::date(1970, 1, 1);

fn day_to_string(days: i32) -> String {
    let date = UNIX_EPOCH_DATE + days.days();
    format!("{:04}-{:02}-{:02}", date.year(), date.month(), date.day())
}

fn date_to_string(date: Date) -> String {
    day_to_string(date.get_inner())
}

const MILLIS_PER_SECOND: i64 = 1_000;
const MILLIS_PER_MINUTE: i64 = 60 * MILLIS_PER_SECOND;
const MILLIS_PER_HOUR: i64 = 60 * MILLIS_PER_MINUTE;

fn milli_to_string(milli: i32) -> String {
    let hour = milli.div_euclid(MILLIS_PER_HOUR as i32);
    let min = milli
        .rem_euclid(MILLIS_PER_HOUR as i32)
        .div_euclid(MILLIS_PER_MINUTE as i32);
    let sec = milli
        .rem_euclid(MILLIS_PER_MINUTE as i32)
        .div_euclid(MILLIS_PER_SECOND as i32);
    let ms = milli.rem_euclid(MILLIS_PER_SECOND as i32);

    format!("{hour:02}-{min:02}-{sec:02}_{ms:03}")
}

fn time_to_string(time: Time) -> String {
    milli_to_string(time.get_inner())
}

trait Timestamp {
    fn get_milli(&self) -> i64;
    fn get_nano_of_milli(&self) -> i32;
}

impl Timestamp for TimestampNtz {
    fn get_milli(&self) -> i64 {
        self.get_millisecond()
    }

    fn get_nano_of_milli(&self) -> i32 {
        self.get_nano_of_millisecond()
    }
}

impl Timestamp for TimestampLtz {
    fn get_milli(&self) -> i64 {
        self.get_epoch_millisecond()
    }

    fn get_nano_of_milli(&self) -> i32 {
        self.get_nano_of_millisecond()
    }
}

/// This formats date time while adhering to java side behaviour
///
fn timestamp_to_string<T: Timestamp>(ts: T) -> String {
    let millis = ts.get_milli();
    let nanos = ts.get_nano_of_milli();

    let millis_of_second = millis.rem_euclid(MILLIS_PER_SECOND);
    let total_secs = millis.div_euclid(MILLIS_PER_SECOND);

    let epoch = jiff::Timestamp::UNIX_EPOCH;
    let ts_jiff = epoch + jiff::Span::new().seconds(total_secs);
    let dt = ts_jiff.to_zoned(jiff::tz::TimeZone::UTC).datetime();

    if nanos > 0 {
        format!(
            "{:04}-{:02}-{:02}-{:02}-{:02}-{:02}_{:03}{:06}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            millis_of_second,
            nanos
        )
    } else if millis_of_second > 0 {
        format!(
            "{:04}-{:02}-{:02}-{:02}-{:02}-{:02}_{:03}",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            millis_of_second
        )
    } else {
        format!(
            "{:04}-{:02}-{:02}-{:02}-{:02}-{:02}_",
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
        )
    }
}

/// Converts a Datum value to its string representation for partition naming.
pub fn convert_value_of_type(value: &Datum, data_type: &DataType) -> Result<String> {
    match (value, data_type) {
        (Datum::String(s), DataType::Char(_) | DataType::String(_)) => Ok(s.to_string()),
        (Datum::Bool(b), DataType::Boolean(_)) => Ok(b.to_string()),
        (Datum::Blob(bytes), DataType::Binary(_) | DataType::Bytes(_)) => Ok(hex_string(bytes)),
        (Datum::Int8(v), DataType::TinyInt(_)) => Ok(v.to_string()),
        (Datum::Int16(v), DataType::SmallInt(_)) => Ok(v.to_string()),
        (Datum::Int32(v), DataType::Int(_)) => Ok(v.to_string()),
        (Datum::Int64(v), DataType::BigInt(_)) => Ok(v.to_string()),
        (Datum::Date(d), DataType::Date(_)) => Ok(date_to_string(*d)),
        (Datum::Time(t), DataType::Time(_)) => Ok(time_to_string(*t)),
        (Datum::Float32(f), DataType::Float(_)) => Ok(reformat_float(f.into_inner())),
        (Datum::Float64(f), DataType::Double(_)) => Ok(reformat_double(f.into_inner())),
        (Datum::TimestampLtz(ts), DataType::TimestampLTz(_)) => Ok(timestamp_to_string(*ts)),
        (Datum::TimestampNtz(ts), DataType::Timestamp(_)) => Ok(timestamp_to_string(*ts)),
        _ => Err(IllegalArgument {
            message: format!(
                "Unsupported conversion to partition key from data type: {data_type:?}, value: {value:?}"
            ),
        }),
    }
}

/// Parses a non-null partition value using the format of [`convert_value_of_type`].
/// Temporal values retain their full encoded precision; this does not apply
/// column-precision rounding. Invalid encodings and unsupported types are errors.
pub(crate) fn parse_value_of_type<'a>(value: &'a str, data_type: &DataType) -> Result<Datum<'a>> {
    let parsed = (|| -> Option<Datum<'a>> {
        Some(match data_type {
            DataType::Char(_) | DataType::String(_) => Datum::String(value.into()),
            DataType::Boolean(_) if value.eq_ignore_ascii_case("true") => Datum::Bool(true),
            DataType::Boolean(_) if value.eq_ignore_ascii_case("false") => Datum::Bool(false),
            DataType::TinyInt(_) => Datum::Int8(value.parse().ok()?),
            DataType::SmallInt(_) => Datum::Int16(value.parse().ok()?),
            DataType::Int(_) => Datum::Int32(value.parse().ok()?),
            DataType::BigInt(_) => Datum::Int64(value.parse().ok()?),
            DataType::Float(_) => {
                Datum::Float32(value.replace('_', ".").parse::<f32>().ok()?.into())
            }
            DataType::Double(_) => {
                Datum::Float64(value.replace('_', ".").parse::<f64>().ok()?.into())
            }
            DataType::Binary(_) | DataType::Bytes(_) => {
                if value.len() % 2 != 0 {
                    return None;
                }
                let bytes = value
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| {
                        let high = char::from(pair[0]).to_digit(16)?;
                        let low = char::from(pair[1]).to_digit(16)?;
                        Some((high * 16 + low) as u8)
                    })
                    .collect::<Option<Vec<_>>>()?;
                Datum::Blob(bytes.into())
            }
            DataType::Date(_) => {
                let date = value.parse::<jiff::civil::Date>().ok()?;
                let days = UNIX_EPOCH_DATE.until(date).ok()?.get_days();
                Datum::Date(Date::new(days))
            }
            DataType::Time(_) => {
                let (time, millis) = value.split_once('_')?;
                let parts = time
                    .split('-')
                    .map(str::parse::<i8>)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .ok()?;
                let [hour, minute, second] = parts.as_slice() else {
                    return None;
                };
                let millis: i32 = millis.parse().ok()?;
                if !(0..1000).contains(&millis) {
                    return None;
                }
                let time =
                    jiff::civil::Time::new(*hour, *minute, *second, millis * 1_000_000).ok()?;
                Datum::Time(Time::new(
                    i32::from(time.hour()) * 3_600_000
                        + i32::from(time.minute()) * 60_000
                        + i32::from(time.second()) * 1000
                        + millis,
                ))
            }
            DataType::Timestamp(_) | DataType::TimestampLTz(_) => {
                let (datetime, fraction) = value.split_once('_').unwrap_or((value, ""));
                if fraction.len() > 9 || !fraction.bytes().all(|c| c.is_ascii_digit()) {
                    return None;
                }
                let nanos = if fraction.is_empty() {
                    0
                } else {
                    fraction.parse::<i32>().ok()? * 10_i32.pow(9 - fraction.len() as u32)
                };
                let negative_year = datetime.starts_with('-');
                let parts = datetime
                    .strip_prefix('-')
                    .unwrap_or(datetime)
                    .split('-')
                    .map(str::parse::<i16>)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .ok()?;
                let [year, month, day, time @ ..] = parts.as_slice() else {
                    return None;
                };
                if time.len() > 3 {
                    return None;
                }
                let year = if negative_year {
                    year.checked_neg()?
                } else {
                    *year
                };
                let datetime = jiff::civil::DateTime::new(
                    year,
                    (*month).try_into().ok()?,
                    (*day).try_into().ok()?,
                    (*time.first().unwrap_or(&0)).try_into().ok()?,
                    (*time.get(1).unwrap_or(&0)).try_into().ok()?,
                    (*time.get(2).unwrap_or(&0)).try_into().ok()?,
                    0,
                )
                .ok()?;
                let instant = datetime.to_zoned(jiff::tz::TimeZone::UTC).ok()?.timestamp();
                // Split the positive fraction from the whole second, matching
                // Fluss's floor-based representation before the Unix epoch.
                let millis = instant.as_millisecond() + i64::from(nanos / 1_000_000);
                let nanos = nanos % 1_000_000;
                if matches!(data_type, DataType::Timestamp(_)) {
                    Datum::TimestampNtz(TimestampNtz::from_millis_nanos(millis, nanos).ok()?)
                } else {
                    Datum::TimestampLtz(TimestampLtz::from_millis_nanos(millis, nanos).ok()?)
                }
            }
            _ => return None,
        })
    })();
    parsed.ok_or_else(|| IllegalArgument {
        message: format!("Invalid or unsupported partition value {value:?} for {data_type:?}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{
        BigIntType, BinaryType, BooleanType, BytesType, CharType, DateType, DoubleType, FloatType,
        IntType, SmallIntType, StringType, TimeType, TimestampLTzType, TimestampType, TinyIntType,
    };
    use crate::row::{Date, Time, TimestampLtz, TimestampNtz};
    use std::borrow::Cow;

    use crate::metadata::TablePath;

    #[test]
    fn test_string() {
        let datum = Datum::String(Cow::Borrowed("Fluss"));

        let to_string_result = convert_value_of_type(&datum, &DataType::String(StringType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "Fluss");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_char() {
        let datum = Datum::String(Cow::Borrowed("F"));

        let to_string_result = convert_value_of_type(&datum, &DataType::Char(CharType::new(1)))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "F");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_boolean() {
        let datum = Datum::Bool(true);

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Boolean(BooleanType::new()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "true");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_byte() {
        let datum = Datum::Blob(Cow::Borrowed(&[0x10, 0x20, 0x30, 0x40, 0x50, 0xFF]));

        let to_string_result = convert_value_of_type(&datum, &DataType::Bytes(BytesType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1020304050ff");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_binary() {
        let datum = Datum::Blob(Cow::Borrowed(&[0x10, 0x20, 0x30, 0x40, 0x50, 0xFF]));

        let to_string_result = convert_value_of_type(&datum, &DataType::Binary(BinaryType::new(6)))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1020304050ff");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_tiny_int() {
        let datum = Datum::Int8(100);

        let to_string_result =
            convert_value_of_type(&datum, &DataType::TinyInt(TinyIntType::new()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "100");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_small_int() {
        let datum = Datum::Int16(-32760);

        let to_string_result =
            convert_value_of_type(&datum, &DataType::SmallInt(SmallIntType::new()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "-32760");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_int() {
        let datum = Datum::Int32(299000);

        let to_string_result = convert_value_of_type(&datum, &DataType::Int(IntType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "299000");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_big_int() {
        let datum = Datum::Int64(1748662955428);

        let to_string_result = convert_value_of_type(&datum, &DataType::BigInt(BigIntType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1748662955428");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_date() {
        let datum = Datum::Date(Date::new(20235));

        let to_string_result = convert_value_of_type(&datum, &DataType::Date(DateType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-27");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_time() {
        let datum = Datum::Time(Time::new(5402199));

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Time(TimeType::new(3).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "01-30-02_199");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_float() {
        let datum = Datum::Float32(5.73.into());

        let to_string_result = convert_value_of_type(&datum, &DataType::Float(FloatType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "5_73");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        let datum = Datum::Float32(f32::NAN.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Float(FloatType::new()))
                .expect("datum conversion to partition string failed"),
            "NaN"
        );

        let datum = Datum::Float32(f32::INFINITY.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Float(FloatType::new()))
                .expect("datum conversion to partition string failed"),
            "Inf"
        );

        let datum = Datum::Float32(f32::NEG_INFINITY.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Float(FloatType::new()))
                .expect("datum conversion to partition string failed"),
            "-Inf"
        );
    }

    #[test]
    fn test_double() {
        let datum = Datum::Float64(5.73737.into());

        let to_string_result = convert_value_of_type(&datum, &DataType::Double(DoubleType::new()))
            .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "5_73737");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        let datum = Datum::Float64(f64::NAN.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Double(DoubleType::new()))
                .expect("datum conversion to partition string failed"),
            "NaN"
        );

        let datum = Datum::Float64(f64::INFINITY.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Double(DoubleType::new()))
                .expect("datum conversion to partition string failed"),
            "Inf"
        );

        let datum = Datum::Float64(f64::NEG_INFINITY.into());
        assert_eq!(
            convert_value_of_type(&datum, &DataType::Double(DoubleType::new()))
                .expect("datum conversion to partition string failed"),
            "-Inf"
        );
    }

    #[test]
    fn test_timestamp_ntz() {
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(1748662955428, 99988)
                .expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero nanos of millis
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(1748662955428, 0).expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero millis
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(1748662955000, 99988)
                .expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_000099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero millis and zero nanos
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(1748662955000, 0).expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Negative millis
        let datum = Datum::TimestampNtz(
            TimestampNtz::from_millis_nanos(-1748662955428, 99988)
                .expect("TimestampNtz init failed"),
        );

        let to_string_result =
            convert_value_of_type(&datum, &DataType::Timestamp(TimestampType::new(9).unwrap()))
                .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1914-08-03-20-17-24_572099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }

    #[test]
    fn test_timestamp_ltz() {
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(1748662955428, 99988)
                .expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero nanos of millis
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(1748662955428, 0).expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_428");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero millis
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(1748662955000, 99988)
                .expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_000099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Zero millis and zero nanos
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(1748662955000, 0).expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "2025-05-31-03-42-35_");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());

        // Negative millis
        let datum = Datum::TimestampLtz(
            TimestampLtz::from_millis_nanos(-1748662955428, 99988)
                .expect("TimestampLtz init failed"),
        );

        let to_string_result = convert_value_of_type(
            &datum,
            &DataType::TimestampLTz(TimestampLTzType::new(9).unwrap()),
        )
        .expect("datum conversion to partition string failed");
        assert_eq!(to_string_result, "1914-08-03-20-17-24_572099988");
        let detect_invalid = TablePath::detect_invalid_name(&to_string_result);
        assert!(detect_invalid.is_none());
    }
}
