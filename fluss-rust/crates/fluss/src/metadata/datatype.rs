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

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Data type for Fluss table.
/// Impl reference: <todo: link>
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean(BooleanType),
    TinyInt(TinyIntType),
    SmallInt(SmallIntType),
    Int(IntType),
    BigInt(BigIntType),
    Float(FloatType),
    Double(DoubleType),
    Char(CharType),
    String(StringType),
    Decimal(DecimalType),
    Date(DateType),
    Time(TimeType),
    Timestamp(TimestampType),
    TimestampLTz(TimestampLTzType),
    Bytes(BytesType),
    Binary(BinaryType),
    Array(ArrayType),
    Map(MapType),
    Row(RowType),
}

impl DataType {
    pub fn is_nullable(&self) -> bool {
        match self {
            DataType::Boolean(v) => v.nullable,
            DataType::TinyInt(v) => v.nullable,
            DataType::SmallInt(v) => v.nullable,
            DataType::Int(v) => v.nullable,
            DataType::BigInt(v) => v.nullable,
            DataType::Decimal(v) => v.nullable,
            DataType::Double(v) => v.nullable,
            DataType::Float(v) => v.nullable,
            DataType::Binary(v) => v.nullable,
            DataType::Char(v) => v.nullable,
            DataType::String(v) => v.nullable,
            DataType::Date(v) => v.nullable,
            DataType::TimestampLTz(v) => v.nullable,
            DataType::Time(v) => v.nullable,
            DataType::Timestamp(v) => v.nullable,
            DataType::Array(v) => v.nullable,
            DataType::Map(v) => v.nullable,
            DataType::Row(v) => v.nullable,
            DataType::Bytes(v) => v.nullable,
        }
    }

    pub fn as_non_nullable(&self) -> Self {
        match self {
            DataType::Boolean(v) => DataType::Boolean(v.as_non_nullable()),
            DataType::TinyInt(v) => DataType::TinyInt(v.as_non_nullable()),
            DataType::SmallInt(v) => DataType::SmallInt(v.as_non_nullable()),
            DataType::Int(v) => DataType::Int(v.as_non_nullable()),
            DataType::BigInt(v) => DataType::BigInt(v.as_non_nullable()),
            DataType::Decimal(v) => DataType::Decimal(v.as_non_nullable()),
            DataType::Double(v) => DataType::Double(v.as_non_nullable()),
            DataType::Float(v) => DataType::Float(v.as_non_nullable()),
            DataType::Binary(v) => DataType::Binary(v.as_non_nullable()),
            DataType::Char(v) => DataType::Char(v.as_non_nullable()),
            DataType::String(v) => DataType::String(v.as_non_nullable()),
            DataType::Date(v) => DataType::Date(v.as_non_nullable()),
            DataType::TimestampLTz(v) => DataType::TimestampLTz(v.as_non_nullable()),
            DataType::Time(v) => DataType::Time(v.as_non_nullable()),
            DataType::Timestamp(v) => DataType::Timestamp(v.as_non_nullable()),
            DataType::Array(v) => DataType::Array(v.as_non_nullable()),
            DataType::Map(v) => DataType::Map(v.as_non_nullable()),
            DataType::Row(v) => DataType::Row(v.as_non_nullable()),
            DataType::Bytes(v) => DataType::Bytes(v.as_non_nullable()),
        }
    }

    /// Structural equality ignoring the outermost nullability flag at
    /// every level. Equivalent to comparing `as_non_nullable()` on both
    /// sides but without the recursive clone.
    pub(crate) fn eq_ignore_nullable(&self, other: &DataType) -> bool {
        match self {
            DataType::Boolean(_) => matches!(other, DataType::Boolean(_)),
            DataType::TinyInt(_) => matches!(other, DataType::TinyInt(_)),
            DataType::SmallInt(_) => matches!(other, DataType::SmallInt(_)),
            DataType::Int(_) => matches!(other, DataType::Int(_)),
            DataType::BigInt(_) => matches!(other, DataType::BigInt(_)),
            DataType::Float(_) => matches!(other, DataType::Float(_)),
            DataType::Double(_) => matches!(other, DataType::Double(_)),
            DataType::Date(_) => matches!(other, DataType::Date(_)),
            DataType::String(_) => matches!(other, DataType::String(_)),
            DataType::Bytes(_) => matches!(other, DataType::Bytes(_)),
            DataType::Char(a) => {
                matches!(other, DataType::Char(b) if a.length() == b.length())
            }
            DataType::Binary(a) => {
                matches!(other, DataType::Binary(b) if a.length() == b.length())
            }
            DataType::Decimal(a) => matches!(
                other,
                DataType::Decimal(b) if a.precision() == b.precision() && a.scale() == b.scale()
            ),
            DataType::Time(a) => {
                matches!(other, DataType::Time(b) if a.precision() == b.precision())
            }
            DataType::Timestamp(a) => {
                matches!(other, DataType::Timestamp(b) if a.precision() == b.precision())
            }
            DataType::TimestampLTz(a) => {
                matches!(other, DataType::TimestampLTz(b) if a.precision() == b.precision())
            }
            DataType::Array(a) => matches!(
                other,
                DataType::Array(b) if a.get_element_type().eq_ignore_nullable(b.get_element_type())
            ),
            DataType::Map(a) => matches!(
                other,
                DataType::Map(b)
                    if a.key_type().eq_ignore_nullable(b.key_type())
                        && a.value_type().eq_ignore_nullable(b.value_type())
            ),
            DataType::Row(a) => matches!(
                other,
                DataType::Row(b) if a.fields().len() == b.fields().len()
                    && a.fields().iter().zip(b.fields().iter()).all(|(x, y)| {
                        x.name() == y.name() && x.data_type().eq_ignore_nullable(y.data_type())
                    })
            ),
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Boolean(v) => write!(f, "{v}"),
            DataType::TinyInt(v) => write!(f, "{v}"),
            DataType::SmallInt(v) => write!(f, "{v}"),
            DataType::Int(v) => write!(f, "{v}"),
            DataType::BigInt(v) => write!(f, "{v}"),
            DataType::Float(v) => write!(f, "{v}"),
            DataType::Double(v) => write!(f, "{v}"),
            DataType::Char(v) => write!(f, "{v}"),
            DataType::String(v) => write!(f, "{v}"),
            DataType::Decimal(v) => write!(f, "{v}"),
            DataType::Date(v) => write!(f, "{v}"),
            DataType::Time(v) => write!(f, "{v}"),
            DataType::Timestamp(v) => write!(f, "{v}"),
            DataType::TimestampLTz(v) => write!(f, "{v}"),
            DataType::Bytes(v) => write!(f, "{v}"),
            DataType::Binary(v) => write!(f, "{v}"),
            DataType::Array(v) => write!(f, "{v}"),
            DataType::Map(v) => write!(f, "{v}"),
            DataType::Row(v) => write!(f, "{v}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BooleanType {
    nullable: bool,
}

impl Default for BooleanType {
    fn default() -> Self {
        Self::new()
    }
}

impl BooleanType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for BooleanType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BOOLEAN")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TinyIntType {
    nullable: bool,
}

impl Default for TinyIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl TinyIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for TinyIntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TINYINT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SmallIntType {
    nullable: bool,
}

impl Default for SmallIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl SmallIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for SmallIntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SMALLINT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IntType {
    nullable: bool,
}

impl Default for IntType {
    fn default() -> Self {
        Self::new()
    }
}

impl IntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for IntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "INT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BigIntType {
    nullable: bool,
}

impl Default for BigIntType {
    fn default() -> Self {
        Self::new()
    }
}

impl BigIntType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for BigIntType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BIGINT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FloatType {
    nullable: bool,
}

impl Default for FloatType {
    fn default() -> Self {
        Self::new()
    }
}

impl FloatType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for FloatType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "FLOAT")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DoubleType {
    nullable: bool,
}

impl Default for DoubleType {
    fn default() -> Self {
        Self::new()
    }
}

impl DoubleType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for DoubleType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DOUBLE")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CharType {
    nullable: bool,
    length: u32,
}

impl CharType {
    pub fn new(length: u32) -> Self {
        Self::with_nullable(length, true)
    }

    pub fn with_nullable(length: u32, nullable: bool) -> Self {
        Self { nullable, length }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(self.length, false)
    }

    pub fn length(&self) -> u32 {
        self.length
    }
}

impl Default for CharType {
    fn default() -> Self {
        Self::new(1)
    }
}

impl Display for CharType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CHAR({})", self.length)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StringType {
    nullable: bool,
}

impl Default for StringType {
    fn default() -> Self {
        Self::new()
    }
}

impl StringType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for StringType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "STRING")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DecimalType {
    nullable: bool,
    precision: u32,
    scale: u32,
}

impl DecimalType {
    pub const MIN_PRECISION: u32 = 1;

    pub const MAX_PRECISION: u32 = 38;

    pub const DEFAULT_PRECISION: u32 = 10;

    pub const MIN_SCALE: u32 = 0;

    pub const DEFAULT_SCALE: u32 = 0;

    pub fn new(precision: u32, scale: u32) -> Result<Self> {
        Self::with_nullable(true, precision, scale)
    }

    /// Create a DecimalType with validation, returning an error if parameters are invalid.
    pub fn with_nullable(nullable: bool, precision: u32, scale: u32) -> Result<Self> {
        // Validate precision
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return Err(IllegalArgument {
                message: format!(
                    "Decimal precision must be between {} and {} (both inclusive), got: {}",
                    Self::MIN_PRECISION,
                    Self::MAX_PRECISION,
                    precision
                ),
            });
        }
        // Validate scale
        if scale > precision {
            return Err(IllegalArgument {
                message: format!(
                    "Decimal scale must be between {} and the precision {} (both inclusive), got: {}",
                    Self::MIN_SCALE,
                    precision,
                    scale
                ),
            });
        }
        Ok(DecimalType {
            nullable,
            precision,
            scale,
        })
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision, self.scale)
            .expect("Invalid decimal precision or scale")
    }
}

impl Default for DecimalType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION, Self::DEFAULT_SCALE)
            .expect("Invalid default decimal precision or scale")
    }
}

impl Display for DecimalType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DECIMAL({}, {})", self.precision, self.scale)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DateType {
    nullable: bool,
}

impl Default for DateType {
    fn default() -> Self {
        Self::new()
    }
}

impl DateType {
    pub fn new() -> Self {
        Self::with_nullable(true)
    }

    pub fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for DateType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DATE")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimeType {
    nullable: bool,
    precision: u32,
}

impl Default for TimeType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION).expect("Invalid default time precision")
    }
}

impl TimeType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 0;

    pub fn new(precision: u32) -> Result<Self> {
        Self::with_nullable(true, precision)
    }

    /// Create a TimeType with validation, returning an error if precision is invalid.
    pub fn with_nullable(nullable: bool, precision: u32) -> Result<Self> {
        // Validate precision
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return Err(IllegalArgument {
                message: format!(
                    "Time precision must be between {} and {} (both inclusive), got: {}",
                    Self::MIN_PRECISION,
                    Self::MAX_PRECISION,
                    precision
                ),
            });
        }
        Ok(TimeType {
            nullable,
            precision,
        })
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision).expect("Invalid time precision")
    }
}

impl Display for TimeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TIME({})", self.precision)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimestampType {
    nullable: bool,
    precision: u32,
}

impl Default for TimestampType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION).expect("Invalid default timestamp precision")
    }
}

impl TimestampType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 6;

    pub fn new(precision: u32) -> Result<Self> {
        Self::with_nullable(true, precision)
    }

    /// Create a TimestampType with validation, returning an error if precision is invalid.
    pub fn with_nullable(nullable: bool, precision: u32) -> Result<Self> {
        // Validate precision
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return Err(IllegalArgument {
                message: format!(
                    "Timestamp precision must be between {} and {} (both inclusive), got: {}",
                    Self::MIN_PRECISION,
                    Self::MAX_PRECISION,
                    precision
                ),
            });
        }
        Ok(TimestampType {
            nullable,
            precision,
        })
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision).expect("Invalid timestamp precision")
    }
}

impl Display for TimestampType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TIMESTAMP({})", self.precision)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimestampLTzType {
    nullable: bool,
    precision: u32,
}

impl Default for TimestampLTzType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_PRECISION)
            .expect("Invalid default timestamp with local time zone precision")
    }
}

impl TimestampLTzType {
    pub const MIN_PRECISION: u32 = 0;

    pub const MAX_PRECISION: u32 = 9;

    pub const DEFAULT_PRECISION: u32 = 6;

    pub fn new(precision: u32) -> Result<Self> {
        Self::with_nullable(true, precision)
    }

    /// Create a TimestampLTzType with validation, returning an error if precision is invalid.
    pub fn with_nullable(nullable: bool, precision: u32) -> Result<Self> {
        // Validate precision
        if !(Self::MIN_PRECISION..=Self::MAX_PRECISION).contains(&precision) {
            return Err(IllegalArgument {
                message: format!(
                    "Timestamp with local time zone precision must be between {} and {} (both inclusive), got: {}",
                    Self::MIN_PRECISION,
                    Self::MAX_PRECISION,
                    precision
                ),
            });
        }
        Ok(TimestampLTzType {
            nullable,
            precision,
        })
    }

    pub fn precision(&self) -> u32 {
        self.precision
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.precision)
            .expect("Invalid timestamp with local time zone precision")
    }
}

impl Display for TimestampLTzType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TIMESTAMP_LTZ({})", self.precision)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BytesType {
    nullable: bool,
}

impl Default for BytesType {
    fn default() -> Self {
        Self::new()
    }
}

impl BytesType {
    pub const fn new() -> Self {
        Self::with_nullable(true)
    }

    pub const fn with_nullable(nullable: bool) -> Self {
        Self { nullable }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false)
    }
}

impl Display for BytesType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BYTES")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BinaryType {
    nullable: bool,
    length: usize,
}

impl BinaryType {
    pub const MIN_LENGTH: usize = 1;

    pub const MAX_LENGTH: usize = usize::MAX;

    pub const DEFAULT_LENGTH: usize = 1;

    pub fn new(length: usize) -> Self {
        Self::with_nullable(true, length)
    }

    pub fn with_nullable(nullable: bool, length: usize) -> Self {
        Self { nullable, length }
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.length)
    }
}

impl Default for BinaryType {
    fn default() -> Self {
        Self::new(Self::DEFAULT_LENGTH)
    }
}

impl Display for BinaryType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BINARY({})", self.length)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArrayType {
    nullable: bool,
    element_type: Box<DataType>,
}

impl ArrayType {
    pub fn new(element_type: DataType) -> Self {
        Self::with_nullable(true, element_type)
    }

    pub fn with_nullable(nullable: bool, element_type: DataType) -> Self {
        Self {
            nullable,
            element_type: Box::new(element_type),
        }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self {
            nullable: false,
            element_type: self.element_type.clone(),
        }
    }

    pub fn get_element_type(&self) -> &DataType {
        &self.element_type
    }
}

impl Display for ArrayType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ARRAY<{}>", self.element_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Hash)]
pub struct MapType {
    nullable: bool,
    key_type: Box<DataType>,
    value_type: Box<DataType>,
}

// Route Deserialize through `with_nullable` so a Serde-built MapType
// collapses to the same canonical form as the constructor (otherwise
// equivalent maps disagree under `PartialEq`).
impl<'de> Deserialize<'de> for MapType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            nullable: bool,
            key_type: Box<DataType>,
            value_type: Box<DataType>,
        }
        let raw = Raw::deserialize(deserializer)?;
        Ok(MapType::with_nullable(
            raw.nullable,
            *raw.key_type,
            *raw.value_type,
        ))
    }
}

impl MapType {
    pub fn new(key_type: DataType, value_type: DataType) -> Self {
        Self::with_nullable(true, key_type, value_type)
    }

    pub fn with_nullable(nullable: bool, key_type: DataType, value_type: DataType) -> Self {
        Self {
            nullable,
            key_type: Box::new(key_type.as_non_nullable()),
            value_type: Box::new(value_type),
        }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self {
            nullable: false,
            key_type: self.key_type.clone(),
            value_type: self.value_type.clone(),
        }
    }

    pub fn key_type(&self) -> &DataType {
        &self.key_type
    }

    pub fn value_type(&self) -> &DataType {
        &self.value_type
    }
}

impl Display for MapType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MAP<{}, {}>", self.key_type, self.value_type)?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct RowType {
    nullable: bool,
    fields: Vec<DataField>,
}

impl RowType {
    pub const fn new(fields: Vec<DataField>) -> Self {
        Self::with_nullable(true, fields)
    }

    pub const fn with_nullable(nullable: bool, fields: Vec<DataField>) -> Self {
        Self { nullable, fields }
    }

    pub fn as_non_nullable(&self) -> Self {
        Self::with_nullable(false, self.fields.clone())
    }

    pub fn fields(&self) -> &Vec<DataField> {
        &self.fields
    }

    pub fn get_field_index(&self, field_name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == field_name)
    }

    pub fn field_types(&self) -> impl Iterator<Item = &DataType> + '_ {
        self.fields.iter().map(|f| &f.data_type)
    }

    pub fn get_field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    pub fn project_with_field_names(&self, field_names: &[String]) -> Result<RowType> {
        let indices: Vec<usize> = field_names
            .iter()
            .map(|name| {
                self.get_field_index(name).ok_or_else(|| IllegalArgument {
                    message: format!("Field '{name}' does not exist in the row type"),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        self.project(indices.as_slice())
    }

    pub fn project(&self, project_field_positions: &[usize]) -> Result<RowType> {
        Ok(RowType::with_nullable(
            self.nullable,
            project_field_positions
                .iter()
                .map(|pos| {
                    self.fields
                        .get(*pos)
                        .cloned()
                        .ok_or_else(|| IllegalArgument {
                            message: format!("invalid field position: {}", *pos),
                        })
                })
                .collect::<Result<Vec<_>>>()?,
        ))
    }

    #[cfg(test)]
    pub fn with_data_types(data_types: Vec<DataType>) -> Self {
        let mut fields: Vec<DataField> = Vec::new();
        data_types.iter().enumerate().for_each(|(idx, data_type)| {
            fields.push(DataField::new(format!("f{idx}"), data_type.clone(), None));
        });

        Self::with_nullable(true, fields)
    }

    #[cfg(test)]
    pub fn with_data_types_and_field_names(
        data_types: Vec<DataType>,
        field_names: Vec<&str>,
    ) -> Self {
        let fields = data_types
            .into_iter()
            .zip(field_names)
            .map(|(data_type, field_name)| {
                DataField::new(field_name.to_string(), data_type.clone(), None)
            })
            .collect::<Vec<_>>();

        Self::with_nullable(true, fields)
    }
}

impl Display for RowType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ROW<")?;
        for (i, field) in self.fields.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{field}")?;
        }
        write!(f, ">")?;
        if !self.nullable {
            write!(f, " NOT NULL")?;
        }
        Ok(())
    }
}

pub struct DataTypes;

impl DataTypes {
    pub fn binary(length: usize) -> DataType {
        DataType::Binary(BinaryType::new(length))
    }

    pub const fn bytes() -> DataType {
        DataType::Bytes(BytesType::new())
    }

    pub fn boolean() -> DataType {
        DataType::Boolean(BooleanType::new())
    }

    pub fn int() -> DataType {
        DataType::Int(IntType::new())
    }

    /// Data type of a 1-byte signed integer with values from -128 to 127.
    pub fn tinyint() -> DataType {
        DataType::TinyInt(TinyIntType::new())
    }

    /// Data type of a 2-byte signed integer with values from -32,768 to 32,767.
    pub fn smallint() -> DataType {
        DataType::SmallInt(SmallIntType::new())
    }

    pub fn bigint() -> DataType {
        DataType::BigInt(BigIntType::new())
    }

    /// Data type of a 4-byte single precision floating point number.
    pub fn float() -> DataType {
        DataType::Float(FloatType::new())
    }

    /// Data type of an 8-byte double precision floating point number.
    pub fn double() -> DataType {
        DataType::Double(DoubleType::new())
    }

    pub fn char(length: u32) -> DataType {
        DataType::Char(CharType::new(length))
    }

    /// Data type of a variable-length character string.
    pub fn string() -> DataType {
        DataType::String(StringType::new())
    }

    /// Data type of a decimal number with fixed precision and scale `DECIMAL(p, s)` where
    /// `p` is the number of digits in a number (=precision) and `s` is the number of
    /// digits to the right of the decimal point in a number (=scale). `p` must have a value
    /// between 1 and 38 (both inclusive). `s` must have a value between 0 and `p` (both inclusive).
    pub fn decimal(precision: u32, scale: u32) -> DataType {
        DataType::Decimal(DecimalType::new(precision, scale).expect("Invalid decimal parameters"))
    }

    pub fn date() -> DataType {
        DataType::Date(DateType::new())
    }

    /// Data type of a time WITHOUT time zone `TIME` with no fractional seconds by default.
    pub fn time() -> DataType {
        DataType::Time(TimeType::default())
    }

    /// Data type of a time WITHOUT time zone `TIME(p)` where `p` is the number of digits
    /// of fractional seconds (=precision). `p` must have a value between 0 and 9 (both inclusive).
    pub fn time_with_precision(precision: u32) -> DataType {
        DataType::Time(TimeType::new(precision).expect("Invalid time precision"))
    }

    /// Data type of a timestamp WITHOUT time zone `TIMESTAMP` with 6 digits of fractional
    /// seconds by default.
    pub fn timestamp() -> DataType {
        DataType::Timestamp(TimestampType::default())
    }

    /// Data type of a timestamp WITHOUT time zone `TIMESTAMP(p)` where `p` is the number
    /// of digits of fractional seconds (=precision). `p` must have a value between 0 and 9
    /// (both inclusive).
    pub fn timestamp_with_precision(precision: u32) -> DataType {
        DataType::Timestamp(TimestampType::new(precision).expect("Invalid timestamp precision"))
    }

    /// Data type of a timestamp WITH time zone `TIMESTAMP WITH TIME ZONE` with 6 digits of
    /// fractional seconds by default.
    pub fn timestamp_ltz() -> DataType {
        DataType::TimestampLTz(TimestampLTzType::default())
    }

    /// Data type of a timestamp WITH time zone `TIMESTAMP WITH TIME ZONE(p)` where `p` is the number
    /// of digits of fractional seconds (=precision). `p` must have a value between 0 and 9 (both inclusive).
    pub fn timestamp_ltz_with_precision(precision: u32) -> DataType {
        DataType::TimestampLTz(
            TimestampLTzType::new(precision)
                .expect("Invalid timestamp with local time zone precision"),
        )
    }

    /// Data type of an array of elements with same subtype.
    pub fn array(element: DataType) -> DataType {
        DataType::Array(ArrayType::new(element))
    }

    /// Data type of an associative array that maps keys to values.
    pub fn map(key_type: DataType, value_type: DataType) -> DataType {
        DataType::Map(MapType::new(key_type, value_type))
    }

    /// Field definition with field name and data type.
    pub fn field<N: Into<String>>(name: N, data_type: DataType) -> DataField {
        DataField::new(name, data_type, None)
    }

    /// Field definition with field name, data type, and a description.
    pub fn field_with_description<N: Into<String>>(
        name: N,
        data_type: DataType,
        description: String,
    ) -> DataField {
        DataField::new(name, data_type, Some(description))
    }

    /// Data type of a sequence of fields.
    pub fn row(fields: Vec<DataField>) -> DataType {
        DataType::Row(RowType::new(fields))
    }

    /// Data type of a sequence of fields with generated field names (f0, f1, f2, ...).
    pub fn row_from_types(field_types: Vec<DataType>) -> DataType {
        let fields = field_types
            .into_iter()
            .enumerate()
            .map(|(i, dt)| DataField::new(format!("f{i}"), dt, None))
            .collect();
        DataType::Row(RowType::new(fields))
    }
}

pub const UNASSIGNED_FIELD_ID: i32 = -1;

pub fn reassign_field_ids(data_type: &DataType, counter: &mut i32) -> DataType {
    match data_type {
        DataType::Array(at) => DataType::Array(ArrayType::with_nullable(
            at.nullable,
            reassign_field_ids(at.get_element_type(), counter),
        )),
        DataType::Map(mt) => DataType::Map(MapType::with_nullable(
            mt.nullable,
            reassign_field_ids(mt.key_type(), counter),
            reassign_field_ids(mt.value_type(), counter),
        )),
        DataType::Row(rt) => {
            let new_fields: Vec<DataField> = rt
                .fields()
                .iter()
                .map(|f| {
                    *counter += 1;
                    let id = *counter;
                    let new_inner = reassign_field_ids(&f.data_type, counter);
                    DataField::with_field_id(f.name.clone(), new_inner, f.description.clone(), id)
                })
                .collect();
            DataType::Row(RowType::with_nullable(rt.nullable, new_fields))
        }
        _ => data_type.clone(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataField {
    pub name: String,
    pub data_type: DataType,
    pub description: Option<String>,
    pub field_id: i32,
}

// field_id is excluded from PartialEq/Eq/Hash to match Java's DataField.equals/hashCode.
impl PartialEq for DataField {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.data_type == other.data_type
            && self.description == other.description
    }
}

impl Eq for DataField {}

impl std::hash::Hash for DataField {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.data_type.hash(state);
        self.description.hash(state);
    }
}

impl DataField {
    pub fn new<N: Into<String>>(
        name: N,
        data_type: DataType,
        description: Option<String>,
    ) -> DataField {
        DataField {
            name: name.into(),
            data_type,
            description,
            field_id: UNASSIGNED_FIELD_ID,
        }
    }

    pub fn with_field_id<N: Into<String>>(
        name: N,
        data_type: DataType,
        description: Option<String>,
        field_id: i32,
    ) -> DataField {
        DataField {
            name: name.into(),
            data_type,
            description,
            field_id,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn field_id(&self) -> i32 {
        self.field_id
    }
}

impl Display for DataField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.name, self.data_type)
    }
}

#[test]
fn test_primitive_types_display() {
    // Test simple primitive types with nullable and non-nullable variants
    assert_eq!(BooleanType::new().to_string(), "BOOLEAN");
    assert_eq!(
        BooleanType::with_nullable(false).to_string(),
        "BOOLEAN NOT NULL"
    );

    assert_eq!(TinyIntType::new().to_string(), "TINYINT");
    assert_eq!(
        TinyIntType::with_nullable(false).to_string(),
        "TINYINT NOT NULL"
    );

    assert_eq!(SmallIntType::new().to_string(), "SMALLINT");
    assert_eq!(
        SmallIntType::with_nullable(false).to_string(),
        "SMALLINT NOT NULL"
    );

    assert_eq!(IntType::new().to_string(), "INT");
    assert_eq!(IntType::with_nullable(false).to_string(), "INT NOT NULL");

    assert_eq!(BigIntType::new().to_string(), "BIGINT");
    assert_eq!(
        BigIntType::with_nullable(false).to_string(),
        "BIGINT NOT NULL"
    );

    assert_eq!(FloatType::new().to_string(), "FLOAT");
    assert_eq!(
        FloatType::with_nullable(false).to_string(),
        "FLOAT NOT NULL"
    );

    assert_eq!(DoubleType::new().to_string(), "DOUBLE");
    assert_eq!(
        DoubleType::with_nullable(false).to_string(),
        "DOUBLE NOT NULL"
    );

    assert_eq!(StringType::new().to_string(), "STRING");
    assert_eq!(
        StringType::with_nullable(false).to_string(),
        "STRING NOT NULL"
    );

    assert_eq!(DateType::new().to_string(), "DATE");
    assert_eq!(DateType::with_nullable(false).to_string(), "DATE NOT NULL");

    assert_eq!(BytesType::new().to_string(), "BYTES");
    assert_eq!(
        BytesType::with_nullable(false).to_string(),
        "BYTES NOT NULL"
    );
}

#[test]
fn test_parameterized_types_display() {
    // Test types with parameters (length, precision, scale, etc.)
    assert_eq!(CharType::new(10).to_string(), "CHAR(10)");
    assert_eq!(
        CharType::with_nullable(20, false).to_string(),
        "CHAR(20) NOT NULL"
    );

    assert_eq!(BinaryType::new(100).to_string(), "BINARY(100)");
    assert_eq!(
        BinaryType::with_nullable(false, 256).to_string(),
        "BINARY(256) NOT NULL"
    );

    assert_eq!(
        DecimalType::new(10, 2).unwrap().to_string(),
        "DECIMAL(10, 2)"
    );
    assert_eq!(
        DecimalType::with_nullable(false, 38, 10)
            .unwrap()
            .to_string(),
        "DECIMAL(38, 10) NOT NULL"
    );

    assert_eq!(TimeType::new(0).unwrap().to_string(), "TIME(0)");
    assert_eq!(TimeType::new(3).unwrap().to_string(), "TIME(3)");
    assert_eq!(
        TimeType::with_nullable(false, 9).unwrap().to_string(),
        "TIME(9) NOT NULL"
    );

    assert_eq!(TimestampType::new(6).unwrap().to_string(), "TIMESTAMP(6)");
    assert_eq!(TimestampType::new(0).unwrap().to_string(), "TIMESTAMP(0)");
    assert_eq!(
        TimestampType::with_nullable(false, 9).unwrap().to_string(),
        "TIMESTAMP(9) NOT NULL"
    );

    assert_eq!(
        TimestampLTzType::new(6).unwrap().to_string(),
        "TIMESTAMP_LTZ(6)"
    );
    assert_eq!(
        TimestampLTzType::new(3).unwrap().to_string(),
        "TIMESTAMP_LTZ(3)"
    );
    assert_eq!(
        TimestampLTzType::with_nullable(false, 9)
            .unwrap()
            .to_string(),
        "TIMESTAMP_LTZ(9) NOT NULL"
    );
}

#[test]
fn test_array_display() {
    let array_type = ArrayType::new(DataTypes::int());
    assert_eq!(array_type.to_string(), "ARRAY<INT>");

    let array_type_non_null = ArrayType::with_nullable(false, DataTypes::string());
    assert_eq!(array_type_non_null.to_string(), "ARRAY<STRING> NOT NULL");

    let nested_array = ArrayType::new(DataTypes::array(DataTypes::int()));
    assert_eq!(nested_array.to_string(), "ARRAY<ARRAY<INT>>");
}

#[test]
fn test_map_display() {
    let map_type = MapType::new(DataTypes::string(), DataTypes::int());
    assert_eq!(map_type.to_string(), "MAP<STRING NOT NULL, INT>");

    let map_type_non_null = MapType::with_nullable(false, DataTypes::int(), DataTypes::string());
    assert_eq!(
        map_type_non_null.to_string(),
        "MAP<INT NOT NULL, STRING> NOT NULL"
    );

    let nested_map = MapType::new(
        DataTypes::string(),
        DataTypes::map(DataTypes::int(), DataTypes::boolean()),
    );
    assert_eq!(
        nested_map.to_string(),
        "MAP<STRING NOT NULL, MAP<INT NOT NULL, BOOLEAN>>"
    );
}

#[test]
fn test_map_deserialize_normalises_key_nullability() {
    let json = r#"{
        "nullable": true,
        "key_type": {"Int": {"nullable": true}},
        "value_type": {"String": {"nullable": true}}
    }"#;
    let from_json: MapType = serde_json::from_str(json).expect("deserialize");
    let from_ctor = MapType::new(DataTypes::int(), DataTypes::string());
    assert_eq!(from_json, from_ctor);
    assert!(!from_json.key_type().is_nullable());
}

#[test]
fn test_map_deserialize_normalises_nested_map_keys() {
    let json = r#"{
        "nullable": true,
        "key_type": {"String": {"nullable": true}},
        "value_type": {"Map": {
            "nullable": true,
            "key_type": {"Int": {"nullable": true}},
            "value_type": {"Boolean": {"nullable": true}}
        }}
    }"#;
    let from_json: MapType = serde_json::from_str(json).expect("deserialize");
    let from_ctor = MapType::new(
        DataTypes::string(),
        DataTypes::map(DataTypes::int(), DataTypes::boolean()),
    );
    assert_eq!(from_json, from_ctor);
    assert!(!from_json.key_type().is_nullable());
    let inner = match from_json.value_type() {
        DataType::Map(m) => m,
        other => panic!("expected nested Map, got {other:?}"),
    };
    assert!(!inner.key_type().is_nullable());
}

#[test]
fn test_row_display() {
    let fields = vec![
        DataTypes::field("id", DataTypes::int()),
        DataTypes::field("name", DataTypes::string()),
    ];
    let row_type = RowType::new(fields);
    assert_eq!(row_type.to_string(), "ROW<id INT, name STRING>");

    let fields_non_null = vec![DataTypes::field("age", DataTypes::bigint())];
    let row_type_non_null = RowType::with_nullable(false, fields_non_null);
    assert_eq!(row_type_non_null.to_string(), "ROW<age BIGINT> NOT NULL");
}

#[test]
fn test_datatype_display() {
    assert_eq!(DataTypes::boolean().to_string(), "BOOLEAN");
    assert_eq!(DataTypes::int().to_string(), "INT");
    assert_eq!(DataTypes::string().to_string(), "STRING");
    assert_eq!(DataTypes::char(50).to_string(), "CHAR(50)");
    assert_eq!(DataTypes::decimal(10, 2).to_string(), "DECIMAL(10, 2)");
    assert_eq!(DataTypes::time_with_precision(3).to_string(), "TIME(3)");
    assert_eq!(
        DataTypes::timestamp_with_precision(6).to_string(),
        "TIMESTAMP(6)"
    );
    assert_eq!(
        DataTypes::timestamp_ltz_with_precision(9).to_string(),
        "TIMESTAMP_LTZ(9)"
    );
    assert_eq!(DataTypes::array(DataTypes::int()).to_string(), "ARRAY<INT>");
    assert_eq!(
        DataTypes::map(DataTypes::string(), DataTypes::int()).to_string(),
        "MAP<STRING NOT NULL, INT>"
    );
}

#[test]
fn test_datafield_display() {
    let field = DataTypes::field("user_id", DataTypes::bigint());
    assert_eq!(field.to_string(), "user_id BIGINT");

    let field2 = DataTypes::field("email", DataTypes::string());
    assert_eq!(field2.to_string(), "email STRING");

    let field3 = DataTypes::field("score", DataTypes::decimal(10, 2));
    assert_eq!(field3.to_string(), "score DECIMAL(10, 2)");
}

#[test]
fn test_complex_nested_display() {
    let row_type = DataTypes::row(vec![
        DataTypes::field("id", DataTypes::int()),
        DataTypes::field("tags", DataTypes::array(DataTypes::string())),
        DataTypes::field(
            "metadata",
            DataTypes::map(DataTypes::string(), DataTypes::string()),
        ),
    ]);
    assert_eq!(
        row_type.to_string(),
        "ROW<id INT, tags ARRAY<STRING>, metadata MAP<STRING NOT NULL, STRING>>"
    );
}

#[test]
fn test_non_nullable_datatype() {
    let nullable_int = DataTypes::int();
    assert_eq!(nullable_int.to_string(), "INT");

    let non_nullable_int = nullable_int.as_non_nullable();
    assert_eq!(non_nullable_int.to_string(), "INT NOT NULL");
}

#[test]
fn test_deeply_nested_types() {
    let nested = DataTypes::array(DataTypes::map(
        DataTypes::string(),
        DataTypes::row(vec![
            DataTypes::field("x", DataTypes::int()),
            DataTypes::field("y", DataTypes::int()),
        ]),
    ));
    assert_eq!(
        nested.to_string(),
        "ARRAY<MAP<STRING NOT NULL, ROW<x INT, y INT>>>"
    );
}

// ============================================================================
// DecimalType validation tests
// ============================================================================

#[test]
fn test_decimal_invalid_precision() {
    // DecimalType::with_nullable should return an error for invalid precision
    let result = DecimalType::with_nullable(true, 50, 2);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Decimal precision must be between 1 and 38")
    );
}

#[test]
fn test_decimal_invalid_scale() {
    // DecimalType::with_nullable should return an error when scale > precision
    let result = DecimalType::with_nullable(true, 10, 15);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Decimal scale must be between 0 and the precision 10")
    );
}

// ============================================================================
// DecimalType validation tests - edge cases
// ============================================================================

#[test]
fn test_decimal_valid_precision_and_scale() {
    // Valid: precision=10, scale=2
    let result = DecimalType::with_nullable(true, 10, 2);
    assert!(result.is_ok());
    let decimal = result.unwrap();
    assert_eq!(decimal.precision(), 10);
    assert_eq!(decimal.scale(), 2);
    // Nullable: should NOT contain "NOT NULL"
    assert!(!decimal.to_string().contains("NOT NULL"));

    // Valid: precision=38, scale=0
    let result = DecimalType::with_nullable(true, 38, 0);
    assert!(result.is_ok());
    let decimal = result.unwrap();
    assert_eq!(decimal.precision(), 38);
    assert_eq!(decimal.scale(), 0);

    // Valid: precision=1, scale=0
    let result = DecimalType::with_nullable(false, 1, 0);
    assert!(result.is_ok());
    let decimal = result.unwrap();
    assert_eq!(decimal.precision(), 1);
    assert_eq!(decimal.scale(), 0);
    // Non-nullable: should contain "NOT NULL"
    assert!(decimal.to_string().contains("NOT NULL"));
}

#[test]
fn test_decimal_invalid_precision_zero() {
    // Invalid: precision=0 (edge case not covered by existing tests)
    let result = DecimalType::with_nullable(true, 0, 0);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Decimal precision must be between 1 and 38")
    );
}

#[test]
fn test_decimal_scale_equals_precision_boundary() {
    // Boundary: precision=10, scale=10 (scale == precision is valid)
    let result = DecimalType::with_nullable(true, 10, 10);
    assert!(result.is_ok());
    let decimal = result.unwrap();
    assert_eq!(decimal.precision(), 10);
    assert_eq!(decimal.scale(), 10);
}

// ============================================================================
// TimeType validation tests
// ============================================================================

#[test]
fn test_time_valid_precision() {
    // Test all valid precision values 0 through 9
    for precision in 0..=9 {
        let result = TimeType::with_nullable(true, precision);
        assert!(result.is_ok(), "precision {precision} should be valid");
        let time = result.unwrap();
        assert_eq!(time.precision(), precision);
    }
}

#[test]
fn test_time_invalid_precision() {
    // TimeType::with_nullable should return an error for invalid precision
    let result = TimeType::with_nullable(true, 10);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Time precision must be between 0 and 9")
    );
}

// ============================================================================
// TimestampType validation tests
// ============================================================================

#[test]
fn test_timestamp_valid_precision() {
    // Test all valid precision values 0 through 9
    for precision in 0..=9 {
        let result = TimestampType::with_nullable(true, precision);
        assert!(result.is_ok(), "precision {precision} should be valid");
        let timestamp_type = result.unwrap();
        assert_eq!(timestamp_type.precision(), precision);
    }
}

#[test]
fn test_timestamp_invalid_precision() {
    // TimestampType::with_nullable should return an error for invalid precision
    let result = TimestampType::with_nullable(true, 10);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Timestamp precision must be between 0 and 9")
    );
}

#[test]
fn test_timestamp_ltz_invalid_precision() {
    // TimestampLTzType::with_nullable should return an error for invalid precision
    let result = TimestampLTzType::with_nullable(true, 10);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Timestamp with local time zone precision must be between 0 and 9")
    );
}

// ============================================================================
// RowType projection tests
// ============================================================================

#[test]
fn test_row_type_project_valid_indices() {
    // Create a 3-column row type
    let row_type = RowType::with_data_types_and_field_names(
        vec![DataTypes::int(), DataTypes::string(), DataTypes::bigint()],
        vec!["id", "name", "age"],
    );

    // Valid projection by indices: [0, 2]
    let projected = row_type.project(&[0, 2]).unwrap();
    assert_eq!(projected.fields().len(), 2);
    assert_eq!(projected.fields()[0].name, "id");
    assert_eq!(projected.fields()[1].name, "age");
}

#[test]
fn test_row_type_project_empty_indices() {
    // Create a 3-column row type
    let row_type = RowType::with_data_types_and_field_names(
        vec![DataTypes::int(), DataTypes::string(), DataTypes::bigint()],
        vec!["id", "name", "age"],
    );

    // Projection with an empty indices array should yield an empty RowType
    let projected = row_type.project(&[]).unwrap();
    assert_eq!(projected.fields().len(), 0);
}

#[test]
fn test_row_type_project_with_field_names_valid() {
    // Create a 3-column row type
    let row_type = RowType::with_data_types_and_field_names(
        vec![DataTypes::int(), DataTypes::string(), DataTypes::bigint()],
        vec!["id", "name", "age"],
    );

    // Valid projection by names: ["id", "name"]
    let projected = row_type
        .project_with_field_names(&["id".to_string(), "name".to_string()])
        .unwrap();
    assert_eq!(projected.fields().len(), 2);
    assert_eq!(projected.fields()[0].name, "id");
    assert_eq!(projected.fields()[1].name, "name");
}

#[test]
fn test_row_type_project_index_out_of_bounds() {
    // Create a 3-column row type
    let row_type = RowType::with_data_types_and_field_names(
        vec![DataTypes::int(), DataTypes::string(), DataTypes::bigint()],
        vec!["id", "name", "age"],
    );

    // Error: index out of bounds
    let result = row_type.project(&[0, 5]);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid field position: 5")
    );
}

#[test]
fn test_row_type_project_with_field_names_nonexistent() {
    // Create a 3-column row type
    let row_type = RowType::with_data_types_and_field_names(
        vec![DataTypes::int(), DataTypes::string(), DataTypes::bigint()],
        vec!["id", "name", "age"],
    );

    // Error: non-existent field name should throw exception
    let result = row_type.project_with_field_names(&["nonexistent".to_string()]);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Field 'nonexistent' does not exist in the row type")
    );

    // Mixed existing and non-existing: should also error on the first non-existent field
    let result = row_type.project_with_field_names(&["id".to_string(), "nonexistent".to_string()]);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Field 'nonexistent' does not exist in the row type")
    );
}

#[test]
fn test_row_type_project_duplicate_indices() {
    // Create a 3-column row type
    let row_type = RowType::with_data_types_and_field_names(
        vec![DataTypes::int(), DataTypes::string(), DataTypes::bigint()],
        vec!["id", "name", "age"],
    );

    // Projection with duplicate indices: [0, 0, 1]
    // This documents the expected behavior - duplicates are allowed
    let projected = row_type.project(&[0, 0, 1]).unwrap();
    assert_eq!(projected.fields().len(), 3);
    assert_eq!(projected.fields()[0].name, "id");
    assert_eq!(projected.fields()[1].name, "id");
    assert_eq!(projected.fields()[2].name, "name");
}

#[cfg(test)]
mod eq_ignore_nullable_tests {
    use super::*;

    #[test]
    fn ignores_nullability_at_top_level() {
        let nullable = DataType::Int(IntType::new());
        let non_nullable = DataType::Int(IntType::with_nullable(false));
        assert_ne!(nullable, non_nullable, "PartialEq still distinguishes");
        assert!(nullable.eq_ignore_nullable(&non_nullable));
        assert!(non_nullable.eq_ignore_nullable(&nullable));
    }

    #[test]
    fn rejects_different_kinds() {
        assert!(
            !DataType::Int(IntType::new()).eq_ignore_nullable(&DataType::BigInt(BigIntType::new()))
        );
    }

    #[test]
    fn compares_parameterized_types() {
        // Char length must match.
        assert!(
            DataType::Char(CharType::with_nullable(10, true))
                .eq_ignore_nullable(&DataType::Char(CharType::with_nullable(10, false)))
        );
        assert!(
            !DataType::Char(CharType::with_nullable(10, true))
                .eq_ignore_nullable(&DataType::Char(CharType::with_nullable(11, true)))
        );

        // Decimal precision + scale must match.
        let a = DataType::Decimal(DecimalType::with_nullable(true, 10, 2).unwrap());
        let b = DataType::Decimal(DecimalType::with_nullable(false, 10, 2).unwrap());
        let c = DataType::Decimal(DecimalType::with_nullable(true, 10, 3).unwrap());
        assert!(a.eq_ignore_nullable(&b));
        assert!(!a.eq_ignore_nullable(&c));
    }

    #[test]
    fn recurses_into_array_and_map() {
        // Array<Int NULL> ~ Array<Int NOT NULL>
        let a = DataType::Array(ArrayType::with_nullable(
            true,
            DataType::Int(IntType::new()),
        ));
        let b = DataType::Array(ArrayType::with_nullable(
            false,
            DataType::Int(IntType::with_nullable(false)),
        ));
        assert!(a.eq_ignore_nullable(&b));

        // Map<String, Int> on both sides, mixed nullability.
        let m1 = DataType::Map(MapType::with_nullable(
            true,
            DataType::String(StringType::new()),
            DataType::Int(IntType::new()),
        ));
        let m2 = DataType::Map(MapType::with_nullable(
            false,
            DataType::String(StringType::with_nullable(false)),
            DataType::Int(IntType::with_nullable(false)),
        ));
        assert!(m1.eq_ignore_nullable(&m2));

        // Map element-type mismatch is still caught.
        let m3 = DataType::Map(MapType::with_nullable(
            true,
            DataType::String(StringType::new()),
            DataType::BigInt(BigIntType::new()),
        ));
        assert!(!m1.eq_ignore_nullable(&m3));
    }

    #[test]
    fn recurses_into_row_fields() {
        let r1 = DataType::Row(RowType::new(vec![
            DataField::new("a", DataType::Int(IntType::new()), None),
            DataField::new("b", DataType::String(StringType::new()), None),
        ]));
        let r2 = DataType::Row(RowType::with_nullable(
            false,
            vec![
                DataField::new("a", DataType::Int(IntType::with_nullable(false)), None),
                DataField::new(
                    "b",
                    DataType::String(StringType::with_nullable(false)),
                    None,
                ),
            ],
        ));
        assert!(r1.eq_ignore_nullable(&r2));

        // Field name mismatch must fail.
        let r3 = DataType::Row(RowType::new(vec![
            DataField::new("renamed_a", DataType::Int(IntType::new()), None),
            DataField::new("b", DataType::String(StringType::new()), None),
        ]));
        assert!(!r1.eq_ignore_nullable(&r3));
    }
}
