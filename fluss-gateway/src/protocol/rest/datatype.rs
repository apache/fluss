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

//! REST data types using Fluss's persisted structured vocabulary.

use crate::error::{GatewayError, GatewayResult};
use fluss::metadata::{
    ArrayType, BigIntType, BinaryType, BooleanType, BytesType, CharType, DataField, DataType,
    DateType, DecimalType, DoubleType, FloatType, IntType, MapType, RowType, SmallIntType,
    StringType, TimeType, TimestampLTzType, TimestampType, TinyIntType,
};
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::openapi::{RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

const MAX_TYPE_NESTING: usize = 64;

/// The exact recursive Fluss type as it appears on the wire.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE", deny_unknown_fields)]
#[schema(no_recursion)]
pub enum WireDataType {
    Boolean {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    #[serde(rename = "TINYINT")]
    TinyInt {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    #[serde(rename = "SMALLINT")]
    SmallInt {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    #[serde(rename = "INTEGER")]
    Int {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    #[serde(rename = "BIGINT")]
    BigInt {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    Float {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    Double {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    Char {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
        length: u32,
    },
    String {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    Decimal {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
        #[schema(minimum = 1, maximum = 38)]
        precision: u32,
        #[schema(maximum = 38)]
        scale: u32,
    },
    Date {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    #[serde(rename = "TIME_WITHOUT_TIME_ZONE")]
    Time {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
        #[schema(maximum = 9)]
        precision: u32,
    },
    #[serde(rename = "TIMESTAMP_WITHOUT_TIME_ZONE")]
    Timestamp {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
        #[schema(maximum = 9)]
        precision: u32,
    },
    #[serde(rename = "TIMESTAMP_WITH_LOCAL_TIME_ZONE")]
    TimestampLtz {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
        #[schema(maximum = 9)]
        precision: u32,
    },
    Bytes {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
    },
    Binary {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
        length: usize,
    },
    Array {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
        element_type: Box<WireDataType>,
    },
    Map {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
        key_type: Box<WireDataType>,
        value_type: Box<WireDataType>,
    },
    Row {
        #[serde(default = "default_nullable", skip_serializing_if = "is_true")]
        nullable: bool,
        fields: Vec<WireRowField>,
    },
}

fn default_nullable() -> bool {
    true
}

fn is_true(value: &bool) -> bool {
    *value
}

/// One `ROW` field. Server-assigned field IDs are not exposed.
#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WireRowField {
    pub name: String,
    #[schema(no_recursion)]
    pub field_type: WireDataType,
    #[serde(
        default,
        deserialize_with = "optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    #[schema(nullable = false)]
    pub description: Option<String>,
}

/// Missing fields default to `None`; present fields must contain a non-null value.
fn optional_non_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}

impl WireDataType {
    /// Replaces only the top-level nullability.
    pub fn with_root_nullable(mut self, root: bool) -> Self {
        let nullable = match &mut self {
            Self::Boolean { nullable }
            | Self::TinyInt { nullable }
            | Self::SmallInt { nullable }
            | Self::Int { nullable }
            | Self::BigInt { nullable }
            | Self::Float { nullable }
            | Self::Double { nullable }
            | Self::String { nullable }
            | Self::Date { nullable }
            | Self::Bytes { nullable }
            | Self::Char { nullable, .. }
            | Self::Decimal { nullable, .. }
            | Self::Time { nullable, .. }
            | Self::Timestamp { nullable, .. }
            | Self::TimestampLtz { nullable, .. }
            | Self::Binary { nullable, .. }
            | Self::Array { nullable, .. }
            | Self::Map { nullable, .. }
            | Self::Row { nullable, .. } => nullable,
        };
        *nullable = root;
        self
    }
}

impl From<&DataType> for WireDataType {
    /// Renders the native type, keeping nullability at every level.
    fn from(data_type: &DataType) -> Self {
        let nullable = data_type.is_nullable();
        match data_type {
            DataType::Boolean(_) => Self::Boolean { nullable },
            DataType::TinyInt(_) => Self::TinyInt { nullable },
            DataType::SmallInt(_) => Self::SmallInt { nullable },
            DataType::Int(_) => Self::Int { nullable },
            DataType::BigInt(_) => Self::BigInt { nullable },
            DataType::Float(_) => Self::Float { nullable },
            DataType::Double(_) => Self::Double { nullable },
            DataType::Char(value) => Self::Char {
                nullable,
                length: value.length(),
            },
            DataType::String(_) => Self::String { nullable },
            DataType::Decimal(value) => Self::Decimal {
                nullable,
                precision: value.precision(),
                scale: value.scale(),
            },
            DataType::Date(_) => Self::Date { nullable },
            DataType::Time(value) => Self::Time {
                nullable,
                precision: value.precision(),
            },
            DataType::Timestamp(value) => Self::Timestamp {
                nullable,
                precision: value.precision(),
            },
            DataType::TimestampLTz(value) => Self::TimestampLtz {
                nullable,
                precision: value.precision(),
            },
            DataType::Bytes(_) => Self::Bytes { nullable },
            DataType::Binary(value) => Self::Binary {
                nullable,
                length: value.length(),
            },
            DataType::Array(value) => Self::Array {
                nullable,
                element_type: Box::new(Self::from(value.get_element_type())),
            },
            DataType::Map(value) => Self::Map {
                nullable,
                key_type: Box::new(Self::from(value.key_type())),
                value_type: Box::new(Self::from(value.value_type())),
            },
            DataType::Row(value) => Self::Row {
                nullable,
                fields: value.fields().iter().map(WireRowField::from).collect(),
            },
        }
    }
}

impl From<&DataField> for WireRowField {
    /// Drops the field ID, which the server assigns and the API never exposes.
    fn from(field: &DataField) -> Self {
        Self {
            name: field.name.clone(),
            field_type: WireDataType::from(&field.data_type),
            description: field.description.clone(),
        }
    }
}

impl TryFrom<WireDataType> for DataType {
    type Error = GatewayError;

    /// Builds the native type using its constructors.
    fn try_from(data_type: WireDataType) -> Result<Self, Self::Error> {
        to_native(data_type, 0)
    }
}

/// Builds the native tree within the gateway's nesting limit.
fn to_native(data_type: WireDataType, depth: usize) -> GatewayResult<DataType> {
    if depth > MAX_TYPE_NESTING {
        return Err(GatewayError::invalid_argument(format!(
            "the data type nests deeper than {MAX_TYPE_NESTING} levels"
        )));
    }
    // TODO: Delegate length and ROW field validation once fluss-rs supports it.
    let converted = match data_type {
        WireDataType::Boolean { nullable } => {
            DataType::Boolean(BooleanType::with_nullable(nullable))
        }
        WireDataType::TinyInt { nullable } => {
            DataType::TinyInt(TinyIntType::with_nullable(nullable))
        }
        WireDataType::SmallInt { nullable } => {
            DataType::SmallInt(SmallIntType::with_nullable(nullable))
        }
        WireDataType::Int { nullable } => DataType::Int(IntType::with_nullable(nullable)),
        WireDataType::BigInt { nullable } => DataType::BigInt(BigIntType::with_nullable(nullable)),
        WireDataType::Float { nullable } => DataType::Float(FloatType::with_nullable(nullable)),
        WireDataType::Double { nullable } => DataType::Double(DoubleType::with_nullable(nullable)),
        WireDataType::Char { nullable, length } => {
            DataType::Char(CharType::with_nullable(length, nullable))
        }
        WireDataType::String { nullable } => DataType::String(StringType::with_nullable(nullable)),
        WireDataType::Decimal {
            nullable,
            precision,
            scale,
        } => DataType::Decimal(
            DecimalType::with_nullable(nullable, precision, scale).map_err(invalid_type)?,
        ),
        WireDataType::Date { nullable } => DataType::Date(DateType::with_nullable(nullable)),
        WireDataType::Time {
            nullable,
            precision,
        } => DataType::Time(TimeType::with_nullable(nullable, precision).map_err(invalid_type)?),
        WireDataType::Timestamp {
            nullable,
            precision,
        } => DataType::Timestamp(
            TimestampType::with_nullable(nullable, precision).map_err(invalid_type)?,
        ),
        WireDataType::TimestampLtz {
            nullable,
            precision,
        } => DataType::TimestampLTz(
            TimestampLTzType::with_nullable(nullable, precision).map_err(invalid_type)?,
        ),
        WireDataType::Bytes { nullable } => DataType::Bytes(BytesType::with_nullable(nullable)),
        WireDataType::Binary { nullable, length } => {
            DataType::Binary(BinaryType::with_nullable(nullable, length))
        }
        WireDataType::Array {
            nullable,
            element_type,
        } => DataType::Array(ArrayType::with_nullable(
            nullable,
            to_native(*element_type, depth + 1)?,
        )),
        WireDataType::Map {
            nullable,
            key_type,
            value_type,
        } => DataType::Map(MapType::with_nullable(
            nullable,
            to_native(*key_type, depth + 1)?,
            to_native(*value_type, depth + 1)?,
        )),
        WireDataType::Row { nullable, fields } => DataType::Row(RowType::with_nullable(
            nullable,
            fields
                .into_iter()
                .map(|field| {
                    Ok(DataField::new(
                        field.name,
                        to_native(field.field_type, depth + 1)?,
                        field.description,
                    ))
                })
                .collect::<GatewayResult<Vec<_>>>()?,
        )),
    };
    Ok(converted)
}

/// A type parameter the native constructor refused, which came from a caller's body.
fn invalid_type(error: fluss::error::Error) -> GatewayError {
    GatewayError::invalid_argument(format!("invalid data type: {error}"))
}

/// A column type without top-level `nullable`; nested nullability is unchanged.
#[derive(Debug)]
pub struct ColumnDataType(pub WireDataType);

impl PartialSchema for ColumnDataType {
    fn schema() -> RefOr<Schema> {
        let mut schema = WireDataType::schema();
        if let RefOr::T(Schema::OneOf(one_of)) = &mut schema {
            one_of.description = Some(
                "A Fluss column type; top-level nullability is declared by the column.".to_string(),
            );
            for item in &mut one_of.items {
                if let RefOr::T(Schema::Object(object)) = item {
                    object.properties.remove("nullable");
                }
            }
        }
        schema
    }
}

impl ToSchema for ColumnDataType {}

impl<'de> Deserialize<'de> for ColumnDataType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut value = serde_json::Value::deserialize(deserializer)?;
        if value.get("nullable").is_some() {
            return Err(serde::de::Error::custom(
                "a column's nullability is expressed by the column-level `nullable` field; \
                 `nullable` at the top level of `data_type` is not allowed",
            ));
        }
        normalize_type_names(&mut value);
        serde_json::from_value(value)
            .map(ColumnDataType)
            .map_err(serde::de::Error::custom)
    }
}

/// Canonicalizes only type tags, including nested types; field names and descriptions stay intact.
fn normalize_type_names(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(serde_json::Value::String(name)) = object.get_mut("type") {
                name.make_ascii_uppercase();
            }
            for value in object.values_mut() {
                normalize_type_names(value);
            }
        }
        serde_json::Value::Array(array) => {
            for value in array {
                normalize_type_names(value);
            }
        }
        _ => {}
    }
}

impl Serialize for ColumnDataType {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut value = serde_json::to_value(&self.0).map_err(serde::ser::Error::custom)?;
        if let Some(object) = value.as_object_mut() {
            object.remove("nullable");
        }
        value.serialize(serializer)
    }
}

impl From<&DataType> for ColumnDataType {
    fn from(data_type: &DataType) -> Self {
        Self(WireDataType::from(data_type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;
    use serde_json::json;

    fn parse(value: serde_json::Value) -> Result<DataType, String> {
        let wire: WireDataType =
            serde_json::from_value(value).map_err(|error| error.to_string())?;
        DataType::try_from(wire).map_err(|error| error.message().to_string())
    }

    fn render(data_type: &DataType) -> serde_json::Value {
        serde_json::to_value(WireDataType::from(data_type)).expect("the type renders")
    }

    #[test]
    fn every_type_round_trips_through_the_wire_shape() {
        let bodies = [
            json!({"type": "BOOLEAN"}),
            json!({"type": "TINYINT", "nullable": false}),
            json!({"type": "SMALLINT"}),
            json!({"type": "INTEGER", "nullable": false}),
            json!({"type": "BIGINT"}),
            json!({"type": "FLOAT", "nullable": false}),
            json!({"type": "DOUBLE"}),
            json!({"type": "CHAR", "nullable": false, "length": 3}),
            json!({"type": "STRING"}),
            json!({"type": "DECIMAL", "nullable": false, "precision": 18, "scale": 2}),
            json!({"type": "DATE"}),
            json!({"type": "TIME_WITHOUT_TIME_ZONE", "nullable": false, "precision": 9}),
            json!({"type": "TIMESTAMP_WITHOUT_TIME_ZONE", "precision": 6}),
            json!({"type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "nullable": false, "precision": 9}),
            json!({"type": "BYTES"}),
            json!({"type": "BINARY", "nullable": false, "length": 16}),
            json!({"type": "ARRAY", "element_type": {"type": "STRING", "nullable": false}}),
            json!({"type": "MAP", "nullable": false, "key_type": {"type": "STRING", "nullable": false}, "value_type": {"type": "INTEGER"}}),
            json!({"type": "ROW", "fields": [
                {"name": "inner", "field_type": {"type": "INTEGER", "nullable": false}, "description": "a nested field"},
                {"name": "plain", "field_type": {"type": "STRING"}}
            ]}),
        ];

        for body in bodies {
            let native = parse(body.clone()).expect("the body parses");
            assert_eq!(render(&native), body, "{body}");
            for nullable in [true, false] {
                let wire = WireDataType::from(&native).with_root_nullable(nullable);
                let mut expected = body.clone();
                if nullable {
                    expected.as_object_mut().unwrap().remove("nullable");
                } else {
                    expected["nullable"] = json!(false);
                }
                assert_eq!(serde_json::to_value(wire).unwrap(), expected, "{body}");
            }
        }
    }

    #[test]
    fn type_names_are_case_insensitive_at_the_rest_boundary() {
        for (body, expected) in [
            (json!({"type": "bigint"}), json!({"type": "BIGINT"})),
            (
                json!({"type": "aRrAy", "element_type": {
                    "type": "mAp", "key_type": {"type": "sTrIng"}, "value_type": {
                        "type": "rOw", "fields": [{
                            "name": "mixedCase", "description": "Keep this case",
                            "field_type": {"type": "iNtEgEr", "nullable": false}
                        }]
                    }
                }}),
                json!({"type": "ARRAY", "element_type": {
                    "type": "MAP", "key_type": {"type": "STRING"}, "value_type": {
                        "type": "ROW", "fields": [{
                            "name": "mixedCase", "description": "Keep this case",
                            "field_type": {"type": "INTEGER", "nullable": false}
                        }]
                    }
                }}),
            ),
        ] {
            let column: ColumnDataType = serde_json::from_value(body).expect("the type parses");
            assert_eq!(serde_json::to_value(column).unwrap(), expected);
        }
    }

    #[test]
    fn native_nullability_defaults_are_preserved() {
        let native = parse(json!({"type": "ARRAY", "element_type": {"type": "INTEGER"}}))
            .expect("the type parses");
        assert_eq!(
            render(&native),
            json!({"type": "ARRAY", "element_type": {"type": "INTEGER"}})
        );

        let native = parse(json!({
            "type": "MAP",
            "key_type": {"type": "STRING"},
            "value_type": {"type": "INTEGER"}
        }))
        .expect("the type parses");
        assert_eq!(
            render(&native),
            json!({
                "type": "MAP",
                "key_type": {"type": "STRING", "nullable": false},
                "value_type": {"type": "INTEGER"}
            })
        );
    }

    #[test]
    fn a_type_that_is_not_exactly_described_is_refused() {
        for body in [
            json!({}),
            json!({"type": "NUMBER"}),
            json!({"type": "INT"}),
            json!({"type": "DECIMAL", "precision": 10}),
            json!({"type": "CHAR"}),
            json!({"type": "BIGINT", "precision": 3}),
            json!({"type": "BIGINT", "precision": null}),
            json!({"type": "DECIMAL", "precision": 10, "scale": 2, "length": 4}),
            json!({"type": "ARRAY"}),
            json!({"type": "MAP", "key_type": {"type": "STRING"}}),
            json!({"type": "ROW"}),
            json!({"type": "STRING", "unexpected": 1}),
            json!({"type": "CHAR", "length": null}),
            json!({"type": "CHAR", "length": u64::from(u32::MAX) + 1}),
            json!({"type": "BINARY", "length": -1}),
            json!({"type": "DECIMAL", "precision": null, "scale": 0}),
            json!({"type": "DECIMAL", "precision": 10, "scale": null}),
            json!({"type": "ARRAY", "element_type": null}),
            json!({"type": "MAP", "key_type": null, "value_type": {"type": "STRING"}}),
            json!({"type": "MAP", "key_type": {"type": "STRING"}, "value_type": null}),
            json!({"type": "ROW", "fields": null}),
            json!({"type": "ARRAY", "element_type": {"type": "STRING", "nullable": null}}),
            json!({"type": "ROW", "fields": [{"name": "id", "field_type": {"type": "INTEGER"}, "description": null}]}),
            json!({"type": "ROW", "fields": [{"name": "id", "field_type": {"type": "INTEGER"}, "field_id": 1}]}),
        ] {
            assert!(parse(body.clone()).is_err(), "the body is refused: {body}");
        }
    }

    #[test]
    fn native_constructors_and_nesting_limit_define_validation() {
        for body in [
            json!({"type": "DECIMAL", "precision": 0, "scale": 0}),
            json!({"type": "DECIMAL", "precision": 2, "scale": 3}),
            json!({"type": "TIME_WITHOUT_TIME_ZONE", "precision": 10}),
            json!({"type": "TIMESTAMP_WITHOUT_TIME_ZONE", "precision": 10}),
            json!({"type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "precision": 10}),
        ] {
            let wire: WireDataType =
                serde_json::from_value(body.clone()).expect("the shape parses");
            assert_eq!(
                DataType::try_from(wire)
                    .expect_err("the value is refused")
                    .kind(),
                ErrorKind::InvalidArgument,
                "{body}"
            );
        }

        for length in [0, 1, u32::MAX] {
            for (type_name, native) in [
                ("CHAR", DataType::Char(CharType::new(length))),
                ("BINARY", DataType::Binary(BinaryType::new(length as usize))),
            ] {
                let body = json!({"type": type_name, "length": length});
                assert_eq!(parse(body.clone()).unwrap(), native);
                assert_eq!(render(&native), body);
            }
        }

        let fields = ["", "a\nb", "id", "id"]
            .into_iter()
            .map(|name| DataField::new(name, DataType::Int(IntType::new()), None))
            .collect();
        let native = DataType::Row(RowType::new(fields));
        assert_eq!(parse(render(&native)).unwrap(), native);

        for depth in [MAX_TYPE_NESTING, MAX_TYPE_NESTING + 1] {
            let mut wire = WireDataType::Int { nullable: true };
            for _ in 0..depth {
                wire = WireDataType::Array {
                    nullable: true,
                    element_type: Box::new(wire),
                };
            }
            let result = DataType::try_from(wire);
            if depth == MAX_TYPE_NESTING {
                assert!(result.is_ok(), "the maximum nesting is accepted");
            } else {
                let error = result.expect_err("excessive nesting is refused");
                assert_eq!(error.kind(), ErrorKind::InvalidArgument);
                assert!(error.message().contains("nests deeper"));
            }
        }
    }

    #[test]
    fn a_column_type_neither_accepts_nor_emits_a_top_level_nullability() {
        for nullable in [json!(true), json!(false), json!(null)] {
            let error = serde_json::from_value::<ColumnDataType>(
                json!({"type": "INTEGER", "nullable": nullable}),
            )
            .expect_err("a top-level nullability is refused");
            assert!(error.to_string().contains("column-level `nullable`"));
        }

        let column =
            ColumnDataType::from(&parse(json!({"type": "INTEGER", "nullable": false})).unwrap());
        assert_eq!(
            serde_json::to_value(&column).expect("the column type renders"),
            json!({"type": "INTEGER"})
        );

        let nested = ColumnDataType::from(
            &parse(json!({"type": "ARRAY", "nullable": false, "element_type": {"type": "INTEGER", "nullable": false}}))
                .unwrap(),
        );
        assert_eq!(
            serde_json::to_value(&nested).expect("the column type renders"),
            json!({"type": "ARRAY", "element_type": {"type": "INTEGER", "nullable": false}})
        );
    }
}
