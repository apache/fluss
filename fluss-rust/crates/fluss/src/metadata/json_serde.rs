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

use crate::error::Error::JsonSerdeError;
use crate::error::{Error, Result};
use crate::metadata::datatype::{
    DataField, DataType, DataTypes, DecimalType, TimeType, TimestampLTzType, TimestampType,
    UNASSIGNED_FIELD_ID,
};
use crate::metadata::table::{Column, Schema, TableDescriptor};
use serde_json::{Value, json};
use std::collections::HashMap;

pub trait JsonSerde: Sized {
    fn serialize_json(&self) -> Result<Value>;

    fn deserialize_json(node: &Value) -> Result<Self>;
}

impl DataType {
    pub fn to_type_root(&self) -> &str {
        match &self {
            DataType::Boolean(_) => "BOOLEAN",
            DataType::TinyInt(_) => "TINYINT",
            DataType::SmallInt(_) => "SMALLINT",
            DataType::Int(_) => "INTEGER",
            DataType::BigInt(_) => "BIGINT",
            DataType::Float(_) => "FLOAT",
            DataType::Double(_) => "DOUBLE",
            DataType::Char(_) => "CHAR",
            DataType::String(_) => "STRING",
            DataType::Decimal(_) => "DECIMAL",
            DataType::Date(_) => "DATE",
            DataType::Time(_) => "TIME_WITHOUT_TIME_ZONE",
            DataType::Timestamp(_) => "TIMESTAMP_WITHOUT_TIME_ZONE",
            DataType::TimestampLTz(_) => "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
            DataType::Bytes(_) => "BYTES",
            DataType::Binary(_) => "BINARY",
            DataType::Array(_) => "ARRAY",
            DataType::Map(_) => "MAP",
            DataType::Row(_) => "ROW",
        }
    }
}

impl DataType {
    const FIELD_NAME_TYPE_NAME: &'static str = "type";
    const FIELD_NAME_NULLABLE: &'static str = "nullable";
    const FIELD_NAME_LENGTH: &'static str = "length";
    const FIELD_NAME_PRECISION: &'static str = "precision";
    const FIELD_NAME_SCALE: &'static str = "scale";
    #[allow(dead_code)]
    const FIELD_NAME_ELEMENT_TYPE: &'static str = "element_type";
    #[allow(dead_code)]
    const FIELD_NAME_KEY_TYPE: &'static str = "key_type";
    #[allow(dead_code)]
    const FIELD_NAME_VALUE_TYPE: &'static str = "value_type";
    #[allow(dead_code)]
    const FIELD_NAME_FIELDS: &'static str = "fields";
    #[allow(dead_code)]
    const FIELD_NAME_FIELD_NAME: &'static str = "name";
    // ROW
    #[allow(dead_code)]
    const FIELD_NAME_FIELD_TYPE: &'static str = "field_type";
    #[allow(dead_code)]
    const FIELD_NAME_FIELD_DESCRIPTION: &'static str = "description";
}

impl JsonSerde for DataType {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        obj.insert(
            Self::FIELD_NAME_TYPE_NAME.to_string(),
            json!(Self::to_type_root(self)),
        );
        if !self.is_nullable() {
            obj.insert(Self::FIELD_NAME_NULLABLE.to_string(), json!(false));
        }

        match &self {
            DataType::Boolean(_)
            | DataType::TinyInt(_)
            | DataType::SmallInt(_)
            | DataType::Int(_)
            | DataType::BigInt(_)
            | DataType::Float(_)
            | DataType::Double(_)
            | DataType::String(_)
            | DataType::Bytes(_)
            | DataType::Date(_) => {
                // do nothing
            }
            DataType::Char(_type) => {
                obj.insert(Self::FIELD_NAME_LENGTH.to_string(), json!(_type.length()));
            }
            DataType::Binary(_type) => {
                obj.insert(Self::FIELD_NAME_LENGTH.to_string(), json!(_type.length()));
            }
            DataType::Decimal(_type) => {
                obj.insert(
                    Self::FIELD_NAME_PRECISION.to_string(),
                    json!(_type.precision()),
                );
                obj.insert(Self::FIELD_NAME_SCALE.to_string(), json!(_type.scale()));
            }
            DataType::Time(_type) => {
                obj.insert(
                    Self::FIELD_NAME_PRECISION.to_string(),
                    json!(_type.precision()),
                );
            }
            DataType::Timestamp(_type) => {
                obj.insert(
                    Self::FIELD_NAME_PRECISION.to_string(),
                    json!(_type.precision()),
                );
            }
            DataType::TimestampLTz(_type) => {
                obj.insert(
                    Self::FIELD_NAME_PRECISION.to_string(),
                    json!(_type.precision()),
                );
            }
            DataType::Array(_type) => {
                obj.insert(
                    Self::FIELD_NAME_ELEMENT_TYPE.to_string(),
                    _type.get_element_type().serialize_json()?,
                );
            }
            DataType::Map(_type) => {
                obj.insert(
                    Self::FIELD_NAME_KEY_TYPE.to_string(),
                    _type.key_type().serialize_json()?,
                );
                obj.insert(
                    Self::FIELD_NAME_VALUE_TYPE.to_string(),
                    _type.value_type().serialize_json()?,
                );
            }
            DataType::Row(_type) => {
                let fields: Vec<Value> = _type
                    .fields()
                    .iter()
                    .map(|field| field.serialize_json())
                    .collect::<Result<_>>()?;
                obj.insert(Self::FIELD_NAME_FIELDS.to_string(), json!(fields));
            }
        }
        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<Self> {
        let mut _is_nullable = true;
        let type_root = node
            .get(Self::FIELD_NAME_TYPE_NAME)
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::JsonSerdeError {
                message: format!(
                    "Couldn't find field {} while deserializing datatype.",
                    Self::FIELD_NAME_TYPE_NAME
                ),
            })?;

        let mut data_type = match type_root {
            "BOOLEAN" => DataTypes::boolean(),
            "TINYINT" => DataTypes::tinyint(),
            "SMALLINT" => DataTypes::smallint(),
            "INTEGER" => DataTypes::int(),
            "BIGINT" => DataTypes::bigint(),
            "FLOAT" => DataTypes::float(),
            "DOUBLE" => DataTypes::double(),
            "CHAR" => {
                let length = node
                    .get(Self::FIELD_NAME_LENGTH)
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| Error::JsonSerdeError {
                        message: format!("Missing required field: {}", Self::FIELD_NAME_LENGTH),
                    })? as u32;
                DataTypes::char(length)
            }
            "STRING" => DataTypes::string(),
            "DECIMAL" => {
                let precision = node
                    .get(Self::FIELD_NAME_PRECISION)
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| Error::JsonSerdeError {
                        message: format!("Missing required field: {}", Self::FIELD_NAME_PRECISION),
                    })? as u32;
                let scale = node
                    .get(Self::FIELD_NAME_SCALE)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32;
                DataType::Decimal(DecimalType::with_nullable(true, precision, scale).map_err(
                    |e| Error::JsonSerdeError {
                        message: format!("Invalid DECIMAL parameters: {e}"),
                    },
                )?)
            }
            "DATE" => DataTypes::date(),
            "TIME_WITHOUT_TIME_ZONE" => {
                let precision = node
                    .get(Self::FIELD_NAME_PRECISION)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0) as u32;
                DataType::Time(TimeType::with_nullable(true, precision).map_err(|e| {
                    Error::JsonSerdeError {
                        message: format!("Invalid TIME_WITHOUT_TIME_ZONE precision: {e}"),
                    }
                })?)
            }
            "TIMESTAMP_WITHOUT_TIME_ZONE" => {
                let precision = node
                    .get(Self::FIELD_NAME_PRECISION)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(6) as u32;
                DataType::Timestamp(TimestampType::with_nullable(true, precision).map_err(|e| {
                    Error::JsonSerdeError {
                        message: format!("Invalid TIMESTAMP_WITHOUT_TIME_ZONE precision: {e}"),
                    }
                })?)
            }
            "TIMESTAMP_WITH_LOCAL_TIME_ZONE" => {
                let precision = node
                    .get(Self::FIELD_NAME_PRECISION)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(6) as u32;
                DataType::TimestampLTz(TimestampLTzType::with_nullable(true, precision).map_err(
                    |e| Error::JsonSerdeError {
                        message: format!("Invalid TIMESTAMP_WITH_LOCAL_TIME_ZONE precision: {e}"),
                    },
                )?)
            }
            "BYTES" => DataTypes::bytes(),
            "BINARY" => {
                let length = node
                    .get(Self::FIELD_NAME_LENGTH)
                    .and_then(|v| v.as_u64())
                    .unwrap_or(1) as usize;
                DataTypes::binary(length)
            }
            "ARRAY" => {
                let element_type_node =
                    node.get(Self::FIELD_NAME_ELEMENT_TYPE).ok_or_else(|| {
                        Error::JsonSerdeError {
                            message: format!(
                                "Missing required field: {}",
                                Self::FIELD_NAME_ELEMENT_TYPE
                            ),
                        }
                    })?;
                let element_type = DataType::deserialize_json(element_type_node)?;
                DataTypes::array(element_type)
            }
            "MAP" => {
                let key_type_node =
                    node.get(Self::FIELD_NAME_KEY_TYPE)
                        .ok_or_else(|| Error::JsonSerdeError {
                            message: format!(
                                "Missing required field: {}",
                                Self::FIELD_NAME_KEY_TYPE
                            ),
                        })?;
                let key_type = DataType::deserialize_json(key_type_node)?;
                let value_type_node =
                    node.get(Self::FIELD_NAME_VALUE_TYPE)
                        .ok_or_else(|| Error::JsonSerdeError {
                            message: format!(
                                "Missing required field: {}",
                                Self::FIELD_NAME_VALUE_TYPE
                            ),
                        })?;
                let value_type = DataType::deserialize_json(value_type_node)?;
                DataTypes::map(key_type, value_type)
            }
            "ROW" => {
                let fields_node = node
                    .get(Self::FIELD_NAME_FIELDS)
                    .ok_or_else(|| Error::JsonSerdeError {
                        message: format!("Missing required field: {}", Self::FIELD_NAME_FIELDS),
                    })?
                    .as_array()
                    .ok_or_else(|| Error::JsonSerdeError {
                        message: format!("{} must be an array", Self::FIELD_NAME_FIELDS),
                    })?;
                let mut fields = Vec::with_capacity(fields_node.len());
                for field_node in fields_node {
                    fields.push(DataField::deserialize_json(field_node)?);
                }
                DataTypes::row(fields)
            }
            _ => {
                return Err(Error::JsonSerdeError {
                    message: format!("Unknown type root: {type_root}"),
                });
            }
        };

        if let Some(nullable) = node.get(Self::FIELD_NAME_NULLABLE) {
            let nullable_value = nullable.as_bool().unwrap_or(true);
            if !nullable_value {
                data_type = data_type.as_non_nullable();
            }
        }
        Ok(data_type)
    }
}

impl DataField {
    const NAME: &'static str = "name";
    const FIELD_TYPE: &'static str = "field_type";
    const DESCRIPTION: &'static str = "description";
    const FIELD_ID: &'static str = "field_id";
}

impl JsonSerde for DataField {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        obj.insert(Self::NAME.to_string(), json!(self.name()));
        obj.insert(
            Self::FIELD_TYPE.to_string(),
            self.data_type.serialize_json()?,
        );

        if let Some(description) = &self.description {
            obj.insert(Self::DESCRIPTION.to_string(), json!(description));
        }

        obj.insert(Self::FIELD_ID.to_string(), json!(self.field_id()));

        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<DataField> {
        let name = node
            .get(Self::NAME)
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::JsonSerdeError {
                message: format!("Missing required field: {}", Self::NAME),
            })?
            .to_string();

        let field_type_node = node
            .get(Self::FIELD_TYPE)
            .ok_or_else(|| Error::JsonSerdeError {
                message: format!("Missing required field: {}", Self::FIELD_TYPE),
            })?;

        let data_type = DataType::deserialize_json(field_type_node)?;

        let description = node
            .get(Self::DESCRIPTION)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let field_id = node
            .get(Self::FIELD_ID)
            .and_then(|v| v.as_i64())
            .map(|v| v as i32)
            .unwrap_or(UNASSIGNED_FIELD_ID);

        Ok(DataField::with_field_id(
            name,
            data_type,
            description,
            field_id,
        ))
    }
}

impl Column {
    const NAME: &'static str = "name";
    const DATA_TYPE: &'static str = "data_type";
    const COMMENT: &'static str = "comment";
    const ID: &'static str = "id";
}

impl JsonSerde for Column {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        // Common fields
        obj.insert(Self::NAME.to_string(), json!(self.name()));
        obj.insert(
            Self::DATA_TYPE.to_string(),
            self.data_type().serialize_json()?,
        );

        if let Some(comment) = &self.comment() {
            obj.insert(Self::COMMENT.to_string(), json!(comment));
        }

        // The Java client requires `id` on input.
        obj.insert(Self::ID.to_string(), json!(self.id()));

        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<Column> {
        let name = node
            .get(Self::NAME)
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::JsonSerdeError {
                message: format!("Missing required field: {}", Self::NAME),
            })?;

        let data_type_node = node
            .get(Self::DATA_TYPE)
            .ok_or_else(|| Error::JsonSerdeError {
                message: format!("Missing required field: {}", Self::DATA_TYPE),
            })?;

        let data_type = DataType::deserialize_json(data_type_node)?;

        let mut column = Column::new(name, data_type);

        if let Some(comment) = node.get(Self::COMMENT).and_then(|v| v.as_str()) {
            column = column.with_comment(comment);
        }

        // Pre-id JSON is treated as unassigned; SchemaBuilder will
        // auto-assign on build.
        if let Some(id) = node.get(Self::ID).and_then(|v| v.as_i64()) {
            let id = i32::try_from(id).map_err(|_| Error::JsonSerdeError {
                message: format!("Column id {id} does not fit in i32"),
            })?;
            column = column.with_id(id);
        }

        Ok(column)
    }
}

impl Schema {
    const COLUMNS_NAME: &'static str = "columns";
    const PRIMARY_KEY_NAME: &'static str = "primary_key";
    const HIGHEST_FIELD_ID: &'static str = "highest_field_id";
    const VERSION_KEY: &'static str = "version";
    const VERSION: u32 = 1;
}

impl JsonSerde for Schema {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        // Serialize version
        obj.insert(Self::VERSION_KEY.to_string(), json!(Self::VERSION));

        // Serialize columns
        let columns: Vec<Value> = self
            .columns()
            .iter()
            .map(|col| col.serialize_json())
            .collect::<Result<_>>()?;
        obj.insert(Self::COLUMNS_NAME.to_string(), json!(columns));

        // Serialize primary key if present
        if let Some(primary_key) = &self.primary_key() {
            let pk_values: Vec<Value> = primary_key
                .column_names()
                .iter()
                .map(|name| json!(name))
                .collect();
            obj.insert(Self::PRIMARY_KEY_NAME.to_string(), json!(pk_values));
        }

        obj.insert(
            Self::HIGHEST_FIELD_ID.to_string(),
            json!(self.highest_field_id()),
        );

        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<Schema> {
        let columns_node = node
            .get(Self::COLUMNS_NAME)
            .ok_or_else(|| Error::JsonSerdeError {
                message: format!("Missing required field: {}", Self::COLUMNS_NAME),
            })?
            .as_array()
            .ok_or_else(|| Error::JsonSerdeError {
                message: format!("{} must be an array", Self::COLUMNS_NAME),
            })?;

        let mut columns = Vec::with_capacity(columns_node.len());
        for col_node in columns_node {
            columns.push(Column::deserialize_json(col_node)?);
        }

        let mut schema_builder = Schema::builder().with_columns(columns);

        if let Some(pk_node) = node.get(Self::PRIMARY_KEY_NAME) {
            let pk_array = pk_node
                .as_array()
                .ok_or_else(|| Error::invalid_table("Primary key must be an array"))?;

            let mut primary_keys = Vec::with_capacity(pk_array.len());
            for name_node in pk_array {
                primary_keys.push(
                    name_node.as_str().ok_or_else(|| {
                        Error::invalid_table("Primary key element must be a string")
                    })?,
                );
            }

            schema_builder = schema_builder.primary_key(primary_keys);
        }

        schema_builder.build()
    }
}

impl TableDescriptor {
    const SCHEMA_NAME: &'static str = "schema";
    const COMMENT_NAME: &'static str = "comment";
    const PARTITION_KEY_NAME: &'static str = "partition_key";
    const BUCKET_KEY_NAME: &'static str = "bucket_key";
    const BUCKET_COUNT_NAME: &'static str = "bucket_count";
    const PROPERTIES_NAME: &'static str = "properties";
    const CUSTOM_PROPERTIES_NAME: &'static str = "custom_properties";
    const VERSION_KEY: &'static str = "version";
    const VERSION: u32 = 1;

    fn deserialize_properties(node: &Value) -> Result<HashMap<String, String>> {
        let obj = node.as_object().ok_or_else(|| Error::JsonSerdeError {
            message: "Properties must be an object".to_string(),
        })?;

        let mut properties = HashMap::with_capacity(obj.len());
        for (key, value) in obj {
            properties.insert(
                key.clone(),
                value
                    .as_str()
                    .ok_or_else(|| Error::JsonSerdeError {
                        message: "Property value must be a string".to_string(),
                    })?
                    .to_owned(),
            );
        }

        Ok(properties)
    }
}

impl JsonSerde for TableDescriptor {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        // Serialize version
        obj.insert(Self::VERSION_KEY.to_string(), json!(Self::VERSION));

        // Serialize schema
        obj.insert(
            Self::SCHEMA_NAME.to_string(),
            self.schema().serialize_json()?,
        );

        // Serialize comment if present
        if let Some(comment) = &self.comment() {
            obj.insert(Self::COMMENT_NAME.to_string(), json!(comment));
        }

        // Serialize partition keys
        let partition_keys: Vec<Value> =
            self.partition_keys().iter().map(|key| json!(key)).collect();
        obj.insert(Self::PARTITION_KEY_NAME.to_string(), json!(partition_keys));

        // Serialize table distribution if present
        if let Some(dist) = &self.table_distribution() {
            let bucket_keys: Vec<Value> = dist.bucket_keys().iter().map(|key| json!(key)).collect();
            obj.insert(Self::BUCKET_KEY_NAME.to_string(), json!(bucket_keys));

            if let Some(count) = dist.bucket_count() {
                obj.insert(Self::BUCKET_COUNT_NAME.to_string(), json!(count));
            }
        }

        // Serialize properties
        obj.insert(Self::PROPERTIES_NAME.to_string(), json!(self.properties()));

        obj.insert(
            Self::CUSTOM_PROPERTIES_NAME.to_string(),
            json!(self.custom_properties()),
        );

        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<Self> {
        let mut builder = TableDescriptor::builder();

        // Deserialize schema
        let schema_node = node.get(Self::SCHEMA_NAME).ok_or_else(|| JsonSerdeError {
            message: format!("Missing required field: {}", Self::SCHEMA_NAME),
        })?;
        let schema = Schema::deserialize_json(schema_node)?;
        builder = builder.schema(schema);

        // Deserialize comment if present
        if let Some(comment_node) = node.get(Self::COMMENT_NAME) {
            let comment = comment_node
                .as_str()
                .ok_or_else(|| JsonSerdeError {
                    message: format!("{} must be a string", Self::COMMENT_NAME),
                })?
                .to_owned();
            builder = builder.comment(comment.as_str());
        }

        let partition_node = node
            .get(Self::PARTITION_KEY_NAME)
            .ok_or_else(|| JsonSerdeError {
                message: format!("Missing required field: {}", Self::PARTITION_KEY_NAME),
            })?
            .as_array()
            .ok_or_else(|| JsonSerdeError {
                message: format!("{} must be an array", Self::PARTITION_KEY_NAME),
            })?;

        let mut partition_keys = Vec::with_capacity(partition_node.len());
        for key_node in partition_node {
            partition_keys.push(
                key_node
                    .as_str()
                    .ok_or_else(|| JsonSerdeError {
                        message: format!("{} element must be a string", Self::PARTITION_KEY_NAME),
                    })?
                    .to_owned(),
            );
        }
        builder = builder.partitioned_by(partition_keys);

        let mut bucket_count = None;
        let mut bucket_keys = vec![];
        if let Some(bucket_key_node) = node.get(Self::BUCKET_KEY_NAME) {
            let bucket_key_node = bucket_key_node.as_array().ok_or_else(|| JsonSerdeError {
                message: format!("{} must be an array", Self::BUCKET_KEY_NAME),
            })?;

            for key_node in bucket_key_node {
                bucket_keys.push(
                    key_node
                        .as_str()
                        .ok_or_else(|| JsonSerdeError {
                            message: "Bucket key must be a string".to_string(),
                        })?
                        .to_owned(),
                );
            }
        }

        if let Some(bucket_count_node) = node.get(Self::BUCKET_COUNT_NAME) {
            bucket_count = bucket_count_node.as_u64().map(|n| n as i32);
        }

        if bucket_count.is_some() || !bucket_keys.is_empty() {
            builder = builder.distributed_by(bucket_count, bucket_keys);
        }

        // Deserialize properties
        let properties =
            Self::deserialize_properties(node.get(Self::PROPERTIES_NAME).ok_or_else(|| {
                JsonSerdeError {
                    message: format!("Missing required field: {}", Self::PROPERTIES_NAME),
                }
            })?)?;
        builder = builder.properties(properties);

        // Deserialize custom properties
        let custom_properties = Self::deserialize_properties(
            node.get(Self::CUSTOM_PROPERTIES_NAME)
                .ok_or_else(|| JsonSerdeError {
                    message: format!("Missing required field: {}", Self::CUSTOM_PROPERTIES_NAME),
                })?,
        )?;
        builder = builder.custom_properties(custom_properties);

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::reassign_field_ids;
    use crate::metadata::{
        Column, DataField, DataType, DataTypes as DT, DataTypes, MapType, Schema,
    };

    #[test]
    fn column_id_round_trip_through_json() {
        let col = Column::new("a", DataTypes::int())
            .with_id(7)
            .with_comment("desc");
        let json = col.serialize_json().unwrap();
        assert_eq!(json.get("id").and_then(|v| v.as_i64()), Some(7));
        let round_tripped = Column::deserialize_json(&json).unwrap();
        assert_eq!(round_tripped, col);
    }

    #[test]
    fn schema_assigns_ids_when_absent_and_preserves_when_present() {
        let auto = Schema::builder()
            .column("a", DataTypes::int())
            .column("b", DataTypes::string())
            .build()
            .unwrap();
        let ids: Vec<i32> = auto.columns().iter().map(|c| c.id()).collect();
        assert_eq!(ids, vec![0, 1]);

        let preserved = Schema::builder()
            .with_columns(vec![
                Column::new("a", DataTypes::int()).with_id(3),
                Column::new("b", DataTypes::string()).with_id(7),
            ])
            .build()
            .unwrap();
        let ids: Vec<i32> = preserved.columns().iter().map(|c| c.id()).collect();
        assert_eq!(ids, vec![3, 7]);
    }

    #[test]
    fn schema_rejects_duplicate_ids() {
        let err = Schema::builder()
            .with_columns(vec![
                Column::new("a", DataTypes::int()).with_id(7),
                Column::new("b", DataTypes::string()).with_id(7),
            ])
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("Duplicate field id 7"), "{err}");
    }

    #[test]
    fn schema_rejects_negative_non_sentinel_ids() {
        let err = Schema::builder()
            .with_columns(vec![Column::new("a", DataTypes::int()).with_id(-7)])
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("invalid id -7"), "{err}");
    }

    #[test]
    fn column_json_id_overflow_errors() {
        let json = serde_json::json!({
            "name": "a",
            "data_type": Column::new("a", DataTypes::int()).serialize_json().unwrap()
                .get("data_type").unwrap(),
            "id": (i32::MAX as i64) + 1,
        });
        let err = Column::deserialize_json(&json).unwrap_err();
        assert!(err.to_string().contains("does not fit in i32"), "{err}");
    }

    #[test]
    fn schema_rejects_partially_assigned_ids() {
        let err = Schema::builder()
            .with_columns(vec![
                Column::new("a", DataTypes::int()).with_id(0),
                Column::new("b", DataTypes::string()),
            ])
            .build()
            .unwrap_err();
        assert!(
            err.to_string().contains("All columns must have an id"),
            "{err}"
        );
    }

    #[test]
    fn schema_assigns_nested_field_ids_in_java_dfs_order() {
        let inner_row = DataTypes::row(vec![DataField::new("n", DataTypes::int(), None)]);
        let nested_row = DataTypes::row(vec![
            DataField::new("x", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);
        let deep_row = DataTypes::row(vec![DataField::new("inner", inner_row, None)]);

        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("nested", nested_row)
            .column("deep", deep_row)
            .build()
            .unwrap();

        let top_ids: Vec<i32> = schema.columns().iter().map(|c| c.id()).collect();
        assert_eq!(top_ids, vec![0, 1, 4]);

        fn nested_field(dt: &DataType, idx: usize) -> &DataField {
            match dt {
                DataType::Row(rt) => &rt.fields()[idx],
                _ => panic!("not a Row"),
            }
        }
        let nested_dt = schema.columns()[1].data_type();
        assert_eq!(nested_field(nested_dt, 0).field_id(), 2); // x
        assert_eq!(nested_field(nested_dt, 1).field_id(), 3); // label

        let deep_dt = schema.columns()[2].data_type();
        let inner_field = nested_field(deep_dt, 0); // inner
        assert_eq!(inner_field.field_id(), 5);
        let n_field = nested_field(inner_field.data_type(), 0); // n
        assert_eq!(n_field.field_id(), 6);

        assert_eq!(schema.highest_field_id(), 6);

        for c in schema.columns() {
            assert_ne!(c.id(), UNASSIGNED_FIELD_ID);
        }
    }

    #[test]
    fn schema_array_of_row_assigns_nested_ids() {
        let elem = DataTypes::row(vec![
            DataField::new("seq", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("events", DataTypes::array(elem))
            .build()
            .unwrap();
        assert_eq!(schema.highest_field_id(), 3);
        let array_dt = schema.columns()[1].data_type();
        let elem_dt = match array_dt {
            DataType::Array(at) => at.get_element_type(),
            _ => unreachable!(),
        };
        let fields = match elem_dt {
            DataType::Row(rt) => rt.fields(),
            _ => unreachable!(),
        };
        assert_eq!(fields[0].field_id(), 2);
        assert_eq!(fields[1].field_id(), 3);
    }

    #[test]
    fn schema_nested_row_round_trips_through_json() {
        let nested = DataTypes::row(vec![
            DataField::new("x", DataTypes::int(), None),
            DataField::new("label", DataTypes::string(), None),
        ]);
        let original = Schema::builder()
            .column("id", DataTypes::int())
            .column("nested", nested)
            .build()
            .unwrap();

        let json = original.serialize_json().unwrap();

        assert_eq!(
            json.get("highest_field_id").and_then(|v| v.as_i64()),
            Some(3)
        );

        let round_tripped = Schema::deserialize_json(&json).unwrap();
        assert_eq!(round_tripped.highest_field_id(), 3);
        assert_eq!(
            round_tripped
                .columns()
                .iter()
                .map(|c| c.id())
                .collect::<Vec<_>>(),
            vec![0, 1],
        );
        assert_eq!(round_tripped, original);
    }

    #[test]
    fn schema_rejects_duplicate_nested_field_ids() {
        let nested = DataTypes::row(vec![
            DataField::with_field_id("x", DT::int(), None, 0),
            DataField::with_field_id("y", DT::int(), None, 2),
        ]);
        let err = Schema::builder()
            .with_columns(vec![
                Column::new("a", DT::int()).with_id(0),
                Column::new("b", nested).with_id(1),
            ])
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("Duplicate field id 0"), "{err}");
    }

    #[test]
    fn schema_rejects_partially_assigned_nested_field_ids() {
        let nested = DataTypes::row(vec![DataField::new("x", DT::int(), None)]);
        let err = Schema::builder()
            .with_columns(vec![
                Column::new("a", DT::int()).with_id(0),
                Column::new("b", nested).with_id(1),
            ])
            .build()
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("nested DataField ids are unassigned"),
            "{err}"
        );
    }

    #[test]
    fn schema_preserves_nested_ids_with_gaps() {
        // n2.m1=11), f2=2 (nested n0=9, n1=10).
        let inner_for_n2 = DataTypes::row(vec![DataField::with_field_id(
            "m1",
            DataTypes::tinyint(),
            None,
            11,
        )]);
        let f1_row = DataTypes::row(vec![
            DataField::with_field_id("n0", DataTypes::tinyint(), None, 6),
            DataField::with_field_id("n1", DataTypes::string(), None, 7),
            DataField::with_field_id("n2", inner_for_n2, None, 8),
        ]);
        let f2_row = DataTypes::row(vec![
            DataField::with_field_id("n0", DataTypes::tinyint(), None, 9),
            DataField::with_field_id("n1", DataTypes::string(), None, 10),
        ]);

        let schema = Schema::builder()
            .with_columns(vec![
                Column::new("f0", DataTypes::string().as_non_nullable()).with_id(0),
                Column::new("f1", f1_row).with_id(1),
                Column::new("f2", f2_row).with_id(2),
            ])
            .build()
            .unwrap();

        let top_ids: Vec<i32> = schema.columns().iter().map(|c| c.id()).collect();
        assert_eq!(top_ids, vec![0, 1, 2]);

        fn row_fields(dt: &DataType) -> &[DataField] {
            match dt {
                DataType::Row(rt) => rt.fields(),
                _ => panic!("not a Row"),
            }
        }
        let f1_fields = row_fields(schema.columns()[1].data_type());
        assert_eq!(f1_fields[0].field_id(), 6); // n0
        assert_eq!(f1_fields[1].field_id(), 7); // n1
        assert_eq!(f1_fields[2].field_id(), 8); // n2
        let n2_fields = row_fields(f1_fields[2].data_type());
        assert_eq!(n2_fields[0].field_id(), 11); // m1 — the "gap"

        let f2_fields = row_fields(schema.columns()[2].data_type());
        assert_eq!(f2_fields[0].field_id(), 9);
        assert_eq!(f2_fields[1].field_id(), 10);

        assert_eq!(schema.highest_field_id(), 11);
    }

    #[test]
    fn schema_deserializes_legacy_json_without_column_ids() {
        let legacy_json: Value = serde_json::from_str(
            r#"{
                "version": 1,
                "columns": [
                    {"name": "a", "data_type": {"type": "INTEGER", "nullable": false}, "comment": "first"},
                    {"name": "b", "data_type": {"type": "STRING"}, "comment": "second"},
                    {"name": "c", "data_type": {"type": "CHAR", "nullable": false, "length": 10}, "comment": "third"}
                ],
                "primary_key": ["a", "c"]
            }"#,
        )
        .unwrap();

        let schema = Schema::deserialize_json(&legacy_json).expect("legacy JSON must deserialize");
        let ids: Vec<i32> = schema.columns().iter().map(|c| c.id()).collect();
        assert_eq!(ids, vec![0, 1, 2], "missing IDs auto-assigned 0..N-1");
        assert_eq!(schema.highest_field_id(), 2);
        assert!(schema.primary_key().is_some());
    }

    #[test]
    fn empty_schema_has_minus_one_highest_field_id() {
        let s = Schema::builder().build().unwrap();
        assert_eq!(s.highest_field_id(), -1);
        let json = s.serialize_json().unwrap();
        assert_eq!(
            json.get("highest_field_id").and_then(|v| v.as_i64()),
            Some(-1)
        );
    }

    #[test]
    fn reassign_field_ids_walks_array_map_row() {
        let dt = DataTypes::array(DataTypes::row(vec![
            DataField::new("a", DataTypes::int(), None),
            DataField::new("b", DataTypes::string(), None),
        ]));
        let mut counter = -1_i32;
        let assigned = reassign_field_ids(&dt, &mut counter);
        match assigned {
            DataType::Array(at) => match at.get_element_type() {
                DataType::Row(rt) => {
                    assert_eq!(rt.fields()[0].field_id(), 0);
                    assert_eq!(rt.fields()[1].field_id(), 1);
                }
                _ => panic!("expected Row"),
            },
            _ => panic!("expected Array"),
        }
        assert_eq!(counter, 1);

        let dt = DataType::Map(MapType::new(
            DataTypes::int(),
            DataTypes::row(vec![DataField::new("x", DataTypes::int(), None)]),
        ));
        let mut counter = -1_i32;
        let assigned = reassign_field_ids(&dt, &mut counter);
        let value_type = match &assigned {
            DataType::Map(mt) => mt.value_type(),
            _ => panic!("expected Map"),
        };
        match value_type {
            DataType::Row(rt) => assert_eq!(rt.fields()[0].field_id(), 0),
            _ => panic!("expected Row"),
        }
        assert_eq!(counter, 0);
    }

    #[test]
    fn test_datatype_json_serde() {
        let data_types = vec![
            DataTypes::boolean(),
            DataTypes::tinyint(),
            DataTypes::smallint(),
            DataTypes::int().as_non_nullable(),
            DataTypes::bigint(),
            DataTypes::float(),
            DataTypes::double(),
            DataTypes::char(10),
            DataTypes::string(),
            DataTypes::decimal(10, 2),
            DataTypes::date(),
            DataTypes::time(),
            DataTypes::timestamp(),
            DataTypes::timestamp_ltz(),
            DataTypes::bytes(),
            DataTypes::binary(100),
            DataTypes::array(DataTypes::int()),
            DataTypes::map(DataTypes::string(), DataTypes::int()),
            DataTypes::row(vec![
                DataField::new("f1".to_string(), DataTypes::int(), None),
                DataField::new(
                    "f2".to_string(),
                    DataTypes::string(),
                    Some("desc".to_string()),
                ),
            ]),
        ];

        for dt in data_types {
            let json = dt.serialize_json().unwrap();
            let deserialized = DataType::deserialize_json(&json).unwrap();
            assert_eq!(dt, deserialized);
        }
    }

    #[test]
    fn test_invalid_datatype_validation() {
        use serde_json::json;

        // Invalid DECIMAL precision (> 38)
        let invalid_decimal = json!({
            "type": "DECIMAL",
            "precision": 50,
            "scale": 2
        });
        let result = DataType::deserialize_json(&invalid_decimal);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid DECIMAL parameters")
        );

        // Invalid TIME precision (> 9)
        let invalid_time = json!({
            "type": "TIME_WITHOUT_TIME_ZONE",
            "precision": 15
        });
        let result = DataType::deserialize_json(&invalid_time);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid TIME_WITHOUT_TIME_ZONE precision")
        );

        // Invalid TIMESTAMP precision (> 9)
        let invalid_timestamp = json!({
            "type": "TIMESTAMP_WITHOUT_TIME_ZONE",
            "precision": 20
        });
        let result = DataType::deserialize_json(&invalid_timestamp);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid TIMESTAMP_WITHOUT_TIME_ZONE precision")
        );

        // Invalid TIMESTAMP_LTZ precision (> 9)
        let invalid_timestamp_ltz = json!({
            "type": "TIMESTAMP_WITH_LOCAL_TIME_ZONE",
            "precision": 10
        });
        let result = DataType::deserialize_json(&invalid_timestamp_ltz);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid TIMESTAMP_WITH_LOCAL_TIME_ZONE precision")
        );

        // Invalid DECIMAL scale (> precision)
        let invalid_decimal_scale = json!({
            "type": "DECIMAL",
            "precision": 10,
            "scale": 15
        });
        let result = DataType::deserialize_json(&invalid_decimal_scale);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid DECIMAL parameters")
        );
    }
}
