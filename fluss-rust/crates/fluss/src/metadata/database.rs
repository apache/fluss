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
use crate::error::Result;
use crate::metadata::JsonSerde;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseDescriptor {
    comment: Option<String>,
    custom_properties: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DatabaseInfo {
    database_name: String,
    database_descriptor: DatabaseDescriptor,
    created_time: i64,
    modified_time: i64,
}

impl DatabaseInfo {
    pub fn new(
        database_name: String,
        database_descriptor: DatabaseDescriptor,
        created_time: i64,
        modified_time: i64,
    ) -> Self {
        Self {
            database_name,
            database_descriptor,
            created_time,
            modified_time,
        }
    }

    pub fn database_name(&self) -> &str {
        &self.database_name
    }

    pub fn database_descriptor(&self) -> &DatabaseDescriptor {
        &self.database_descriptor
    }

    pub fn created_time(&self) -> i64 {
        self.created_time
    }

    pub fn modified_time(&self) -> i64 {
        self.modified_time
    }
}

#[derive(Debug, Default)]
pub struct DatabaseDescriptorBuilder {
    comment: Option<String>,
    custom_properties: HashMap<String, String>,
}

impl DatabaseDescriptor {
    pub fn builder() -> DatabaseDescriptorBuilder {
        DatabaseDescriptorBuilder::default()
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn custom_properties(&self) -> &HashMap<String, String> {
        &self.custom_properties
    }
}

impl DatabaseDescriptorBuilder {
    pub fn comment<C: Into<String>>(mut self, comment: C) -> Self {
        self.comment = Some(comment.into());
        self
    }

    pub fn custom_properties<K: Into<String>, V: Into<String>>(
        mut self,
        properties: HashMap<K, V>,
    ) -> Self {
        for (k, v) in properties {
            self.custom_properties.insert(k.into(), v.into());
        }
        self
    }

    pub fn custom_property<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.custom_properties.insert(key.into(), value.into());
        self
    }

    pub fn build(self) -> DatabaseDescriptor {
        DatabaseDescriptor {
            comment: self.comment,
            custom_properties: self.custom_properties,
        }
    }
}

impl DatabaseDescriptor {
    const CUSTOM_PROPERTIES_NAME: &'static str = "custom_properties";
    const COMMENT_NAME: &'static str = "comment";
    const VERSION_KEY: &'static str = "version";
    const VERSION: u32 = 1;
}

impl JsonSerde for DatabaseDescriptor {
    fn serialize_json(&self) -> Result<Value> {
        let mut obj = serde_json::Map::new();

        // Serialize version
        obj.insert(Self::VERSION_KEY.to_string(), json!(Self::VERSION));

        // Serialize comment if present
        if let Some(comment) = self.comment() {
            obj.insert(Self::COMMENT_NAME.to_string(), json!(comment));
        }

        // Serialize custom properties
        obj.insert(
            Self::CUSTOM_PROPERTIES_NAME.to_string(),
            json!(self.custom_properties()),
        );

        Ok(Value::Object(obj))
    }

    fn deserialize_json(node: &Value) -> Result<Self> {
        let mut builder = DatabaseDescriptor::builder();

        // Deserialize comment if present
        if let Some(comment_node) = node.get(Self::COMMENT_NAME) {
            let comment = comment_node
                .as_str()
                .ok_or_else(|| JsonSerdeError {
                    message: format!("{} should be a string", Self::COMMENT_NAME),
                })?
                .to_owned();
            builder = builder.comment(&comment);
        }

        // Deserialize custom properties directly
        let custom_properties = if let Some(props_node) = node.get(Self::CUSTOM_PROPERTIES_NAME) {
            let obj = props_node.as_object().ok_or_else(|| JsonSerdeError {
                message: "Custom properties should be an object".to_string(),
            })?;

            let mut properties = HashMap::with_capacity(obj.len());
            for (key, value) in obj {
                properties.insert(
                    key.clone(),
                    value
                        .as_str()
                        .ok_or_else(|| JsonSerdeError {
                            message: "Property value should be a string".to_string(),
                        })?
                        .to_owned(),
                );
            }
            properties
        } else {
            HashMap::new()
        };
        builder = builder.custom_properties(custom_properties);

        Ok(builder.build())
    }
}

impl DatabaseDescriptor {
    /// Create DatabaseDescriptor from JSON bytes (equivalent to Java's fromJsonBytes)
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
        let json_value: Value = serde_json::from_slice(bytes).map_err(|e| JsonSerdeError {
            message: format!("Failed to parse JSON: {e}"),
        })?;
        Self::deserialize_json(&json_value)
    }

    /// Convert DatabaseDescriptor to JSON bytes
    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        let json_value = self.serialize_json()?;
        serde_json::to_vec(&json_value).map_err(|e| JsonSerdeError {
            message: format!("Failed to serialize to JSON: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_descriptor_json_serde() {
        let mut custom_props = HashMap::new();
        custom_props.insert("key1".to_string(), "value1".to_string());
        custom_props.insert("key2".to_string(), "value2".to_string());

        let descriptor = DatabaseDescriptor::builder()
            .comment("Test database")
            .custom_properties(custom_props)
            .build();

        // Test serialization
        let json_bytes = descriptor.to_json_bytes().unwrap();
        println!("Serialized JSON: {}", String::from_utf8_lossy(&json_bytes));

        // Test deserialization
        let deserialized = DatabaseDescriptor::from_json_bytes(&json_bytes).unwrap();
        assert_eq!(descriptor, deserialized);
    }

    #[test]
    fn test_empty_database_descriptor() {
        let descriptor = DatabaseDescriptor::builder().build();
        let json_bytes = descriptor.to_json_bytes().unwrap();
        let deserialized = DatabaseDescriptor::from_json_bytes(&json_bytes).unwrap();
        assert_eq!(descriptor, deserialized);
    }
}
