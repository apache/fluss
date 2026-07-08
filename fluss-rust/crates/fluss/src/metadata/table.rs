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

use crate::compression::ArrowCompressionInfo;
use crate::error::Error::IllegalArgument;
use crate::error::{Error, Result};
use crate::metadata::DataLakeFormat;
use crate::metadata::datatype::{
    DataField, DataType, RowType, UNASSIGNED_FIELD_ID, reassign_field_ids,
};
use crate::{BucketId, PartitionId, TableId};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use strum_macros::EnumString;

/// Sentinel for a column whose stable id has not yet been assigned.
pub const UNKNOWN_COLUMN_ID: i32 = -1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Column {
    name: String,
    data_type: DataType,
    comment: Option<String>,
    id: i32,
}

impl Column {
    pub fn new<N: Into<String>>(name: N, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            comment: None,
            id: UNKNOWN_COLUMN_ID,
        }
    }

    pub fn with_comment<C: Into<String>>(mut self, comment: C) -> Self {
        self.comment = Some(comment.into());
        self
    }

    pub fn with_data_type(&self, data_type: DataType) -> Self {
        Self {
            name: self.name.clone(),
            data_type: data_type.clone(),
            comment: self.comment.clone(),
            id: self.id,
        }
    }

    pub fn with_id(mut self, id: i32) -> Self {
        self.id = id;
        self
    }

    // Getters...
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    /// Returns the stable column id, or [`UNKNOWN_COLUMN_ID`] when the
    /// id has not yet been assigned by a [`SchemaBuilder`].
    pub fn id(&self) -> i32 {
        self.id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PrimaryKey {
    constraint_name: String,
    column_names: Vec<String>,
}

impl PrimaryKey {
    pub fn new<N: Into<String>>(constraint_name: N, column_names: Vec<String>) -> Self {
        Self {
            constraint_name: constraint_name.into(),
            column_names,
        }
    }

    // Getters...
    pub fn constraint_name(&self) -> &str {
        &self.constraint_name
    }

    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }
}

fn collect_field_id_state(data_type: &DataType, max_id: &mut i32, has_unassigned: &mut bool) {
    match data_type {
        DataType::Row(rt) => {
            for f in rt.fields() {
                if f.field_id == UNASSIGNED_FIELD_ID {
                    *has_unassigned = true;
                } else {
                    *max_id = (*max_id).max(f.field_id);
                }
                collect_field_id_state(&f.data_type, max_id, has_unassigned);
            }
        }
        DataType::Array(at) => {
            collect_field_id_state(at.get_element_type(), max_id, has_unassigned);
        }
        DataType::Map(mt) => {
            collect_field_id_state(mt.key_type(), max_id, has_unassigned);
            collect_field_id_state(mt.value_type(), max_id, has_unassigned);
        }
        _ => {}
    }
}

fn collect_nested_field_ids(data_type: &DataType, ids: &mut Vec<i32>) {
    match data_type {
        DataType::Row(rt) => {
            for f in rt.fields() {
                if f.field_id != UNASSIGNED_FIELD_ID {
                    ids.push(f.field_id);
                }
                collect_nested_field_ids(&f.data_type, ids);
            }
        }
        DataType::Array(at) => collect_nested_field_ids(at.get_element_type(), ids),
        DataType::Map(mt) => {
            collect_nested_field_ids(mt.key_type(), ids);
            collect_nested_field_ids(mt.value_type(), ids);
        }
        _ => {}
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Schema {
    columns: Vec<Column>,
    primary_key: Option<PrimaryKey>,
    row_type: RowType,
    auto_increment_col_names: Vec<String>,
    highest_field_id: i32,
}

impl Schema {
    pub fn empty() -> Result<Self> {
        Self::builder().build()
    }

    pub fn builder() -> SchemaBuilder {
        SchemaBuilder::new()
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn primary_key(&self) -> Option<&PrimaryKey> {
        self.primary_key.as_ref()
    }

    pub fn row_type(&self) -> &RowType {
        &self.row_type
    }

    pub fn primary_key_indexes(&self) -> Vec<usize> {
        self.primary_key
            .as_ref()
            .map(|pk| {
                pk.column_names
                    .iter()
                    .filter_map(|name| self.columns.iter().position(|c| &c.name == name))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn primary_key_column_names(&self) -> Vec<&str> {
        self.primary_key
            .as_ref()
            .map(|pk| pk.column_names.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    pub fn auto_increment_col_names(&self) -> &Vec<String> {
        &self.auto_increment_col_names
    }

    pub fn highest_field_id(&self) -> i32 {
        self.highest_field_id
    }
}

/// A schema together with its server-assigned version id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    schema: Schema,
    schema_id: i32,
}

impl SchemaInfo {
    pub fn new(schema: Schema, schema_id: i32) -> Self {
        Self { schema, schema_id }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn schema_id(&self) -> i32 {
        self.schema_id
    }

    pub fn into_parts(self) -> (Schema, i32) {
        (self.schema, self.schema_id)
    }
}

#[derive(Debug, Default)]
pub struct SchemaBuilder {
    columns: Vec<Column>,
    primary_key: Option<PrimaryKey>,
    auto_increment_col_names: Vec<String>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_row_type(mut self, row_type: &DataType) -> Self {
        match row_type {
            DataType::Row(row) => {
                for data_field in row.fields() {
                    self = self.column(&data_field.name, data_field.data_type.clone())
                }
                self
            }
            _ => {
                panic!("data type must be row type")
            }
        }
    }

    pub fn column<N: Into<String>>(mut self, name: N, data_type: DataType) -> Self {
        self.columns.push(Column::new(name.into(), data_type));
        self
    }

    pub fn with_columns(mut self, columns: Vec<Column>) -> Self {
        self.columns.extend_from_slice(columns.as_ref());
        self
    }

    pub fn with_comment<C: Into<String>>(mut self, comment: C) -> Self {
        if let Some(last) = self.columns.last_mut() {
            *last = last.clone().with_comment(comment.into());
        }
        self
    }

    pub fn primary_key<I, S>(self, column_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let names: Vec<String> = column_names.into_iter().map(|s| s.into()).collect();

        let constraint_name = format!("PK_{}", names.join("_"));

        self.primary_key_named(&constraint_name, names)
    }

    pub fn primary_key_named<N: Into<String>, P: Into<String>>(
        mut self,
        constraint_name: N,
        column_names: Vec<P>,
    ) -> Self {
        self.primary_key = Some(PrimaryKey::new(
            constraint_name.into(),
            column_names.into_iter().map(|s| s.into()).collect(),
        ));
        self
    }

    /// Declares a column to be auto-incremented. With an auto-increment column in the table,
    /// whenever a new row is inserted into the table, the new row will be assigned with the next
    /// available value from the auto-increment sequence. A table can have at most one auto
    /// increment column.
    pub fn enable_auto_increment<N: Into<String>>(mut self, column_name: N) -> Result<Self> {
        if !self.auto_increment_col_names.is_empty() {
            return Err(IllegalArgument {
                message: "Multiple auto increment columns are not supported yet.".to_string(),
            });
        }

        self.auto_increment_col_names.push(column_name.into());
        Ok(self)
    }

    pub fn build(&self) -> Result<Schema> {
        let columns = Self::normalize_columns(&self.columns, self.primary_key.as_ref())?;
        let (columns_with_ids, highest_field_id) = Self::assign_all_field_ids(columns)?;

        let column_names: HashSet<_> = columns_with_ids.iter().map(|c| &c.name).collect();
        for auto_inc_col in &self.auto_increment_col_names {
            if !column_names.contains(auto_inc_col) {
                return Err(IllegalArgument {
                    message: format!(
                        "Auto increment column '{auto_inc_col}' is not found in the schema columns."
                    ),
                });
            }
        }

        let data_fields = columns_with_ids
            .iter()
            .map(|c| DataField {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                description: c.comment.clone(),
                field_id: c.id,
            })
            .collect();

        Ok(Schema {
            columns: columns_with_ids,
            primary_key: self.primary_key.clone(),
            row_type: RowType::new(data_fields),
            auto_increment_col_names: self.auto_increment_col_names.clone(),
            highest_field_id,
        })
    }

    fn assign_all_field_ids(columns: Vec<Column>) -> Result<(Vec<Column>, i32)> {
        let with_top_id = columns.iter().filter(|c| c.id != UNKNOWN_COLUMN_ID).count();
        let none_set = with_top_id == 0;
        let all_top_set = with_top_id == columns.len();

        if !none_set && !all_top_set {
            return Err(IllegalArgument {
                message: "All columns must have an id assigned, or none of them must.".to_string(),
            });
        }

        let mut max_nested_id = -1_i32;
        let mut has_unassigned_nested = false;
        for c in &columns {
            collect_field_id_state(&c.data_type, &mut max_nested_id, &mut has_unassigned_nested);
        }

        if all_top_set && !has_unassigned_nested {
            let mut seen: HashSet<i32> = HashSet::new();
            let mut max_id = -1_i32;
            for col in &columns {
                if col.id < 0 {
                    return Err(IllegalArgument {
                        message: format!(
                            "Column '{}' has invalid id {}; ids must be non-negative",
                            col.name, col.id
                        ),
                    });
                }
                if !seen.insert(col.id) {
                    return Err(IllegalArgument {
                        message: format!("Duplicate field id {} in schema", col.id),
                    });
                }
                max_id = max_id.max(col.id);

                let mut nested_ids = Vec::new();
                collect_nested_field_ids(&col.data_type, &mut nested_ids);
                for id in nested_ids {
                    if id < 0 {
                        return Err(IllegalArgument {
                            message: format!(
                                "Nested DataField in column '{}' has invalid id {}; ids must be non-negative",
                                col.name, id
                            ),
                        });
                    }
                    if !seen.insert(id) {
                        return Err(IllegalArgument {
                            message: format!(
                                "Duplicate field id {} in schema (column '{}')",
                                id, col.name
                            ),
                        });
                    }
                }
            }
            max_id = max_id.max(max_nested_id);
            return Ok((columns, max_id));
        }

        if all_top_set && has_unassigned_nested {
            return Err(IllegalArgument {
                message: "Top-level column ids are set but some nested DataField ids are unassigned; reassign all or none."
                    .to_string(),
            });
        }

        let mut counter: i32 = -1;
        let new_columns: Vec<Column> = columns
            .into_iter()
            .map(|c| {
                counter += 1;
                let id = counter;
                let new_data_type = reassign_field_ids(&c.data_type, &mut counter);
                Column {
                    name: c.name,
                    data_type: new_data_type,
                    comment: c.comment,
                    id,
                }
            })
            .collect();
        Ok((new_columns, counter))
    }

    /// All-or-none: preserve ids if every column has one, auto-assign
    /// 0..N-1 if none do, error on mixed input. When preserving ids,
    /// also reject duplicates and negative-but-not-sentinel values.
    #[allow(dead_code)]
    fn assign_column_ids(columns: Vec<Column>) -> Result<Vec<Column>> {
        let with_id = columns.iter().filter(|c| c.id != UNKNOWN_COLUMN_ID).count();
        if with_id == 0 {
            return Ok(columns
                .into_iter()
                .enumerate()
                .map(|(i, c)| c.with_id(i as i32))
                .collect());
        }
        if with_id != columns.len() {
            return Err(IllegalArgument {
                message: "All columns must have an id assigned, or none of them must.".to_string(),
            });
        }
        let mut seen: HashSet<i32> = HashSet::with_capacity(columns.len());
        for col in &columns {
            if col.id < 0 {
                return Err(IllegalArgument {
                    message: format!(
                        "Column '{}' has invalid id {}; ids must be non-negative",
                        col.name, col.id
                    ),
                });
            }
            if !seen.insert(col.id) {
                return Err(IllegalArgument {
                    message: format!("Duplicate column id {} in schema", col.id),
                });
            }
        }
        Ok(columns)
    }

    fn normalize_columns(
        columns: &[Column],
        primary_key: Option<&PrimaryKey>,
    ) -> Result<Vec<Column>> {
        let names: Vec<_> = columns.iter().map(|c| &c.name).collect();
        if let Some(duplicates) = Self::find_duplicates(&names) {
            return Err(Error::invalid_table(format!(
                "Duplicate column names found: {duplicates:?}"
            )));
        }

        let Some(pk) = primary_key else {
            return Ok(columns.to_vec());
        };

        let pk_set: HashSet<_> = pk.column_names.iter().collect();
        let all_columns: HashSet<_> = columns.iter().map(|c| &c.name).collect();
        if !pk_set.is_subset(&all_columns) {
            return Err(Error::invalid_table(format!(
                "Primary key columns {pk_set:?} not found in schema"
            )));
        }

        Ok(columns
            .iter()
            .map(|col| {
                if pk_set.contains(&col.name) && col.data_type.is_nullable() {
                    col.with_data_type(col.data_type.as_non_nullable())
                } else {
                    col.clone()
                }
            })
            .collect())
    }

    fn find_duplicates<'a>(names: &'a [&String]) -> Option<HashSet<&'a String>> {
        let mut seen = HashSet::new();
        let mut duplicates = HashSet::new();

        for name in names {
            if !seen.insert(name) {
                duplicates.insert(*name);
            }
        }

        if duplicates.is_empty() {
            None
        } else {
            Some(duplicates)
        }
    }
}

/// distribution of table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableDistribution {
    bucket_count: Option<i32>,
    bucket_keys: Vec<String>,
}

impl TableDistribution {
    pub fn bucket_keys(&self) -> &[String] {
        &self.bucket_keys
    }

    pub fn bucket_count(&self) -> Option<i32> {
        self.bucket_count
    }
}

#[derive(Debug, Default)]
pub struct TableDescriptorBuilder {
    schema: Option<Schema>,
    properties: HashMap<String, String>,
    custom_properties: HashMap<String, String>,
    partition_keys: Arc<[String]>,
    comment: Option<String>,
    table_distribution: Option<TableDistribution>,
}

impl TableDescriptorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    pub fn log_format(mut self, log_format: LogFormat) -> Self {
        self.properties
            .insert("table.log.format".to_string(), log_format.to_string());
        self
    }

    pub fn kv_format(mut self, kv_format: KvFormat) -> Self {
        self.properties
            .insert("table.kv.format".to_string(), kv_format.to_string());
        self
    }

    pub fn property<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    pub fn properties<K: Into<String>, V: Into<String>>(
        mut self,
        properties: HashMap<K, V>,
    ) -> Self {
        for (k, v) in properties {
            self.properties.insert(k.into(), v.into());
        }
        self
    }

    pub fn custom_property<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.custom_properties.insert(key.into(), value.into());
        self
    }

    pub fn custom_properties<K: Into<String>, V: Into<String>>(
        mut self,
        custom_properties: HashMap<K, V>,
    ) -> Self {
        for (k, v) in custom_properties {
            self.custom_properties.insert(k.into(), v.into());
        }
        self
    }

    pub fn partitioned_by<P: Into<String>>(mut self, partition_keys: Vec<P>) -> Self {
        self.partition_keys = Arc::from(
            partition_keys
                .into_iter()
                .map(|s| s.into())
                .collect::<Vec<String>>(),
        );
        self
    }

    pub fn distributed_by(mut self, bucket_count: Option<i32>, bucket_keys: Vec<String>) -> Self {
        self.table_distribution = Some(TableDistribution {
            bucket_count,
            bucket_keys,
        });
        self
    }

    pub fn comment<S: Into<String>>(mut self, comment: S) -> Self {
        self.comment = Some(comment.into());
        self
    }

    pub fn build(self) -> Result<TableDescriptor> {
        let schema = self.schema.expect("Schema must be set");
        let table_distribution = TableDescriptor::normalize_distribution(
            &schema,
            &self.partition_keys,
            self.table_distribution,
        )?;
        Ok(TableDescriptor {
            schema,
            comment: self.comment,
            partition_keys: self.partition_keys,
            table_distribution,
            properties: self.properties,
            custom_properties: self.custom_properties,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableDescriptor {
    schema: Schema,
    comment: Option<String>,
    partition_keys: Arc<[String]>,
    table_distribution: Option<TableDistribution>,
    properties: HashMap<String, String>,
    custom_properties: HashMap<String, String>,
}

impl TableDescriptor {
    pub fn builder() -> TableDescriptorBuilder {
        TableDescriptorBuilder::new()
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn bucket_keys(&self) -> Vec<&str> {
        self.table_distribution
            .as_ref()
            .map(|td| td.bucket_keys.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }

    pub fn is_default_bucket_key(&self) -> Result<bool> {
        if self.schema.primary_key().is_some() {
            Ok(self.bucket_keys()
                == Self::default_bucket_key_of_primary_key_table(
                    self.schema(),
                    &self.partition_keys,
                )?
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>())
        } else {
            Ok(self.bucket_keys().is_empty())
        }
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_keys.is_empty()
    }

    pub fn has_primary_key(&self) -> bool {
        self.schema.primary_key().is_some()
    }

    pub fn partition_keys(&self) -> &[String] {
        &self.partition_keys
    }

    pub fn table_distribution(&self) -> Option<&TableDistribution> {
        self.table_distribution.as_ref()
    }

    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn custom_properties(&self) -> &HashMap<String, String> {
        &self.custom_properties
    }

    pub fn replication_factor(&self) -> Result<i32> {
        self.properties
            .get("table.replication.factor")
            .ok_or_else(|| Error::invalid_table("Replication factor is not set"))?
            .parse()
            .map_err(|_e| Error::invalid_table("Replication factor can't be converted to int"))
    }

    pub fn with_properties<K: Into<String>, V: Into<String>>(
        &self,
        new_properties: HashMap<K, V>,
    ) -> Self {
        let mut properties = HashMap::new();
        for (k, v) in new_properties {
            properties.insert(k.into(), v.into());
        }
        Self {
            properties,
            ..self.clone()
        }
    }

    pub fn with_replication_factor(&self, new_replication_factor: i32) -> Self {
        let mut properties = self.properties.clone();
        properties.insert(
            "table.replication.factor".to_string(),
            new_replication_factor.to_string(),
        );
        self.with_properties(properties)
    }

    pub fn with_bucket_count(&self, new_bucket_count: i32) -> Self {
        Self {
            table_distribution: Some(TableDistribution {
                bucket_count: Some(new_bucket_count),
                bucket_keys: self
                    .table_distribution
                    .as_ref()
                    .map(|td| td.bucket_keys.clone())
                    .unwrap_or_default(),
            }),
            ..self.clone()
        }
    }

    pub fn comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    fn default_bucket_key_of_primary_key_table(
        schema: &Schema,
        partition_keys: &[String],
    ) -> Result<Vec<String>> {
        let mut bucket_keys = schema
            .primary_key()
            .expect("Primary key must be set")
            .column_names()
            .to_vec();

        bucket_keys.retain(|k| !partition_keys.contains(k));

        if bucket_keys.is_empty() {
            return Err(Error::invalid_table(format!(
                "Primary Key constraint {:?} should not be same with partition fields {:?}.",
                schema.primary_key().unwrap().column_names(),
                partition_keys
            )));
        }

        Ok(bucket_keys)
    }

    fn normalize_distribution(
        schema: &Schema,
        partition_keys: &[String],
        origin_distribution: Option<TableDistribution>,
    ) -> Result<Option<TableDistribution>> {
        if let Some(distribution) = origin_distribution {
            if distribution
                .bucket_keys
                .iter()
                .any(|k| partition_keys.contains(k))
            {
                return Err(Error::invalid_table(format!(
                    "Bucket key {:?} shouldn't include any column in partition keys {:?}.",
                    distribution.bucket_keys, partition_keys
                )));
            }

            return if let Some(pk) = schema.primary_key() {
                if distribution.bucket_keys.is_empty() {
                    Ok(Some(TableDistribution {
                        bucket_count: distribution.bucket_count,
                        bucket_keys: Self::default_bucket_key_of_primary_key_table(
                            schema,
                            partition_keys,
                        )?,
                    }))
                } else {
                    let pk_columns: HashSet<_> = pk.column_names().iter().collect();
                    if !distribution
                        .bucket_keys
                        .iter()
                        .all(|k| pk_columns.contains(k))
                    {
                        return Err(Error::invalid_table(format!(
                            "Bucket keys must be a subset of primary keys excluding partition keys for primary-key tables. \
                            The primary keys are {:?}, the partition keys are {:?}, but the user-defined bucket keys are {:?}.",
                            pk.column_names(),
                            partition_keys,
                            distribution.bucket_keys
                        )));
                    }
                    Ok(Some(distribution))
                }
            } else {
                Ok(Some(distribution))
            };
        } else if schema.primary_key().is_some() {
            return Ok(Some(TableDistribution {
                bucket_count: None,
                bucket_keys: Self::default_bucket_key_of_primary_key_table(schema, partition_keys)?,
            }));
        }

        Ok(None)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LogFormat {
    ARROW,
    INDEXED,
}

impl Display for LogFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            LogFormat::ARROW => {
                write!(f, "ARROW")?;
            }
            LogFormat::INDEXED => {
                write!(f, "INDEXED")?;
            }
        }
        Ok(())
    }
}

impl LogFormat {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "ARROW" => Ok(LogFormat::ARROW),
            "INDEXED" => Ok(LogFormat::INDEXED),
            _ => Err(Error::invalid_table(format!("Unknown log format: {s}"))),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, EnumString)]
pub enum KvFormat {
    INDEXED,
    COMPACTED,
}

impl Display for KvFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            KvFormat::COMPACTED => write!(f, "COMPACTED")?,
            KvFormat::INDEXED => write!(f, "INDEXED")?,
        }
        Ok(())
    }
}

impl KvFormat {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_uppercase().as_str() {
            "INDEXED" => Ok(KvFormat::INDEXED),
            "COMPACTED" => Ok(KvFormat::COMPACTED),
            _ => Err(Error::invalid_table(format!("Unknown kv format: {s}"))),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct TablePath {
    database: String,
    table: String,
}

impl Display for TablePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

const MAX_NAME_LENGTH: usize = 200;

const INTERNAL_NAME_PREFIX: &str = "__";

impl TablePath {
    pub fn new<D: Into<String>, T: Into<String>>(db: D, tbl: T) -> Self {
        TablePath {
            database: db.into(),
            table: tbl.into(),
        }
    }

    #[inline]
    pub fn database(&self) -> &str {
        &self.database
    }

    #[inline]
    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn detect_invalid_name(identifier: &str) -> Option<String> {
        if identifier.is_empty() {
            return Some("the empty string is not allowed".to_string());
        }
        if identifier == "." {
            return Some("'.' is not allowed".to_string());
        }
        if identifier == ".." {
            return Some("'..' is not allowed".to_string());
        }
        if identifier.len() > MAX_NAME_LENGTH {
            return Some(format!(
                "the length of '{identifier}' is longer than the max allowed length {MAX_NAME_LENGTH}"
            ));
        }
        if Self::contains_invalid_pattern(identifier) {
            return Some(format!(
                "'{identifier}' contains one or more characters other than ASCII alphanumerics, '_' and '-'"
            ));
        }
        None
    }

    pub fn validate_prefix(identifier: &str) -> Option<String> {
        if identifier.starts_with(INTERNAL_NAME_PREFIX) {
            return Some(format!(
                "'{INTERNAL_NAME_PREFIX}' is not allowed as prefix, since it is reserved for internal databases/internal tables/internal partitions in Fluss server"
            ));
        }
        None
    }

    // Valid characters for Fluss table names are the ASCII alphanumerics, '_' and '-'.
    fn contains_invalid_pattern(identifier: &str) -> bool {
        for c in identifier.chars() {
            let valid_char = c.is_ascii_alphanumeric() || c == '_' || c == '-';
            if !valid_char {
                return true;
            }
        }
        false
    }
}

/// A database name, table name and partition name combo. It's used to represent the physical path of
/// a bucket. If the bucket belongs to a partition (i.e., the table is a partitioned table),
/// `partition_name` will be `Some(...)`; otherwise, it will be `None`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PhysicalTablePath {
    table_path: Arc<TablePath>,
    partition_name: Option<String>,
}

impl PhysicalTablePath {
    pub fn of(table_path: Arc<TablePath>) -> Self {
        Self {
            table_path,
            partition_name: None,
        }
    }

    pub fn of_partitioned(table_path: Arc<TablePath>, partition_name: Option<String>) -> Self {
        Self {
            table_path,
            partition_name,
        }
    }

    pub fn of_with_names<D: Into<String>, T: Into<String>, P: Into<String>>(
        database_name: D,
        table_name: T,
        partition_name: Option<P>,
    ) -> Self {
        Self {
            table_path: Arc::new(TablePath::new(database_name, table_name)),
            partition_name: partition_name.map(|p| p.into()),
        }
    }

    pub fn get_table_path(&self) -> &TablePath {
        &self.table_path
    }

    pub fn get_database_name(&self) -> &str {
        self.table_path.database()
    }

    pub fn get_table_name(&self) -> &str {
        self.table_path.table()
    }

    pub fn get_partition_name(&self) -> Option<&String> {
        self.partition_name.as_ref()
    }
}

impl Display for PhysicalTablePath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.partition_name {
            Some(partition) => write!(f, "{}(p={})", self.table_path, partition),
            None => write!(f, "{}", self.table_path),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub table_path: TablePath,
    pub table_id: TableId,
    pub schema_id: i32,
    pub schema: Schema,
    pub row_type: RowType,
    pub primary_keys: Vec<String>,
    pub physical_primary_keys: Vec<String>,
    pub bucket_keys: Vec<String>,
    pub partition_keys: Arc<[String]>,
    pub num_buckets: i32,
    pub properties: HashMap<String, String>,
    pub table_config: TableConfig,
    pub custom_properties: HashMap<String, String>,
    pub comment: Option<String>,
    pub created_time: i64,
    pub modified_time: i64,
}

impl TableInfo {
    pub fn row_type(&self) -> &RowType {
        &self.row_type
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoPartitionStrategy {
    auto_partition_enabled: bool,
    auto_partition_key: Option<String>,
    auto_partition_time_unit: String,
    auto_partition_num_precreate: i32,
    auto_partition_num_retention: i32,
    auto_partition_timezone: String,
}

impl AutoPartitionStrategy {
    pub fn from(properties: &HashMap<String, String>) -> Self {
        Self {
            auto_partition_enabled: properties
                .get("table.auto-partition.enabled")
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),
            auto_partition_key: properties
                .get("table.auto-partition.key")
                .map(|s| s.to_string()),
            auto_partition_time_unit: properties
                .get("table.auto-partition.time-unit")
                .map(|s| s.to_string())
                .unwrap_or_else(|| "DAY".to_string()),
            auto_partition_num_precreate: properties
                .get("table.auto-partition.num-precreate")
                .and_then(|s| s.parse().ok())
                .unwrap_or(2),
            auto_partition_num_retention: properties
                .get("table.auto-partition.num-retention")
                .and_then(|s| s.parse().ok())
                .unwrap_or(7),
            auto_partition_timezone: properties
                .get("table.auto-partition.time-zone")
                .map(|s| s.to_string())
                .unwrap_or_else(|| {
                    jiff::tz::TimeZone::system()
                        .iana_name()
                        .unwrap_or("UTC")
                        .to_string()
                }),
        }
    }

    pub fn is_auto_partition_enabled(&self) -> bool {
        self.auto_partition_enabled
    }

    pub fn key(&self) -> Option<&str> {
        self.auto_partition_key.as_deref()
    }

    pub fn time_unit(&self) -> &str {
        &self.auto_partition_time_unit
    }

    pub fn num_precreate(&self) -> i32 {
        self.auto_partition_num_precreate
    }

    pub fn num_retention(&self) -> i32 {
        self.auto_partition_num_retention
    }

    pub fn timezone(&self) -> &str {
        &self.auto_partition_timezone
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableConfig {
    pub properties: HashMap<String, String>,
}

impl TableConfig {
    pub fn from_properties(properties: HashMap<String, String>) -> Self {
        TableConfig { properties }
    }

    pub fn get_arrow_compression_info(&self) -> Result<ArrowCompressionInfo> {
        ArrowCompressionInfo::from_conf(&self.properties)
    }

    /// Returns the data lake format if configured, or None if not set.
    pub fn get_datalake_format(&self) -> Result<Option<DataLakeFormat>> {
        self.properties
            .get("table.datalake.format")
            .map(|f| f.parse().map_err(Error::from))
            .transpose()
    }

    pub fn get_kv_format(&self) -> Result<KvFormat> {
        // TODO: Consolidate configurations logic, constants, defaults in a single place
        const DEFAULT_KV_FORMAT: &str = "COMPACTED";
        let kv_format = self
            .properties
            .get("table.kv.format")
            .map(String::as_str)
            .unwrap_or(DEFAULT_KV_FORMAT);
        kv_format.parse().map_err(Into::into)
    }

    /// Returns the KV-format version of the table, defaulting to `1` when the
    /// `table.kv.format-version` property is absent. Matches Java's
    /// `TableConfig.getKvFormatVersion().orElse(1)` semantics.
    pub fn get_kv_format_version(&self) -> Result<i32> {
        const DEFAULT: i32 = 1;
        match self.properties.get("table.kv.format-version") {
            Some(v) => v.parse::<i32>().map_err(|e| {
                Error::invalid_table(format!("Invalid table.kv.format-version {v:?}: {e}"))
            }),
            None => Ok(DEFAULT),
        }
    }

    pub fn get_log_format(&self) -> Result<LogFormat> {
        // TODO: Consolidate configurations logic, constants, defaults in a single place
        const DEFAULT_LOG_FORMAT: &str = "ARROW";
        let log_format = self
            .properties
            .get("table.log.format")
            .map(String::as_str)
            .unwrap_or(DEFAULT_LOG_FORMAT);
        LogFormat::parse(log_format)
    }

    pub fn get_auto_partition_strategy(&self) -> AutoPartitionStrategy {
        AutoPartitionStrategy::from(&self.properties)
    }
}

impl TableInfo {
    pub fn of(
        table_path: TablePath,
        table_id: i64,
        schema_id: i32,
        table_descriptor: TableDescriptor,
        created_time: i64,
        modified_time: i64,
    ) -> TableInfo {
        let TableDescriptor {
            schema,
            table_distribution,
            comment,
            partition_keys,
            properties,
            custom_properties,
        } = table_descriptor;
        let TableDistribution {
            bucket_count,
            bucket_keys,
        } = table_distribution.unwrap();
        TableInfo::new(
            table_path,
            table_id,
            schema_id,
            schema,
            bucket_keys,
            partition_keys,
            bucket_count.unwrap(),
            properties,
            custom_properties,
            comment,
            created_time,
            modified_time,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_path: TablePath,
        table_id: TableId,
        schema_id: i32,
        schema: Schema,
        bucket_keys: Vec<String>,
        partition_keys: Arc<[String]>,
        num_buckets: i32,
        properties: HashMap<String, String>,
        custom_properties: HashMap<String, String>,
        comment: Option<String>,
        created_time: i64,
        modified_time: i64,
    ) -> Self {
        let row_type = schema.row_type.clone();
        let primary_keys: Vec<String> = schema
            .primary_key_column_names()
            .iter()
            .map(|col| (*col).to_string())
            .collect();
        let physical_primary_keys =
            Self::generate_physical_primary_key(&primary_keys, &partition_keys);
        let table_config = TableConfig::from_properties(properties.clone());

        TableInfo {
            table_path,
            table_id,
            schema_id,
            schema,
            row_type,
            primary_keys,
            physical_primary_keys,
            bucket_keys,
            partition_keys,
            num_buckets,
            properties,
            table_config,
            custom_properties,
            comment,
            created_time,
            modified_time,
        }
    }

    pub fn get_table_path(&self) -> &TablePath {
        &self.table_path
    }

    pub fn get_table_id(&self) -> i64 {
        self.table_id
    }

    pub fn get_schema_id(&self) -> i32 {
        self.schema_id
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn get_row_type(&self) -> &RowType {
        &self.row_type
    }

    pub fn has_primary_key(&self) -> bool {
        !self.primary_keys.is_empty()
    }

    pub fn get_primary_keys(&self) -> &Vec<String> {
        &self.primary_keys
    }

    pub fn get_physical_primary_keys(&self) -> &[String] {
        &self.physical_primary_keys
    }

    pub fn has_bucket_key(&self) -> bool {
        !self.bucket_keys.is_empty()
    }

    pub fn is_default_bucket_key(&self) -> bool {
        if self.has_primary_key() {
            self.bucket_keys == self.physical_primary_keys
        } else {
            self.bucket_keys.is_empty()
        }
    }

    pub fn get_bucket_keys(&self) -> &[String] {
        &self.bucket_keys
    }

    pub fn is_partitioned(&self) -> bool {
        !self.partition_keys.is_empty()
    }

    pub fn is_auto_partitioned(&self) -> bool {
        self.is_partitioned()
            && self
                .table_config
                .get_auto_partition_strategy()
                .is_auto_partition_enabled()
    }

    pub fn get_partition_keys(&self) -> &Arc<[String]> {
        &self.partition_keys
    }

    pub fn get_num_buckets(&self) -> i32 {
        self.num_buckets
    }

    pub fn get_properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    pub fn get_table_config(&self) -> &TableConfig {
        &self.table_config
    }

    pub fn get_custom_properties(&self) -> &HashMap<String, String> {
        &self.custom_properties
    }

    pub fn get_comment(&self) -> Option<&str> {
        self.comment.as_deref()
    }

    pub fn get_created_time(&self) -> i64 {
        self.created_time
    }

    pub fn get_modified_time(&self) -> i64 {
        self.modified_time
    }

    pub fn to_table_descriptor(&self) -> Result<TableDescriptor> {
        let mut builder = TableDescriptor::builder()
            .schema(self.schema.clone())
            .partitioned_by(self.partition_keys.to_vec())
            .distributed_by(Some(self.num_buckets), self.bucket_keys.clone())
            .properties(self.properties.clone())
            .custom_properties(self.custom_properties.clone());

        if let Some(comment) = &self.comment {
            builder = builder.comment(comment.clone());
        }

        builder.build()
    }

    fn generate_physical_primary_key(
        primary_keys: &[String],
        partition_keys: &[String],
    ) -> Vec<String> {
        primary_keys
            .iter()
            .filter(|pk| !partition_keys.contains(*pk))
            .cloned()
            .collect()
    }
}

impl Display for TableInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TableInfo{{ table_path={:?}, table_id={}, schema_id={}, schema={:?}, physical_primary_keys={:?}, bucket_keys={:?}, partition_keys={:?}, num_buckets={}, properties={:?}, custom_properties={:?}, comment={:?}, created_time={}, modified_time={} }}",
            self.table_path,
            self.table_id,
            self.schema_id,
            self.schema,
            self.physical_primary_keys,
            self.bucket_keys,
            self.partition_keys,
            self.num_buckets,
            self.properties,
            self.custom_properties,
            self.comment,
            self.created_time,
            self.modified_time
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct TableBucket {
    table_id: TableId,
    partition_id: Option<PartitionId>,
    bucket: BucketId,
}

impl TableBucket {
    pub fn new(table_id: TableId, bucket: BucketId) -> Self {
        Self {
            table_id,
            partition_id: None,
            bucket,
        }
    }

    pub fn new_with_partition(
        table_id: TableId,
        partition_id: Option<PartitionId>,
        bucket: BucketId,
    ) -> Self {
        TableBucket {
            table_id,
            partition_id,
            bucket,
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn bucket_id(&self) -> BucketId {
        self.bucket
    }

    pub fn partition_id(&self) -> Option<PartitionId> {
        self.partition_id
    }

    pub fn to_pb(&self) -> crate::proto::PbTableBucket {
        crate::proto::PbTableBucket {
            table_id: self.table_id,
            partition_id: self.partition_id,
            bucket_id: self.bucket,
        }
    }

    pub fn from_pb(pb: &crate::proto::PbTableBucket) -> Self {
        Self {
            table_id: pb.table_id,
            partition_id: pb.partition_id,
            bucket: pb.bucket_id,
        }
    }
}

impl Display for TableBucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(partition_id) = self.partition_id {
            write!(
                f,
                "TableBucket(table_id={}, partition_id={}, bucket={})",
                self.table_id, partition_id, self.bucket
            )
        } else {
            write!(
                f,
                "TableBucket(table_id={}, bucket={})",
                self.table_id, self.bucket
            )
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LakeSnapshot {
    pub snapshot_id: i64,
    pub table_buckets_offset: HashMap<TableBucket, i64>,
}

impl LakeSnapshot {
    pub fn new(snapshot_id: i64, table_buckets_offset: HashMap<TableBucket, i64>) -> Self {
        Self {
            snapshot_id,
            table_buckets_offset,
        }
    }

    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }

    pub fn table_buckets_offset(&self) -> &HashMap<TableBucket, i64> {
        &self.table_buckets_offset
    }
}

/// Tests for [`TablePath`].
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::DataTypes;

    #[test]
    fn test_validate() {
        // assert valid name
        let path = TablePath::new("db_2-abc3".to_string(), "table-1_abc_2".to_string());
        assert!(TablePath::detect_invalid_name(path.database()).is_none());
        assert!(TablePath::detect_invalid_name(path.table()).is_none());
        assert_eq!(path.to_string(), "db_2-abc3.table-1_abc_2");

        // assert invalid name prefix
        assert!(
            TablePath::validate_prefix("__table-1")
                .unwrap()
                .contains("'__' is not allowed as prefix")
        );

        // check max length
        let long_name = "a".repeat(200);
        assert!(TablePath::detect_invalid_name(&long_name).is_none());

        // assert invalid names
        assert_invalid_name("*abc", "'*abc' contains one or more characters other than");
        assert_invalid_name(
            "table.abc",
            "'table.abc' contains one or more characters other than",
        );
        assert_invalid_name("", "the empty string is not allowed");
        assert_invalid_name(" ", "' ' contains one or more characters other than");
        assert_invalid_name(".", "'.' is not allowed");
        assert_invalid_name("..", "'..' is not allowed");
        let invalid_long_name = "a".repeat(201);
        assert_invalid_name(
            &invalid_long_name,
            &format!(
                "the length of '{invalid_long_name}' is longer than the max allowed length {MAX_NAME_LENGTH}"
            ),
        );
    }

    fn assert_invalid_name(name: &str, expected_message: &str) {
        let result = TablePath::detect_invalid_name(name);
        assert!(
            result.is_some(),
            "Expected '{name}' to be invalid, but it was valid"
        );
        assert!(
            result.as_ref().unwrap().contains(expected_message),
            "Expected message containing '{}', but got '{}'",
            expected_message,
            result.unwrap()
        );
    }

    #[test]
    fn test_is_auto_partitioned() {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .primary_key(vec!["id".to_string()])
            .build()
            .unwrap();

        let table_path = TablePath::new("db".to_string(), "tbl".to_string());

        // 1. Not partitioned, auto partition disabled
        let mut properties = HashMap::new();
        let table_info = TableInfo::new(
            table_path.clone(),
            1,
            1,
            schema.clone(),
            vec!["id".to_string()],
            Arc::from(vec![]), // No partition keys
            1,
            properties.clone(),
            HashMap::new(),
            None,
            0,
            0,
        );
        assert!(!table_info.is_auto_partitioned());

        // 2. Not partitioned, auto partition enabled
        properties.insert(
            "table.auto-partition.enabled".to_string(),
            "true".to_string(),
        );
        let table_info = TableInfo::new(
            table_path.clone(),
            1,
            1,
            schema.clone(),
            vec!["id".to_string()],
            Arc::from(vec![]), // No partition keys
            1,
            properties.clone(),
            HashMap::new(),
            None,
            0,
            0,
        );
        assert!(!table_info.is_auto_partitioned());

        // 3. Partitioned, auto partition disabled
        properties.insert(
            "table.auto-partition.enabled".to_string(),
            "false".to_string(),
        );
        let table_info = TableInfo::new(
            table_path.clone(),
            1,
            1,
            schema.clone(),
            vec!["id".to_string()],
            Arc::from(vec!["name".to_string()]), // Partition keys
            1,
            properties.clone(),
            HashMap::new(),
            None,
            0,
            0,
        );
        assert!(!table_info.is_auto_partitioned());

        // 4. Partitioned, auto partition enabled
        properties.insert(
            "table.auto-partition.enabled".to_string(),
            "true".to_string(),
        );
        let table_info = TableInfo::new(
            table_path.clone(),
            1,
            1,
            schema.clone(),
            vec!["id".to_string()],
            Arc::from(vec!["name".to_string()]), // Partition keys
            1,
            properties.clone(),
            HashMap::new(),
            None,
            0,
            0,
        );
        assert!(table_info.is_auto_partitioned());
    }
}
