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

use crate::atoms::to_nif_err;
use fluss::error::Error;
use fluss::metadata::{self, DataTypes, Schema, TableDescriptor};
use rustler::{NifStruct, NifTaggedEnum, ResourceArc};
use std::collections::HashMap;

pub struct TableDescriptorResource {
    pub inner: TableDescriptor,
}

impl std::panic::RefUnwindSafe for TableDescriptorResource {}

#[rustler::resource_impl]
impl rustler::Resource for TableDescriptorResource {}

/// Fluss data type for NIF interop.
///
/// Simple types map to atoms: `:int`, `:string`, etc.
/// Parameterized types map to tuples: `{:decimal, 10, 2}`, `{:char, 20}`.
#[derive(NifTaggedEnum)]
pub enum DataType {
    Boolean,
    Tinyint,
    Smallint,
    Int,
    Bigint,
    Float,
    Double,
    String,
    Bytes,
    Date,
    Time,
    Timestamp,
    TimestampLtz,
    Decimal(u32, u32),
    Char(u32),
    Binary(usize),
}

fn to_fluss_type(dt: &DataType) -> metadata::DataType {
    match dt {
        DataType::Boolean => DataTypes::boolean(),
        DataType::Tinyint => DataTypes::tinyint(),
        DataType::Smallint => DataTypes::smallint(),
        DataType::Int => DataTypes::int(),
        DataType::Bigint => DataTypes::bigint(),
        DataType::Float => DataTypes::float(),
        DataType::Double => DataTypes::double(),
        DataType::String => DataTypes::string(),
        DataType::Bytes => DataTypes::bytes(),
        DataType::Date => DataTypes::date(),
        DataType::Time => DataTypes::time(),
        DataType::Timestamp => DataTypes::timestamp(),
        DataType::TimestampLtz => DataTypes::timestamp_ltz(),
        DataType::Decimal(precision, scale) => DataTypes::decimal(*precision, *scale),
        DataType::Char(length) => DataTypes::char(*length),
        DataType::Binary(length) => DataTypes::binary(*length),
    }
}

fn from_fluss_type(dt: &metadata::DataType) -> Result<DataType, Error> {
    match dt {
        metadata::DataType::Boolean(_) => Ok(DataType::Boolean),
        metadata::DataType::TinyInt(_) => Ok(DataType::Tinyint),
        metadata::DataType::SmallInt(_) => Ok(DataType::Smallint),
        metadata::DataType::Int(_) => Ok(DataType::Int),
        metadata::DataType::BigInt(_) => Ok(DataType::Bigint),
        metadata::DataType::Float(_) => Ok(DataType::Float),
        metadata::DataType::Double(_) => Ok(DataType::Double),
        metadata::DataType::String(_) => Ok(DataType::String),
        metadata::DataType::Bytes(_) => Ok(DataType::Bytes),
        metadata::DataType::Date(_) => Ok(DataType::Date),
        metadata::DataType::Time(_) => Ok(DataType::Time),
        metadata::DataType::Timestamp(_) => Ok(DataType::Timestamp),
        metadata::DataType::TimestampLTz(_) => Ok(DataType::TimestampLtz),
        metadata::DataType::Decimal(d) => Ok(DataType::Decimal(d.precision(), d.scale())),
        metadata::DataType::Char(c) => Ok(DataType::Char(c.length())),
        metadata::DataType::Binary(b) => Ok(DataType::Binary(b.length())),
        metadata::DataType::Array(_) | metadata::DataType::Map(_) | metadata::DataType::Row(_) => {
            Err(Error::UnsupportedOperation {
                message: format!("data type {dt:?} is not supported by the Elixir bindings"),
            })
        }
    }
}

/// Decoded from `%Fluss.Schema{}` Elixir struct.
#[derive(NifStruct)]
#[module = "Fluss.Schema"]
pub struct NifSchema {
    pub columns: Vec<(String, DataType)>,
    pub primary_key: Vec<String>,
}

impl NifSchema {
    pub fn from_core(schema: &Schema) -> Result<NifSchema, Error> {
        let mut columns: Vec<(String, DataType)> = Vec::new();

        for col in schema.columns() {
            columns.push((col.name().to_string(), from_fluss_type(col.data_type())?));
        }

        let primary_key: Vec<String> = schema
            .primary_key_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        Ok(Self {
            columns,
            primary_key,
        })
    }
}

#[derive(NifStruct)]
#[module = "Fluss.TableDescriptor.Options"]
pub struct NifTableOptions {
    pub bucket_count: Option<i32>,
    pub bucket_keys: Vec<String>,
    pub partition_keys: Vec<String>,
    pub properties: HashMap<String, String>,
    pub custom_properties: HashMap<String, String>,
    pub comment: Option<String>,
}

#[rustler::nif]
fn table_descriptor_new(
    schema: NifSchema,
    opts: NifTableOptions,
) -> Result<ResourceArc<TableDescriptorResource>, rustler::Error> {
    let mut schema_builder = Schema::builder();
    for (name, dt) in &schema.columns {
        schema_builder = schema_builder.column(name, to_fluss_type(dt));
    }
    if !schema.primary_key.is_empty() {
        schema_builder = schema_builder.primary_key(schema.primary_key);
    }

    let built_schema = schema_builder.build().map_err(to_nif_err)?;

    let mut builder = TableDescriptor::builder()
        .schema(built_schema)
        .properties(opts.properties)
        .custom_properties(opts.custom_properties)
        .partitioned_by(opts.partition_keys)
        .distributed_by(opts.bucket_count, opts.bucket_keys);

    if let Some(comment) = opts.comment {
        builder = builder.comment(comment);
    }
    let descriptor = builder.build().map_err(to_nif_err)?;
    Ok(ResourceArc::new(TableDescriptorResource {
        inner: descriptor,
    }))
}
