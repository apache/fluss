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
use fluss::metadata::{self, DataTypes, Schema, TableDescriptor};
use rustler::{NifStruct, NifTaggedEnum, ResourceArc};

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

/// Decoded from `%Fluss.Schema{}` Elixir struct.
#[derive(NifStruct)]
#[module = "Fluss.Schema"]
pub struct NifSchema {
    pub columns: Vec<(String, DataType)>,
    pub primary_key: Vec<String>,
}

#[rustler::nif]
fn table_descriptor_new(
    schema: NifSchema,
    bucket_count: Option<i32>,
    properties: Vec<(String, String)>,
) -> Result<ResourceArc<TableDescriptorResource>, rustler::Error> {
    let mut schema_builder = Schema::builder();
    for (name, dt) in &schema.columns {
        schema_builder = schema_builder.column(name, to_fluss_type(dt));
    }
    if !schema.primary_key.is_empty() {
        schema_builder = schema_builder.primary_key(schema.primary_key);
    }
    let built_schema = schema_builder.build().map_err(to_nif_err)?;

    let mut builder = TableDescriptor::builder().schema(built_schema);
    if let Some(count) = bucket_count {
        builder = builder.distributed_by(Some(count), vec![]);
    }
    for (key, value) in properties {
        builder = builder.property(&key, &value);
    }
    let descriptor = builder.build().map_err(to_nif_err)?;
    Ok(ResourceArc::new(TableDescriptorResource {
        inner: descriptor,
    }))
}
