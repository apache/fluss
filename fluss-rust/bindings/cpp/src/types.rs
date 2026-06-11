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

use crate::ffi;
use anyhow::{Result, anyhow};
use arrow::array::Array;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use fcore::metadata::{
    ArrayType, Column, DataField, DataType, DataTypes, DecimalType, MapType, RowType,
};
use fcore::row::Datum;
use fluss as fcore;
use std::borrow::Cow;
use std::str::FromStr;

pub const DATA_TYPE_BOOLEAN: i32 = 1;
pub const DATA_TYPE_TINYINT: i32 = 2;
pub const DATA_TYPE_SMALLINT: i32 = 3;
pub const DATA_TYPE_INT: i32 = 4;
pub const DATA_TYPE_BIGINT: i32 = 5;
pub const DATA_TYPE_FLOAT: i32 = 6;
pub const DATA_TYPE_DOUBLE: i32 = 7;
pub const DATA_TYPE_STRING: i32 = 8;
pub const DATA_TYPE_BYTES: i32 = 9;
pub const DATA_TYPE_DATE: i32 = 10;
pub const DATA_TYPE_TIME: i32 = 11;
pub const DATA_TYPE_TIMESTAMP: i32 = 12;
pub const DATA_TYPE_TIMESTAMP_LTZ: i32 = 13;
pub const DATA_TYPE_DECIMAL: i32 = 14;
pub const DATA_TYPE_CHAR: i32 = 15;
pub const DATA_TYPE_BINARY: i32 = 16;
pub const DATA_TYPE_ARRAY: i32 = 17;
pub const DATA_TYPE_MAP: i32 = 18;
pub const DATA_TYPE_ROW: i32 = 19;

fn ffi_column_to_core_data_type(col: &ffi::FfiColumn) -> Result<DataType> {
    let mut cursor = 0usize;
    let dt = nodes_to_data_type(&col.type_nodes, &mut cursor)?;
    if cursor != col.type_nodes.len() {
        return Err(anyhow!(
            "Column '{}': type tree has {} trailing nodes",
            col.name,
            col.type_nodes.len() - cursor
        ));
    }
    Ok(dt)
}

/// Reconstruct one type from a preorder node arena, advancing `cursor` past
/// the consumed nodes. Mirrors the C++ `data_type_to_nodes` encoder.
fn nodes_to_data_type(nodes: &[ffi::FfiTypeNode], cursor: &mut usize) -> Result<DataType> {
    let node = nodes
        .get(*cursor)
        .ok_or_else(|| anyhow!("type tree ended before all nodes were read"))?;
    *cursor += 1;

    if node.precision < 0 || node.scale < 0 {
        return Err(anyhow!(
            "type node precision and scale must be non-negative"
        ));
    }
    let precision = node.precision as u32;
    let scale = node.scale as u32;

    let dt = match node.type_id {
        DATA_TYPE_ARRAY => {
            let element = nodes_to_data_type(nodes, cursor)?;
            DataType::Array(ArrayType::with_nullable(node.nullable, element))
        }
        DATA_TYPE_MAP => {
            let key = nodes_to_data_type(nodes, cursor)?;
            let value = nodes_to_data_type(nodes, cursor)?;
            DataType::Map(MapType::with_nullable(node.nullable, key, value))
        }
        DATA_TYPE_ROW => {
            let mut fields = Vec::with_capacity(node.child_count as usize);
            for _ in 0..node.child_count {
                let field_name = nodes
                    .get(*cursor)
                    .ok_or_else(|| anyhow!("ROW field missing from type tree"))?
                    .field_name
                    .clone();
                let field_type = nodes_to_data_type(nodes, cursor)?;
                // C++ ROW columns carry no per-field description.
                fields.push(DataField::new(field_name, field_type, None));
            }
            DataType::Row(RowType::with_nullable(node.nullable, fields))
        }
        scalar => return scalar_to_core(scalar, precision, scale, node.nullable),
    };
    Ok(dt)
}

fn type_precision_scale(dt: &DataType) -> (i32, i32) {
    match dt {
        DataType::Decimal(d) => (d.precision() as i32, d.scale() as i32),
        DataType::Time(t) => (t.precision() as i32, 0),
        DataType::Timestamp(ts) => (ts.precision() as i32, 0),
        DataType::TimestampLTz(ts) => (ts.precision() as i32, 0),
        DataType::Char(ch) => (ch.length() as i32, 0),
        DataType::Binary(bin) => (bin.length() as i32, 0),
        _ => (0, 0),
    }
}

fn scalar_to_core(data_type: i32, precision: u32, scale: u32, nullable: bool) -> Result<DataType> {
    let dt = match data_type {
        DATA_TYPE_BOOLEAN => DataTypes::boolean(),
        DATA_TYPE_TINYINT => DataTypes::tinyint(),
        DATA_TYPE_SMALLINT => DataTypes::smallint(),
        DATA_TYPE_INT => DataTypes::int(),
        DATA_TYPE_BIGINT => DataTypes::bigint(),
        DATA_TYPE_FLOAT => DataTypes::float(),
        DATA_TYPE_DOUBLE => DataTypes::double(),
        DATA_TYPE_STRING => DataTypes::string(),
        DATA_TYPE_BYTES => DataTypes::bytes(),
        DATA_TYPE_DATE => DataTypes::date(),
        DATA_TYPE_TIME => DataTypes::time_with_precision(precision),
        DATA_TYPE_TIMESTAMP => DataTypes::timestamp_with_precision(precision),
        DATA_TYPE_TIMESTAMP_LTZ => DataTypes::timestamp_ltz_with_precision(precision),
        DATA_TYPE_DECIMAL => DataType::Decimal(DecimalType::new(precision, scale)?),
        DATA_TYPE_CHAR => DataTypes::char(precision),
        DATA_TYPE_BINARY => DataTypes::binary(precision as usize),
        _ => return Err(anyhow!("Unknown data type: {}", data_type)),
    };
    if nullable {
        Ok(dt)
    } else {
        Ok(dt.as_non_nullable())
    }
}

/// Serialize a type tree to a preorder node arena. Mirrors the C++
/// `nodes_to_data_type` decoder; `field_name` is set only for ROW fields.
fn core_data_type_to_nodes(dt: &DataType, field_name: &str, out: &mut Vec<ffi::FfiTypeNode>) {
    let (precision, scale) = type_precision_scale(dt);
    let child_count = match dt {
        DataType::Array(_) => 1,
        DataType::Map(_) => 2,
        DataType::Row(rt) => rt.fields().len() as u32,
        _ => 0,
    };
    out.push(ffi::FfiTypeNode {
        type_id: core_data_type_to_ffi(dt),
        nullable: dt.is_nullable(),
        precision,
        scale,
        field_name: field_name.to_string(),
        child_count,
    });
    match dt {
        DataType::Array(at) => core_data_type_to_nodes(at.get_element_type(), "", out),
        DataType::Map(mt) => {
            core_data_type_to_nodes(mt.key_type(), "", out);
            core_data_type_to_nodes(mt.value_type(), "", out);
        }
        DataType::Row(rt) => {
            for field in rt.fields() {
                core_data_type_to_nodes(field.data_type(), field.name(), out);
            }
        }
        _ => {}
    }
}

/// Build a nested `ARRAY<…<scalar>>` from a flat leaf description. Used by the
/// data-writer path (`element_type_from_ffi`), which carries array-of-scalar
/// element types without a full node arena.
fn build_array_type_from_leaf(
    element_data_type: i32,
    element_precision: u32,
    element_scale: u32,
    array_nesting: u32,
) -> Result<DataType> {
    if array_nesting == 0 {
        return Err(anyhow!("ARRAY nesting must be >= 1"));
    }
    let mut dt = scalar_to_core(element_data_type, element_precision, element_scale, true)?;
    for _ in 0..array_nesting {
        dt = DataType::Array(ArrayType::new(dt));
    }
    Ok(dt)
}

pub fn core_data_type_to_ffi(dt: &DataType) -> i32 {
    match dt {
        DataType::Boolean(_) => DATA_TYPE_BOOLEAN,
        DataType::TinyInt(_) => DATA_TYPE_TINYINT,
        DataType::SmallInt(_) => DATA_TYPE_SMALLINT,
        DataType::Int(_) => DATA_TYPE_INT,
        DataType::BigInt(_) => DATA_TYPE_BIGINT,
        DataType::Float(_) => DATA_TYPE_FLOAT,
        DataType::Double(_) => DATA_TYPE_DOUBLE,
        DataType::String(_) => DATA_TYPE_STRING,
        DataType::Bytes(_) => DATA_TYPE_BYTES,
        DataType::Date(_) => DATA_TYPE_DATE,
        DataType::Time(_) => DATA_TYPE_TIME,
        DataType::Timestamp(_) => DATA_TYPE_TIMESTAMP,
        DataType::TimestampLTz(_) => DATA_TYPE_TIMESTAMP_LTZ,
        DataType::Decimal(_) => DATA_TYPE_DECIMAL,
        DataType::Char(_) => DATA_TYPE_CHAR,
        DataType::Binary(_) => DATA_TYPE_BINARY,
        DataType::Array(_) => DATA_TYPE_ARRAY,
        DataType::Map(_) => DATA_TYPE_MAP,
        DataType::Row(_) => DATA_TYPE_ROW,
    }
}

fn core_column_to_ffi(col: &Column) -> ffi::FfiColumn {
    let mut type_nodes = Vec::new();
    core_data_type_to_nodes(col.data_type(), "", &mut type_nodes);
    ffi::FfiColumn {
        name: col.name().to_string(),
        comment: col.comment().unwrap_or("").to_string(),
        type_nodes,
    }
}

pub fn ffi_descriptor_to_core(
    descriptor: &ffi::FfiTableDescriptor,
) -> Result<fcore::metadata::TableDescriptor> {
    let mut schema_builder = fcore::metadata::Schema::builder();

    for col in &descriptor.schema.columns {
        let dt = ffi_column_to_core_data_type(col)?;
        schema_builder = schema_builder.column(&col.name, dt);
        if !col.comment.is_empty() {
            schema_builder = schema_builder.with_comment(&col.comment);
        }
    }

    if !descriptor.schema.primary_keys.is_empty() {
        schema_builder = schema_builder.primary_key(descriptor.schema.primary_keys.clone());
    }

    build_descriptor(schema_builder.build()?, descriptor)
}

/// Assemble a core `TableDescriptor` from a pre-built `Schema` plus the
/// descriptor's table-level metadata (partition/bucket keys, properties,
/// comment).
fn build_descriptor(
    schema: fcore::metadata::Schema,
    descriptor: &ffi::FfiTableDescriptor,
) -> Result<fcore::metadata::TableDescriptor> {
    let mut builder = fcore::metadata::TableDescriptor::builder()
        .schema(schema)
        .partitioned_by(descriptor.partition_keys.clone());

    if descriptor.bucket_count > 0 {
        builder = builder.distributed_by(
            Some(descriptor.bucket_count),
            descriptor.bucket_keys.clone(),
        );
    } else {
        builder = builder.distributed_by(None, descriptor.bucket_keys.clone());
    }

    for prop in &descriptor.properties {
        builder = builder.property(&prop.key, &prop.value);
    }

    if !descriptor.custom_properties.is_empty() {
        let custom: std::collections::HashMap<String, String> = descriptor
            .custom_properties
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        builder = builder.custom_properties(custom);
    }

    if !descriptor.comment.is_empty() {
        builder = builder.comment(&descriptor.comment);
    }

    Ok(builder.build()?)
}

/// Import a heap `FFI_ArrowSchema` (exported by C++) and return its fields as
/// Fluss DataTypes. Lets ArrayWriter/MapWriter carry ROW/MAP element and
/// key/value types that the flat FFI encoding cannot express.
///
/// # Safety
/// `schema_ptr` must be a valid `FFI_ArrowSchema` heap pointer exported by C++
/// (e.g. via `arrow::ExportSchema`); ownership is taken and released here.
pub unsafe fn arrow_ffi_to_data_types(schema_ptr: usize) -> Result<Vec<fcore::metadata::DataType>> {
    let ffi_schema = unsafe { Box::from_raw(schema_ptr as *mut arrow::ffi::FFI_ArrowSchema) };
    let arrow_schema = arrow::datatypes::Schema::try_from(ffi_schema.as_ref())
        .map_err(|e| anyhow!("Failed to import Arrow schema: {e}"))?;
    let mut out = Vec::with_capacity(arrow_schema.fields().len());
    for field in arrow_schema.fields() {
        out.push(fcore::record::from_arrow_field(field.as_ref())?);
    }
    Ok(out)
}

pub fn core_table_info_to_ffi(info: &fcore::metadata::TableInfo) -> ffi::FfiTableInfo {
    let schema = info.get_schema();
    let columns: Vec<ffi::FfiColumn> = schema.columns().iter().map(core_column_to_ffi).collect();

    let primary_keys: Vec<String> = schema
        .primary_key()
        .map(|pk| pk.column_names().to_vec())
        .unwrap_or_default();

    let properties: Vec<ffi::HashMapValue> = info
        .get_properties()
        .iter()
        .map(|(k, v)| ffi::HashMapValue {
            key: k.clone(),
            value: v.clone(),
        })
        .collect();

    let custom_properties: Vec<ffi::HashMapValue> = info
        .get_custom_properties()
        .iter()
        .map(|(k, v)| ffi::HashMapValue {
            key: k.clone(),
            value: v.clone(),
        })
        .collect();

    ffi::FfiTableInfo {
        table_id: info.get_table_id(),
        schema_id: info.get_schema_id(),
        table_path: ffi::FfiTablePath {
            database_name: info.get_table_path().database().to_string(),
            table_name: info.get_table_path().table().to_string(),
        },
        created_time: info.get_created_time(),
        modified_time: info.get_modified_time(),
        primary_keys: info.get_primary_keys().clone(),
        bucket_keys: info.get_bucket_keys().to_vec(),
        partition_keys: info.get_partition_keys().to_vec(),
        has_partition_expressions: info.has_partition_expressions(),
        num_buckets: info.get_num_buckets(),
        has_primary_key: info.has_primary_key(),
        is_partitioned: info.is_partitioned(),
        properties,
        custom_properties,
        comment: info.get_comment().unwrap_or("").to_string(),
        schema: ffi::FfiSchema {
            columns,
            primary_keys,
        },
    }
}

pub fn empty_table_info() -> ffi::FfiTableInfo {
    ffi::FfiTableInfo {
        table_id: 0,
        schema_id: 0,
        table_path: ffi::FfiTablePath {
            database_name: String::new(),
            table_name: String::new(),
        },
        created_time: 0,
        modified_time: 0,
        primary_keys: vec![],
        bucket_keys: vec![],
        partition_keys: vec![],
        has_partition_expressions: false,
        num_buckets: 0,
        has_primary_key: false,
        is_partitioned: false,
        properties: vec![],
        custom_properties: vec![],
        comment: String::new(),
        schema: ffi::FfiSchema {
            columns: vec![],
            primary_keys: vec![],
        },
    }
}

/// Convert element type tag + precision/scale to core DataType.
/// Used by ArrayWriterInner construction from C++.
///
/// Nullability is hardcoded to `true` (the default) because `ArrayWriter`
/// only needs the type for encoding — the binary array format does not
/// vary based on nullability. Nullability is a schema-level constraint
/// enforced elsewhere (column definition, primary key normalization).
pub fn element_type_from_ffi(
    leaf_dt: i32,
    precision: u32,
    scale: u32,
    array_nesting: u32,
) -> Result<fcore::metadata::DataType> {
    if array_nesting == 0 {
        scalar_to_core(leaf_dt, precision, scale, true)
    } else {
        build_array_type_from_leaf(leaf_dt, precision, scale, array_nesting)
    }
}

/// Convert FFI database descriptor to core. Returns None if descriptor is effectively empty
/// (no comment and no properties), so create_database can pass Option::None to core.
pub fn ffi_database_descriptor_to_core(
    d: &ffi::FfiDatabaseDescriptor,
) -> Option<fcore::metadata::DatabaseDescriptor> {
    if d.comment.is_empty() && d.properties.is_empty() {
        return None;
    }
    let mut builder = fcore::metadata::DatabaseDescriptor::builder();
    if !d.comment.is_empty() {
        builder = builder.comment(&d.comment);
    }
    if !d.properties.is_empty() {
        let props: std::collections::HashMap<String, String> = d
            .properties
            .iter()
            .map(|kv| (kv.key.clone(), kv.value.clone()))
            .collect();
        builder = builder.custom_properties(props);
    }
    Some(builder.build())
}

/// Convert core DatabaseInfo to FFI.
pub fn core_database_info_to_ffi(info: &fcore::metadata::DatabaseInfo) -> ffi::FfiDatabaseInfo {
    let desc = info.database_descriptor();
    let properties: Vec<ffi::HashMapValue> = desc
        .custom_properties()
        .iter()
        .map(|(k, v)| ffi::HashMapValue {
            key: k.clone(),
            value: v.clone(),
        })
        .collect();
    ffi::FfiDatabaseInfo {
        database_name: info.database_name().to_string(),
        comment: desc.comment().unwrap_or("").to_string(),
        properties,
        created_time: info.created_time(),
        modified_time: info.modified_time(),
    }
}

/// Resolve types in a GenericRow using schema metadata.
/// Narrows Int32 → Int8/Int16, parses decimal strings, etc.
/// Used by both AppendWriter and UpsertWriter.
pub fn resolve_row_types(
    row: &fcore::row::GenericRow<'_>,
    schema: Option<&fcore::metadata::Schema>,
) -> Result<fcore::row::GenericRow<'static>> {
    let mut out = fcore::row::GenericRow::new(row.values.len());

    for (idx, datum) in row.values.iter().enumerate() {
        let target = schema
            .and_then(|s| s.columns().get(idx))
            .map(|c| c.data_type());
        out.set_field(idx, resolve_datum(datum, target, idx)?);
    }

    Ok(out)
}

/// Resolve a single datum against its (optional) target column type, recursing
/// into nested ROW values. Narrows Int32 → Int8/Int16, parses decimal strings,
/// and leaves already-typed ARRAY/MAP binaries (built by the writers) untouched.
fn resolve_datum(
    datum: &fcore::row::Datum<'_>,
    target: Option<&fcore::metadata::DataType>,
    idx: usize,
) -> Result<fcore::row::Datum<'static>> {
    Ok(match datum {
        Datum::Null => Datum::Null,
        Datum::Bool(v) => Datum::Bool(*v),
        Datum::Int32(v) => match target {
            Some(fcore::metadata::DataType::TinyInt(_)) => Datum::Int8(
                i8::try_from(*v).map_err(|_| anyhow!("Column {idx}: {v} overflows TinyInt"))?,
            ),
            Some(fcore::metadata::DataType::SmallInt(_)) => Datum::Int16(
                i16::try_from(*v).map_err(|_| anyhow!("Column {idx}: {v} overflows SmallInt"))?,
            ),
            _ => Datum::Int32(*v),
        },
        Datum::Int64(v) => Datum::Int64(*v),
        Datum::Float32(v) => Datum::Float32(*v),
        Datum::Float64(v) => Datum::Float64(*v),
        Datum::Int8(v) => Datum::Int8(*v),
        Datum::Int16(v) => Datum::Int16(*v),
        Datum::String(cow) => match target {
            // String standing in for a Decimal column — parse it.
            Some(fcore::metadata::DataType::Decimal(dt)) => {
                let (precision, scale) = (dt.precision(), dt.scale());
                let bd = bigdecimal::BigDecimal::from_str(cow.as_ref())
                    .map_err(|e| anyhow!("Column {idx}: invalid decimal string '{cow}': {e}"))?;
                let decimal = fcore::row::Decimal::from_big_decimal(bd, precision, scale)
                    .map_err(|e| anyhow!("Column {idx}: {e}"))?;
                Datum::Decimal(decimal)
            }
            _ => Datum::String(Cow::Owned(cow.to_string())),
        },
        Datum::Blob(cow) => Datum::Blob(Cow::Owned(cow.to_vec())),
        Datum::Decimal(d) => Datum::Decimal(d.clone()),
        Datum::Date(d) => Datum::Date(*d),
        Datum::Time(t) => Datum::Time(*t),
        Datum::TimestampNtz(ts) => Datum::TimestampNtz(*ts),
        Datum::TimestampLtz(ts) => Datum::TimestampLtz(*ts),
        Datum::Array(a) => Datum::Array(a.clone()),
        Datum::Map(m) => Datum::Map(m.clone()),
        Datum::Row(nested) => {
            // A nested row carries untyped datums; resolve each field against
            // the ROW's own field types so decimals/narrowing work recursively.
            let field_types = match target {
                Some(fcore::metadata::DataType::Row(rt)) => Some(rt.fields()),
                _ => None,
            };
            let mut out = fcore::row::GenericRow::new(nested.values.len());
            for (i, d) in nested.values.iter().enumerate() {
                let ft = field_types.and_then(|f| f.get(i)).map(|f| f.data_type());
                out.set_field(i, resolve_datum(d, ft, i)?);
            }
            Datum::Row(Box::new(out))
        }
    })
}

/// Convert a CompactedRow (lookup result) to an owned GenericRow<'static>.
/// One copy for strings/bytes (Cow::Owned), but no second copy into FfiDatum.
pub fn compacted_row_to_owned(
    row: &dyn fcore::row::InternalRow,
    table_info: &fcore::metadata::TableInfo,
) -> Result<fcore::row::GenericRow<'static>> {
    internal_row_to_owned_generic(row, table_info.get_schema().columns())
}

/// Read a single field of an InternalRow into an owned `'static` Datum, using
/// the column's declared type. This is the per-field core of
/// `internal_row_to_owned_generic`; the scan read path uses it to materialize
/// one complex cell without unpacking the whole row.
pub fn field_to_owned_datum(
    row: &dyn fcore::row::InternalRow,
    columns: &[fcore::metadata::Column],
    field: usize,
) -> Result<fcore::row::Datum<'static>> {
    let col = columns.get(field).ok_or_else(|| {
        anyhow!(
            "field index {field} out of range ({} columns)",
            columns.len()
        )
    })?;
    if row.is_null_at(field)? {
        return Ok(Datum::Null);
    }

    Ok(match col.data_type() {
        fcore::metadata::DataType::Boolean(_) => Datum::Bool(row.get_boolean(field)?),
        fcore::metadata::DataType::TinyInt(_) => Datum::Int8(row.get_byte(field)?),
        fcore::metadata::DataType::SmallInt(_) => Datum::Int16(row.get_short(field)?),
        fcore::metadata::DataType::Int(_) => Datum::Int32(row.get_int(field)?),
        fcore::metadata::DataType::BigInt(_) => Datum::Int64(row.get_long(field)?),
        fcore::metadata::DataType::Float(_) => Datum::Float32(row.get_float(field)?.into()),
        fcore::metadata::DataType::Double(_) => Datum::Float64(row.get_double(field)?.into()),
        fcore::metadata::DataType::String(_) => {
            Datum::String(Cow::Owned(row.get_string(field)?.to_string()))
        }
        fcore::metadata::DataType::Bytes(_) => {
            Datum::Blob(Cow::Owned(row.get_bytes(field)?.to_vec()))
        }
        fcore::metadata::DataType::Date(_) => Datum::Date(row.get_date(field)?),
        fcore::metadata::DataType::Time(_) => Datum::Time(row.get_time(field)?),
        fcore::metadata::DataType::Timestamp(dt) => {
            Datum::TimestampNtz(row.get_timestamp_ntz(field, dt.precision())?)
        }
        fcore::metadata::DataType::TimestampLTz(dt) => {
            Datum::TimestampLtz(row.get_timestamp_ltz(field, dt.precision())?)
        }
        fcore::metadata::DataType::Decimal(dt) => {
            Datum::Decimal(row.get_decimal(field, dt.precision() as usize, dt.scale() as usize)?)
        }
        fcore::metadata::DataType::Char(dt) => Datum::String(Cow::Owned(
            row.get_char(field, dt.length() as usize)?.to_string(),
        )),
        fcore::metadata::DataType::Binary(dt) => {
            Datum::Blob(Cow::Owned(row.get_binary(field, dt.length())?.to_vec()))
        }
        fcore::metadata::DataType::Array(_) => {
            Datum::Array(row.get_array(field)?.try_into_binary()?)
        }
        fcore::metadata::DataType::Map(_) => Datum::Map(row.get_map(field)?.try_into_binary()?),
        fcore::metadata::DataType::Row(rt) => {
            Datum::Row(Box::new(row.get_row(field)?.try_into_generic(rt)?))
        }
    })
}

/// Walk an InternalRow field-by-field into an owned GenericRow<'static>, using
/// the given column types. Recurses through nested ROW/MAP/ARRAY values, so it
/// also materializes a ROW element read out of an array or map.
pub fn internal_row_to_owned_generic(
    row: &dyn fcore::row::InternalRow,
    columns: &[fcore::metadata::Column],
) -> Result<fcore::row::GenericRow<'static>> {
    let mut out = fcore::row::GenericRow::new(columns.len());
    for i in 0..columns.len() {
        out.set_field(i, field_to_owned_datum(row, columns, i)?);
    }
    Ok(out)
}

pub fn core_lake_snapshot_to_ffi(snapshot: &fcore::metadata::LakeSnapshot) -> ffi::FfiLakeSnapshot {
    let bucket_offsets: Vec<ffi::FfiBucketOffset> = snapshot
        .table_buckets_offset
        .iter()
        .map(|(bucket, offset)| ffi::FfiBucketOffset {
            table_id: bucket.table_id(),
            partition_id: bucket.partition_id().unwrap_or(-1),
            bucket_id: bucket.bucket_id(),
            offset: *offset,
        })
        .collect();

    ffi::FfiLakeSnapshot {
        snapshot_id: snapshot.snapshot_id,
        bucket_offsets,
    }
}

pub fn core_scan_batches_to_ffi(
    batches: &[fcore::record::ScanBatch],
) -> Result<ffi::FfiArrowRecordBatches, String> {
    let mut ffi_batches = Vec::new();
    for batch in batches {
        let record_batch = batch.batch();
        // Convert RecordBatch to StructArray first, then get the data
        let struct_array = arrow::array::StructArray::from(record_batch.clone());
        let ffi_array = Box::new(FFI_ArrowArray::new(&struct_array.into_data()));
        let ffi_schema = Box::new(
            FFI_ArrowSchema::try_from(record_batch.schema().as_ref()).map_err(|e| e.to_string())?,
        );
        // Export as raw pointers
        ffi_batches.push(ffi::FfiArrowRecordBatch {
            array_ptr: Box::into_raw(ffi_array) as usize,
            schema_ptr: Box::into_raw(ffi_schema) as usize,
            table_id: batch.bucket().table_id(),
            partition_id: batch.bucket().partition_id().unwrap_or(-1),
            bucket_id: batch.bucket().bucket_id(),
            base_offset: batch.base_offset(),
        });
    }

    Ok(ffi::FfiArrowRecordBatches {
        batches: ffi_batches,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_info_ffi_exposes_implicit_partition_capability() {
        let descriptor = fcore::metadata::TableDescriptor::builder()
            .schema(
                fcore::metadata::Schema::builder()
                    .column("event_time", fcore::metadata::DataTypes::timestamp())
                    .build()
                    .unwrap(),
            )
            .partitioned_by(vec!["event_day"])
            .partition_expressions(vec![fcore::metadata::PartitionExpression::new(
                "event_day",
                fcore::metadata::PartitionTransform::DateTrunc(
                    fcore::metadata::DateTruncPartitionTransform::new(
                        "event_time",
                        "DAY",
                        Some("UTC".to_string()),
                    ),
                ),
            )])
            .distributed_by(Some(1), Vec::new())
            .build()
            .unwrap();
        let table_info = fcore::metadata::TableInfo::of(
            fcore::metadata::TablePath::new("db".to_string(), "tbl".to_string()),
            1,
            1,
            descriptor,
            0,
            0,
        );

        let ffi_table_info = core_table_info_to_ffi(&table_info);

        assert!(ffi_table_info.has_partition_expressions);
        assert_eq!(ffi_table_info.partition_keys, vec!["event_day".to_string()]);
    }
}
