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

use crate::*;
use arrow_pyarrow::{FromPyArrow, ToPyArrow};
use arrow_schema::SchemaRef;
use std::sync::Arc;

/// Utilities for schema conversion between PyArrow, Arrow, and Fluss
pub struct Utils;

impl Utils {
    /// Convert PyArrow schema to Rust Arrow schema
    pub fn pyarrow_to_arrow_schema(py_schema: &Py<PyAny>) -> PyResult<SchemaRef> {
        Python::attach(|py| {
            let schema_bound = py_schema.bind(py);
            let schema: arrow_schema::Schema = FromPyArrow::from_pyarrow_bound(schema_bound)
                .map_err(|e| {
                    FlussError::new_err(format!("Failed to convert PyArrow schema: {e}"))
                })?;
            Ok(Arc::new(schema))
        })
    }

    /// Convert an Arrow Field to a Fluss DataType, preserving nullability.
    pub fn arrow_field_to_fluss_type(
        field: &arrow::datatypes::Field,
    ) -> PyResult<fcore::metadata::DataType> {
        use arrow::datatypes::DataType as ArrowDataType;
        use fcore::metadata::DataTypes;

        let fluss_type = match field.data_type() {
            ArrowDataType::Boolean => DataTypes::boolean(),
            ArrowDataType::Int8 => DataTypes::tinyint(),
            ArrowDataType::Int16 => DataTypes::smallint(),
            ArrowDataType::Int32 => DataTypes::int(),
            ArrowDataType::Int64 => DataTypes::bigint(),
            ArrowDataType::UInt8 => DataTypes::tinyint(),
            ArrowDataType::UInt16 => DataTypes::smallint(),
            ArrowDataType::UInt32 => DataTypes::int(),
            ArrowDataType::UInt64 => DataTypes::bigint(),
            ArrowDataType::Float32 => DataTypes::float(),
            ArrowDataType::Float64 => DataTypes::double(),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => DataTypes::string(),
            ArrowDataType::Binary | ArrowDataType::LargeBinary => DataTypes::bytes(),
            ArrowDataType::FixedSizeBinary(n) => DataTypes::binary(*n as usize),
            ArrowDataType::Date32 => DataTypes::date(),
            ArrowDataType::Date64 => DataTypes::date(),
            ArrowDataType::Time32(unit) => match unit {
                arrow_schema::TimeUnit::Second => DataTypes::time_with_precision(0),
                arrow_schema::TimeUnit::Millisecond => DataTypes::time_with_precision(3),
                _ => {
                    return Err(FlussError::new_err(format!(
                        "Unsupported Time32 unit: {unit:?}"
                    )));
                }
            },
            ArrowDataType::Time64(unit) => match unit {
                arrow_schema::TimeUnit::Microsecond => DataTypes::time_with_precision(6),
                arrow_schema::TimeUnit::Nanosecond => DataTypes::time_with_precision(9),
                _ => {
                    return Err(FlussError::new_err(format!(
                        "Unsupported Time64 unit: {unit:?}"
                    )));
                }
            },
            ArrowDataType::Timestamp(unit, tz) => {
                let precision = match unit {
                    arrow_schema::TimeUnit::Second => 0,
                    arrow_schema::TimeUnit::Millisecond => 3,
                    arrow_schema::TimeUnit::Microsecond => 6,
                    arrow_schema::TimeUnit::Nanosecond => 9,
                };
                // Arrow Timestamp with timezone -> Fluss TimestampLtz
                // Arrow Timestamp without timezone -> Fluss Timestamp (NTZ)
                if tz.is_some() {
                    DataTypes::timestamp_ltz_with_precision(precision)
                } else {
                    DataTypes::timestamp_with_precision(precision)
                }
            }
            ArrowDataType::Decimal128(precision, scale) => {
                DataTypes::decimal(*precision as u32, *scale as u32)
            }
            ArrowDataType::List(element_field) => {
                let element_type = Utils::arrow_field_to_fluss_type(element_field)?;
                DataTypes::array(element_type)
            }
            other => {
                return Err(FlussError::new_err(format!(
                    "Unsupported Arrow data type: {other:?}"
                )));
            }
        };

        if field.is_nullable() {
            Ok(fluss_type)
        } else {
            Ok(fluss_type.as_non_nullable())
        }
    }

    /// Convert Fluss DataType to string representation, appending " NOT NULL"
    /// for non-nullable types (matches Java's `withNullability` and Rust core's
    /// `Display` impl).
    pub fn datatype_to_string(data_type: &fcore::metadata::DataType) -> String {
        let type_str = match data_type {
            fcore::metadata::DataType::Boolean(_) => "boolean".to_string(),
            fcore::metadata::DataType::TinyInt(_) => "tinyint".to_string(),
            fcore::metadata::DataType::SmallInt(_) => "smallint".to_string(),
            fcore::metadata::DataType::Int(_) => "int".to_string(),
            fcore::metadata::DataType::BigInt(_) => "bigint".to_string(),
            fcore::metadata::DataType::Float(_) => "float".to_string(),
            fcore::metadata::DataType::Double(_) => "double".to_string(),
            fcore::metadata::DataType::String(_) => "string".to_string(),
            fcore::metadata::DataType::Bytes(_) => "bytes".to_string(),
            fcore::metadata::DataType::Date(_) => "date".to_string(),
            fcore::metadata::DataType::Time(t) => {
                if t.precision() == 0 {
                    "time".to_string()
                } else {
                    format!("time({})", t.precision())
                }
            }
            fcore::metadata::DataType::Timestamp(t) => {
                if t.precision() == 6 {
                    "timestamp".to_string()
                } else {
                    format!("timestamp({})", t.precision())
                }
            }
            fcore::metadata::DataType::TimestampLTz(t) => {
                if t.precision() == 6 {
                    "timestamp_ltz".to_string()
                } else {
                    format!("timestamp_ltz({})", t.precision())
                }
            }
            fcore::metadata::DataType::Char(c) => format!("char({})", c.length()),
            fcore::metadata::DataType::Decimal(d) => {
                format!("decimal({},{})", d.precision(), d.scale())
            }
            fcore::metadata::DataType::Binary(b) => format!("binary({})", b.length()),
            fcore::metadata::DataType::Array(arr) => format!(
                "array<{}>",
                Utils::datatype_to_string(arr.get_element_type())
            ),
            fcore::metadata::DataType::Map(map) => format!(
                "map<{},{}>",
                Utils::datatype_to_string(map.key_type()),
                Utils::datatype_to_string(map.value_type())
            ),
            fcore::metadata::DataType::Row(row) => {
                let fields: Vec<String> = row
                    .fields()
                    .iter()
                    .map(|field| {
                        format!(
                            "{}: {}",
                            field.name(),
                            Utils::datatype_to_string(field.data_type())
                        )
                    })
                    .collect();
                format!("row<{}>", fields.join(", "))
            }
        };

        if data_type.is_nullable() {
            type_str
        } else {
            format!("{type_str} NOT NULL")
        }
    }

    /// Parse log format string to LogFormat enum
    pub fn parse_log_format(format_str: &str) -> PyResult<fcore::metadata::LogFormat> {
        fcore::metadata::LogFormat::parse(format_str)
            .map_err(|e| FlussError::new_err(format!("Invalid log format '{format_str}': {e}")))
    }

    /// Parse kv format string to KvFormat enum
    pub fn parse_kv_format(format_str: &str) -> PyResult<fcore::metadata::KvFormat> {
        fcore::metadata::KvFormat::parse(format_str)
            .map_err(|e| FlussError::new_err(format!("Invalid kv format '{format_str}': {e}")))
    }

    /// Convert Vec<ScanRecord> to Arrow RecordBatch
    pub fn convert_scan_records_to_arrow(
        _scan_records: Vec<fcore::record::ScanRecord>,
    ) -> Vec<Arc<arrow::record_batch::RecordBatch>> {
        let mut result = Vec::new();
        for record in _scan_records {
            let columnar_row = record.row();
            let row_id = columnar_row.get_row_id();
            if row_id == 0 {
                let record_batch = columnar_row.get_record_batch();
                result.push(Arc::new(record_batch.clone()));
            }
        }
        result
    }

    /// Combine multiple Arrow batches into a single Table
    pub fn combine_batches_to_table(
        py: Python,
        batches: Vec<Arc<arrow::record_batch::RecordBatch>>,
    ) -> PyResult<Py<PyAny>> {
        let py_batches: Result<Vec<Py<PyAny>>, _> = batches
            .iter()
            .map(|batch| {
                // Just dereference the Arc - no need to recreate the batch
                batch
                    .as_ref()
                    .to_pyarrow(py)
                    .map(|x| x.into())
                    .map_err(|e| FlussError::new_err(format!("Failed to convert to PyObject: {e}")))
            })
            .collect();

        let py_batches = py_batches?;

        let pyarrow = py.import("pyarrow")?;

        // Use pyarrow.Table.from_batches to combine batches
        let table = pyarrow
            .getattr("Table")?
            .call_method1("from_batches", (py_batches,))?;

        Ok(table.into())
    }
}
