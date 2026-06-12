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

use crate::error::{Error, Result};
use crate::metadata::{Schema, UNKNOWN_COLUMN_ID};
use std::collections::{HashMap, HashSet};

/// Sentinel for an expected column that does not exist in the origin
/// schema. Used by [`index_mapping`] and [`crate::row::ProjectedRow`].
pub(crate) const UNEXIST_MAPPING: i32 = -1;

/// For each column in `expected_schema`, return the index of the column
/// with the same id in `origin_schema`, or [`UNEXIST_MAPPING`] if absent.
/// Matching by id keeps mappings stable across `ALTER TABLE … RENAME`.
pub(crate) fn index_mapping(origin_schema: &Schema, expected_schema: &Schema) -> Result<Vec<i32>> {
    let origin_columns = origin_schema.columns();
    let mut origin_id_to_index: HashMap<i32, usize> = HashMap::with_capacity(origin_columns.len());
    for (i, col) in origin_columns.iter().enumerate() {
        if col.id() == UNKNOWN_COLUMN_ID {
            return Err(Error::RowConvertError {
                message: format!(
                    "origin schema column '{}' has no assigned id; cannot build index mapping",
                    col.name()
                ),
            });
        }
        if origin_id_to_index.insert(col.id(), i).is_some() {
            return Err(Error::RowConvertError {
                message: format!("duplicate column id {} in origin schema", col.id()),
            });
        }
    }

    let expected_columns = expected_schema.columns();
    let mut mapping = Vec::with_capacity(expected_columns.len());
    let mut expected_seen: HashSet<i32> = HashSet::with_capacity(expected_columns.len());

    for expected in expected_columns {
        if expected.id() == UNKNOWN_COLUMN_ID {
            return Err(Error::RowConvertError {
                message: format!(
                    "expected schema column '{}' has no assigned id; cannot build index mapping",
                    expected.name()
                ),
            });
        }
        if !expected_seen.insert(expected.id()) {
            return Err(Error::RowConvertError {
                message: format!("duplicate column id {} in expected schema", expected.id()),
            });
        }
        match origin_id_to_index.get(&expected.id()) {
            None => mapping.push(UNEXIST_MAPPING),
            Some(&idx) => {
                let origin = &origin_columns[idx];
                if !origin.data_type().eq_ignore_nullable(expected.data_type()) {
                    return Err(Error::RowConvertError {
                        message: format!(
                            "Expected datatype of column(id={},name={}) is [{}], while the actual datatype is [{}]",
                            expected.id(),
                            expected.name(),
                            expected.data_type(),
                            origin.data_type()
                        ),
                    });
                }
                mapping.push(idx as i32);
            }
        }
    }

    Ok(mapping)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{Column, DataType, DataTypes};

    fn schema_auto(columns: &[(&str, DataType)]) -> Schema {
        let mut b = Schema::builder();
        for (name, dt) in columns {
            b = b.column(*name, dt.clone());
        }
        b.build().expect("schema build")
    }

    fn schema_with_ids(columns: &[(i32, &str, DataType)]) -> Schema {
        let cols: Vec<Column> = columns
            .iter()
            .map(|(id, name, dt)| Column::new(*name, dt.clone()).with_id(*id))
            .collect();
        Schema::builder()
            .with_columns(cols)
            .build()
            .expect("schema build")
    }

    #[test]
    fn identity_mapping_when_schemas_equal() {
        let s = schema_auto(&[
            ("a", DataTypes::bigint()),
            ("b", DataTypes::string()),
            ("c", DataTypes::int()),
        ]);
        assert_eq!(index_mapping(&s, &s).unwrap(), vec![0, 1, 2]);
    }

    #[test]
    fn projection_subset_in_order() {
        let origin = schema_auto(&[
            ("a", DataTypes::bigint()),
            ("b", DataTypes::string()),
            ("c", DataTypes::int()),
        ]);
        let expected =
            schema_with_ids(&[(0, "a", DataTypes::bigint()), (2, "c", DataTypes::int())]);
        assert_eq!(index_mapping(&origin, &expected).unwrap(), vec![0, 2]);
    }

    #[test]
    fn reorder_mapping() {
        let origin = schema_auto(&[
            ("a", DataTypes::bigint()),
            ("b", DataTypes::string()),
            ("c", DataTypes::int()),
        ]);
        let expected = schema_with_ids(&[
            (2, "c", DataTypes::int()),
            (0, "a", DataTypes::bigint()),
            (1, "b", DataTypes::string()),
        ]);
        assert_eq!(index_mapping(&origin, &expected).unwrap(), vec![2, 0, 1]);
    }

    #[test]
    fn missing_column_returns_sentinel() {
        let origin = schema_auto(&[("a", DataTypes::bigint())]);
        let expected = schema_with_ids(&[
            (0, "a", DataTypes::bigint()),
            (1, "new_col", DataTypes::string()),
        ]);
        assert_eq!(
            index_mapping(&origin, &expected).unwrap(),
            vec![0, UNEXIST_MAPPING]
        );
    }

    #[test]
    fn rename_preserves_mapping_when_id_matches() {
        let origin = schema_with_ids(&[(0, "old_name", DataTypes::int())]);
        let expected = schema_with_ids(&[(0, "new_name", DataTypes::int())]);
        assert_eq!(index_mapping(&origin, &expected).unwrap(), vec![0]);
    }

    #[test]
    fn drop_then_add_with_same_name_does_not_alias() {
        let origin = schema_with_ids(&[(0, "a", DataTypes::int())]);
        let expected = schema_with_ids(&[(5, "a", DataTypes::int())]);
        assert_eq!(
            index_mapping(&origin, &expected).unwrap(),
            vec![UNEXIST_MAPPING]
        );
    }

    #[test]
    fn datatype_mismatch_returns_error() {
        let origin = schema_auto(&[("a", DataTypes::bigint())]);
        let expected = schema_with_ids(&[(0, "a", DataTypes::int())]);
        let err = index_mapping(&origin, &expected).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("id=0"), "{msg}");
        assert!(msg.contains("name=a"), "{msg}");
        assert!(msg.contains("INT"), "{msg}");
        assert!(msg.contains("BIGINT"), "{msg}");
    }

    #[test]
    fn nullability_difference_does_not_error() {
        // Primary-key normalization makes the origin non-nullable while
        // the expected is nullable.
        let origin = Schema::builder()
            .column("a", DataTypes::int())
            .primary_key(["a"])
            .build()
            .unwrap();
        let expected = schema_with_ids(&[(0, "a", DataTypes::int())]);
        assert_eq!(index_mapping(&origin, &expected).unwrap(), vec![0]);
    }
}
