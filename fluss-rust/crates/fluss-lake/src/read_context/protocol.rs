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

//! Shared wire representation; runtime credentials and physical tasks stay outside it.

use super::*;
use fluss::metadata::SchemaInfo;
use fluss::protocol::{PbUnionReadContext, PbUnionReadLake, PbUnionReadLogRange};

impl FlussLakeReadContext {
    /// Produces the versioned boundary message shared with Java and native engines.
    /// No schema payload or credentials are included. Workers resolve schema_id
    /// against the same table identity, or reuse a previously resolved schema.
    pub fn to_protobuf(&self) -> Result<PbUnionReadContext> {
        Ok(PbUnionReadContext {
            version: Some(1),
            database_name: Some(self.table_path.database().into()),
            table_name: Some(self.table_path.table().into()),
            table_id: Some(self.table_id),
            schema_id: Some(self.schema_id),
            table_bucket_count: Some(self.num_buckets),
            bucket_keys: self.bucket_keys.clone(),
            partition_keys: self.partition_keys.clone(),
            merge_engine: self.merge_engine.clone(),
            lake: self.lake.as_ref().map(|lake| PbUnionReadLake {
                format: Some(lake.format.clone()),
                database_name: Some(lake.table_path.database().into()),
                table_name: Some(lake.table_path.table().into()),
                snapshot_id: lake.snapshot_id,
            }),
            partition_scope: self
                .partition_scope
                .as_ref()
                .map(|scope| {
                    scope
                        .to_protobuf(self.schema.row_type())
                        .map_err(invalid_context)
                })
                .transpose()?,
            log_ranges: self
                .log_ranges
                .iter()
                .map(|range| {
                    let FlussLakeLogPartition::Ordinary(identity) = &range.partition;
                    PbUnionReadLogRange {
                        partition_id: range.table_bucket.partition_id(),
                        partition_values: match identity {
                            FlussLakePartitionIdentity::Unpartitioned => vec![],
                            FlussLakePartitionIdentity::KeyValues(values) => {
                                values.iter().map(|(_, value)| value.clone()).collect()
                            }
                        },
                        bucket_id: Some(range.table_bucket.bucket_id()),
                        bucket_count: Some(range.bucket_count),
                        baseline: Some(match range.baseline {
                            FlussLakeBaseline::None => 0,
                            FlussLakeBaseline::Lake => 1,
                        }),
                        start_offset: Some(range.start_offset),
                        stop_offset: Some(range.stop_offset),
                        earliest_offset: Some(range.earliest_offset),
                    }
                })
                .collect(),
            coverage: Some(if self.lake_only {
                2
            } else {
                i32::from(self.bucket_scoped)
            }),
        })
    }

    /// Imports a wire boundary using current table metadata and the exact planned schema.
    /// This performs no I/O, allowing engines to cache resolved metadata per query.
    /// The caller must obtain the schema from this table identity, not another table
    /// with the same name. Use FlussLakeTable::import_read_context for RPC resolution.
    pub fn from_protobuf(
        pb: &PbUnionReadContext,
        live: &TableInfo,
        schema: &SchemaInfo,
    ) -> Result<Self> {
        validate_wire_identity(pb, live)?;
        let context = Self::from_wire_schema(pb, schema)?;
        context.validate_table(live)?;
        Ok(context)
    }

    fn from_wire_schema(pb: &PbUnionReadContext, schema: &SchemaInfo) -> Result<Self> {
        if pb.version != Some(1) {
            return Err(invalid_context(
                "unsupported or missing read boundary version",
            ));
        }
        if required(pb.schema_id, "schema_id")? != schema.schema_id() {
            return Err(invalid_context(
                "resolved schema does not match the planned schema ID",
            ));
        }
        let (bucket_scoped, lake_only) = match pb.coverage {
            Some(0) => (false, false),
            Some(1) => (true, false),
            Some(2) => (false, true),
            _ => return Err(invalid_context("unknown or missing read coverage")),
        };
        let lake = pb
            .lake
            .as_ref()
            .map(|lake| {
                Ok(LakeDescriptor {
                    format: required(lake.format.clone(), "lake.format")?,
                    table_path: TablePath::new(
                        required(lake.database_name.as_deref(), "lake.database_name")?,
                        required(lake.table_name.as_deref(), "lake.table_name")?,
                    ),
                    snapshot_id: lake.snapshot_id,
                })
            })
            .transpose()?;
        let ranges = pb
            .log_ranges
            .iter()
            .map(|range| {
                if range.partition_values.len() != pb.partition_keys.len() {
                    return Err(invalid_context(
                        "partition values do not match the captured keys",
                    ));
                }
                let identity = if pb.partition_keys.is_empty() {
                    FlussLakePartitionIdentity::Unpartitioned
                } else {
                    FlussLakePartitionIdentity::KeyValues(
                        pb.partition_keys
                            .iter()
                            .cloned()
                            .zip(range.partition_values.iter().cloned())
                            .collect(),
                    )
                };
                Ok(FlussLakeLogRange {
                    table_bucket: TableBucket::new_with_partition(
                        required(pb.table_id, "table_id")?,
                        range.partition_id,
                        required(range.bucket_id, "bucket_id")?,
                    ),
                    partition: FlussLakeLogPartition::Ordinary(identity),
                    bucket_count: required(range.bucket_count, "bucket_count")?,
                    baseline: match range.baseline {
                        Some(0) => FlussLakeBaseline::None,
                        Some(1) => FlussLakeBaseline::Lake,
                        _ => return Err(invalid_context("unknown or missing baseline kind")),
                    },
                    start_offset: required(range.start_offset, "start_offset")?,
                    stop_offset: required(range.stop_offset, "stop_offset")?,
                    earliest_offset: required(range.earliest_offset, "earliest_offset")?,
                })
            })
            .collect::<Result<_>>()?;
        let context = Self {
            table_path: TablePath::new(
                required(pb.database_name.as_deref(), "database_name")?,
                required(pb.table_name.as_deref(), "table_name")?,
            ),
            table_id: required(pb.table_id, "table_id")?,
            schema_id: schema.schema_id(),
            schema: schema.schema().clone(),
            partition_keys: pb.partition_keys.clone(),
            partition_scope: pb
                .partition_scope
                .as_ref()
                .map(|scope| {
                    Predicate::from_protobuf(scope, schema.schema().row_type())
                        .map_err(invalid_context)
                })
                .transpose()?,
            bucket_scoped,
            lake_only,
            bucket_keys: pb.bucket_keys.clone(),
            num_buckets: required(pb.table_bucket_count, "table_bucket_count")?,
            merge_engine: pb.merge_engine.clone(),
            lake,
            log_ranges: ranges,
        };
        context.validate()?;
        Ok(context)
    }
}

pub(crate) fn validate_wire_identity(pb: &PbUnionReadContext, live: &TableInfo) -> Result<()> {
    crate::table::validate_supported_table(live)?;
    if pb.version != Some(1) {
        return Err(invalid_context(
            "unsupported or missing read boundary version",
        ));
    }
    if pb.table_id != Some(live.table_id)
        || pb.database_name.as_deref() != Some(live.table_path.database())
        || pb.table_name.as_deref() != Some(live.table_path.table())
    {
        return Err(FlussLakeError::SchemaIncompatible(
            "read boundary belongs to a different source table".into(),
        ));
    }
    Ok(())
}

pub(crate) fn required<T>(value: Option<T>, field: &str) -> Result<T> {
    value.ok_or_else(|| invalid_context(format!("missing boundary field {field}")))
}
