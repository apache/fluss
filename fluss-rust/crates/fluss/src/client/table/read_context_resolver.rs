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

//! Per-schema `ReadContext` cache for schema evolution support.
//!
//! In DYNAMIC mode (no projection), records are returned with their write-time
//! schema: old-schema batches return fewer columns, new-schema batches return
//! more columns.
//!
//! When projection is active, the schema is pinned at scanner creation time
//! and all batches use the initial ReadContext regardless of schema_id.
//!
//! When fixed-schema mode is active, batches are still decoded with their
//! write-time schema and then aligned to the scanner creation schema.

use crate::client::ClientSchemaGetter;
use crate::error::{Error, Result};
use crate::metadata::{RowType, Schema, index_mapping};
use crate::record::{ReadContext, to_arrow_schema};
use arrow_schema::SchemaRef;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// Resolves `ReadContext` per schema version to support schema evolution.
pub(crate) struct ReadContextResolver {
    /// Schema ID at scanner creation time.
    initial_schema_id: i16,
    /// ReadContexts keyed by schema_id. Contains both local and remote contexts.
    contexts: RwLock<HashMap<i16, ResolvedContexts>>,
    /// When Some, projection is active and schema is pinned to the initial one.
    projected_fields: Option<Vec<usize>>,
    /// Used to lazily fetch schema versions when decoding a batch whose schema
    /// was not prewarmed by the fetch response path.
    schema_getter: Option<Arc<ClientSchemaGetter>>,
    /// Fixed-schema target captured when the scanner is created. When set,
    /// older batches decode with their write-time schema and then align to
    /// this target by stable Fluss column ID.
    fixed_target: Option<FixedSchemaTarget>,
}

/// A pair of ReadContexts for local and remote reads.
struct ResolvedContexts {
    local: Arc<ReadContext>,
    remote: Arc<ReadContext>,
}

struct FixedSchemaTarget {
    fluss_schema: Schema,
    arrow_schema: SchemaRef,
    row_type: Arc<RowType>,
}

impl ReadContextResolver {
    /// Create a new resolver with the initial schema's ReadContexts.
    pub fn new(
        initial_schema_id: i16,
        local_context: Arc<ReadContext>,
        remote_context: Arc<ReadContext>,
        projected_fields: Option<Vec<usize>>,
    ) -> Self {
        let mut map = HashMap::new();
        map.insert(
            initial_schema_id,
            ResolvedContexts {
                local: local_context,
                remote: remote_context,
            },
        );
        Self {
            initial_schema_id,
            contexts: RwLock::new(map),
            projected_fields,
            schema_getter: None,
            fixed_target: None,
        }
    }

    pub fn with_schema_getter(mut self, schema_getter: Arc<ClientSchemaGetter>) -> Self {
        self.schema_getter = Some(schema_getter);
        self
    }

    pub fn with_fixed_schema(mut self, target_fluss_schema: &Schema) -> Self {
        if self.projected_fields.is_some() {
            return self;
        }
        self.fixed_target = {
            let guard = self.contexts.read();
            guard
                .get(&self.initial_schema_id)
                .map(|ctx| FixedSchemaTarget {
                    fluss_schema: target_fluss_schema.clone(),
                    arrow_schema: ctx.local.target_schema(),
                    row_type: ctx.local.row_type_arc(),
                })
        };
        self
    }

    /// Resolve the ReadContext for the given schema_id.
    /// Returns the initial context if projection is active (schema pinned).
    /// Returns None if the schema_id is not yet cached.
    pub fn resolve(&self, schema_id: i16, is_remote: bool) -> Option<Arc<ReadContext>> {
        // If projection is active, always return the initial context
        let effective_id = if self.projected_fields.is_some() {
            self.initial_schema_id
        } else {
            schema_id
        };

        let guard = self.contexts.read();
        guard.get(&effective_id).map(|ctx| {
            if is_remote {
                Arc::clone(&ctx.remote)
            } else {
                Arc::clone(&ctx.local)
            }
        })
    }

    /// Fetch and register one schema version without blocking a Tokio worker.
    ///
    /// Callers should invoke this only after encountering a concrete batch
    /// whose schema is absent. This keeps remote log files streaming: no scan
    /// of the remaining segment is needed to discover all schema IDs up front.
    pub async fn fetch_and_register(&self, schema_id: i16) -> Result<()> {
        if self.projected_fields.is_some() || self.contexts.read().contains_key(&schema_id) {
            return Ok(());
        }

        let schema_getter = self
            .schema_getter
            .as_ref()
            .ok_or_else(|| Error::UnexpectedError {
                message: format!("No schema getter configured for schema_id {schema_id}"),
                source: None,
            })?
            .clone();
        let schema = schema_getter.get_schema(schema_id as i32).await?;
        self.register_schema(schema_id, &schema)?;
        Ok(())
    }

    /// Register a new schema by its ID. Builds ReadContexts from the Schema.
    /// No-op if already cached or if projection is active.
    pub fn register_schema(&self, schema_id: i16, schema: &Schema) -> Result<()> {
        if self.projected_fields.is_some() {
            // Projection pins the schema, no need to register new ones
            return Ok(());
        }
        if self.contexts.read().contains_key(&schema_id) {
            return Ok(());
        }

        let source_row_type = schema.row_type();
        let source_arrow_schema = to_arrow_schema(source_row_type)?;
        let source_row_type_arc = Arc::new(source_row_type.clone());
        let fixed_target = self
            .fixed_target
            .as_ref()
            .map(|target| {
                index_mapping(schema, &target.fluss_schema).map(|mapping| {
                    (
                        target.arrow_schema.clone(),
                        target.row_type.clone(),
                        Arc::<[i32]>::from(mapping.into_boxed_slice()),
                    )
                })
            })
            .transpose()?;
        let output_row_type = fixed_target
            .as_ref()
            .map(|(_, row_type, _)| row_type.clone())
            .unwrap_or(source_row_type_arc);

        let mut local_context =
            ReadContext::new(source_arrow_schema.clone(), output_row_type.clone(), false)
                .with_fluss_row_type(output_row_type.clone());
        let mut remote_context =
            ReadContext::new(source_arrow_schema, output_row_type.clone(), true)
                .with_fluss_row_type(output_row_type);

        if let Some((target_schema, _, schema_alignment)) = fixed_target {
            local_context = local_context
                .with_target_schema_alignment(target_schema.clone(), Arc::clone(&schema_alignment));
            remote_context =
                remote_context.with_target_schema_alignment(target_schema, schema_alignment);
        }

        let local_context = Arc::new(local_context);
        let remote_context = Arc::new(remote_context);

        self.contexts.write().insert(
            schema_id,
            ResolvedContexts {
                local: local_context,
                remote: remote_context,
            },
        );
        Ok(())
    }

    /// Returns the initial schema ID.
    pub fn initial_schema_id(&self) -> i16 {
        self.initial_schema_id
    }

    /// Returns the projection fields if active, used for fetch request pushdown.
    #[allow(dead_code)]
    pub fn projected_fields(&self) -> Option<&[usize]> {
        self.projected_fields.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::ReadContextResolver;
    use crate::error::Result;
    use crate::metadata::{DataTypes, Schema};
    use crate::record::{ReadContext, to_arrow_schema};
    use std::sync::Arc;

    #[test]
    fn fixed_schema_does_not_capture_target_with_projection() -> Result<()> {
        let schema = Schema::builder()
            .column("id", DataTypes::int())
            .column("name", DataTypes::string())
            .build()?;
        let arrow_schema = to_arrow_schema(schema.row_type())?;
        let projected_fields = vec![0];
        let projected_row_type = Arc::new(schema.row_type().project(&projected_fields)?);
        let local_context = Arc::new(ReadContext::with_projection_pushdown(
            arrow_schema.clone(),
            Arc::clone(&projected_row_type),
            projected_fields.clone(),
            false,
        )?);
        let remote_context = Arc::new(ReadContext::with_projection_pushdown(
            arrow_schema,
            projected_row_type,
            projected_fields.clone(),
            true,
        )?);

        let resolver =
            ReadContextResolver::new(1, local_context, remote_context, Some(projected_fields))
                .with_fixed_schema(&schema);

        assert!(resolver.fixed_target.is_none());
        Ok(())
    }
}
