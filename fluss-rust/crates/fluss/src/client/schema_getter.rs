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

//! Per-table schema cache that lazily fetches missing schema versions
//! from the coordinator. Used by the lookup path to decode rows that
//! predate the table's current schema.

use crate::client::admin::FlussAdmin;
use crate::error::{Error, Result};
use crate::metadata::{Schema, SchemaInfo, TablePath};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct ClientSchemaGetter {
    table_path: TablePath,
    admin: Arc<FlussAdmin>,
    /// Pre-seeded with the table's current schema so the dominant case
    /// (every row written under the latest schema) needs zero RPCs.
    cache: RwLock<HashMap<i32, Arc<Schema>>>,
}

impl ClientSchemaGetter {
    pub fn new(table_path: TablePath, admin: Arc<FlussAdmin>, latest: SchemaInfo) -> Self {
        let mut map = HashMap::new();
        let (schema, schema_id) = latest.into_parts();
        map.insert(schema_id, Arc::new(schema));
        Self {
            table_path,
            admin,
            cache: RwLock::new(map),
        }
    }

    /// Concurrent fetches for the same id are not deduplicated; we
    /// accept one redundant RPC in exchange for staying off
    /// `tokio::sync` machinery. Schemas are immutable per id, so
    /// last-write-wins on the cache insert is correct.
    pub async fn get_schema(&self, schema_id: i32) -> Result<Arc<Schema>> {
        if let Some(schema) = self.cache.read().get(&schema_id).cloned() {
            return Ok(schema);
        }

        let info = self
            .admin
            .get_table_schema(&self.table_path, Some(schema_id))
            .await?;
        let (schema, fetched_id) = info.into_parts();
        if fetched_id != schema_id {
            return Err(Error::UnexpectedError {
                message: format!(
                    "Requested schema id {schema_id}, but server returned schema id {fetched_id}"
                ),
                source: None,
            });
        }
        let schema = Arc::new(schema);

        self.cache.write().insert(schema_id, Arc::clone(&schema));
        Ok(schema)
    }
}
