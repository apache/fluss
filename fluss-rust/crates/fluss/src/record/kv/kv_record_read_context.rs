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

//! Default implementation of ReadContext with decoder caching.

use super::ReadContext;
use crate::error::Result;
use crate::metadata::{KvFormat, Schema};
use crate::row::{RowDecoder, RowDecoderFactory};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Trait for fetching schemas by ID.
///
/// This trait abstracts schema retrieval, allowing different implementations
/// (e.g., from metadata store, cache, or test mocks).
pub trait SchemaGetter: Send + Sync {
    /// Get the schema for the given schema ID.
    ///
    /// # Arguments
    /// * `schema_id` - The schema ID to fetch
    ///
    /// # Returns
    /// An Arc-wrapped Schema for the specified ID, or an error if the schema
    /// cannot be fetched (missing ID, network error, etc.)
    fn get_schema(&self, schema_id: i16) -> Result<Arc<Schema>>;
}

/// Default implementation of ReadContext with decoder caching.
///
/// This implementation caches RowDecoders by schema ID for performance,
/// avoiding repeated schema lookups and decoder creation.
///
/// Reference: org.apache.fluss.record.KvRecordReadContext
pub struct KvRecordReadContext {
    kv_format: KvFormat,
    schema_getter: Arc<dyn SchemaGetter>,
    row_decoder_cache: Mutex<HashMap<i16, Arc<dyn RowDecoder>>>,
}

impl KvRecordReadContext {
    /// Create a new KvRecordReadContext.
    ///
    /// # Arguments
    /// * `kv_format` - The KV format (COMPACTED or INDEXED)
    /// * `schema_getter` - The schema getter for fetching schemas by ID
    ///
    /// # Returns
    /// A new KvRecordReadContext instance
    pub fn new(kv_format: KvFormat, schema_getter: Arc<dyn SchemaGetter>) -> Self {
        Self {
            kv_format,
            schema_getter,
            row_decoder_cache: Mutex::new(HashMap::new()),
        }
    }
}

impl ReadContext for KvRecordReadContext {
    fn get_row_decoder(&self, schema_id: i16) -> Result<Arc<dyn RowDecoder>> {
        // First check: fast path
        {
            let cache = self
                .row_decoder_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(decoder) = cache.get(&schema_id) {
                return Ok(Arc::clone(decoder));
            }
        } // Release lock before expensive operations

        // Build decoder outside the lock to avoid blocking other threads
        let schema = self.schema_getter.get_schema(schema_id)?;
        let row_type = schema.row_type().clone();

        // Create decoder outside lock
        let decoder = RowDecoderFactory::create(self.kv_format, row_type)?;

        // Second check: insert only if another thread didn't beat us to it
        {
            let mut cache = self
                .row_decoder_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Check again - another thread might have inserted while we were building
            if let Some(existing) = cache.get(&schema_id) {
                return Ok(Arc::clone(existing));
            }
            cache.insert(schema_id, Arc::clone(&decoder));
        }

        Ok(decoder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataTypes, Schema};

    struct MockSchemaGetter {
        schema: Arc<Schema>,
    }

    impl MockSchemaGetter {
        fn new(data_types: Vec<crate::metadata::DataType>) -> Self {
            let mut builder = Schema::builder();
            for (i, dt) in data_types.iter().enumerate() {
                builder = builder.column(format!("field{i}"), dt.clone());
            }
            let schema = builder.build().expect("Failed to build schema");

            Self {
                schema: Arc::new(schema),
            }
        }
    }

    impl SchemaGetter for MockSchemaGetter {
        fn get_schema(&self, _schema_id: i16) -> Result<Arc<Schema>> {
            Ok(Arc::clone(&self.schema))
        }
    }

    #[test]
    fn test_kv_record_read_context() {
        // Test decoder caching for same schema ID
        let schema_getter = Arc::new(MockSchemaGetter::new(vec![
            DataTypes::int(),
            DataTypes::string(),
        ]));
        let read_context = KvRecordReadContext::new(KvFormat::COMPACTED, schema_getter);

        // Get decoder twice - should return the same instance (cached)
        let decoder1 = read_context.get_row_decoder(42).unwrap();
        let decoder2 = read_context.get_row_decoder(42).unwrap();

        // Verify same instance (Arc pointer equality)
        assert!(Arc::ptr_eq(&decoder1, &decoder2));

        // Test different schema IDs get different decoders
        let schema_getter = Arc::new(MockSchemaGetter::new(vec![DataTypes::int()]));
        let read_context = KvRecordReadContext::new(KvFormat::COMPACTED, schema_getter);

        let decoder1 = read_context.get_row_decoder(10).unwrap();
        let decoder2 = read_context.get_row_decoder(20).unwrap();

        // Should be different instances
        assert!(!Arc::ptr_eq(&decoder1, &decoder2));
    }
}
