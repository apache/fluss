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

//! Read context for KV record batches.
//!
//! Provides schema and decoder information needed for typed record reading.

use crate::error::Result;
use crate::row::RowDecoder;
use std::sync::Arc;

/// Context for reading KV records with type information.
///
/// The ReadContext provides access to RowDecoders based on schema IDs,
/// enabling typed deserialization of KV record values.
///
/// Reference: org.apache.fluss.record.KvRecordBatch.ReadContext
pub trait ReadContext: Send + Sync {
    /// Get the row decoder for the given schema ID.
    ///
    /// The decoder is typically cached, so repeated calls with the same
    /// schema ID should return the same decoder instance.
    ///
    /// # Arguments
    /// * `schema_id` - The schema ID for which to get the decoder
    ///
    /// # Returns
    /// An Arc-wrapped RowDecoder for the specified schema, or an error if
    /// the schema is invalid or cannot be retrieved
    fn get_row_decoder(&self, schema_id: i16) -> Result<Arc<dyn RowDecoder>>;
}
