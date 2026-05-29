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

//! Test utilities for KV record reading.

use super::ReadContext;
use crate::error::Result;
use crate::metadata::{DataType, KvFormat, RowType};
use crate::row::{RowDecoder, RowDecoderFactory};
use std::sync::Arc;

/// Simple test-only ReadContext that creates decoders directly from data types.
///
/// This bypasses the production Schema/SchemaGetter machinery for simpler tests.
pub(crate) struct TestReadContext {
    kv_format: KvFormat,
    data_types: Vec<DataType>,
}

impl TestReadContext {
    /// Create a test context for COMPACTED format (most common case).
    pub(crate) fn compacted(data_types: Vec<DataType>) -> Self {
        Self {
            kv_format: KvFormat::COMPACTED,
            data_types,
        }
    }
}

impl ReadContext for TestReadContext {
    fn get_row_decoder(&self, _schema_id: i16) -> Result<Arc<dyn RowDecoder>> {
        // Directly create decoder from data types - no Schema needed!
        let row_type = RowType::with_data_types(self.data_types.clone());
        RowDecoderFactory::create(self.kv_format, row_type)
    }
}
