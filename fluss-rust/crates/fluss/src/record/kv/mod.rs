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

//! Key-Value record and batch implementations.

mod kv_record;
mod kv_record_batch;
mod kv_record_batch_builder;
mod kv_record_read_context;
mod read_context;
mod value_record_batch;

#[cfg(test)]
mod test_util;

pub use kv_record::{KvRecord, LENGTH_LENGTH as KV_RECORD_LENGTH_LENGTH};
pub use kv_record_batch::*;
pub use kv_record_batch_builder::*;
pub use kv_record_read_context::{KvRecordReadContext, SchemaGetter};
pub use read_context::ReadContext;
pub(crate) use value_record_batch::ValueRecordBatch;

/// Current KV magic value
pub const CURRENT_KV_MAGIC_VALUE: u8 = 0;

/// No writer ID constant
pub const NO_WRITER_ID: i64 = -1;

/// No batch sequence constant
pub const NO_BATCH_SEQUENCE: i32 = -1;
