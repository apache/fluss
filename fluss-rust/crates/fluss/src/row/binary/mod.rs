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

mod binary_writer;
mod iceberg_binary_row_writer;

pub use binary_writer::*;
pub use iceberg_binary_row_writer::IcebergBinaryRowWriter;

/// The binary row format types, it indicates the generated row type by the [`BinaryWriter`]
#[allow(dead_code)]
pub enum BinaryRowFormat {
    Compacted,
    Aligned,
    Indexed,
}
