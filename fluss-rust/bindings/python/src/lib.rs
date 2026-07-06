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

use std::sync::LazyLock;

pub use ::fluss as fcore;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

mod admin;
mod config;
mod connection;
mod error;
mod lookup;
mod metadata;
mod table;
mod upsert;
mod utils;
mod write_handle;

pub use admin::*;
pub use config::*;
pub use connection::*;
pub use error::*;
pub use lookup::*;
pub use metadata::*;
pub use table::*;
pub use upsert::*;
pub use utils::*;
pub use write_handle::*;

static TOKIO_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

/// Offset specification for list_offsets(), matching Java's OffsetSpec.
///
/// Use factory methods to create instances:
///   OffsetSpec.earliest()
///   OffsetSpec.latest()
///   OffsetSpec.timestamp(ts)
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct OffsetSpec {
    pub(crate) inner: fcore::rpc::message::OffsetSpec,
}

#[pymethods]
impl OffsetSpec {
    /// Create an OffsetSpec for the earliest available offset.
    #[staticmethod]
    fn earliest() -> Self {
        Self {
            inner: fcore::rpc::message::OffsetSpec::Earliest,
        }
    }

    /// Create an OffsetSpec for the latest available offset.
    #[staticmethod]
    fn latest() -> Self {
        Self {
            inner: fcore::rpc::message::OffsetSpec::Latest,
        }
    }

    /// Create an OffsetSpec for the offset at or after the given timestamp.
    #[staticmethod]
    fn timestamp(ts: i64) -> Self {
        Self {
            inner: fcore::rpc::message::OffsetSpec::Timestamp(ts),
        }
    }

    fn __repr__(&self) -> String {
        match &self.inner {
            fcore::rpc::message::OffsetSpec::Earliest => "OffsetSpec.earliest()".to_string(),
            fcore::rpc::message::OffsetSpec::Latest => "OffsetSpec.latest()".to_string(),
            fcore::rpc::message::OffsetSpec::Timestamp(ts) => {
                format!("OffsetSpec.timestamp({ts})")
            }
        }
    }
}

#[pymodule]
fn _fluss(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register all classes
    m.add_class::<Config>()?;
    m.add_class::<FlussConnection>()?;
    m.add_class::<TablePath>()?;
    m.add_class::<TableInfo>()?;
    m.add_class::<TableDescriptor>()?;
    m.add_class::<FlussAdmin>()?;
    m.add_class::<FlussTable>()?;
    m.add_class::<TableScan>()?;
    m.add_class::<TableAppend>()?;
    m.add_class::<TableUpsert>()?;
    m.add_class::<TableLookup>()?;
    m.add_class::<TablePrefixLookup>()?;
    m.add_class::<AppendWriter>()?;
    m.add_class::<UpsertWriter>()?;
    m.add_class::<Lookuper>()?;
    m.add_class::<PrefixLookuper>()?;
    m.add_class::<Schema>()?;
    m.add_class::<LogScanner>()?;
    m.add_class::<BatchScanner>()?;
    m.add_class::<LakeSnapshot>()?;
    m.add_class::<TableBucket>()?;
    m.add_class::<ChangeType>()?;
    m.add_class::<ScanRecord>()?;
    m.add_class::<ScanRecords>()?;
    m.add_class::<RecordBatch>()?;
    m.add_class::<PartitionInfo>()?;
    m.add_class::<ServerNode>()?;
    m.add_class::<OffsetSpec>()?;
    m.add_class::<WriteResultHandle>()?;
    m.add_class::<DatabaseDescriptor>()?;
    m.add_class::<DatabaseInfo>()?;

    // Register constants
    m.add("EARLIEST_OFFSET", fcore::client::EARLIEST_OFFSET)?;

    // Register exception types and error codes
    m.add_class::<FlussError>()?;
    m.add_class::<ErrorCode>()?;

    Ok(())
}
