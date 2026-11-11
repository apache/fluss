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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Reusable source boundaries for bounded lake and log reads.

#![doc = include_str!("../README.md")]

mod boundary;
mod bucket_pruning;
mod error;
#[cfg(feature = "paimon")]
mod paimon;
mod partition;
mod plan;
mod planner;
mod pruning;
mod read_context;
mod source;
mod split;
mod split_descriptor;
mod table;

pub use error::{FlussLakeError, Result};
#[cfg(feature = "paimon")]
pub use paimon::{PaimonBaselineReader, PaimonLakeSource, PaimonReadContext};
pub use partition::{FlussLakeLogPartition, FlussLakePartitionIdentity};
pub use plan::{FlussLakePlanStatistics, FlussLakeReadPlan};
pub use read_context::{FlussLakeBaseline, FlussLakeLogRange, FlussLakeReadContext};
pub use source::{LakePlannerContext, LakeReadSemantics, LakeReaderContext, LakeSource, LakeSplit};
pub use split::FlussLakeReadSplit;
pub use split_descriptor::FlussLakeSplitDescriptor;
pub use table::{FlussLakeScan, FlussLakeTable};

/// A finite stream of Arrow record batches returned by a lake source.
pub type RecordBatchStream =
    std::pin::Pin<Box<dyn futures::Stream<Item = Result<arrow::record_batch::RecordBatch>> + Send>>;
