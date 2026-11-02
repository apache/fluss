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

//! Engine-neutral bounded lake and log read kernel for Apache Fluss.
//!
//! Union read is a bounded batch query. Results are delivered lazily as a
//! finite stream of Arrow record batches.
//!
//! Three integration levels share the same source contract:
//! - [`FlussLakeScan::plan`] and [`FlussLakeScan::new_reader`] provide the
//!   complete default reader, including lake access and reconciliation.
//!   Local callers and workers use matching scan settings to
//!   execute transported splits without retaining or rebuilding the plan.
//! - [`FlussLakeScan::with_lake_source`] replaces lake planning/reading while
//!   retaining the default Fluss log reader and current-view reconciliation.
//! - [`FlussLakeTable::capture_read_context`] returns a portable [`FlussLakeReadContext`]
//!   without a lake backend. Engines can use their own lake planners, readers,
//!   reconciliation operators, and schedulers against these frozen inputs.
//!
//! A context records the table or partition scope captured by planning.
//! Explicit preparation allows scans to share boundaries independently of projection
//! and predicates.
//! It is not a retention lease or a global transactional snapshot.

#![doc = include_str!("../README.md")]

mod boundary;
mod bucket_pruning;
mod error;
mod executor;
mod partition;
mod plan;
mod planner;
mod pruning;
mod read_context;
mod reader;
mod source;
mod split;
mod split_descriptor;
mod table;

pub use error::{FlussLakeError, Result};
pub use partition::{FlussLakeLogPartition, FlussLakePartitionIdentity};
pub use plan::{FlussLakePlanStatistics, FlussLakeReadPlan};
pub use read_context::{FlussLakeBaseline, FlussLakeLogRange, FlussLakeReadContext};
pub use reader::FlussLakeReader;
pub use source::{LakePlannerContext, LakeReadSemantics, LakeReaderContext, LakeSource, LakeSplit};
pub(crate) use split::CURRENT_FLUSS_LAKE_SPLIT_VERSION;
pub use split::FlussLakeReadSplit;
pub use table::{FlussLakeScan, FlussLakeTable};

use arrow::record_batch::RecordBatch;
use futures::Stream;
use std::pin::Pin;

/// A finite stream of Arrow record batches produced from bounded UnionRead splits.
///
/// Despite the `Stream` name, this represents a bounded batch result. The
/// stream terminates after the immutable split boundary has been consumed.
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;
