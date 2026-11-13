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

//! UnionRead planning result.

use crate::split::FlussLakeReadSplit;
use crate::{FlussLakeError, FlussLakeReadContext, Result};
use arrow::datatypes::SchemaRef;
use std::collections::HashSet;
use std::sync::Arc;

/// Aggregated statistics about a planned UnionRead job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FlussLakePlanStatistics {
    /// Number of logical tasks, not the engine's execution parallelism.
    pub split_count: usize,
    /// Total estimated input rows; `None` means an estimate is missing or overflows.
    /// This is not an exact PK result cardinality.
    pub estimated_rows: Option<usize>,
    /// Total estimated input bytes; `None` means an estimate is missing or overflows.
    pub estimated_size: Option<usize>,
}

impl FlussLakePlanStatistics {
    pub(crate) fn from_splits(splits: &[FlussLakeReadSplit]) -> Self {
        Self {
            split_count: splits.len(),
            estimated_rows: sum_estimates(splits.iter().map(|split| split.estimated_rows())),
            estimated_size: sum_estimates(splits.iter().map(|split| split.estimated_size())),
        }
    }
}

/// Immutable frozen tasks, output schema and source context.
/// Execution uses a reader created from the matching scan configuration.
#[derive(Clone)]
pub struct FlussLakeReadPlan {
    inner: Arc<ReadPlan>,
}

struct ReadPlan {
    context: FlussLakeReadContext,
    schema: SchemaRef,
    splits: Vec<FlussLakeReadSplit>,
    statistics: FlussLakePlanStatistics,
}

impl FlussLakeReadPlan {
    pub(crate) fn new(
        context: FlussLakeReadContext,
        schema: SchemaRef,
        splits: Vec<FlussLakeReadSplit>,
    ) -> Result<Self> {
        let mut split_ids = HashSet::with_capacity(splits.len());
        for split in &splits {
            if !split_ids.insert(split.split_id()) {
                return Err(FlussLakeError::PlanningFailed(
                    "duplicate logical split identity".to_string(),
                ));
            }
        }
        let statistics = FlussLakePlanStatistics::from_splits(&splits);
        Ok(Self {
            inner: Arc::new(ReadPlan {
                context,
                schema,
                splits,
                statistics,
            }),
        })
    }

    /// Source state used by this plan, reusable with another scan configuration.
    pub fn read_context(&self) -> &FlussLakeReadContext {
        &self.inner.context
    }

    /// Schema of every output batch, including zero-column count scans.
    pub fn schema(&self) -> SchemaRef {
        self.inner.schema.clone()
    }

    /// Number of logical splits, not engine parallelism.
    pub fn split_count(&self) -> usize {
        self.inner.splits.len()
    }

    /// Immutable logical tasks. Engines decide how to schedule them.
    pub fn splits(&self) -> &[FlussLakeReadSplit] {
        &self.inner.splits
    }

    /// Best-effort input estimates, not an exact PK result cardinality.
    pub fn statistics(&self) -> FlussLakePlanStatistics {
        self.inner.statistics
    }
}

impl std::fmt::Debug for FlussLakeReadPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlussLakeReadPlan")
            .field("table_path", self.inner.context.table_path())
            .field("statistics", &self.inner.statistics)
            .finish_non_exhaustive()
    }
}

fn sum_estimates(mut estimates: impl Iterator<Item = Option<usize>>) -> Option<usize> {
    estimates.try_fold(0usize, |total, estimate| total.checked_add(estimate?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_estimates_are_not_counted_as_zero() {
        assert_eq!(sum_estimates([Some(2), Some(3)].into_iter()), Some(5));
        assert_eq!(sum_estimates([Some(2), None].into_iter()), None);
    }
}
