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

//! Scan-configured readers, sequential task consumption and stream cancellation.

use crate::executor::execute_split;
use crate::{FlussLakeError, FlussLakeReadSplit, FlussLakeScan, RecordBatchStream, Result};
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Reusable reader with immutable scan configuration.
///
/// Execute local tasks with the scan settings that produced them, including a
/// compatible lake backend. Tasks can outlive their plan and be scheduled by
/// an in-process engine; no plan identity or task transport is required.
#[derive(Clone, Debug)]
pub struct FlussLakeReader {
    scan: Arc<FlussLakeScan>,
}

impl FlussLakeReader {
    pub(crate) fn from_scan(scan: FlussLakeScan) -> Self {
        Self {
            scan: Arc::new(scan),
        }
    }

    /// Read a frozen local task, which may be cloned from a plan.
    /// Construction validates scan configuration and the task table without I/O. Polling
    /// the stream opens a fresh metadata session and starts reading. Initialization
    /// and read failures are terminal stream errors. Drop cancels the read.
    pub fn read_split(&self, split: &FlussLakeReadSplit) -> Result<RecordBatchStream> {
        self.scan.validate_configuration()?;
        execute_split(split, &self.scan, Arc::new(OnceCell::new())).map(stop_after_first_error)
    }

    /// Read tasks sequentially in the supplied order. Duplicate tasks are rejected.
    /// Callers control concurrency by scheduling `read_split` streams themselves.
    /// Nonempty tasks share one lazy table metadata load within this call; later
    /// calls load it again. Routing refresh remains managed by the core client.
    /// The first error terminates the stream without starting remaining tasks;
    /// discard this attempt's output before replanning with fresh boundaries.
    pub fn read_splits(&self, splits: &[FlussLakeReadSplit]) -> Result<RecordBatchStream> {
        self.scan.validate_configuration()?;
        let mut seen = HashSet::with_capacity(splits.len());
        for split in splits {
            if !seen.insert(split.split_id()) {
                return Err(FlussLakeError::PlanningFailed(
                    "duplicate read task".to_string(),
                ));
            }
        }
        let metadata = Arc::new(OnceCell::new());
        let streams = splits
            .iter()
            .map(|split| execute_split(split, &self.scan, metadata.clone()))
            .collect::<Result<Vec<_>>>()?;
        Ok(concatenate_split_streams(streams))
    }
}

fn concatenate_split_streams(streams: Vec<RecordBatchStream>) -> RecordBatchStream {
    stop_after_first_error(Box::pin(futures::stream::iter(streams).flatten()))
}

/// Makes the first stream error terminal and immediately drops the source.
///
/// For `read_splits`, this also drops all remaining unpolled task streams.
fn stop_after_first_error(stream: RecordBatchStream) -> RecordBatchStream {
    Box::pin(futures::stream::unfold(Some(stream), |stream| async move {
        let mut stream = stream?;
        match stream.next().await {
            Some(Ok(batch)) => Some((Ok(batch), Some(stream))),
            Some(Err(error)) => Some((Err(error), None)),
            None => None,
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ActiveRead(Arc<AtomicUsize>);
    impl Drop for ActiveRead {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn counted_stream(active: Arc<AtomicUsize>, peak: Arc<AtomicUsize>) -> RecordBatchStream {
        Box::pin(
            futures::stream::once(async move {
                let count = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(count, Ordering::SeqCst);
                let guard = ActiveRead(active);
                let batch = RecordBatch::try_from_iter(vec![(
                    "id",
                    Arc::new(Int32Array::from(vec![1])) as arrow::array::ArrayRef,
                )])
                .unwrap();
                Ok::<RecordBatchStream, FlussLakeError>(Box::pin(
                    futures::stream::iter([Ok(batch.clone()), Ok(batch)]).map(move |item| {
                        let _keep_alive = &guard;
                        item
                    }),
                ))
            })
            .try_flatten(),
        )
    }

    #[test]
    fn sequential_reads_release_active_streams_on_completion_and_drop() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FlussLakeReader>();
        futures::executor::block_on(async {
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let streams = (0..2)
                .map(|_| counted_stream(active.clone(), peak.clone()))
                .collect();
            let batches = concatenate_split_streams(streams)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches.len(), 4);
            assert_eq!(peak.load(Ordering::SeqCst), 1);
            assert_eq!(active.load(Ordering::SeqCst), 0);

            let streams = (0..2)
                .map(|_| counted_stream(active.clone(), peak.clone()))
                .collect();
            let mut merged = concatenate_split_streams(streams);
            assert!(merged.next().await.unwrap().is_ok());
            drop(merged);
            assert_eq!(active.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn error_does_not_start_remaining_tasks() {
        futures::executor::block_on(async {
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let batch = RecordBatch::try_from_iter(vec![(
                "id",
                Arc::new(Int32Array::from(vec![1])) as arrow::array::ArrayRef,
            )])
            .unwrap();
            let streams = vec![
                Box::pin(futures::stream::iter([
                    Ok(batch.clone()),
                    Err(FlussLakeError::DataUnavailable("expired input".into())),
                    Ok(batch),
                ])) as RecordBatchStream,
                counted_stream(active.clone(), peak.clone()),
            ];
            let mut stream = concatenate_split_streams(streams);
            assert!(stream.next().await.unwrap().is_ok());
            assert!(matches!(
                stream.next().await,
                Some(Err(FlussLakeError::DataUnavailable(_)))
            ));
            assert!(stream.next().await.is_none());
            assert_eq!(peak.load(Ordering::SeqCst), 0);
            assert_eq!(active.load(Ordering::SeqCst), 0);
        });
    }
}
