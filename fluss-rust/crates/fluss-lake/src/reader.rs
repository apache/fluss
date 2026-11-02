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

//! Scan-configured readers, bounded task concurrency and stream cancellation.

use crate::executor::execute_split;
use crate::{FlussLakeError, FlussLakeReadSplit, FlussLakeScan, RecordBatchStream, Result};
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;

/// Reusable reader with immutable scan configuration.
///
/// Local callers and distributed workers use the same scan-created reader.
/// The caller must supply the original tasks and the planner's scan settings,
/// including a compatible lake backend. No plan object is retained or rebuilt.
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

    /// Read a frozen task, including a serialized round-trip from a coordinator.
    /// Opening is lazy; the first stream error is terminal. Drop cancels the read.
    pub async fn read_split(&self, split: &FlussLakeReadSplit) -> Result<RecordBatchStream> {
        self.scan.validate_configuration()?;
        execute_split(split, &self.scan).map(stop_after_first_error)
    }

    /// Read a task collection as an unordered stream with at most eight
    /// active logical tasks. Duplicate tasks are rejected. Engines may instead
    /// schedule `read_split` themselves. An error drops all sibling streams;
    /// discard this attempt's output before replanning with fresh boundaries.
    pub async fn read_splits(&self, splits: &[FlussLakeReadSplit]) -> Result<RecordBatchStream> {
        self.read_splits_with_concurrency(splits, 8).await
    }

    /// As `read_splits`, with an explicit positive active-task limit.
    pub async fn read_splits_with_concurrency(
        &self,
        splits: &[FlussLakeReadSplit],
        concurrency: usize,
    ) -> Result<RecordBatchStream> {
        if concurrency == 0 {
            return Err(FlussLakeError::PlanningFailed(
                "read concurrency must be positive".to_string(),
            ));
        }
        self.scan.validate_configuration()?;
        let mut seen = HashSet::with_capacity(splits.len());
        for split in splits {
            if !seen.insert(&split.split_id) {
                return Err(FlussLakeError::PlanningFailed(
                    "duplicate read task".to_string(),
                ));
            }
        }
        let streams = splits
            .iter()
            .map(|split| execute_split(split, &self.scan))
            .collect::<Result<Vec<_>>>()?;
        Ok(merge_split_streams(streams, concurrency))
    }
}

fn merge_split_streams(streams: Vec<RecordBatchStream>, concurrency: usize) -> RecordBatchStream {
    stop_after_first_error(Box::pin(
        futures::stream::iter(streams).flatten_unordered(concurrency),
    ))
}

/// Makes the first stream error terminal and immediately drops the source.
///
/// For `read_splits`, dropping the merged stream also cancels every sibling
/// split as soon as one split invalidates the attempt.
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
    use arrow::array::{ArrayRef, Int32Array};
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
    fn merged_reads_bound_active_streams_and_release_on_drop() {
        futures::executor::block_on(async {
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let streams = (0..12)
                .map(|_| counted_stream(active.clone(), peak.clone()))
                .collect();
            let batches = merge_split_streams(streams, 2)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches.len(), 24);
            assert!(peak.load(Ordering::SeqCst) <= 2);
            assert_eq!(active.load(Ordering::SeqCst), 0);

            let streams = (0..12)
                .map(|_| counted_stream(active.clone(), peak.clone()))
                .collect();
            let mut merged = merge_split_streams(streams, 2);
            assert!(merged.next().await.unwrap().is_ok());
            drop(merged);
            assert_eq!(active.load(Ordering::SeqCst), 0);
        });
    }

    #[test]
    fn merged_error_drops_siblings_and_cannot_be_followed_by_rows() {
        futures::executor::block_on(async {
            let active = Arc::new(AtomicUsize::new(0));
            let peak = Arc::new(AtomicUsize::new(0));
            let streams = vec![
                counted_stream(active.clone(), peak),
                Box::pin(futures::stream::once(async {
                    Err(FlussLakeError::DataUnavailable("expired input".into()))
                })) as RecordBatchStream,
            ];
            let mut stream = merge_split_streams(streams, 2);
            loop {
                if stream
                    .next()
                    .await
                    .expect("must report the failure")
                    .is_err()
                {
                    break;
                }
            }
            assert_eq!(active.load(Ordering::SeqCst), 0);
            assert!(stream.next().await.is_none());
        });
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn reader_is_send_and_sync() {
        assert_send_sync::<crate::FlussLakeReader>();
    }

    #[test]
    fn reader_stream_stops_after_the_first_error() {
        let batch = arrow::record_batch::RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        let source: RecordBatchStream = Box::pin(futures::stream::iter(vec![
            Ok(batch.clone()),
            Err(FlussLakeError::DataUnavailable(
                "planned range expired".to_string(),
            )),
            Ok(batch),
        ]));

        let items = futures::executor::block_on(stop_after_first_error(source).collect::<Vec<_>>());

        assert_eq!(items.len(), 2);
        assert!(items[0].is_ok());
        assert!(matches!(items[1], Err(FlussLakeError::DataUnavailable(_))));
    }
}
