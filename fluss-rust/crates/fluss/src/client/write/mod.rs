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

mod accumulator;
mod batch;
mod dynamic_batch_size;
mod idempotence;

use crate::client::broadcast::{self as client_broadcast, BatchWriteResult, BroadcastOnceReceiver};
use crate::error::Error;
use crate::metadata::{PhysicalTablePath, TableInfo};

use crate::row::InternalRow;
pub use accumulator::*;
use arrow::array::RecordBatch;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

pub(crate) mod broadcast;
mod bucket_assigner;

mod sender;
mod write_format;
mod writer_client;

pub(crate) use idempotence::IdempotenceManager;
pub use write_format::WriteFormat;
pub(crate) use writer_client::WriterClient;

#[allow(dead_code)]
pub struct WriteRecord<'a> {
    record: Record<'a>,
    physical_table_path: Arc<PhysicalTablePath>,
    bucket_key: Option<Bytes>,
    schema_id: i32,
    write_format: WriteFormat,
    table_info: Arc<TableInfo>,
    /// Optional deadline bounding the buffer-memory wait during append. `None`
    /// falls back to the writer's configured buffer wait timeout.
    submit_deadline: Option<Instant>,
}

impl<'a> WriteRecord<'a> {
    pub fn record(&self) -> &Record<'a> {
        &self.record
    }

    pub fn physical_table_path(&self) -> &Arc<PhysicalTablePath> {
        &self.physical_table_path
    }

    /// Minimum batch capacity needed to fit this record, including batch header
    /// overhead. Used to size memory reservations and KV write limits so that
    /// oversized records don't panic on append.
    pub fn estimated_record_size(&self) -> usize {
        match &self.record {
            Record::Kv(kv) => {
                let record_size = crate::record::kv::KvRecord::size_of(
                    &kv.key,
                    kv.row_bytes.as_ref().map(|rb| rb.as_slice()),
                );
                crate::record::kv::RECORD_BATCH_HEADER_SIZE + record_size
            }
            Record::Log(_) => 0, // Arrow batches use record count, not byte size
        }
    }
}

pub enum Record<'a> {
    Log(LogWriteRecord<'a>),
    Kv(KvWriteRecord<'a>),
}

pub enum LogWriteRecord<'a> {
    InternalRow(&'a dyn InternalRow),
    RecordBatch(Arc<RecordBatch>),
}

#[derive(Clone)]
pub enum RowBytes<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

impl<'a> RowBytes<'a> {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            RowBytes::Borrowed(slice) => slice,
            RowBytes::Owned(bytes) => bytes.as_ref(),
        }
    }
}

pub struct KvWriteRecord<'a> {
    key: Bytes,
    target_columns: Option<Arc<Vec<usize>>>,
    row_bytes: Option<RowBytes<'a>>,
}

impl<'a> KvWriteRecord<'a> {
    fn new(
        key: Bytes,
        target_columns: Option<Arc<Vec<usize>>>,
        row_bytes: Option<RowBytes<'a>>,
    ) -> Self {
        KvWriteRecord {
            key,
            target_columns,
            row_bytes,
        }
    }

    pub fn row_bytes(&self) -> Option<&[u8]> {
        self.row_bytes.as_ref().map(|rb| rb.as_slice())
    }
}

impl<'a> WriteRecord<'a> {
    pub fn for_append(
        table_info: Arc<TableInfo>,
        physical_table_path: Arc<PhysicalTablePath>,
        schema_id: i32,
        row: &'a dyn InternalRow,
    ) -> Self {
        Self {
            table_info,
            record: Record::Log(LogWriteRecord::InternalRow(row)),
            physical_table_path,
            bucket_key: None,
            schema_id,
            write_format: WriteFormat::ArrowLog,
            submit_deadline: None,
        }
    }

    pub fn for_append_record_batch(
        table_info: Arc<TableInfo>,
        physical_table_path: Arc<PhysicalTablePath>,
        schema_id: i32,
        row: RecordBatch,
    ) -> Self {
        Self {
            table_info,
            record: Record::Log(LogWriteRecord::RecordBatch(Arc::new(row))),
            physical_table_path,
            bucket_key: None,
            schema_id,
            write_format: WriteFormat::ArrowLog,
            submit_deadline: None,
        }
    }

    /// Sets the bucket key used to hash-assign this record to a bucket.
    pub fn with_bucket_key(mut self, bucket_key: Option<Bytes>) -> Self {
        self.bucket_key = bucket_key;
        self
    }

    /// Sets a submit deadline that bounds how long the buffer-memory wait may block
    /// before this record's append fails fast. `None` uses the writer's configured
    /// buffer wait timeout.
    pub fn with_submit_deadline(mut self, deadline: Option<Instant>) -> Self {
        self.submit_deadline = deadline;
        self
    }

    #[allow(clippy::too_many_arguments)]
    pub fn for_upsert(
        table_info: Arc<TableInfo>,
        physical_table_path: Arc<PhysicalTablePath>,
        schema_id: i32,
        key: Bytes,
        bucket_key: Option<Bytes>,
        write_format: WriteFormat,
        target_columns: Option<Arc<Vec<usize>>>,
        row_bytes: Option<RowBytes<'a>>,
    ) -> Self {
        Self {
            table_info,
            record: Record::Kv(KvWriteRecord::new(key, target_columns, row_bytes)),
            physical_table_path,
            bucket_key,
            schema_id,
            write_format,
            submit_deadline: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ResultHandle {
    receiver: BroadcastOnceReceiver<BatchWriteResult>,
}

impl ResultHandle {
    pub fn new(receiver: BroadcastOnceReceiver<BatchWriteResult>) -> Self {
        ResultHandle { receiver }
    }

    /// Force-complete with an error if not already completed.
    pub(crate) fn fail(&self, error: client_broadcast::Error) {
        self.receiver.fail(error);
    }

    pub async fn wait(&self) -> Result<BatchWriteResult, Error> {
        self.receiver
            .receive()
            .await
            .map_err(|e| Error::UnexpectedError {
                message: format!("Fail to wait write result {e:?}"),
                source: None,
            })
    }

    pub fn result(&self, batch_result: BatchWriteResult) -> Result<(), Error> {
        Self::resolve(batch_result)
    }

    fn resolve(batch_result: BatchWriteResult) -> Result<(), Error> {
        batch_result.map_err(|e| match e {
            client_broadcast::Error::WriteFailed { code, message } => Error::FlussAPIError {
                api_error: crate::rpc::ApiError { code, message },
            },
            client_broadcast::Error::Client { message } => Error::UnexpectedError {
                message,
                source: None,
            },
            client_broadcast::Error::Dropped => Error::UnexpectedError {
                message: "Fail to get write result because broadcast was dropped.".to_string(),
                source: None,
            },
        })
    }
}

/// A future that represents a pending write operation.
///
/// This type implements [`Future`], allowing users to either:
/// 1. Await immediately to block on acknowledgment: `writer.upsert(&row)?.await?`
/// 2. Fire-and-forget with later flush: `writer.upsert(&row)?; writer.flush().await?`
///
/// This pattern is similar to rdkafka's `DeliveryFuture` and allows for efficient batching
/// when users don't need immediate per-record acknowledgment.
pub struct WriteResultFuture {
    state: WriteResultState,
}

enum WriteResultState {
    Single(ResultHandle),
    Joined(Vec<ResultHandle>),
    Waiting(Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>),
}

/// An opaque group of write completions for language-binding executors.
#[doc(hidden)]
pub type WriteCallbackBatch = broadcast::CompletionBatch<BatchWriteResult>;

impl std::fmt::Debug for WriteResultFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteResultFuture").finish_non_exhaustive()
    }
}

impl WriteResultFuture {
    /// Create a new WriteResultFuture from a ResultHandle.
    pub fn new(result_handle: ResultHandle) -> Self {
        Self {
            state: WriteResultState::Single(result_handle),
        }
    }

    pub fn join(handles: Vec<ResultHandle>) -> Self {
        Self {
            state: WriteResultState::Joined(handles),
        }
    }

    /// Register completions directly on their owning batches, without spawning
    /// per-record waiting tasks. `dispatch` must reliably enqueue every job,
    /// must not panic, and must not run callbacks on the calling/I/O thread.
    ///
    /// A previously polled future is returned with its callback untouched for
    /// an executor to await normally. Dispatch may happen before this returns.
    #[doc(hidden)]
    pub fn try_on_complete<C>(
        self,
        callback: C,
        dispatch: fn(WriteCallbackBatch),
    ) -> std::result::Result<(), (Self, C)>
    where
        C: FnOnce(Result<(), Error>) + Send + 'static,
    {
        match self.state {
            WriteResultState::Single(handle) => {
                handle.receiver.subscribe(
                    Box::new(move |result| callback(resolve_callback_result(result))),
                    dispatch,
                );
            }
            WriteResultState::Joined(handles) if handles.is_empty() => {
                // Use the same executor even for an empty Arrow RecordBatch.
                let completed = broadcast::BroadcastOnce::default();
                completed.receiver().subscribe(
                    Box::new(move |result| callback(resolve_callback_result(result))),
                    dispatch,
                );
                completed.broadcast(Ok(()));
            }
            WriteResultState::Joined(handles) => {
                // Preserve join's input-order error semantics, even when batches
                // finish out of order. Do not call user code under this mutex.
                let count = handles.len();
                let joined = Arc::new(parking_lot::Mutex::new(JoinedCallback {
                    results: (0..count).map(|_| None).collect(),
                    next: 0,
                    callback: Some(callback),
                }));
                for (index, handle) in handles.into_iter().enumerate() {
                    let joined = Arc::clone(&joined);
                    handle.receiver.subscribe(
                        Box::new(move |result| {
                            let ready = {
                                let mut joined = joined.lock();
                                joined.complete(index, resolve_callback_result(result))
                            };
                            if let Some((callback, result)) = ready {
                                callback(result);
                            }
                        }),
                        dispatch,
                    );
                }
            }
            WriteResultState::Waiting(_) => return Err((self, callback)),
        }
        Ok(())
    }
}

struct JoinedCallback<C> {
    results: Vec<Option<Result<(), Error>>>,
    next: usize,
    callback: Option<C>,
}

impl<C> JoinedCallback<C> {
    fn complete(
        &mut self,
        index: usize,
        result: Result<(), Error>,
    ) -> Option<(C, Result<(), Error>)> {
        self.callback.as_ref()?;
        self.results[index] = Some(result);
        while self.next < self.results.len() {
            let result = self.results[self.next].take()?;
            self.next += 1;
            if result.is_err() || self.next == self.results.len() {
                return self.callback.take().map(|callback| (callback, result));
            }
        }
        None
    }
}

fn resolve_callback_result(
    result: &client_broadcast::Result<BatchWriteResult>,
) -> Result<(), Error> {
    let result = result.clone().map_err(|e| Error::UnexpectedError {
        message: format!("Fail to wait write result {e:?}"),
        source: None,
    })?;
    ResultHandle::resolve(result)
}

impl Future for WriteResultFuture {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            match &mut self.state {
                WriteResultState::Waiting(future) => return future.as_mut().poll(cx),
                _ => {
                    let state =
                        std::mem::replace(&mut self.state, WriteResultState::Joined(Vec::new()));
                    self.state = WriteResultState::Waiting(Box::pin(async move {
                        match state {
                            WriteResultState::Single(handle) => {
                                let result = handle.wait().await?;
                                handle.result(result)
                            }
                            WriteResultState::Joined(handles) => {
                                for handle in handles {
                                    let result = handle.wait().await?;
                                    handle.result(result)?;
                                }
                                Ok(())
                            }
                            WriteResultState::Waiting(_) => unreachable!(),
                        }
                    }));
                }
            }
        }
    }
}

#[cfg(test)]
mod callback_tests {
    use super::*;
    use broadcast::BroadcastOnce;
    use std::sync::mpsc;
    use std::time::Duration;

    fn future(batch: &BroadcastOnce<BatchWriteResult>) -> WriteResultFuture {
        WriteResultFuture::new(ResultHandle::new(batch.receiver()))
    }

    fn register(future: WriteResultFuture) -> mpsc::Receiver<Result<(), Error>> {
        let (tx, rx) = mpsc::channel();
        assert!(
            future
                .try_on_complete(move |r| tx.send(r).unwrap(), WriteCallbackBatch::run)
                .is_ok()
        );
        rx
    }

    #[tokio::test]
    async fn test_callback_and_wait_share_result_without_consuming_each_other() {
        let batch = BroadcastOnce::default();
        let wait = future(&batch);
        let rx = register(future(&batch));
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        batch.broadcast(Ok(()));
        assert!(wait.await.is_ok());
        assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn test_callbacks_preserve_batch_and_abort_errors() {
        let batch = BroadcastOnce::default();
        let rx = register(future(&batch));
        batch.broadcast(Err(client_broadcast::Error::WriteFailed {
            code: 57,
            message: "Deletion is disabled".into(),
        }));
        assert!(
            matches!(rx.recv().unwrap(), Err(Error::FlussAPIError { api_error })
            if api_error.code == 57 && api_error.message == "Deletion is disabled")
        );

        let batch = BroadcastOnce::default();
        let rx = register(future(&batch));
        batch.receiver().fail(client_broadcast::Error::Client {
            message: "abort".into(),
        });
        assert!(
            rx.recv()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("abort")
        );

        let batch = BroadcastOnce::default();
        let rx = register(future(&batch));
        drop(batch);
        assert!(
            rx.recv()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("Dropped")
        );
    }

    #[test]
    fn test_join_preserves_input_order_and_short_circuits_error() {
        let first = BroadcastOnce::default();
        let second = BroadcastOnce::default();
        let third = BroadcastOnce::default();
        let rx = register(WriteResultFuture::join(vec![
            ResultHandle::new(first.receiver()),
            ResultHandle::new(second.receiver()),
            ResultHandle::new(third.receiver()),
        ]));
        second.broadcast(Err(client_broadcast::Error::WriteFailed {
            code: 57,
            message: "second error".into(),
        }));
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        first.broadcast(Ok(()));
        assert!(
            rx.recv()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("second error")
        );
        // No need to wait for the third batch after the first ordered error.
        third.broadcast(Ok(()));
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn test_join_all_success_empty_and_duplicate_batch_handles() {
        assert!(
            register(WriteResultFuture::join(Vec::new()))
                .recv()
                .unwrap()
                .is_ok()
        );
        let batch = BroadcastOnce::default();
        let rx = register(WriteResultFuture::join(vec![
            ResultHandle::new(batch.receiver()),
            ResultHandle::new(batch.receiver()),
        ]));
        batch.broadcast(Ok(()));
        assert!(rx.recv().unwrap().is_ok());
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn test_join_first_error_wins_despite_reverse_completion_order() {
        let first = BroadcastOnce::default();
        let second = BroadcastOnce::default();
        let rx = register(WriteResultFuture::join(vec![
            ResultHandle::new(first.receiver()),
            ResultHandle::new(second.receiver()),
        ]));
        second.broadcast(Err(client_broadcast::Error::Client {
            message: "second".into(),
        }));
        first.broadcast(Err(client_broadcast::Error::Client {
            message: "first".into(),
        }));
        assert!(
            rx.recv()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("first")
        );
    }

    #[test]
    fn test_join_concurrent_completions_invoke_callback_once() {
        for _ in 0..32 {
            let batches: Vec<_> = (0..8).map(|_| BroadcastOnce::default()).collect();
            let rx = register(WriteResultFuture::join(
                batches
                    .iter()
                    .map(|b| ResultHandle::new(b.receiver()))
                    .collect(),
            ));
            std::thread::scope(|scope| {
                for batch in &batches {
                    scope.spawn(move || batch.broadcast(Ok(())));
                }
            });
            assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
            assert!(matches!(
                rx.try_recv(),
                Err(mpsc::TryRecvError::Disconnected)
            ));
        }
    }

    #[tokio::test]
    async fn test_polled_future_returns_untouched_callback_for_fallback() {
        let batch = BroadcastOnce::default();
        let mut wait = future(&batch);
        assert!(
            Pin::new(&mut wait)
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        let (tx, rx) = mpsc::channel();
        let registered =
            wait.try_on_complete(move |r| tx.send(r).unwrap(), WriteCallbackBatch::run);
        let Err((wait, callback)) = registered else {
            panic!("polled future must use fallback")
        };
        batch.broadcast(Ok(()));
        callback(wait.await);
        assert!(rx.recv().unwrap().is_ok());
    }
}
