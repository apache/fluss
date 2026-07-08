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
        }
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
    inner: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>,
}

impl std::fmt::Debug for WriteResultFuture {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteResultFuture").finish_non_exhaustive()
    }
}

impl WriteResultFuture {
    /// Create a new WriteResultFuture from a ResultHandle.
    pub fn new(result_handle: ResultHandle) -> Self {
        Self {
            inner: Box::pin(async move {
                let result = result_handle.wait().await?;
                result_handle.result(result)
            }),
        }
    }
}

impl Future for WriteResultFuture {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}
