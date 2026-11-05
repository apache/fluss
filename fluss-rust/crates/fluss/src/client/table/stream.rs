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

//! Stream adapters and bridges for bounded readers ([`RecordBatchLogReader`] and [`LimitBatchScanner`]).
//!
//! Provides:
//! - [`ScanBatchStream`]: An asynchronous [`Stream`] yielding [`ScanBatch`] items with schema metadata.
//! - [`RecordBatchStream`]: An asynchronous [`Stream`] yielding bare Arrow [`RecordBatch`] items,
//!   suitable for DataFusion (`RecordBatchStreamAdapter`) and PyArrow interop.
//! - [`SyncRecordBatchStreamReader`]: A synchronous [`arrow::record_batch::RecordBatchReader`]
//!   adapter driving an async stream using a Tokio runtime handle.

use crate::error::Result;
use crate::record::ScanBatch;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

/// An asynchronous [`Stream`] yielding [`ScanBatch`] items from a bounded scan or reader.
///
/// Created via [`RecordBatchLogReader::into_stream`] or [`LimitBatchScanner::into_stream`].
///
/// Provides access to the Arrow schema via [`schema`](Self::schema) and can be converted
/// to a bare-`RecordBatch` stream via [`into_record_batch_stream`](Self::into_record_batch_stream)
/// or to a synchronous [`arrow::record_batch::RecordBatchReader`] via
/// [`to_record_batch_reader`](Self::to_record_batch_reader).
pub struct ScanBatchStream {
    inner: BoxStream<'static, Result<ScanBatch>>,
    schema: SchemaRef,
}

impl ScanBatchStream {
    /// Creates a new `ScanBatchStream` from a boxed stream and its Arrow schema.
    pub fn new(inner: BoxStream<'static, Result<ScanBatch>>, schema: SchemaRef) -> Self {
        Self { inner, schema }
    }

    /// Returns the Arrow schema of batches produced by this stream.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Converts this [`ScanBatchStream`] into a [`RecordBatchStream`], stripping
    /// Fluss metadata and yielding bare Arrow [`RecordBatch`]es suitable for
    /// DataFusion or PyArrow.
    pub fn into_record_batch_stream(self) -> RecordBatchStream {
        RecordBatchStream::new(self)
    }

    /// Converts this stream into a synchronous [`arrow::record_batch::RecordBatchReader`].
    ///
    /// The returned adapter blocks on the provided Tokio runtime handle.
    pub fn to_record_batch_reader(
        self,
        handle: tokio::runtime::Handle,
    ) -> SyncRecordBatchStreamReader {
        self.into_record_batch_stream().to_record_batch_reader(handle)
    }
}

impl Stream for ScanBatchStream {
    type Item = Result<ScanBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl fmt::Debug for ScanBatchStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScanBatchStream")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// An asynchronous [`Stream`] yielding bare Arrow [`RecordBatch`] items.
///
/// Created via [`ScanBatchStream::into_record_batch_stream`],
/// [`RecordBatchLogReader::into_record_batch_stream`], or
/// [`LimitBatchScanner::into_record_batch_stream`].
///
/// Strips Fluss-specific metadata (bucket ID, log offsets) from each [`ScanBatch`],
/// yielding standard Arrow `Result<RecordBatch, arrow::error::ArrowError>` items.
/// This type exposes [`schema`](Self::schema), allowing it to plug directly into
/// DataFusion (e.g. `RecordBatchStreamAdapter`) and PyArrow.
pub struct RecordBatchStream {
    inner: BoxStream<'static, std::result::Result<RecordBatch, arrow::error::ArrowError>>,
    schema: SchemaRef,
}

impl RecordBatchStream {
    /// Creates a new `RecordBatchStream` by wrapping a [`ScanBatchStream`].
    pub fn new(stream: ScanBatchStream) -> Self {
        let schema = stream.schema();
        let inner = Box::pin(stream.map(|res| {
            res.map(|scan_batch| scan_batch.into_batch())
                .map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e)))
        }));
        Self { inner, schema }
    }

    /// Creates a new `RecordBatchStream` from an existing boxed stream and its Arrow schema.
    pub fn from_stream(
        inner: BoxStream<'static, std::result::Result<RecordBatch, arrow::error::ArrowError>>,
        schema: SchemaRef,
    ) -> Self {
        Self { inner, schema }
    }

    /// Returns the Arrow schema of batches produced by this stream.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Converts this stream into a synchronous [`arrow::record_batch::RecordBatchReader`].
    ///
    /// The returned adapter blocks on the provided Tokio runtime handle.
    pub fn to_record_batch_reader(
        self,
        handle: tokio::runtime::Handle,
    ) -> SyncRecordBatchStreamReader {
        SyncRecordBatchStreamReader {
            stream: self,
            handle,
        }
    }
}

impl Stream for RecordBatchStream {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl From<ScanBatchStream> for RecordBatchStream {
    fn from(stream: ScanBatchStream) -> Self {
        stream.into_record_batch_stream()
    }
}

impl fmt::Debug for RecordBatchStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordBatchStream")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

/// Synchronous adapter that wraps a [`RecordBatchStream`] to implement
/// [`arrow::record_batch::RecordBatchReader`].
///
/// Created via [`RecordBatchStream::to_record_batch_reader`] or
/// [`ScanBatchStream::to_record_batch_reader`].
/// Blocks the current thread on each `next()` call using the provided
/// Tokio runtime handle.
pub struct SyncRecordBatchStreamReader {
    stream: RecordBatchStream,
    handle: tokio::runtime::Handle,
}

impl SyncRecordBatchStreamReader {
    /// Creates a new synchronous reader wrapping the given stream.
    pub fn new(stream: RecordBatchStream, handle: tokio::runtime::Handle) -> Self {
        Self { stream, handle }
    }

    /// Returns the Arrow schema of batches produced by this reader.
    pub fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl Iterator for SyncRecordBatchStreamReader {
    type Item = std::result::Result<RecordBatch, arrow::error::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.handle.block_on(self.stream.next())
    }
}

impl arrow::record_batch::RecordBatchReader for SyncRecordBatchStreamReader {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl fmt::Debug for SyncRecordBatchStreamReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SyncRecordBatchStreamReader")
            .field("schema", &self.stream.schema())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use crate::metadata::TableBucket;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow_schema::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", ArrowDataType::Int32, false),
            Field::new("name", ArrowDataType::Utf8, false),
        ]))
    }

    fn test_scan_batch(schema: SchemaRef, ids: Vec<i32>, names: Vec<&str>) -> ScanBatch {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ];
        let record_batch = RecordBatch::try_new(schema, columns).unwrap();
        ScanBatch::new(TableBucket::new(1, 0), record_batch, 0)
    }

    #[tokio::test]
    async fn test_scan_batch_stream_consumption() {
        let schema = test_schema();
        let batch1 = test_scan_batch(schema.clone(), vec![1, 2], vec!["a", "b"]);
        let batch2 = test_scan_batch(schema.clone(), vec![3], vec!["c"]);

        let items = vec![Ok(batch1), Ok(batch2)];
        let boxed = Box::pin(futures::stream::iter(items));
        let mut stream = ScanBatchStream::new(boxed, schema.clone());

        assert_eq!(stream.schema(), schema);

        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.num_records(), 2);

        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.num_records(), 1);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_into_record_batch_stream() {
        let schema = test_schema();
        let batch1 = test_scan_batch(schema.clone(), vec![1, 2], vec!["a", "b"]);

        let items = vec![Ok(batch1)];
        let boxed = Box::pin(futures::stream::iter(items));
        let scan_stream = ScanBatchStream::new(boxed, schema.clone());
        let mut rb_stream = scan_stream.into_record_batch_stream();

        assert_eq!(rb_stream.schema(), schema);

        let first = rb_stream.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        assert_eq!(first.num_columns(), 2);

        assert!(rb_stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_record_batch_stream_error_propagation() {
        let schema = test_schema();
        let items: Vec<Result<ScanBatch>> = vec![
            Err(Error::UnexpectedError {
                message: "scan failure".to_string(),
                source: None,
            }),
        ];
        let boxed = Box::pin(futures::stream::iter(items));
        let scan_stream = ScanBatchStream::new(boxed, schema.clone());
        let mut rb_stream = scan_stream.into_record_batch_stream();

        let err = rb_stream.next().await.unwrap().unwrap_err();
        match err {
            arrow::error::ArrowError::ExternalError(e) => {
                assert!(e.to_string().contains("scan failure"));
            }
            _ => panic!("Expected ExternalError, got {:?}", err),
        }

        assert!(rb_stream.next().await.is_none());
    }

    #[test]
    fn test_sync_record_batch_stream_reader() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let handle = rt.handle().clone();

        let schema = test_schema();
        let batch1 = test_scan_batch(schema.clone(), vec![10, 20], vec!["x", "y"]);
        let boxed = Box::pin(futures::stream::iter(vec![Ok(batch1)]));
        let scan_stream = ScanBatchStream::new(boxed, schema.clone());
        let mut reader = scan_stream.to_record_batch_reader(handle);

        assert_eq!(arrow::record_batch::RecordBatchReader::schema(&reader), schema);

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        assert!(reader.next().is_none());
    }
}
