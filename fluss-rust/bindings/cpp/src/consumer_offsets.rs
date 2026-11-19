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

use super::*;
use fcore::client::{
    OffsetAndMetadata, OffsetCommitError, OffsetCommitHandle, OffsetCommitOutcome,
};
use fcore::metadata::TableBucket;

fn bucket(value: &ffi::FfiConsumerOffset) -> TableBucket {
    TableBucket::new_with_partition(
        value.table_id,
        value.has_partition.then_some(value.partition_id),
        value.bucket_id,
    )
}

fn entry(
    bucket: TableBucket,
    value: Option<OffsetAndMetadata>,
    result: ffi::FfiResult,
    unknown: bool,
) -> ffi::FfiConsumerOffset {
    ffi::FfiConsumerOffset {
        table_id: bucket.table_id(),
        bucket_id: bucket.bucket_id(),
        has_partition: bucket.partition_id().is_some(),
        partition_id: bucket.partition_id().unwrap_or(0),
        has_offset: value.is_some(),
        offset: value.as_ref().map_or(0, |v| v.offset),
        has_metadata: value.as_ref().is_some_and(|v| v.metadata.is_some()),
        metadata: value.and_then(|v| v.metadata).unwrap_or_default(),
        result,
        unknown,
    }
}

impl ffi::FfiConsumerOffsetsResult {
    fn success(offsets: Vec<ffi::FfiConsumerOffset>) -> Self {
        Self {
            result: ok_result(),
            unknown: offsets.iter().any(|offset| offset.unknown),
            needs_recovery: false,
            offsets,
        }
    }

    fn failure(error: OffsetCommitError) -> Self {
        Self {
            unknown: matches!(&error, OffsetCommitError::Unknown(_)),
            needs_recovery: matches!(&error, OffsetCommitError::NeedsRecovery),
            result: match error {
                OffsetCommitError::Rejected(error) | OffsetCommitError::Unknown(error) => {
                    err_from_core_error(&error)
                }
                OffsetCommitError::NeedsRecovery => ffi::FfiResult {
                    error_code: CLIENT_ERROR_CODE,
                    error_message: error.to_string(),
                },
            },
            offsets: Vec::new(),
        }
    }
}

impl Admin {
    pub(super) fn get_consumer_offsets(
        &self,
        group_id: &str,
        buckets: Vec<ffi::FfiConsumerOffset>,
    ) -> ffi::FfiConsumerOffsetsResult {
        let buckets: Vec<_> = buckets.iter().map(bucket).collect();
        match RUNTIME.block_on(self.inner.get_consumer_offsets(group_id, &buckets)) {
            Ok(offsets) => ffi::FfiConsumerOffsetsResult::success(
                offsets
                    .into_iter()
                    .map(|(bucket, value)| entry(bucket, value, ok_result(), false))
                    .collect(),
            ),
            Err(error) => {
                ffi::FfiConsumerOffsetsResult::failure(OffsetCommitError::Rejected(error))
            }
        }
    }
}

impl LogScanner {
    pub(super) fn check_delivery(&self) -> fcore::error::Result<()> {
        if self
            .delivery_pending
            .as_ref()
            .is_some_and(|v| v.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(fcore::error::Error::IllegalArgument {
                message:
                    "Poll result was not delivered; recreate the scanner from committed offsets"
                        .into(),
            });
        }
        Ok(())
    }
    pub(super) fn await_delivery(&self) {
        if let Some(flag) = &self.delivery_pending {
            flag.store(true, std::sync::atomic::Ordering::Release);
        }
    }
    pub(super) fn delivered(&self) {
        if let Some(flag) = &self.delivery_pending {
            flag.store(false, std::sync::atomic::Ordering::Release);
        }
    }

    pub(super) fn subscribe_with_initializer(
        &self,
        buckets: Vec<ffi::FfiConsumerOffset>,
        initializer: Vec<ffi::FfiOffsetsInitializer>,
    ) -> ffi::FfiResult {
        use fcore::client::OffsetsInitializer;
        let buckets: Vec<_> = buckets.iter().map(bucket).collect();
        let mut strategy = OffsetsInitializer::fail();
        for step in initializer.into_iter().rev() {
            strategy = match step.kind {
                0 => OffsetsInitializer::offset(step.value),
                1 => OffsetsInitializer::latest(),
                2 => OffsetsInitializer::timestamp(step.value),
                3 => OffsetsInitializer::fail(),
                4 => OffsetsInitializer::committed_with_fallback(step.group_id, strategy),
                _ => {
                    return err_from_core_error(&fcore::error::Error::IllegalArgument {
                        message: "Unknown offsets initializer".into(),
                    });
                }
            };
        }
        let result = RUNTIME.block_on(async {
            match &self.scanner {
                ScannerKind::Record(s) => s.subscribe_with_initializer(&buckets, &strategy).await,
                ScannerKind::Batch(s) => s.subscribe_with_initializer(&buckets, &strategy).await,
                ScannerKind::MultiRecord(s) => {
                    s.subscribe_with_initializer(&buckets, &strategy).await
                }
                ScannerKind::MultiBatch(s) => {
                    s.subscribe_with_initializer(&buckets, &strategy).await
                }
            }
        });
        match result {
            Ok(()) => ok_result(),
            Err(error) => err_from_core_error(&error),
        }
    }

    pub(super) fn begin_commit(
        &self,
        current: bool,
        offsets: Vec<ffi::FfiConsumerOffset>,
        mut promise: cxx::UniquePtr<ffi::CommitPromise>,
    ) {
        let _enter = RUNTIME.enter();
        match self.enqueue_commit(current, offsets) {
            Ok(handle) => {
                RUNTIME.spawn(async move {
                    let result = handle.await.unwrap_or_else(|error| Err(error.into()));
                    promise.pin_mut().resolve(&mut commit_response(result));
                });
            }
            Err(error) => {
                promise
                    .pin_mut()
                    .resolve(&mut commit_response(Err(OffsetCommitError::Rejected(
                        error,
                    ))))
            }
        }
    }

    fn enqueue_commit(
        &self,
        current: bool,
        offsets: Vec<ffi::FfiConsumerOffset>,
    ) -> fcore::error::Result<OffsetCommitHandle> {
        self.check_delivery()?;
        let values = commit_offsets(offsets)?;
        match &self.scanner {
            ScannerKind::Record(scanner) => {
                if current {
                    scanner.commit_async()
                } else {
                    scanner.commit_offsets_async(values)
                }
            }
            ScannerKind::Batch(scanner) => {
                if current {
                    scanner.commit_async()
                } else {
                    scanner.commit_offsets_async(values)
                }
            }
            ScannerKind::MultiRecord(scanner) => {
                if current {
                    scanner.commit_async()
                } else {
                    scanner.commit_offsets_async(values)
                }
            }
            ScannerKind::MultiBatch(scanner) => {
                if current {
                    scanner.commit_async()
                } else {
                    scanner.commit_offsets_async(values)
                }
            }
        }
    }
}

// SAFETY: one completion task owns the promise; resolving it never invokes user code.
unsafe impl Send for ffi::CommitPromise {}

impl Connection {
    pub(super) fn create_multi_table_scanner(
        &self,
        tables: Vec<ffi::FfiTablePath>,
        group_id: String,
        timeout_ms: u64,
        auto_commit_ms: u64,
        batch: bool,
    ) -> ffi::FfiPtrResult {
        let result: fcore::error::Result<LogScanner> = RUNTIME.block_on(async {
            let paths: Vec<_> = tables
                .into_iter()
                .map(|path| fcore::metadata::TablePath::new(path.database_name, path.table_name))
                .collect();
            let options = fcore::client::ConsumerGroupOptions {
                group_id,
                timeout: Duration::from_millis(timeout_ms),
            };
            let mut scan = self.inner.new_multi_table_scan(&paths, options).await?;
            if auto_commit_ms > 0 {
                scan = scan.with_auto_commit(Duration::from_millis(auto_commit_ms))?;
            }
            let mut table_columns = HashMap::new();
            for info in scan.table_infos() {
                table_columns.insert(info.table_id, info.schema.columns().to_vec());
            }
            let scanner = if batch {
                ScannerKind::MultiBatch(scan.create_record_batch_log_scanner()?)
            } else {
                ScannerKind::MultiRecord(scan.create_log_scanner()?)
            };
            Ok(LogScanner {
                delivery_pending: Some(Default::default()),
                scanner,
                projected_columns: Vec::new(),
                table_columns,
            })
        });
        match result {
            Ok(scanner) => ok_ptr(Box::into_raw(Box::new(scanner)) as usize),
            Err(error) => err_ptr_from_core(&error),
        }
    }
}

impl LogScanner {
    pub(super) fn unsubscribe_table_bucket(&self, value: ffi::FfiConsumerOffset) -> ffi::FfiResult {
        let bucket = bucket(&value);
        let result = RUNTIME.block_on(async {
            match &self.scanner {
                ScannerKind::MultiRecord(scanner) => scanner.unsubscribe(&bucket).await,
                ScannerKind::MultiBatch(scanner) => scanner.unsubscribe(&bucket).await,
                _ => Err(fcore::error::Error::IllegalArgument {
                    message: "Table-qualified unsubscribe requires a multi-table scanner".into(),
                }),
            }
        });
        match result {
            Ok(()) => ok_result(),
            Err(error) => err_from_core_error(&error),
        }
    }
}

fn commit_offsets(
    values: Vec<ffi::FfiConsumerOffset>,
) -> fcore::error::Result<HashMap<TableBucket, OffsetAndMetadata>> {
    let mut result = HashMap::new();
    for value in values {
        let key = bucket(&value);
        if result
            .insert(
                key,
                OffsetAndMetadata {
                    offset: value.offset,
                    metadata: value.has_metadata.then_some(value.metadata),
                },
            )
            .is_some()
        {
            return Err(fcore::error::Error::IllegalArgument {
                message: "Duplicate offset bucket".into(),
            });
        }
    }
    Ok(result)
}

impl Admin {
    pub(super) fn list_consumer_offsets(&self, group_id: &str) -> ffi::FfiConsumerOffsetsResult {
        match RUNTIME.block_on(self.inner.list_consumer_offsets(group_id)) {
            Ok(offsets) => ffi::FfiConsumerOffsetsResult::success(
                offsets
                    .into_iter()
                    .map(|(bucket, value)| entry(bucket, Some(value), ok_result(), false))
                    .collect(),
            ),
            Err(error) => {
                ffi::FfiConsumerOffsetsResult::failure(OffsetCommitError::Rejected(error))
            }
        }
    }
    pub(super) fn alter_consumer_offsets(
        &self,
        group_id: &str,
        values: Vec<ffi::FfiConsumerOffset>,
    ) -> ffi::FfiConsumerOffsetsResult {
        let result = RUNTIME.block_on(async {
            self.inner
                .alter_consumer_offsets(
                    group_id,
                    commit_offsets(values).map_err(OffsetCommitError::Rejected)?,
                )
                .await
        });
        commit_response(result)
    }
    pub(super) fn delete_consumer_offsets(
        &self,
        group_id: &str,
        values: Vec<ffi::FfiConsumerOffset>,
    ) -> ffi::FfiConsumerOffsetsResult {
        let buckets: Vec<_> = values.iter().map(bucket).collect();
        match RUNTIME.block_on(self.inner.delete_consumer_offsets(group_id, &buckets)) {
            Ok(results) => ffi::FfiConsumerOffsetsResult::success(
                results
                    .into_iter()
                    .map(|(bucket, result)| {
                        let (result, unknown) = deletion_status(result);
                        entry(bucket, None, result, unknown)
                    })
                    .collect(),
            ),
            Err(error) => ffi::FfiConsumerOffsetsResult::failure(error),
        }
    }
    pub(super) fn list_consumer_groups(&self) -> ffi::FfiConsumerGroupsResult {
        match RUNTIME.block_on(self.inner.list_consumer_groups()) {
            Ok(groups) => ffi::FfiConsumerGroupsResult {
                result: ok_result(),
                groups: groups
                    .into_iter()
                    .map(|group_id| ffi::FfiConsumerGroupResult {
                        group_id,
                        result: ok_result(),
                        unknown: false,
                    })
                    .collect(),
            },
            Err(error) => ffi::FfiConsumerGroupsResult {
                result: err_from_core_error(&error),
                groups: Vec::new(),
            },
        }
    }
    pub(super) fn delete_consumer_groups(
        &self,
        groups: Vec<String>,
    ) -> ffi::FfiConsumerGroupsResult {
        match RUNTIME.block_on(self.inner.delete_consumer_groups(&groups)) {
            Ok(groups) => ffi::FfiConsumerGroupsResult {
                result: ok_result(),
                groups: groups
                    .into_iter()
                    .map(|(group_id, result)| {
                        let (result, unknown) = deletion_status(result);
                        ffi::FfiConsumerGroupResult {
                            group_id,
                            result,
                            unknown,
                        }
                    })
                    .collect(),
            },
            Err(error) => ffi::FfiConsumerGroupsResult {
                result: err_from_core_error(&error),
                groups: Vec::new(),
            },
        }
    }
}

fn deletion_status(result: std::result::Result<(), OffsetCommitError>) -> (ffi::FfiResult, bool) {
    match result {
        Ok(()) => (ok_result(), false),
        Err(error) => {
            let value = ffi::FfiConsumerOffsetsResult::failure(error);
            (value.result, value.unknown)
        }
    }
}

fn commit_response(
    result: std::result::Result<fcore::client::OffsetCommitResult, OffsetCommitError>,
) -> ffi::FfiConsumerOffsetsResult {
    match result {
        Ok(offsets) => ffi::FfiConsumerOffsetsResult::success(
            offsets
                .into_iter()
                .map(|(bucket, outcome)| {
                    let unknown = matches!(&outcome, OffsetCommitOutcome::Unknown(_));
                    let result = match outcome {
                        OffsetCommitOutcome::Committed => ok_result(),
                        OffsetCommitOutcome::Rejected(error)
                        | OffsetCommitOutcome::Unknown(error) => ffi::FfiResult {
                            error_code: error.code,
                            error_message: error.message,
                        },
                    };
                    entry(bucket, None, result, unknown)
                })
                .collect(),
        ),
        Err(error) => ffi::FfiConsumerOffsetsResult::failure(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewind_guard_is_distinct_from_an_unknown_write() {
        let refused = ffi::FfiConsumerOffsetsResult::failure(OffsetCommitError::NeedsRecovery);
        assert!(refused.needs_recovery && !refused.unknown);
        let unknown = ffi::FfiConsumerOffsetsResult::failure(OffsetCommitError::Unknown(
            fcore::error::Error::IllegalArgument {
                message: "test".into(),
            },
        ));
        assert!(unknown.unknown && !unknown.needs_recovery);
    }
}
