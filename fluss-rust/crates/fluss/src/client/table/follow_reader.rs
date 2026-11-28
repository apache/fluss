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

use super::{RecordBatchLogScanner, reader::validate_read_buckets};
use crate::client::FlussAdmin;
use crate::error::{Error, FlussError, Result};
use crate::metadata::TableBucket;
use crate::record::ScanBatch;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Source batches and exclusive progress belong to the same poll.
pub enum FollowLogEvent {
    /// Apply every batch before committing these next unread offsets.
    Data {
        batches: Vec<ScanBatch>,
        next_offsets: HashMap<TableBucket, i64>,
    },
    /// Confirmed deleted partition IDs, with the next unread offset of each bucket.
    PartitionsDropped(HashMap<TableBucket, i64>),
}

/// Follows concrete offsets without discovering new partitions.
pub struct FollowLogReader {
    scanner: RecordBatchLogScanner,
    admin: Arc<FlussAdmin>,
    offsets: HashMap<TableBucket, i64>,
    subscribed: bool,
}

impl FollowLogReader {
    pub(super) fn new(
        scanner: RecordBatchLogScanner,
        admin: Arc<FlussAdmin>,
        offsets: HashMap<TableBucket, i64>,
    ) -> Result<Self> {
        validate_read_buckets(
            scanner.table_id(),
            scanner.is_partitioned(),
            |b| scanner.bucket_count(b.partition_id()),
            offsets.keys(),
        )?;
        if offsets.values().any(|o| *o < 0) {
            return Err(Error::IllegalArgument {
                message: "follow requires concrete nonnegative offsets".into(),
            });
        }
        scanner.try_set_reader_active()?;
        Ok(Self {
            scanner,
            admin,
            offsets,
            subscribed: false,
        })
    }
    /// Ends when all assigned partitions have been removed. A removed partition's
    /// last consumed position is not a guarantee that its log was drained.
    pub async fn next_event(&mut self) -> Result<Option<FollowLogEvent>> {
        while !self.offsets.is_empty() {
            let result = if self.subscribed {
                self.scanner.poll_follow().await.map(Some)
            } else {
                let result = if self.scanner.is_partitioned() {
                    let assignments = self
                        .offsets
                        .iter()
                        .map(|(b, o)| ((b.partition_id().unwrap(), b.bucket_id()), *o))
                        .collect();
                    self.scanner
                        .subscribe_partition_buckets_for_reader(&assignments)
                        .await
                } else {
                    self.scanner
                        .subscribe_buckets_for_reader(
                            &self
                                .offsets
                                .iter()
                                .map(|(b, o)| (b.bucket_id(), *o))
                                .collect(),
                        )
                        .await
                };
                result.and_then(|()| {
                    validate_read_buckets(
                        self.scanner.table_id(),
                        self.scanner.is_partitioned(),
                        |b| self.scanner.bucket_count(b.partition_id()),
                        self.offsets.keys(),
                    )?;
                    self.subscribed = true;
                    Ok(None)
                })
            };
            match result {
                Ok(Some(p)) if !p.consumed_up_to_offsets.is_empty() || !p.batches.is_empty() => {
                    self.offsets.extend(
                        p.consumed_up_to_offsets
                            .iter()
                            .map(|(b, o)| (b.clone(), *o)),
                    );
                    return Ok(Some(FollowLogEvent::Data {
                        batches: p.batches,
                        next_offsets: p.consumed_up_to_offsets,
                    }));
                }
                Ok(_) => (),
                Err(e)
                    if e.api_error() == Some(FlussError::PartitionNotExists)
                        && self.scanner.is_partitioned() =>
                {
                    let path = self.scanner.table_path();
                    let live: HashSet<_> = self
                        .admin
                        .list_partition_infos(path)
                        .await?
                        .into_iter()
                        .map(|p| p.get_partition_id())
                        .collect();
                    if self.admin.get_table_info(path).await?.table_id != self.scanner.table_id() {
                        return Err(Error::table_not_exist(format!(
                            "follow table {path} was replaced"
                        )));
                    }
                    let mut dropped = HashMap::new();
                    self.offsets.retain(|bucket, offset| {
                        if live.contains(&bucket.partition_id().unwrap()) {
                            true
                        } else {
                            dropped.insert(bucket.clone(), *offset);
                            false
                        }
                    });
                    if dropped.is_empty() {
                        return Err(e);
                    }
                    for b in dropped.keys() {
                        self.scanner
                            .unsubscribe_partition_sync(b.partition_id().unwrap(), b.bucket_id());
                    }
                    return Ok(Some(FollowLogEvent::PartitionsDropped(dropped)));
                }
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }
}
