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

use crate::error::{Error, Result};
use crate::metadata::{TableBucket, TablePath};
use bytes::Bytes;
use tokio::sync::oneshot;

pub struct LookupQuery<T> {
    table_path: TablePath,
    table_bucket: TableBucket,
    key: Bytes,
    retries: i32,
    result_tx: Option<oneshot::Sender<Result<T>>>,
}

impl<T> LookupQuery<T> {
    pub fn new(
        table_path: TablePath,
        table_bucket: TableBucket,
        key: Bytes,
        result_tx: oneshot::Sender<Result<T>>,
    ) -> Self {
        Self {
            table_path,
            table_bucket,
            key,
            retries: 0,
            result_tx: Some(result_tx),
        }
    }

    pub fn table_path(&self) -> &TablePath {
        &self.table_path
    }

    pub fn table_bucket(&self) -> &TableBucket {
        &self.table_bucket
    }

    pub fn key(&self) -> &Bytes {
        &self.key
    }

    pub fn retries(&self) -> i32 {
        self.retries
    }

    pub fn increment_retries(&mut self) {
        self.retries += 1;
    }

    pub fn is_done(&self) -> bool {
        self.result_tx.is_none()
    }

    pub fn complete(&mut self, result: Result<T>) {
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(result);
        }
    }

    pub fn complete_with_error(&mut self, error: Error) {
        self.complete(Err(error));
    }
}

pub type PrimaryLookupQuery = LookupQuery<Option<Vec<u8>>>;
pub type PrefixLookupQuery = LookupQuery<Vec<Vec<u8>>>;

pub enum QueuedLookup {
    Primary(PrimaryLookupQuery),
    Prefix(PrefixLookupQuery),
}

impl QueuedLookup {
    pub fn table_path(&self) -> &TablePath {
        match self {
            Self::Primary(q) => q.table_path(),
            Self::Prefix(q) => q.table_path(),
        }
    }

    pub fn table_bucket(&self) -> &TableBucket {
        match self {
            Self::Primary(q) => q.table_bucket(),
            Self::Prefix(q) => q.table_bucket(),
        }
    }

    pub fn key(&self) -> &Bytes {
        match self {
            Self::Primary(q) => q.key(),
            Self::Prefix(q) => q.key(),
        }
    }

    pub fn complete_with_error(&mut self, error: Error) {
        match self {
            Self::Primary(q) => q.complete_with_error(error),
            Self::Prefix(q) => q.complete_with_error(error),
        }
    }
}

impl From<PrimaryLookupQuery> for QueuedLookup {
    fn from(q: PrimaryLookupQuery) -> Self {
        QueuedLookup::Primary(q)
    }
}

impl From<PrefixLookupQuery> for QueuedLookup {
    fn from(q: PrefixLookupQuery) -> Self {
        QueuedLookup::Prefix(q)
    }
}
