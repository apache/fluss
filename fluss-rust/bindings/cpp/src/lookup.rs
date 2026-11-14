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

//! Lookups for C++: blocking, or started in the background and waited for
//! through a handle.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use fluss as fcore;
use fluss::metadata::TableInfo;
use fluss::row::GenericRow;
use fluss::rpc::FlussError as CoreFlussError;
use tokio::task::JoinHandle;

use crate::{
    CLIENT_ERROR_CODE, GenericRowInner, LookupResultInner, Lookuper, PrefixLookupResultInner,
    PrefixLookuper, RUNTIME, client_err_ptr, err_from_core_error, ffi, ok_ptr, types,
};

/// A lookup result for C++, or the error in its place.
trait Outcome: Send + 'static {
    fn from_error(code: i32, message: String) -> Self;
}

impl Outcome for Box<LookupResultInner> {
    fn from_error(code: i32, message: String) -> Self {
        Box::new(LookupResultInner::from_error(code, message))
    }
}

impl Outcome for Box<PrefixLookupResultInner> {
    fn from_error(code: i32, message: String) -> Self {
        Box::new(PrefixLookupResultInner::from_error(code, message))
    }
}

/// A lookup running in the background. Dropping it abandons the lookup.
struct Pending<T> {
    task: Option<JoinHandle<T>>,
}

impl<T: Outcome> Pending<T> {
    fn start(lookup: impl Future<Output = T> + Send + 'static) -> Self {
        Self {
            task: Some(RUNTIME.spawn(lookup)),
        }
    }

    /// Waits up to `timeout_ms` for the outcome, or without limit when it is
    /// negative. A timeout leaves the lookup running, to be waited for again.
    fn wait(&mut self, timeout_ms: i64) -> T {
        let Some(task) = self.task.as_mut() else {
            return T::from_error(
                CLIENT_ERROR_CODE,
                "Lookup was already waited for".to_string(),
            );
        };
        let joined = match u64::try_from(timeout_ms) {
            Ok(ms) => {
                let timeout = Duration::from_millis(ms);
                match RUNTIME.block_on(async { tokio::time::timeout(timeout, task).await }) {
                    Ok(joined) => joined,
                    Err(_) => {
                        return T::from_error(
                            CoreFlussError::RequestTimeOut.code(),
                            "Lookup did not complete within the wait timeout".to_string(),
                        );
                    }
                }
            }
            Err(_) => RUNTIME.block_on(task),
        };
        self.task = None;
        joined.unwrap_or_else(|e| T::from_error(CLIENT_ERROR_CODE, format!("Lookup failed: {e}")))
    }

    fn is_pending(&self) -> bool {
        self.task.is_some()
    }
}

impl<T> Drop for Pending<T> {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub struct PendingLookup(Pending<Box<LookupResultInner>>);

pub struct PendingPrefixLookup(Pending<Box<PrefixLookupResultInner>>);

impl PendingLookup {
    pub(crate) fn lookup_wait(&mut self, timeout_ms: i64) -> Box<LookupResultInner> {
        self.0.wait(timeout_ms)
    }

    pub(crate) fn lookup_is_pending(&self) -> bool {
        self.0.is_pending()
    }
}

impl PendingPrefixLookup {
    pub(crate) fn prefix_lookup_wait(&mut self, timeout_ms: i64) -> Box<PrefixLookupResultInner> {
        self.0.wait(timeout_ms)
    }

    pub(crate) fn prefix_lookup_is_pending(&self) -> bool {
        self.0.is_pending()
    }
}

pub(crate) unsafe fn delete_pending_lookup(pending: *mut PendingLookup) {
    if !pending.is_null() {
        unsafe {
            drop(Box::from_raw(pending));
        }
    }
}

pub(crate) unsafe fn delete_pending_prefix_lookup(pending: *mut PendingPrefixLookup) {
    if !pending.is_null() {
        unsafe {
            drop(Box::from_raw(pending));
        }
    }
}

impl Lookuper {
    /// The lookup key: the primary-key values, dense as the core key encoder
    /// expects them, and owned, so a lookup can outlive the C++ row.
    fn key(&self, pk_row: &GenericRowInner) -> Result<GenericRow<'static>, String> {
        let schema = self.table_info.get_schema();
        let pk_indices = schema.primary_key_indexes();
        types::resolve_dense_row_types(&pk_row.row, Some(schema), &pk_indices)
            .map(|row| row.into_owned().into_owned())
            .map_err(|e| e.to_string())
    }

    fn run(
        &self,
        key: GenericRow<'static>,
    ) -> impl Future<Output = Box<LookupResultInner>> + Send + 'static {
        let inner = Arc::clone(&self.inner);
        let table_info = Arc::clone(&self.table_info);
        async move { to_lookup_result(inner.lookup(&key).await, &table_info) }
    }

    pub(crate) fn lookup(&self, pk_row: &GenericRowInner) -> Box<LookupResultInner> {
        match self.key(pk_row) {
            Ok(key) => RUNTIME.block_on(self.run(key)),
            Err(e) => Box::new(LookupResultInner::from_error(CLIENT_ERROR_CODE, e)),
        }
    }

    pub(crate) fn lookup_async(&self, pk_row: &GenericRowInner) -> ffi::FfiPtrResult {
        match self.key(pk_row) {
            Ok(key) => {
                let pending = PendingLookup(Pending::start(self.run(key)));
                ok_ptr(Box::into_raw(Box::new(pending)) as usize)
            }
            Err(e) => client_err_ptr(e),
        }
    }
}

impl PrefixLookuper {
    /// The lookup key: the prefix values, dense and in lookup-column order as the
    /// core prefix encoder expects them, and owned, so a lookup can outlive the
    /// C++ row.
    fn key(&self, prefix_row: &GenericRowInner) -> Result<GenericRow<'static>, String> {
        let schema = self.table_info.get_schema();
        types::resolve_dense_row_types(&prefix_row.row, Some(schema), &self.lookup_column_indices)
            .map(|row| row.into_owned().into_owned())
            .map_err(|e| e.to_string())
    }

    fn run(
        &self,
        key: GenericRow<'static>,
    ) -> impl Future<Output = Box<PrefixLookupResultInner>> + Send + 'static {
        let inner = Arc::clone(&self.inner);
        let table_info = Arc::clone(&self.table_info);
        async move { to_prefix_lookup_result(inner.lookup(&key).await, &table_info) }
    }

    pub(crate) fn prefix_lookup(
        &self,
        prefix_row: &GenericRowInner,
    ) -> Box<PrefixLookupResultInner> {
        match self.key(prefix_row) {
            Ok(key) => RUNTIME.block_on(self.run(key)),
            Err(e) => Box::new(PrefixLookupResultInner::from_error(CLIENT_ERROR_CODE, e)),
        }
    }

    pub(crate) fn prefix_lookup_async(&self, prefix_row: &GenericRowInner) -> ffi::FfiPtrResult {
        match self.key(prefix_row) {
            Ok(key) => {
                let pending = PendingPrefixLookup(Pending::start(self.run(key)));
                ok_ptr(Box::into_raw(Box::new(pending)) as usize)
            }
            Err(e) => client_err_ptr(e),
        }
    }
}

fn to_lookup_result(
    result: fcore::error::Result<fcore::client::LookupResult>,
    table_info: &TableInfo,
) -> Box<LookupResultInner> {
    let lookup_result = match result {
        Ok(r) => r,
        Err(e) => return core_error(&e),
    };
    let columns = table_info.get_schema().columns().to_vec();
    match lookup_result.get_single_row() {
        Ok(Some(row)) => match types::compacted_row_to_owned(&row, table_info) {
            Ok(owned_row) => Box::new(LookupResultInner {
                error: None,
                found: true,
                row: Some(owned_row),
                columns,
            }),
            Err(e) => Box::new(LookupResultInner::from_error(
                CLIENT_ERROR_CODE,
                e.to_string(),
            )),
        },
        Ok(None) => Box::new(LookupResultInner {
            error: None,
            found: false,
            row: None,
            columns,
        }),
        Err(e) => core_error(&e),
    }
}

fn to_prefix_lookup_result(
    result: fcore::error::Result<fcore::client::LookupResult>,
    table_info: &TableInfo,
) -> Box<PrefixLookupResultInner> {
    let lookup_result = match result {
        Ok(r) => r,
        Err(e) => return core_error(&e),
    };
    let lookup_rows = match lookup_result.get_rows() {
        Ok(rows) => rows,
        Err(e) => return core_error(&e),
    };
    let mut rows = Vec::with_capacity(lookup_rows.len());
    for row in &lookup_rows {
        match types::compacted_row_to_owned(row, table_info) {
            Ok(owned_row) => rows.push(owned_row),
            Err(e) => {
                return Box::new(PrefixLookupResultInner::from_error(
                    CLIENT_ERROR_CODE,
                    e.to_string(),
                ));
            }
        }
    }
    let columns = table_info.get_schema().columns().to_vec();
    Box::new(PrefixLookupResultInner {
        error: None,
        rows,
        columns,
    })
}

fn core_error<T: Outcome>(e: &fcore::error::Error) -> T {
    let ffi_err = err_from_core_error(e);
    T::from_error(ffi_err.error_code, ffi_err.error_message)
}
