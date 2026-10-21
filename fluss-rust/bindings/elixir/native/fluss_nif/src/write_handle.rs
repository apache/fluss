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

use crate::RUNTIME;
use crate::async_nif;
use crate::atoms::{self, client_err, to_nif_err};
use fluss::client::WriteResultFuture;
use fluss::error::Error as CoreError;
use rustler::{Encoder, Env, ResourceArc, Term};
use std::sync::Mutex;
use std::time::Instant;

pub struct WriteHandleResource {
    inner: Mutex<Option<WriteResultFuture>>,
}

impl std::panic::RefUnwindSafe for WriteHandleResource {}

#[rustler::resource_impl]
impl rustler::Resource for WriteHandleResource {}

impl WriteHandleResource {
    pub fn new(future: WriteResultFuture) -> Self {
        Self {
            inner: Mutex::new(Some(future)),
        }
    }
}

/// Queues a write and returns its handle, or `{:pending, ref}` when the buffer is full.
/// Core blocks the calling thread until the buffer has room, so the first try uses a
/// deadline of now (fails fast) and only the retry waits, on a tokio blocking thread.
pub fn queue_write<'a, W>(env: Env<'a>, write: W) -> Result<Term<'a>, rustler::Error>
where
    W: Fn(Option<Instant>) -> Result<WriteResultFuture, CoreError> + Send + 'static,
{
    match write(Some(Instant::now())) {
        Ok(future) => Ok(ResourceArc::new(WriteHandleResource::new(future)).encode(env)),
        Err(CoreError::BufferExhausted { .. }) => {
            let join = RUNTIME.spawn_blocking(move || write(None));
            let pending = async_nif::spawn_task_with_result(env, async move {
                let future = join.await.map_err(|e| CoreError::UnexpectedError {
                    message: "write task failed".to_string(),
                    source: Some(Box::new(e)),
                })??;
                Ok(ResourceArc::new(WriteHandleResource::new(future)))
            });
            Ok((atoms::pending(), pending).encode(env))
        }
        Err(e) => Err(to_nif_err(e)),
    }
}

#[rustler::nif]
fn write_handle_wait<'a>(
    env: Env<'a>,
    handle: ResourceArc<WriteHandleResource>,
) -> Result<Term<'a>, rustler::Error> {
    let future = handle.inner.lock().unwrap().take();
    match future {
        Some(f) => Ok(async_nif::spawn_task(env, f)),
        None => Err(client_err("WriteHandle already consumed")),
    }
}
