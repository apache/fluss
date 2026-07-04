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

use crate::async_nif;
use fluss::client::WriteResultFuture;
use rustler::{Env, ResourceArc, Term};
use std::sync::Mutex;

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

#[rustler::nif]
fn write_handle_wait<'a>(env: Env<'a>, handle: ResourceArc<WriteHandleResource>) -> Term<'a> {
    let future = handle.inner.lock().unwrap().take();
    match future {
        Some(f) => async_nif::spawn_task(env, f),
        None => async_nif::send_client_error(env, "WriteHandle already consumed"),
    }
}
