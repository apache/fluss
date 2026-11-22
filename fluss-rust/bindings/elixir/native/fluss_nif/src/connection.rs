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
use crate::config::NifConfig;
use fluss::client::FlussConnection;
use rustler::{Env, ResourceArc, Term};
use std::sync::Arc;

pub struct ConnectionResource {
    pub inner: Arc<FlussConnection>,
}

impl std::panic::RefUnwindSafe for ConnectionResource {}

#[rustler::resource_impl]
impl rustler::Resource for ConnectionResource {}

#[rustler::nif]
fn connection_new<'a>(env: Env<'a>, config: NifConfig) -> Term<'a> {
    let core_config = config.into_core();
    async_nif::spawn_task_with_result(env, async move {
        FlussConnection::new(core_config).await.map(|conn| {
            ResourceArc::new(ConnectionResource {
                inner: Arc::new(conn),
            })
        })
    })
}
