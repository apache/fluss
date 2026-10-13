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

//! Async NIF helpers — spawn on tokio, send `{ref, result}` back as a BEAM
//! message instead of blocking dirty schedulers.

use crate::RUNTIME;
use crate::atoms::{self, NifFlussError};
use fluss::error::Error as CoreError;
use rustler::env::OwnedEnv;
use rustler::{Encoder, Env, Term};
use std::future::Future;

fn encode_err<'a>(env: Env<'a>, err: CoreError) -> Term<'a> {
    (atoms::error(), NifFlussError::from_core(&err)).encode(env)
}

pub fn spawn_task<'a, F>(env: Env<'a>, future: F) -> Term<'a>
where
    F: Future<Output = Result<(), CoreError>> + Send + 'static,
{
    let pid = env.pid();
    let ref_term: Term<'a> = *env.make_ref();
    let mut task_env = OwnedEnv::new();
    let saved_ref = task_env.save(ref_term);

    RUNTIME.spawn(async move {
        let result = future.await;
        let _ = task_env.send_and_clear(&pid, |env| {
            let r = saved_ref.load(env);
            match result {
                Ok(()) => (r, atoms::ok()).encode(env),
                Err(e) => (r, encode_err(env, e)).encode(env),
            }
        });
    });

    ref_term
}

pub fn spawn_task_with_result<'a, F, T>(env: Env<'a>, future: F) -> Term<'a>
where
    F: Future<Output = Result<T, CoreError>> + Send + 'static,
    T: Encoder + Send + 'static,
{
    let pid = env.pid();
    let ref_term: Term<'a> = *env.make_ref();
    let mut task_env = OwnedEnv::new();
    let saved_ref = task_env.save(ref_term);

    RUNTIME.spawn(async move {
        let result = future.await;
        let _ = task_env.send_and_clear(&pid, |env| {
            let r = saved_ref.load(env);
            match result {
                Ok(val) => (r, (atoms::ok(), val)).encode(env),
                Err(e) => (r, encode_err(env, e)).encode(env),
            }
        });
    });

    ref_term
}

pub fn send_client_error<'a>(env: Env<'a>, msg: &str) -> Term<'a> {
    let pid = env.pid();
    let ref_term: Term<'a> = *env.make_ref();
    let mut task_env = OwnedEnv::new();
    let saved_ref = task_env.save(ref_term);
    let message = msg.to_string();

    let _ = task_env.send_and_clear(&pid, |env| {
        let r = saved_ref.load(env);
        let err = NifFlussError::client(message);
        (r, (atoms::error(), err)).encode(env)
    });

    ref_term
}
