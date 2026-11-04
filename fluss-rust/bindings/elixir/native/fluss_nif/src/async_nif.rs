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
use rustler::{Encoder, Env, ResourceArc, Term};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use tokio::task::{AbortHandle, JoinHandle};

const PENDING: u8 = 0;
const REPLYING: u8 = 1;
const CANCELLED: u8 = 2;

/// A task its caller can cancel. Dropping it cancels too, so a lookup doesn't outlive a caller
/// that died; dropping core's future makes core stop retrying it.
pub struct TaskResource {
    /// The first CAS out of PENDING wins: a reply is sent only if no cancel claimed it first.
    state: Arc<AtomicU8>,
    abort: AbortHandle,
}

impl TaskResource {
    /// `true` if the task won't reply; `false` if its reply is on its way.
    fn cancel(&self) -> bool {
        let prev = self
            .state
            .compare_exchange(PENDING, CANCELLED, Ordering::AcqRel, Ordering::Acquire)
            .unwrap_or_else(|current| current);
        // A replying task has no await left, so aborting it is a no-op.
        self.abort.abort();
        prev != REPLYING
    }
}

impl Drop for TaskResource {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl std::panic::RefUnwindSafe for TaskResource {}

#[rustler::resource_impl]
impl rustler::Resource for TaskResource {}

#[rustler::nif]
fn task_cancel(task: ResourceArc<TaskResource>) -> rustler::Atom {
    if task.cancel() {
        atoms::cancelled()
    } else {
        atoms::replying()
    }
}

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
    spawn_reply(env, future, |env, val| Ok(val.encode(env)), None).0
}

/// `encode` builds the reply term in the reply `Env`; an `Err` is sent as a client error.
/// Returns the reply ref and a `TaskResource` that cancels the task.
pub fn spawn_task_encode<'a, F, T, E>(
    env: Env<'a>,
    future: F,
    encode: E,
) -> (Term<'a>, ResourceArc<TaskResource>)
where
    F: Future<Output = Result<T, CoreError>> + Send + 'static,
    T: Send + 'static,
    E: for<'b> FnOnce(Env<'b>, T) -> Result<Term<'b>, String> + Send + 'static,
{
    let state = Arc::new(AtomicU8::new(PENDING));
    let (ref_term, join) = spawn_reply(env, future, encode, Some(state.clone()));
    let task = TaskResource {
        state,
        abort: join.abort_handle(),
    };
    (ref_term, ResourceArc::new(task))
}

fn spawn_reply<'a, F, T, E>(
    env: Env<'a>,
    future: F,
    encode: E,
    state: Option<Arc<AtomicU8>>,
) -> (Term<'a>, JoinHandle<()>)
where
    F: Future<Output = Result<T, CoreError>> + Send + 'static,
    T: Send + 'static,
    E: for<'b> FnOnce(Env<'b>, T) -> Result<Term<'b>, String> + Send + 'static,
{
    let pid = env.pid();
    let ref_term: Term<'a> = *env.make_ref();
    let mut task_env = OwnedEnv::new();
    let saved_ref = task_env.save(ref_term);

    let join = RUNTIME.spawn(async move {
        let result = future.await;
        let cancelled = state.is_some_and(|s| {
            s.compare_exchange(PENDING, REPLYING, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        });
        if cancelled {
            return;
        }
        let _ = task_env.send_and_clear(&pid, |env| {
            let r = saved_ref.load(env);
            match result {
                // A panic would leave a caller that was told `:replying` waiting forever.
                Ok(val) => match std::panic::catch_unwind(AssertUnwindSafe(|| encode(env, val)))
                    .unwrap_or_else(|_| Err("reply encoding panicked".to_string()))
                {
                    Ok(term) => (r, (atoms::ok(), term)).encode(env),
                    Err(message) => {
                        (r, (atoms::error(), NifFlussError::client(message))).encode(env)
                    }
                },
                Err(e) => (r, encode_err(env, e)).encode(env),
            }
        });
    });

    (ref_term, join)
}
