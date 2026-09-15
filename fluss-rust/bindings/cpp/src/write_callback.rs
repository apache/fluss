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

#[cfg(test)]
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, LazyLock, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use crate::{RUNTIME, WriteResult, client_err, err_from_core_error, ffi, ok_result};

const CALLBACK_WORKERS: usize = 4;
const COMPLETION_BATCH_SIZE: usize = 64;
type Completion = Box<dyn FnOnce() + Send + 'static>;

// Like RUNTIME, the executor is process-wide and lives until process exit.
// Initialize it on the submitting thread, not an async I/O worker.
static CALLBACK_EXECUTOR: LazyLock<Option<CallbackExecutor>> =
    LazyLock::new(|| match CallbackExecutor::new(CALLBACK_WORKERS) {
        Ok(executor) => Some(executor),
        Err(error) => {
            // The write has already been accepted. Keep the old dispatch path
            // as a resource-exhaustion fallback rather than lose its callback.
            eprintln!("Fluss callback worker initialization failed: {error}; using blocking pool");
            None
        }
    });

struct CallbackExecutor {
    sender: Option<mpsc::Sender<Completion>>,
    workers: Vec<JoinHandle<()>>,
}

impl CallbackExecutor {
    fn new(worker_count: usize) -> std::io::Result<Self> {
        assert!(worker_count > 0);
        let (sender, receiver) = mpsc::channel::<Completion>();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut executor = Self {
            sender: Some(sender),
            workers: Vec::with_capacity(worker_count),
        };
        for index in 0..worker_count {
            let receiver = Arc::clone(&receiver);
            executor.workers.push(
                thread::Builder::new()
                    .name(format!("fluss-callback-{index}"))
                    .spawn(move || {
                        loop {
                            let completion = {
                                // Each job already contains up to 64 callbacks.
                                // Do not prefetch 64 jobs here: that would let a
                                // worker hoard up to 4096 callbacks.
                                let receiver = receiver.lock().unwrap();
                                let Ok(completion) = receiver.recv() else {
                                    break;
                                };
                                completion
                            };
                            // Never hold a queue lock or enter a Tokio runtime
                            // while running user code. Synchronous SDK calls
                            // from a callback can safely use RUNTIME.block_on.
                            if catch_unwind(AssertUnwindSafe(completion)).is_err() {
                                eprintln!("Fluss callback worker contained a Rust panic");
                            }
                        }
                    })?,
            );
        }
        Ok(executor)
    }

    fn enqueue(&self, completion: Completion) -> Result<(), Completion> {
        // An unbounded completion queue keeps slow user callbacks from blocking
        // async I/O workers. Applications must bound outstanding callbacks;
        // this caps threads, not queued captures or total process memory.
        self.sender
            .as_ref()
            .unwrap()
            .send(completion)
            .map_err(|error| error.0)
    }
}

impl Drop for CallbackExecutor {
    fn drop(&mut self) {
        // Also handles partial worker initialization and lets tests verify
        // drain/release. The process-wide static is not dropped at exit.
        drop(self.sender.take());
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

// SAFETY: The C++ wrapper is transferred by UniquePtr and accessed exclusively
// by one batch registration, then one callback worker. It is never shared concurrently.
// The public C++ contract requires captures to support background execution.
unsafe impl Send for ffi::WriteCallback {}

impl WriteResult {
    pub(crate) fn notify(
        &mut self,
        mut callback: cxx::UniquePtr<ffi::WriteCallback>,
    ) -> ffi::FfiResult {
        if callback.is_null() {
            return client_err("Write callback must not be empty".to_string());
        }
        let Some(future) = self.inner.take() else {
            return client_err("WriteResult already consumed".to_string());
        };
        dispatch_write(future, move |result| {
            callback
                .pin_mut()
                .complete(result.error_code, &result.error_message);
        });
        ok_result()
    }
}

fn dispatch_write(
    future: fluss::client::WriteResultFuture,
    callback: impl FnOnce(ffi::FfiResult) + Send + 'static,
) {
    // Force worker initialization before registering with an in-flight batch.
    let _ = CALLBACK_EXECUTOR.as_ref();
    let callback = move |result| callback(to_ffi_result(result));
    if let Err((future, callback)) = future.try_on_complete(callback, dispatch_batch) {
        // Only futures already polled before registration need this path.
        // Normal C++ Append/Upsert/Delete never poll before registering.
        RUNTIME.spawn(async move {
            let result = future.await;
            deliver(
                CALLBACK_EXECUTOR.as_ref(),
                Box::new(move || callback(result)),
            );
        });
    }
}

fn dispatch_batch(batch: fluss::client::WriteCallbackBatch) {
    let executor = CALLBACK_EXECUTOR.as_ref();
    for chunk in batch.into_chunks(COMPLETION_BATCH_SIZE) {
        deliver(executor, Box::new(move || chunk.run()));
    }
}

fn to_ffi_result(result: Result<(), fluss::error::Error>) -> ffi::FfiResult {
    match result {
        Ok(()) => ok_result(),
        Err(e) => err_from_core_error(&e),
    }
}

#[cfg(test)]
fn dispatch(
    future: impl Future<Output = Result<(), fluss::error::Error>> + Send + 'static,
    callback: impl FnOnce(ffi::FfiResult) + Send + 'static,
) {
    let executor = CALLBACK_EXECUTOR.as_ref();
    RUNTIME.spawn(async move {
        let result = to_ffi_result(future.await);
        deliver(executor, Box::new(move || callback(result)));
    });
}

fn deliver(executor: Option<&CallbackExecutor>, completion: Completion) {
    let completion = match executor {
        Some(executor) => match executor.enqueue(completion) {
            Ok(()) => return,
            Err(completion) => completion,
        },
        None => completion,
    };
    // Preserve completion even if dedicated workers could not be started or
    // their channel disconnected. Never execute user code on the I/O worker.
    RUNTIME.spawn_blocking(completion);
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    use super::{CallbackExecutor, deliver, dispatch, dispatch_write};
    use crate::{CLIENT_ERROR_CODE, RUNTIME};

    #[test]
    fn test_direct_batch_completion_runs_off_runtime_and_releases_capture() {
        let (tx, rx) = mpsc::channel();
        let capture = Arc::new(());
        let weak = Arc::downgrade(&capture);
        RUNTIME.block_on(async {
            dispatch_write(
                fluss::client::WriteResultFuture::join(Vec::new()),
                move |r| {
                    // Empty/previously completed batches must still use the executor.
                    let answer = RUNTIME.block_on(async { 42 });
                    tx.send((r.error_code, answer, capture)).unwrap();
                },
            );
        });
        let (code, answer, capture) = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!((code, answer), (0, 42));
        drop(capture);
        assert!(weak.upgrade().is_none());
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn test_callback_waits_asynchronously_for_acknowledgment() {
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        let (callback_tx, callback_rx) = mpsc::channel();
        dispatch(
            async move {
                ack_rx.await.unwrap();
                Ok(())
            },
            move |result| callback_tx.send(result).unwrap(),
        );
        assert!(matches!(
            callback_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        ack_tx.send(()).unwrap();
        let result = callback_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(result.error_code, 0);
        assert!(result.error_message.is_empty());
        assert!(matches!(
            callback_rx.recv_timeout(Duration::from_secs(10)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        ));
    }

    #[test]
    fn test_callback_preserves_server_error() {
        let (tx, rx) = mpsc::channel();
        dispatch(
            async {
                Err(fluss::error::Error::FlussAPIError {
                    api_error: fluss::rpc::ApiError {
                        code: 57,
                        message: "Deletion is disabled".to_string(),
                    },
                })
            },
            move |result| tx.send(result).unwrap(),
        );
        let result = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(result.error_code, 57);
        assert_eq!(result.error_message, "Deletion is disabled");
    }

    #[test]
    fn test_callback_preserves_client_error() {
        let (tx, rx) = mpsc::channel();
        dispatch(
            async {
                Err(fluss::error::Error::UnexpectedError {
                    message: "Writer closed".to_string(),
                    source: None,
                })
            },
            move |result| tx.send(result).unwrap(),
        );
        let result = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_eq!(result.error_code, CLIENT_ERROR_CODE);
        assert!(result.error_message.contains("Writer closed"));
    }

    #[test]
    fn test_callback_can_reenter_synchronous_runtime_calls() {
        let (tx, rx) = mpsc::channel();
        dispatch(async { Ok(()) }, move |_| {
            // block_on would panic if the callback ran on an async worker.
            let result = RUNTIME.block_on(async { RUNTIME.spawn(async { 42 }).await.unwrap() });
            tx.send(result).unwrap();
        });
        assert_eq!(rx.recv_timeout(Duration::from_secs(10)).unwrap(), 42);
    }

    #[test]
    fn test_executor_bounds_workers_and_does_not_block_the_runtime() {
        let executor = CallbackExecutor::new(2).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let mut releases = Vec::new();
        let mut worker_ids = HashSet::new();
        for _ in 0..2 {
            let (release_tx, release_rx) = mpsc::channel();
            releases.push(release_tx);
            let started_tx = started_tx.clone();
            assert!(
                executor
                    .enqueue(Box::new(move || {
                        started_tx.send(thread::current().id()).unwrap();
                        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    }))
                    .is_ok()
            );
            // Ensure this worker is occupied before giving another worker work.
            worker_ids.insert(started_rx.recv_timeout(Duration::from_secs(10)).unwrap());
        }
        assert_eq!(worker_ids.len(), 2);
        for _ in 0..256 {
            let done_tx = done_tx.clone();
            assert!(
                executor
                    .enqueue(Box::new(move || {
                        done_tx.send(thread::current().id()).unwrap();
                    }))
                    .is_ok()
            );
        }
        assert!(matches!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(
            RUNTIME.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), RUNTIME.spawn(async { 42 }))
                    .await
                    .unwrap()
                    .unwrap()
            }),
            42
        );
        for release in releases {
            release.send(()).unwrap();
        }
        drop(executor);
        for _ in 0..256 {
            assert!(worker_ids.contains(&done_rx.recv_timeout(Duration::from_secs(10)).unwrap()));
        }
    }

    #[test]
    fn test_executor_drains_and_survives_a_panicking_callback() {
        let executor = CallbackExecutor::new(1).unwrap();
        let completed = Arc::new(AtomicUsize::new(0));
        assert!(
            executor
                .enqueue(Box::new(|| panic!("test callback panic")))
                .is_ok()
        );
        for _ in 0..1000 {
            let completed = Arc::clone(&completed);
            assert!(
                executor
                    .enqueue(Box::new(move || {
                        completed.fetch_add(1, Ordering::Relaxed);
                    }))
                    .is_ok()
            );
        }
        drop(executor);
        assert_eq!(completed.load(Ordering::Relaxed), 1000);
        assert_eq!(Arc::strong_count(&completed), 1);
    }

    #[test]
    fn test_concurrent_producers_complete_each_job_once() {
        let executor = Arc::new(CallbackExecutor::new(4).unwrap());
        let (tx, rx) = mpsc::channel();
        let mut producers = Vec::new();
        for producer in 0..4 {
            let executor = Arc::clone(&executor);
            let tx = tx.clone();
            producers.push(thread::spawn(move || {
                for index in 0..1000 {
                    let tx = tx.clone();
                    assert!(
                        executor
                            .enqueue(Box::new(move || {
                                tx.send(producer * 1000 + index).unwrap();
                            }))
                            .is_ok()
                    );
                }
            }));
        }
        for producer in producers {
            producer.join().unwrap();
        }
        drop(executor);
        drop(tx);
        let mut completed: Vec<_> = rx.into_iter().collect();
        completed.sort_unstable();
        assert_eq!(completed, (0..4000).collect::<Vec<_>>());
    }

    #[test]
    fn test_unavailable_executor_falls_back_off_the_caller_thread() {
        let (tx, rx) = mpsc::channel();
        deliver(
            None,
            Box::new(move || {
                let answer = RUNTIME.block_on(async { 42 });
                tx.send((thread::current().id(), answer)).unwrap();
            }),
        );
        let (thread_id, answer) = rx.recv_timeout(Duration::from_secs(10)).unwrap();
        assert_ne!(thread_id, thread::current().id());
        assert_eq!(answer, 42);
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::TryRecvError::Disconnected)
        ));
    }
}
