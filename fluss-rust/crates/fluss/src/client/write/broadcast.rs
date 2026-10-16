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

use parking_lot::{Mutex, RwLock};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Notify;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type BatchWriteResult = Result<(), Error>;

type Callback<T> = Box<dyn FnOnce(&Result<T>) + Send + 'static>;
type Dispatcher<T> = fn(CompletionBatch<T>);

/// An owned set of callbacks sharing one published result.
///
/// Binding executors enqueue this batch and run it off the I/O thread.
/// User callbacks are never invoked by the broadcast itself.
#[doc(hidden)]
pub struct CompletionBatch<T> {
    result: Arc<Result<T>>,
    callbacks: Vec<Callback<T>>,
}

impl<T> CompletionBatch<T> {
    /// Execute every callback, isolating panics so later callbacks still run.
    pub fn run(self) {
        for callback in self.callbacks {
            if catch_unwind(AssertUnwindSafe(|| callback(&self.result))).is_err() {
                log::error!("Write completion callback panicked");
            }
        }
    }
}

struct CallbackGroup<T> {
    dispatch: Dispatcher<T>,
    callbacks: Vec<Callback<T>>,
}

impl<T> std::fmt::Debug for CallbackGroup<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CallbackGroup")
            .field("len", &self.callbacks.len())
            .finish()
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
    #[error("BroadcastOnce dropped")]
    Dropped,
    #[error("Write failed: {message} (code {code})")]
    WriteFailed { code: i32, message: String },
    #[error("Write failed before request was sent: {message}")]
    Client { message: String },
}

#[derive(Debug, Clone)]
pub struct BroadcastOnceReceiver<T> {
    shared: Arc<Shared<T>>,
}

impl<T: Clone + Send + Sync> BroadcastOnceReceiver<T> {
    /// Returns `Some(_)` if data has been produced
    pub fn peek(&self) -> Option<Result<T>> {
        self.shared.data.read().as_deref().cloned()
    }

    /// Register under the result read lock. Late registrations join the same
    /// dispatch queue, so they cannot overtake callbacks awaiting dispatch.
    pub(crate) fn subscribe(&self, callback: Callback<T>, dispatch: Dispatcher<T>) {
        let data = self.shared.data.read();
        let result = data.as_ref().map(Arc::clone);
        {
            let mut state = self.shared.callbacks.lock();
            if let Some(group) = state
                .groups
                .last_mut()
                .filter(|group| std::ptr::fn_addr_eq(group.dispatch, dispatch))
            {
                group.callbacks.push(callback);
            } else {
                state.groups.push(CallbackGroup {
                    dispatch,
                    callbacks: vec![callback],
                });
            }
        }
        drop(data);
        if let Some(result) = result {
            self.shared.dispatch_callbacks(result, false);
        }
    }

    /// Waits for [`BroadcastOnce::broadcast`] to be called or returns an error
    /// if the [`BroadcastOnce`] is dropped without a value being published
    pub async fn receive(&self) -> Result<T> {
        let notified = self.shared.notify.notified();

        if let Some(v) = self.peek() {
            return v;
        }

        notified.await;

        self.peek().expect("just got notified")
    }

    /// Force-complete with an error if not already completed.
    /// Used by `abort_batches` to fail in-flight handles that can't be
    /// reached through `WriteBatch::complete`.
    pub(crate) fn fail(&self, error: Error) {
        let result = Arc::new(Err(error));
        {
            let mut data = self.shared.data.write();
            if data.is_some() {
                return;
            }
            *data = Some(Arc::clone(&result));
        }
        self.shared.notify_completion(result);
    }
}

#[derive(Debug)]
struct Shared<T> {
    data: RwLock<Option<Arc<Result<T>>>>,
    notify: Notify,
    callbacks: Mutex<CallbackState<T>>,
}

#[derive(Debug)]
struct CallbackState<T> {
    groups: Vec<CallbackGroup<T>>,
    dispatching: bool,
    publication_notified: bool,
}

impl<T> Default for CallbackState<T> {
    fn default() -> Self {
        Self {
            groups: Vec::new(),
            dispatching: false,
            publication_notified: false,
        }
    }
}

impl<T> Shared<T> {
    fn notify_completion(&self, result: Arc<Result<T>>) {
        self.notify.notify_waiters();
        self.dispatch_callbacks(result, true);
    }

    fn dispatch_callbacks(&self, result: Arc<Result<T>>, publishing: bool) {
        let mut state = self.callbacks.lock();
        state.publication_notified |= publishing;
        // The publishing thread must own the first drain. A late subscriber
        // must not steal it while publication is between storing the result
        // and notifying: the publisher could otherwise return and complete
        // the next batch before this batch has actually been dispatched.
        if !state.publication_notified || state.dispatching {
            return;
        }
        state.dispatching = true;
        loop {
            let groups = std::mem::take(&mut state.groups);
            if groups.is_empty() {
                state.dispatching = false;
                return;
            }
            drop(state);
            // Exactly one drainer invokes dispatchers, outside every lock.
            // Registrations during dispatch (including reentrant ones) queue
            // behind this batch rather than dispatching ahead of it.
            for group in groups {
                (group.dispatch)(CompletionBatch {
                    result: Arc::clone(&result),
                    callbacks: group.callbacks,
                });
            }
            state = self.callbacks.lock();
        }
    }
}

#[derive(Debug)]
pub struct BroadcastOnce<T>
where
    T: Send + Sync,
{
    shared: Arc<Shared<T>>,
}

impl<T> Default for BroadcastOnce<T>
where
    T: Send + Sync,
{
    fn default() -> Self {
        Self {
            shared: Arc::new(Shared {
                data: Default::default(),
                notify: Default::default(),
                callbacks: Default::default(),
            }),
        }
    }
}

impl<T: Clone + Send + Sync> BroadcastOnce<T> {
    /// Returns a [`BroadcastOnceReceiver`] that can be used to wait on
    /// a call to [`BroadcastOnce::broadcast`] on this instance
    pub fn receiver(&self) -> BroadcastOnceReceiver<T> {
        BroadcastOnceReceiver {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Broadcast a value to all [`BroadcastOnceReceiver`] handles
    pub fn broadcast(&self, r: T) {
        let result = Arc::new(Ok(r));
        {
            let mut locked = self.shared.data.write();
            assert!(locked.is_none(), "double publish");
            *locked = Some(Arc::clone(&result));
        }
        // Woken receivers immediately read the result. Publish it and release
        // the write lock before waking them, rather than make them contend
        // with the notification loop for the same lock.
        self.shared.notify_completion(result);
    }
}

impl<T> Drop for BroadcastOnce<T>
where
    T: Send + Sync,
{
    fn drop(&mut self) {
        let result = {
            let mut data = self.shared.data.write();
            if data.is_some() {
                return;
            }
            let result = Arc::new(Err(Error::Dropped));
            *data = Some(Arc::clone(&result));
            result
        };
        log::warn!("BroadcastOnce dropped without producing");
        self.shared.notify_completion(result);
    }
}

#[cfg(test)]
mod tests {
    use super::{BroadcastOnce, CompletionBatch, Error, Shared};
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Duration;

    #[test]
    fn test_callbacks_share_one_dispatch_and_late_registration_is_delivered() {
        use std::sync::atomic::AtomicUsize;
        static DISPATCHES: AtomicUsize = AtomicUsize::new(0);
        fn dispatch(batch: CompletionBatch<u32>) {
            DISPATCHES.fetch_add(1, Ordering::SeqCst);
            batch.run();
        }
        let broadcast = BroadcastOnce::default();
        let receiver = broadcast.receiver();
        let (tx, rx) = std::sync::mpsc::channel();
        for index in 0..1000 {
            let tx = tx.clone();
            receiver.subscribe(
                Box::new(move |result| tx.send((index, result.clone())).unwrap()),
                dispatch,
            );
        }
        assert_eq!(DISPATCHES.load(Ordering::SeqCst), 0);
        broadcast.broadcast(42);
        assert_eq!(DISPATCHES.load(Ordering::SeqCst), 1);
        let late_tx = tx.clone();
        receiver.subscribe(
            Box::new(move |result| late_tx.send((1000, result.clone())).unwrap()),
            dispatch,
        );
        assert_eq!(DISPATCHES.load(Ordering::SeqCst), 2);
        drop(tx);
        let mut results: Vec<_> = rx.into_iter().collect();
        results.sort_by_key(|(index, _)| *index);
        assert_eq!(results, (0..1001).map(|i| (i, Ok(42))).collect::<Vec<_>>());
    }

    #[test]
    fn test_panics_do_not_drop_remaining_callbacks() {
        fn dispatch(batch: CompletionBatch<u32>) {
            batch.run();
        }
        let broadcast = BroadcastOnce::default();
        let (tx, rx) = std::sync::mpsc::channel();
        broadcast
            .receiver()
            .subscribe(Box::new(|_| panic!("isolated callback")), dispatch);
        for i in 0..130 {
            let tx = tx.clone();
            broadcast
                .receiver()
                .subscribe(Box::new(move |_| tx.send(i).unwrap()), dispatch);
        }
        broadcast.broadcast(42);
        drop(tx);
        assert_eq!(
            rx.into_iter().collect::<Vec<_>>(),
            (0..130).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_callback_registration_races_all_terminal_paths() {
        for mode in 0..3 {
            for _ in 0..16 {
                let broadcast = BroadcastOnce::<u32>::default();
                let receiver = broadcast.receiver();
                let (tx, rx) = std::sync::mpsc::channel();
                let barrier = Arc::new(std::sync::Barrier::new(5));
                let mut threads = Vec::new();
                for producer in 0..4 {
                    let receiver = receiver.clone();
                    let tx = tx.clone();
                    let barrier = Arc::clone(&barrier);
                    threads.push(std::thread::spawn(move || {
                        barrier.wait();
                        for i in 0..32 {
                            let tx = tx.clone();
                            receiver.subscribe(
                                Box::new(move |r| tx.send((producer * 32 + i, r.clone())).unwrap()),
                                CompletionBatch::run,
                            );
                        }
                    }));
                }
                barrier.wait();
                let expected = match mode {
                    0 => {
                        broadcast.broadcast(42);
                        Ok(42)
                    }
                    1 => {
                        receiver.fail(Error::Client {
                            message: "abort".into(),
                        });
                        Err(Error::Client {
                            message: "abort".into(),
                        })
                    }
                    _ => {
                        drop(broadcast);
                        Err(Error::Dropped)
                    }
                };
                for thread in threads {
                    thread.join().unwrap();
                }
                drop(tx);
                let mut results: Vec<_> = rx.into_iter().collect();
                results.sort_by_key(|(index, _)| *index);
                assert_eq!(
                    results,
                    (0..128).map(|i| (i, expected.clone())).collect::<Vec<_>>()
                );
                assert_eq!(receiver.peek(), Some(expected));
            }
        }
    }

    #[test]
    fn test_dispatch_runs_outside_locks_and_failure_cannot_complete_twice() {
        let broadcast = BroadcastOnce::<u32>::default();
        let receiver = broadcast.receiver();
        let nested = receiver.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        receiver.subscribe(
            Box::new(move |result| {
                assert_eq!(nested.peek(), Some(result.clone()));
                // A synchronous test dispatcher deliberately reenters registration.
                nested.subscribe(
                    Box::new(move |r| tx.send(r.clone()).unwrap()),
                    CompletionBatch::run,
                );
            }),
            CompletionBatch::run,
        );
        broadcast.broadcast(42);
        receiver.fail(Error::Dropped);
        drop(broadcast);
        assert_eq!(rx.recv_timeout(Duration::from_secs(5)).unwrap(), Ok(42));
        assert!(matches!(
            rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn test_late_registration_cannot_overtake_published_callbacks() {
        let broadcast = BroadcastOnce::<u32>::default();
        let receiver = broadcast.receiver();
        let (tx, rx) = std::sync::mpsc::channel();
        let first = tx.clone();
        receiver.subscribe(
            Box::new(move |_| first.send(1).unwrap()),
            CompletionBatch::run,
        );
        // Pause publication exactly between making data visible and notifying.
        let result = Arc::new(Ok(42));
        *receiver.shared.data.write() = Some(Arc::clone(&result));
        receiver.subscribe(Box::new(move |_| tx.send(2).unwrap()), CompletionBatch::run);
        // Merely observing the published data must not take ownership of the
        // publisher's first dispatch, including its earlier registered callback.
        assert!(matches!(
            rx.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ));
        receiver.shared.notify_completion(result);
        assert_eq!(rx.into_iter().collect::<Vec<_>>(), vec![1, 2]);
    }

    #[test]
    fn test_reentrant_registration_runs_after_existing_callbacks() {
        let broadcast = BroadcastOnce::<u32>::default();
        let receiver = broadcast.receiver();
        let nested = receiver.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        let first = tx.clone();
        receiver.subscribe(
            Box::new(move |_| {
                first.send(1).unwrap();
                nested.subscribe(
                    Box::new(move |_| first.send(3).unwrap()),
                    CompletionBatch::run,
                );
            }),
            CompletionBatch::run,
        );
        receiver.subscribe(Box::new(move |_| tx.send(2).unwrap()), CompletionBatch::run);
        broadcast.broadcast(42);
        assert_eq!(rx.into_iter().collect::<Vec<_>>(), vec![1, 2, 3]);
    }

    struct InspectOnWake {
        shared: Arc<Shared<u32>>,
        woke: AtomicBool,
        result_readable: AtomicBool,
    }

    impl InspectOnWake {
        fn inspect(&self) {
            self.woke.store(true, Ordering::SeqCst);
            let readable = self
                .shared
                .data
                .try_read()
                .is_some_and(|data| data.is_some());
            self.result_readable.store(readable, Ordering::SeqCst);
        }
    }

    impl Wake for InspectOnWake {
        fn wake(self: Arc<Self>) {
            self.inspect();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.inspect();
        }
    }

    fn assert_unlocked_on_notification(
        action: impl FnOnce(BroadcastOnce<u32>),
        expected: super::Result<u32>,
    ) {
        let broadcast = BroadcastOnce::<u32>::default();
        let receiver = broadcast.receiver();
        let probe = Arc::new(InspectOnWake {
            shared: Arc::clone(&receiver.shared),
            woke: AtomicBool::new(false),
            result_readable: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(receiver.receive());
        assert!(future.as_mut().poll(&mut context).is_pending());
        action(broadcast);
        assert!(probe.woke.load(Ordering::SeqCst));
        assert!(probe.result_readable.load(Ordering::SeqCst));
        assert_eq!(future.as_mut().poll(&mut context), Poll::Ready(expected));
    }

    #[test]
    fn test_broadcast_releases_result_lock_before_waking() {
        assert_unlocked_on_notification(|broadcast| broadcast.broadcast(42), Ok(42));
    }

    #[test]
    fn test_failure_releases_result_lock_before_waking() {
        let error = Error::Client {
            message: "writer closed".to_string(),
        };
        assert_unlocked_on_notification(
            |broadcast| broadcast.receiver().fail(error.clone()),
            Err(error.clone()),
        );
    }

    #[test]
    fn test_drop_releases_result_lock_before_waking() {
        assert_unlocked_on_notification(drop, Err(Error::Dropped));
    }

    #[test]
    fn test_failure_does_not_overwrite_published_result() {
        let broadcast = BroadcastOnce::<u32>::default();
        let receiver = broadcast.receiver();
        broadcast.broadcast(42);
        receiver.fail(Error::Dropped);
        assert_eq!(receiver.peek(), Some(Ok(42)));
        drop(broadcast);
        assert_eq!(receiver.peek(), Some(Ok(42)));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_receivers_observe_results_before_and_after_notification() {
        tokio::time::timeout(Duration::from_secs(10), async {
            for round in 0..64 {
                let broadcast = BroadcastOnce::<u32>::default();
                let late = broadcast.receiver();
                let barrier = Arc::new(tokio::sync::Barrier::new(33));
                let mut readers = Vec::new();
                for _ in 0..32 {
                    let receiver = broadcast.receiver();
                    let barrier = Arc::clone(&barrier);
                    readers.push(tokio::spawn(async move {
                        barrier.wait().await;
                        receiver.receive().await
                    }));
                }
                barrier.wait().await;
                let expected = match round % 3 {
                    0 => {
                        broadcast.broadcast(round);
                        Ok(round)
                    }
                    1 => {
                        broadcast.receiver().fail(Error::Dropped);
                        Err(Error::Dropped)
                    }
                    _ => {
                        drop(broadcast);
                        Err(Error::Dropped)
                    }
                };
                for reader in readers {
                    assert_eq!(reader.await.unwrap(), expected);
                }
                assert_eq!(late.receive().await, expected);
            }
        })
        .await
        .expect("all receivers must finish without a missed notification");
    }
}
