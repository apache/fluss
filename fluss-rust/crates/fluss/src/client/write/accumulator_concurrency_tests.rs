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

use super::*;
use crate::row::{Datum, GenericRow};
use crate::test_utils::{build_cluster, build_table_info};
use std::sync::{Barrier, mpsc};
use std::thread;

struct ObservedAssigner {
    own: std::sync::Weak<Self>,
    max_owners: AtomicUsize,
}

impl BucketAssigner for ObservedAssigner {
    fn abort_if_batch_full(&self) -> bool {
        false
    }
    fn on_new_batch(&self, _: &Cluster, _: BucketId) {}
    fn assign_bucket(&self, _: Option<&bytes::Bytes>, _: &Cluster) -> Result<BucketId> {
        self.max_owners
            .fetch_max(self.own.strong_count(), Ordering::Relaxed);
        Ok(0)
    }
}

#[test]
fn cached_selection_borrows_assigner_and_releases_guard() {
    let (accumulator, cluster, path) = fixture(Config::default());
    let info = Arc::new(build_table_info(path.get_table_path().clone(), 1, 4));
    let row = GenericRow {
        values: vec![Datum::Int32(42)],
    };
    let record = WriteRecord::for_append(info.clone(), path.clone(), 1, &row);
    let queue = accumulator.create_bucket_queue(&record, 0, &cluster, &info);
    let assigner = Arc::new_cyclic(|own| ObservedAssigner {
        own: own.clone(),
        max_owners: AtomicUsize::new(0),
    });
    let observed = Arc::downgrade(&assigner);
    assert!(
        accumulator
            .write_batches
            .get(&path)
            .unwrap()
            .bucket_assigner
            .set(assigner)
            .is_ok()
    );
    for _ in 0..10 {
        let (bucket, abort, selected_queue) = accumulator
            .select_bucket_queue(&record, &cluster, &info, None)
            .unwrap();
        assert_eq!((bucket, abort), (0, false));
        assert!(Arc::ptr_eq(&queue, &selected_queue));
        assert!(accumulator.write_batches.try_get_mut(&path).is_present());
    }
    assert_eq!(
        observed
            .upgrade()
            .unwrap()
            .max_owners
            .load(Ordering::Relaxed),
        1
    );
}

#[test]
fn concurrent_first_routed_appends_share_assigner_and_queues() {
    let (accumulator, cluster, path) = fixture(Config {
        writer_batch_size: 1024 * 1024,
        ..Default::default()
    });
    let info = Arc::new(build_table_info(path.get_table_path().clone(), 1, 4));
    let barrier = Barrier::new(16);
    thread::scope(|scope| {
        let mut threads = Vec::new();
        for _ in 0..16 {
            let (accumulator, cluster, path, info, barrier) =
                (&accumulator, &cluster, &path, &info, &barrier);
            threads.push(scope.spawn(move || {
                let row = GenericRow {
                    values: vec![Datum::Int32(42)],
                };
                let record = WriteRecord::for_append(info.clone(), path.clone(), 1, &row);
                barrier.wait();
                for _ in 0..100 {
                    accumulator.append_routed(&record, cluster, info).unwrap();
                }
                accumulator
                    .write_batches
                    .get(path)
                    .unwrap()
                    .bucket_assigner
                    .get()
                    .unwrap()
                    .clone()
            }));
        }
        let assigners: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
        for assigner in &assigners[1..] {
            assert!(Arc::ptr_eq(&assigners[0], assigner));
        }
    });
    assert_eq!(accumulator.write_batches.len(), 1);
    let records: i32 = accumulator
        .write_batches
        .get(&path)
        .unwrap()
        .batches
        .values()
        .map(|queue| {
            queue
                .lock()
                .iter()
                .map(WriteBatch::record_count)
                .sum::<i32>()
        })
        .sum();
    assert_eq!(records, 1600);
    abort(&accumulator);
}

fn fixture(config: Config) -> (Arc<RecordAccumulator>, Arc<Cluster>, Arc<PhysicalTablePath>) {
    let path = TablePath::new("db", "append_concurrency");
    (
        Arc::new(RecordAccumulator::new(
            config,
            Arc::new(IdempotenceManager::new(false, 5)),
        )),
        Arc::new(build_cluster(&path, 1, 4)),
        Arc::new(PhysicalTablePath::of(Arc::new(path))),
    )
}

fn append(
    accumulator: &RecordAccumulator,
    cluster: &Cluster,
    path: &Arc<PhysicalTablePath>,
    bucket: BucketId,
) -> Result<RecordAppendResult> {
    let row = GenericRow {
        values: vec![Datum::Int32(42)],
    };
    let record = WriteRecord::for_append(
        Arc::new(build_table_info(path.get_table_path().clone(), 1, 4)),
        path.clone(),
        1,
        &row,
    );
    accumulator.append(&record, bucket, cluster, false)
}

fn abort(accumulator: &RecordAccumulator) {
    accumulator.abort_batches(broadcast::Error::WriteFailed {
        code: FlussError::NetworkException.code(),
        message: "test cleanup".into(),
    });
    assert!(!accumulator.has_incomplete());
    assert_eq!(
        accumulator.buffer_available_bytes(),
        accumulator.buffer_total_bytes()
    );
}

#[test]
fn existing_bucket_append_does_not_require_exclusive_map_access() {
    let (accumulator, cluster, path) = fixture(Config::default());
    append(&accumulator, &cluster, &path, 0).unwrap();
    let read_guard = accumulator.write_batches.get(&path).unwrap();
    let queue = read_guard.batches[&0].clone();
    let (tx, rx) = mpsc::channel();
    let worker = {
        let (accumulator, path) = (accumulator.clone(), path.clone());
        thread::spawn(move || {
            tx.send(append(&accumulator, &cluster, &path, 0).is_ok())
                .unwrap();
        })
    };
    let completed_with_reader = rx.recv_timeout(Duration::from_secs(5));
    // Release before asserting/joining so the old implementation fails rather
    // than deadlocking the test process.
    drop(read_guard);
    worker.join().unwrap();
    assert!(completed_with_reader.unwrap());
    assert!(Arc::ptr_eq(
        &queue,
        &accumulator.write_batches.get(&path).unwrap().batches[&0]
    ));
    assert_eq!(
        queue
            .lock()
            .iter()
            .map(WriteBatch::record_count)
            .sum::<i32>(),
        2
    );
    abort(&accumulator);
}

fn concurrent_initialization(precreate_table: bool) {
    let (accumulator, cluster, path) = fixture(Config {
        writer_batch_size: 64 * 1024,
        ..Default::default()
    });
    if precreate_table {
        append(&accumulator, &cluster, &path, 0).unwrap();
    }
    let barrier = Barrier::new(16);
    thread::scope(|scope| {
        for producer in 0..16 {
            let (accumulator, cluster, path, barrier) = (&accumulator, &cluster, &path, &barrier);
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    append(accumulator, cluster, path, producer % 4).unwrap();
                }
            });
        }
    });
    assert_eq!(accumulator.write_batches.len(), 1);
    {
        let entry = accumulator.write_batches.get(&path).unwrap();
        assert_eq!(entry.batches.len(), 4);
        for bucket in 0..4 {
            let dq = entry.batches[&bucket].lock();
            let count: i32 = dq.iter().map(WriteBatch::record_count).sum();
            assert_eq!(count, 400 + i32::from(precreate_table && bucket == 0));
        }
    }
    // Every accepted row must be in the one owned queue per bucket, and abort
    // must find all batches/permits (including initialization race losers).
    abort(&accumulator);
}

#[test]
fn concurrent_first_appends_share_one_table_and_bucket_queue() {
    concurrent_initialization(false);
}

#[test]
fn concurrent_missing_buckets_in_existing_table_are_not_lost() {
    concurrent_initialization(true);
}

#[test]
fn fast_path_reads_current_dynamic_size_and_reuses_estimator() {
    for dynamic in [false, true] {
        let (accumulator, cluster, path) = fixture(Config {
            writer_batch_size: 4096,
            writer_dynamic_batch_size_enabled: dynamic,
            writer_dynamic_batch_size_min: 512,
            ..Default::default()
        });
        append(&accumulator, &cluster, &path, 0).unwrap();
        let (queue, estimator, target) = {
            let entry = accumulator.write_batches.get(&path).unwrap();
            let target = entry
                .dynamic_batch_size
                .as_ref()
                .map_or(4096, |est| est.update(0));
            (
                entry.batches[&0].clone(),
                entry.compression_ratio_estimator.clone(),
                target,
            )
        };
        queue.lock().back_mut().unwrap().close().unwrap();
        let before = accumulator.buffer_available_bytes();
        assert!(
            append(&accumulator, &cluster, &path, 0)
                .unwrap()
                .new_batch_created
        );
        assert_eq!(before - accumulator.buffer_available_bytes(), target);
        {
            let entry = accumulator.write_batches.get(&path).unwrap();
            assert!(Arc::ptr_eq(&queue, &entry.batches[&0]));
            assert!(Arc::ptr_eq(&estimator, &entry.compression_ratio_estimator));
        }
        abort(&accumulator);
    }
}

#[test]
fn physical_partition_paths_do_not_share_bucket_queues() {
    let (accumulator, cluster, path) = fixture(Config::default());
    let paths: Vec<_> = ["p1", "p2"]
        .into_iter()
        .map(|partition| {
            Arc::new(PhysicalTablePath::of_partitioned(
                Arc::new(path.get_table_path().clone()),
                Some(partition.into()),
            ))
        })
        .collect();
    // This checks full physical-path key identity, not partition discovery.
    for path in &paths {
        for _ in 0..2 {
            append(&accumulator, &cluster, path, 0).unwrap();
        }
    }
    let queues: Vec<_> = paths
        .iter()
        .map(|path| accumulator.write_batches.get(path).unwrap().batches[&0].clone())
        .collect();
    assert!(!Arc::ptr_eq(&queues[0], &queues[1]));
    for queue in queues {
        assert_eq!(
            queue
                .lock()
                .iter()
                .map(WriteBatch::record_count)
                .sum::<i32>(),
            2
        );
    }
    abort(&accumulator);
}

#[test]
fn fast_path_releases_map_and_deque_before_waiting_for_memory() {
    releases_map_and_deque_before_waiting_for_memory(false);
}

#[test]
fn routed_path_releases_map_and_deque_before_waiting_for_memory() {
    releases_map_and_deque_before_waiting_for_memory(true);
}

fn releases_map_and_deque_before_waiting_for_memory(routed: bool) {
    let (accumulator, cluster, path) = fixture(Config {
        writer_batch_size: 4096,
        writer_buffer_memory_size: 4096,
        writer_dynamic_batch_size_enabled: false,
        writer_buffer_wait_timeout_ms: 10_000,
        ..Default::default()
    });
    append(&accumulator, &cluster, &path, 0).unwrap();
    if routed {
        let assigner = Arc::new_cyclic(|own| ObservedAssigner {
            own: own.clone(),
            max_owners: AtomicUsize::new(0),
        });
        assert!(
            accumulator
                .write_batches
                .get(&path)
                .unwrap()
                .bucket_assigner
                .set(assigner)
                .is_ok()
        );
    }
    let queue = accumulator.write_batches.get(&path).unwrap().batches[&0].clone();
    queue.lock().back_mut().unwrap().close().unwrap();
    let worker = {
        let (accumulator, path) = (accumulator.clone(), path.clone());
        thread::spawn(move || {
            if routed {
                let info = Arc::new(build_table_info(path.get_table_path().clone(), 1, 4));
                let row = GenericRow {
                    values: vec![Datum::Int32(42)],
                };
                let record = WriteRecord::for_append(info.clone(), path, 1, &row);
                accumulator.append_routed(&record, &cluster, &info).is_ok()
            } else {
                append(&accumulator, &cluster, &path, 0).is_ok()
            }
        })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while accumulator.buffer_waiting_threads() == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    let waiting = accumulator.buffer_waiting_threads() == 1;
    let can_write_map = accumulator.write_batches.try_get_mut(&path).is_present();
    let can_lock_deque = queue.try_lock().is_some();
    // Nonblocking cancellation also lets a broken implementation exit before
    // the assertions, without hanging inside abort_batches' exclusive lock.
    accumulator.memory_limiter.close();
    assert!(!worker.join().unwrap());
    assert!(waiting, "producer should have exhausted the buffer");
    assert!(can_write_map, "map guard leaked into the memory wait");
    assert!(can_lock_deque, "deque guard leaked into the memory wait");
    abort(&accumulator);
}
