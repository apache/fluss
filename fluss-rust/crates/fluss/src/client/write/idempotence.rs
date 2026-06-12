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

use crate::metadata::TableBucket;
use crate::record::{NO_BATCH_SEQUENCE, NO_WRITER_ID};
use crate::rpc::FlussError;
use log::debug;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};

struct InFlightBatch {
    batch_sequence: i32,
    batch_id: i64,
}

struct BucketEntry {
    writer_id: i64,
    next_sequence: i32,
    last_acked_sequence: i32,
    in_flight: Vec<InFlightBatch>,
    reset_batch_ids: HashSet<i64>,
}

impl BucketEntry {
    fn new() -> Self {
        Self {
            writer_id: NO_WRITER_ID,
            next_sequence: 0,
            last_acked_sequence: -1,
            in_flight: Vec::new(),
            reset_batch_ids: HashSet::new(),
        }
    }
}

pub struct IdempotenceManager {
    writer_id: AtomicI64,
    bucket_entries: Mutex<HashMap<TableBucket, BucketEntry>>,
    enabled: bool,
    max_in_flight_requests_per_bucket: usize,
}

impl IdempotenceManager {
    pub fn new(enabled: bool, max_in_flight_requests_per_bucket: usize) -> Self {
        Self {
            writer_id: AtomicI64::new(NO_WRITER_ID),
            bucket_entries: Mutex::new(HashMap::new()),
            enabled,
            max_in_flight_requests_per_bucket,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn writer_id(&self) -> i64 {
        self.writer_id.load(Ordering::Acquire)
    }

    pub fn has_writer_id(&self) -> bool {
        self.writer_id() != NO_WRITER_ID
    }

    pub fn is_writer_id_valid(&self) -> bool {
        self.has_writer_id()
    }

    pub fn in_flight_count(&self, bucket: &TableBucket) -> usize {
        let entries = self.bucket_entries.lock();
        entries.get(bucket).map_or(0, |e| e.in_flight.len())
    }

    pub fn can_send_more_requests(&self, bucket: &TableBucket) -> bool {
        self.in_flight_count(bucket) < self.max_in_flight_requests_per_bucket
    }

    pub fn set_writer_id(&self, id: i64) {
        self.writer_id.store(id, Ordering::Release);
    }

    pub fn reset_writer_id(&self) {
        self.writer_id.store(NO_WRITER_ID, Ordering::Release);
        self.bucket_entries.lock().clear();
    }

    pub fn next_sequence_and_increment(&self, bucket: &TableBucket) -> i32 {
        let mut entries = self.bucket_entries.lock();
        let entry = entries
            .entry(bucket.clone())
            .or_insert_with(BucketEntry::new);
        let seq = entry.next_sequence;
        entry.next_sequence += 1;
        seq
    }

    pub fn add_in_flight_batch(&self, bucket: &TableBucket, batch_sequence: i32, batch_id: i64) {
        debug_assert!(
            batch_sequence != NO_BATCH_SEQUENCE,
            "Can't track batch for bucket {bucket} when batch sequence is not set"
        );
        let mut entries = self.bucket_entries.lock();
        let entry = entries
            .entry(bucket.clone())
            .or_insert_with(BucketEntry::new);
        // Insert sorted by batch_sequence
        let pos = entry
            .in_flight
            .binary_search_by_key(&batch_sequence, |b| b.batch_sequence)
            .unwrap_or_else(|e| e);
        entry.in_flight.insert(
            pos,
            InFlightBatch {
                batch_sequence,
                batch_id,
            },
        );
    }

    pub fn handle_completed_batch(
        &self,
        bucket: &TableBucket,
        batch_id: i64,
        batch_writer_id: i64,
    ) {
        if batch_writer_id != self.writer_id() {
            debug!(
                "Ignoring completed batch for bucket {bucket} with stale writer_id {batch_writer_id} (current: {})",
                self.writer_id()
            );
            return;
        }
        let mut entries = self.bucket_entries.lock();
        if let Some(entry) = entries.get_mut(bucket) {
            // Find by batch_id to handle the case where the in-flight entry's sequence
            // was adjusted by a prior handle_failed_batch call.
            if let Some(pos) = entry.in_flight.iter().position(|b| b.batch_id == batch_id) {
                let adjusted_seq = entry.in_flight[pos].batch_sequence;
                entry.in_flight.remove(pos);
                entry.reset_batch_ids.remove(&batch_id);
                if adjusted_seq > entry.last_acked_sequence {
                    entry.last_acked_sequence = adjusted_seq;
                }
            }
        }
    }

    /// Handle a failed batch. Matches Java's `IdempotenceManager.handleFailedBatch`.
    ///
    /// For `OutOfOrderSequenceException` or `UnknownWriterIdException`, resets ALL
    /// writer state (matching Java: "we cannot make any guarantees about the previously
    /// committed message").
    ///
    /// For other errors, removes the specific in-flight entry by `batch_id` and
    /// optionally adjusts downstream sequences. `adjust_sequences` should only be true
    /// when the batch has NOT exhausted its retries.
    pub fn handle_failed_batch(
        &self,
        bucket: &TableBucket,
        batch_id: i64,
        batch_writer_id: i64,
        error: Option<FlussError>,
        adjust_sequences: bool,
    ) {
        if batch_writer_id != self.writer_id() {
            debug!(
                "Ignoring failed batch for bucket {bucket} with stale writer_id {batch_writer_id} (current: {})",
                self.writer_id()
            );
            return;
        }

        let mut entries = self.bucket_entries.lock();

        // Matches Java: OutOfOrderSequence or UnknownWriterId → reset all writer state.
        // Java's synchronized handleFailedBatch can call synchronized resetWriterId
        // because Java monitors are reentrant. We inline the reset here to stay in
        // the same lock scope.
        if let Some(e) = error {
            if e == FlussError::OutOfOrderSequenceException
                || e == FlussError::UnknownWriterIdException
            {
                debug!(
                    "Resetting writer ID due to {e:?} for bucket {bucket} \
                     (writer_id={batch_writer_id}, batch_id={batch_id})"
                );
                self.writer_id.store(NO_WRITER_ID, Ordering::Release);
                entries.clear();
                return;
            }
        }
        if let Some(entry) = entries.get_mut(bucket) {
            // Find and remove by batch_id, capturing the (possibly adjusted) sequence
            let failed_sequence = entry
                .in_flight
                .iter()
                .position(|b| b.batch_id == batch_id)
                .map(|pos| {
                    let seq = entry.in_flight[pos].batch_sequence;
                    entry.in_flight.remove(pos);
                    seq
                });
            entry.reset_batch_ids.remove(&batch_id);
            if adjust_sequences {
                if let Some(failed_seq) = failed_sequence {
                    // Decrement sequences of in-flight batches that have higher sequences
                    for b in &mut entry.in_flight {
                        if b.batch_sequence > failed_seq {
                            b.batch_sequence -= 1;
                            debug_assert!(
                                b.batch_sequence >= 0,
                                "Batch sequence for batch_id={} went negative: {}",
                                b.batch_id,
                                b.batch_sequence
                            );
                            entry.reset_batch_ids.insert(b.batch_id);
                        }
                    }
                    // Roll back next_sequence
                    if entry.next_sequence > failed_seq {
                        entry.next_sequence -= 1;
                        debug_assert!(
                            entry.next_sequence >= 0,
                            "Next sequence went negative: {}",
                            entry.next_sequence
                        );
                    }
                }
            }
        }
    }

    #[cfg(test)]
    pub fn remove_in_flight_batch(&self, bucket: &TableBucket, batch_id: i64) {
        let mut entries = self.bucket_entries.lock();
        if let Some(entry) = entries.get_mut(bucket) {
            entry.in_flight.retain(|b| b.batch_id != batch_id);
        }
    }

    /// If the bucket's stored writer_id doesn't match the current writer_id
    /// and there are no in-flight batches, reset the bucket entry to start
    /// sequences from 0. Matches Java's `IdempotenceManager.maybeUpdateWriterId`.
    pub fn maybe_update_writer_id(&self, bucket: &TableBucket) {
        let current_writer_id = self.writer_id();
        let mut entries = self.bucket_entries.lock();
        let entry = entries
            .entry(bucket.clone())
            .or_insert_with(BucketEntry::new);
        if entry.writer_id != current_writer_id && entry.in_flight.is_empty() {
            entry.writer_id = current_writer_id;
            entry.next_sequence = 0;
            entry.last_acked_sequence = -1;
            debug!(
                "Writer id of bucket {bucket} set to {current_writer_id}. Reinitialize batch sequence at beginning."
            );
        }
    }

    /// Returns true if the given batch (identified by `batch_id`) is the first
    /// in-flight batch for its bucket. Uses batch_id rather than batch_sequence
    /// because sequence adjustment (`handle_failed_batch` with `adjust_sequences`)
    /// modifies InFlightBatch sequences without updating the actual WriteBatch,
    /// so batch_sequence on the WriteBatch may be stale.
    pub fn is_first_in_flight_batch(&self, bucket: &TableBucket, batch_id: i64) -> bool {
        let entries = self.bucket_entries.lock();
        entries
            .get(bucket)
            .and_then(|e| e.in_flight.first())
            .is_some_and(|b| b.batch_id == batch_id)
    }

    /// Returns the current (possibly adjusted) in-flight sequence for a batch.
    /// Used by `re_enqueue` to sync the WriteBatch's sequence with the adjusted
    /// InFlightBatch sequence.
    ///
    /// Does NOT clear `reset_batch_ids` — the reset marker must survive
    /// re-enqueue so that `can_retry_for_error` can still see it on subsequent
    /// retries. It is cleared only on terminal events: `handle_completed_batch`
    /// or `handle_failed_batch`. This matches Java where `reopened` persists
    /// across retries and is only cleared in `close()` (resource cleanup).
    pub fn get_adjusted_sequence(&self, bucket: &TableBucket, batch_id: i64) -> Option<i32> {
        let entries = self.bucket_entries.lock();
        let entry = entries.get(bucket)?;
        entry
            .in_flight
            .iter()
            .find(|b| b.batch_id == batch_id)
            .map(|b| b.batch_sequence)
    }

    pub fn is_next_sequence(&self, bucket: &TableBucket, batch_sequence: i32) -> bool {
        let entries = self.bucket_entries.lock();
        if let Some(entry) = entries.get(bucket) {
            entry.last_acked_sequence + 1 == batch_sequence
        } else {
            // No entry means sequence 0 is expected (last_acked = -1, so -1 + 1 = 0)
            batch_sequence == 0
        }
    }

    /// Returns true if the batch has already been committed on the server.
    ///
    /// If the batch's sequence is less than or equal to `last_acked_sequence`, it means
    /// a higher-sequence batch has already been acknowledged. This implies the current batch
    /// was also successfully written on the server (otherwise the higher-sequence batch could
    /// not have been committed).
    pub fn is_already_committed(&self, bucket: &TableBucket, batch_sequence: i32) -> bool {
        let entries = self.bucket_entries.lock();
        entries
            .get(bucket)
            .is_some_and(|e| e.last_acked_sequence >= 0 && batch_sequence <= e.last_acked_sequence)
    }

    pub fn can_retry_for_error(
        &self,
        bucket: &TableBucket,
        batch_sequence: i32,
        batch_id: i64,
        error: FlussError,
    ) -> bool {
        if !self.has_writer_id() {
            return false;
        }
        let entries = self.bucket_entries.lock();
        let entry = entries.get(bucket);
        let is_reset = entry.is_some_and(|e| e.reset_batch_ids.contains(&batch_id));

        if error == FlussError::OutOfOrderSequenceException {
            // Inline is_next_sequence logic to avoid double-locking
            let is_next = entry.map_or(batch_sequence == 0, |e| {
                e.last_acked_sequence + 1 == batch_sequence
            });
            return is_reset || !is_next;
        }
        if error == FlussError::UnknownWriterIdException {
            return is_reset;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_bucket(bucket_id: i32) -> TableBucket {
        TableBucket::new(1, bucket_id)
    }

    /// Setup: 3 in-flight batches (seq 0,1,2 / batch_id 100,101,102) for bucket 0.
    fn setup_three_in_flight() -> (IdempotenceManager, TableBucket) {
        let mgr = IdempotenceManager::new(true, 5);
        mgr.set_writer_id(42);
        let b0 = test_bucket(0);
        let _ = mgr.next_sequence_and_increment(&b0); // 0
        let _ = mgr.next_sequence_and_increment(&b0); // 1
        let _ = mgr.next_sequence_and_increment(&b0); // 2
        mgr.add_in_flight_batch(&b0, 0, 100);
        mgr.add_in_flight_batch(&b0, 1, 101);
        mgr.add_in_flight_batch(&b0, 2, 102);
        (mgr, b0)
    }

    #[test]
    fn test_handle_completed_batch() {
        let (mgr, b0) = setup_three_in_flight();

        // Basic: complete middle batch, verify removal and last_acked update
        mgr.handle_completed_batch(&b0, 101, 42);
        {
            let entries = mgr.bucket_entries.lock();
            let entry = entries.get(&b0).unwrap();
            assert_eq!(entry.last_acked_sequence, 1);
            assert_eq!(entry.in_flight.len(), 2);
            assert_eq!(entry.in_flight[0].batch_sequence, 0);
            assert_eq!(entry.in_flight[1].batch_sequence, 2);
        }

        // Adjusted: fail batch_id=100 (seq=0) with adjustment, then complete
        // batch_id=102 whose seq was adjusted from 2→1. last_acked should use
        // the adjusted sequence.
        let (mgr, b0) = setup_three_in_flight();
        mgr.handle_failed_batch(&b0, 101, 42, None, true);
        mgr.handle_completed_batch(&b0, 102, 42);
        {
            let entries = mgr.bucket_entries.lock();
            let entry = entries.get(&b0).unwrap();
            assert_eq!(entry.last_acked_sequence, 1); // adjusted, not original 2
            assert_eq!(entry.in_flight.len(), 1);
            assert_eq!(entry.in_flight[0].batch_id, 100);
        }
    }

    #[test]
    fn test_handle_failed_batch() {
        // With sequence adjustment
        let (mgr, b0) = setup_three_in_flight();
        mgr.handle_failed_batch(&b0, 101, 42, None, true);
        {
            let entries = mgr.bucket_entries.lock();
            let entry = entries.get(&b0).unwrap();
            assert_eq!(entry.in_flight.len(), 2);
            assert_eq!(entry.in_flight[0].batch_sequence, 0);
            assert_eq!(entry.in_flight[1].batch_sequence, 1); // was 2, decremented
            assert_eq!(entry.next_sequence, 2); // was 3, decremented
        }

        // Without sequence adjustment (retries exhausted)
        let (mgr, b0) = setup_three_in_flight();
        mgr.handle_failed_batch(&b0, 101, 42, None, false);
        {
            let entries = mgr.bucket_entries.lock();
            let entry = entries.get(&b0).unwrap();
            assert_eq!(entry.in_flight.len(), 2);
            assert_eq!(entry.in_flight[0].batch_sequence, 0);
            assert_eq!(entry.in_flight[1].batch_sequence, 2); // NOT decremented
            assert_eq!(entry.next_sequence, 3); // NOT decremented
        }

        // OOS / UnknownWriterId errors reset all writer state
        for error in [
            FlussError::OutOfOrderSequenceException,
            FlussError::UnknownWriterIdException,
        ] {
            let (mgr, b0) = setup_three_in_flight();
            mgr.handle_failed_batch(&b0, 100, 42, Some(error), true);
            assert!(!mgr.has_writer_id());
            assert!(mgr.bucket_entries.lock().is_empty());
        }
    }

    #[test]
    fn test_can_retry_out_of_order() {
        let mgr = IdempotenceManager::new(true, 5);
        let b0 = test_bucket(0);

        // No writer_id → never retriable
        assert!(!mgr.can_retry_for_error(&b0, 0, 100, FlussError::OutOfOrderSequenceException));

        mgr.set_writer_id(42);
        mgr.add_in_flight_batch(&b0, 0, 100);
        mgr.add_in_flight_batch(&b0, 1, 101);

        // seq=0 IS next expected (last_acked=-1+1=0) → genuine violation, NOT retriable
        assert!(!mgr.can_retry_for_error(&b0, 0, 100, FlussError::OutOfOrderSequenceException));
        // seq=1 is NOT next expected → retriable
        assert!(mgr.can_retry_for_error(&b0, 1, 101, FlussError::OutOfOrderSequenceException));
    }

    #[test]
    fn test_can_retry_after_sequence_reset() {
        // OOS: batch whose seq was adjusted to match last_acked+1 is still retriable
        let (mgr, b0) = setup_three_in_flight();
        mgr.handle_completed_batch(&b0, 100, 42); // last_acked=0
        mgr.handle_failed_batch(&b0, 101, 42, None, true); // batch_id=102 adjusted to seq=1

        // seq=1 == last_acked(0)+1, but batch was reset → retriable
        assert!(mgr.can_retry_for_error(&b0, 1, 102, FlussError::OutOfOrderSequenceException));

        // UnknownWriterId: non-reset → NOT retriable, reset → retriable
        let (mgr, b0) = setup_three_in_flight();
        assert!(!mgr.can_retry_for_error(&b0, 0, 100, FlussError::UnknownWriterIdException));
        mgr.handle_failed_batch(&b0, 101, 42, None, true); // batch_id=102 is reset
        assert!(mgr.can_retry_for_error(&b0, 1, 102, FlussError::UnknownWriterIdException));
    }

    #[test]
    fn test_maybe_update_writer_id() {
        let mgr = IdempotenceManager::new(true, 5);
        mgr.set_writer_id(42);
        let b0 = test_bucket(0);

        mgr.maybe_update_writer_id(&b0);
        let seq = mgr.next_sequence_and_increment(&b0);
        mgr.add_in_flight_batch(&b0, seq, 100);

        // With in-flight batches: rotation is deferred
        mgr.set_writer_id(99);
        mgr.maybe_update_writer_id(&b0);
        {
            let entries = mgr.bucket_entries.lock();
            let entry = entries.get(&b0).unwrap();
            assert_eq!(entry.writer_id, 42); // unchanged
            assert_eq!(entry.next_sequence, 1);
        }

        // Complete must use the writer_id that was active when batch was sent
        mgr.handle_completed_batch(&b0, 100, 99);
        mgr.maybe_update_writer_id(&b0);
        {
            let entries = mgr.bucket_entries.lock();
            let entry = entries.get(&b0).unwrap();
            assert_eq!(entry.writer_id, 99);
            assert_eq!(entry.next_sequence, 0);
            assert_eq!(entry.last_acked_sequence, -1);
        }
    }

    #[test]
    fn test_is_first_in_flight_batch() {
        let (mgr, b0) = setup_three_in_flight();

        assert!(mgr.is_first_in_flight_batch(&b0, 100));
        assert!(!mgr.is_first_in_flight_batch(&b0, 101));

        // After adjustment + completion, batch_id still identifies first correctly
        mgr.handle_failed_batch(&b0, 101, 42, None, true);
        mgr.handle_completed_batch(&b0, 100, 42);
        assert!(mgr.is_first_in_flight_batch(&b0, 102));
        assert!(!mgr.is_first_in_flight_batch(&b0, 100));
    }

    #[test]
    fn test_can_send_more_requests() {
        let mgr = IdempotenceManager::new(true, 2);
        let b0 = test_bucket(0);

        assert!(mgr.can_send_more_requests(&b0));

        mgr.add_in_flight_batch(&b0, 0, 100);
        assert!(mgr.can_send_more_requests(&b0));

        mgr.add_in_flight_batch(&b0, 1, 101);
        assert!(!mgr.can_send_more_requests(&b0)); // at limit

        mgr.remove_in_flight_batch(&b0, 100);
        assert!(mgr.can_send_more_requests(&b0)); // under limit again
    }

    #[test]
    fn test_is_already_committed() {
        let mgr = IdempotenceManager::new(true, 5);
        let b0 = test_bucket(0);
        mgr.set_writer_id(42);

        // No entry yet → not committed
        assert!(!mgr.is_already_committed(&b0, 0));

        // Initialize bucket and ack seq=0
        let _ = mgr.next_sequence_and_increment(&b0); // 0
        mgr.add_in_flight_batch(&b0, 0, 100);
        mgr.handle_completed_batch(&b0, 100, 42); // last_acked=0

        // seq=0 <= last_acked(0) → committed
        assert!(mgr.is_already_committed(&b0, 0));
        // seq=1 > last_acked(0) → not committed
        assert!(!mgr.is_already_committed(&b0, 1));

        // Ack up to seq=4, then check seq=0 still committed
        for seq in 1..=4 {
            let _ = mgr.next_sequence_and_increment(&b0);
            mgr.add_in_flight_batch(&b0, seq, 100 + seq as i64);
            mgr.handle_completed_batch(&b0, 100 + seq as i64, 42);
        }
        assert!(mgr.is_already_committed(&b0, 0)); // seq=0 <= last_acked(4)
        assert!(mgr.is_already_committed(&b0, 4)); // seq=4 <= last_acked(4)
        assert!(!mgr.is_already_committed(&b0, 5)); // seq=5 > last_acked(4)
    }

    #[test]
    fn test_reset_batch_ids_cleaned_on_complete() {
        let (mgr, b0) = setup_three_in_flight();

        // Fail batch_id=100 → batch_id=101 and 102 marked as reset
        mgr.handle_failed_batch(&b0, 100, 42, None, true);
        {
            let entries = mgr.bucket_entries.lock();
            let entry = entries.get(&b0).unwrap();
            assert!(entry.reset_batch_ids.contains(&101));
            assert!(entry.reset_batch_ids.contains(&102));
        }

        // Complete batch_id=101 → cleaned from reset set
        mgr.handle_completed_batch(&b0, 101, 42);
        {
            let entries = mgr.bucket_entries.lock();
            let entry = entries.get(&b0).unwrap();
            assert!(!entry.reset_batch_ids.contains(&101));
            assert!(entry.reset_batch_ids.contains(&102)); // still there
        }
    }

    #[test]
    fn test_get_adjusted_sequence() {
        let (mgr, b0) = setup_three_in_flight();

        // No entry for unknown bucket
        assert_eq!(mgr.get_adjusted_sequence(&test_bucket(9), 100), None);

        // Before adjustment: returns original sequences
        assert_eq!(mgr.get_adjusted_sequence(&b0, 101), Some(1));
        assert_eq!(mgr.get_adjusted_sequence(&b0, 999), None);

        // After adjustment: returns adjusted sequences
        mgr.handle_failed_batch(&b0, 100, 42, None, true);
        assert_eq!(mgr.get_adjusted_sequence(&b0, 100), None); // removed
        assert_eq!(mgr.get_adjusted_sequence(&b0, 101), Some(0)); // was 1
        assert_eq!(mgr.get_adjusted_sequence(&b0, 102), Some(1)); // was 2

        // Reset flag survives get_adjusted_sequence (unlike the old take_ variant).
        // This matches Java where `reopened` persists across retries.
        {
            let entries = mgr.bucket_entries.lock();
            let entry = entries.get(&b0).unwrap();
            assert!(entry.reset_batch_ids.contains(&101));
            assert!(entry.reset_batch_ids.contains(&102));
        }
    }

    // --- Scenario tests ---
    // Simulate Sender-level orchestration on IdempotenceManager.
    // Each test mirrors a Java SenderTest integration test, exercising the same
    // state transitions that Sender.handle_write_batch_error / complete_batch make.
    //
    // Convention: retriable failures make NO IdempotenceManager call (batch stays
    // in-flight, Sender re-enqueues via accumulator). Non-retriable failures call
    // handle_failed_batch. Successes call handle_completed_batch.

    #[test]
    fn scenario_multiple_inflight_retried_in_order() {
        // Java: testIdempotenceWithMultipleInflightBatchesRetriedInOrder
        // 3 batches in-flight, batch 0 times out, batches 1+2 get OOS.
        // All are retriable and must be retried one-at-a-time in sequence order.
        let (mgr, b0) = setup_three_in_flight();

        // Batch 0 (seq=0) times out → retriable, stays in in-flight
        // Batch 1 (seq=1) OOS → retriable (not next expected seq)
        assert!(mgr.can_retry_for_error(&b0, 1, 101, FlussError::OutOfOrderSequenceException));
        // Batch 2 (seq=2) OOS → retriable
        assert!(mgr.can_retry_for_error(&b0, 2, 102, FlussError::OutOfOrderSequenceException));

        // Retry phase: only first-in-flight batch should be drained
        assert!(mgr.is_first_in_flight_batch(&b0, 100));
        assert!(!mgr.is_first_in_flight_batch(&b0, 101));

        // Retry batch 0 succeeds → last_acked=0
        mgr.handle_completed_batch(&b0, 100, 42);
        assert_eq!(last_acked(&mgr, &b0), 0);

        // Batch 1 is now first, retry succeeds → last_acked=1
        assert!(mgr.is_first_in_flight_batch(&b0, 101));
        mgr.handle_completed_batch(&b0, 101, 42);
        assert_eq!(last_acked(&mgr, &b0), 1);

        // Batch 2 is now first, retry succeeds → last_acked=2
        assert!(mgr.is_first_in_flight_batch(&b0, 102));
        mgr.handle_completed_batch(&b0, 102, 42);
        assert_eq!(last_acked(&mgr, &b0), 2);
    }

    #[test]
    fn scenario_out_of_order_responses() {
        // Java: testCorrectHandlingOfOutOfOrderResponses
        // Server responds to batch 1 (OOS) before batch 0 (timeout).
        // Both re-enqueued, retried in order.
        let mgr = IdempotenceManager::new(true, 5);
        mgr.set_writer_id(42);
        let b0 = test_bucket(0);
        let _ = mgr.next_sequence_and_increment(&b0);
        let _ = mgr.next_sequence_and_increment(&b0);
        mgr.add_in_flight_batch(&b0, 0, 100);
        mgr.add_in_flight_batch(&b0, 1, 101);

        // Batch 1 response arrives first: OOS → retriable (seq 1 ≠ next expected 0)
        assert!(mgr.can_retry_for_error(&b0, 1, 101, FlussError::OutOfOrderSequenceException));
        // Batch 0 response: timeout → retriable (no IdempotenceManager call)

        // Retry: batch 0 must go first
        assert!(mgr.is_first_in_flight_batch(&b0, 100));
        mgr.handle_completed_batch(&b0, 100, 42);
        assert_eq!(last_acked(&mgr, &b0), 0);

        // Then batch 1
        assert!(mgr.is_first_in_flight_batch(&b0, 101));
        mgr.handle_completed_batch(&b0, 101, 42);
        assert_eq!(last_acked(&mgr, &b0), 1);
    }

    #[test]
    fn scenario_second_batch_succeeds_first() {
        // Java: testCorrectHandlingOfOutOfOrderResponsesWhenSecondSucceeds
        //       + testCorrectHandlingOfDuplicateSequenceError (same at this level)
        // Batch 1 succeeds before batch 0. last_acked jumps ahead, then batch 0
        // completes without regressing last_acked.
        let mgr = IdempotenceManager::new(true, 5);
        mgr.set_writer_id(42);
        let b0 = test_bucket(0);
        let _ = mgr.next_sequence_and_increment(&b0);
        let _ = mgr.next_sequence_and_increment(&b0);
        mgr.add_in_flight_batch(&b0, 0, 100);
        mgr.add_in_flight_batch(&b0, 1, 101);

        // Batch 1 succeeds first → last_acked jumps to 1
        mgr.handle_completed_batch(&b0, 101, 42);
        assert_eq!(last_acked(&mgr, &b0), 1);

        // Batch 0 timeout → retriable → re-enqueued → retry succeeds
        mgr.handle_completed_batch(&b0, 100, 42);
        // last_acked stays 1 (0 < 1, higher wins)
        assert_eq!(last_acked(&mgr, &b0), 1);
        assert!(
            mgr.bucket_entries
                .lock()
                .get(&b0)
                .unwrap()
                .in_flight
                .is_empty()
        );
    }

    #[test]
    fn scenario_unknown_writer_id_resets_and_restarts() {
        // Java: testRetryAfterResettingInFlightBatchSequence
        // Batch 0 times out (retriable), batch 1 gets UnknownWriterId (non-retriable).
        // UnknownWriterId resets all state. After new writer ID, sequences restart at 0.
        let mgr = IdempotenceManager::new(true, 5);
        mgr.set_writer_id(42);
        let b0 = test_bucket(0);
        let _ = mgr.next_sequence_and_increment(&b0);
        let _ = mgr.next_sequence_and_increment(&b0);
        mgr.add_in_flight_batch(&b0, 0, 100);
        mgr.add_in_flight_batch(&b0, 1, 101);

        // Batch 0 times out → retriable (stays in in-flight)
        // Batch 1 UnknownWriterId → NOT retriable (non-reset batch)
        assert!(!mgr.can_retry_for_error(&b0, 1, 101, FlussError::UnknownWriterIdException));

        // Sender calls fail_batch → handle_failed_batch with error → full reset
        mgr.handle_failed_batch(
            &b0,
            101,
            42,
            Some(FlussError::UnknownWriterIdException),
            true,
        );
        assert!(!mgr.has_writer_id());
        assert!(mgr.bucket_entries.lock().is_empty());

        // New writer ID allocated, sequences restart at 0
        mgr.set_writer_id(99);
        assert_eq!(mgr.next_sequence_and_increment(&b0), 0);
    }

    fn last_acked(mgr: &IdempotenceManager, bucket: &TableBucket) -> i32 {
        mgr.bucket_entries
            .lock()
            .get(bucket)
            .unwrap()
            .last_acked_sequence
    }
}
