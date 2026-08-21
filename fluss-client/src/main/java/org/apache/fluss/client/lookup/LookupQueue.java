/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.client.lookup;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Accumulates lookup operations into independently drainable batches per table bucket. */
@ThreadSafe
@Internal
class LookupQueue {

    private final ReentrantLock stateLock = new ReentrantLock();

    // Appenders wait here when their current batch is full or absent and there is not enough queue
    // capacity to create another batch.
    private final Condition appendCondition = stateLock.newCondition();

    // The sender waits here when no batch or retry is currently drainable.
    private final Condition drainCondition = stateLock.newCondition();
    private final Map<AccumulatorKey, Deque<LookupBatch>> batches;
    private final Deque<AbstractLookupQuery<?>> reEnqueuedLookups;
    private final Map<TableBucket, Integer> inFlightRequestsByBucket;
    private final int queueSize;
    private final int maxBatchSize;
    private final int maxInFlightRequestsPerBucket;
    private final long batchTimeoutNanos;

    private boolean closed;
    private int remainingQueueSize;
    private int waitingAppenders;

    LookupQueue(Configuration conf) {
        this.queueSize = conf.get(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE);
        this.maxBatchSize = conf.get(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE);
        this.maxInFlightRequestsPerBucket =
                conf.get(ConfigOptions.CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET);
        checkArgument(queueSize > 0, "Lookup queue size must be greater than 0.");
        checkArgument(maxBatchSize > 0, "Lookup batch size must be greater than 0.");
        checkArgument(
                queueSize >= maxBatchSize,
                "Lookup queue size (%s) must not be smaller than batch size (%s).",
                queueSize,
                maxBatchSize);
        checkArgument(
                maxInFlightRequestsPerBucket > 0,
                "Maximum in-flight lookup requests per bucket must be greater than 0.");

        this.batches = new LinkedHashMap<>();
        this.reEnqueuedLookups = new ArrayDeque<>();
        this.inFlightRequestsByBucket = new HashMap<>();
        this.remainingQueueSize = queueSize;
        this.batchTimeoutNanos = conf.get(ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT).toNanos();
    }

    void appendLookup(AbstractLookupQuery<?> lookup) {
        AccumulatorKey key = AccumulatorKey.of(lookup);
        stateLock.lock();
        try {
            while (true) {
                if (closed) {
                    throw new IllegalStateException(
                            "Can not append lookup operation since the LookupQueue is closed.");
                }

                LookupBatch appendedBatch = tryAppend(key, lookup);
                if (appendedBatch != null) {
                    if (appendedBatch.size() == maxBatchSize) {
                        drainCondition.signal();
                    }
                    return;
                }

                if (remainingQueueSize >= maxBatchSize) {
                    Deque<LookupBatch> bucketBatches =
                            batches.computeIfAbsent(key, ignored -> new ArrayDeque<>());
                    bucketBatches.addLast(new LookupBatch(lookup, System.nanoTime()));
                    remainingQueueSize -= maxBatchSize;
                    // Wake the sender to start the batch timeout.
                    drainCondition.signal();
                    return;
                }

                waitingAppenders++;
                try {
                    // A waiting appender makes underfilled batches ready, so wake the sender before
                    // waiting for one completed batch to return its reserved capacity.
                    if (waitingAppenders == 1) {
                        drainCondition.signal();
                    }
                    appendCondition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    lookup.future().completeExceptionally(e);
                    return;
                } finally {
                    waitingAppenders--;
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    /** Re-enqueues a retry without blocking an RPC callback thread on queue capacity. */
    void reEnqueue(AbstractLookupQuery<?> lookup) {
        stateLock.lock();
        try {
            if (closed) {
                throw new IllegalStateException(
                        "Can not re-enqueue lookup operation since the LookupQueue is closed.");
            }
            reEnqueuedLookups.addLast(lookup);
            drainCondition.signal();
        } finally {
            stateLock.unlock();
        }
    }

    boolean hasUnDrained() {
        stateLock.lock();
        try {
            return hasUnDrainedUnsafe();
        } finally {
            stateLock.unlock();
        }
    }

    /** Waits until at least one bucket batch is ready, then drains all currently ready batches. */
    List<LookupBatch> drain() throws InterruptedException {
        return drain(false);
    }

    /** Drains all batches that can currently be sent while respecting the per-bucket limit. */
    List<LookupBatch> drainAll() throws InterruptedException {
        return drain(true);
    }

    void addInFlightRequests(Set<TableBucket> tableBuckets) {
        stateLock.lock();
        try {
            for (TableBucket tableBucket : tableBuckets) {
                inFlightRequestsByBucket.merge(tableBucket, 1, Integer::sum);
            }
        } finally {
            stateLock.unlock();
        }
    }

    void completeInFlightRequests(Set<TableBucket> tableBuckets) {
        stateLock.lock();
        try {
            boolean bucketDroppedBelowInFlightLimit = false;
            for (TableBucket tableBucket : tableBuckets) {
                int inFlight = inFlightRequestsByBucket.getOrDefault(tableBucket, 0);
                int remaining = inFlight - 1;
                checkState(
                        remaining >= 0,
                        "No in-flight lookup request exists for table bucket %s.",
                        tableBucket);
                if (inFlight >= maxInFlightRequestsPerBucket
                        && remaining < maxInFlightRequestsPerBucket) {
                    bucketDroppedBelowInFlightLimit = true;
                }
                if (remaining == 0) {
                    inFlightRequestsByBucket.remove(tableBucket);
                } else {
                    inFlightRequestsByBucket.put(tableBucket, remaining);
                }
            }
            if (bucketDroppedBelowInFlightLimit) {
                drainCondition.signal();
            }
        } finally {
            stateLock.unlock();
        }
    }

    void releaseBatchCapacity(LookupBatch batch) {
        stateLock.lock();
        try {
            if (!batch.markCompleted()) {
                return;
            }
            remainingQueueSize += maxBatchSize;
            checkState(
                    remainingQueueSize <= queueSize,
                    "Lookup queue capacity was released more than once.");

            // Retries must reserve returned capacity before regular appenders are woken up.
            // Otherwise, under sustained full-queue pressure, new lookups can repeatedly take every
            // released slot and starve the retry deque.
            if (!reEnqueuedLookups.isEmpty()) {
                int reEnqueuedBefore = reEnqueuedLookups.size();
                boolean retryWaitingForCapacity = appendReadyRetries(System.currentTimeMillis());
                if (reEnqueuedLookups.size() < reEnqueuedBefore || retryWaitingForCapacity) {
                    drainCondition.signal();
                }
            }

            if (waitingAppenders > 0 && remainingQueueSize >= maxBatchSize) {
                appendCondition.signalAll();
            }
        } finally {
            stateLock.unlock();
        }
    }

    void close() {
        stateLock.lock();
        try {
            closed = true;
            appendCondition.signalAll();
            drainCondition.signalAll();
        } finally {
            stateLock.unlock();
        }
    }

    private List<LookupBatch> drain(boolean drainAll) throws InterruptedException {
        stateLock.lock();
        try {
            while (true) {
                long nowNanos = System.nanoTime();
                boolean retryWaitingForCapacity = appendReadyRetries(System.currentTimeMillis());
                List<LookupBatch> readyBatches =
                        drainReadyBatches(nowNanos, drainAll, retryWaitingForCapacity);
                if (!readyBatches.isEmpty()) {
                    return readyBatches;
                }
                if (!hasUnDrainedUnsafe() && (drainAll || closed)) {
                    return readyBatches;
                }

                long waitNanos = nextReadyWaitNanos(nowNanos, System.currentTimeMillis());
                if (waitNanos == Long.MAX_VALUE) {
                    drainCondition.await();
                } else if (waitNanos > 0) {
                    drainCondition.awaitNanos(waitNanos);
                }
            }
        } finally {
            stateLock.unlock();
        }
    }

    private LookupBatch tryAppend(AccumulatorKey key, AbstractLookupQuery<?> lookup) {
        Deque<LookupBatch> bucketBatches = batches.get(key);
        if (bucketBatches == null) {
            return null;
        }
        LookupBatch lastBatch = bucketBatches.peekLast();
        if (lastBatch == null || lastBatch.size() >= maxBatchSize) {
            return null;
        }
        lastBatch.addLookup(lookup);
        return lastBatch;
    }

    private boolean appendReadyRetries(long nowMs) {
        boolean waitingForCapacity = false;
        Iterator<AbstractLookupQuery<?>> iterator = reEnqueuedLookups.iterator();
        while (iterator.hasNext()) {
            AbstractLookupQuery<?> lookup = iterator.next();
            if (lookup.nextRetryTimeMs() > nowMs) {
                continue;
            }

            AccumulatorKey key = AccumulatorKey.of(lookup);
            if (tryAppend(key, lookup) != null) {
                iterator.remove();
            } else if (remainingQueueSize >= maxBatchSize) {
                Deque<LookupBatch> bucketBatches =
                        batches.computeIfAbsent(key, ignored -> new ArrayDeque<>());
                bucketBatches.addLast(new LookupBatch(lookup, System.nanoTime()));
                remainingQueueSize -= maxBatchSize;
                iterator.remove();
            } else {
                waitingForCapacity = true;
            }
        }
        return waitingForCapacity;
    }

    /**
     * Drains ready batches fairly across accumulator keys.
     *
     * <p>The queue stores batches by {@link AccumulatorKey}, where different lookup kinds for the
     * same {@link TableBucket} use different keys. If we keep taking from one key until that key is
     * empty or the bucket permit is exhausted, a key with continuous backlog can stay at the head
     * of the {@link LinkedHashMap} and starve later keys for the same bucket. To avoid that, each
     * pass selects at most one batch from each key. A selected key that still has more batches is
     * moved to the tail, so the next pass starts from the other keys first.
     */
    private List<LookupBatch> drainReadyBatches(
            long nowNanos, boolean drainAll, boolean retryWaitingForCapacity) {
        List<LookupBatch> readyBatches = new ArrayList<>();
        Map<TableBucket, Integer> selectedByBucket = new HashMap<>();
        boolean drainedAnyBatch;
        do {
            drainedAnyBatch = false;
            List<AccumulatorKey> keys = new ArrayList<>(batches.keySet());
            for (AccumulatorKey key : keys) {
                Deque<LookupBatch> bucketBatches = batches.get(key);
                if (bucketBatches == null || bucketBatches.isEmpty()) {
                    batches.remove(key);
                    continue;
                }

                LookupBatch batch = bucketBatches.peekFirst();
                TableBucket tableBucket = batch.tableBucket();
                int inFlight = inFlightRequestsByBucket.getOrDefault(tableBucket, 0);
                int selected = selectedByBucket.getOrDefault(tableBucket, 0);
                if (inFlight + selected >= maxInFlightRequestsPerBucket) {
                    continue;
                }
                if (!drainAll
                        && !closed
                        && batch.size() < maxBatchSize
                        && batch.waitedNanos(nowNanos) < batchTimeoutNanos
                        && waitingAppenders == 0
                        && !retryWaitingForCapacity) {
                    continue;
                }

                readyBatches.add(bucketBatches.removeFirst());
                selectedByBucket.put(tableBucket, selected + 1);
                drainedAnyBatch = true;
                if (bucketBatches.isEmpty()) {
                    batches.remove(key);
                } else {
                    // This key has used its turn in this pass. Move it behind the other keys so a
                    // later drain can pick another lookup kind for the same bucket before this
                    // key's remaining backlog.
                    batches.remove(key);
                    batches.put(key, bucketBatches);
                }
            }
        } while (drainedAnyBatch);
        return readyBatches;
    }

    private long nextReadyWaitNanos(long nowNanos, long nowMs) {
        long waitNanos = Long.MAX_VALUE;
        for (Deque<LookupBatch> bucketBatches : batches.values()) {
            LookupBatch batch = bucketBatches.peekFirst();
            if (batch == null
                    || inFlightRequestsByBucket.getOrDefault(batch.tableBucket(), 0)
                            >= maxInFlightRequestsPerBucket) {
                continue;
            }
            waitNanos =
                    Math.min(
                            waitNanos,
                            Math.max(0L, batchTimeoutNanos - batch.waitedNanos(nowNanos)));
        }
        for (AbstractLookupQuery<?> lookup : reEnqueuedLookups) {
            long retryDelayMs = lookup.nextRetryTimeMs() - nowMs;
            if (retryDelayMs > 0) {
                waitNanos = Math.min(waitNanos, TimeUnit.MILLISECONDS.toNanos(retryDelayMs));
            }
        }
        return waitNanos;
    }

    private boolean hasUnDrainedUnsafe() {
        return !batches.isEmpty() || !reEnqueuedLookups.isEmpty();
    }

    @VisibleForTesting
    int remainingQueueSize() {
        stateLock.lock();
        try {
            return remainingQueueSize;
        } finally {
            stateLock.unlock();
        }
    }

    int maxBatchSize() {
        return maxBatchSize;
    }

    @VisibleForTesting
    int queuedBatchCount() {
        stateLock.lock();
        try {
            int count = 0;
            for (Deque<LookupBatch> bucketBatches : batches.values()) {
                count += bucketBatches.size();
            }
            return count;
        } finally {
            stateLock.unlock();
        }
    }

    @VisibleForTesting
    int reEnqueuedLookupCount() {
        stateLock.lock();
        try {
            return reEnqueuedLookups.size();
        } finally {
            stateLock.unlock();
        }
    }

    @VisibleForTesting
    int waitingAppenderCount() {
        stateLock.lock();
        try {
            return waitingAppenders;
        } finally {
            stateLock.unlock();
        }
    }

    private static final class AccumulatorKey {
        private final TableBucket tableBucket;
        private final LookupType lookupType;
        private final boolean historical;

        private AccumulatorKey(TableBucket tableBucket, LookupType lookupType, boolean historical) {
            this.tableBucket = tableBucket;
            this.lookupType = lookupType;
            this.historical = historical;
        }

        private static AccumulatorKey of(AbstractLookupQuery<?> lookup) {
            return new AccumulatorKey(
                    lookup.tableBucket(),
                    lookup.lookupType(),
                    lookup.originalPartitionName() != null);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AccumulatorKey that = (AccumulatorKey) o;
            return historical == that.historical
                    && Objects.equals(tableBucket, that.tableBucket)
                    && lookupType == that.lookupType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableBucket, lookupType, historical);
        }
    }
}
