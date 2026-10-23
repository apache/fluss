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

package org.apache.fluss.server.index;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.RecordTooLargeException;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * TabletServer-global staging area that accumulates pre-encoded {@link IndexBatch}es per target
 * index-table bucket. Producers ({@link IndexReplicator}s, running on the read worker pool) append
 * batches; consumers ({@link IndexSender} workers) claim batches front-to-back. A claimed batch
 * remains at the front until the buffer acknowledges, retries, or discards it, preserving
 * per-bucket order without a second sender-owned registry.
 *
 * <p>The buffer has no knowledge of leaders, RPC, or retry policy. It owns per-bucket ordering,
 * claims, and pending-byte accounting. It tracks total pending (un-acknowledged) retained bytes as
 * a hard admission bound and tracks per-replicator pending bytes so the read layer can apply soft
 * back-pressure without letting one stalled replicator stop unrelated replicators from deriving new
 * windows.
 *
 * <p>Lock order is {@code window} then the per-bucket {@code deque}. Code inside a {@code deque}
 * critical section may only read volatile or atomic state of a window or batch: calling a {@code
 * synchronized} method on a window from there inverts the order and deadlocks against {@link
 * #tryAppendWindow} and {@link #retryClaim}.
 */
@Internal
@ThreadSafe
public final class IndexSendBuffer {

    private final ConcurrentMap<TableBucket, Deque<IndexBatch>> batches = new ConcurrentHashMap<>();

    /** The front batch currently claimed for sending, at most one per target bucket. */
    private final ConcurrentMap<TableBucket, IndexBatch> claimedBatches = new ConcurrentHashMap<>();

    /** Upper bound on pending retained bytes for one producing replicator. */
    private final long maxPendingBytes;

    /** Hard upper bound on pending retained bytes across this TabletServer sendBuffer. */
    private final long maxTotalPendingBytes;

    /** Serializes competing capacity checks and reservations, but never releases. */
    private final Object admissionLock = new Object();

    /** Total retained bytes of all batches currently pending, including queued and in-flight. */
    private final AtomicLong pendingBytes = new AtomicLong(0L);

    /** Pending retained bytes grouped by the source main-table bucket that produced them. */
    private final ConcurrentMap<TableBucket, Long> pendingBytesBySource = new ConcurrentHashMap<>();

    /** Optional callback fired on each append to promptly wake the owning sender worker. */
    @Nullable private volatile Consumer<TableBucket> appendListener;

    /** Creates an sendBuffer with no back-pressure bound (primarily for tests). */
    public IndexSendBuffer() {
        this(Long.MAX_VALUE, Long.MAX_VALUE);
    }

    /** Creates an sendBuffer with separate per-producer and TabletServer-wide bounds. */
    public IndexSendBuffer(long maxPendingBytes, long maxTotalPendingBytes) {
        checkArgument(maxPendingBytes > 0, "maxPendingBytes must be positive");
        checkArgument(maxTotalPendingBytes > 0, "maxTotalPendingBytes must be positive");
        this.maxPendingBytes = maxPendingBytes;
        this.maxTotalPendingBytes = maxTotalPendingBytes;
    }

    /** Registers a listener invoked for every admitted batch to wake consumers. */
    public void setAppendListener(Consumer<TableBucket> appendListener) {
        this.appendListener = appendListener;
    }

    /**
     * Atomically admits and publishes every target batch of one source window.
     *
     * <p>Capacity reservation is serialized across producers, while sender-visible queue
     * publication is staged behind the window's admission flag. A rejected or failed publication
     * leaves no queue or accounting residue.
     */
    public boolean tryAppendWindow(List<IndexBatch> windowBatches) {
        checkNotNull(windowBatches, "windowBatches");
        checkArgument(!windowBatches.isEmpty(), "windowBatches must not be empty");
        IndexBatch first = checkNotNull(windowBatches.get(0), "window batch");
        IndexReplicationWindow window = first.window();

        synchronized (window) {
            validateWindowBatches(windowBatches, window);
            if (!window.isActive() || window.isOwnerClosed()) {
                return false;
            }

            long windowBytes = retainedBytes(windowBatches);
            if (windowBytes > maxTotalPendingBytes) {
                throw new RecordTooLargeException(
                        "Index replication window retains "
                                + windowBytes
                                + " bytes, exceeding max total pending bytes="
                                + maxTotalPendingBytes);
            }
            if (!reserve(window, windowBytes)) {
                return false;
            }

            try {
                if (!window.isActive() || window.isOwnerClosed()) {
                    rollbackWindow(windowBatches);
                    return false;
                }

                for (IndexBatch batch : windowBatches) {
                    publishStaged(batch);
                    if (!window.isActive() || window.isOwnerClosed()) {
                        rollbackWindow(windowBatches);
                        return false;
                    }
                }
                window.markAdmitted(payloadBytes(windowBatches));
            } catch (RuntimeException | Error failure) {
                rollbackWindow(windowBatches);
                throw failure;
            }
        }

        for (IndexBatch batch : windowBatches) {
            notifyAppend(batch);
        }
        return true;
    }

    private void validateWindowBatches(
            List<IndexBatch> windowBatches, IndexReplicationWindow window) {
        checkArgument(!window.isAdmitted(), "window is already admitted");
        checkArgument(
                windowBatches.size() == window.expectedBatchCount(),
                "window batch list does not match its expected batch count");
        Set<TableBucket> targetBuckets = new HashSet<>();
        for (IndexBatch batch : windowBatches) {
            checkNotNull(batch, "window batch");
            checkArgument(batch.window() == window, "all batches must reference the same window");
            checkArgument(
                    targetBuckets.add(batch.targetBucket()),
                    "window contains a duplicate target bucket");
            checkArgument(!batch.isReleased(), "window contains a released batch");
        }
    }

    private long retainedBytes(List<IndexBatch> windowBatches) {
        long bytes = 0L;
        for (IndexBatch batch : windowBatches) {
            try {
                bytes = Math.addExact(bytes, batch.retainedBytes());
            } catch (ArithmeticException overflow) {
                throw new RecordTooLargeException(
                        "Index replication window retained-byte total overflowed", overflow);
            }
        }
        return bytes;
    }

    private long payloadBytes(List<IndexBatch> windowBatches) {
        long bytes = 0L;
        for (IndexBatch batch : windowBatches) {
            bytes = Math.addExact(bytes, batch.encoded().getBytesLength());
        }
        return bytes;
    }

    private boolean reserve(IndexReplicationWindow window, long windowBytes) {
        synchronized (admissionLock) {
            if (!window.isActive() || window.isOwnerClosed()) {
                return false;
            }

            while (true) {
                long current = pendingBytes.get();
                if (current > maxTotalPendingBytes - windowBytes) {
                    return false;
                }
                if (pendingBytes.compareAndSet(current, current + windowBytes)) {
                    break;
                }
            }

            try {
                pendingBytesBySource.compute(
                        window.sourceBucket(),
                        (ignoredOwner, ownerBytes) ->
                                ownerBytes == null
                                        ? windowBytes
                                        : Math.addExact(ownerBytes, windowBytes));
            } catch (RuntimeException | Error failure) {
                pendingBytes.addAndGet(-windowBytes);
                throw failure;
            }

            return true;
        }
    }

    private void publishStaged(IndexBatch batch) {
        batches.compute(
                batch.targetBucket(),
                (ignored, current) -> {
                    Deque<IndexBatch> deque = current == null ? new ArrayDeque<>() : current;
                    synchronized (deque) {
                        deque.addLast(batch);
                    }
                    return deque;
                });
    }

    private void rollbackWindow(List<IndexBatch> windowBatches) {
        for (IndexBatch batch : windowBatches) {
            remove(batch);
        }
        for (IndexBatch batch : windowBatches) {
            release(batch);
        }
    }

    private void notifyAppend(IndexBatch batch) {
        Consumer<TableBucket> listener = this.appendListener;
        if (listener != null) {
            listener.accept(batch.targetBucket());
        }
    }

    @VisibleForTesting
    boolean hasRetriedBatchForTesting(TableBucket bucket) {
        Deque<IndexBatch> deque = batches.get(bucket);
        if (deque == null) {
            return false;
        }
        synchronized (deque) {
            for (IndexBatch batch : deque) {
                if (batch.attempts() > 0) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Returns {@code true} once the total pending retained bytes reach the configured bound. The
     * read layer consults this before deriving a new window so derivation cannot outrun the send
     * layer.
     */
    public boolean isFull() {
        return pendingBytes.get() >= maxTotalPendingBytes;
    }

    /**
     * Returns {@code true} when the given source main-table bucket has reached its pending-byte
     * back-pressure bound. The bound is intentionally scoped to the producer so one unhealthy
     * target index bucket cannot stop unrelated main-table buckets from reading WAL.
     */
    public boolean isFull(TableBucket source) {
        return pendingBytes(source) >= maxPendingBytes;
    }

    /** Total retained bytes currently pending, including queued and in-flight. */
    public long pendingBytes() {
        return pendingBytes.get();
    }

    /** Retained bytes currently pending for the given source main-table bucket. */
    public long pendingBytes(TableBucket source) {
        Long sourcePendingBytes = pendingBytesBySource.get(source);
        return sourcePendingBytes == null ? 0L : sourcePendingBytes;
    }

    /** Snapshot of the buckets currently tracked. May include buckets that have just drained. */
    public Set<TableBucket> buckets() {
        return new HashSet<>(batches.keySet());
    }

    /** Returns {@code true} if the bucket currently has at least one queued batch. */
    public boolean hasPending(TableBucket bucket) {
        Deque<IndexBatch> deque = batches.get(bucket);
        if (deque == null) {
            return false;
        }
        synchronized (deque) {
            return !deque.isEmpty();
        }
    }

    /**
     * Claims the front batch if it is eligible for sending at {@code nowMs}. The batch remains in
     * the queue until one of the exact-claim completion methods is called.
     */
    @Nullable
    public IndexBatch claimFirstReady(TableBucket bucket, long nowMs) {
        IndexBatch[] result = new IndexBatch[1];
        batches.computeIfPresent(
                bucket,
                (ignored, deque) -> {
                    synchronized (deque) {
                        if (claimedBatches.containsKey(bucket)) {
                            return deque;
                        }
                        IndexBatch head = deque.peekFirst();
                        if (head == null) {
                            return null;
                        }
                        if (!head.window().isAdmitted()) {
                            return deque;
                        }
                        if (head.readyAtMs() > nowMs) {
                            return deque;
                        }
                        if (claimedBatches.putIfAbsent(bucket, head) == null) {
                            result[0] = head;
                        }
                        return deque;
                    }
                });
        return result[0];
    }

    /** Returns whether this buffer still owns the exact claimed batch. */
    public boolean ownsClaim(IndexBatch batch) {
        return claimedBatches.get(batch.targetBucket()) == batch;
    }

    /** Returns whether the bucket currently has a claimed front batch. */
    public boolean hasClaim(TableBucket bucket) {
        return claimedBatches.containsKey(bucket);
    }

    /** Releases the exact claim for retry while leaving the batch at the queue front. */
    public boolean retryClaim(IndexBatch batch, long readyAtMs) {
        boolean[] retried = new boolean[1];
        boolean[] discarded = new boolean[1];
        synchronized (batch.window()) {
            batches.computeIfPresent(
                    batch.targetBucket(),
                    (ignored, deque) -> {
                        synchronized (deque) {
                            if (!claimedBatches.remove(batch.targetBucket(), batch)) {
                                return deque;
                            }
                            if (deque.peekFirst() == batch
                                    && batch.ownerActive()
                                    && !batch.isReleased()) {
                                batch.setReadyAtMs(readyAtMs);
                                batch.reEnqueued();
                                retried[0] = true;
                            } else {
                                discarded[0] = removeExact(deque, batch);
                            }
                            return deque.isEmpty() ? null : deque;
                        }
                    });
        }
        if (discarded[0]) {
            release(batch);
        }
        return retried[0];
    }

    /** Acknowledges and removes the exact claimed batch. */
    public boolean acknowledgeClaim(IndexBatch batch) {
        boolean removed = removeClaim(batch);
        if (removed) {
            release(batch);
        }
        return removed;
    }

    /** Discards and releases the exact claimed batch. */
    public boolean discardClaim(IndexBatch batch) {
        boolean removed = removeClaim(batch);
        if (removed) {
            release(batch);
        }
        return removed;
    }

    /** Discards every batch currently claimed by a sender that is closing. */
    public void discardClaims() {
        for (IndexBatch batch : new ArrayList<>(claimedBatches.values())) {
            discardClaim(batch);
        }
    }

    private boolean removeClaim(IndexBatch batch) {
        boolean[] removed = new boolean[1];
        batches.computeIfPresent(
                batch.targetBucket(),
                (ignored, deque) -> {
                    synchronized (deque) {
                        if (claimedBatches.remove(batch.targetBucket(), batch)) {
                            removed[0] = removeExact(deque, batch);
                        }
                        return deque.isEmpty() ? null : deque;
                    }
                });
        return removed[0];
    }

    /** Remove the exact batch from its target queue without changing pending-byte accounting. */
    private boolean remove(IndexBatch batch) {
        boolean[] removed = new boolean[1];
        batches.computeIfPresent(
                batch.targetBucket(),
                (ignored, deque) -> {
                    synchronized (deque) {
                        removed[0] = removeExact(deque, batch);
                        return deque.isEmpty() ? null : deque;
                    }
                });
        return removed[0];
    }

    /** Release pending-byte accounting for a batch that reached a terminal state. */
    private void release(IndexBatch batch) {
        if (batch.markReleased()) {
            releaseReservedBytes(batch.window().sourceBucket(), batch.retainedBytes());
        }
    }

    private void releaseReservedBytes(TableBucket source, long bytes) {
        pendingBytesBySource.computeIfPresent(
                source, (ignored, current) -> current == bytes ? null : current - bytes);
        pendingBytes.addAndGet(-bytes);
    }

    private static boolean removeExact(Deque<IndexBatch> deque, IndexBatch batch) {
        Iterator<IndexBatch> iterator = deque.iterator();
        while (iterator.hasNext()) {
            if (iterator.next() == batch) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    /**
     * Discard every queued batch produced by the given source main-table bucket, returning how many
     * were dropped. Called when a producer stops (its main-table bucket lost leadership or the
     * table was dropped) so its undelivered batches do not loop forever in the sender's
     * at-least-once retry, pinning memory and holding back-pressure. Cleanup is scoped to the
     * producing source bucket: batches from other sources are left untouched, including other
     * buckets of the same index table that may still have a live leader.
     */
    public int dropForSource(TableBucket source) {
        Set<IndexBatch> droppedBatches = new HashSet<>();
        for (TableBucket bucket : new ArrayList<>(batches.keySet())) {
            batches.computeIfPresent(
                    bucket,
                    (ignored, deque) -> {
                        synchronized (deque) {
                            Iterator<IndexBatch> it = deque.iterator();
                            while (it.hasNext()) {
                                IndexBatch batch = it.next();
                                IndexReplicationWindow window = batch.window();
                                if (window != null && window.sourceBucket().equals(source)) {
                                    it.remove();
                                    claimedBatches.remove(bucket, batch);
                                    droppedBatches.add(batch);
                                }
                            }
                            return deque.isEmpty() ? null : deque;
                        }
                    });
        }
        releaseDroppedBatches(droppedBatches);
        return droppedBatches.size();
    }

    private void releaseDroppedBatches(Iterable<IndexBatch> droppedBatches) {
        for (IndexBatch batch : droppedBatches) {
            release(batch);
        }
    }
}
