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
import org.apache.fluss.memory.MemorySegment;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * TabletServer-global staging area that accumulates pre-encoded {@link IndexBatch}es per target
 * index-table bucket. Producers ({@link IndexReplicator}s, running on the read worker pool) append
 * batches; consumers ({@link IndexSender} workers) claim batches front-to-back. A claimed batch
 * remains at the front until the sender completes or retries its exact claim, preserving per-bucket
 * order without a second sender-owned registry.
 *
 * <p>The buffer has no knowledge of leaders, RPC, or retry policy. It owns per-bucket ordering,
 * claims, and pending-byte accounting. It accounts for retained payload pages during construction,
 * queuing and active delivery against a shared hard budget. Retiring windows after sustained
 * failures returns shared capacity so other source buckets can compete for it.
 *
 * <p>Lock order is {@code window} then the per-bucket {@code queue}. Code inside a {@code queue}
 * critical section may only read volatile or atomic state of a window or batch: calling a {@code
 * synchronized} method on a window from there inverts the order taken by {@link #retryClaim}.
 */
@Internal
@ThreadSafe
final class IndexSendBuffer {

    private final ConcurrentMap<TableBucket, BucketQueue> queues = new ConcurrentHashMap<>();

    /** Hard upper bound on pending retained bytes across this TabletServer sendBuffer. */
    private final long maxTotalPendingBytes;

    /** Serializes admission and construction reservation ownership changes. */
    private final Object admissionLock = new Object();

    /** Retained payload pages in construction, queued and in-flight batches. */
    private final AtomicLong pendingBytes = new AtomicLong(0L);

    private final AtomicBoolean waitingForCapacity = new AtomicBoolean();
    @Nullable private volatile Runnable capacityListener;

    /** Optional callback fired when a target bucket may have work ready for sending. */
    @Nullable private volatile Consumer<TableBucket> appendListener;

    /** Creates a buffer with no backpressure bound (primarily for tests). */
    IndexSendBuffer() {
        this(Long.MAX_VALUE);
    }

    /** Creates a buffer with a TabletServer-wide retained payload budget. */
    IndexSendBuffer(long maxTotalPendingBytes) {
        checkArgument(maxTotalPendingBytes > 0, "maxTotalPendingBytes must be positive");
        this.maxTotalPendingBytes = maxTotalPendingBytes;
    }

    /** Registers a listener invoked when append or source cleanup may make a bucket ready. */
    void setReadyBucketListener(Consumer<TableBucket> appendListener) {
        this.appendListener = appendListener;
    }

    /** Wakes readers that yielded because the total memory budget was occupied. */
    void setCapacityListener(Runnable capacityListener) {
        this.capacityListener = capacityListener;
    }

    long capacityBytes() {
        return maxTotalPendingBytes;
    }

    Reservation newReservation() {
        return new Reservation();
    }

    /**
     * Atomically admits and publishes every target batch of one source window.
     *
     * <p>The source holds its lifecycle lock through publication. Until the window is admitted,
     * senders cannot claim its queued batches and the builder still owns its reservation.
     */
    void appendWindow(List<IndexBatch> windowBatches, Reservation reservation) {
        IndexReplicationWindow window = windowBatches.get(0).window();
        checkArgument(!window.isAdmitted(), "window is already admitted");
        Set<TableBucket> targets = new HashSet<>();
        long retainedBytes = 0L;
        long payloadBytes = 0L;
        for (IndexBatch batch : windowBatches) {
            checkArgument(batch.window() == window, "all batches must reference the same window");
            checkArgument(
                    targets.add(batch.targetBucket()), "window contains a duplicate target bucket");
            retainedBytes += batch.retainedBytes();
            payloadBytes += batch.encoded().getBytesLength();
        }
        try {
            for (IndexBatch batch : windowBatches) {
                publishStaged(batch);
            }
            reservation.transferToWindow(retainedBytes);
        } catch (RuntimeException | Error failure) {
            for (IndexBatch batch : windowBatches) {
                remove(batch);
            }
            throw failure;
        }
        window.markAdmitted(payloadBytes, targets);
        for (IndexBatch batch : windowBatches) {
            notifyReadyBucket(batch.targetBucket());
        }
    }

    private void publishStaged(IndexBatch batch) {
        queues.compute(
                batch.targetBucket(),
                (ignored, current) -> {
                    BucketQueue queue = current == null ? new BucketQueue() : current;
                    synchronized (queue) {
                        queue.batches.addLast(batch);
                    }
                    return queue;
                });
    }

    private void notifyReadyBucket(TableBucket bucket) {
        Consumer<TableBucket> listener = this.appendListener;
        if (listener != null) {
            listener.accept(bucket);
        }
    }

    @VisibleForTesting
    boolean hasRetriedBatchForTesting(TableBucket bucket) {
        BucketQueue queue = queues.get(bucket);
        if (queue == null) {
            return false;
        }
        synchronized (queue) {
            for (IndexBatch batch : queue.batches) {
                if (batch.requeueCount() > 0) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Returns whether retained payload has reached the node-wide budget. If full, requests a
     * capacity-release notification through the shared listener; repeated requests coalesce.
     *
     * <p>Does not block or reserve capacity. A false result does not cancel an earlier request.
     */
    boolean requestCapacityWakeupIfFull() {
        boolean full = pendingBytes.get() >= maxTotalPendingBytes;
        if (full) {
            waitingForCapacity.set(true);
        }
        return full && !hasPageCapacity(1);
    }

    boolean hasPageCapacity(int pageSize) {
        return pendingBytes.get() <= maxTotalPendingBytes - pageSize;
    }

    /** Total retained payload page bytes, including construction, queued and in-flight batches. */
    long pendingBytes() {
        return pendingBytes.get();
    }

    /** Snapshot of the buckets currently tracked. May include buckets that have just drained. */
    Set<TableBucket> buckets() {
        return new HashSet<>(queues.keySet());
    }

    /** Returns {@code true} if the bucket currently has at least one queued batch. */
    boolean hasPending(TableBucket bucket) {
        BucketQueue queue = queues.get(bucket);
        if (queue == null) {
            return false;
        }
        synchronized (queue) {
            return !queue.batches.isEmpty();
        }
    }

    /**
     * Claims the front batch when its index retry deadline permits sending. The batch remains in
     * the queue until one of the exact-claim completion methods is called.
     */
    @Nullable
    IndexBatch claimFirstReady(TableBucket bucket) {
        IndexBatch[] result = new IndexBatch[1];
        queues.computeIfPresent(
                bucket,
                (ignored, queue) -> {
                    synchronized (queue) {
                        if (queue.claimed != null) {
                            return queue;
                        }
                        IndexBatch head = queue.batches.peekFirst();
                        if (head == null) {
                            return null;
                        }
                        if (!head.window().isAdmitted()) {
                            return queue;
                        }
                        if (!head.window().isReadyForSend()) {
                            return queue;
                        }
                        queue.claimed = head;
                        result[0] = head;
                        return queue;
                    }
                });
        return result[0];
    }

    /** Returns whether this buffer still owns the exact claimed batch. */
    boolean ownsClaim(IndexBatch batch) {
        BucketQueue queue = queues.get(batch.targetBucket());
        return queue != null && queue.claimed == batch;
    }

    /** Returns whether the bucket currently has a claimed front batch. */
    boolean hasClaim(TableBucket bucket) {
        BucketQueue queue = queues.get(bucket);
        return queue != null && queue.claimed != null;
    }

    /** Releases the exact claim for retry while leaving the batch at the queue front. */
    boolean retryClaim(IndexBatch batch) {
        boolean[] retried = new boolean[1];
        boolean[] discarded = new boolean[1];
        synchronized (batch.window()) {
            if (!ownsClaim(batch)) {
                return false;
            }
            batch.window().onFailure();
            queues.computeIfPresent(
                    batch.targetBucket(),
                    (ignored, queue) -> {
                        synchronized (queue) {
                            if (queue.claimed != batch) {
                                return queue;
                            }
                            queue.claimed = null;
                            if (batch.window().acceptsBatches()) {
                                batch.reEnqueued();
                                retried[0] = true;
                            } else {
                                queue.batches.removeFirst();
                                discarded[0] = true;
                            }
                            return queue.batches.isEmpty() ? null : queue;
                        }
                    });
        }
        if (discarded[0]) {
            release(batch);
        }
        return retried[0];
    }

    /** Removes and releases an exact claim, whether acknowledged or discarded by the sender. */
    boolean completeClaim(IndexBatch batch) {
        boolean[] removed = new boolean[1];
        queues.computeIfPresent(
                batch.targetBucket(),
                (ignored, queue) -> {
                    synchronized (queue) {
                        if (queue.claimed == batch) {
                            queue.claimed = null;
                            queue.batches.removeFirst();
                            removed[0] = true;
                        }
                        return queue.batches.isEmpty() ? null : queue;
                    }
                });
        if (removed[0]) {
            release(batch);
        }
        return removed[0];
    }

    /** Discards every batch currently claimed by a sender that is closing. */
    void discardClaims() {
        List<IndexBatch> claimed = new ArrayList<>();
        for (BucketQueue queue : queues.values()) {
            IndexBatch batch = queue.claimed;
            if (batch != null) {
                claimed.add(batch);
            }
        }
        for (IndexBatch batch : claimed) {
            completeClaim(batch);
        }
    }

    /** Remove the exact batch from its target queue without changing pending-byte accounting. */
    private void remove(IndexBatch batch) {
        queues.computeIfPresent(
                batch.targetBucket(),
                (ignored, queue) -> {
                    synchronized (queue) {
                        queue.batches.removeFirstOccurrence(batch);
                        return queue.batches.isEmpty() ? null : queue;
                    }
                });
    }

    /** Release pending-byte accounting for a batch that reached a terminal state. */
    private void release(IndexBatch batch) {
        pendingBytes.addAndGet(-batch.retainedBytes());
        notifyCapacityAvailable();
    }

    private void notifyCapacityAvailable() {
        Runnable listener = capacityListener;
        if (listener != null && waitingForCapacity.getAndSet(false)) {
            listener.run();
        }
    }

    /** Charges construction pages until admission transfers them to the window's batches. */
    final class Reservation implements AutoCloseable {
        private long bytes;
        private boolean closed;

        MemorySegment allocatePage(int pageSize) {
            synchronized (admissionLock) {
                checkArgument(!closed, "reservation is closed");
                if (bytes > maxTotalPendingBytes - pageSize) {
                    throw new RecordTooLargeException(
                            "Index replication window needs at least "
                                    + (bytes + pageSize)
                                    + " retained bytes, exceeding"
                                    + " index.replication.buffer.max-bytes="
                                    + maxTotalPendingBytes);
                }
                while (true) {
                    long current = pendingBytes.get();
                    if (current > maxTotalPendingBytes - pageSize) {
                        waitingForCapacity.set(true);
                        if (!hasPageCapacity(pageSize)) {
                            throw new BufferFullException();
                        }
                        continue;
                    }
                    if (pendingBytes.compareAndSet(current, current + pageSize)) {
                        bytes += pageSize;
                        break;
                    }
                }
            }
            try {
                return MemorySegment.allocateHeapMemory(pageSize);
            } catch (RuntimeException | Error failure) {
                synchronized (admissionLock) {
                    bytes -= pageSize;
                    pendingBytes.addAndGet(-pageSize);
                }
                notifyCapacityAvailable();
                throw failure;
            }
        }

        private void transferToWindow(long retainedBytes) {
            synchronized (admissionLock) {
                checkArgument(!closed, "reservation is closed");
                checkArgument(bytes == retainedBytes, "reservation must cover the window");
                closed = true;
                bytes = 0;
            }
        }

        @Override
        public void close() {
            synchronized (admissionLock) {
                if (closed) {
                    return;
                }
                closed = true;
                pendingBytes.addAndGet(-bytes);
                bytes = 0;
            }
            notifyCapacityAvailable();
        }
    }

    /** Temporary competition for the shared construction and send budget. */
    static final class BufferFullException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static final class BucketQueue {
        private final Deque<IndexBatch> batches = new ArrayDeque<>();

        // Null or the queue head. Mutations hold the queue monitor inside a per-key map compute;
        // sender lifecycle checks read the claim without taking the queue lock.
        @Nullable private volatile IndexBatch claimed;
    }

    /**
     * Removes retired windows from their target queues before releasing bytes and waking senders.
     */
    int dropWindows(List<IndexReplicationWindow> windows) {
        List<IndexBatch> droppedBatches = new ArrayList<>();
        for (IndexReplicationWindow window : windows) {
            for (TableBucket bucket : window.targetBuckets()) {
                removeWindowBatch(window, bucket, droppedBatches);
            }
        }
        for (IndexBatch batch : droppedBatches) {
            release(batch);
        }
        // Removing a claimed head can expose another source's batch after its ready notification
        // was consumed. Notify outside the queue locks even if the queue has since changed.
        for (IndexBatch batch : droppedBatches) {
            notifyReadyBucket(batch.targetBucket());
        }
        return droppedBatches.size();
    }

    private void removeWindowBatch(
            IndexReplicationWindow window, TableBucket bucket, List<IndexBatch> droppedBatches) {
        queues.computeIfPresent(
                bucket,
                (ignored, queue) -> {
                    synchronized (queue) {
                        Iterator<IndexBatch> it = queue.batches.iterator();
                        while (it.hasNext()) {
                            IndexBatch batch = it.next();
                            if (batch.window() == window) {
                                it.remove();
                                if (queue.claimed == batch) {
                                    queue.claimed = null;
                                }
                                droppedBatches.add(batch);
                                break;
                            }
                        }
                        return queue.batches.isEmpty() ? null : queue;
                    }
                });
    }
}
