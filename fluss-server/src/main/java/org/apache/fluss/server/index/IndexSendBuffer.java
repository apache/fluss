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
import org.apache.fluss.shaded.guava32.com.google.common.collect.Iterables;

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
import java.util.function.Predicate;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

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
 * synchronized} method on a window from there inverts the order and deadlocks against {@link
 * #tryAppendWindow} and {@link #retryClaim}.
 */
@Internal
@ThreadSafe
public final class IndexSendBuffer {

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
    public IndexSendBuffer() {
        this(Long.MAX_VALUE);
    }

    /** Creates a buffer with a TabletServer-wide retained payload budget. */
    public IndexSendBuffer(long maxTotalPendingBytes) {
        checkArgument(maxTotalPendingBytes > 0, "maxTotalPendingBytes must be positive");
        this.maxTotalPendingBytes = maxTotalPendingBytes;
    }

    /** Registers a listener invoked when append or source cleanup may make a bucket ready. */
    public void setReadyBucketListener(Consumer<TableBucket> appendListener) {
        this.appendListener = appendListener;
    }

    /** Wakes readers that yielded because the total memory budget was occupied. */
    public void setCapacityListener(Runnable capacityListener) {
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
     * <p>Capacity reservation is serialized across producers, while sender-visible queue
     * publication is staged behind the window's admission flag. A rejected or failed publication
     * leaves no queue or accounting residue.
     */
    public boolean tryAppendWindow(List<IndexBatch> windowBatches) {
        return tryAppendWindow(windowBatches, null);
    }

    boolean tryAppendWindow(List<IndexBatch> windowBatches, @Nullable Reservation reservation) {
        checkNotNull(windowBatches, "windowBatches");
        checkArgument(!windowBatches.isEmpty(), "windowBatches must not be empty");
        IndexBatch first = checkNotNull(windowBatches.get(0), "window batch");
        IndexReplicationWindow window = first.window();

        synchronized (window) {
            Set<TableBucket> targets = validateWindowBatches(windowBatches, window);
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
            if (!reserve(window, windowBytes, reservation)) {
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
                window.markAdmitted(payloadBytes(windowBatches), targets);
            } catch (RuntimeException | Error failure) {
                rollbackWindow(windowBatches);
                throw failure;
            }
        }

        for (IndexBatch batch : windowBatches) {
            notifyReadyBucket(batch.targetBucket());
        }
        return true;
    }

    private Set<TableBucket> validateWindowBatches(
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
        return targetBuckets;
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

    private boolean reserve(
            IndexReplicationWindow window, long windowBytes, @Nullable Reservation reservation) {
        synchronized (admissionLock) {
            if (!window.isActive() || window.isOwnerClosed()) {
                return false;
            }

            if (reservation != null) {
                checkArgument(!reservation.closed, "reservation is closed");
                checkArgument(
                        reservation.bytes == windowBytes, "reservation must cover the window");
                reservation.closed = true;
                reservation.bytes = 0;
            } else {
                while (true) {
                    long current = pendingBytes.get();
                    if (current > maxTotalPendingBytes - windowBytes) {
                        waitingForCapacity.set(true);
                        return false;
                    }
                    if (pendingBytes.compareAndSet(current, current + windowBytes)) {
                        break;
                    }
                }
            }

            return true;
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

    private void rollbackWindow(List<IndexBatch> windowBatches) {
        for (IndexBatch batch : windowBatches) {
            remove(batch);
        }
        for (IndexBatch batch : windowBatches) {
            release(batch);
        }
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
        boolean full = pendingBytes.get() >= maxTotalPendingBytes;
        if (full) {
            waitingForCapacity.set(true);
        }
        return full;
    }

    boolean hasPageCapacity(int pageSize) {
        return pendingBytes.get() <= maxTotalPendingBytes - pageSize;
    }

    /** Total retained payload page bytes, including construction, queued and in-flight batches. */
    public long pendingBytes() {
        return pendingBytes.get();
    }

    /** Snapshot of the buckets currently tracked. May include buckets that have just drained. */
    public Set<TableBucket> buckets() {
        return new HashSet<>(queues.keySet());
    }

    /** Returns {@code true} if the bucket currently has at least one queued batch. */
    public boolean hasPending(TableBucket bucket) {
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
    public IndexBatch claimFirstReady(TableBucket bucket) {
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
    public boolean ownsClaim(IndexBatch batch) {
        BucketQueue queue = queues.get(batch.targetBucket());
        return queue != null && queue.claimed == batch;
    }

    /** Returns whether the bucket currently has a claimed front batch. */
    public boolean hasClaim(TableBucket bucket) {
        BucketQueue queue = queues.get(bucket);
        return queue != null && queue.claimed != null;
    }

    /** Releases the exact claim for retry while leaving the batch at the queue front. */
    public boolean retryClaim(IndexBatch batch) {
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
                            if (queue.batches.peekFirst() == batch
                                    && batch.ownerActive()
                                    && !batch.isReleased()) {
                                batch.reEnqueued();
                                retried[0] = true;
                            } else {
                                discarded[0] = removeExact(queue.batches, batch);
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
                            removed[0] = removeExact(queue.batches, batch);
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
    public void discardClaims() {
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
    private boolean remove(IndexBatch batch) {
        boolean[] removed = new boolean[1];
        queues.computeIfPresent(
                batch.targetBucket(),
                (ignored, queue) -> {
                    synchronized (queue) {
                        removed[0] = removeExact(queue.batches, batch);
                        return queue.batches.isEmpty() ? null : queue;
                    }
                });
        return removed[0];
    }

    /** Release pending-byte accounting for a batch that reached a terminal state. */
    private void release(IndexBatch batch) {
        if (batch.markReleased()) {
            pendingBytes.addAndGet(-batch.retainedBytes());
            notifyCapacityAvailable();
        }
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
                        throw new BufferFullException();
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

    /** Releases a stopped source's retired windows without scanning unrelated target queues. */
    int dropWindows(Set<IndexReplicationWindow> windows) {
        return dropBatches(
                Iterables.concat(
                        Iterables.transform(windows, IndexReplicationWindow::targetBuckets)),
                batch -> windows.contains(batch.window()));
    }

    /** Releases one retired index window while preserving every other producer's batches. */
    int dropWindow(IndexReplicationWindow window) {
        return dropBatches(window.targetBuckets(), batch -> batch.window() == window);
    }

    private int dropBatches(Iterable<TableBucket> targets, Predicate<IndexBatch> shouldDrop) {
        List<IndexBatch> droppedBatches = new ArrayList<>();
        Set<TableBucket> affectedBuckets = new HashSet<>();
        for (TableBucket bucket : targets) {
            queues.computeIfPresent(
                    bucket,
                    (ignored, queue) -> {
                        synchronized (queue) {
                            Iterator<IndexBatch> it = queue.batches.iterator();
                            while (it.hasNext()) {
                                IndexBatch batch = it.next();
                                if (shouldDrop.test(batch)) {
                                    it.remove();
                                    if (queue.claimed == batch) {
                                        queue.claimed = null;
                                    }
                                    droppedBatches.add(batch);
                                    affectedBuckets.add(bucket);
                                }
                            }
                            return queue.batches.isEmpty() ? null : queue;
                        }
                    });
        }
        for (IndexBatch batch : droppedBatches) {
            release(batch);
        }
        // Removing a claimed head can expose another source's batch after its ready notification
        // was consumed. Notify outside the queue locks even if the queue has since changed.
        for (TableBucket bucket : affectedBuckets) {
            notifyReadyBucket(bucket);
        }
        return droppedBatches.size();
    }
}
