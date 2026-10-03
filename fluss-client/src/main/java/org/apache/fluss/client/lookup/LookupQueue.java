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

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A queue that buffers pending lookup operations by lookup queue key and drains globally bounded
 * batches.
 *
 * <p>Lookups within a queue key preserve FIFO order. A drain consumes one key continuously before
 * moving to the next key. If a key still has pending lookups after the global batch is full, it is
 * moved to the tail so the next drain starts from another key. Before a drained batch is sent, its
 * keys are counted as in-flight until the corresponding requests complete.
 */
@ThreadSafe
@Internal
class LookupQueue {

    private final ReentrantLock stateLock = new ReentrantLock();
    private final Condition appendCondition = stateLock.newCondition();
    private final Condition drainCondition = stateLock.newCondition();

    private final Map<LookupQueueKey, Deque<AbstractLookupQuery<?>>> lookupQueues;
    private final Deque<LookupQueueKey> lookupOrder;
    private final Deque<AbstractLookupQuery<?>> reEnqueuedLookups;
    // Counts started send batches, including batches waiting to be submitted to the network.
    private final Map<LookupQueueKey, Integer> inFlightRequestsByKey;
    private final int queueSize;
    private final int maxBatchSize;
    private final int maxInFlightRequestsPerKey;
    private final long batchTimeoutNanos;

    private boolean closed;
    private boolean forceClosed;
    private int queuedSize;

    LookupQueue(Configuration conf) {
        this.queueSize = conf.get(ConfigOptions.CLIENT_LOOKUP_QUEUE_SIZE);
        this.maxBatchSize = conf.get(ConfigOptions.CLIENT_LOOKUP_MAX_BATCH_SIZE);
        this.maxInFlightRequestsPerKey =
                conf.get(ConfigOptions.CLIENT_LOOKUP_MAX_INFLIGHT_REQUESTS_PER_BUCKET);
        this.batchTimeoutNanos = conf.get(ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT).toNanos();
        checkArgument(queueSize > 0, "Lookup queue size must be greater than 0.");
        checkArgument(maxBatchSize > 0, "Lookup batch size must be greater than 0.");
        checkArgument(
                maxInFlightRequestsPerKey > 0,
                "Maximum in-flight lookup requests per lookup queue key must be greater than 0.");

        this.lookupQueues = new HashMap<>();
        this.lookupOrder = new ArrayDeque<>();
        this.reEnqueuedLookups = new ArrayDeque<>();
        this.inFlightRequestsByKey = new HashMap<>();
    }

    void appendLookup(AbstractLookupQuery<?> lookup) {
        InterruptedException interruptedException = null;
        stateLock.lock();
        try {
            while (queuedSize >= queueSize && !closed) {
                try {
                    appendCondition.await();
                } catch (InterruptedException e) {
                    interruptedException = e;
                    break;
                }
            }

            if (interruptedException == null) {
                if (closed) {
                    throw new IllegalStateException(
                            "Can not append lookup operation since the LookupQueue is closed.");
                }

                LookupQueueKey lookupQueueKey = LookupQueueKey.fromLookup(lookup);
                Deque<AbstractLookupQuery<?>> lookupQueue = lookupQueues.get(lookupQueueKey);
                if (lookupQueue == null) {
                    lookupQueue = new ArrayDeque<>();
                    lookupQueues.put(lookupQueueKey, lookupQueue);
                    lookupOrder.addLast(lookupQueueKey);
                }
                lookupQueue.addLast(lookup);
                queuedSize++;
                drainCondition.signal();
            }
        } finally {
            stateLock.unlock();
        }

        if (interruptedException != null) {
            Thread.currentThread().interrupt();
            lookup.future().completeExceptionally(interruptedException);
        }
    }

    /** Re-enqueues a retry without blocking an RPC callback thread on regular queue capacity. */
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

    /** Drain a globally bounded batch of lookup operations. */
    List<AbstractLookupQuery<?>> drain() throws InterruptedException {
        return drain(false);
    }

    /** Drain all lookup operations without waiting for the batch timeout. */
    List<AbstractLookupQuery<?>> drainAll() throws InterruptedException {
        return drain(true);
    }

    void startInFlightRequests(Set<LookupQueueKey> lookupQueueKeys) {
        stateLock.lock();
        try {
            for (LookupQueueKey lookupQueueKey : lookupQueueKeys) {
                inFlightRequestsByKey.merge(lookupQueueKey, 1, Integer::sum);
            }
        } finally {
            stateLock.unlock();
        }
    }

    void completeInFlightRequests(Set<LookupQueueKey> lookupQueueKeys) {
        stateLock.lock();
        try {
            boolean keyBecameSendable = false;
            for (LookupQueueKey lookupQueueKey : lookupQueueKeys) {
                int inFlightRequests = inFlightRequestsByKey.getOrDefault(lookupQueueKey, 0);
                checkState(
                        inFlightRequests > 0,
                        "No in-flight lookup request exists for lookup queue key %s.",
                        lookupQueueKey);
                if (inFlightRequests == maxInFlightRequestsPerKey) {
                    keyBecameSendable = true;
                }
                if (inFlightRequests == 1) {
                    inFlightRequestsByKey.remove(lookupQueueKey);
                } else {
                    inFlightRequestsByKey.put(lookupQueueKey, inFlightRequests - 1);
                }
            }
            if (keyBecameSendable) {
                drainCondition.signal();
            }
        } finally {
            stateLock.unlock();
        }
    }

    public void close() {
        close(false);
    }

    void forceClose() {
        close(true);
    }

    private void close(boolean forceClose) {
        stateLock.lock();
        try {
            closed = true;
            forceClosed |= forceClose;
            appendCondition.signalAll();
            drainCondition.signalAll();
        } finally {
            stateLock.unlock();
        }
    }

    private List<AbstractLookupQuery<?>> drain(boolean drainAll) throws InterruptedException {
        final long startNanos = System.nanoTime();
        final int drainLimit = drainAll ? Integer.MAX_VALUE : maxBatchSize;
        List<AbstractLookupQuery<?>> lookupOperations = new ArrayList<>(maxBatchSize);
        Set<LookupQueueKey> drainedKeys = new HashSet<>();
        stateLock.lock();
        try {
            while (lookupOperations.size() < drainLimit) {
                if (forceClosed) {
                    lookupOperations.clear();
                    return lookupOperations;
                }

                long nowNanos = System.nanoTime();
                long nextRetryDelayNanos =
                        drainReEnqueuedLookups(
                                lookupOperations,
                                drainedKeys,
                                drainLimit,
                                drainAll,
                                System.currentTimeMillis());
                drainLookups(lookupOperations, drainedKeys, drainLimit);

                if (lookupOperations.size() >= drainLimit) {
                    return lookupOperations;
                }
                if (drainAll) {
                    if (!hasUnDrainedUnsafe()) {
                        return lookupOperations;
                    }
                    drainCondition.await();
                    continue;
                }
                if (closed) {
                    return lookupOperations;
                }

                long waitNanos = batchTimeoutNanos - (nowNanos - startNanos);
                if (waitNanos <= 0) {
                    return lookupOperations;
                }
                if (nextRetryDelayNanos != Long.MAX_VALUE) {
                    waitNanos = Math.min(waitNanos, Math.max(1L, nextRetryDelayNanos));
                }
                drainCondition.awaitNanos(waitNanos);
            }
            return lookupOperations;
        } finally {
            stateLock.unlock();
        }
    }

    private long drainReEnqueuedLookups(
            List<AbstractLookupQuery<?>> lookupOperations,
            Set<LookupQueueKey> drainedKeys,
            int drainLimit,
            boolean drainAll,
            long nowMs) {
        long nextRetryDelayNanos = Long.MAX_VALUE;
        int retriesToCheck = reEnqueuedLookups.size();
        while (retriesToCheck > 0 && lookupOperations.size() < drainLimit) {
            AbstractLookupQuery<?> lookup = reEnqueuedLookups.removeFirst();
            long retryDelayMs = lookup.nextRetryTimeMs() - nowMs;
            if (!drainAll && retryDelayMs > 0) {
                nextRetryDelayNanos =
                        Math.min(nextRetryDelayNanos, TimeUnit.MILLISECONDS.toNanos(retryDelayMs));
                reEnqueuedLookups.addLast(lookup);
            } else if (!tryDrainKeyUnsafe(LookupQueueKey.fromLookup(lookup), drainedKeys)) {
                reEnqueuedLookups.addLast(lookup);
            } else {
                lookupOperations.add(lookup);
            }
            retriesToCheck--;
        }
        return nextRetryDelayNanos;
    }

    private void drainLookups(
            List<AbstractLookupQuery<?>> lookupOperations,
            Set<LookupQueueKey> drainedKeys,
            int drainLimit) {
        int keysToCheck = lookupOrder.size();
        int drainedLookups = 0;
        while (keysToCheck > 0 && lookupOperations.size() < drainLimit) {
            LookupQueueKey lookupQueueKey = lookupOrder.removeFirst();
            Deque<AbstractLookupQuery<?>> lookupQueue = lookupQueues.get(lookupQueueKey);
            checkState(
                    lookupQueue != null && !lookupQueue.isEmpty(),
                    "Lookup queue key %s is active without pending lookups.",
                    lookupQueueKey);

            if (!tryDrainKeyUnsafe(lookupQueueKey, drainedKeys)) {
                lookupOrder.addLast(lookupQueueKey);
                keysToCheck--;
                continue;
            }

            while (!lookupQueue.isEmpty() && lookupOperations.size() < drainLimit) {
                lookupOperations.add(lookupQueue.removeFirst());
                queuedSize--;
                drainedLookups++;
            }
            if (lookupQueue.isEmpty()) {
                lookupQueues.remove(lookupQueueKey);
            } else {
                lookupOrder.addLast(lookupQueueKey);
            }
            keysToCheck--;
        }

        if (drainedLookups > 0) {
            appendCondition.signalAll();
        }
    }

    private boolean tryDrainKeyUnsafe(
            LookupQueueKey lookupQueueKey, Set<LookupQueueKey> drainedKeys) {
        if (drainedKeys.contains(lookupQueueKey)) {
            return true;
        }
        if (!canSendMoreRequestsUnsafe(lookupQueueKey)) {
            return false;
        }
        drainedKeys.add(lookupQueueKey);
        return true;
    }

    private boolean canSendMoreRequestsUnsafe(LookupQueueKey lookupQueueKey) {
        return inFlightRequestsByKey.getOrDefault(lookupQueueKey, 0) < maxInFlightRequestsPerKey;
    }

    private boolean hasUnDrainedUnsafe() {
        return queuedSize > 0 || !reEnqueuedLookups.isEmpty();
    }

    @VisibleForTesting
    int queuedSize() {
        stateLock.lock();
        try {
            return queuedSize;
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
    int inFlightRequestCount(LookupQueueKey lookupQueueKey) {
        stateLock.lock();
        try {
            return inFlightRequestsByKey.getOrDefault(lookupQueueKey, 0);
        } finally {
            stateLock.unlock();
        }
    }
}
