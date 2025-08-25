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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.exception.WakeupException;
import com.alibaba.fluss.metadata.TableBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * {@link LogFetchBuffer} buffers up {@link CompletedFetch the results} from the tablet server
 * responses as they are received. It's essentially a wrapper around a {@link java.util.Queue} of
 * {@link CompletedFetch}. There is at most one {@link LogFetchBuffer} per bucket in the queue.
 *
 * <p>Note: this class is thread-safe with the intention that {@link CompletedFetch the data} will
 * be created by a background thread and consumed by the application thread.
 */
@ThreadSafe
@Internal
public class LogFetchBuffer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(LogFetchBuffer.class);

    private final Lock lock = new ReentrantLock();
    private final Condition notEmptyCondition = lock.newCondition();
    private final AtomicBoolean wokenup = new AtomicBoolean(false);

    @GuardedBy("lock")
    private final LinkedList<CompletedFetch> completedFetches;

    @GuardedBy("lock")
    private final Map<TableBucket, LinkedList<PendingFetch>> pendingFetches = new HashMap<>();

    @GuardedBy("lock")
    private @Nullable CompletedFetch nextInLineFetch;

    public LogFetchBuffer() {
        this.completedFetches = new LinkedList<>();
    }

    /**
     * Returns {@code true} if there are no completed fetches pending to return to the user.
     *
     * @return {@code true} if the buffer is empty, {@code false} otherwise
     */
    boolean isEmpty() {
        return inLock(lock, completedFetches::isEmpty);
    }

    void pend(PendingFetch pendingFetch) {
        inLock(
                lock,
                () -> {
                    pendingFetches
                            .computeIfAbsent(pendingFetch.tableBucket(), k -> new LinkedList<>())
                            .add(pendingFetch);
                });
    }

    /**
     * Tries to complete the pending fetches in order, convert them into completed fetches in the
     * buffer.
     */
    void tryComplete(TableBucket tableBucket) {
        inLock(
                lock,
                () -> {
                    boolean hasCompleted = false;
                    LinkedList<PendingFetch> pendings = this.pendingFetches.get(tableBucket);
                    while (pendings != null && !pendings.isEmpty()) {
                        PendingFetch pendingFetch = pendings.peek();
                        if (pendingFetch.isCompleted()) {
                            CompletedFetch completedFetch = pendingFetch.toCompletedFetch();
                            completedFetches.add(completedFetch);
                            pendings.poll();
                            hasCompleted = true;
                        } else {
                            break;
                        }
                    }
                    if (hasCompleted) {
                        notEmptyCondition.signalAll();
                        // clear the bucket entry if there is no pending fetches for the bucket.
                        if (pendings.isEmpty()) {
                            this.pendingFetches.remove(tableBucket);
                        }
                    }
                });
    }

    void add(CompletedFetch completedFetch) {
        inLock(
                lock,
                () -> {
                    LinkedList<PendingFetch> pendings =
                            pendingFetches.get(completedFetch.tableBucket);
                    if (pendings == null || pendings.isEmpty()) {
                        completedFetches.add(completedFetch);
                        notEmptyCondition.signalAll();
                    } else {
                        pendings.add(new CompletedPendingFetch(completedFetch));
                    }
                });
    }

    void addAll(Collection<CompletedFetch> completedFetches) {
        if (completedFetches == null || completedFetches.isEmpty()) {
            return;
        }
        inLock(lock, () -> completedFetches.forEach(this::add));
    }

    CompletedFetch nextInLineFetch() {
        return inLock(lock, () -> nextInLineFetch);
    }

    void setNextInLineFetch(@Nullable CompletedFetch nextInLineFetch) {
        inLock(lock, () -> this.nextInLineFetch = nextInLineFetch);
    }

    CompletedFetch peek() {
        return inLock(lock, completedFetches::peek);
    }

    CompletedFetch poll() {
        return inLock(lock, completedFetches::poll);
    }

    /**
     * Allows the caller to await presence of data in the buffer. The method will block, returning
     * only under one of the following conditions:
     *
     * <ol>
     *   <li>The buffer was already non-empty on entry
     *   <li>The buffer was populated during the wait
     *   <li>The time out
     *   <li>The thread was interrupted
     * </ol>
     *
     * @param deadlineNanos the deadline time to wait until
     * @return false if the waiting time detectably elapsed before return from the method, else true
     */
    boolean awaitNotEmpty(long deadlineNanos) throws InterruptedException {
        return inLock(
                lock,
                () -> {
                    while (isEmpty()) {
                        if (wokenup.compareAndSet(true, false)) {
                            throw new WakeupException("The await is wakeup.");
                        }
                        long remainingNanos = deadlineNanos - System.nanoTime();
                        if (remainingNanos <= 0) {
                            // false for timeout
                            return false;
                        }

                        if (notEmptyCondition.await(remainingNanos, TimeUnit.NANOSECONDS)) {
                            // true for signal
                            return true;
                        }
                    }

                    // true for wakeup or we have data to return
                    return true;
                });
    }

    public void wakeup() {
        wokenup.set(true);
        inLock(lock, notEmptyCondition::signalAll);
    }

    /**
     * Updates the buffer to retain only the fetch data that corresponds to the given buckets. Any
     * previously {@link CompletedFetch fetched data} and {@link PendingFetch pending fetch} is
     * removed if its bucket is not in the given set of buckets.
     *
     * @param buckets {@link Set} of {@link TableBucket}s for which any buffered data should be kept
     */
    void retainAll(Set<TableBucket> buckets) {
        inLock(
                lock,
                () -> {
                    completedFetches.removeIf(cf -> maybeDrain(buckets, cf));

                    if (maybeDrain(buckets, nextInLineFetch)) {
                        nextInLineFetch = null;
                    }

                    // remove entries that not matches the buckets from pendingFetches
                    pendingFetches.entrySet().removeIf(entry -> !buckets.contains(entry.getKey()));
                });
    }

    /**
     * Drains (i.e. <em>removes</em>) the contents of the given {@link CompletedFetch} as its data
     * should not be returned to the user.
     */
    private boolean maybeDrain(Set<TableBucket> excludedBuckets, CompletedFetch completedFetch) {
        if (completedFetch != null && !excludedBuckets.contains(completedFetch.tableBucket)) {
            LOG.debug(
                    "Removing {} from buffered fetch data as it is not in the set of buckets to retain ({})",
                    completedFetch.tableBucket,
                    excludedBuckets);
            completedFetch.drain();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Return the set of {@link TableBucket buckets} for which we have data in the buffer.
     *
     * @return {@link TableBucket bucket} set
     */
    @Nullable
    Set<TableBucket> bufferedBuckets() {
        return inLock(
                lock,
                () -> {
                    final Set<TableBucket> buckets = new HashSet<>();
                    if (nextInLineFetch != null && !nextInLineFetch.isConsumed()) {
                        buckets.add(nextInLineFetch.tableBucket);
                    }
                    completedFetches.forEach(cf -> buckets.add(cf.tableBucket));
                    buckets.addAll(pendingFetches.keySet());
                    return buckets;
                });
    }

    /** Return the set of {@link TableBucket buckets} for which we have pending fetches. */
    Set<TableBucket> pendedBuckets() {
        return inLock(lock, pendingFetches::keySet);
    }

    @Override
    public void close() throws Exception {
        inLock(lock, () -> retainAll(Collections.emptySet()));
    }
}
