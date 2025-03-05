/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.replica.delay;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.server.utils.timer.DefaultTimer;
import com.alibaba.fluss.server.utils.timer.Timer;
import com.alibaba.fluss.server.utils.timer.TimerTask;
import com.alibaba.fluss.utils.MapUtils;
import com.alibaba.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;
import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** A manager for bookkeeping delay operations with a timeout, and expiring timed out operations. */
public final class DelayedOperationManager<T extends DelayedOperation> {
    private static final Logger LOG = LoggerFactory.getLogger(DelayedOperationManager.class);

    // Shard the watcher list to reduce lock contention.
    private static final int DEFAULT_SHARDS = 512;

    private final String managerName;
    private final Timer timeoutTimer;

    private final int serverId;
    private final int purgeInterval;

    private final List<WatcherList> watcherLists;
    // the number of estimated total operations in the manager.
    private final AtomicInteger estimatedTotalOperations = new AtomicInteger(0);
    private final ExpiredOperationReaper expirationReaper;

    public DelayedOperationManager(String managerName, int serverId, int purgeInterval) {
        this.managerName = managerName;
        this.timeoutTimer = new DefaultTimer(managerName);
        this.serverId = serverId;
        this.purgeInterval = purgeInterval;
        this.watcherLists = new ArrayList<>(DEFAULT_SHARDS);
        for (int i = 0; i < DEFAULT_SHARDS; i++) {
            watcherLists.add(new WatcherList());
        }

        this.expirationReaper = new ExpiredOperationReaper();
        expirationReaper.start();
    }

    /**
     * Check if the operation can be completed, if not watch it based on the given watch keys.
     *
     * <p>Note that a delayed operation can be watched on multiple keys. It is possible that an
     * operation is completed after it has been added to the watch list for some, but not all the
     * keys. In this case, the operation is considered completed and won't be added to the watch
     * list of the remaining keys. The expiration reaper thread will remove this operation from any
     * watcher list in which the operation exists.
     *
     * @param operation the delayed operation to be checked
     * @param watchKeys keys for bookkeeping the operation
     * @return true if the delayed operations can be completed by the caller
     */
    public boolean tryCompleteElseWatch(T operation, List<Object> watchKeys) {
        checkArgument(!watchKeys.isEmpty());

        // The cost of tryComplete() is typically proportional to the number of keys. Calling
        // tryComplete() for each key is going to be expensive if there are many keys. Instead, we
        // do the check in the following way through safeTryCompleteOrElse().
        // If the operation is not completed, we just add the operation to all keys. Then we call
        // tryComplete() again. At this time, if the operation is still not completed, we are
        // guaranteed that it won't miss any future triggering event since the operation is already
        // on the watcher list for all keys.
        if (operation.safeTryCompleteOrElse(
                () -> {
                    watchKeys.forEach(key -> watchForOperation(key, operation));
                    if (!watchKeys.isEmpty()) {
                        estimatedTotalOperations.incrementAndGet();
                    }
                })) {
            return true;
        }

        // if it cannot be completed by now and hence is watched, add to the expiry queue also.
        if (!operation.isCompleted()) {
            timeoutTimer.add(operation);

            if (operation.isCompleted()) {
                // cancel the timer task.
                operation.cancel();
            }
        }

        return false;
    }

    /**
     * Check if some delayed operations can be completed with the given watch key, and if yes
     * complete them.
     *
     * @return the number of completed operations during this process
     */
    public int checkAndComplete(Object key) {
        WatcherList wl = watcherList(key);
        Watcher watcher = inLock(wl.watcherLock, () -> wl.watchersByKey.get(key));

        int numCompleted = watcher == null ? 0 : watcher.tryCompletedWatched();
        if (numCompleted > 0) {
            LOG.debug("Request key {} unblocked {} {} operations", key, numCompleted, managerName);
        }

        return numCompleted;
    }

    /**
     * Return the total size of watch lists the manager contains. Since an operation may be watched
     * on multiple lists, and some of its watched entries may still be in the watch lists even when
     * it has been completed, this number may be larger than the number of real operations watched.
     */
    public int watched() {
        int count = 0;
        for (WatcherList wl : watcherLists) {
            for (Watcher watcher : wl.allWatchers()) {
                count += watcher.countWatched();
            }
        }
        return count;
    }

    /** Return the number of delayed operations in the expiry queue. */
    public int numDelayed() {
        return timeoutTimer.numOfTimerTasks();
    }

    /**
     * Cancel watching on any delayed operations for the given key. Note the operation will not be
     * completed.
     */
    @VisibleForTesting
    public List<T> cancelForKey(Object key) {
        WatcherList wl = watcherList(key);
        return inLock(
                wl.watcherLock,
                () -> {
                    Watcher watcher = wl.watchersByKey.remove(key);
                    if (watcher != null) {
                        return watcher.cancel();
                    } else {
                        return new ArrayList<>();
                    }
                });
    }

    @VisibleForTesting
    void watchForOperation(Object key, T operation) {
        WatcherList wl = watcherList(key);
        inLock(
                wl.watcherLock,
                () -> {
                    Watcher watcher = wl.watchersByKey.computeIfAbsent(key, Watcher::new);
                    watcher.watch(operation);
                });
    }

    private WatcherList watcherList(Object key) {
        return watcherLists.get(Math.abs(key.hashCode()) % watcherLists.size());
    }

    private void removeKeyIfEmpty(Object key, Watcher watcher) {
        WatcherList wl = watcherList(key);
        inLock(
                wl.watcherLock,
                () -> {
                    // if the current key is no longer correlated to the watcher to remove, skip.
                    if (wl.watchersByKey.get(key) != watcher) {
                        return;
                    }

                    if (watcher != null && watcher.isEmpty()) {
                        wl.watchersByKey.remove(key);
                    }
                });
    }

    public void shutdown() {
        expirationReaper.initiateShutdown();
        // improve shutdown time by waking up any ShutdownableThread(s) blocked on poll by
        // sending a no-op.
        timeoutTimer.add(
                new TimerTask(0) {
                    @Override
                    public void run() {}
                });
        try {
            expirationReaper.awaitShutdown();
        } catch (InterruptedException e) {
            throw new FlussRuntimeException("Error while shutdown delayed operation manager", e);
        }

        timeoutTimer.shutdown();
    }

    /** A list of operation watching keys. */
    private class WatcherList {
        private final Map<Object, Watcher> watchersByKey;
        private final Lock watcherLock;

        public WatcherList() {
            this.watchersByKey = MapUtils.newConcurrentHashMap();
            this.watcherLock = new ReentrantLock();
        }

        /**
         * Return all the current watcher lists, note that the returned watcher may be removed from
         * the list by other threads.
         */
        public Collection<Watcher> allWatchers() {
            return watchersByKey.values();
        }
    }

    /** A linked list of watched delayed operations based on some key. */
    private class Watcher {
        private final Object key;
        private final Queue<T> operations;

        public Watcher(Object key) {
            this.key = key;
            this.operations = new ConcurrentLinkedQueue<>();
        }

        /**
         * count the current number of watched operations. This is O(n), so use isEmpty() if
         * possible
         */
        public int countWatched() {
            return operations.size();
        }

        public boolean isEmpty() {
            return operations.isEmpty();
        }

        /** Add the element to watch. */
        public void watch(T t) {
            operations.add(t);
        }

        /** traverse the list and try to complete some watched elements. */
        public int tryCompletedWatched() {
            int completed = 0;
            Iterator<T> iter = operations.iterator();
            while (iter.hasNext()) {
                T curr = iter.next();
                if (curr.isCompleted()) {
                    iter.remove();
                } else if (curr.safeTryComplete()) {
                    iter.remove();
                    completed++;
                }
            }

            if (operations.isEmpty()) {
                removeKeyIfEmpty(key, this);
            }
            return completed;
        }

        public List<T> cancel() {
            Iterator<T> iter = operations.iterator();
            List<T> cancelled = new ArrayList<>();
            while (iter.hasNext()) {
                T curr = iter.next();
                curr.cancel();
                iter.remove();
                cancelled.add(curr);
            }
            return cancelled;
        }

        /** traverse the list and purge elements that are already completed by others. */
        public int purgeCompleted() {
            int purged = 0;
            Iterator<T> iter = operations.iterator();
            while (iter.hasNext()) {
                T curr = iter.next();
                if (curr.isCompleted()) {
                    iter.remove();
                    purged++;
                }
            }
            if (operations.isEmpty()) {
                removeKeyIfEmpty(key, this);
            }
            return purged;
        }
    }

    /** A background reaper to expire delayed operations that have timed out. */
    private class ExpiredOperationReaper extends ShutdownableThread {
        public ExpiredOperationReaper() {
            super(String.format("ExpirationReaper-%d-%s", serverId, managerName), false);
        }

        @Override
        public void doWork() {
            try {
                advanceClock();
            } catch (InterruptedException e) {
                throw new FlussRuntimeException("Interrupt while advancing clock", e);
            }
        }

        private void advanceClock() throws InterruptedException {
            timeoutTimer.advanceClock(200L);

            // Trigger an expiry operation if the number of completed but still being watched
            // operations is
            // larger than the purge threshold. That number is computed by the difference btw the
            // estimated total number of operations and the number of pending delayed operations.
            if (estimatedTotalOperations.get() - numDelayed() > purgeInterval) {
                // now set estimatedTotalOperations to delayed (the number of pending operations)
                // since we are going to clean up watcher. Note that, if more operations are
                // completed during the cleanup, we may end up with a little overestimated total
                // number of operations.
                estimatedTotalOperations.getAndSet(numDelayed());
                LOG.debug("Begin purging watch lists");
                int purged = 0;
                for (WatcherList watcherList : watcherLists) {
                    for (Watcher watcher : watcherList.allWatchers()) {
                        purged += watcher.purgeCompleted();
                    }
                }
                LOG.debug("Purged {} elements from watch lists", purged);
            }
        }
    }
}
