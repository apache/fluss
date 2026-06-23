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

package org.apache.fluss.server.metadata;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A thread-safe negative cache for partition IDs that are known to not exist in ZooKeeper.
 *
 * <p>This cache helps reduce ZooKeeper pressure when clients repeatedly request metadata for
 * partitions that have been deleted (e.g., during hourly partition rotation). Instead of querying
 * ZK every time, we cache the "not exist" result and return it directly.
 *
 * <p>The cache uses access-time-based TTL: entries are evicted after a configurable duration of no
 * access. As long as clients keep asking for the same non-existent partition, the cache entry stays
 * alive and protects ZK.
 */
@ThreadSafe
public class PartitionNegativeCache {

    /** Default TTL for negative cache entries: 10 minutes of no access. */
    private static final Duration DEFAULT_TTL = Duration.ofMinutes(10);

    /**
     * Cleanup interval: only run eviction check when at least this many milliseconds have passed
     * since the last cleanup.
     */
    private static final long CLEANUP_INTERVAL_MS = 60_000;

    private final ConcurrentHashMap<Long, Long> cache;
    private final long ttlMs;
    private final AtomicLong lastCleanupTime;
    private final Clock clock;

    public PartitionNegativeCache() {
        this(DEFAULT_TTL);
    }

    public PartitionNegativeCache(Duration ttl) {
        this(ttl, SystemClock.getInstance());
    }

    @VisibleForTesting
    PartitionNegativeCache(Duration ttl, Clock clock) {
        this.cache = new ConcurrentHashMap<>();
        this.ttlMs = ttl.toMillis();
        this.clock = clock;
        this.lastCleanupTime = new AtomicLong(clock.milliseconds());
    }

    /**
     * Checks if the given partition ID is known to not exist.
     *
     * <p>If the entry exists and is not expired, its access time is refreshed and this method
     * returns {@code true}. If the entry is expired or doesn't exist, returns {@code false}.
     *
     * @param partitionId the partition ID to check
     * @return {@code true} if the partition is known to not exist, {@code false} otherwise
     */
    public boolean isKnownNonExistent(long partitionId) {
        Long lastAccessTime = cache.get(partitionId);
        if (lastAccessTime == null) {
            return false;
        }
        long now = clock.milliseconds();
        if (now - lastAccessTime > ttlMs) {
            // Entry expired, remove it
            cache.remove(partitionId, lastAccessTime);
            return false;
        }
        // Refresh access time
        cache.put(partitionId, now);
        return true;
    }

    /**
     * Marks the given partition ID as known to not exist. The entry will remain in the cache until
     * it hasn't been accessed for the configured TTL duration.
     *
     * @param partitionId the partition ID to mark as non-existent
     */
    public void markNonExistent(long partitionId) {
        cache.put(partitionId, clock.milliseconds());
        maybeCleanup();
    }

    /**
     * Returns the current number of entries in the cache. Includes potentially expired entries that
     * haven't been cleaned up yet.
     */
    @VisibleForTesting
    public int size() {
        return cache.size();
    }

    /** Removes all entries from the cache. */
    @VisibleForTesting
    public void clear() {
        cache.clear();
    }

    /**
     * Performs lazy cleanup of expired entries. Only runs if enough time has passed since the last
     * cleanup to avoid excessive iteration.
     */
    private void maybeCleanup() {
        long now = clock.milliseconds();
        long lastCleanup = lastCleanupTime.get();
        if (now - lastCleanup < CLEANUP_INTERVAL_MS) {
            return;
        }
        // CAS to ensure only one thread does the cleanup
        if (!lastCleanupTime.compareAndSet(lastCleanup, now)) {
            return;
        }
        cache.entrySet().removeIf(entry -> now - entry.getValue() > ttlMs);
    }
}
