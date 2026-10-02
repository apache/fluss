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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;

import javax.annotation.concurrent.ThreadSafe;

import java.time.Duration;

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

    /** Default maximum number of negative cache entries. */
    private static final long DEFAULT_MAXIMUM_SIZE = 10_000L;

    private final Cache<Long, Boolean> cache;

    public PartitionNegativeCache() {
        this(DEFAULT_TTL, DEFAULT_MAXIMUM_SIZE);
    }

    public PartitionNegativeCache(Duration ttl, long maximumSize) {
        this(ttl, maximumSize, SystemClock.getInstance());
    }

    @VisibleForTesting
    PartitionNegativeCache(Duration ttl, long maximumSize, Clock clock) {
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("TTL must be positive.");
        }
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("Maximum size must be positive.");
        }
        this.cache =
                Caffeine.newBuilder()
                        .maximumSize(maximumSize)
                        .expireAfterAccess(ttl)
                        .ticker(
                                new Ticker() {
                                    @Override
                                    public long read() {
                                        return clock.nanoseconds();
                                    }
                                })
                        .build();
    }

    /**
     * Checks if the given partition ID is known to not exist.
     *
     * <p>If the entry exists and is not expired, Caffeine refreshes its access time and this method
     * returns {@code true}. If the entry is expired or doesn't exist, returns {@code false}.
     *
     * @param partitionId the partition ID to check
     * @return {@code true} if the partition is known to not exist, {@code false} otherwise
     */
    public boolean isKnownNonExistent(long partitionId) {
        return cache.getIfPresent(partitionId) != null;
    }

    /**
     * Marks the given partition ID as known to not exist. The entry will remain in the cache until
     * it hasn't been accessed for the configured TTL duration or the bounded cache evicts it.
     *
     * @param partitionId the partition ID to mark as non-existent
     */
    public void markNonExistent(long partitionId) {
        cache.put(partitionId, Boolean.TRUE);
    }

    /**
     * Marks the given partition ID as existing by removing any stale negative-cache entry.
     *
     * @param partitionId the partition ID to mark as existent
     */
    public void markExistent(long partitionId) {
        cache.invalidate(partitionId);
    }

    /** Returns the current number of entries in the cache after triggering cleanup. */
    @VisibleForTesting
    public long size() {
        cache.cleanUp();
        return cache.estimatedSize();
    }

    /** Removes all entries from the cache. */
    @VisibleForTesting
    public void clear() {
        cache.invalidateAll();
    }
}
