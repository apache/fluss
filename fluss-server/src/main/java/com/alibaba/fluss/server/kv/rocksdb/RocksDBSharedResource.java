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

package com.alibaba.fluss.server.kv.rocksdb;

import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.ReadableConfig;
import com.alibaba.fluss.utils.IOUtils;

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A class that manages global shared RocksDB resources, mainly responsible for managing shared
 * block cache across multiple RocksDB instances.
 *
 * <p>This class uses reference counting to manage resource lifecycle, following these principles:
 *
 * <ul>
 *   <li>When reference count is 0, resources enter closeable state but are not auto-released
 *   <li>Only when the manager actively calls close() method, resources are actually released
 *   <li>If reference count is not 0 when close() is called, it blocks until all references are
 *       released
 * </ul>
 *
 * <p>Closeable state determination: When reference count is 0, it's in closeable state.
 *
 * <p>Note: This class is thread-safe and can be used concurrently across multiple threads.
 */
@ThreadSafe
public class RocksDBSharedResource {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBSharedResource.class);

    /** Global singleton instance. */
    private static volatile RocksDBSharedResource instance;

    private final ReadableConfig configuration;
    private final Object lock = new Object();

    /** Reference count for resource lifecycle management. */
    @GuardedBy("lock")
    private final AtomicInteger referenceCount = new AtomicInteger(0);

    /** Shared block cache. */
    @GuardedBy("lock")
    private Cache sharedBlockCache;

    /** Whether the resource has been closed. */
    @GuardedBy("lock")
    private boolean closed = false;

    private RocksDBSharedResource(ReadableConfig configuration) {
        this.configuration = configuration;
    }

    /**
     * Get the global RocksDBSharedResource instance.
     *
     * @param configuration configuration parameters
     * @return RocksDBSharedResource instance
     */
    public static RocksDBSharedResource getInstance(ReadableConfig configuration) {
        if (instance == null) {
            synchronized (RocksDBSharedResource.class) {
                if (instance == null) {
                    instance = new RocksDBSharedResource(configuration);
                }
            }
        }
        return instance;
    }

    /**
     * Get the shared block cache instance.
     *
     * @return Shared Cache instance, returns null if shared block cache is not enabled or already
     *     closed
     */
    @Nullable
    public Cache getSharedBlockCache() {
        synchronized (lock) {
            if (closed) {
                return null;
            }

            if (sharedBlockCache == null && shouldCreateSharedBlockCache()) {
                createSharedBlockCache();
            }

            return sharedBlockCache;
        }
    }

    /**
     * Increase reference count.
     *
     * @return Current reference count
     */
    public int acquire() {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("RocksDBSharedResource has been closed");
            }

            int newCount = referenceCount.incrementAndGet();
            LOG.debug("Acquired RocksDBSharedResource, reference count: {}", newCount);
            return newCount;
        }
    }

    /**
     * Decrease reference count, enters closeable state when reference count becomes 0.
     *
     * @return Current reference count
     */
    public int release() {
        synchronized (lock) {
            int newCount = referenceCount.decrementAndGet();
            LOG.debug("Released RocksDBSharedResource, reference count: {}", newCount);

            if (newCount == 0) {
                // Wake up any waiting close() method
                lock.notifyAll();
            } else if (newCount < 0) {
                LOG.warn("Reference count became negative: {}", newCount);
                referenceCount.set(0);
                lock.notifyAll();
            }

            return Math.max(newCount, 0);
        }
    }

    /**
     * Close shared resources. If current reference count is not 0, it will block and wait until all
     * references are released.
     */
    public void close() {
        synchronized (lock) {
            if (closed) {
                return;
            }

            // Wait for all references to be released
            while (referenceCount.get() > 0) {
                LOG.info(
                        "Waiting for {} references to be released before closing RocksDBSharedResource",
                        referenceCount.get());
                try {
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting for references to be released", e);
                    break;
                }
            }

            closed = true;
            closeSharedResources();

            LOG.info("RocksDBSharedResource closed");
        }
    }

    /**
     * Get current reference count (mainly for testing).
     *
     * @return Current reference count
     */
    @VisibleForTesting
    public int getReferenceCount() {
        synchronized (lock) {
            return referenceCount.get();
        }
    }

    /**
     * Check if in closeable state (reference count is 0) (mainly for testing).
     *
     * @return true if in closeable state
     */
    @VisibleForTesting
    public boolean isCloseable() {
        synchronized (lock) {
            return referenceCount.get() == 0;
        }
    }

    /**
     * Check if already closed (mainly for testing).
     *
     * @return true if already closed
     */
    @VisibleForTesting
    public boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    /**
     * Check if shared block cache should be created.
     *
     * @return true if shared block cache should be created
     */
    private boolean shouldCreateSharedBlockCache() {
        return configuration.get(ConfigOptions.KV_SHARED_BLOCK_CACHE_ENABLED);
    }

    /** Create shared block cache. */
    private void createSharedBlockCache() {
        if (sharedBlockCache != null) {
            return;
        }

        long cacheSize = configuration.get(ConfigOptions.KV_SHARED_BLOCK_CACHE_SIZE).getBytes();
        int cacheNumShardBits =
                configuration.get(ConfigOptions.KV_SHARED_BLOCK_CACHE_NUM_SHARD_BITS);
        boolean strictCapacityLimit =
                configuration.get(ConfigOptions.KV_SHARED_BLOCK_CACHE_STRICT_CAPACITY_LIMIT);
        double highPriPoolRatio =
                configuration.get(ConfigOptions.KV_SHARED_BLOCK_CACHE_HIGH_PRI_POOL_RATIO);

        // Load RocksDB native library if needed, this operation is idempotent
        RocksDB.loadLibrary();

        // Create LRU cache
        // Parameters:
        // - capacity: cache size
        // - numShardBits: number of bits for shard count (8 means 256 shards)
        // - strictCapacityLimit: whether to strictly limit capacity
        // - highPriPoolRatio: ratio of high priority pool
        sharedBlockCache =
                new LRUCache(cacheSize, cacheNumShardBits, strictCapacityLimit, highPriPoolRatio);

        LOG.info(
                "Created shared block cache with size: {} bytes, numShardBits: {}, strictCapacityLimit: {}, highPriPoolRatio: {}",
                cacheSize,
                cacheNumShardBits,
                strictCapacityLimit,
                highPriPoolRatio);
    }

    /** Close shared resources. */
    private void closeSharedResources() {
        if (sharedBlockCache != null) {
            IOUtils.closeQuietly(sharedBlockCache);
            sharedBlockCache = null;
            LOG.info("Closed shared block cache");
        }
    }

    /** Reset singleton instance (mainly for testing). */
    @VisibleForTesting
    public static void resetInstance() {
        if (instance != null) {
            instance.close();
            instance = null;
        }
    }
}
