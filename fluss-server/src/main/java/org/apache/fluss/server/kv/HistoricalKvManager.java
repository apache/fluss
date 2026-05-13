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

package org.apache.fluss.server.kv;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.kv.rocksdb.RocksDBKvBuilder;
import org.apache.fluss.server.kv.rocksdb.RocksDBResourceContainer;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.clock.SystemClock;

import org.rocksdb.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manages non-persistent RocksDB instances for {@code __historical__} partition buckets.
 *
 * <p>Each {@link TableBucket} gets a lazily-created RocksDB instance that serves as a temporary KV
 * buffer before data is flushed to the data lake. The instances are:
 *
 * <ul>
 *   <li>Non-persistent: WAL is disabled (default in {@link RocksDBResourceContainer}) and no
 *       snapshots are taken.
 *   <li>Lazily created: A RocksDB instance is only opened on the first {@link #put} call.
 *   <li>Auto-evicted: Idle instances are cleaned up after a configurable timeout.
 * </ul>
 *
 * <p>Thread safety: {@link #put}, {@link #get}, and {@link #delete} acquire a shared read lock so
 * they can execute concurrently. The cleanup path uses {@code writeLock.tryLock()} to avoid
 * blocking active operations.
 */
@Internal
public class HistoricalKvManager {

    private static final Logger LOG = LoggerFactory.getLogger(HistoricalKvManager.class);

    private final Configuration conf;
    private final RateLimiter sharedRateLimiter;
    private final Clock clock;
    private final long idleTimeoutMs;

    private final Map<TableBucket, HistoricalKvHandle> handles = new ConcurrentHashMap<>();

    public HistoricalKvManager(Configuration conf, RateLimiter sharedRateLimiter) {
        this(conf, sharedRateLimiter, SystemClock.getInstance());
    }

    @VisibleForTesting
    HistoricalKvManager(Configuration conf, RateLimiter sharedRateLimiter, Clock clock) {
        this.conf = conf;
        this.sharedRateLimiter = sharedRateLimiter;
        this.clock = clock;
        this.idleTimeoutMs = conf.get(ConfigOptions.KV_HISTORICAL_IDLE_TIMEOUT).toMillis();
    }

    /**
     * Starts a periodic cleanup scheduler that evicts idle RocksDB instances.
     *
     * @param scheduler the executor to schedule cleanup tasks on
     */
    public void startCleanupScheduler(ScheduledExecutorService scheduler) {
        long periodMs = Math.max(idleTimeoutMs / 3, 1000);
        scheduler.scheduleAtFixedRate(
                this::cleanIdleInstances, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Writes a key-value pair into the RocksDB instance for the given bucket, creating the instance
     * lazily if needed.
     *
     * @param bucket the table bucket
     * @param dataDir the base data directory for this bucket's RocksDB storage
     * @param key the key bytes
     * @param value the value bytes
     * @throws IOException if the RocksDB write fails
     */
    public void put(TableBucket bucket, File dataDir, byte[] key, byte[] value) throws IOException {
        HistoricalKvHandle handle =
                handles.computeIfAbsent(bucket, k -> new HistoricalKvHandle(dataDir));
        handle.rwLock.readLock().lock();
        try {
            handle.ensureOpen(conf, sharedRateLimiter);
            handle.rocksDBKv.put(key, value);
            handle.lastAccessTimeMs = clock.milliseconds();
        } finally {
            handle.rwLock.readLock().unlock();
        }
    }

    /**
     * Looks up a key from the RocksDB instance for the given bucket.
     *
     * @param bucket the table bucket
     * @param key the key bytes
     * @return the value bytes, or {@code null} if the key is not found or the instance is not open
     * @throws IOException if the RocksDB read fails
     */
    @Nullable
    public byte[] get(TableBucket bucket, byte[] key) throws IOException {
        HistoricalKvHandle handle = handles.get(bucket);
        if (handle == null || handle.rocksDBKv == null) {
            return null;
        }
        handle.rwLock.readLock().lock();
        try {
            RocksDBKv kv = handle.rocksDBKv;
            if (kv == null) {
                return null;
            }
            byte[] result = kv.get(key);
            handle.lastAccessTimeMs = clock.milliseconds();
            return result;
        } finally {
            handle.rwLock.readLock().unlock();
        }
    }

    /**
     * Deletes a key from the RocksDB instance for the given bucket, creating the instance lazily if
     * needed.
     *
     * @param bucket the table bucket
     * @param dataDir the base data directory for this bucket's RocksDB storage
     * @param key the key bytes
     * @throws IOException if the RocksDB delete fails
     */
    public void delete(TableBucket bucket, File dataDir, byte[] key) throws IOException {
        HistoricalKvHandle handle =
                handles.computeIfAbsent(bucket, k -> new HistoricalKvHandle(dataDir));
        handle.rwLock.readLock().lock();
        try {
            handle.ensureOpen(conf, sharedRateLimiter);
            handle.rocksDBKv.delete(key);
            handle.lastAccessTimeMs = clock.milliseconds();
        } finally {
            handle.rwLock.readLock().unlock();
        }
    }

    /**
     * Forcefully drops the RocksDB instance for the given bucket, closing it and deleting its local
     * data.
     *
     * @param bucket the table bucket to drop
     */
    public void dropInstance(TableBucket bucket) {
        HistoricalKvHandle handle = handles.remove(bucket);
        if (handle != null) {
            handle.rwLock.writeLock().lock();
            try {
                handle.closeAndDelete();
            } finally {
                handle.rwLock.writeLock().unlock();
            }
        }
    }

    /** Closes all managed RocksDB instances and cleans up local data. */
    public void close() {
        Iterator<Map.Entry<TableBucket, HistoricalKvHandle>> it = handles.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<TableBucket, HistoricalKvHandle> entry = it.next();
            HistoricalKvHandle handle = entry.getValue();
            handle.rwLock.writeLock().lock();
            try {
                handle.closeAndDelete();
            } finally {
                handle.rwLock.writeLock().unlock();
            }
            it.remove();
        }
    }

    /**
     * Returns the number of currently open RocksDB instances. This is intended for testing
     * purposes.
     */
    @VisibleForTesting
    int openInstanceCount() {
        int count = 0;
        for (HistoricalKvHandle handle : handles.values()) {
            if (handle.rocksDBKv != null) {
                count++;
            }
        }
        return count;
    }

    /** Scans all handles and evicts those that have been idle longer than the timeout. */
    @VisibleForTesting
    void cleanIdleInstances() {
        long now = clock.milliseconds();
        Iterator<Map.Entry<TableBucket, HistoricalKvHandle>> it = handles.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<TableBucket, HistoricalKvHandle> entry = it.next();
            HistoricalKvHandle handle = entry.getValue();
            if (handle.rocksDBKv == null) {
                continue;
            }
            if (now - handle.lastAccessTimeMs <= idleTimeoutMs) {
                continue;
            }
            if (!handle.rwLock.writeLock().tryLock()) {
                continue;
            }
            try {
                // Double-check after acquiring write lock
                if (handle.rocksDBKv != null && now - handle.lastAccessTimeMs > idleTimeoutMs) {
                    LOG.info("Closing idle historical KV instance for bucket {}", entry.getKey());
                    handle.closeAndDelete();
                    it.remove();
                }
            } finally {
                handle.rwLock.writeLock().unlock();
            }
        }
    }

    /**
     * Internal handle wrapping a lazily-created RocksDB instance together with its concurrency
     * control state.
     */
    private static class HistoricalKvHandle {

        final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        final Object createLock = new Object();
        final File kvDir;

        volatile RocksDBKv rocksDBKv;
        volatile long lastAccessTimeMs;

        HistoricalKvHandle(File kvDir) {
            this.kvDir = kvDir;
        }

        /**
         * Ensures the RocksDB instance is open, creating it if necessary. This method must be
         * called while holding the read lock.
         */
        void ensureOpen(Configuration conf, RateLimiter sharedRateLimiter) throws IOException {
            if (rocksDBKv != null) {
                return;
            }
            synchronized (createLock) {
                if (rocksDBKv != null) {
                    return;
                }
                RocksDBResourceContainer container =
                        new RocksDBResourceContainer(conf, kvDir, true, sharedRateLimiter);
                RocksDBKvBuilder builder =
                        new RocksDBKvBuilder(kvDir, container, container.getColumnOptions());
                rocksDBKv = builder.build();
            }
        }

        /** Closes the RocksDB instance and deletes the local data directory. */
        void closeAndDelete() {
            RocksDBKv kv = rocksDBKv;
            rocksDBKv = null;
            if (kv != null) {
                try {
                    kv.close();
                } catch (Exception e) {
                    LOG.warn("Failed to close historical RocksDB instance", e);
                }
            }
            FileUtils.deleteDirectoryQuietly(kvDir);
        }
    }
}
