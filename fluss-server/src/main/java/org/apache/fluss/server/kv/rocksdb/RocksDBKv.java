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

package org.apache.fluss.server.kv.rocksdb;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.metrics.Histogram;
import org.apache.fluss.rocksdb.RocksDBOperationUtils;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/** A wrapper for the operation of {@link org.rocksdb.RocksDB}. */
public class RocksDBKv implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBKv.class);

    /** RocksDB property name for the number of SST files at level 0 (column-family scoped). */
    private static final String NUM_FILES_AT_LEVEL0 = "rocksdb.num-files-at-level0";

    private static final float DELAYED_WRITE_PRESSURE_FLOOR = 0.8f;

    private static final long L0_REFRESH_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(100);

    /** The container of RocksDB option factory and predefined options. */
    private final RocksDBResourceContainer optionsContainer;

    /**
     * Protects access to RocksDB in other threads, like the checkpointing thread from parallel call
     * that disposes the RocksDB object.
     */
    private final ResourceGuard rocksDBResourceGuard;

    /** The write options to use in the states. We disable write ahead logging. */
    private final WriteOptions writeOptions;

    /**
     * We are not using the default column family for KV ops, but we still need to remember this
     * handle so that we can close it properly when the kv is closed. Note that the one returned by
     * {@link RocksDB#open(String)} is different from that by {@link
     * RocksDB#getDefaultColumnFamily()}, probably it's a bug of RocksDB java API.
     */
    private final ColumnFamilyHandle defaultColumnFamilyHandle;

    /** Our RocksDB database. Currently, one kv tablet, one RocksDB instance. */
    protected final RocksDB db;

    /** RocksDB Statistics for metrics collection. */
    private final @Nullable Statistics statistics;

    /** L0 file count at which RocksDB starts throttling writes. Read from ColumnFamilyOptions. */
    private final int level0SlowdownWritesTrigger;

    /** L0 file count at which RocksDB stops writes. Read from ColumnFamilyOptions. */
    private final int level0StopWritesTrigger;

    /**
     * Maximum number of L0 files that could be produced between a gate check and the actual
     * completion of all pending memtable flushes. Derived from {@code maxWriteBufferNumber}: at
     * most 1 file from the flush triggered by this write, plus up to {@code maxWriteBufferNumber -
     * 1} immutable memtables already queued for flush that have not yet materialized as L0 SSTs.
     * The gate predicate uses this as the headroom to guarantee RocksDB's slowdown trigger is never
     * reached.
     */
    private final int maxPendingFlushL0Files;

    /** RocksDB memtable size in bytes. Used to compute the flush budget for the write path. */
    private final long writeBufferSize;

    /**
     * L0 file count at which Fluss starts emitting proactive backpressure signals (piggybacked on
     * PutKv responses) so clients can throttle before the storage engine blocks writes. Strictly
     * below {@link #level0SlowdownWritesTrigger}; when {@code >=} the RocksDB L0 slowdown trigger,
     * proactive backpressure is treated as misconfigured and disabled.
     */
    private final int flussL0SlowdownTrigger;

    private volatile boolean writeRejected;

    private final AtomicLong nextL0RefreshNanos = new AtomicLong(Long.MIN_VALUE);

    private volatile long cachedL0FileCount;

    // mark whether this kv is already closed and prevent duplicate closing
    private volatile boolean closed = false;

    public RocksDBKv(
            RocksDBResourceContainer optionsContainer,
            RocksDB db,
            ResourceGuard rocksDBResourceGuard,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            @Nullable Statistics statistics,
            int level0SlowdownWritesTrigger,
            int level0StopWritesTrigger,
            int maxWriteBufferNumber,
            long writeBufferSize,
            int flussL0SlowdownTrigger) {
        this.optionsContainer = optionsContainer;
        this.db = db;
        this.rocksDBResourceGuard = rocksDBResourceGuard;
        this.writeOptions = optionsContainer.getWriteOptions();
        this.defaultColumnFamilyHandle = defaultColumnFamilyHandle;
        this.statistics = statistics;
        this.level0SlowdownWritesTrigger = level0SlowdownWritesTrigger;
        this.level0StopWritesTrigger = level0StopWritesTrigger;
        this.maxPendingFlushL0Files = maxWriteBufferNumber;
        this.writeBufferSize = writeBufferSize;
        this.flussL0SlowdownTrigger = flussL0SlowdownTrigger;

        // Validate configuration relationship: Fluss proactive threshold must be strictly below
        // RocksDB's own slowdown trigger, otherwise the ramp-up window is zero and proactive
        // backpressure is silently disabled.
        if (flussL0SlowdownTrigger != Integer.MAX_VALUE
                && level0SlowdownWritesTrigger > 0
                && flussL0SlowdownTrigger >= level0SlowdownWritesTrigger) {
            LOG.warn(
                    "Proactive KV backpressure is effectively disabled: "
                            + "kv.backpressure.l0-slowdown-trigger ({}) >= RocksDB "
                            + "level0_slowdown_writes_trigger ({}). "
                            + "The Fluss threshold must be strictly lower to form a valid "
                            + "throttle ramp-up window.",
                    flussL0SlowdownTrigger,
                    level0SlowdownWritesTrigger);
        }
    }

    public ResourceGuard getResourceGuard() {
        return rocksDBResourceGuard;
    }

    public RocksDBWriteBatchWrapper newWriteBatch(
            long writeBatchSize, Counter flushCount, Histogram flushLatencyHistogram) {
        return new RocksDBWriteBatchWrapper(db, writeBatchSize, flushCount, flushLatencyHistogram);
    }

    /**
     * Creates a write batch that fails fast instead of waiting when RocksDB enters delayed-write or
     * stall state.
     */
    public RocksDBWriteBatchWrapper newNoSlowdownWriteBatch(
            long writeBatchSize, Counter flushCount, Histogram flushLatencyHistogram) {
        return new RocksDBWriteBatchWrapper(
                db,
                writeBatchSize,
                flushCount,
                flushLatencyHistogram,
                true,
                this::recordWriteRejected);
    }

    public @Nullable byte[] get(byte[] key) throws IOException {
        try {
            return db.get(key);
        } catch (RocksDBException e) {
            throw new IOException("Fail to get key.", e);
        }
    }

    public List<byte[]> multiGet(List<byte[]> keys) throws IOException {
        try {
            return db.multiGetAsList(keys);
        } catch (RocksDBException e) {
            throw new IOException("Fail to get keys.", e);
        }
    }

    public List<byte[]> prefixLookup(byte[] prefixKey) {
        List<byte[]> pkList = new ArrayList<>();
        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = db.newIterator(defaultColumnFamilyHandle, readOptions);
        try {
            iterator.seek(prefixKey);
            while (iterator.isValid() && BytesUtils.prefixEquals(prefixKey, iterator.key())) {
                pkList.add(iterator.value());
                iterator.next();
            }
        } finally {
            readOptions.close();
            iterator.close();
        }

        return pkList;
    }

    public List<byte[]> limitScan(Integer limit) {
        List<byte[]> pkList = new ArrayList<>();
        ReadOptions readOptions = new ReadOptions();
        RocksIterator iterator = db.newIterator(defaultColumnFamilyHandle, readOptions);

        int count = 0;
        try {
            iterator.seekToFirst();
            while (iterator.isValid() && count < limit) {
                pkList.add(iterator.value());
                iterator.next();
                count++;
            }
        } finally {
            readOptions.close();
            iterator.close();
        }

        return pkList;
    }

    public void put(byte[] key, byte[] value) throws IOException {
        try {
            db.put(writeOptions, key, value);
        } catch (RocksDBException e) {
            throw new IOException("Fail to put key.", e);
        }
    }

    public void delete(byte[] key) throws IOException {
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new IOException("Fail to delete key.", e);
        }
    }

    public void checkIfRocksDBClosed() {
        if (this.closed) {
            throw new FlussRuntimeException(
                    "The RocksDb for kv in "
                            + optionsContainer.getInstanceRocksDBPath()
                            + " is already closed");
        }
    }

    @Override
    public void close() throws Exception {
        if (this.closed) {
            return;
        }

        // This call will block until all clients that still acquire access to the RocksDB instance
        // have released it,
        // so that we cannot release the native resources while clients are still working with it in
        // parallel.
        rocksDBResourceGuard.close();

        // IMPORTANT: null reference to signal potential async checkpoint workers that the db was
        // disposed, as
        // working on the disposed object results in SEGFAULTS.
        if (db != null) {

            // RocksDB's native memory management requires that *all* CFs (including default) are
            // closed before the
            // DB is closed. See:
            // https://github.com/facebook/rocksdb/wiki/RocksJava-Basics#opening-a-database-with-column-families
            // Start with default CF ...
            List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>();
            RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(
                    columnFamilyOptions, defaultColumnFamilyHandle);
            IOUtils.closeQuietly(defaultColumnFamilyHandle);

            // ... and finally close the DB instance ...
            IOUtils.closeQuietly(db);

            columnFamilyOptions.forEach(IOUtils::closeQuietly);

            IOUtils.closeQuietly(optionsContainer);
        }
        this.closed = true;
    }

    public RocksDB getDb() {
        return db;
    }

    @Nullable
    public Statistics getStatistics() {
        return optionsContainer.getStatistics();
    }

    @Nullable
    public Cache getBlockCache() {
        return optionsContainer.getBlockCache();
    }

    public ColumnFamilyHandle getDefaultColumnFamilyHandle() {
        return defaultColumnFamilyHandle;
    }

    /**
     * Returns whether accepting more writes would exceed RocksDB's safe flush budget. The caller
     * must pass the total estimated bytes after accepting the current request.
     */
    public boolean wouldExceedFlushBudget(long pendingBufferBytes) {
        return hasWriteRejection()
                || reachesL0Slowdown(
                        cachedL0FileCount(),
                        estimateFlushFiles(pendingBufferBytes) + maxPendingFlushL0Files);
    }

    private int estimateFlushFiles(long pendingBufferBytes) {
        if (pendingBufferBytes <= 0 || writeBufferSize <= 0) {
            return 0;
        }
        long flushFiles = (pendingBufferBytes + writeBufferSize - 1) / writeBufferSize;
        return flushFiles >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) flushFiles;
    }

    private boolean reachesL0Slowdown(long l0Files, int extraL0Files) {
        int trigger = effectiveL0SlowdownTrigger();
        return trigger > 0 && l0Files + extraL0Files >= trigger;
    }

    private int effectiveL0SlowdownTrigger() {
        if (level0SlowdownWritesTrigger > 0) {
            return level0SlowdownWritesTrigger;
        }
        return level0StopWritesTrigger;
    }

    private long cachedL0FileCount() {
        if (closed) {
            return 0L;
        }
        refreshL0FileCountIfNeeded();
        return cachedL0FileCount;
    }

    @VisibleForTesting
    void refreshL0FileCount() {
        cachedL0FileCount = sampleL0FileCount();
        nextL0RefreshNanos.set(System.nanoTime() + L0_REFRESH_INTERVAL_NANOS);
    }

    private void refreshL0FileCountIfNeeded() {
        long now = System.nanoTime();
        long nextRefresh = nextL0RefreshNanos.get();
        if (now < nextRefresh
                || !nextL0RefreshNanos.compareAndSet(
                        nextRefresh, now + L0_REFRESH_INTERVAL_NANOS)) {
            return;
        }
        cachedL0FileCount = sampleL0FileCount();
    }

    private long sampleL0FileCount() {
        if (closed) {
            return 0L;
        }
        final ResourceGuard.Lease lease;
        try {
            lease = rocksDBResourceGuard.acquireResource();
        } catch (IOException acquireFailed) {
            // RocksDB is already closed (or being closed); treat as no pressure.
            return 0L;
        }
        try {
            return readLongProperty(NUM_FILES_AT_LEVEL0);
        } finally {
            lease.close();
        }
    }

    void recordWriteRejected() {
        writeRejected = true;
    }

    /** Clears the no-slowdown rejection latch after a later non-empty flush succeeds. */
    public void recordWriteSucceeded() {
        writeRejected = false;
    }

    private boolean hasWriteRejection() {
        return writeRejected;
    }

    private long readLongProperty(String propertyName) {
        try {
            String value = db.getProperty(defaultColumnFamilyHandle, propertyName);
            if (value == null || value.isEmpty()) {
                return 0L;
            }
            return Long.parseLong(value);
        } catch (RocksDBException | NumberFormatException e) {
            LOG.warn("Failed to query RocksDB property {} for backpressure", propertyName, e);
            return 0L;
        }
    }

    /**
     * Returns the recent normalized backpressure pressure in {@code [0, 1)} for piggyback on write
     * responses. The signal follows the primary L0 write-stall trend with a small sampling cache
     * and is raised after RocksDB rejects a no-slowdown flush until a later flush succeeds.
     */
    public float currentPressure() {
        if (closed) {
            return 0f;
        }
        float pressure = l0Pressure(cachedL0FileCount());
        if (hasWriteRejection()) {
            pressure = Math.max(pressure, DELAYED_WRITE_PRESSURE_FLOOR);
        }
        return Math.min(pressure, 0.999f);
    }

    private float l0Pressure(long l0Files) {
        if (level0SlowdownWritesTrigger <= flussL0SlowdownTrigger) {
            return 0f;
        }
        if (l0Files < flussL0SlowdownTrigger) {
            return 0f;
        }
        int window = level0SlowdownWritesTrigger - flussL0SlowdownTrigger;
        long offset = Math.min(l0Files - flussL0SlowdownTrigger, (long) window - 1);
        return (float) offset / window;
    }
}
