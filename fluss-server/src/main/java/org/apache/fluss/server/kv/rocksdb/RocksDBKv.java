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

/** A wrapper for the operation of {@link org.rocksdb.RocksDB}. */
public class RocksDBKv implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBKv.class);

    /** RocksDB property name for the number of SST files at level 0 (column-family scoped). */
    private static final String NUM_FILES_AT_LEVEL0 = "rocksdb.num-files-at-level0";

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

    // mark whether this kv is already closed and prevent duplicate closing
    private volatile boolean closed = false;

    public RocksDBKv(
            RocksDBResourceContainer optionsContainer,
            RocksDB db,
            ResourceGuard rocksDBResourceGuard,
            ColumnFamilyHandle defaultColumnFamilyHandle,
            @Nullable Statistics statistics,
            int level0SlowdownWritesTrigger,
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
        this.maxPendingFlushL0Files = maxWriteBufferNumber;
        this.writeBufferSize = writeBufferSize;
        this.flussL0SlowdownTrigger = flussL0SlowdownTrigger;
    }

    public ResourceGuard getResourceGuard() {
        return rocksDBResourceGuard;
    }

    public RocksDBWriteBatchWrapper newWriteBatch(
            long writeBatchSize, Counter flushCount, Histogram flushLatencyHistogram) {
        return new RocksDBWriteBatchWrapper(db, writeBatchSize, flushCount, flushLatencyHistogram);
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
     * Returns whether executing one more flush could push L0 file count to or beyond the storage
     * engine's slowdown trigger, accounting for pending memtable flushes that have not yet
     * materialized as L0 SSTs.
     *
     * <p>Used exclusively by the <b>flush path</b> ({@code KvTablet#flush}). The write path uses
     * {@link #wouldExceedFlushBudget(long)} which additionally accounts for buffer accumulation.
     *
     * <p>The headroom ({@link #maxPendingFlushL0Files}) is derived from {@code
     * maxWriteBufferNumber}: 1 file from this flush + up to {@code maxWriteBufferNumber - 1}
     * immutable memtables already queued. This guarantees the slowdown trigger is never reached
     * regardless of timing between the gate check and background flush completion.
     */
    public boolean wouldExceedSlowdownTriggerOnFlush() {
        if (level0SlowdownWritesTrigger <= 0) {
            return false;
        }
        return currentL0FileCount() + maxPendingFlushL0Files >= level0SlowdownWritesTrigger;
    }

    /**
     * Returns whether accepting more writes would exceed the safe flush budget. Combines two checks
     * with a single L0 read:
     *
     * <ol>
     *   <li><b>L0 gate</b>: {@code currentL0 + maxPendingFlushL0Files >= slowdownTrigger}
     *   <li><b>Buffer budget</b>: {@code pendingBufferBytes >= remainingL0Slots * writeBufferSize}
     * </ol>
     *
     * <p>This guarantees that no flush of the accumulated pre-write buffer can trigger RocksDB's
     * {@code level0_slowdown_writes_trigger}, regardless of how many memtable switches the
     * WriteBatch induces.
     */
    public boolean wouldExceedFlushBudget(long pendingBufferBytes) {
        if (level0SlowdownWritesTrigger <= 0) {
            return false;
        }
        long l0 = currentL0FileCount();
        long remainingSlots = (long) level0SlowdownWritesTrigger - l0 - maxPendingFlushL0Files - 1;
        if (remainingSlots <= 0) {
            return true;
        }
        if (writeBufferSize <= 0) {
            return false;
        }
        return pendingBufferBytes >= remainingSlots * writeBufferSize;
    }

    /**
     * Returns the current normalized backpressure pressure in {@code [0, 1)} for piggyback on write
     * responses. Mapping:
     *
     * <ul>
     *   <li>{@code l0 < flussL0SlowdownTrigger}: returns {@code 0}.
     *   <li>{@code flussL0SlowdownTrigger <= l0 < level0SlowdownWritesTrigger}: returns {@code (l0
     *       - flussL0SlowdownTrigger) / (level0SlowdownWritesTrigger - flussL0SlowdownTrigger)}.
     *   <li>{@code l0 >= level0SlowdownWritesTrigger}: clamped to a value strictly below 1.
     * </ul>
     *
     * <p>Never throws; hard rejection is the responsibility of {@link
     * #wouldExceedSlowdownTriggerOnFlush()} at the write/flush gates.
     */
    public float currentPressure() {
        if (level0SlowdownWritesTrigger <= flussL0SlowdownTrigger) {
            return 0f;
        }
        long l0Files = currentL0FileCount();
        if (l0Files < flussL0SlowdownTrigger) {
            return 0f;
        }
        int window = level0SlowdownWritesTrigger - flussL0SlowdownTrigger;
        long offset = Math.min(l0Files - flussL0SlowdownTrigger, (long) window - 1);
        return (float) offset / window;
    }

    /**
     * Reads the current L0 file count via {@link RocksDB#getProperty(ColumnFamilyHandle, String)}.
     *
     * <p>Note: {@code rocksdb.num-files-at-level<N>} is a parametric <em>string</em> property in
     * RocksDB, not registered as an int property. {@code getLongProperty} (which maps to C++ {@code
     * GetIntProperty}) returns NotFound for it. We therefore use the string accessor and parse the
     * result. Returns {@code 0} when the property is unavailable so the caller treats it as "no
     * pressure".
     *
     * <p>The native call is fenced by {@link #rocksDBResourceGuard} so that {@link #close()} blocks
     * until any in-flight pressure sampling completes; touching the RocksDB native handle after
     * disposal would otherwise SIGABRT the JVM. When the guard is already closed, returns {@code 0}
     * so callers degrade to "no pressure" instead of failing the request.
     */
    private long currentL0FileCount() {
        final ResourceGuard.Lease lease;
        try {
            lease = rocksDBResourceGuard.acquireResource();
        } catch (IOException acquireFailed) {
            // RocksDB is already closed (or being closed); treat as no pressure.
            return 0L;
        }
        try {
            String value = db.getProperty(defaultColumnFamilyHandle, NUM_FILES_AT_LEVEL0);
            if (value == null || value.isEmpty()) {
                return 0L;
            }
            return Long.parseLong(value);
        } catch (RocksDBException e) {
            LOG.warn("Failed to query L0 file count for backpressure", e);
            return 0L;
        } finally {
            lease.close();
        }
    }
}
