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

package org.apache.fluss.server.kv.scan;

import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.utils.ResourceGuard;
import org.apache.fluss.utils.IOUtils;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Server-side state for a single KV full-scan session: a pinned RocksDB {@link Snapshot}, a {@link
 * RocksIterator} cursor, and a {@link ResourceGuard.Lease} that keeps the underlying RocksDB alive
 * for the lifetime of the scan.
 *
 * <p>Cursor methods ({@link #advance()}, {@link #isValid()}, {@link #currentValue()}) are
 * single-threaded; callers must hold the {@link #tryAcquireForUse()} flag while mutating cursor or
 * sequence-id state. {@link #close()} is thread-safe: it immediately rejects new users and defers
 * native-resource cleanup until the current user releases the cursor.
 *
 * <p>The lease is required even though point lookups are protected from RocksDB close by {@code
 * KvTablet}'s state lock: a scanner and its native snapshot/iterator live across multiple RPCs and
 * therefore outlive any one state-lock acquisition. Closing follows a two-phase protocol. First,
 * {@code closed} prevents another RPC from acquiring the cursor. Second, the thread that observes
 * both {@code closed} and {@code inUse == false} closes the native resources exactly once. This may
 * be the closing thread, the current RPC in {@link #releaseAfterUse()}, or an acquire racing with
 * close. The protocol never force-clears {@code inUse}, because doing so could release the lease
 * and close an iterator while JNI is still using it.
 */
@NotThreadSafe
public class ScannerContext implements Closeable {
    private final String scannerId;
    private final byte[] scannerIdBytes;
    private final TableBucket tableBucket;
    private final RocksDBKv rocksDBKv;
    private final RocksIterator iterator;
    private final ReadOptions readOptions;
    private final Snapshot snapshot;
    private final ResourceGuard.Lease resourceLease;

    /** Log offset captured when the snapshot was opened. */
    private final long logOffset;

    private long remainingLimit;

    // -1 so the first client call_seq_id of 0 satisfies expectedSeqId = callSeqId + 1.
    // volatile: a continuation may run on a different RPC worker than the previous one.
    private volatile int callSeqId = -1;

    /** Last-access wall-clock (ms); volatile for the TTL evictor. */
    private volatile long lastAccessTime;

    /** Set when closure is requested; prevents every subsequent cursor acquisition. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /** Exclusive ownership of the native iterator by one RPC continuation. */
    private final AtomicBoolean inUse = new AtomicBoolean(false);

    /** Makes deferred native-resource cleanup idempotent across all close/release races. */
    private final AtomicBoolean resourcesClosed = new AtomicBoolean(false);

    public ScannerContext(
            String scannerId,
            TableBucket tableBucket,
            RocksDBKv rocksDBKv,
            RocksIterator iterator,
            ReadOptions readOptions,
            Snapshot snapshot,
            ResourceGuard.Lease resourceLease,
            long limit,
            long logOffset,
            long initialAccessTimeMs) {
        this.scannerId = scannerId;
        this.scannerIdBytes = scannerId.getBytes(StandardCharsets.UTF_8);
        this.tableBucket = tableBucket;
        this.rocksDBKv = rocksDBKv;
        this.iterator = iterator;
        this.readOptions = readOptions;
        this.snapshot = snapshot;
        this.resourceLease = resourceLease;
        this.remainingLimit = limit <= 0 ? -1L : limit;
        this.logOffset = logOffset;
        this.lastAccessTime = initialAccessTimeMs;
    }

    /** Log offset captured when the snapshot was opened. */
    public long getLogOffset() {
        return logOffset;
    }

    public byte[] getScannerId() {
        return scannerIdBytes;
    }

    /** Scanner ID as a UTF-8 string; used as the key in {@code ScannerManager#scanners}. */
    String getIdString() {
        return scannerId;
    }

    public TableBucket getTableBucket() {
        return tableBucket;
    }

    public boolean isValid() {
        return iterator.isValid() && remainingLimit != 0;
    }

    public byte[] currentValue() {
        return iterator.value();
    }

    /** Advances the cursor; must be called only when {@link #isValid()} is {@code true}. */
    public void advance() {
        iterator.next();
        if (remainingLimit > 0) {
            remainingLimit--;
        }
    }

    /**
     * Surfaces RocksDB-internal iterator errors. {@link RocksIterator#next()} does not throw on
     * such errors; instead the iterator silently transitions to invalid. Callers MUST invoke this
     * once iteration has stopped, otherwise an internal failure would be indistinguishable from a
     * clean end-of-range and the client would think the scan completed when rows were dropped.
     *
     * @throws KvStorageException if the iterator reports an internal error
     */
    public void checkIteratorStatus() {
        try {
            iterator.status();
        } catch (RocksDBException e) {
            throw new KvStorageException(
                    "RocksDB iterator error during scan for bucket " + tableBucket, e);
        }
    }

    /** Returns the call-sequence ID of the last successfully served request, or {@code -1}. */
    public int getCallSeqId() {
        return callSeqId;
    }

    /**
     * Updates the call-sequence ID. Must be called <em>after</em> the response payload is fully
     * prepared so a client retry with the same id can be detected.
     */
    public void setCallSeqId(int callSeqId) {
        this.callSeqId = callSeqId;
    }

    /** Resets the idle-TTL timer to the given wall-clock time. */
    public void updateLastAccessTime(long nowMs) {
        this.lastAccessTime = nowMs;
    }

    /** Returns {@code true} if idle for longer than {@code ttlMs}. */
    public boolean isExpired(long ttlMs, long nowMs) {
        return nowMs - lastAccessTime > ttlMs;
    }

    /**
     * Tries to claim exclusive use of the cursor. Returns {@code false} if another thread already
     * holds it, or if {@link #close()} has been initiated. Every successful acquire MUST be paired
     * with {@link #releaseAfterUse()} in a {@code finally}, otherwise native-resource cleanup is
     * deferred indefinitely after {@code close()}.
     */
    public boolean tryAcquireForUse() {
        if (closed.get()) {
            return false;
        }
        if (!inUse.compareAndSet(false, true)) {
            return false;
        }
        // Re-check after the CAS in case close() flipped `closed` in between. close() may have
        // observed inUse=true and deferred cleanup, so this path must finish it after releasing.
        if (closed.get()) {
            inUse.set(false);
            closeResources();
            return false;
        }
        return true;
    }

    /**
     * Releases the flag obtained via {@link #tryAcquireForUse()}. If close was requested while this
     * RPC owned the cursor, this method also performs the deferred native-resource cleanup.
     */
    public void releaseAfterUse() {
        inUse.set(false);
        if (closed.get()) {
            closeResources();
        }
    }

    /**
     * Prevents new cursor users immediately and closes resources when no RPC currently owns the
     * cursor. This method does not block waiting for an in-flight RPC; that RPC completes cleanup
     * from {@link #releaseAfterUse()}.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true) && !inUse.get()) {
            closeResources();
        }
    }

    private void closeResources() {
        if (resourcesClosed.compareAndSet(false, true)) {
            IOUtils.closeQuietly(iterator);
            IOUtils.closeQuietly(readOptions);
            IOUtils.closeQuietly(() -> rocksDBKv.getDb().releaseSnapshot(snapshot));
            IOUtils.closeQuietly(snapshot);
            IOUtils.closeQuietly(resourceLease);
        }
    }
}
