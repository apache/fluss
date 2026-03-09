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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.server.utils.ResourceGuard;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Server-side state for a single KV full-scan session.
 *
 * <p>A {@code ScannerContext} holds a point-in-time RocksDB {@link Snapshot}, the {@link
 * ReadOptions} pinning it, and a cursor ({@link RocksIterator}) that persists across multiple
 * batched-fetch RPCs from the same client. It also holds a {@link ResourceGuard.Lease} that
 * prevents the underlying RocksDB instance from being closed while the scan is in progress.
 *
 * <p>Instances are created by {@link org.apache.fluss.server.kv.KvTablet#openScan} and registered
 * by {@link ScannerManager}. They must be closed when the scan completes, the client requests an
 * explicit close, or the session expires due to inactivity.
 *
 * <p><b>Thread safety:</b> The iterator cursor ({@link #advance()}, {@link #isValid()}, {@link
 * #currentValue()}) must be driven by only one thread at a time. {@link #close()} is thread-safe.
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
    private long remainingLimit;
    private int callSeqId = -1;

    /**
     * Wall-clock timestamp (ms) of the most recent request that touched this session. Used by
     * {@link ScannerManager} for TTL-based eviction.
     */
    private long lastAccessTime;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    public ScannerContext(
            String scannerId,
            TableBucket tableBucket,
            RocksDBKv rocksDBKv,
            RocksIterator iterator,
            ReadOptions readOptions,
            Snapshot snapshot,
            ResourceGuard.Lease resourceLease,
            long limit,
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
        this.lastAccessTime = initialAccessTimeMs;
    }

    public byte[] getScannerId() {
        return scannerIdBytes;
    }

    String getId() {
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

    /**
     * Advances the cursor by one entry and decrements the remaining-rows limit if applicable. Must
     * only be called when {@link #isValid()} returns {@code true}.
     */
    public void advance() {
        iterator.next();
        if (remainingLimit > 0) {
            remainingLimit--;
        }
    }

    /** Returns the call-sequence ID of the last successfully served request, or {@code -1}. */
    public int getCallSeqId() {
        return callSeqId;
    }

    /**
     * Updates the call-sequence ID. Must be called <em>after</em> the response payload has been
     * fully prepared, so that a client retry with the same {@code callSeqId} can be detected.
     */
    public void setCallSeqId(int callSeqId) {
        this.callSeqId = callSeqId;
    }

    /** Resets the idle-TTL timer to the given wall-clock time. */
    public void updateLastAccessTime(long nowMs) {
        this.lastAccessTime = nowMs;
    }

    /**
     * Returns {@code true} if the session has been idle for longer than {@code ttlMs}, based on the
     * provided current time.
     */
    public boolean isExpired(long ttlMs, long nowMs) {
        return nowMs - lastAccessTime > ttlMs;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                iterator.close();
            } finally {
                try {
                    readOptions.close();
                } finally {
                    try {
                        rocksDBKv.getDb().releaseSnapshot(snapshot);
                        snapshot.close();
                    } finally {
                        resourceLease.close();
                    }
                }
            }
        }
    }
}
