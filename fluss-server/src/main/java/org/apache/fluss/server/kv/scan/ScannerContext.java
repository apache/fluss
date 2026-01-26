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

import org.apache.fluss.server.kv.rocksdb.RocksDBKv;
import org.apache.fluss.utils.clock.Clock;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import javax.annotation.concurrent.NotThreadSafe;

/** The context for a scanner. */
@NotThreadSafe
public class ScannerContext implements AutoCloseable {
    private final RocksDBKv rocksDBKv;
    private final RocksIterator iterator;
    private final ReadOptions readOptions;
    private final Snapshot snapshot;
    private final long limit;

    private int callSeqId;
    private long lastAccessTime;
    private long rowsScanned;

    public ScannerContext(
            RocksDBKv rocksDBKv,
            RocksIterator iterator,
            ReadOptions readOptions,
            Snapshot snapshot,
            long limit,
            Clock clock) {
        this.rocksDBKv = rocksDBKv;
        this.iterator = iterator;
        this.readOptions = readOptions;
        this.snapshot = snapshot;
        this.limit = limit;
        this.callSeqId = 0;
        this.lastAccessTime = clock.milliseconds();
        this.rowsScanned = 0;
    }

    public RocksIterator getIterator() {
        return iterator;
    }

    public Snapshot getSnapshot() {
        return snapshot;
    }

    public long getLimit() {
        return limit;
    }

    public int getCallSeqId() {
        return callSeqId;
    }

    public void setCallSeqId(int callSeqId) {
        this.callSeqId = callSeqId;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void updateLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getRowsScanned() {
        return rowsScanned;
    }

    public void incrementRowsScanned(long count) {
        this.rowsScanned += count;
    }

    @Override
    public void close() {
        iterator.close();
        readOptions.close();
        rocksDBKv.releaseSnapshot(snapshot);
    }
}
