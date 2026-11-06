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

import io.github.fluss_contrib.rocksdb.Statistics;

import java.util.HashMap;
import java.util.Map;

/** Shares native statistics by table ID within one TabletServer. */
public final class RocksDBStatisticsRegistry {

    private final Map<Long, Entry> entries = new HashMap<>();

    /**
     * Acquires statistics that remain alive until the last RocksDB instance of the table releases
     * its lease.
     */
    public synchronized Lease acquire(long tableId) {
        Entry entry = entries.get(tableId);
        if (entry == null) {
            entry = new Entry(new Statistics());
            entries.put(tableId, entry);
        }
        entry.references++;
        return new Lease(tableId, entry);
    }

    /** An independently closeable reference to a table's shared statistics. */
    public final class Lease implements AutoCloseable {

        private final long tableId;
        private final Entry entry;
        private boolean closed;

        private Lease(long tableId, Entry entry) {
            this.tableId = tableId;
            this.entry = entry;
        }

        /** Returns the statistics borrowed for the lifetime of this lease. */
        public Statistics getStatistics() {
            return entry.statistics;
        }

        @Override
        public void close() {
            synchronized (RocksDBStatisticsRegistry.this) {
                if (!closed) {
                    closed = true;
                    if (--entry.references == 0) {
                        entries.remove(tableId);
                        entry.statistics.close();
                    }
                }
            }
        }
    }

    private static final class Entry {
        private final Statistics statistics;
        private int references;

        private Entry(Statistics statistics) {
            this.statistics = statistics;
        }
    }
}
