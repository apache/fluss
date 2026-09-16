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

package org.apache.fluss.server.kv.prewrite;

import org.apache.fluss.annotation.Internal;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * TabletServer-wide memory ledger shared by all KV pre-write buffers. It atomically tracks the
 * total estimated memory usage and the total number of entries held across all buffers, and serves
 * as the single source of truth for both the exposed metrics and future backpressure decisions.
 *
 * <p>Each buffer reports its accounting deltas to this ledger on the write path through {@link
 * #add} and {@link #subtract}, so a metric read is a single atomic snapshot instead of a sum of
 * non-atomic samples collected from multiple buffers.
 *
 * <p>The memory usage covers the key/value payload bytes plus a per-entry object overhead
 * approximation, reflecting the real retained heap of the buffered entries.
 */
@Internal
@ThreadSafe
public final class KvPreWriteBufferMemoryLedger {

    private final AtomicLong memoryUsageBytes = new AtomicLong();

    private final AtomicLong entryCount = new AtomicLong();

    /**
     * Adds the given amount of memory usage and entries to the ledger. Called when entries are
     * appended to a pre-write buffer.
     */
    public void add(long memoryBytes, int entries) {
        checkArgument(memoryBytes >= 0, "The added memory bytes must not be negative.");
        checkArgument(entries >= 0, "The added entry count must not be negative.");
        memoryUsageBytes.addAndGet(memoryBytes);
        entryCount.addAndGet(entries);
    }

    /**
     * Subtracts the given amount of memory usage and entries from the ledger. Called when entries
     * leave a pre-write buffer by flushing or truncation, or when a buffer is closed.
     */
    public void subtract(long memoryBytes, int entries) {
        checkArgument(memoryBytes >= 0, "The subtracted memory bytes must not be negative.");
        checkArgument(entries >= 0, "The subtracted entry count must not be negative.");
        memoryUsageBytes.addAndGet(-memoryBytes);
        entryCount.addAndGet(-entries);
    }

    /**
     * Returns the total estimated memory usage across all pre-write buffers in bytes, including the
     * key/value payload bytes and the per-entry object overhead. This is an approximation for
     * observability purposes, not an exact measurement.
     */
    public long memoryUsageBytes() {
        return memoryUsageBytes.get();
    }

    /** Returns the total number of entries held across all pre-write buffers. */
    public long entryCount() {
        return entryCount.get();
    }
}
