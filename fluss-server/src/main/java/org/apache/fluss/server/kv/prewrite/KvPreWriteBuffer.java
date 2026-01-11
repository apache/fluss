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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.metrics.Counter;
import org.apache.fluss.server.kv.KvBatchWriter;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.utils.MurmurHashUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import static org.apache.fluss.utils.UnsafeUtils.BYTE_ARRAY_BASE_OFFSET;

/**
 * An in-memory pre-write buffer for putting kv records. The kv records will first be put into the
 * buffer and then be flushed to the underlying kv storage by method {@link #flush(long)}.
 *
 * <p>In Fluss, when putting a key-value pair, Fluss will first write WAL first. Only when the WAL
 * has been persisted(with fault tolerance), can Fluss safely writing the key-value pair to the
 * underlying kv storage. Otherwise, it'll cause in-consistent data in the kv storage.
 *
 * <p>For example, if Fluss write data to the kv storage without waiting for the WAL to be
 * persisted, then user can read the data from kv storage. But unfortunately, the kv storage was
 * lost. Then, Fluss can never restore the piece of data to kv storage from the WAL as it hasn't
 * been persisted, which will cause user can not read the data any more although the data has been
 * ever read.
 *
 * <p>To solve this problem, we introduce the pre-write buffer. It's mainly designed for two
 * purpose:
 *
 * <ol>
 *   <li>Buffer all the key-value pairs that are waiting for the corresponding WAL to be persisted.
 *       And flush these key-value pairs whose WAL has been persisted to underlying kv storage.
 *   <li>A temporary in-memory key-value buffer for put/get a key. Since Fluss will lookup the
 *       previous written data to generate CDC as WAL, it needs a buffer to buffer the data been
 *       written before but is still waiting for the WAL to be persisted before flush to underlying
 *       kv storage.
 * </ol>
 *
 * <p>In implementation, to achieve the above two purposes, it maintains a map kvEntryMap for
 * put/get a key and a linked list kvEntryList to iterate the kv entries to be flushed.
 *
 * <p>The kvEntryMap is a map from key to the corresponding KvEntry. Each KvEntry wraps a key-value
 * pair and a log sequence number corresponding to the offset of the WAL, with which, we can find
 * the key-value pair to be flush when a perice of WAL was persisted.
 *
 * <p>When put a key-value pair, it will always create a new KvEntry wrapping the key-value and put
 * it into the map.
 *
 * <p>Then it will append the new KvEntry to the tail of the kvEntryList. So, the kvEntryList
 * maintains the key-value pair in putting order. When flushing, it will iterate the list to flush
 * all the entries whose log sequence number is less or equal than the given log sequence number.
 *
 * <p>Note: The key-value pairs to be put into the buffer must be with non-decreasing log sequence
 * number. Otherwise, the flushing will not work as expected since once it found any kv entry whose
 * log sequence number is greater or equal than the given log sequence number whiling iterating from
 * head to tail, it will stop flush.
 */
@NotThreadSafe
public class KvPreWriteBuffer implements AutoCloseable {
    private final KvBatchWriter kvBatchWriter;

    // a mapping from the key to the kv-entry
    private final Map<Key, KvEntry> kvEntryMap = new HashMap<>();

    // a linked list for all kv entries
    private final LinkedList<KvEntry> allKvEntries = new LinkedList<>();

    // metrics related.
    private final Counter truncateAsDuplicatedCount;
    private final Counter truncateAsErrorCount;

    // the max LSN in the buffer
    private long maxLogSequenceNumber = -1;

    public KvPreWriteBuffer(
            KvBatchWriter kvBatchWriter, TabletServerMetricGroup serverMetricGroup) {
        this.kvBatchWriter = kvBatchWriter;

        truncateAsDuplicatedCount = serverMetricGroup.kvTruncateAsDuplicatedCount();
        truncateAsErrorCount = serverMetricGroup.kvTruncateAsErrorCount();
    }

    /**
     * Delete a key-value pair with the given key.
     *
     * @param logSequenceNumber the log sequence number for the delete operation
     */
    public void delete(Key key, long logSequenceNumber) {
        update(key, Value.of(null), logSequenceNumber);
    }

    /**
     * Put a key-value pair.
     *
     * @param logSequenceNumber the log sequence number for the put operation
     */
    public void put(Key key, @Nullable byte[] value, long logSequenceNumber) {
        update(key, Value.of(value), logSequenceNumber);
    }

    private void update(Key key, Value value, long lsn) {
        if (maxLogSequenceNumber >= lsn) {
            throw new IllegalArgumentException(
                    "The log sequence number must be non-decreasing. "
                            + "The current log sequence number is "
                            + maxLogSequenceNumber
                            + ", but the new log sequence number is "
                            + lsn);
        }

        // create the kv entry with previous pointer if exists, and put the new entry to the map
        KvEntry kvEntry =
                kvEntryMap.compute(
                        key,
                        (k, v) ->
                                v == null
                                        ? KvEntry.of(key, value, lsn)
                                        : KvEntry.of(key, value, lsn, v));
        // append the entry to the tail of the list for all kv entries
        allKvEntries.addLast(kvEntry);
        // update the max lsn
        maxLogSequenceNumber = lsn;
    }

    /**
     * Return a value with the given key.
     *
     * @return A value wrapping a null byte array if the key is marked as deleted; null if any
     *     key-value pair can be found by the key in the buffer.
     */
    public @Nullable Value get(Key key) {
        KvEntry kvEntry = kvEntryMap.get(key);

        return kvEntry == null ? null : kvEntry.getValue();
    }

    /**
     * Truncate the buffer to the given log sequence number so that it only contains key-value pairs
     * whose log sequence number is less than the given log sequence number.
     *
     * @param targetLogSequenceNumber the lower bound of the log sequence number truncated to.
     * @param truncateReason the reason to truncate
     */
    public void truncateTo(long targetLogSequenceNumber, TruncateReason truncateReason) {
        if (truncateReason == TruncateReason.DUPLICATED) {
            truncateAsDuplicatedCount.inc();
        } else {
            truncateAsErrorCount.inc();
        }

        Iterator<KvEntry> descIter = allKvEntries.descendingIterator();
        while (descIter.hasNext()) {
            KvEntry entry = descIter.next();
            if (entry.getLogSequenceNumber() < targetLogSequenceNumber) {
                maxLogSequenceNumber = entry.logSequenceNumber;
                break;
            }
            descIter.remove();
            boolean removed = kvEntryMap.remove(entry.getKey(), entry);
            // if the latest entry is removed, we need to rollback the previous entry to the map
            if (removed && entry.previousEntry != null) {
                kvEntryMap.put(entry.getKey(), entry.previousEntry);
            }
        }
        if (!descIter.hasNext()) {
            maxLogSequenceNumber = -1;
        }
    }

    /**
     * To flush the key-value pairs whose sequence number is less than the given sequence number.
     *
     * @param exclusiveUpToLogSequenceNumber the exclusive upper bound of the log sequence number to
     *     be flushed
     */
    public void flush(long exclusiveUpToLogSequenceNumber) throws IOException {
        int flushedCount = 0;
        for (Iterator<KvEntry> it = allKvEntries.iterator(); it.hasNext(); ) {
            KvEntry entry = it.next();
            // if find one entry whose sequence number is greater than the given sequence number,
            // break the loop
            if (entry.getLogSequenceNumber() >= exclusiveUpToLogSequenceNumber) {
                break;
            }

            // first remove the entry from the list
            it.remove();

            // then write data using write batch writer
            Value value = entry.getValue();
            if (value.value != null) {
                flushedCount += 1;
                kvBatchWriter.put(entry.getKey().key, value.value);
            } else {
                flushedCount += 1;
                kvBatchWriter.delete(entry.getKey().key);
            }

            // if the kv entry to be flushed is equal to the one in the kvEntryMap, we
            // can remove it from the map. Although it's not a must to remove from the map,
            // we remove it to reduce the memory usage
            kvEntryMap.remove(entry.getKey(), entry);
        }
        // flush to underlying kv tablet
        if (flushedCount > 0) {
            kvBatchWriter.flush();
        }
    }

    @VisibleForTesting
    public Map<Key, KvEntry> getKvEntryMap() {
        return kvEntryMap;
    }

    @VisibleForTesting
    public LinkedList<KvEntry> getAllKvEntries() {
        return allKvEntries;
    }

    @VisibleForTesting
    public long getMaxLSN() {
        return maxLogSequenceNumber;
    }

    @Override
    public void close() throws Exception {
        if (kvBatchWriter != null) {
            kvBatchWriter.close();
        }
    }

    public Counter getTruncateAsDuplicatedCount() {
        return truncateAsDuplicatedCount;
    }

    public Counter getTruncateAsErrorCount() {
        return truncateAsErrorCount;
    }

    /**
     * A class to wrap a key-value pair and the sequence number for the key-value pair. If the byte
     * array in the value is null, it means the key in the entry is marked as deleted.
     *
     * <p>The log sequence number is to represent the log offset in the corresponding WAL for the
     * key-value pair.
     */
    public static class KvEntry {

        private final Key key;
        private final Value value;
        private final long logSequenceNumber;

        // the previous mapped value in the buffer before this key-value put
        @Nullable private final KvEntry previousEntry;

        public static KvEntry of(Key key, Value value, long sequenceNumber) {
            return new KvEntry(key, value, sequenceNumber, null);
        }

        public static KvEntry of(Key key, Value value, long sequenceNumber, KvEntry previousEntry) {
            return new KvEntry(key, value, sequenceNumber, previousEntry);
        }

        private KvEntry(
                Key key, Value value, long logSequenceNumber, @Nullable KvEntry previousEntry) {
            this.key = key;
            this.value = value;
            this.logSequenceNumber = logSequenceNumber;
            this.previousEntry = previousEntry;
        }

        public Key getKey() {
            return key;
        }

        public Value getValue() {
            return value;
        }

        long getLogSequenceNumber() {
            return logSequenceNumber;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KvEntry kvEntry = (KvEntry) o;
            return logSequenceNumber == kvEntry.logSequenceNumber
                    && Objects.equals(key, kvEntry.key)
                    && Objects.equals(value, kvEntry.value)
                    && Objects.equals(previousEntry, kvEntry.previousEntry);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value, logSequenceNumber, previousEntry);
        }

        @Override
        public String toString() {
            return "KvEntry{"
                    + "key="
                    + key
                    + ", value="
                    + value
                    + ", lsn="
                    + logSequenceNumber
                    + ", previous="
                    + previousEntry
                    + '}';
        }
    }

    /** A key wrapper to wrap a byte array with overriding the hashCode and equals method. */
    public static class Key {
        private final byte[] key;

        // Currently, in our design, the Key is always created for putting to a map, or getting from
        // a map, which means the hash code for the Key will always be calculated.
        // So, in here, we calculate the hash code eagerly for the key.
        private final int hashCode;

        public static Key of(byte[] key) {
            return new Key(key);
        }

        private Key(byte[] key) {
            this.key = key;
            this.hashCode =
                    MurmurHashUtils.hashUnsafeBytes(key, BYTE_ARRAY_BASE_OFFSET, key.length);
        }

        public byte[] get() {
            return key;
        }

        @Override
        public int hashCode() {
            return hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Key that = (Key) o;

            // first compare hash code, if hash code is not equal,
            // it must be not equal
            if (this.hashCode != that.hashCode) {
                return false;
            }

            // then, compare the key
            // we use MemorySegment to compare the key since it's faster
            // than Arrays.equals
            MemorySegment s1 = MemorySegment.wrap(key);
            MemorySegment s2 = MemorySegment.wrap(that.key);
            return key.length == that.key.length && s1.equalTo(s2, 0, 0, key.length);
        }

        @Override
        public String toString() {
            return "[" + Base64.getEncoder().encodeToString(key) + "]";
        }
    }

    /**
     * A wrapper class to wrap a byte array value. If the wrapping byte array is null, it means the
     * {@link KvEntry} with the value is for key deletion.
     */
    public static class Value {
        private final @Nullable byte[] value;

        private Value(@Nullable byte[] value) {
            this.value = value;
        }

        public static Value of(@Nullable byte[] value) {
            return new Value(value);
        }

        /** Return the value. Return null if marked as deleted. */
        @Nullable
        public byte[] get() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Value value1 = (Value) o;
            return Arrays.equals(value, value1.value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }

        @Override
        public String toString() {
            return value == null ? "null" : "[" + Base64.getEncoder().encodeToString(value) + "]";
        }
    }

    /** The reason why we truncate the kv pre-write buffer. */
    public enum TruncateReason {
        DUPLICATED,
        ERROR,
    }
}
