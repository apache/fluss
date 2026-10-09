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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.utils.AbstractIterator;
import org.apache.fluss.utils.IOUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A container that holds the scanned Arrow batches per bucket for a particular table.
 *
 * <p>Each {@link ArrowBatchData} holds off-heap Arrow memory. Callers should use try-with-resources
 * on this container to ensure all batches are released if processing fails mid-iteration.
 */
@Internal
public class ArrowScanRecords implements Iterable<ArrowBatchData>, AutoCloseable {
    public static final ArrowScanRecords EMPTY = new ArrowScanRecords(Collections.emptyMap());

    private final Map<TableBucket, List<ArrowBatchData>> records;

    /** The exclusive upper bound of consumed offsets per polled bucket in this round. */
    private final Map<TableBucket, Long> consumedUpToOffsets;

    /** The bounded buckets that reached their stopping offsets in this poll round. */
    private final Set<TableBucket> finishedBuckets;

    public ArrowScanRecords(Map<TableBucket, List<ArrowBatchData>> records) {
        this(records, Collections.emptyMap());
    }

    public ArrowScanRecords(
            Map<TableBucket, List<ArrowBatchData>> records,
            Map<TableBucket, Long> consumedUpToOffsets) {
        this(records, consumedUpToOffsets, Collections.emptySet());
    }

    ArrowScanRecords(
            Map<TableBucket, List<ArrowBatchData>> records,
            Map<TableBucket, Long> consumedUpToOffsets,
            Set<TableBucket> finishedBuckets) {
        this.records = withProgressOrFinishedBuckets(records, consumedUpToOffsets, finishedBuckets);
        this.consumedUpToOffsets = consumedUpToOffsets;
        this.finishedBuckets = finishedBuckets;
    }

    /** Get just the Arrow batches for the given bucket. */
    public List<ArrowBatchData> records(TableBucket scanBucket) {
        List<ArrowBatchData> recs = records.get(scanBucket);
        if (recs == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(recs);
    }

    /**
     * Get the buckets that were polled in this round, including buckets whose batch list is empty
     * but whose log offset still advanced or whose bounded subscription finished.
     */
    public Set<TableBucket> buckets() {
        return Collections.unmodifiableSet(records.keySet());
    }

    /**
     * Get the exclusive upper bound of offsets consumed for the given bucket in this poll round.
     *
     * @param bucket the bucket to query
     * @return the exclusive upper bound offset, or {@code null} if the bucket was not polled in
     *     this round
     */
    @Nullable
    public Long consumedUpToOffset(TableBucket bucket) {
        return consumedUpToOffsets.get(bucket);
    }

    /** Returns the total number of rows in all batches. */
    public int count() {
        int count = 0;
        for (List<ArrowBatchData> recs : records.values()) {
            for (ArrowBatchData rec : recs) {
                count += rec.getRecordCount();
            }
        }
        return count;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    /**
     * Returns {@code true} if this {@code ArrowScanRecords} carries any scanner progress, either by
     * returning records, advancing a consumed offset, or completing a bounded subscription.
     */
    public boolean hasProgress() {
        return count() > 0 || !consumedUpToOffsets.isEmpty() || !finishedBuckets.isEmpty();
    }

    /**
     * Returns the bounded buckets that finished in this poll round, including empty ranges.
     *
     * <p>Each subscription reports completion only once. A completion event may accompany the final
     * records or arrive without any records. Callers must consume all records in this result before
     * treating the corresponding buckets as fully read.
     */
    public Set<TableBucket> finishedBuckets() {
        return Collections.unmodifiableSet(finishedBuckets);
    }

    /** Closes all Arrow batches held by this container, releasing off-heap memory. */
    @Override
    public void close() {
        for (List<ArrowBatchData> recs : records.values()) {
            for (ArrowBatchData rec : recs) {
                IOUtils.closeQuietly(rec);
            }
        }
    }

    @Override
    @Nonnull
    public Iterator<ArrowBatchData> iterator() {
        return new ConcatenatedIterable(records.values()).iterator();
    }

    /**
     * Ensures every bucket with a consumed offset or completion event has a (possibly empty) record
     * list entry, so that {@link #buckets()} surfaces progress-only and finished-only buckets.
     */
    private static Map<TableBucket, List<ArrowBatchData>> withProgressOrFinishedBuckets(
            Map<TableBucket, List<ArrowBatchData>> records,
            Map<TableBucket, Long> consumedUpToOffsets,
            Set<TableBucket> finishedBuckets) {
        if (records.keySet().containsAll(consumedUpToOffsets.keySet())
                && records.keySet().containsAll(finishedBuckets)) {
            return records;
        }

        Map<TableBucket, List<ArrowBatchData>> merged = new LinkedHashMap<>(records);
        for (TableBucket bucket : consumedUpToOffsets.keySet()) {
            merged.putIfAbsent(bucket, Collections.emptyList());
        }
        for (TableBucket bucket : finishedBuckets) {
            merged.putIfAbsent(bucket, Collections.emptyList());
        }
        return merged;
    }

    private static class ConcatenatedIterable implements Iterable<ArrowBatchData> {

        private final Iterable<? extends Iterable<ArrowBatchData>> iterables;

        private ConcatenatedIterable(Iterable<? extends Iterable<ArrowBatchData>> iterables) {
            this.iterables = iterables;
        }

        @Override
        @Nonnull
        public Iterator<ArrowBatchData> iterator() {
            return new AbstractIterator<ArrowBatchData>() {
                final Iterator<? extends Iterable<ArrowBatchData>> iters = iterables.iterator();
                Iterator<ArrowBatchData> current;

                public ArrowBatchData makeNext() {
                    while (current == null || !current.hasNext()) {
                        if (iters.hasNext()) {
                            current = iters.next().iterator();
                        } else {
                            return allDone();
                        }
                    }
                    return current.next();
                }
            };
        }
    }
}
