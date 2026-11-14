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

    public ArrowScanRecords(Map<TableBucket, List<ArrowBatchData>> records) {
        this(records, Collections.emptyMap());
    }

    public ArrowScanRecords(
            Map<TableBucket, List<ArrowBatchData>> records,
            Map<TableBucket, Long> consumedUpToOffsets) {
        this.records = records;
        this.consumedUpToOffsets = consumedUpToOffsets;
    }

    /** Get just the Arrow batches for the given bucket. */
    public List<ArrowBatchData> records(TableBucket scanBucket) {
        List<ArrowBatchData> recs = records.get(scanBucket);
        if (recs == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(recs);
    }

    /** Returns the buckets that were polled in this round. */
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
