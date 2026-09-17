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

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ArrowIpcBatch;
import org.apache.fluss.utils.AbstractIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A container that holds the scanned Arrow batches per bucket for a particular table.
 *
 * <p>Batches own immutable heap-backed IPC bytes and remain valid after the scanner closes. This
 * container does not require closing.
 */
@PublicEvolving
public class ArrowScanRecords implements Iterable<ArrowIpcBatch> {
    public static final ArrowScanRecords EMPTY = new ArrowScanRecords(Collections.emptyMap());

    private final Map<TableBucket, List<ArrowIpcBatch>> records;

    /** The exclusive upper bound of consumed offsets per polled bucket in this round. */
    private final Map<TableBucket, Long> consumedUpToOffsets;

    public ArrowScanRecords(Map<TableBucket, List<ArrowIpcBatch>> records) {
        this(records, Collections.emptyMap());
    }

    public ArrowScanRecords(
            Map<TableBucket, List<ArrowIpcBatch>> records,
            Map<TableBucket, Long> consumedUpToOffsets) {
        Map<TableBucket, List<ArrowIpcBatch>> batches = new LinkedHashMap<>();
        records.forEach(
                (bucket, values) ->
                        batches.put(bucket, Collections.unmodifiableList(new ArrayList<>(values))));
        consumedUpToOffsets
                .keySet()
                .forEach(bucket -> batches.putIfAbsent(bucket, Collections.emptyList()));
        this.records = Collections.unmodifiableMap(batches);
        this.consumedUpToOffsets =
                Collections.unmodifiableMap(new LinkedHashMap<>(consumedUpToOffsets));
    }

    /** Get just the Arrow batches for the given bucket. */
    public List<ArrowIpcBatch> records(TableBucket scanBucket) {
        List<ArrowIpcBatch> recs = records.get(scanBucket);
        if (recs == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(recs);
    }

    /** Returns the polled buckets, including buckets carrying only offset progress. */
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
        for (List<ArrowIpcBatch> recs : records.values()) {
            for (ArrowIpcBatch rec : recs) {
                count += rec.getRecordCount();
            }
        }
        return count;
    }

    /** Returns whether this result contains no rows, even if it carries offset progress. */
    public boolean isEmpty() {
        return count() == 0;
    }

    /**
     * Returns whether this result contains rows or consumed offsets, including empty log batches.
     */
    public boolean hasProgress() {
        return !isEmpty() || !consumedUpToOffsets.isEmpty();
    }

    @Override
    @Nonnull
    public Iterator<ArrowIpcBatch> iterator() {
        return new ConcatenatedIterable(records.values()).iterator();
    }

    private static class ConcatenatedIterable implements Iterable<ArrowIpcBatch> {

        private final Iterable<? extends Iterable<ArrowIpcBatch>> iterables;

        private ConcatenatedIterable(Iterable<? extends Iterable<ArrowIpcBatch>> iterables) {
            this.iterables = iterables;
        }

        @Override
        @Nonnull
        public Iterator<ArrowIpcBatch> iterator() {
            return new AbstractIterator<ArrowIpcBatch>() {
                final Iterator<? extends Iterable<ArrowIpcBatch>> iters = iterables.iterator();
                Iterator<ArrowIpcBatch> current;

                public ArrowIpcBatch makeNext() {
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
