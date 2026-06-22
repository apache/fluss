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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.AbstractIterator;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A container that holds the list {@link ScanRecord} per bucket for a particular table. There is
 * one {@link ScanRecord} list for every bucket returned by a {@link
 * LogScanner#poll(java.time.Duration)} operation.
 *
 * @since 0.1
 */
@PublicEvolving
public class ScanRecords implements Iterable<ScanRecord> {
    public static final ScanRecords EMPTY = new ScanRecords(Collections.emptyMap());

    private final Map<TableBucket, List<ScanRecord>> records;

    /** The exclusive upper bound of consumed offsets per polled bucket in this round. */
    private final Map<TableBucket, Long> consumedUpToOffsets;

    public ScanRecords(Map<TableBucket, List<ScanRecord>> records) {
        this(records, Collections.emptyMap());
    }

    public ScanRecords(
            Map<TableBucket, List<ScanRecord>> records,
            Map<TableBucket, Long> consumedUpToOffsets) {
        this.records = records;
        this.consumedUpToOffsets = consumedUpToOffsets;
    }

    /**
     * Get just the records for the given bucketId.
     *
     * @param scanBucket The bucket to get records for
     */
    public List<ScanRecord> records(TableBucket scanBucket) {
        List<ScanRecord> recs = records.get(scanBucket);
        if (recs == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(recs);
    }

    /**
     * Get the bucket ids that were polled in this round, including buckets whose record list is
     * empty but whose log offset still advanced.
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

    /** The number of records for all buckets. */
    public int count() {
        int count = 0;
        for (List<ScanRecord> recs : records.values()) {
            count += recs.size();
        }
        return count;
    }

    /** Returns {@code true} if this {@code ScanRecords} contains no materialized records. */
    public boolean isEmpty() {
        return count() == 0;
    }

    @Override
    public Iterator<ScanRecord> iterator() {
        return new ConcatenatedIterable(records.values()).iterator();
    }

    private static class ConcatenatedIterable implements Iterable<ScanRecord> {

        private final Iterable<? extends Iterable<ScanRecord>> iterables;

        public ConcatenatedIterable(Iterable<? extends Iterable<ScanRecord>> iterables) {
            this.iterables = iterables;
        }

        @Override
        public Iterator<ScanRecord> iterator() {
            return new AbstractIterator<ScanRecord>() {
                final Iterator<? extends Iterable<ScanRecord>> iters = iterables.iterator();
                Iterator<ScanRecord> current;

                public ScanRecord makeNext() {
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
