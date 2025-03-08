/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.utils.AbstractIterator;

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

    public ScanRecords(Map<TableBucket, List<ScanRecord>> records) {
        this.records = records;
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
     * Get the bucket ids which have records contained in this record set.
     *
     * @return the set of partitions with data in this record set (maybe empty if no data was
     *     returned)
     */
    public Set<TableBucket> buckets() {
        return Collections.unmodifiableSet(records.keySet());
    }

    /** The number of records for all buckets. */
    public int count() {
        int count = 0;
        for (List<ScanRecord> recs : records.values()) {
            count += recs.size();
        }
        return count;
    }

    public boolean isEmpty() {
        return records.isEmpty();
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
