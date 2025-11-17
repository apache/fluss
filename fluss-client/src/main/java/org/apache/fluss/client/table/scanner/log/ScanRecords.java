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
public class ScanRecords<T> implements Iterable<ScanRecord<T>> {
    public static final <T> ScanRecords<T> empty() {
        return new ScanRecords<>(Collections.emptyMap());
    }

    private final Map<TableBucket, List<ScanRecord<T>>> records;

    public ScanRecords(Map<TableBucket, List<ScanRecord<T>>> records) {
        this.records = records;
    }

    /**
     * Get just the records for the given bucketId.
     *
     * @param scanBucket The bucket to get records for
     */
    public List<ScanRecord<T>> records(TableBucket scanBucket) {
        List<ScanRecord<T>> recs = records.get(scanBucket);
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
        for (List<ScanRecord<T>> recs : records.values()) {
            count += recs.size();
        }
        return count;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    @Override
    public Iterator<ScanRecord<T>> iterator() {
        return new ConcatenatedIterable<>(records.values()).iterator();
    }

    private static class ConcatenatedIterable<T> implements Iterable<ScanRecord<T>> {

        private final Iterable<? extends Iterable<ScanRecord<T>>> iterables;

        public ConcatenatedIterable(Iterable<? extends Iterable<ScanRecord<T>>> iterables) {
            this.iterables = iterables;
        }

        @Override
        public Iterator<ScanRecord<T>> iterator() {
            return new AbstractIterator<ScanRecord<T>>() {
                final Iterator<? extends Iterable<ScanRecord<T>>> iters = iterables.iterator();
                Iterator<ScanRecord<T>> current;

                public ScanRecord<T> makeNext() {
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
