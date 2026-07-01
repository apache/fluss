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
import org.apache.fluss.client.table.scanner.MultiTableRecord;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.AbstractIterator;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Container for {@link MultiTableRecord}s produced by {@link MultiTableLogScanner#poll(Duration)}.
 * Records are organized by {@link TablePath} and {@link TableBucket}.
 *
 * @since 0.7
 */
@PublicEvolving
public class MultiTableRecords implements Iterable<MultiTableRecord> {

    public static final MultiTableRecords EMPTY = new MultiTableRecords(Collections.emptyMap());

    private final Map<TablePath, Map<TableBucket, List<MultiTableRecord>>> records;

    public MultiTableRecords(Map<TablePath, Map<TableBucket, List<MultiTableRecord>>> records) {
        this.records = records;
    }

    /** All tables with records in this result set. */
    public Set<TablePath> tablePaths() {
        return Collections.unmodifiableSet(records.keySet());
    }

    /** All buckets with records for a given table. */
    public Set<TableBucket> buckets(TablePath tablePath) {
        Map<TableBucket, List<MultiTableRecord>> byBucket = records.get(tablePath);
        if (byBucket == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(byBucket.keySet());
    }

    /** Records for all buckets of a given table. */
    public List<MultiTableRecord> records(TablePath tablePath) {
        Map<TableBucket, List<MultiTableRecord>> byBucket = records.get(tablePath);
        if (byBucket == null || byBucket.isEmpty()) {
            return Collections.emptyList();
        }
        // Preserve insertion order across buckets.
        java.util.ArrayList<MultiTableRecord> all = new java.util.ArrayList<>();
        for (List<MultiTableRecord> list : byBucket.values()) {
            all.addAll(list);
        }
        return Collections.unmodifiableList(all);
    }

    /** Records for a specific bucket of a given table. */
    public List<MultiTableRecord> records(TablePath tablePath, TableBucket bucket) {
        Map<TableBucket, List<MultiTableRecord>> byBucket = records.get(tablePath);
        if (byBucket == null) {
            return Collections.emptyList();
        }
        List<MultiTableRecord> list = byBucket.get(bucket);
        if (list == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(list);
    }

    /** Total number of records across all tables. */
    public int count() {
        int count = 0;
        for (Map<TableBucket, List<MultiTableRecord>> byBucket : records.values()) {
            for (List<MultiTableRecord> list : byBucket.values()) {
                count += list.size();
            }
        }
        return count;
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    /** Iterate over every record across all tables. */
    @Override
    public Iterator<MultiTableRecord> iterator() {
        // Flatten the nested structure for iteration while keeping insertion order stable.
        Set<List<MultiTableRecord>> flat = new LinkedHashSet<>();
        for (Map<TableBucket, List<MultiTableRecord>> byBucket : records.values()) {
            flat.addAll(byBucket.values());
        }
        return new ConcatenatedIterable(flat).iterator();
    }

    /**
     * A mutable builder for {@link MultiTableRecords}; not part of the public API surface, used
     * internally by the scanner.
     */
    public static final class Builder {
        private final Map<TablePath, Map<TableBucket, List<MultiTableRecord>>> records =
                new LinkedHashMap<>();

        public Builder add(TablePath tablePath, TableBucket bucket, MultiTableRecord record) {
            records.computeIfAbsent(tablePath, k -> new LinkedHashMap<>())
                    .computeIfAbsent(bucket, k -> new java.util.ArrayList<>())
                    .add(record);
            return this;
        }

        public boolean isEmpty() {
            return records.isEmpty();
        }

        public MultiTableRecords build() {
            if (records.isEmpty()) {
                return EMPTY;
            }
            return new MultiTableRecords(records);
        }
    }

    private static class ConcatenatedIterable implements Iterable<MultiTableRecord> {

        private final Iterable<? extends Iterable<MultiTableRecord>> iterables;

        ConcatenatedIterable(Iterable<? extends Iterable<MultiTableRecord>> iterables) {
            this.iterables = iterables;
        }

        @Override
        public Iterator<MultiTableRecord> iterator() {
            return new AbstractIterator<MultiTableRecord>() {
                final Iterator<? extends Iterable<MultiTableRecord>> iters = iterables.iterator();
                Iterator<MultiTableRecord> current;

                public MultiTableRecord makeNext() {
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
