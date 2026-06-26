/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.values;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.TableAlreadyExistException;
import org.apache.fluss.exception.TableNotExistException;
import org.apache.fluss.lake.lakestorage.LakeCatalog;
import org.apache.fluss.lake.lakestorage.LakeStorage;
import org.apache.fluss.lake.lakestorage.LakeStoragePlugin;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.Planner;
import org.apache.fluss.lake.source.RecordReader;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.GenericRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.CloseableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Test-only Paimon lake storage plugin for hybrid lake lookup tests. */
public class TestingPaimonLakeStoragePlugin implements LakeStoragePlugin {

    private static final Map<TablePath, List<LogRecord>> RECORDS_BY_TABLE =
            new ConcurrentHashMap<>();
    private static final Map<TablePath, AtomicInteger> PLANNED_LOOKUPS_BY_TABLE =
            new ConcurrentHashMap<>();

    public static void setRows(TablePath tablePath, List<InternalRow> rows) {
        List<LogRecord> records = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            records.add(new GenericRecord(i, 0, ChangeType.APPEND_ONLY, rows.get(i)));
        }
        RECORDS_BY_TABLE.put(tablePath, records);
        PLANNED_LOOKUPS_BY_TABLE.put(tablePath, new AtomicInteger());
    }

    public static int plannedLookups(TablePath tablePath) {
        AtomicInteger plannedLookups = PLANNED_LOOKUPS_BY_TABLE.get(tablePath);
        return plannedLookups == null ? 0 : plannedLookups.get();
    }

    public static void clear() {
        RECORDS_BY_TABLE.clear();
        PLANNED_LOOKUPS_BY_TABLE.clear();
    }

    @Override
    public String identifier() {
        return DataLakeFormat.PAIMON.toString();
    }

    @Override
    public LakeStorage createLakeStorage(Configuration configuration) {
        return new TestingPaimonLakeStorage();
    }

    private static class TestingPaimonLakeStorage implements LakeStorage {
        @Override
        public LakeTieringFactory<?, ?> createLakeTieringFactory() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public LakeCatalog createLakeCatalog() {
            return new TestingPaimonLakeCatalog();
        }

        @Override
        public LakeSource<?> createLakeSource(TablePath tablePath) {
            return new TestingPaimonLakeSource(tablePath);
        }
    }

    private static class TestingPaimonLakeCatalog implements LakeCatalog {
        @Override
        public void createTable(
                TablePath tablePath, TableDescriptor tableDescriptor, Context context)
                throws TableAlreadyExistException {}

        @Override
        public void alterTable(TablePath tablePath, List<TableChange> tableChanges, Context context)
                throws TableNotExistException {}
    }

    private static class TestingPaimonLakeSource implements LakeSource<LakeSplit> {
        private final TablePath tablePath;

        private TestingPaimonLakeSource(TablePath tablePath) {
            this.tablePath = tablePath;
        }

        @Override
        public void withProject(int[][] project) {}

        @Override
        public void withLimit(int limit) {}

        @Override
        public FilterPushDownResult withFilters(List<Predicate> predicates) {
            return FilterPushDownResult.of(predicates, Collections.emptyList());
        }

        @Override
        public Planner<LakeSplit> createPlanner(PlannerContext context) {
            return () -> {
                PLANNED_LOOKUPS_BY_TABLE
                        .computeIfAbsent(tablePath, ignored -> new AtomicInteger())
                        .incrementAndGet();
                return Collections.singletonList(new TestingPaimonSplit());
            };
        }

        @Override
        public RecordReader createRecordReader(ReaderContext<LakeSplit> context) {
            return new RecordReader() {
                @Override
                public CloseableIterator<LogRecord> read() throws IOException {
                    return CloseableIterator.wrap(
                            RECORDS_BY_TABLE
                                    .getOrDefault(tablePath, Collections.emptyList())
                                    .iterator());
                }
            };
        }

        @Override
        public SimpleVersionedSerializer<LakeSplit> getSplitSerializer() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }

    private static class TestingPaimonSplit implements LakeSplit {
        @Override
        public int bucket() {
            return 0;
        }

        @Override
        public List<String> partition() {
            return Collections.emptyList();
        }
    }
}
