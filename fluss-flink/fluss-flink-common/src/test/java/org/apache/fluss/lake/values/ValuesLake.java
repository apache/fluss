/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.fluss.metadata.Schema;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * In-memory implementation of a lake storage for testing purposes.
 *
 * <p>Provides utilities for managing tables, writing records, committing stages, and retrieving
 * results in a test environment.
 */
public class ValuesLake {
    private static final Logger LOG = LoggerFactory.getLogger(ValuesLake.class);

    private static final Map<String, ValuesTable> globalTables = MapUtils.newConcurrentHashMap();
    private static final Map<String, TableFailureController> FAILURE_CONTROLLERS =
            MapUtils.newConcurrentHashMap();

    public static TableFailureController failWhen(String tableId) {
        return FAILURE_CONTROLLERS.computeIfAbsent(tableId, k -> new TableFailureController());
    }

    public static void clearAllFailureControls() {
        FAILURE_CONTROLLERS.clear();
    }

    public static Schema getTableSchema(String tableId) {
        return globalTables.get(tableId).schema;
    }

    public static List<InternalRow> getResults(String tableId) {
        ValuesTable table = globalTables.get(tableId);
        checkNotNull(table, tableId + " does not exist");
        return table.getResult();
    }

    public static void writeRecord(String tableId, String stageId, LogRecord record)
            throws IOException {
        ValuesTable table = globalTables.get(tableId);
        checkNotNull(table, tableId + " does not exist");
        TableFailureController controller = FAILURE_CONTROLLERS.get(tableId);
        if (controller != null) {
            controller.checkWriteShouldFail(tableId);
        }
        table.writeRecord(stageId, record);
        LOG.info("Write record to stage {}: {}", stageId, record);
    }

    public static long commit(
            String tableId, List<String> stageIds, Map<String, String> snapshotProperties)
            throws IOException {
        ValuesTable table = globalTables.get(tableId);
        checkNotNull(table, "commit stage %s failed, table %s does not exist", stageIds, tableId);
        table.commit(stageIds, snapshotProperties);
        LOG.info("Commit table {} stage {}", tableId, stageIds);
        return table.getSnapshotId();
    }

    public static void abort(String tableId, List<String> stageIds) {
        ValuesTable table = globalTables.get(tableId);
        checkNotNull(
                table, "abort stage record %s failed, table %s does not exist", stageIds, tableId);
        table.abort(stageIds);
        LOG.info("Abort table {} stage {}", tableId, stageIds);
    }

    public static ValuesTable getTable(String tableId) {
        return globalTables.get(tableId);
    }

    public static void createTable(String tableId, Schema schema) {
        if (!globalTables.containsKey(tableId)) {
            globalTables.put(tableId, new ValuesTable(schema));
            ValuesTable table = globalTables.get(tableId);
            checkNotNull(table, "create table %s failed", tableId);
        }
    }

    public static void dropTable(String tableId) {
        globalTables.remove(tableId);
    }

    public static void truncateTable(String tableId) {
        ValuesTable table = globalTables.get(tableId);
        checkNotNull(table, "truncate table %s failed", tableId);
        table.truncateTable();
    }

    public static void clear() {
        globalTables.clear();
    }

    /** maintain the columns, primaryKeys and records of a specific table in memory. */
    public static class ValuesTable {

        private final Object lock;

        // [primaryKeys, rowValue]
        private final Map<String, InternalRow> records;
        private final List<InternalRow> logRecords;
        private final Map<String, List<Tuple2<String, LogRecord>>> stageRecords;
        private final Map<String, List<LogRecord>> stageLogRecords;
        private final Schema schema;

        private final List<Schema.Column> columns;

        private final List<String> primaryKeys;

        // indexes of primaryKeys in columns
        private final List<Integer> primaryKeyIndexes;

        private long snapshotId = 0L;

        private final Map<Long, Map<String, String>> snapshotProperties = new HashMap<>();

        public ValuesTable(Schema schema) {
            this.lock = new Object();
            this.records = new LinkedHashMap<>();
            this.logRecords = new ArrayList<>();
            this.stageRecords = new HashMap<>();
            this.stageLogRecords = new HashMap<>();
            this.schema = schema;
            this.columns = new ArrayList<>(schema.getColumns());
            this.primaryKeys = schema.getPrimaryKeyColumnNames();
            this.primaryKeyIndexes = new ArrayList<>();
            if (!primaryKeys.isEmpty()) {
                updatePrimaryKeyIndexes();
            }
        }

        private void updatePrimaryKeyIndexes() {
            checkArgument(!primaryKeys.isEmpty(), "primaryKeys couldn't be empty");
            primaryKeyIndexes.clear();
            for (String primaryKey : primaryKeys) {
                for (int i = 0; i < columns.size(); i++) {
                    if (columns.get(i).getName().equals(primaryKey)) {
                        primaryKeyIndexes.add(i);
                    }
                }
            }
            primaryKeyIndexes.sort(Comparator.naturalOrder());
        }

        public List<InternalRow> getResult() {
            List<InternalRow> results = new ArrayList<>();
            synchronized (lock) {
                if (primaryKeys.isEmpty()) {
                    results.addAll(logRecords);
                } else {
                    records.forEach((key, record) -> results.add(record));
                }
            }
            return results;
        }

        public void writeRecord(String stageId, LogRecord record) {
            synchronized (lock) {
                if (primaryKeys.isEmpty()) {
                    this.stageLogRecords
                            .computeIfAbsent(stageId, k -> new ArrayList<>())
                            .add(record);
                } else {
                    this.stageRecords
                            .computeIfAbsent(stageId, k -> new ArrayList<>())
                            .add(new Tuple2<>(buildPrimaryKeyStr(record.getRow()), record));
                }
            }
        }

        public void commit(List<String> stageIds, Map<String, String> snapshotProperties) {
            synchronized (lock) {
                if (primaryKeys.isEmpty()) {
                    for (String stageId : stageIds) {
                        List<LogRecord> stageRecords = this.stageLogRecords.get(stageId);
                        stageRecords.forEach(record -> logRecords.add(record.getRow()));
                        this.stageLogRecords.remove(stageId);
                    }
                } else {
                    for (String stageId : stageIds) {
                        List<Tuple2<String, LogRecord>> stageRecords =
                                this.stageRecords.get(stageId);
                        stageRecords.forEach(record -> records.put(record.f0, record.f1.getRow()));
                        this.stageRecords.remove(stageId);
                    }
                    this.snapshotId++;
                    this.snapshotProperties.put(this.snapshotId, snapshotProperties);
                }
            }
        }

        public void abort(List<String> stageIds) {
            synchronized (lock) {
                if (primaryKeys.isEmpty()) {
                    for (String stageId : stageIds) {
                        this.stageLogRecords.remove(stageId);
                    }
                } else {
                    for (String stageId : stageIds) {
                        this.stageRecords.remove(stageId);
                    }
                }
            }
        }

        private String buildPrimaryKeyStr(InternalRow row) {
            StringBuilder stringBuilder = new StringBuilder();
            for (Integer primaryKeyIndex : primaryKeyIndexes) {
                stringBuilder.append(row.getString(primaryKeyIndex).toString()).append(",");
            }
            stringBuilder.deleteCharAt(stringBuilder.length() - 1);
            return stringBuilder.toString();
        }

        public void truncateTable() {
            records.clear();
        }

        public long getSnapshotId() {
            return snapshotId;
        }

        public Map<String, String> getSnapshotProperties(long snapshotId) {
            return snapshotProperties.get(snapshotId);
        }

        public Map<String, String> currentSnapshotProperties() {
            return getSnapshotProperties(snapshotId);
        }
    }

    /** Controller to control the failure of table write and commit. */
    public static class TableFailureController {
        private volatile boolean writeFailEnabled = false;
        private final AtomicInteger writeFailTimes = new AtomicInteger(0);
        private final AtomicInteger writeFailCounter = new AtomicInteger(0);

        public TableFailureController failWriteOnce() {
            return failWriteNext(1);
        }

        /** Force the next N write calls to throw IOException (thread-safe). */
        public TableFailureController failWriteNext(int times) {
            this.writeFailEnabled = true;
            this.writeFailTimes.set(times);
            this.writeFailCounter.set(0);
            return this;
        }

        /** Fail only the next single write call. */
        public TableFailureController disableWriteFail() {
            this.writeFailEnabled = false;
            return this;
        }

        private void checkWriteShouldFail(String tableId) throws IOException {
            if (!writeFailEnabled) {
                return;
            }

            int count = writeFailCounter.incrementAndGet();
            if (count <= writeFailTimes.get()) {
                LOG.warn(
                        "ValuesLake FAIL_INJECTED: write() intentionally failed [table={} attempt={}/{}]",
                        tableId,
                        count,
                        writeFailTimes.get());
                throw new IOException(
                        String.format(
                                "ValuesLake write failure injected for test (attempt %d of %d)",
                                count, writeFailTimes.get()));
            } else {
                writeFailEnabled = false;
            }
        }
    }
}
