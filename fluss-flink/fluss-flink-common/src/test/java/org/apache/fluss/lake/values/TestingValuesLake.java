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

import org.apache.fluss.record.LogRecord;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.utils.MapUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/**
 * An in-memory lake storage implementation for testing, supporting only log tables.
 *
 * <p>Provides utilities for managing tables, writing records, committing stages, and retrieving
 * results in a test environment.
 */
public class TestingValuesLake {
    private static final Logger LOG = LoggerFactory.getLogger(TestingValuesLake.class);

    private static final Map<String, TestingValuesTable> globalTables =
            MapUtils.newConcurrentHashMap();
    private static final Map<String, TableFailureController> FAILURE_CONTROLLERS =
            MapUtils.newConcurrentHashMap();

    public static TableFailureController failWhen(String tableId) {
        return FAILURE_CONTROLLERS.computeIfAbsent(tableId, k -> new TableFailureController());
    }

    public static List<InternalRow> getResults(String tableId) {
        TestingValuesTable table = globalTables.get(tableId);
        checkNotNull(table, tableId + " does not exist");
        return table.getResult();
    }

    public static void writeRecord(String tableId, String stageId, LogRecord record)
            throws IOException {
        TestingValuesTable table = globalTables.get(tableId);
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
        TestingValuesTable table = globalTables.get(tableId);
        checkNotNull(table, "commit stage %s failed, table %s does not exist", stageIds, tableId);
        table.commit(stageIds, snapshotProperties);
        LOG.info("Commit table {} stage {}", tableId, stageIds);
        return table.getSnapshotId();
    }

    public static void abort(String tableId, List<String> stageIds) {
        TestingValuesTable table = globalTables.get(tableId);
        checkNotNull(
                table, "abort stage record %s failed, table %s does not exist", stageIds, tableId);
        table.abort(stageIds);
        LOG.info("Abort table {} stage {}", tableId, stageIds);
    }

    public static TestingValuesTable getTable(String tableId) {
        return globalTables.get(tableId);
    }

    public static void createTable(String tableId) {
        if (!globalTables.containsKey(tableId)) {
            globalTables.put(tableId, new TestingValuesTable());
            TestingValuesTable table = globalTables.get(tableId);
            checkNotNull(table, "create table %s failed", tableId);
        }
    }

    /** maintain the columns, primaryKeys and records of a specific table in memory. */
    public static class TestingValuesTable {

        private final Lock lock = new ReentrantLock();

        private final List<InternalRow> records;
        private final Map<String, List<LogRecord>> stageRecords;

        private long snapshotId = -1L;

        public TestingValuesTable() {
            this.records = new ArrayList<>();
            this.stageRecords = new HashMap<>();
        }

        public List<InternalRow> getResult() {
            return inLock(lock, () -> new ArrayList<>(records));
        }

        public void writeRecord(String stageId, LogRecord record) {
            inLock(
                    lock,
                    () -> {
                        this.stageRecords
                                .computeIfAbsent(stageId, k -> new ArrayList<>())
                                .add(record);
                    });
        }

        public void commit(List<String> stageIds, Map<String, String> snapshotProperties) {
            inLock(
                    lock,
                    () -> {
                        for (String stageId : stageIds) {
                            List<LogRecord> stageRecords = this.stageRecords.get(stageId);
                            stageRecords.forEach(record -> records.add(record.getRow()));
                            this.stageRecords.remove(stageId);
                        }
                        this.snapshotId++;
                    });
        }

        public void abort(List<String> stageIds) {
            inLock(
                    lock,
                    () -> {
                        for (String stageId : stageIds) {
                            this.stageRecords.remove(stageId);
                        }
                    });
        }

        public long getSnapshotId() {
            return snapshotId;
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
