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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.arrow.ArrowBatch;
import org.apache.fluss.client.arrow.ArrowBatchReader;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ArrowIpcBatch;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.types.DataTypes;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;

import static org.apache.fluss.record.TestData.DATA1_SCHEMA;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the Arrow Java adapter with live log scanning. */
public class ArrowLogScannerITCase extends ClientToServerITCaseBase {
    @Test
    void testScannerCloseDoesNotInvalidateBatchesOrAllocator() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "arrow_batch_lifetime");
        createTable(
                tablePath,
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .build(),
                false);
        try (Table table = conn.getTable(tablePath);
                BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                LogScanner scanner = table.newScan().createLogScanner()) {
            AppendWriter writer = table.newAppend().createWriter();
            writer.append(row(7, "retained")).get();
            writer.flush();
            scanner.subscribeFromBeginning(0);
            ArrowScanRecords records;
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            do {
                assertThat(System.nanoTime()).isLessThan(deadline);
                records = scanner.pollRecordBatch(Duration.ofSeconds(1));
            } while (records.isEmpty());
            ArrowIpcBatch ipc = records.iterator().next();
            assertThat(ipc.getRecordBatch().isReadOnly()).isTrue();
            assertThat(ipc.getSchema().isReadOnly()).isTrue();
            try (ArrowBatch decoded = ArrowBatchReader.read(ipc, allocator)) {
                scanner.close();
                assertThat(((IntVector) decoded.getVectorSchemaRoot().getVector(0)).get(0))
                        .isEqualTo(7);
                try (ArrowBatch decodedAgain = ArrowBatchReader.read(ipc, allocator)) {
                    assertThat(
                                    decodedAgain
                                            .getVectorSchemaRoot()
                                            .getVector(1)
                                            .getObject(0)
                                            .toString())
                            .isEqualTo("retained");
                }
            }
            assertThat(allocator.getAllocatedMemory()).isZero();
            try (ArrowBatch decodedAgain = ArrowBatchReader.read(ipc, allocator)) {
                assertThat(decodedAgain.getRecordCount()).isEqualTo(1);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPollArrowBatchesWithReorderedProjection(boolean primaryKey) throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_arrow_projection_" + primaryKey);
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.STRING());
        if (primaryKey) {
            schema.primaryKey("a");
        }
        createTable(
                tablePath,
                TableDescriptor.builder()
                        .schema(schema.build())
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .build(),
                false);

        try (Table table = conn.getTable(tablePath)) {
            if (primaryKey) {
                UpsertWriter writer = table.newUpsert().createWriter();
                for (int i = 0; i < 3; i++) {
                    writer.upsert(row(i, "b-" + i, "c-" + i)).get();
                }
                writer.flush();
            } else {
                AppendWriter writer = table.newAppend().createWriter();
                for (int i = 0; i < 3; i++) {
                    writer.append(row(i, "b-" + i, "c-" + i));
                }
                writer.flush();
            }

            try (LogScanner scanner =
                    table.newScan().project(new int[] {2, 1}).createLogScanner()) {
                scanner.subscribeFromBeginning(0);
                int count = 0;
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (count < 3) {
                    assertThat(System.nanoTime())
                            .as("Waiting for projected Arrow batches")
                            .isLessThan(deadline);
                    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                        ArrowScanRecords records = scanner.pollRecordBatch(Duration.ofSeconds(1));
                        for (ArrowIpcBatch ipcBatch : records) {
                            try (ArrowBatch batch = ArrowBatchReader.read(ipcBatch, allocator)) {
                                assertThat(batch.getVectorSchemaRoot().getSchema().getFields())
                                        .extracting(field -> field.getName())
                                        .containsExactly("c", "b");
                                VarCharVector c =
                                        (VarCharVector) batch.getVectorSchemaRoot().getVector(0);
                                VarCharVector b =
                                        (VarCharVector) batch.getVectorSchemaRoot().getVector(1);
                                for (int rowId = 0; rowId < batch.getRecordCount(); rowId++) {
                                    assertThat(batch.getBaseLogOffset() + rowId).isEqualTo(count);
                                    assertThat(batch.getChangeType(rowId))
                                            .isEqualTo(
                                                    primaryKey
                                                            ? ChangeType.INSERT
                                                            : ChangeType.APPEND_ONLY);
                                    assertThat(c.getObject(rowId).toString())
                                            .isEqualTo("c-" + count);
                                    assertThat(b.getObject(rowId).toString())
                                            .isEqualTo("b-" + count);
                                    count++;
                                }
                            }
                        }
                    }
                }
                assertThat(count).isEqualTo(3);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void testCannotSwitchArrowPollingMode(boolean arrowFirst, boolean pollEmptyFirst)
            throws Exception {
        TablePath tablePath =
                TablePath.of(
                        "test_db_1",
                        "test_arrow_polling_mode_" + arrowFirst + "_" + pollEmptyFirst);
        createTable(
                tablePath,
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .build(),
                false);
        clientConf.set(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS, 1);
        try (Connection connection = ConnectionFactory.createConnection(clientConf);
                Table table = connection.getTable(tablePath);
                LogScanner scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            int count = 0;
            if (pollEmptyFirst) {
                if (arrowFirst) {
                    ArrowScanRecords records = scanner.pollRecordBatch(Duration.ZERO);
                    assertThat(records.isEmpty()).isTrue();
                } else {
                    assertThat(scanner.poll(Duration.ZERO).isEmpty()).isTrue();
                }
            }
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < 3; i++) {
                writer.append(row(i, "value-" + i));
            }
            writer.flush();

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            if (!pollEmptyFirst) {
                while (count == 0) {
                    assertThat(System.nanoTime())
                            .as("Waiting for the first poll result")
                            .isLessThan(deadline);
                    if (arrowFirst) {
                        ArrowScanRecords records = scanner.pollRecordBatch(Duration.ofSeconds(1));
                        count += records.count();
                    } else {
                        count += scanner.poll(Duration.ofSeconds(1)).count();
                    }
                }
                if (!arrowFirst) {
                    assertThat(count).isEqualTo(1);
                }
            }

            assertThatThrownBy(
                            () -> {
                                if (arrowFirst) {
                                    scanner.poll(Duration.ZERO);
                                } else {
                                    scanner.pollRecordBatch(Duration.ZERO);
                                }
                            })
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Cannot switch between poll() and pollRecordBatch()");

            // Rejection must preserve unread rows, including a partially consumed row batch.
            while (count < 3) {
                assertThat(System.nanoTime())
                        .as("Waiting for unread rows after rejecting a mode switch")
                        .isLessThan(deadline);
                if (arrowFirst) {
                    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                        ArrowScanRecords records = scanner.pollRecordBatch(Duration.ofSeconds(1));
                        for (ArrowIpcBatch ipcBatch : records) {
                            try (ArrowBatch batch = ArrowBatchReader.read(ipcBatch, allocator)) {
                                assertThat(batch.getBaseLogOffset()).isEqualTo(count);
                                count += batch.getRecordCount();
                            }
                        }
                    }
                } else {
                    for (ScanRecord record : scanner.poll(Duration.ofSeconds(1))) {
                        assertThat(record.logOffset()).isEqualTo(count);
                        assertThat(record.getRow().getInt(0)).isEqualTo(count);
                        count++;
                    }
                }
            }
            assertThat(count).isEqualTo(3);
        }
    }

    @Test
    void testPollArrowBatchesWithPrimaryKeyChangelog() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_arrow_batches_with_changelog");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("a", DataTypes.INT())
                                        .column("b", DataTypes.STRING())
                                        .primaryKey("a")
                                        .build())
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .build();
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            writer.upsert(row(1, "old")).get();
            writer.flush();
            writer.upsert(row(1, "new")).get();
            writer.flush();
            writer.upsert(row(2, "deleted")).get();
            writer.flush();
            writer.delete(row(2, "deleted")).get();
            writer.flush();

            ChangeType[] expectedChangeTypes = {
                ChangeType.INSERT,
                ChangeType.UPDATE_BEFORE,
                ChangeType.UPDATE_AFTER,
                ChangeType.INSERT,
                ChangeType.DELETE
            };
            int[] expectedKeys = {1, 1, 1, 2, 2};
            String[] expectedValues = {"old", "old", "new", "deleted", "deleted"};

            try (LogScanner scanner = table.newScan().createLogScanner()) {
                scanner.subscribeFromBeginning(0);
                pollAndVerifyChangelogArrowBatches(
                        scanner, expectedChangeTypes, expectedKeys, expectedValues, 0);
            }

            try (LogScanner scanner = table.newScan().project(new int[] {1}).createLogScanner()) {
                scanner.subscribeFromBeginning(0);
                pollAndVerifyProjectedChangelogArrowBatches(
                        scanner, expectedChangeTypes, expectedValues);
            }

            // Offset 2 starts in the middle of the update batch and verifies that slicing keeps
            // the change-type vector aligned with the Arrow rows.
            try (LogScanner scanner = table.newScan().createLogScanner()) {
                scanner.subscribe(0, 2L);
                pollAndVerifyChangelogArrowBatches(
                        scanner, expectedChangeTypes, expectedKeys, expectedValues, 2);
            }
        }
    }

    @Test
    void testPollArrowBatchesWithSchemaEvolution() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_arrow_batches_with_schema_evolution");
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(DATA1_SCHEMA)
                        .distributedBy(1)
                        .logFormat(LogFormat.ARROW)
                        .build();
        createTable(tablePath, tableDescriptor, false);

        // write 3 rows with the original schema (a: INT, b: STRING)
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 0; i < 3; i++) {
                appendWriter.append(row(i, "value-" + i));
            }
            appendWriter.flush();
        }

        // add column c: STRING
        admin.alterTable(
                        tablePath,
                        Collections.singletonList(
                                TableChange.addColumn(
                                        "c",
                                        DataTypes.STRING(),
                                        null,
                                        TableChange.ColumnPosition.last())),
                        false)
                .get();

        // write 3 more rows with the evolved schema (a: INT, b: STRING, c: STRING)
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            for (int i = 3; i < 6; i++) {
                appendWriter.append(row(i, "value-" + i, "extra-" + i));
            }
            appendWriter.flush();

            int totalRecords = 6;
            // subscribe from beginning and verify all 6 records
            try (LogScanner scanner = table.newScan().createLogScanner()) {
                scanner.subscribeFromBeginning(0);
                pollAndVerifyArrowBatches(scanner, totalRecords, 0);
            }

            // subscribe from the middle of the first batch (offset 1)
            // to ensure records before the subscribe offset are not returned
            int subscribeOffset = 1;
            try (LogScanner scanner2 = table.newScan().createLogScanner()) {
                scanner2.subscribe(0, subscribeOffset);
                pollAndVerifyArrowBatches(
                        scanner2, totalRecords - subscribeOffset, subscribeOffset);
            }
        }
    }

    private void pollAndVerifyArrowBatches(
            LogScanner scanner, int expectedRecords, int minExpectedOffset) throws Exception {
        int count = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (count < expectedRecords) {
            assertThat(System.nanoTime())
                    .as("Timed out waiting for %s records, got %s", expectedRecords, count)
                    .isLessThan(deadline);
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                ArrowScanRecords records = scanner.pollRecordBatch(Duration.ofSeconds(1));
                for (ArrowIpcBatch batch : records) {
                    try (ArrowBatch b = ArrowBatchReader.read(batch, allocator)) {
                        IntVector intVector = (IntVector) b.getVectorSchemaRoot().getVector(0);
                        VarCharVector stringVector =
                                (VarCharVector) b.getVectorSchemaRoot().getVector(1);
                        VarCharVector extraVector =
                                (VarCharVector) b.getVectorSchemaRoot().getVector(2);
                        for (int rowId = 0; rowId < b.getRecordCount(); rowId++) {
                            int expectedValue = (int) (b.getBaseLogOffset() + rowId);
                            assertThat(expectedValue).isGreaterThanOrEqualTo(minExpectedOffset);
                            assertThat(intVector.get(rowId)).isEqualTo(expectedValue);
                            assertThat(stringVector.getObject(rowId).toString())
                                    .isEqualTo("value-" + expectedValue);
                            if (expectedValue < 3) {
                                assertThat(extraVector.isNull(rowId)).isTrue();
                            } else {
                                assertThat(extraVector.getObject(rowId).toString())
                                        .isEqualTo("extra-" + expectedValue);
                            }
                            count++;
                        }
                    }
                }
            }
        }
        assertThat(count).isEqualTo(expectedRecords);
    }

    private void pollAndVerifyChangelogArrowBatches(
            LogScanner scanner,
            ChangeType[] expectedChangeTypes,
            int[] expectedKeys,
            String[] expectedValues,
            int startingOffset)
            throws Exception {
        int count = startingOffset;
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (count < expectedChangeTypes.length) {
            assertThat(System.nanoTime())
                    .as(
                            "Timed out waiting for %s changelog records, got %s",
                            expectedChangeTypes.length - startingOffset, count - startingOffset)
                    .isLessThan(deadline);
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                ArrowScanRecords records = scanner.pollRecordBatch(Duration.ofSeconds(1));
                for (ArrowIpcBatch batch : records) {
                    try (ArrowBatch b = ArrowBatchReader.read(batch, allocator)) {
                        assertThat(b.isAppendOnly()).isFalse();
                        ByteBuffer changeTypes = b.getChangeTypes().get();
                        assertThat(changeTypes.remaining()).isEqualTo(b.getRecordCount());
                        IntVector keys = (IntVector) b.getVectorSchemaRoot().getVector(0);
                        VarCharVector values = (VarCharVector) b.getVectorSchemaRoot().getVector(1);
                        for (int rowId = 0; rowId < b.getRecordCount(); rowId++) {
                            long offset = b.getBaseLogOffset() + rowId;
                            assertThat(offset).isEqualTo(count);
                            assertThat(b.getChangeType(rowId))
                                    .isEqualTo(expectedChangeTypes[count]);
                            assertThat(changeTypes.get(rowId))
                                    .isEqualTo(expectedChangeTypes[count].toByteValue());
                            assertThat(keys.get(rowId)).isEqualTo(expectedKeys[count]);
                            assertThat(values.getObject(rowId).toString())
                                    .isEqualTo(expectedValues[count]);
                            count++;
                        }
                    }
                }
            }
        }
        assertThat(count).isEqualTo(expectedChangeTypes.length);
    }

    private void pollAndVerifyProjectedChangelogArrowBatches(
            LogScanner scanner, ChangeType[] expectedChangeTypes, String[] expectedValues)
            throws Exception {
        int count = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (count < expectedChangeTypes.length) {
            assertThat(System.nanoTime())
                    .as(
                            "Timed out waiting for %s projected changelog records, got %s",
                            expectedChangeTypes.length, count)
                    .isLessThan(deadline);
            try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
                ArrowScanRecords records = scanner.pollRecordBatch(Duration.ofSeconds(1));
                for (ArrowIpcBatch batch : records) {
                    try (ArrowBatch b = ArrowBatchReader.read(batch, allocator)) {
                        assertThat(b.getVectorSchemaRoot().getFieldVectors()).hasSize(1);
                        VarCharVector values = (VarCharVector) b.getVectorSchemaRoot().getVector(0);
                        for (int rowId = 0; rowId < b.getRecordCount(); rowId++) {
                            long offset = b.getBaseLogOffset() + rowId;
                            assertThat(offset).isEqualTo(count);
                            assertThat(b.getChangeType(rowId))
                                    .isEqualTo(expectedChangeTypes[count]);
                            assertThat(values.getObject(rowId).toString())
                                    .isEqualTo(expectedValues[count]);
                            count++;
                        }
                    }
                }
            }
        }
        assertThat(count).isEqualTo(expectedChangeTypes.length);
    }
}
