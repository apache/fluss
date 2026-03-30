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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.utils.TestUpsertWriter;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ByteArrayWrapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link UndoComputer}.
 *
 * <p>Tests verify: (1) ChangeType to undo operation mapping, (2) primary key deduplication.
 */
class UndoComputerTest {

    private static final RowType ROW_TYPE =
            RowType.of(DataTypes.INT(), DataTypes.STRING(), DataTypes.INT());
    private static final List<String> PRIMARY_KEY_COLUMNS = Collections.singletonList("f0");

    private KeyEncoder keyEncoder;
    private TestUpsertWriter mockWriter;
    private UndoComputer undoComputer;

    @BeforeEach
    void setUp() {
        keyEncoder = KeyEncoder.of(ROW_TYPE, PRIMARY_KEY_COLUMNS, null);
        mockWriter = new TestUpsertWriter();
        undoComputer = new UndoComputer(keyEncoder, mockWriter);
    }

    // ==================== Undo Logic Mapping (Parameterized) ====================

    /**
     * Parameterized test for ChangeType to undo operation mapping.
     *
     * <p>Validates: Requirements 4.3 - Correct undo logic mapping.
     *
     * <p>Mapping rules:
     *
     * <ul>
     *   <li>INSERT → delete (undo insert by deleting)
     *   <li>UPDATE_BEFORE → upsert (restore old value)
     *   <li>UPDATE_AFTER → skip (no action needed)
     *   <li>DELETE → upsert (restore deleted row)
     * </ul>
     */
    @ParameterizedTest(name = "{0} → expectDelete={1}, expectUpsert={2}, expectNull={3}")
    @MethodSource("changeTypeUndoMappingProvider")
    void testChangeTypeToUndoMapping(
            ChangeType changeType,
            boolean expectDelete,
            boolean expectUpsert,
            boolean expectNullFuture) {
        Set<ByteArrayWrapper> processedKeys = new HashSet<>();
        GenericRow testRow = row(1, "test", 100);
        ScanRecord record = new ScanRecord(0L, 0L, changeType, testRow);

        CompletableFuture<?> future = undoComputer.processRecord(record, processedKeys);

        if (expectNullFuture) {
            assertThat(future).isNull();
            assertThat(processedKeys).isEmpty();
        } else {
            assertThat(future).isNotNull();
            assertThat(processedKeys).hasSize(1);
        }
        assertThat(mockWriter.getDeleteCount()).isEqualTo(expectDelete ? 1 : 0);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(expectUpsert ? 1 : 0);

        if (expectDelete) {
            assertThat(mockWriter.getLastDeletedRow()).isSameAs(testRow);
        }
        if (expectUpsert) {
            assertThat(mockWriter.getLastUpsertedRow()).isSameAs(testRow);
        }
    }

    static Stream<Arguments> changeTypeUndoMappingProvider() {
        return Stream.of(
                // ChangeType, expectDelete, expectUpsert, expectNullFuture
                Arguments.of(ChangeType.INSERT, true, false, false),
                Arguments.of(ChangeType.UPDATE_BEFORE, false, true, false),
                Arguments.of(ChangeType.UPDATE_AFTER, false, false, true),
                Arguments.of(ChangeType.DELETE, false, true, false));
    }

    // ==================== Primary Key Deduplication ====================

    /**
     * Test primary key deduplication: only first occurrence of each key is processed.
     *
     * <p>Validates: Requirements 2.2, 2.3 - Primary key deduplication and key tracking.
     */
    @Test
    void testPrimaryKeyDeduplication() {
        Set<ByteArrayWrapper> processedKeys = new HashSet<>();

        // First record for key=1 (INSERT → delete)
        GenericRow row1 = row(1, "first", 100);
        CompletableFuture<?> f1 =
                undoComputer.processRecord(
                        new ScanRecord(0L, 0L, ChangeType.INSERT, row1), processedKeys);

        // Second record for same key=1 (should be skipped)
        GenericRow row2 = row(1, "second", 200);
        CompletableFuture<?> f2 =
                undoComputer.processRecord(
                        new ScanRecord(1L, 0L, ChangeType.UPDATE_BEFORE, row2), processedKeys);

        // Third record for different key=2 (INSERT → delete)
        GenericRow row3 = row(2, "third", 300);
        CompletableFuture<?> f3 =
                undoComputer.processRecord(
                        new ScanRecord(2L, 0L, ChangeType.INSERT, row3), processedKeys);

        // Fourth record for key=2 (should be skipped)
        GenericRow row4 = row(2, "fourth", 400);
        CompletableFuture<?> f4 =
                undoComputer.processRecord(
                        new ScanRecord(3L, 0L, ChangeType.DELETE, row4), processedKeys);

        // Verify: only first occurrence of each key processed
        assertThat(f1).isNotNull();
        assertThat(f2).as("Duplicate key=1 should be skipped").isNull();
        assertThat(f3).isNotNull();
        assertThat(f4).as("Duplicate key=2 should be skipped").isNull();

        // Verify: 2 unique keys tracked
        assertThat(processedKeys).hasSize(2);

        // Verify: 2 deletes (one per unique key, both were INSERTs)
        assertThat(mockWriter.getDeleteCount()).isEqualTo(2);
        assertThat(mockWriter.getUpsertCount()).isEqualTo(0);

        // Verify: correct rows were processed
        assertThat(mockWriter.getAllDeletedRows()).containsExactly(row1, row3);
    }

    /**
     * Test that UPDATE_AFTER records don't add keys to processed set.
     *
     * <p>This ensures subsequent records for the same key can still be processed.
     */
    @Test
    void testUpdateAfterDoesNotBlockSubsequentRecords() {
        Set<ByteArrayWrapper> processedKeys = new HashSet<>();

        // UPDATE_AFTER for key=1 (skipped, key NOT added to set)
        undoComputer.processRecord(
                new ScanRecord(0L, 0L, ChangeType.UPDATE_AFTER, row(1, "after", 100)),
                processedKeys);

        // INSERT for same key=1 (should be processed since UPDATE_AFTER didn't add key)
        CompletableFuture<?> f =
                undoComputer.processRecord(
                        new ScanRecord(1L, 0L, ChangeType.INSERT, row(1, "insert", 200)),
                        processedKeys);

        assertThat(f).isNotNull();
        assertThat(mockWriter.getDeleteCount()).isEqualTo(1);
        assertThat(processedKeys).hasSize(1);
    }
}
