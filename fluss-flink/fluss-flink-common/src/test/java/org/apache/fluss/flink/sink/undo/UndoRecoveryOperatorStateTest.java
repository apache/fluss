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

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for checkpoint state maintained by {@link UndoRecoveryOperator}. */
public class UndoRecoveryOperatorStateTest extends FlinkTestBase {

    private static final AtomicInteger TABLE_SEQUENCE = new AtomicInteger();

    private static final Schema AGG_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("value", DataTypes.BIGINT(), AggFunctions.SUM())
                    .primaryKey("id")
                    .build();

    @Test
    void testRestoreEmptyV2CompleteState() throws Exception {
        AggTable table = createAggTable("empty_complete_state", 1);
        String producerId = uniqueProducerId("empty");
        OperatorSubtaskState snapshot;

        try (UndoOperatorHarness initial = createHarness(table, producerId)) {
            initial.open();
            assertThat(initial.getBucketOffsets()).isEmpty();
            snapshot = initial.snapshot(1L, 1L);
        }

        try (UndoOperatorHarness restored = createHarness(table, producerId)) {
            restored.initializeState(snapshot);
            restored.open();
            assertThat(restored.getBucketOffsets()).isEmpty();
        }
    }

    @Test
    void testSnapshotPreservesOffsetsForUnchangedBuckets() throws Exception {
        AggTable table = createAggTable("complete_baseline", 2);
        List<WrittenRow> writtenRows = writeRowsToDistinctBuckets(table);
        WrittenRow unchangedRow = writtenRows.get(0);
        WrittenRow updatedRow = writtenRows.get(1);
        long unchangedLeo = latestOffset(table.tablePath, unchangedRow.bucket);
        String producerId = uniqueProducerId("complete-baseline");
        OperatorSubtaskState snapshot;
        long reportedLeo;

        UndoRecoveryOperatorFactory<InternalRow> initialFactory = createFactory(table, producerId);
        try (UndoOperatorHarness initial = createHarness(initialFactory)) {
            initial.open();

            UpsertResult reportedWrite = upsert(table.tablePath, updatedRow.key, 7L);
            assertThat(reportedWrite.getBucket()).isEqualTo(updatedRow.bucket);
            reportedLeo = reportedWrite.getLogEndOffset();
            initialFactory
                    .createProducerOffsetReporter(0)
                    .reportOffset(reportedWrite.getBucket(), reportedLeo);
            snapshot = initial.snapshot(2L, 2L);
        }

        try (UndoOperatorHarness restored = createHarness(table, producerId)) {
            restored.initializeState(snapshot);
            restored.open();

            assertThat(restored.getBucketOffsets())
                    .hasSize(2)
                    .containsEntry(unchangedRow.bucket, unchangedLeo)
                    .containsEntry(updatedRow.bucket, reportedLeo);
            assertThat(lookupValue(table.tablePath, unchangedRow.key))
                    .isEqualTo(unchangedRow.value);
        }
    }

    @Test
    void testSecondRestoreDoesNotRepeatUndoRecovery() throws Exception {
        AggTable table = createAggTable("post_recovery_baseline", 2);
        List<WrittenRow> writtenRows = writeRowsToDistinctBuckets(table);
        WrittenRow unchangedRow = writtenRows.get(0);
        WrittenRow updatedRow = writtenRows.get(1);
        long unchangedLeo = latestOffset(table.tablePath, unchangedRow.bucket);
        String producerId = uniqueProducerId("post-undo");
        OperatorSubtaskState initialSnapshot;

        try (UndoOperatorHarness initial = createHarness(table, producerId)) {
            initial.open();
            initialSnapshot = initial.snapshot(3L, 3L);
        }

        UpsertResult uncheckpointedWrite = upsert(table.tablePath, updatedRow.key, 7L);
        assertThat(uncheckpointedWrite.getBucket()).isEqualTo(updatedRow.bucket);
        long uncheckpointedLeo = uncheckpointedWrite.getLogEndOffset();
        OperatorSubtaskState postRecoverySnapshot;
        long recoveredLeo;

        try (UndoOperatorHarness recovered = createHarness(table, producerId)) {
            recovered.initializeState(initialSnapshot);
            recovered.open();

            recoveredLeo = latestOffset(table.tablePath, updatedRow.bucket);
            assertThat(recoveredLeo).isGreaterThan(uncheckpointedLeo);
            assertThat(recovered.getBucketOffsets())
                    .hasSize(2)
                    .containsEntry(unchangedRow.bucket, unchangedLeo)
                    .containsEntry(updatedRow.bucket, recoveredLeo);
            assertThat(lookupValue(table.tablePath, unchangedRow.key))
                    .isEqualTo(unchangedRow.value);
            assertThat(lookupValue(table.tablePath, updatedRow.key)).isEqualTo(updatedRow.value);
            postRecoverySnapshot = recovered.snapshot(4L, 4L);
        }

        try (UndoOperatorHarness secondRestore = createHarness(table, producerId)) {
            secondRestore.initializeState(postRecoverySnapshot);
            secondRestore.open();

            assertThat(secondRestore.getBucketOffsets())
                    .hasSize(2)
                    .containsEntry(unchangedRow.bucket, unchangedLeo)
                    .containsEntry(updatedRow.bucket, recoveredLeo);
            assertThat(latestOffset(table.tablePath, unchangedRow.bucket)).isEqualTo(unchangedLeo);
            assertThat(latestOffset(table.tablePath, updatedRow.bucket)).isEqualTo(recoveredLeo);
            assertThat(lookupValue(table.tablePath, unchangedRow.key))
                    .isEqualTo(unchangedRow.value);
            assertThat(lookupValue(table.tablePath, updatedRow.key)).isEqualTo(updatedRow.value);
        }
    }

    private AggTable createAggTable(String prefix, int numBuckets) throws Exception {
        TablePath tablePath =
                TablePath.of(DEFAULT_DB, prefix + "_" + TABLE_SEQUENCE.incrementAndGet());
        long tableId =
                createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(AGG_SCHEMA)
                                .distributedBy(numBuckets, "id")
                                .property(
                                        ConfigOptions.TABLE_MERGE_ENGINE,
                                        MergeEngineType.AGGREGATION)
                                .build());
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);
        return new AggTable(tablePath, tableId, numBuckets);
    }

    private static String uniqueProducerId(String prefix) {
        return "undo-state-" + prefix + "-" + TABLE_SEQUENCE.incrementAndGet();
    }

    private List<WrittenRow> writeRowsToDistinctBuckets(AggTable table) throws Exception {
        List<WrittenRow> rows = new ArrayList<>();
        try (Table flussTable = conn.getTable(table.tablePath)) {
            UpsertWriter writer = flussTable.newUpsert().createWriter();
            for (int key = 0; key < 100 && rows.size() < table.numBuckets; key++) {
                long value = 100L + key;
                CompletableFuture<UpsertResult> future = writer.upsert(row(key, value));
                writer.flush();
                UpsertResult result = future.get();
                assertThat(result.getBucket()).isNotNull();
                assertThat(result.getBucket().getTableId()).isEqualTo(table.tableId);
                if (!containsBucket(rows, result.getBucket())) {
                    rows.add(new WrittenRow(key, value, result.getBucket()));
                }
            }
        }
        assertThat(rows).hasSize(table.numBuckets);
        return rows;
    }

    private static boolean containsBucket(List<WrittenRow> rows, TableBucket bucket) {
        for (WrittenRow row : rows) {
            if (row.bucket.equals(bucket)) {
                return true;
            }
        }
        return false;
    }

    private UpsertResult upsert(TablePath tablePath, int key, long value) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            CompletableFuture<UpsertResult> result = writer.upsert(row(key, value));
            writer.flush();
            return result.get();
        }
    }

    private long lookupValue(TablePath tablePath, int key) throws Exception {
        try (Table table = conn.getTable(tablePath)) {
            Lookuper lookuper = table.newLookup().createLookuper();
            InternalRow result = lookuper.lookup(row(key)).get().getSingletonRow();
            assertThat(result).isNotNull();
            return result.getLong(1);
        }
    }

    private long latestOffset(TablePath tablePath, TableBucket bucket) throws Exception {
        Long offset =
                admin.listOffsets(
                                tablePath,
                                Collections.singletonList(bucket.getBucket()),
                                new OffsetSpec.LatestSpec())
                        .bucketResult(bucket.getBucket())
                        .get();
        assertThat(offset).isNotNull();
        return offset;
    }

    private UndoOperatorHarness createHarness(AggTable table, String producerId) throws Exception {
        return createHarness(createFactory(table, producerId));
    }

    private UndoOperatorHarness createHarness(UndoRecoveryOperatorFactory<InternalRow> factory)
            throws Exception {
        return new UndoOperatorHarness(factory);
    }

    private UndoRecoveryOperatorFactory<InternalRow> createFactory(
            AggTable table, String producerId) {
        return new UndoRecoveryOperatorFactory<>(
                table.tablePath,
                new Configuration(clientConf),
                AGG_SCHEMA.getRowType(),
                null,
                table.numBuckets,
                false,
                producerId);
    }

    private static final class AggTable {
        private final TablePath tablePath;
        private final long tableId;
        private final int numBuckets;

        private AggTable(TablePath tablePath, long tableId, int numBuckets) {
            this.tablePath = tablePath;
            this.tableId = tableId;
            this.numBuckets = numBuckets;
        }
    }

    private static final class WrittenRow {
        private final int key;
        private final long value;
        private final TableBucket bucket;

        private WrittenRow(int key, long value, TableBucket bucket) {
            this.key = key;
            this.value = value;
            this.bucket = bucket;
        }
    }

    private static final class UndoOperatorHarness
            extends OneInputStreamOperatorTestHarness<InternalRow, InternalRow> {

        private UndoOperatorHarness(UndoRecoveryOperatorFactory<InternalRow> factory)
                throws Exception {
            super(factory, 1, 1, 0);
        }

        @SuppressWarnings("unchecked")
        private Map<TableBucket, Long> getBucketOffsets() {
            return ((UndoRecoveryOperator<InternalRow>) getOperator()).getBucketOffsets();
        }
    }
}
