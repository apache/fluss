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
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.sink.state.WriterStateSerializer;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
    private static final String UNDO_RECOVERY_STATE_NAME = "undo_recovery_state";

    private static final Schema AGG_SCHEMA =
            Schema.newBuilder()
                    .column("id", DataTypes.INT())
                    .column("value", DataTypes.BIGINT(), AggFunctions.SUM())
                    .primaryKey("id")
                    .build();

    @Test
    void testV1StateMigratesToV2OnNextCheckpoint() throws Exception {
        AggTable table = createAggTable("v1_migration", 1);
        UpsertResult write = upsert(table.tablePath, 1, 10L);
        TableBucket bucket = write.getBucket();
        long offset = write.getLogEndOffset();
        WriterState legacyState = new WriterState(Collections.singletonMap(bucket, offset));

        OperatorSubtaskState v1Snapshot;
        try (StateOperatorHarness seed =
                new StateOperatorHarness(new StateSeedOperator(legacyState))) {
            seed.open();
            v1Snapshot = seed.snapshot(1L, 1L);
        }

        OperatorSubtaskState migratedSnapshot;
        try (UndoOperatorHarness migrating =
                createHarness(table, uniqueProducerId("v1-migration"))) {
            migrating.initializeState(v1Snapshot);
            migrating.open();
            assertThat(migrating.getBucketOffsets()).containsEntry(bucket, offset);
            migratedSnapshot = migrating.snapshot(2L, 2L);
        }

        StateInspectOperator inspector = new StateInspectOperator();
        try (StateOperatorHarness inspect = new StateOperatorHarness(inspector)) {
            inspect.initializeState(migratedSnapshot);
            inspect.open();
        }

        assertThat(inspector.getRestoredStates()).hasSize(1);
        WriterState migratedState = inspector.getRestoredStates().get(0);
        assertThat(migratedState.getStateFormat()).isEqualTo(WriterState.StateFormat.V2_COMPLETE);
        assertThat(migratedState.getTableId()).isEqualTo(table.tableId);
        assertThat(migratedState.getBucketOffsets()).containsOnlyKeys(bucket);
        assertThat(migratedState.getBucketOffsets().get(bucket)).isEqualTo(offset);
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

    private static final class StateOperatorHarness
            extends OneInputStreamOperatorTestHarness<InternalRow, InternalRow> {

        private StateOperatorHarness(OneInputStreamOperator<InternalRow, InternalRow> operator)
                throws Exception {
            super(operator, 1, 1, 0);
        }
    }

    private static final class StateSeedOperator extends AbstractStreamOperator<InternalRow>
            implements OneInputStreamOperator<InternalRow, InternalRow> {

        private final WriterState state;

        private StateSeedOperator(WriterState state) {
            this.state = state;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            context.getOperatorStateStore().getUnionListState(stateDescriptor()).add(state);
        }

        @Override
        public void processElement(StreamRecord<InternalRow> element) {}
    }

    private static final class StateInspectOperator extends AbstractStreamOperator<InternalRow>
            implements OneInputStreamOperator<InternalRow, InternalRow> {

        private final List<WriterState> restoredStates = new ArrayList<>();

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            ListState<WriterState> state =
                    context.getOperatorStateStore().getUnionListState(stateDescriptor());
            for (WriterState writerState : state.get()) {
                restoredStates.add(writerState);
            }
        }

        private List<WriterState> getRestoredStates() {
            return restoredStates;
        }

        @Override
        public void processElement(StreamRecord<InternalRow> element) {}
    }

    private static ListStateDescriptor<WriterState> stateDescriptor() {
        return new ListStateDescriptor<>(UNDO_RECOVERY_STATE_NAME, new WriterStateSerializer());
    }
}
