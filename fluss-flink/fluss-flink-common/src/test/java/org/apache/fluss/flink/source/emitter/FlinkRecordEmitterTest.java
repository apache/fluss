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

package org.apache.fluss.flink.source.emitter;

import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.state.LakeSnapshotAndFlussLogSplitState;
import org.apache.fluss.flink.source.deserializer.BinlogDeserializationSchema;
import org.apache.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.LogSplitState;
import org.apache.fluss.flink.source.split.SourceSplitState;
import org.apache.fluss.flink.source.testutils.Order;
import org.apache.fluss.flink.source.testutils.OrderDeserializationSchema;
import org.apache.fluss.flink.utils.FlussRowToFlinkRowConverter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRecordEmitter} with RowData output type. */
public class FlinkRecordEmitterTest {
    @Test
    void testEmitRowDataRecordWithHybridSplitInSnapshotPhase() throws Exception {
        // Setup
        long tableId = 1L;

        TableBucket bucket0 = new TableBucket(tableId, 0);

        HybridSnapshotLogSplit hybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(
                        bucket0, null, 0L, 0, false, 0L, LogSplit.NO_STOPPING_OFFSET, false);

        HybridSnapshotLogSplitState splitState =
                new HybridSnapshotLogSplitState(hybridSnapshotLogSplit);

        ScanRecord scanRecord = new ScanRecord(-1, 100L, ChangeType.INSERT, row(1, "a"));

        DataType[] dataTypes = new DataType[] {DataTypes.INT(), DataTypes.STRING()};

        String[] fieldNames = new String[] {"id", "name"};

        RowType sourceOutputType = RowType.of(dataTypes, fieldNames);

        RecordAndPos recordAndPos = new RecordAndPos(scanRecord, 42L);

        FlussRowToFlinkRowConverter converter = new FlussRowToFlinkRowConverter(sourceOutputType);
        RowDataDeserializationSchema deserializationSchema = new RowDataDeserializationSchema();
        deserializationSchema.open(new DeserializerInitContextImpl(null, null, sourceOutputType));

        FlinkRecordEmitter<RowData> emitter = new FlinkRecordEmitter<>(deserializationSchema);

        TestSourceOutput<RowData> sourceOutput = new TestSourceOutput<>();

        // Execute
        emitter.emitRecord(recordAndPos, sourceOutput, splitState);
        List<RowData> results = sourceOutput.getRecords();

        ArrayList<RowData> expectedResult = new ArrayList<>();
        expectedResult.add(converter.toFlinkRowData(row(1, "a")));

        assertThat(splitState.isHybridSnapshotLogSplitState()).isTrue();
        assertThat(results).hasSize(1);
        assertThat(results).isEqualTo(expectedResult);
    }

    @Test
    void testEmitPojoRecordWithHybridSplitInSnapshotPhase() throws Exception {
        // Setup
        Schema tableSchema =
                Schema.newBuilder()
                        .primaryKey("orderId")
                        .column("orderId", DataTypes.BIGINT())
                        .column("itemId", DataTypes.BIGINT())
                        .column("amount", DataTypes.INT())
                        .column("address", DataTypes.STRING())
                        .build();

        long tableId = 1L;

        TableBucket bucket0 = new TableBucket(tableId, 0);

        HybridSnapshotLogSplit hybridSnapshotLogSplit =
                new HybridSnapshotLogSplit(
                        bucket0, null, 0L, 0, false, 0L, LogSplit.NO_STOPPING_OFFSET, false);

        HybridSnapshotLogSplitState splitState =
                new HybridSnapshotLogSplitState(hybridSnapshotLogSplit);

        ScanRecord scanRecord =
                new ScanRecord(-1, 100L, ChangeType.INSERT, row(1001L, 101L, 5, "Test 123 Addr."));

        RecordAndPos recordAndPos = new RecordAndPos(scanRecord, 42L);

        OrderDeserializationSchema deserializationSchema = new OrderDeserializationSchema();
        deserializationSchema.open(
                new DeserializerInitContextImpl(null, null, tableSchema.getRowType()));
        FlinkRecordEmitter<Order> emitter = new FlinkRecordEmitter<>(deserializationSchema);

        TestSourceOutput<Order> sourceOutput = new TestSourceOutput<>();

        // Execute
        emitter.emitRecord(recordAndPos, sourceOutput, splitState);
        List<Order> results = sourceOutput.getRecords();

        ArrayList<Order> expectedResult = new ArrayList<>();
        expectedResult.add(new Order(1001L, 101L, 5, "Test 123 Addr."));

        assertThat(splitState.isHybridSnapshotLogSplitState()).isTrue();
        assertThat(results).hasSize(1);
        assertThat(results).isEqualTo(expectedResult);
    }

    @Test
    void testFilteredUpdateWithLogSplit() throws Exception {
        TableBucket bucket = new TableBucket(1L, 0);
        LogSplitState state = new LogSplitState(new LogSplit(bucket, null, 40L, 100L));
        RowDataDeserializationSchema schema = new RowDataDeserializationSchema();
        FlinkRecordEmitter<RowData> emitter = new FlinkRecordEmitter<>(schema);
        TestSourceOutput<RowData> output = new TestSourceOutput<>();
        emitter.emitRecord(
                RecordAndPos.filteredUpdate(
                        new ScanRecord(41L, 100L, ChangeType.UPDATE_AFTER, row(1))),
                output,
                state);
        assertThat(output.getRecords()).isEmpty();
        assertThat(output.timestamps).isEmpty();
        LogSplitState restored = new LogSplitState(state.toSourceSplit());
        assertThat(restored.toSourceSplit()).isEqualTo(new LogSplit(bucket, null, 42L, 100L));

        RowType rowType = RowType.of(DataTypes.INT());
        schema.open(new DeserializerInitContextImpl(null, null, rowType));
        emitter.emitRecord(
                new RecordAndPos(new ScanRecord(42L, 200L, ChangeType.INSERT, row(2))),
                output,
                restored);
        assertThat(output.getRecords())
                .containsExactly(new FlussRowToFlinkRowConverter(rowType).toFlinkRowData(row(2)));
        assertThat(output.timestamps).containsExactly(200L);
        assertThat(restored.toSourceSplit().getStartingOffset()).isEqualTo(43L);
    }

    @Test
    void testFilteredUpdateWithHybridSplit() {
        TableBucket bucket = new TableBucket(1L, 0);
        HybridSnapshotLogSplitState state =
                new HybridSnapshotLogSplitState(
                        new HybridSnapshotLogSplit(bucket, null, 3L, 7L, false, 40L, 100L, false));
        assertFilteredUpdateSkipped(state);
        HybridSnapshotLogSplit restored =
                new HybridSnapshotLogSplitState(state.toSourceSplit()).toSourceSplit();
        assertThat(restored)
                .isEqualTo(
                        new HybridSnapshotLogSplit(bucket, null, 3L, 7L, true, 42L, 100L, false));
    }

    @Test
    void testFilteredUpdateWithLakeSplit() {
        LakeSnapshotAndFlussLogSplitState state =
                new LakeSnapshotAndFlussLogSplitState(
                        new LakeSnapshotAndFlussLogSplit(
                                new TableBucket(1L, 0), null, null, 40L, 100L, 7L, 0, false));
        assertFilteredUpdateSkipped(state);
        LakeSnapshotAndFlussLogSplit restored =
                (LakeSnapshotAndFlussLogSplit)
                        new LakeSnapshotAndFlussLogSplitState(
                                        (LakeSnapshotAndFlussLogSplit) state.toSourceSplit())
                                .toSourceSplit();
        assertThat(restored.getStartingOffset()).isEqualTo(42L);
        assertThat(restored.isLakeSplitFinished()).isTrue();
        assertThat(restored.getRecordsToSkip()).isEqualTo(7L);
        assertThat(restored.getStoppingOffset()).contains(100L);
    }

    @Test
    void testBinlogUpdateBeforeRetainsRecoveryOffset() throws Exception {
        BinlogDeserializationSchema schema = new BinlogDeserializationSchema();
        schema.open(new DeserializerInitContextImpl(null, null, RowType.of(DataTypes.INT())));
        FlinkRecordEmitter<RowData> emitter = new FlinkRecordEmitter<>(schema);
        LogSplitState state = new LogSplitState(new LogSplit(new TableBucket(1L, 0), null, 40L));
        TestSourceOutput<RowData> output = new TestSourceOutput<>();
        emitter.emitRecord(
                new RecordAndPos(new ScanRecord(40L, 100L, ChangeType.UPDATE_BEFORE, row(1))),
                output,
                state);
        assertThat(output.getRecords()).isEmpty();
        assertThat(output.timestamps).isEmpty();
        assertThat(state.toSourceSplit().getStartingOffset()).isEqualTo(40L);
        emitter.emitRecord(
                new RecordAndPos(new ScanRecord(41L, 200L, ChangeType.UPDATE_AFTER, row(2))),
                output,
                state);
        assertThat(output.getRecords()).hasSize(1);
        assertThat(output.getRecords().get(0).getRow(3, 1).getInt(0)).isEqualTo(1);
        assertThat(output.getRecords().get(0).getRow(4, 1).getInt(0)).isEqualTo(2);
        assertThat(output.timestamps).containsExactly(200L);
        assertThat(state.toSourceSplit().getStartingOffset()).isEqualTo(42L);
    }

    private void assertFilteredUpdateSkipped(SourceSplitState state) {
        // Deliberately unopened: deserialization of the marker would throw.
        FlinkRecordEmitter<RowData> emitter =
                new FlinkRecordEmitter<>(new RowDataDeserializationSchema());
        TestSourceOutput<RowData> output = new TestSourceOutput<>();
        emitter.emitRecord(
                RecordAndPos.filteredUpdate(
                        new ScanRecord(41L, 100L, ChangeType.UPDATE_AFTER, row(1))),
                output,
                state);
        assertThat(output.getRecords()).isEmpty();
        assertThat(output.timestamps).isEmpty();
    }

    private static class TestSourceOutput<OUT> implements SourceOutput<OUT> {
        private final List<OUT> records = new ArrayList<>();
        private final List<Long> timestamps = new ArrayList<>();

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}

        @Override
        public void collect(OUT record) {
            records.add(record);
        }

        @Override
        public void collect(OUT record, long timestamp) {
            records.add(record);
            timestamps.add(timestamp);
        }

        public List<OUT> getRecords() {
            return this.records;
        }
    }
}
