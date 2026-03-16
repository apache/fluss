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
import org.apache.fluss.flink.source.deserializer.BinlogDeserializationSchema;
import org.apache.fluss.flink.source.deserializer.DeserializerInitContextImpl;
import org.apache.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import org.apache.fluss.flink.source.reader.RecordAndPos;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplit;
import org.apache.fluss.flink.source.split.HybridSnapshotLogSplitState;
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
import org.apache.flink.table.data.StringData;
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
                new HybridSnapshotLogSplit(bucket0, null, 0L, 0L);

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
    void testHybridIncrementalBinlogUpdatesDoNotCorruptAcrossSplits() throws Exception {
        RowType sourceOutputType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING(), DataTypes.BIGINT()},
                        new String[] {"id", "name", "amount"});

        BinlogDeserializationSchema deserializationSchema = new BinlogDeserializationSchema();
        deserializationSchema.open(new DeserializerInitContextImpl(null, null, sourceOutputType));
        FlinkRecordEmitter<RowData> emitter = new FlinkRecordEmitter<>(deserializationSchema);

        HybridSnapshotLogSplitState splitStateA =
                new HybridSnapshotLogSplitState(
                        new HybridSnapshotLogSplit(new TableBucket(1L, 0), null, 0L, 0L));
        HybridSnapshotLogSplitState splitStateB =
                new HybridSnapshotLogSplitState(
                        new HybridSnapshotLogSplit(new TableBucket(1L, 1), null, 0L, 0L));

        TestSourceOutput<RowData> sourceOutput = new TestSourceOutput<>();

        // Interleave -U/+U across two hybrid splits and verify pairing stays split-scoped.
        emitter.emitRecord(
                new RecordAndPos(
                        new ScanRecord(100L, 1000L, ChangeType.UPDATE_BEFORE, row(1, "A-old", 100L)),
                        1L),
                sourceOutput,
                splitStateA);
        emitter.emitRecord(
                new RecordAndPos(
                        new ScanRecord(200L, 2000L, ChangeType.UPDATE_BEFORE, row(2, "B-old", 200L)),
                        1L),
                sourceOutput,
                splitStateB);
        emitter.emitRecord(
                new RecordAndPos(
                        new ScanRecord(201L, 2000L, ChangeType.UPDATE_AFTER, row(2, "B-new", 300L)),
                        1L),
                sourceOutput,
                splitStateB);
        emitter.emitRecord(
                new RecordAndPos(
                        new ScanRecord(101L, 1000L, ChangeType.UPDATE_AFTER, row(1, "A-new", 150L)),
                        1L),
                sourceOutput,
                splitStateA);

        List<RowData> results = sourceOutput.getRecords();
        assertThat(results).hasSize(2);

        // Each emitted update should keep its own split's before/after rows and offsets.
        RowData resultB = results.get(0);
        assertThat(resultB.getString(0)).isEqualTo(StringData.fromString("update"));
        assertThat(resultB.getLong(1)).isEqualTo(200L);
        assertThat(resultB.getRow(3, 3).getString(1)).isEqualTo(StringData.fromString("B-old"));
        assertThat(resultB.getRow(4, 3).getString(1)).isEqualTo(StringData.fromString("B-new"));

        RowData resultA = results.get(1);
        assertThat(resultA.getString(0)).isEqualTo(StringData.fromString("update"));
        assertThat(resultA.getLong(1)).isEqualTo(100L);
        assertThat(resultA.getRow(3, 3).getString(1)).isEqualTo(StringData.fromString("A-old"));
        assertThat(resultA.getRow(4, 3).getString(1)).isEqualTo(StringData.fromString("A-new"));
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
                new HybridSnapshotLogSplit(bucket0, null, 0L, 0L);

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

    private static class TestSourceOutput<OUT> implements SourceOutput<OUT> {
        private final List<OUT> records = new ArrayList<>();

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
        }

        public List<OUT> getRecords() {
            return this.records;
        }
    }
}
