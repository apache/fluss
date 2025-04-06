/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.source.emitter;

import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.flink.common.Order;
import com.alibaba.fluss.flink.source.deserializer.InitializationContextImpl;
import com.alibaba.fluss.flink.source.deserializer.OrderDeserializationSchema;
import com.alibaba.fluss.flink.source.deserializer.RowDataDeserializationSchema;
import com.alibaba.fluss.flink.source.reader.RecordAndPos;
import com.alibaba.fluss.flink.source.split.HybridSnapshotLogSplit;
import com.alibaba.fluss.flink.source.split.HybridSnapshotLogSplitState;
import com.alibaba.fluss.flink.source.testutils.FlinkTestBase;
import com.alibaba.fluss.flink.utils.FlussRowToFlinkRowConverter;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.record.ChangeType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkRecordEmitter} with RowData output type. */
public class FlinkRecordEmitterTest extends FlinkTestBase {
    @Test
    void testEmitRowDataRecordWithHybridSplitInSnapshotPhase() throws Exception {
        // Setup
        long tableId = createTable(DEFAULT_TABLE_PATH, DEFAULT_PK_TABLE_DESCRIPTOR);

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
        deserializationSchema.open(new InitializationContextImpl(null, null, sourceOutputType));

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

        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(tableSchema)
                        .distributedBy(DEFAULT_BUCKET_NUM, "orderId")
                        .build();

        long tableId = createTable(DEFAULT_TABLE_PATH, tableDescriptor);

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
                new InitializationContextImpl(null, null, tableSchema.getRowType()));
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
