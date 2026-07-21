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

import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertResult;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.state.WriterState;
import org.apache.fluss.flink.sink.state.WriterStateSerializer;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration test for restoring legacy markerless state through the UNDO path. */
class UndoRecoveryOperatorITCase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    private static final int MAX_PARALLELISM = 4;
    private static final int NUM_BUCKETS = 2;
    private static final String DATABASE = FlussClusterExtension.BUILTIN_DATABASE;

    private static Connection connection;
    private static Admin admin;

    @BeforeAll
    static void beforeAll() {
        connection = ConnectionFactory.createConnection(FLUSS_CLUSTER_EXTENSION.getClientConfig());
        admin = connection.getAdmin();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testLegacyCheckpointWithoutActionMarkerRestoresWithUndo() throws Exception {
        String suffix = Long.toUnsignedString(System.nanoTime());
        TablePath tablePath = TablePath.of(DATABASE, "legacy_markerless_undo_" + suffix);
        String producerId = "legacy-markerless-undo-" + suffix;
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT(), AggFunctions.SUM())
                        .primaryKey("id")
                        .build();

        admin.createTable(
                        tablePath,
                        TableDescriptor.builder()
                                .schema(schema)
                                .distributedBy(NUM_BUCKETS, "id")
                                .property(
                                        ConfigOptions.TABLE_MERGE_ENGINE,
                                        MergeEngineType.AGGREGATION)
                                .build(),
                        false)
                .get();
        long tableId = admin.getTableInfo(tablePath).get().getTableId();
        FLUSS_CLUSTER_EXTENSION.waitUntilTableReady(tableId);

        try (Table table = connection.getTable(tablePath)) {
            UpsertWriter writer = table.newUpsert().createWriter();
            Lookuper lookuper = table.newLookup().createLookuper();

            UpsertResult committedWrite = writer.upsert(row(1, 40L)).get();
            writer.flush();
            TableBucket bucket = new TableBucket(tableId, committedWrite.getBucket().getBucket());
            Map<TableBucket, Long> committedBaseline =
                    Collections.singletonMap(bucket, committedWrite.getLogEndOffset());
            OperatorSubtaskState legacyCheckpoint = createLegacyCheckpoint(committedBaseline);

            writer.upsert(row(1, 7L)).get();
            writer.flush();
            assertThat(lookupAggregateValue(lookuper, 1)).isEqualTo(47L);

            try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                    createHarness(tablePath, schema.getRowType(), producerId)) {
                harness.initializeState(legacyCheckpoint);
                harness.open();

                assertThat(operator(harness).getBucketOffsets())
                        .containsExactlyInAnyOrderEntriesOf(committedBaseline);
                assertThat(lookupAggregateValue(lookuper, 1)).isEqualTo(40L);
            }
        } finally {
            admin.deleteProducerOffsets(producerId).get();
            admin.dropTable(tablePath, true).get();
        }
    }

    private static long lookupAggregateValue(Lookuper lookuper, int key) throws Exception {
        InternalRow result = lookuper.lookup(row(key)).get().getSingletonRow();
        assertThat(result).as("aggregate row for key %s", key).isNotNull();
        return result.getLong(1);
    }

    private static OperatorSubtaskState createLegacyCheckpoint(Map<TableBucket, Long> offsets)
            throws Exception {
        try (OneInputStreamOperatorTestHarness<Integer, Integer> harness =
                new OneInputStreamOperatorTestHarness<>(
                        new LegacyCheckpointStateOperator(offsets), MAX_PARALLELISM, 1, 0)) {
            harness.open();
            return harness.snapshot(1L, 1L);
        }
    }

    private static OneInputStreamOperatorTestHarness<Integer, Integer> createHarness(
            TablePath tablePath, RowType tableRowType, String producerId) throws Exception {
        Configuration configuration = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        UndoRecoveryOperatorFactory<Integer> factory =
                new UndoRecoveryOperatorFactory<>(
                        tablePath,
                        configuration,
                        tableRowType,
                        null,
                        NUM_BUCKETS,
                        false,
                        producerId,
                        RecoveryAction.UNDO,
                        1L,
                        1L);
        return new OneInputStreamOperatorTestHarness<>(factory, MAX_PARALLELISM, 1, 0);
    }

    @SuppressWarnings("unchecked")
    private static UndoRecoveryOperator<Integer> operator(
            OneInputStreamOperatorTestHarness<Integer, Integer> harness) {
        return (UndoRecoveryOperator<Integer>) harness.getOperator();
    }

    private static final class LegacyCheckpointStateOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private static final long serialVersionUID = 1L;

        private final Map<TableBucket, Long> offsets;

        private LegacyCheckpointStateOperator(Map<TableBucket, Long> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void initializeState(StateInitializationContext context) throws Exception {
            super.initializeState(context);
            ListState<WriterState> undoState =
                    context.getOperatorStateStore()
                            .getUnionListState(
                                    new ListStateDescriptor<>(
                                            "undo_recovery_state", new WriterStateSerializer()));
            undoState.add(new WriterState(offsets));
        }

        @Override
        public void processElement(StreamRecord<Integer> element) {
            output.collect(element);
        }
    }
}
