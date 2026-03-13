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

package org.apache.fluss.flink.source;

import org.apache.fluss.client.initializer.OffsetsInitializer;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.source.testutils.MockDataUtils;
import org.apache.fluss.flink.source.testutils.Order;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.testutils.common.CommonTestUtils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** Integration tests for logic of backlog reporting. */
public class BacklogProcessingITCase extends FlinkTestBase {

    private static final String PK_TABLE_NAME = "backlog_test_table";
    private static final Schema PK_SCHEMA = MockDataUtils.getOrdersSchemaPK();
    private static final List<InternalRow> TEST_ROWS =
            Arrays.asList(
                    row(600L, 20L, 600, "addr1"),
                    row(700L, 22L, 601, "addr2"),
                    row(800L, 23L, 602, "addr3"),
                    row(900L, 24L, 603, "addr4"),
                    row(1000L, 25L, 604, "addr5"));

    private final TableDescriptor pkTableDescriptor =
            TableDescriptor.builder().schema(PK_SCHEMA).distributedBy(1, "orderId").build();
    private final TablePath tablePath = new TablePath(DEFAULT_DB, PK_TABLE_NAME);

    @TempDir File savepointDir;

    protected StreamExecutionEnvironment env;

    @BeforeEach
    public void before() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
    }

    @Test
    public void testBacklogEventsPropagation() throws Exception {
        BacklogCollectingOperator.reset();
        createTable(tablePath, pkTableDescriptor);

        writeRows(conn, tablePath, TEST_ROWS, false);

        Configuration flussConfig = new Configuration(clientConf);
        flussConfig.setString(FlinkConnectorOptions.SCAN_BACKLOG_REPORT_ENABLE.key(), "true");

        DataStreamSource<Order> stream =
                env.fromSource(
                        buildBacklogSource(PK_TABLE_NAME, flussConfig),
                        WatermarkStrategy.noWatermarks(),
                        "BacklogTestSource");

        stream.transform(
                        "BacklogRecordCollector",
                        TypeInformation.of(Order.class),
                        new BacklogCollectingOperator<>())
                .executeAndCollect("BacklogProcessingTest");

        CommonTestUtils.waitUntil(
                () -> BacklogCollectingOperator.getCollectedRecordCount() == 5,
                Duration.ofSeconds(5),
                "Expected 5 records to be collected");

        // The source should emit isBacklog=true during backlog consumption,
        // then transition to isBacklog=false after backlog is fully consumed.
        CommonTestUtils.waitUntil(
                () ->
                        BacklogCollectingOperator.getCollectedRecordAttributes().stream()
                                .anyMatch(attr -> !attr.isBacklog()),
                Duration.ofSeconds(15),
                "Expected at least one RecordAttributes with isBacklog=false");

        boolean hasBacklogTrue =
                BacklogCollectingOperator.getCollectedRecordAttributes().stream()
                        .anyMatch(RecordAttributes::isBacklog);
        assertThat(hasBacklogTrue).isTrue();
    }

    @Test
    public void testBacklogReportingDisabled() throws Exception {
        BacklogCollectingOperator.reset();
        createTable(tablePath, pkTableDescriptor);
        writeRows(conn, tablePath, TEST_ROWS, false);

        // Backlog reporting is disabled by default (no flussConfig).
        DataStreamSource<Order> stream =
                env.fromSource(
                        buildBacklogSource(PK_TABLE_NAME, null),
                        WatermarkStrategy.noWatermarks(),
                        "BacklogDisabledSource");

        stream.transform(
                        "BacklogRecordCollector",
                        TypeInformation.of(Order.class),
                        new BacklogCollectingOperator<>())
                .executeAndCollect("BacklogDisabledTest");

        CommonTestUtils.waitUntil(
                () -> BacklogCollectingOperator.getCollectedRecordCount() == 5,
                Duration.ofSeconds(5),
                "Expected 5 records to be collected");

        List<RecordAttributes> backlogEvents =
                BacklogCollectingOperator.getCollectedRecordAttributes();
        boolean hasBacklogTrue = backlogEvents.stream().anyMatch(RecordAttributes::isBacklog);
        assertThat(hasBacklogTrue).isFalse();
    }

    /** Verifies that restoring from a savepoint does not re-enter backlog processing. */
    @Test
    public void testBacklogStateRestoredFromSavepoint() throws Exception {
        createTable(tablePath, pkTableDescriptor);
        writeRows(conn, tablePath, TEST_ROWS, false);

        Configuration flussConfig = new Configuration(clientConf);
        flussConfig.setString(FlinkConnectorOptions.SCAN_BACKLOG_REPORT_ENABLE.key(), "true");

        // Phase 1: consume backlog, then stop with savepoint.
        BacklogCollectingOperator.reset();

        env.fromSource(
                        buildBacklogSource(PK_TABLE_NAME, flussConfig),
                        WatermarkStrategy.noWatermarks(),
                        "BacklogSavepointSource")
                .transform(
                        "BacklogRecordCollector",
                        TypeInformation.of(Order.class),
                        new BacklogCollectingOperator<>())
                .sinkTo(new DiscardingSink<>());

        JobClient phase1Job = env.executeAsync("BacklogSavepointPhase1");

        CommonTestUtils.waitUntil(
                () -> BacklogCollectingOperator.getCollectedRecordCount() == 5,
                Duration.ofSeconds(5),
                "Phase 1: expected at least 5 records");

        CommonTestUtils.waitUntil(
                () ->
                        BacklogCollectingOperator.getCollectedRecordAttributes().stream()
                                .anyMatch(attr -> !attr.isBacklog()),
                Duration.ofSeconds(15),
                "Phase 1: expected isBacklog=false");

        String savepointPath =
                phase1Job
                        .stopWithSavepoint(
                                false,
                                savepointDir.getAbsolutePath(),
                                SavepointFormatType.CANONICAL)
                        .get();

        // Phase 2: restore from savepoint, verify no isBacklog=true is emitted.
        BacklogCollectingOperator.reset();
        writeRows(conn, tablePath, TEST_ROWS, false);

        org.apache.flink.configuration.Configuration restoreConf =
                new org.apache.flink.configuration.Configuration();
        restoreConf.setString("execution.savepoint.path", savepointPath);
        StreamExecutionEnvironment restoreEnv =
                StreamExecutionEnvironment.getExecutionEnvironment(restoreConf);
        restoreEnv.setParallelism(1);

        restoreEnv
                .fromSource(
                        buildBacklogSource(PK_TABLE_NAME, flussConfig),
                        WatermarkStrategy.noWatermarks(),
                        "BacklogSavepointSource")
                .transform(
                        "BacklogRecordCollector",
                        TypeInformation.of(Order.class),
                        new BacklogCollectingOperator<>())
                .sinkTo(new DiscardingSink<>());

        JobClient phase2Job = restoreEnv.executeAsync("BacklogSavepointPhase2");

        CommonTestUtils.waitUntil(
                () -> BacklogCollectingOperator.getCollectedRecordCount() >= 5,
                Duration.ofSeconds(5),
                "Phase 2: expected at least 5 records after restore");

        assertThat(BacklogCollectingOperator.getCollectedRecordAttributes()).isEmpty();

        phase2Job.cancel().get();
    }

    private FlussSource<Order> buildBacklogSource(String tableName, Configuration flussConfig) {
        FlussSourceBuilder<Order> builder =
                FlussSource.<Order>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setDatabase(DEFAULT_DB)
                        .setTable(tableName)
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setScanPartitionDiscoveryIntervalMs(1000L)
                        .setDeserializationSchema(new MockDataUtils.OrderDeserializationSchema());
        if (flussConfig != null) {
            builder.setFlussConfig(flussConfig);
        }
        return builder.build();
    }

    /**
     * A mock operator that collects both data records and {@link RecordAttributes} events.
     *
     * <p>Uses static collections because Flink serializes operators before sending them to
     * TaskManagers. The deserialized instance running in the task is different from the original
     * instance in the test method, so instance-level fields would not be shared.
     */
    private static class BacklogCollectingOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T> {

        private static final long serialVersionUID = 1L;

        private static final List<RecordAttributes> COLLECTED_RECORD_ATTRIBUTES =
                new CopyOnWriteArrayList<>();
        private static final List<Object> COLLECTED_RECORDS = new CopyOnWriteArrayList<>();

        static void reset() {
            COLLECTED_RECORD_ATTRIBUTES.clear();
            COLLECTED_RECORDS.clear();
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            COLLECTED_RECORDS.add(element.getValue());
            output.collect(element);
        }

        @Override
        public void processRecordAttributes(RecordAttributes recordAttributes) throws Exception {
            COLLECTED_RECORD_ATTRIBUTES.add(recordAttributes);
            super.processRecordAttributes(recordAttributes);
        }

        static int getCollectedRecordCount() {
            return COLLECTED_RECORDS.size();
        }

        static List<RecordAttributes> getCollectedRecordAttributes() {
            return Collections.unmodifiableList(new ArrayList<>(COLLECTED_RECORD_ATTRIBUTES));
        }
    }
}
