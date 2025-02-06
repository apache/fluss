/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.TestingMetadataCache;
import com.alibaba.fluss.server.utils.TableAssignmentUtils;
import com.alibaba.fluss.server.zk.NOPErrorHandler;
import com.alibaba.fluss.server.zk.ZooKeeperClient;
import com.alibaba.fluss.server.zk.ZooKeeperExtension;
import com.alibaba.fluss.server.zk.data.BucketAssignment;
import com.alibaba.fluss.server.zk.data.PartitionAssignment;
import com.alibaba.fluss.server.zk.data.TableRegistration;
import com.alibaba.fluss.testutils.common.AllCallbackWrapper;
import com.alibaba.fluss.testutils.common.ManuallyTriggeredScheduledExecutorService;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.clock.ManualClock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.alibaba.fluss.metadata.ResolvedPartitionSpec.fromPartitionName;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link AutoPartitionManager}. */
class AutoPartitionManagerTest {

    @RegisterExtension
    public static final AllCallbackWrapper<ZooKeeperExtension> ZOO_KEEPER_EXTENSION_WRAPPER =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    protected static ZooKeeperClient zookeeperClient;
    private static MetadataManager metadataManager;

    @BeforeAll
    static void beforeAll() {
        zookeeperClient =
                ZOO_KEEPER_EXTENSION_WRAPPER
                        .getCustomExtension()
                        .getZooKeeperClient(NOPErrorHandler.INSTANCE);
        metadataManager = new MetadataManager(zookeeperClient);
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    static Stream<Arguments> parameters() {
        // numPreCreate = 4, numRetention = 2
        return Stream.of(
                Arguments.of(
                        TestParams.builder(AutoPartitionTimeUnit.HOUR)
                                .startTime("2024-09-10T01:00:00")
                                .expectedPartitions(
                                        "2024091001", "2024091002", "2024091003", "2024091004")
                                .manualCreatedPartition("2024091006")
                                .manualDroppedPartition("2024091001")
                                .advanceClock(c -> c.plusHours(3))
                                // current partition is "2024091004"
                                .expectedPartitionsAfterAdvance(
                                        "2024091002",
                                        "2024091003",
                                        "2024091004",
                                        "2024091005",
                                        "2024091006",
                                        "2024091007")
                                .advanceClock2(c -> c.plusHours(2))
                                .expectedPartitionsFinal(
                                        "2024091004",
                                        "2024091005",
                                        "2024091006",
                                        "2024091007",
                                        "2024091008",
                                        "2024091009")
                                .build()),
                Arguments.of(
                        TestParams.builder(AutoPartitionTimeUnit.DAY)
                                .startTime("2024-09-10T00:00:00")
                                .expectedPartitions("20240910", "20240911", "20240912", "20240913")
                                .manualCreatedPartition("20240915")
                                .manualDroppedPartition("20240910")
                                .advanceClock(c -> c.plusDays(3))
                                // current partition is "20240913", retain "20240911", "20240912"
                                .expectedPartitionsAfterAdvance(
                                        "20240911",
                                        "20240912",
                                        "20240913",
                                        "20240914",
                                        "20240915",
                                        "20240916")
                                .advanceClock2(c -> c.plusDays(2))
                                .expectedPartitionsFinal(
                                        "20240913",
                                        "20240914",
                                        "20240915",
                                        "20240916",
                                        "20240917",
                                        "20240918")
                                .build()),
                Arguments.of(
                        TestParams.builder(AutoPartitionTimeUnit.MONTH)
                                .startTime("2024-09-10T00:00:00")
                                .expectedPartitions("202409", "202410", "202411", "202412")
                                .manualCreatedPartition("202502")
                                .manualDroppedPartition("202409")
                                .advanceClock(c -> c.plusMonths(3))
                                // current partition is "202412", retain "202410", "202411"
                                .expectedPartitionsAfterAdvance(
                                        "202410", "202411", "202412", "202501", "202502", "202503")
                                .advanceClock2(c -> c.plusMonths(2))
                                .expectedPartitionsFinal(
                                        "202412", "202501", "202502", "202503", "202504", "202505")
                                .build()),
                Arguments.of(
                        TestParams.builder(AutoPartitionTimeUnit.QUARTER)
                                .startTime("2024-09-10T00:00:00")
                                .manualCreatedPartition("20254")
                                .manualDroppedPartition("20243")
                                .expectedPartitions("20243", "20244", "20251", "20252")
                                .advanceClock(c -> c.plusMonths(3 * 3))
                                // current partition is "20253", retain "20251", "20252"
                                .expectedPartitionsAfterAdvance(
                                        "20244", "20251", "20252", "20253", "20254", "20261")
                                .advanceClock2(c -> c.plusMonths(2 * 3))
                                .expectedPartitionsFinal(
                                        "20252", "20253", "20254", "20261", "20262", "20263")
                                .build()),
                Arguments.of(
                        TestParams.builder(AutoPartitionTimeUnit.YEAR)
                                .startTime("2024-09-10T00:00:00")
                                .manualCreatedPartition("2029")
                                .manualDroppedPartition("2024")
                                .expectedPartitions("2024", "2025", "2026", "2027")
                                .advanceClock(c -> c.plusYears(3))
                                // current partition is "2027", retain "2025", "2026"
                                .expectedPartitionsAfterAdvance(
                                        "2025", "2026", "2027", "2028", "2029", "2030")
                                .advanceClock2(c -> c.plusYears(2))
                                .expectedPartitionsFinal(
                                        "2027", "2028", "2029", "2030", "2031", "2032")
                                .build()));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testAddPartitionedTable(TestParams params) throws Exception {
        ManualClock clock = new ManualClock(params.startTimeMs);
        ManuallyTriggeredScheduledExecutorService periodicExecutor =
                new ManuallyTriggeredScheduledExecutorService();

        AutoPartitionManager autoPartitionManager =
                new AutoPartitionManager(
                        new TestingMetadataCache(3),
                        new MetadataManager(zookeeperClient),
                        new Configuration(),
                        clock,
                        periodicExecutor);
        autoPartitionManager.start();

        TableInfo table = createPartitionedTable(params.timeUnit);
        TablePath tablePath = table.getTablePath();
        autoPartitionManager.addAutoPartitionTable(table);
        // the first auto-partition task is a non-periodic task
        periodicExecutor.triggerNonPeriodicScheduledTask();

        Map<String, Long> partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        // pre-create 4 partitions including current partition
        assertThat(partitions.keySet()).containsExactlyInAnyOrder(params.expectedPartitions);

        // manually create a partition.
        int replicaFactor = table.getTableConfig().getReplicationFactor();
        Map<Integer, BucketAssignment> bucketAssignments =
                TableAssignmentUtils.generateAssignment(
                                table.getNumBuckets(), replicaFactor, new int[] {0, 1, 2})
                        .getBucketAssignments();
        long tableId = table.getTableId();
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableId, bucketAssignments);
        metadataManager.createPartition(
                tablePath,
                tableId,
                partitionAssignment,
                fromPartitionName(table.getPartitionKeys(), params.manualCreatedPartition),
                false);
        // mock the partition is created in zk.
        autoPartitionManager.addPartition(tableId, params.manualCreatedPartition);

        // manually drop a partition.
        metadataManager.dropPartition(
                tablePath,
                fromPartitionName(table.getPartitionKeys(), params.manualDroppedPartition),
                false);
        // mock the partition is dropped in zk.
        autoPartitionManager.removePartition(tableId, params.manualDroppedPartition);

        clock.advanceTime(params.advanceDuration);
        periodicExecutor.triggerPeriodicScheduledTasks();
        partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        assertThat(partitions.keySet())
                .containsExactlyInAnyOrder(params.expectedPartitionsAfterAdvance);

        clock.advanceTime(params.advanceDuration2);
        periodicExecutor.triggerPeriodicScheduledTasks();
        partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        assertThat(partitions.keySet()).containsExactlyInAnyOrder(params.expectedPartitionsFinal);

        // trigger again at the same time, should be nothing changes
        periodicExecutor.triggerPeriodicScheduledTasks();
        partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        assertThat(partitions.keySet()).containsExactlyInAnyOrder(params.expectedPartitionsFinal);
    }

    private static class TestParams {
        final AutoPartitionTimeUnit timeUnit;
        final long startTimeMs;
        final String manualCreatedPartition;
        final String manualDroppedPartition;
        final String[] expectedPartitions;
        final Duration advanceDuration;
        final String[] expectedPartitionsAfterAdvance;
        final Duration advanceDuration2;
        final String[] expectedPartitionsFinal;

        private TestParams(
                AutoPartitionTimeUnit timeUnit,
                long startTimeMs,
                String manualCreatedPartition,
                String manualDroppedPartition,
                String[] expectedPartitions,
                Duration advanceDuration,
                String[] expectedPartitionsAfterAdvance,
                Duration advanceDuration2,
                String[] expectedPartitionsFinal) {
            this.timeUnit = timeUnit;
            this.startTimeMs = startTimeMs;
            this.manualCreatedPartition = manualCreatedPartition;
            this.manualDroppedPartition = manualDroppedPartition;
            this.expectedPartitions = expectedPartitions;
            this.advanceDuration = advanceDuration;
            this.expectedPartitionsAfterAdvance = expectedPartitionsAfterAdvance;
            this.advanceDuration2 = advanceDuration2;
            this.expectedPartitionsFinal = expectedPartitionsFinal;
        }

        @Override
        public String toString() {
            return timeUnit.toString();
        }

        static TestParamsBuilder builder(AutoPartitionTimeUnit timeUnit) {
            return new TestParamsBuilder(timeUnit);
        }
    }

    private static class TestParamsBuilder {
        AutoPartitionTimeUnit timeUnit;
        ZonedDateTime startTime;
        String[] expectedPartitions;
        String manualCreatedPartition;
        String manualDroppedPartition;
        long advanceSeconds;
        String[] expectedPartitionsAfterAdvance;
        long advanceSeconds2;
        String[] expectedPartitionsFinal;

        TestParamsBuilder(AutoPartitionTimeUnit timeUnit) {
            this.timeUnit = timeUnit;
        }

        public TestParamsBuilder startTime(String startTime) {
            this.startTime = LocalDateTime.parse(startTime).atZone(ZoneId.systemDefault());
            return this;
        }

        public TestParamsBuilder expectedPartitions(String... expectedPartitions) {
            this.expectedPartitions = expectedPartitions;
            return this;
        }

        public TestParamsBuilder manualCreatedPartition(String manualCreatedPartition) {
            this.manualCreatedPartition = manualCreatedPartition;
            return this;
        }

        public TestParamsBuilder manualDroppedPartition(String manualDroppedPartition) {
            this.manualDroppedPartition = manualDroppedPartition;
            return this;
        }

        public TestParamsBuilder advanceClock(Function<ZonedDateTime, ZonedDateTime> advance) {
            ZonedDateTime newDateTime = advance.apply(startTime);
            this.advanceSeconds =
                    newDateTime.toInstant().getEpochSecond()
                            - startTime.toInstant().getEpochSecond();
            return this;
        }

        public TestParamsBuilder expectedPartitionsAfterAdvance(
                String... expectedPartitionsAfterAdvance) {
            this.expectedPartitionsAfterAdvance = expectedPartitionsAfterAdvance;
            return this;
        }

        public TestParamsBuilder advanceClock2(Function<ZonedDateTime, ZonedDateTime> advance) {
            ZonedDateTime newDateTime = advance.apply(startTime.plusSeconds(advanceSeconds));
            this.advanceSeconds2 =
                    newDateTime.toInstant().getEpochSecond()
                            - startTime.toInstant().getEpochSecond()
                            - advanceSeconds;
            return this;
        }

        public TestParamsBuilder expectedPartitionsFinal(String... expectedPartitionsFinal) {
            this.expectedPartitionsFinal = expectedPartitionsFinal;
            return this;
        }

        public TestParams build() {
            return new TestParams(
                    timeUnit,
                    startTime.toInstant().toEpochMilli(),
                    manualCreatedPartition,
                    manualDroppedPartition,
                    expectedPartitions,
                    Duration.ofSeconds(advanceSeconds),
                    expectedPartitionsAfterAdvance,
                    Duration.ofSeconds(advanceSeconds2),
                    expectedPartitionsFinal);
        }
    }

    // -------------------------------------------------------------------------------------------

    private TableInfo createPartitionedTable(AutoPartitionTimeUnit timeUnit) throws Exception {
        long tableId = 1;
        TablePath tablePath = TablePath.of("db", "test_partition_" + UUID.randomUUID());
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("dt", DataTypes.STRING())
                                        .column("a", DataTypes.BIGINT())
                                        .column("ts", DataTypes.TIMESTAMP())
                                        .primaryKey("id", "dt")
                                        .build())
                        .comment("partitioned table")
                        .distributedBy(16)
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, timeUnit)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION, 2)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE, 4)
                        .build();
        long currentMillis = System.currentTimeMillis();
        TableInfo tableInfo =
                TableInfo.of(tablePath, tableId, 1, descriptor, currentMillis, currentMillis);
        TableRegistration registration = TableRegistration.newTable(tableId, descriptor);
        zookeeperClient.registerTable(tablePath, registration);
        return tableInfo;
    }
}
