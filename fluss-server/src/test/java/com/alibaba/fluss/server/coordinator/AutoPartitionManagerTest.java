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

package com.alibaba.fluss.server.coordinator;

import com.alibaba.fluss.cluster.TabletServerInfo;
import com.alibaba.fluss.config.AutoPartitionTimeUnit;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.server.testutils.TestingServerMetadataCache;
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
import org.junit.jupiter.api.Test;
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
import static com.alibaba.fluss.server.utils.TableAssignmentUtils.generateAssignment;
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
        metadataManager = new MetadataManager(zookeeperClient, new Configuration());
    }

    @AfterEach
    void afterEach() {
        ZOO_KEEPER_EXTENSION_WRAPPER.getCustomExtension().cleanupRoot();
    }

    static Stream<Arguments> parameters() {
        // numPreCreate = 4 (for table with single partition key only), numRetention = 2
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
                        TestParams.builder(AutoPartitionTimeUnit.HOUR, true)
                                .startTime("2024-09-10T01:00:00")
                                // table with multiple partition keys not supports automatic
                                // creation
                                .expectedPartitions()
                                .manualCreatedPartitions(
                                        "2024091001$A",
                                        "2024091001$B",
                                        "2024091001$C",
                                        "2024091002$A",
                                        "2024091002$B",
                                        "2024091003$A",
                                        "2024091004$A",
                                        "2024091005$A")
                                .manualDroppedPartitions("2024091002$A")
                                .advanceClock(c -> c.plusHours(3))
                                // current time partition is "2024091004"
                                .expectedPartitionsAfterAdvance(
                                        "2024091002$B",
                                        "2024091003$A",
                                        "2024091004$A",
                                        "2024091005$A")
                                .advanceClock2(c -> c.plusHours(2))
                                // current time partition is "2024091006"
                                .expectedPartitionsFinal("2024091004$A", "2024091005$A")
                                .build()),
                Arguments.of(
                        TestParams.builder(AutoPartitionTimeUnit.DAY)
                                .startTime("2024-09-10T00:00:00")
                                .expectedPartitions("20240910", "20240911", "20240912", "20240913")
                                .manualCreatedPartition("20240915")
                                .manualDroppedPartition("20240910")
                                // plus 23 hours to make sure the partition can be created since
                                // we introduce jitter for create day partition
                                .advanceClock(c -> c.plusDays(3).plus(Duration.ofHours(23)))
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
                        TestParams.builder(AutoPartitionTimeUnit.DAY, true)
                                .startTime("2024-09-10T00:00:00")
                                .expectedPartitions()
                                .manualCreatedPartitions(
                                        "20240910$A",
                                        "20240910$B",
                                        "20240910$C",
                                        "20240911$A",
                                        "20240911$B",
                                        "20240912$A",
                                        "20240913$A",
                                        "20240914$A")
                                .manualDroppedPartition("20240911$A")
                                .advanceClock(c -> c.plusDays(3).plus(Duration.ofHours(23)))
                                .expectedPartitionsAfterAdvance(
                                        "20240911$B", "20240912$A", "20240913$A", "20240914$A")
                                .advanceClock2(c -> c.plusDays(2))
                                .expectedPartitionsFinal("20240913$A", "20240914$A")
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
                        TestParams.builder(AutoPartitionTimeUnit.MONTH, true)
                                .startTime("2024-09-10T00:00:00")
                                .expectedPartitions()
                                .manualCreatedPartitions(
                                        "202409$A",
                                        "202409$B",
                                        "202409$C",
                                        "202410$A",
                                        "202410$B",
                                        "202411$A",
                                        "202412$A",
                                        "202413$A")
                                .manualDroppedPartition("202410$A")
                                .advanceClock(c -> c.plusMonths(3))
                                // current partition is "202412"
                                .expectedPartitionsAfterAdvance(
                                        "202410$B", "202411$A", "202412$A", "202413$A")
                                .advanceClock2(c -> c.plusMonths(2))
                                .expectedPartitionsFinal("202412$A", "202413$A")
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
                        TestParams.builder(AutoPartitionTimeUnit.QUARTER, true)
                                .startTime("2024-09-10T00:00:00")
                                .expectedPartitions()
                                .manualCreatedPartitions(
                                        "20243$A", "20243$B", "20243$C", "20244$A", "20244$B",
                                        "20251$A", "20252$A", "20253$B", "20254$C")
                                .manualDroppedPartition("20243$A")
                                .advanceClock(c -> c.plusMonths(3 * 3))
                                // current partition is "20252"
                                .expectedPartitionsAfterAdvance(
                                        "20244$A", "20244$B", "20251$A", "20252$A", "20253$B",
                                        "20254$C")
                                .advanceClock2(c -> c.plusMonths(2 * 3))
                                .expectedPartitionsFinal("20252$A", "20253$B", "20254$C")
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
                                .build()),
                Arguments.of(
                        TestParams.builder(AutoPartitionTimeUnit.YEAR, true)
                                .startTime("2024-09-10T00:00:00")
                                .expectedPartitions()
                                .manualCreatedPartitions(
                                        "2024$A", "2024$B", "2024$C", "2025$A", "2025$B", "2026$A",
                                        "2027$B", "2028$C")
                                .manualDroppedPartition("2025$B")
                                .advanceClock(c -> c.plusYears(3))
                                // current partition is "2027", retain "2025", "2026"
                                .expectedPartitionsAfterAdvance(
                                        "2025$A", "2026$A", "2027$B", "2028$C")
                                .advanceClock2(c -> c.plusYears(2))
                                .expectedPartitionsFinal("2027$B", "2028$C")
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
                        new TestingServerMetadataCache(3),
                        new MetadataManager(zookeeperClient, new Configuration()),
                        new Configuration(),
                        clock,
                        periodicExecutor);
        autoPartitionManager.start();

        TableInfo table =
                createPartitionedTable(2, 4, params.timeUnit, params.multiplePartitionKeys);
        TablePath tablePath = table.getTablePath();
        autoPartitionManager.addAutoPartitionTable(table, true);
        // the first auto-partition task is a non-periodic task
        periodicExecutor.triggerNonPeriodicScheduledTask();

        Map<String, Long> partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        // pre-create 4 partitions including current partition
        assertThat(partitions.keySet()).containsExactlyInAnyOrder(params.expectedPartitions);

        int replicaFactor = table.getTableConfig().getReplicationFactor();
        Map<Integer, BucketAssignment> bucketAssignments =
                generateAssignment(
                                table.getNumBuckets(),
                                replicaFactor,
                                new TabletServerInfo[] {
                                    new TabletServerInfo(0, "rack0"),
                                    new TabletServerInfo(1, "rack1"),
                                    new TabletServerInfo(2, "rack2")
                                })
                        .getBucketAssignments();
        long tableId = table.getTableId();
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableId, bucketAssignments);

        // manually create partitions.
        for (String partitionName : params.manualCreatedPartitions) {
            metadataManager.createPartition(
                    tablePath,
                    tableId,
                    partitionAssignment,
                    fromPartitionName(table.getPartitionKeys(), partitionName),
                    false);
            // mock the partition is created in zk.
            autoPartitionManager.addPartition(tableId, partitionName);
        }

        // manually drop partitions.
        for (String partitionName : params.manualDroppedPartitions) {
            metadataManager.dropPartition(
                    tablePath, fromPartitionName(table.getPartitionKeys(), partitionName), false);
            // mock the partition is dropped in zk.
            autoPartitionManager.removePartition(tableId, partitionName);
        }

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

    @Test
    void testMaxPartitions() throws Exception {
        int expectPartitionNumber = 10;
        Configuration config = new Configuration();
        config.set(ConfigOptions.MAX_PARTITION_NUM, expectPartitionNumber);
        MetadataManager metadataManager = new MetadataManager(zookeeperClient, config);

        ZonedDateTime startTime =
                LocalDateTime.parse("2024-09-10T00:00:00").atZone(ZoneId.systemDefault());
        long startMs = startTime.toInstant().toEpochMilli();
        ManualClock clock = new ManualClock(startMs);
        ManuallyTriggeredScheduledExecutorService periodicExecutor =
                new ManuallyTriggeredScheduledExecutorService();

        AutoPartitionManager autoPartitionManager =
                new AutoPartitionManager(
                        new TestingServerMetadataCache(3),
                        metadataManager,
                        new Configuration(),
                        clock,
                        periodicExecutor);
        autoPartitionManager.start();

        // create a partitioned with -1 retention to never auto-drop partitions
        TableInfo table = createPartitionedTable(-1, 4, AutoPartitionTimeUnit.DAY);
        TablePath tablePath = table.getTablePath();
        autoPartitionManager.addAutoPartitionTable(table, true);
        // when the partitioned table is added, the partition crate task should be scheduled
        // immediately
        periodicExecutor.triggerNonPeriodicScheduledTask();

        Map<String, Long> partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        // pre-create 4 partitions including current partition
        assertThat(partitions.keySet())
                .containsExactlyInAnyOrder("20240910", "20240911", "20240912", "20240913");

        // manually create 4 future partitions.
        int replicaFactor = table.getTableConfig().getReplicationFactor();
        Map<Integer, BucketAssignment> bucketAssignments =
                generateAssignment(
                                table.getNumBuckets(),
                                replicaFactor,
                                new TabletServerInfo[] {
                                    new TabletServerInfo(0, "rack0"),
                                    new TabletServerInfo(1, "rack1"),
                                    new TabletServerInfo(2, "rack2")
                                })
                        .getBucketAssignments();
        long tableId = table.getTableId();
        PartitionAssignment partitionAssignment =
                new PartitionAssignment(tableId, bucketAssignments);
        for (int i = 20250101; i <= 20250104; i++) {
            metadataManager.createPartition(
                    tablePath,
                    tableId,
                    partitionAssignment,
                    fromPartitionName(table.getPartitionKeys(), i + ""),
                    false);
            // mock the partition is created in zk.
            autoPartitionManager.addPartition(tableId, i + "");
        }

        // make sure the partitions can be created automatically
        clock.advanceTime(Duration.ofDays(4).plusHours(23));
        periodicExecutor.triggerPeriodicScheduledTasks();
        partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        assertThat(partitions.keySet())
                .containsExactlyInAnyOrder(
                        "20240910",
                        "20240911",
                        "20240912",
                        "20240913",
                        // only 20240914, 20240915 are created in this round
                        "20240914",
                        "20240915",
                        // 20250101 ~ 20250102 are retained
                        "20250101",
                        "20250102",
                        "20250103",
                        "20250104");
    }

    @Test
    void testAutoCreateDayPartitionShouldJitter() throws Exception {
        ZonedDateTime startTime =
                LocalDateTime.parse("2025-04-19T00:00:00").atZone(ZoneId.systemDefault());
        long startMs = startTime.toInstant().toEpochMilli();
        ManualClock clock = new ManualClock(startMs);
        ManuallyTriggeredScheduledExecutorService periodicExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        AutoPartitionManager autoPartitionManager =
                new AutoPartitionManager(
                        new TestingServerMetadataCache(3),
                        metadataManager,
                        new Configuration(),
                        clock,
                        periodicExecutor);
        autoPartitionManager.start();

        // create one day partition table
        TableInfo table = createPartitionedTable(-1, 4, AutoPartitionTimeUnit.DAY);
        TablePath tablePath = table.getTablePath();
        autoPartitionManager.addAutoPartitionTable(table, true);
        periodicExecutor.triggerNonPeriodicScheduledTasks();
        Map<String, Long> partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        // pre-create 4 partitions including current partition
        assertThat(partitions.keySet())
                .containsExactlyInAnyOrder("20250419", "20250420", "20250422", "20250421");

        Integer delayInMinutes =
                autoPartitionManager.getAutoCreateDayDelayMinutes(table.getTableId());
        // advance 1 day + (delayInMinutes - 1), should still no next partition to create
        // since the current minutes in day don't advance the delayInMinutes
        clock.advanceTime(Duration.ofDays(1).plusMinutes(delayInMinutes - 1));
        periodicExecutor.triggerPeriodicScheduledTasks();
        partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        assertThat(partitions.keySet())
                .containsExactlyInAnyOrder("20250419", "20250420", "20250422", "20250421");

        // now, advance a minutes again, should create a new partition since
        // the current minutes in day advance the delayInMinutes
        clock.advanceTime(Duration.ofMinutes(1));
        periodicExecutor.triggerPeriodicScheduledTasks();
        partitions = zookeeperClient.getPartitionNameAndIds(tablePath);
        assertThat(partitions.keySet())
                .containsExactlyInAnyOrder(
                        "20250419", "20250420", "20250421", "20250422", "20250423");
    }

    /**
     * Test if AutoPartionManager.createPartition adheres to maxBucketLimit while adding new
     * parition automatically, skip if it breaches limit.
     */
    @Test
    void testMaxBucketNum() throws Exception {

        int bucketCountPerPartition = 10;
        int maxBucketNum = 30; // Allow only 3 partitions with 10 buckets each

        Configuration config = new Configuration();
        config.set(ConfigOptions.MAX_BUCKET_NUM, maxBucketNum);
        MetadataManager metadataManager = new MetadataManager(zookeeperClient, config);

        ZonedDateTime startTime =
                LocalDateTime.parse("2025-04-26T00:00:00").atZone(ZoneId.systemDefault());
        long startMs = startTime.toInstant().toEpochMilli();
        ManualClock clock = new ManualClock(startMs);
        ManuallyTriggeredScheduledExecutorService periodicExecutor =
                new ManuallyTriggeredScheduledExecutorService();

        AutoPartitionManager autoPartitionManager =
                new AutoPartitionManager(
                        new TestingServerMetadataCache(3),
                        metadataManager,
                        config,
                        clock,
                        periodicExecutor);
        autoPartitionManager.start();

        // Create a partitioned table with 10 buckets per partition and no auto-drop
        TableInfo table =
                createPartitionedTableWithBuckets(
                        -1, 4, AutoPartitionTimeUnit.DAY, bucketCountPerPartition);
        TablePath tablePath = table.getTablePath();
        autoPartitionManager.addAutoPartitionTable(table, true);
        // Trigger immediate partition creation
        periodicExecutor.triggerNonPeriodicScheduledTask();

        int partitionsNum = zookeeperClient.getPartitionNumber(tablePath);
        // Only 3 partitions should be created (3 * 10 = 30 buckets) out of the 4 requested
        assertThat(partitionsNum).isEqualTo(3);

        // Advance time to trigger another auto-partition cycle
        clock.advanceTime(Duration.ofDays(1).plusHours(23));
        periodicExecutor.triggerPeriodicScheduledTasks();

        // Check partitions again - should still have only 3 due to bucket limit
        partitionsNum = zookeeperClient.getPartitionNumber(tablePath);
        assertThat(partitionsNum).isEqualTo(3);
    }

    private static class TestParams {
        final AutoPartitionTimeUnit timeUnit;
        final boolean multiplePartitionKeys;
        final long startTimeMs;
        final String[] manualCreatedPartitions;
        final String[] manualDroppedPartitions;
        final String[] expectedPartitions;
        final Duration advanceDuration;
        final String[] expectedPartitionsAfterAdvance;
        final Duration advanceDuration2;
        final String[] expectedPartitionsFinal;

        private TestParams(
                AutoPartitionTimeUnit timeUnit,
                boolean multiplePartitionKeys,
                long startTimeMs,
                String[] manualCreatedPartitions,
                String[] manualDroppedPartitions,
                String[] expectedPartitions,
                Duration advanceDuration,
                String[] expectedPartitionsAfterAdvance,
                Duration advanceDuration2,
                String[] expectedPartitionsFinal) {
            this.timeUnit = timeUnit;
            this.multiplePartitionKeys = multiplePartitionKeys;
            this.startTimeMs = startTimeMs;
            this.manualCreatedPartitions = manualCreatedPartitions;
            this.manualDroppedPartitions = manualDroppedPartitions;
            this.expectedPartitions = expectedPartitions;
            this.advanceDuration = advanceDuration;
            this.expectedPartitionsAfterAdvance = expectedPartitionsAfterAdvance;
            this.advanceDuration2 = advanceDuration2;
            this.expectedPartitionsFinal = expectedPartitionsFinal;
        }

        @Override
        public String toString() {
            return timeUnit.toString()
                    + " | "
                    + (multiplePartitionKeys ? "Multiple Partition Keys" : "Single Partition Key");
        }

        static TestParamsBuilder builder(AutoPartitionTimeUnit timeUnit) {
            return new TestParamsBuilder(timeUnit, false);
        }

        static TestParamsBuilder builder(
                AutoPartitionTimeUnit timeUnit, boolean multiplePartitionKeys) {
            return new TestParamsBuilder(timeUnit, multiplePartitionKeys);
        }
    }

    private static class TestParamsBuilder {
        AutoPartitionTimeUnit timeUnit;
        boolean multiplePartitionKeys;
        ZonedDateTime startTime;
        String[] expectedPartitions;
        String[] manualCreatedPartitions;
        String[] manualDroppedPartitions;
        long advanceSeconds;
        String[] expectedPartitionsAfterAdvance;
        long advanceSeconds2;
        String[] expectedPartitionsFinal;

        TestParamsBuilder(AutoPartitionTimeUnit timeUnit, boolean multiplePartitionKeys) {
            this.timeUnit = timeUnit;
            this.multiplePartitionKeys = multiplePartitionKeys;
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
            this.manualCreatedPartitions = new String[] {manualCreatedPartition};
            return this;
        }

        public TestParamsBuilder manualCreatedPartitions(String... manualCreatedPartitions) {
            this.manualCreatedPartitions = manualCreatedPartitions;
            return this;
        }

        public TestParamsBuilder manualDroppedPartition(String manualDroppedPartition) {
            this.manualDroppedPartitions = new String[] {manualDroppedPartition};
            return this;
        }

        public TestParamsBuilder manualDroppedPartitions(String... manualDroppedPartitions) {
            this.manualDroppedPartitions = manualDroppedPartitions;
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
                    multiplePartitionKeys,
                    startTime.toInstant().toEpochMilli(),
                    manualCreatedPartitions,
                    manualDroppedPartitions,
                    expectedPartitions,
                    Duration.ofSeconds(advanceSeconds),
                    expectedPartitionsAfterAdvance,
                    Duration.ofSeconds(advanceSeconds2),
                    expectedPartitionsFinal);
        }
    }

    // -------------------------------------------------------------------------------------------

    private TableInfo createPartitionedTable(
            int partitionRetentionNum, int partitionPreCreateNum, AutoPartitionTimeUnit timeUnit)
            throws Exception {
        return createPartitionedTable(
                partitionRetentionNum, partitionPreCreateNum, timeUnit, false);
    }

    private TableInfo createPartitionedTable(
            int partitionRetentionNum,
            int partitionPreCreateNum,
            AutoPartitionTimeUnit timeUnit,
            boolean multiplePartitionKeys)
            throws Exception {
        long tableId = 1;
        TablePath tablePath =
                multiplePartitionKeys
                        ? TablePath.of("db", "test_multiple_partition_keys_" + UUID.randomUUID())
                        : TablePath.of("db", "test_partition_" + UUID.randomUUID());
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("dt", DataTypes.STRING())
                                        .column("a", DataTypes.BIGINT())
                                        .column("b", DataTypes.BIGINT())
                                        .column("ts", DataTypes.TIMESTAMP())
                                        .primaryKey(
                                                multiplePartitionKeys
                                                        ? new String[] {"id", "dt", "a", "b"}
                                                        : new String[] {"id", "dt"})
                                        .build())
                        .comment(
                                multiplePartitionKeys
                                        ? "partitioned table with multiple partition keys"
                                        : "partitioned table")
                        .distributedBy(16)
                        .partitionedBy(
                                multiplePartitionKeys
                                        ? new String[] {"dt", "a"}
                                        : new String[] {"dt"})
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_KEY, "dt")
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, timeUnit)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION,
                                partitionRetentionNum)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE,
                                multiplePartitionKeys ? 0 : partitionPreCreateNum)
                        .build();
        long currentMillis = System.currentTimeMillis();
        TableInfo tableInfo =
                TableInfo.of(tablePath, tableId, 1, descriptor, currentMillis, currentMillis);
        TableRegistration registration = TableRegistration.newTable(tableId, descriptor);
        zookeeperClient.registerTable(tablePath, registration);
        return tableInfo;
    }

    /**
     * Helper method creates a partitioned table with the specified number of buckets per partition.
     */
    private TableInfo createPartitionedTableWithBuckets(
            int partitionRetentionNum,
            int partitionPreCreateNum,
            AutoPartitionTimeUnit timeUnit,
            int bucketCount)
            throws Exception {
        long tableId = 1;
        TablePath tablePath = TablePath.of("db", "test_bucket_limit_" + UUID.randomUUID());
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(
                                Schema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("name", DataTypes.STRING())
                                        .column("dt", DataTypes.STRING())
                                        .column("ts", DataTypes.TIMESTAMP())
                                        .primaryKey("id", "dt")
                                        .build())
                        .comment("partitioned table with bucket limit")
                        .distributedBy(bucketCount) // Specify bucket count here
                        .partitionedBy("dt")
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 3)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_ENABLED, true)
                        .property(ConfigOptions.TABLE_AUTO_PARTITION_TIME_UNIT, timeUnit)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_NUM_RETENTION,
                                partitionRetentionNum)
                        .property(
                                ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE,
                                partitionPreCreateNum)
                        .build();
        long currentMillis = System.currentTimeMillis();
        TableInfo tableInfo =
                TableInfo.of(tablePath, tableId, 1, descriptor, currentMillis, currentMillis);
        TableRegistration registration = TableRegistration.newTable(tableId, descriptor);
        zookeeperClient.registerTable(tablePath, registration);
        return tableInfo;
    }
}
