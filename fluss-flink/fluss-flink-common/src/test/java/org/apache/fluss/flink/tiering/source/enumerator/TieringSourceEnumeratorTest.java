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

package org.apache.fluss.flink.tiering.source.enumerator;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.event.TieringFailOverEvent;
import org.apache.fluss.flink.tiering.source.TieringTestBase;
import org.apache.fluss.flink.tiering.source.split.TieringLogSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSnapshotSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSplit;
import org.apache.fluss.flink.tiering.source.split.TieringSplitGenerator;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotInfo;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.config.ConfigOptions.TABLE_AUTO_PARTITION_NUM_PRECREATE;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link TieringSourceEnumerator} and {@link TieringSplitGenerator}. */
class TieringSourceEnumeratorTest extends TieringTestBase {

    private static Configuration flussConf;

    @BeforeAll
    protected static void beforeAll() {
        TieringTestBase.beforeAll();
        flussConf = new Configuration(clientConf);
    }

    @BeforeEach
    protected void beforeEach() throws Exception {
        super.beforeEach();
    }

    @Test
    void testPrimaryKeyTableWithNoSnapshotSplits() throws Throwable {
        TablePath tablePath = DEFAULT_TABLE_PATH;
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numSubtasks = 4;
        int expectNumberOfSplits = 3;
        // test get snapshot split & log split and the assignment
        try (MockSplitEnumeratorContext<TieringSplit> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            TieringSourceEnumerator enumerator =
                    new TieringSourceEnumerator(flussConf, context, 500);

            enumerator.start();
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // register all readers
            for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
                registerReader(context, enumerator, subtaskId, "localhost-" + subtaskId);
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }

            // try to assign splits
            context.runPeriodicCallable(0);

            List<TieringSplit> actualAssignment = new ArrayList<>();
            context.getSplitsAssignmentSequence()
                    .forEach(a -> a.assignment().values().forEach(actualAssignment::addAll));

            // no split assignment for empty buckets
            assertThat(actualAssignment).isEmpty();

            // mock finished tiered this round, check second round
            context.getSplitsAssignmentSequence().clear();
            final Map<Integer, Long> bucketOffsetOfEarliest = new HashMap<>();
            final Map<Integer, Long> bucketOffsetOfInitialWrite = new HashMap<>();
            for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                bucketOffsetOfEarliest.put(tableBucket, EARLIEST_OFFSET);
                bucketOffsetOfInitialWrite.put(tableBucket, 0L);
            }
            // commit and notify this table tiering task finished
            coordinatorGateway
                    .commitLakeTableSnapshot(
                            genCommitLakeTableSnapshotRequest(
                                    tableId,
                                    null,
                                    0,
                                    bucketOffsetOfEarliest,
                                    bucketOffsetOfInitialWrite))
                    .get();

            enumerator.handleSourceEvent(1, new FinishedTieringEvent(tableId));

            Map<Integer, Long> bucketOffsetOfSecondWrite =
                    upsertRow(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR, 0, 10);
            long snapshotId = 0;
            waitUntilSnapshot(tableId, snapshotId);

            // request tiering table splits
            for (int subtaskId = 0; subtaskId < 3; subtaskId++) {
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }
            waitUntilTieringTableSplitAssignmentReady(context, DEFAULT_BUCKET_NUM, 500L);

            Map<Integer, List<TieringSplit>> expectedLogAssignment = new HashMap<>();
            for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                expectedLogAssignment.put(
                        tableBucket,
                        Collections.singletonList(
                                new TieringLogSplit(
                                        tablePath,
                                        new TableBucket(tableId, tableBucket),
                                        null,
                                        bucketOffsetOfInitialWrite.get(tableBucket),
                                        bucketOffsetOfInitialWrite.get(tableBucket)
                                                + bucketOffsetOfSecondWrite.get(tableBucket),
                                        expectNumberOfSplits)));
            }
            Map<Integer, List<TieringSplit>> actualLogAssignment = new HashMap<>();
            for (SplitsAssignment<TieringSplit> a : context.getSplitsAssignmentSequence()) {
                actualLogAssignment.putAll(a.assignment());
            }
            assertThat(actualLogAssignment).isEqualTo(expectedLogAssignment);
        }
    }

    @Test
    void testPrimaryKeyTableWithSnapshotSplits() throws Throwable {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "tiering-test-pk-table");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numSubtasks = 3;
        final Map<Integer, Long> bucketOffsetOfInitialWrite =
                upsertRow(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR, 0, 10);
        long snapshotId = 0;
        waitUntilSnapshot(tableId, snapshotId);

        int expectNumberOfSplits = 3;

        // test get snapshot split assignment
        try (MockSplitEnumeratorContext<TieringSplit> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            TieringSourceEnumerator enumerator =
                    new TieringSourceEnumerator(flussConf, context, 500);

            enumerator.start();
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // register all readers
            for (int subtaskId = 0; subtaskId < 3; subtaskId++) {
                registerReader(context, enumerator, subtaskId, "localhost-" + subtaskId);
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }
            waitUntilTieringTableSplitAssignmentReady(context, DEFAULT_BUCKET_NUM, 3000L);

            Map<Integer, List<TieringSplit>> expectedSnapshotAssignment = new HashMap<>();
            for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                expectedSnapshotAssignment.put(
                        tableBucket,
                        Collections.singletonList(
                                new TieringSnapshotSplit(
                                        tablePath,
                                        new TableBucket(tableId, tableBucket),
                                        null,
                                        snapshotId,
                                        bucketOffsetOfInitialWrite.get(tableBucket),
                                        expectNumberOfSplits)));
            }
            Map<Integer, List<TieringSplit>> actualSnapshotAssignment = new HashMap<>();
            for (SplitsAssignment<TieringSplit> a : context.getSplitsAssignmentSequence()) {
                actualSnapshotAssignment.putAll(a.assignment());
            }
            assertThat(actualSnapshotAssignment).isEqualTo(expectedSnapshotAssignment);

            // mock finished tiered this round, check second round
            context.getSplitsAssignmentSequence().clear();
            final Map<Integer, Long> initialBucketOffsets = new HashMap<>();
            for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                initialBucketOffsets.put(tableBucket, EARLIEST_OFFSET);
            }
            // commit and notify this table tiering task finished
            coordinatorGateway
                    .commitLakeTableSnapshot(
                            genCommitLakeTableSnapshotRequest(
                                    tableId,
                                    null,
                                    1,
                                    initialBucketOffsets,
                                    bucketOffsetOfInitialWrite))
                    .get();

            enumerator.handleSourceEvent(1, new FinishedTieringEvent(tableId));

            Map<Integer, Long> bucketOffsetOfSecondWrite =
                    upsertRow(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR, 10, 20);
            snapshotId = 1;
            waitUntilSnapshot(tableId, snapshotId);

            // request tiering table splits
            for (int subtaskId = 0; subtaskId < 3; subtaskId++) {
                String hostName = "localhost-" + subtaskId;
                enumerator.handleSplitRequest(subtaskId, hostName);
            }

            // three log splits will be ready soon
            waitUntilTieringTableSplitAssignmentReady(context, DEFAULT_BUCKET_NUM, 500L);
            Map<Integer, List<TieringSplit>> expectedLogAssignment = new HashMap<>();
            for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                expectedLogAssignment.put(
                        tableBucket,
                        Collections.singletonList(
                                new TieringLogSplit(
                                        tablePath,
                                        new TableBucket(tableId, tableBucket),
                                        null,
                                        bucketOffsetOfInitialWrite.get(tableBucket),
                                        bucketOffsetOfInitialWrite.get(tableBucket)
                                                + bucketOffsetOfSecondWrite.get(tableBucket),
                                        expectNumberOfSplits)));
            }
            Map<Integer, List<TieringSplit>> actualLogAssignment = new HashMap<>();
            for (SplitsAssignment<TieringSplit> a : context.getSplitsAssignmentSequence()) {
                actualLogAssignment.putAll(a.assignment());
            }
            assertThat(actualLogAssignment).isEqualTo(expectedLogAssignment);
        }
    }

    @Test
    void testLogTableSplits() throws Throwable {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "tiering-test-log-table");
        long tableId = createTable(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR);
        int numSubtasks = 4;
        int expectNumberOfSplits = 3;
        // test get log split and the assignment
        try (MockSplitEnumeratorContext<TieringSplit> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            TieringSourceEnumerator enumerator =
                    new TieringSourceEnumerator(flussConf, context, 500);

            enumerator.start();
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // register all readers
            for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
                registerReader(context, enumerator, subtaskId, "localhost-" + subtaskId);
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }

            // write one row to one bucket, keep the other buckets empty
            Map<Integer, Long> bucketOffsetOfFirstWrite =
                    appendRow(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR, 0, 1);

            // only non-empty buckets should generate splits
            waitUntilTieringTableSplitAssignmentReady(context, 1, 200L);
            List<TieringSplit> expectedAssignment = new ArrayList<>();
            for (int bucketId : bucketOffsetOfFirstWrite.keySet()) {
                expectedAssignment.add(
                        new TieringLogSplit(
                                tablePath,
                                new TableBucket(tableId, bucketId),
                                null,
                                EARLIEST_OFFSET,
                                bucketOffsetOfFirstWrite.get(bucketId),
                                bucketOffsetOfFirstWrite.size()));
            }
            List<TieringSplit> actualAssignment = new ArrayList<>();
            context.getSplitsAssignmentSequence()
                    .forEach(a -> a.assignment().values().forEach(actualAssignment::addAll));

            assertThat(actualAssignment).isEqualTo(expectedAssignment);

            // mock finished tiered this round, check second round
            context.getSplitsAssignmentSequence().clear();
            final Map<Integer, Long> bucketOffsetOfEarliest = new HashMap<>();
            final Map<Integer, Long> bucketOffsetOfInitialWrite = new HashMap<>();
            for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                bucketOffsetOfEarliest.put(tableBucket, EARLIEST_OFFSET);
                bucketOffsetOfInitialWrite.put(
                        tableBucket, bucketOffsetOfFirstWrite.getOrDefault(tableBucket, 0L));
            }
            // commit and notify this table tiering task finished
            coordinatorGateway
                    .commitLakeTableSnapshot(
                            genCommitLakeTableSnapshotRequest(
                                    tableId,
                                    null,
                                    0,
                                    bucketOffsetOfEarliest,
                                    bucketOffsetOfInitialWrite))
                    .get();
            enumerator.handleSourceEvent(1, new FinishedTieringEvent(tableId));

            // write rows to every bucket
            Map<Integer, Long> bucketOffsetOfSecondWrite =
                    appendRow(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR, 1, 10);

            // request tiering table splits
            for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }

            waitUntilTieringTableSplitAssignmentReady(context, DEFAULT_BUCKET_NUM, 500L);

            Map<Integer, List<TieringSplit>> expectedLogAssignment = new HashMap<>();
            for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                expectedLogAssignment.put(
                        tableBucket,
                        Collections.singletonList(
                                new TieringLogSplit(
                                        tablePath,
                                        new TableBucket(tableId, tableBucket),
                                        null,
                                        bucketOffsetOfInitialWrite.get(tableBucket),
                                        bucketOffsetOfInitialWrite.get(tableBucket)
                                                + bucketOffsetOfSecondWrite.get(tableBucket),
                                        expectNumberOfSplits)));
            }
            Map<Integer, List<TieringSplit>> actualLogAssignment = new HashMap<>();
            for (SplitsAssignment<TieringSplit> a : context.getSplitsAssignmentSequence()) {
                actualLogAssignment.putAll(a.assignment());
            }
            assertThat(actualLogAssignment).isEqualTo(expectedLogAssignment);
        }
    }

    @Test
    void testPartitionedPrimaryKeyTable() throws Throwable {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "tiering-test-partitioned-pk-table");
        long tableId =
                createPartitionedTable(tablePath, DEFAULT_AUTO_PARTITIONED_PK_TABLE_DESCRIPTOR);
        Map<String, Long> partitionNameByIds =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(
                        tablePath, TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue());

        int numSubtasks = 6;
        int expectNumberOfSplits = 6;
        // test get snapshot split assignment
        try (MockSplitEnumeratorContext<TieringSplit> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            TieringSourceEnumerator enumerator =
                    new TieringSourceEnumerator(flussConf, context, 500);

            enumerator.start();
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // register all readers
            for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
                registerReader(context, enumerator, subtaskId, "localhost-" + subtaskId);
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }

            // try to assign splits
            context.runPeriodicCallable(0);

            List<TieringSplit> actualSnapshotAssignment = new ArrayList<>();
            for (SplitsAssignment<TieringSplit> splitsAssignment :
                    context.getSplitsAssignmentSequence()) {
                splitsAssignment.assignment().values().forEach(actualSnapshotAssignment::addAll);
            }

            // no snapshot split should be assigned for empty buckets
            assertThat(actualSnapshotAssignment).isEmpty();

            // mock finished tiered this round, check second round
            context.getSplitsAssignmentSequence().clear();
            final Map<Long, Map<Integer, Long>> bucketOffsetOfInitialWrite = new HashMap<>();
            for (Map.Entry<String, Long> partitionNameById : partitionNameByIds.entrySet()) {
                Map<Integer, Long> partitionBucketOffsetOfEarliest = new HashMap<>();
                Map<Integer, Long> partitionBucketOffsetOfInitialWrite = new HashMap<>();
                for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                    partitionBucketOffsetOfEarliest.put(tableBucket, EARLIEST_OFFSET);
                    partitionBucketOffsetOfInitialWrite.put(tableBucket, 0L);
                }
                bucketOffsetOfInitialWrite.put(
                        partitionNameById.getValue(), partitionBucketOffsetOfInitialWrite);
                // commit lake table partition
                coordinatorGateway
                        .commitLakeTableSnapshot(
                                genCommitLakeTableSnapshotRequest(
                                        tableId,
                                        partitionNameById.getValue(),
                                        1,
                                        partitionBucketOffsetOfEarliest,
                                        bucketOffsetOfInitialWrite.get(
                                                partitionNameById.getValue())))
                        .get();
            }
            // notify this table tiering task finished
            enumerator.handleSourceEvent(1, new FinishedTieringEvent(tableId));

            Map<Long, Map<Integer, Long>> bucketOffsetOfSecondWrite =
                    upsertRowForPartitionedTable(
                            tablePath, DEFAULT_PK_TABLE_DESCRIPTOR, partitionNameByIds, 10, 20);
            long snapshotId = 0;
            waitUntilPartitionTableSnapshot(tableId, partitionNameByIds, snapshotId);

            // request tiering table splits
            for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }

            waitUntilTieringTableSplitAssignmentReady(
                    context, DEFAULT_BUCKET_NUM * partitionNameByIds.size(), 500L);
            List<TieringSplit> expectedLogAssignment = new ArrayList<>();
            for (Map.Entry<String, Long> partitionNameById : partitionNameByIds.entrySet()) {
                for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                    long partitionId = partitionNameById.getValue();
                    expectedLogAssignment.add(
                            new TieringLogSplit(
                                    tablePath,
                                    new TableBucket(tableId, partitionId, tableBucket),
                                    partitionNameById.getKey(),
                                    bucketOffsetOfInitialWrite.get(partitionId).get(tableBucket),
                                    bucketOffsetOfInitialWrite.get(partitionId).get(tableBucket)
                                            + bucketOffsetOfSecondWrite
                                                    .get(partitionId)
                                                    .get(tableBucket),
                                    expectNumberOfSplits));
                }
            }
            List<TieringSplit> actualLogAssignment = new ArrayList<>();
            for (SplitsAssignment<TieringSplit> splitsAssignment :
                    context.getSplitsAssignmentSequence()) {
                splitsAssignment.assignment().values().forEach(actualLogAssignment::addAll);
            }
            assertThat(sortSplits(actualLogAssignment))
                    .isEqualTo(sortSplits(expectedLogAssignment));
        }
    }

    @Test
    void testPartitionedLogTableSplits() throws Throwable {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "tiering-test-partitioned-log-table");
        long tableId =
                createPartitionedTable(tablePath, DEFAULT_AUTO_PARTITIONED_LOG_TABLE_DESCRIPTOR);
        Map<String, Long> partitionNameByIds =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionsCreated(
                        tablePath, TABLE_AUTO_PARTITION_NUM_PRECREATE.defaultValue());

        int numSubtasks = 6;
        int expectNumberOfSplits = 6;
        // test get log split assignment
        try (MockSplitEnumeratorContext<TieringSplit> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            TieringSourceEnumerator enumerator =
                    new TieringSourceEnumerator(flussConf, context, 500);

            enumerator.start();
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // register all readers
            for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
                registerReader(context, enumerator, subtaskId, "localhost-" + subtaskId);
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }

            Map<Long, Map<Integer, Long>> bucketOffsetOfFirstWrite =
                    appendRowForPartitionedTable(
                            tablePath,
                            DEFAULT_AUTO_PARTITIONED_LOG_TABLE_DESCRIPTOR,
                            partitionNameByIds,
                            0,
                            1);

            waitUntilTieringTableSplitAssignmentReady(context, partitionNameByIds.size(), 3000L);

            List<TieringSplit> expectedAssignment = new ArrayList<>();
            for (Map.Entry<String, Long> partitionNameById : partitionNameByIds.entrySet()) {
                long partitionId = partitionNameById.getValue();
                for (int tableBucket : bucketOffsetOfFirstWrite.get(partitionId).keySet()) {
                    expectedAssignment.add(
                            new TieringLogSplit(
                                    tablePath,
                                    new TableBucket(tableId, partitionId, tableBucket),
                                    partitionNameById.getKey(),
                                    EARLIEST_OFFSET,
                                    bucketOffsetOfFirstWrite.get(partitionId).get(tableBucket),
                                    bucketOffsetOfFirstWrite.size()));
                }
            }
            List<TieringSplit> actualAssignment = new ArrayList<>();
            for (SplitsAssignment<TieringSplit> splitsAssignment :
                    context.getSplitsAssignmentSequence()) {
                splitsAssignment.assignment().values().forEach(actualAssignment::addAll);
            }
            assertThat(sortSplits(actualAssignment)).isEqualTo(sortSplits(expectedAssignment));

            // mock finished tiered this round, check second round
            context.getSplitsAssignmentSequence().clear();
            final Map<Long, Map<Integer, Long>> bucketOffsetOfInitialWrite = new HashMap<>();
            for (Map.Entry<String, Long> partitionNameById : partitionNameByIds.entrySet()) {
                long partitionId = partitionNameById.getValue();
                Map<Integer, Long> partitionInitialBucketOffsets = new HashMap<>();
                Map<Integer, Long> partitionBucketOffsetOfInitialWrite = new HashMap<>();
                for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                    partitionInitialBucketOffsets.put(tableBucket, EARLIEST_OFFSET);
                    partitionBucketOffsetOfInitialWrite.put(
                            tableBucket,
                            bucketOffsetOfFirstWrite
                                    .getOrDefault(partitionId, Collections.emptyMap())
                                    .getOrDefault(tableBucket, 0L));
                }
                bucketOffsetOfInitialWrite.put(partitionId, partitionBucketOffsetOfInitialWrite);
                // commit lake table partition
                coordinatorGateway
                        .commitLakeTableSnapshot(
                                genCommitLakeTableSnapshotRequest(
                                        tableId,
                                        partitionId,
                                        1,
                                        partitionInitialBucketOffsets,
                                        bucketOffsetOfInitialWrite.get(partitionId)))
                        .get();
            }
            // notify this table tiering task finished
            enumerator.handleSourceEvent(1, new FinishedTieringEvent(tableId));

            Map<Long, Map<Integer, Long>> bucketOffsetOfSecondWrite =
                    appendRowForPartitionedTable(
                            tablePath,
                            DEFAULT_AUTO_PARTITIONED_LOG_TABLE_DESCRIPTOR,
                            partitionNameByIds,
                            1,
                            10);

            // request tiering table splits
            for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }

            waitUntilTieringTableSplitAssignmentReady(
                    context, DEFAULT_BUCKET_NUM * partitionNameByIds.size(), 500L);
            List<TieringSplit> expectedLogAssignment = new ArrayList<>();
            for (Map.Entry<String, Long> partitionNameById : partitionNameByIds.entrySet()) {
                for (int tableBucket = 0; tableBucket < DEFAULT_BUCKET_NUM; tableBucket++) {
                    long partionId = partitionNameById.getValue();
                    expectedLogAssignment.add(
                            new TieringLogSplit(
                                    tablePath,
                                    new TableBucket(tableId, partionId, tableBucket),
                                    partitionNameById.getKey(),
                                    bucketOffsetOfInitialWrite.get(partionId).get(tableBucket),
                                    bucketOffsetOfInitialWrite.get(partionId).get(tableBucket)
                                            + bucketOffsetOfSecondWrite
                                                    .get(partionId)
                                                    .get(tableBucket),
                                    expectNumberOfSplits));
                }
            }
            List<TieringSplit> actualLogAssignment = new ArrayList<>();
            for (SplitsAssignment<TieringSplit> splitsAssignment :
                    context.getSplitsAssignmentSequence()) {
                splitsAssignment.assignment().values().forEach(actualLogAssignment::addAll);
            }
            assertThat(sortSplits(actualLogAssignment))
                    .isEqualTo(sortSplits(expectedLogAssignment));
        }
    }

    @Test
    void testHandleFailedTieringTableEvent() throws Throwable {
        TablePath tablePath = TablePath.of(DEFAULT_DB, "tiering-fail-test-log-table");
        long tableId = createTable(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR);
        int numSubtasks = 4;
        int expectNumberOfSplits = 3;
        Map<Integer, Long> bucketOffsetOfWrite =
                appendRow(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR, 0, 10);
        // test get log split and the assignment
        try (MockSplitEnumeratorContext<TieringSplit> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            TieringSourceEnumerator enumerator =
                    new TieringSourceEnumerator(flussConf, context, 500);

            enumerator.start();
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // register all readers
            for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
                registerReader(context, enumerator, subtaskId, "localhost-" + subtaskId);
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }
            waitUntilTieringTableSplitAssignmentReady(context, DEFAULT_BUCKET_NUM, 200);

            List<TieringSplit> expectedAssignment = new ArrayList<>();
            for (int bucketId = 0; bucketId < DEFAULT_BUCKET_NUM; bucketId++) {
                expectedAssignment.add(
                        new TieringLogSplit(
                                tablePath,
                                new TableBucket(tableId, bucketId),
                                null,
                                EARLIEST_OFFSET,
                                bucketOffsetOfWrite.get(bucketId),
                                expectNumberOfSplits));
            }
            List<TieringSplit> actualAssignment = new ArrayList<>();
            context.getSplitsAssignmentSequence()
                    .forEach(a -> a.assignment().values().forEach(actualAssignment::addAll));

            assertThat(actualAssignment).isEqualTo(expectedAssignment);

            // mock tiering fail by send tiering fail event
            context.getSplitsAssignmentSequence().clear();
            enumerator.handleSourceEvent(1, new FailedTieringEvent(tableId, "test_reason"));

            // request tiering table splits, should get splits
            for (int subtaskId = 0; subtaskId < numSubtasks; subtaskId++) {
                enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            }
            waitUntilTieringTableSplitAssignmentReady(context, DEFAULT_BUCKET_NUM, 500L);
            List<TieringSplit> actualAssignment1 = new ArrayList<>();
            context.getSplitsAssignmentSequence()
                    .forEach(a -> a.assignment().values().forEach(actualAssignment1::addAll));
            assertThat(actualAssignment1).isEqualTo(expectedAssignment);
        }
    }

    @Test
    void testHandleFailOverEvent() throws Throwable {
        TablePath tablePath1 = TablePath.of(DEFAULT_DB, "tiering-failover-test-log-table1");
        createTable(tablePath1, DEFAULT_LOG_TABLE_DESCRIPTOR);
        appendRow(tablePath1, DEFAULT_LOG_TABLE_DESCRIPTOR, 0, 10);

        TablePath tablePath2 = TablePath.of(DEFAULT_DB, "tiering-failover-test-log-table2");
        createTable(tablePath2, DEFAULT_LOG_TABLE_DESCRIPTOR);
        appendRow(tablePath2, DEFAULT_LOG_TABLE_DESCRIPTOR, 0, 10);

        int numSubtasks = 1;
        try (MockSplitEnumeratorContext<TieringSplit> context =
                new MockSplitEnumeratorContext<>(numSubtasks)) {
            TieringSourceEnumerator enumerator =
                    new TieringSourceEnumerator(flussConf, context, 500);

            enumerator.start();
            assertThat(context.getSplitsAssignmentSequence()).isEmpty();

            // register one reader
            int subtaskId = 0;
            registerReader(context, enumerator, subtaskId, "localhost-" + subtaskId);

            // handle split request
            enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);

            // should get one tiering split, and the split is for tablePath1
            verifyTieringSplitAssignment(context, 1, tablePath1);

            // clean assignment
            context.getSplitsAssignmentSequence().clear();

            // enumerator handle TieringFailOverEvent, which will mark current tiering tablePath1 as
            // fail, and all pending splits should be clear
            enumerator.handleSourceEvent(subtaskId, new TieringFailOverEvent());

            // handle split request
            enumerator.handleSplitRequest(subtaskId, "localhost-" + subtaskId);
            // now, should get another one tiering split, the split is for tablePath2 since all
            // pending split for tablePath1 is clear
            verifyTieringSplitAssignment(context, 1, tablePath2);
        }
    }

    private static CommitLakeTableSnapshotRequest genCommitLakeTableSnapshotRequest(
            long tableId,
            @Nullable Long partitionId,
            long snapshotId,
            Map<Integer, Long> bucketLogStartOffsets,
            Map<Integer, Long> bucketLogEndOffsets) {
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotInfo reqForTable = commitLakeTableSnapshotRequest.addTablesReq();
        reqForTable.setTableId(tableId);
        reqForTable.setSnapshotId(snapshotId);
        for (Map.Entry<Integer, Long> bucketLogStartOffset : bucketLogStartOffsets.entrySet()) {
            int bucketId = bucketLogStartOffset.getKey();
            TableBucket tb = new TableBucket(tableId, partitionId, bucketId);
            PbLakeTableOffsetForBucket lakeTableOffsetForBucket = reqForTable.addBucketsReq();
            if (tb.getPartitionId() != null) {
                lakeTableOffsetForBucket.setPartitionId(tb.getPartitionId());
            }
            lakeTableOffsetForBucket.setBucketId(tb.getBucket());
            lakeTableOffsetForBucket.setLogStartOffset(bucketLogStartOffset.getValue());
            lakeTableOffsetForBucket.setLogEndOffset(bucketLogEndOffsets.get(bucketId));
        }
        return commitLakeTableSnapshotRequest;
    }

    // --------------------- Test Utils ---------------------
    private void registerReader(
            MockSplitEnumeratorContext<TieringSplit> context,
            TieringSourceEnumerator enumerator,
            int readerId,
            String hostname) {
        context.registerReader(new ReaderInfo(readerId, hostname));
        enumerator.addReader(readerId);
    }

    private void waitUntilTieringTableSplitAssignmentReady(
            MockSplitEnumeratorContext<TieringSplit> context, int expectedSplitsNum, long sleepMs)
            throws Throwable {
        while (context.getSplitsAssignmentSequence().size() < expectedSplitsNum) {
            if (!context.getPeriodicCallables().isEmpty()) {
                context.runPeriodicCallable(0);
            } else {
                context.runNextOneTimeCallable();
            }
            Thread.sleep(sleepMs);
        }
    }

    private static List<TieringSplit> sortSplits(List<TieringSplit> splits) {
        return splits.stream()
                .sorted(Comparator.comparing(Object::toString))
                .collect(Collectors.toList());
    }

    private void verifyTieringSplitAssignment(
            MockSplitEnumeratorContext<TieringSplit> context,
            int expectedSplitSize,
            TablePath expectedTablePath)
            throws Throwable {
        waitUntilTieringTableSplitAssignmentReady(context, 1, 200);
        List<SplitsAssignment<TieringSplit>> actualAssignment =
                context.getSplitsAssignmentSequence();

        List<TieringSplit> allTieringSplits =
                actualAssignment.stream()
                        .flatMap(assignments -> assignments.assignment().values().stream())
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        assertThat(allTieringSplits).hasSize(expectedSplitSize);
        assertThat(allTieringSplits)
                .allMatch(tieringSplit -> tieringSplit.getTablePath().equals(expectedTablePath));
    }
}
