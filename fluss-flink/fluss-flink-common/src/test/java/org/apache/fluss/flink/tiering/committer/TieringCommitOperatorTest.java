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

package org.apache.fluss.flink.tiering.committer;

import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.source.TableBucketWriteResult;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.server.zk.data.LakeTableSnapshot;
import org.apache.fluss.utils.types.Tuple2;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.MockOperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.SourceOperatorStreamTask;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** UT for {@link TieringCommitOperator}. */
class TieringCommitOperatorTest extends FlinkTestBase {

    private TieringCommitOperator<TestingWriteResult, TestingCommittable> committerOperator;
    private MockOperatorEventGateway mockOperatorEventGateway;
    private StreamOperatorParameters<CommittableMessage<TestingCommittable>> parameters;

    @BeforeEach
    void beforeEach() throws Exception {
        mockOperatorEventGateway = new MockOperatorEventGateway();
        MockOperatorEventDispatcher mockOperatorEventDispatcher =
                new MockOperatorEventDispatcher(mockOperatorEventGateway);
        parameters =
                new StreamOperatorParameters<>(
                        new SourceOperatorStreamTask<String>(new DummyEnvironment()),
                        new MockStreamConfig(new Configuration(), 1),
                        new MockOutput<>(new ArrayList<>()),
                        null,
                        mockOperatorEventDispatcher,
                        null);

        committerOperator =
                new TieringCommitOperator<>(
                        parameters,
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        new TestingLakeTieringFactory());
        committerOperator.open();
    }

    @AfterEach
    void afterEach() throws Exception {
        committerOperator.close();
    }

    @Test
    void testCommitNonPartitionedTable() throws Exception {
        TablePath tablePath1 = TablePath.of("fluss", "test1");
        TablePath tablePath2 = TablePath.of("fluss", "test2");

        long tableId1 = createTable(tablePath1, DEFAULT_PK_TABLE_DESCRIPTOR);
        long tableId2 = createTable(tablePath2, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numberOfWriteResults = 3;

        // table1, bucket 0
        TableBucket t1b0 = new TableBucket(tableId1, 0);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath1, t1b0, 1, 11, 21L, numberOfWriteResults));
        // table1, bucket 1
        TableBucket t1b1 = new TableBucket(tableId1, 1);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath1, t1b1, 2, 12, 22L, numberOfWriteResults));
        verifyNoLakeSnapshot(tablePath1);

        // table2, bucket0
        TableBucket t2b0 = new TableBucket(tableId2, 0);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath2, t2b0, 1, 21, 31L, numberOfWriteResults));

        // table2, bucket1
        TableBucket t2b1 = new TableBucket(tableId2, 1);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath2, t2b1, 2, 22, 32L, numberOfWriteResults));
        verifyNoLakeSnapshot(tablePath2);

        // add table1, bucket2
        TableBucket t1b2 = new TableBucket(tableId1, 2);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath1, t1b2, 3, 13, 23L, numberOfWriteResults));
        // verify lake snapshot
        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(t1b0, 11L);
        expectedLogEndOffsets.put(t1b1, 12L);
        expectedLogEndOffsets.put(t1b2, 13L);
        Map<TableBucket, Long> expectedMaxTimestamps = new HashMap<>();
        expectedMaxTimestamps.put(t1b0, 21L);
        expectedMaxTimestamps.put(t1b1, 22L);
        expectedMaxTimestamps.put(t1b2, 23L);
        verifyLakeSnapshot(tablePath1, tableId1, 1, expectedLogEndOffsets, expectedMaxTimestamps);

        // add table2, bucket2
        TableBucket t2b2 = new TableBucket(tableId2, 2);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath2, t2b2, 4, 23, 33L, numberOfWriteResults));
        expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(t2b0, 21L);
        expectedLogEndOffsets.put(t2b1, 22L);
        expectedLogEndOffsets.put(t2b2, 23L);
        expectedMaxTimestamps = new HashMap<>();
        expectedMaxTimestamps.put(t2b0, 31L);
        expectedMaxTimestamps.put(t2b1, 32L);
        expectedMaxTimestamps.put(t2b2, 33L);
        verifyLakeSnapshot(tablePath2, tableId2, 1, expectedLogEndOffsets, expectedMaxTimestamps);

        // let's process one round of TableBucketWriteResult again
        expectedLogEndOffsets = new HashMap<>();
        expectedMaxTimestamps = new HashMap<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId1, bucket);
            long offset = bucket * bucket;
            long timestamp = bucket * bucket;
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath1,
                            tableBucket,
                            bucket,
                            offset,
                            timestamp,
                            numberOfWriteResults));
            expectedLogEndOffsets.put(tableBucket, offset);
            expectedMaxTimestamps.put(tableBucket, timestamp);
        }
        verifyLakeSnapshot(tablePath1, tableId1, 1, expectedLogEndOffsets, expectedMaxTimestamps);
    }

    @Test
    void testCommitPartitionedTable() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "partitioned_table");
        long tableId = createTable(tablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);
        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        Map<TableBucket, Long> expectedMaxTimestamps = new HashMap<>();
        int numberOfWriteResults = 3 * partitionIdByNames.size();
        long offset = 0;
        long timestamp = System.currentTimeMillis();
        for (int bucket = 0; bucket < 3; bucket++) {
            for (Map.Entry<String, Long> partitionIdAndNameEntry : partitionIdByNames.entrySet()) {
                long partitionId = partitionIdAndNameEntry.getValue();
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
                long currentOffset = offset++;
                long currentTimestamp = timestamp++;
                committerOperator.processElement(
                        createTableBucketWriteResultStreamRecord(
                                tablePath,
                                tableBucket,
                                partitionIdAndNameEntry.getKey(),
                                1,
                                currentOffset,
                                currentTimestamp,
                                numberOfWriteResults));
                expectedLogEndOffsets.put(tableBucket, currentOffset);
                expectedMaxTimestamps.put(tableBucket, currentTimestamp);
            }
            if (bucket == 2) {
                verifyLakeSnapshot(
                        tablePath, tableId, 1, expectedLogEndOffsets, expectedMaxTimestamps);
            } else {
                verifyNoLakeSnapshot(tablePath);
            }
        }
    }

    @Test
    void testCommitMeetsEmptyWriteResult() throws Exception {
        TablePath tablePath1 = TablePath.of("fluss", "test_commit_empty_write_result");
        long tableId = createTable(tablePath1, DATA1_PARTITIONED_TABLE_DESCRIPTOR);
        int numberOfWriteResults = 3;

        // verify when all are empty
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath1, tableBucket, null, 3, 6L, numberOfWriteResults));
        }

        verifyNoLakeSnapshot(tablePath1);

        // verify when one bucket result is empty
        for (int bucket = 1; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath1,
                            tableBucket,
                            bucket,
                            // just use bucket as log offset
                            bucket,
                            bucket,
                            numberOfWriteResults));
        }
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath1,
                        new TableBucket(tableId, 0),
                        null,
                        3,
                        6L,
                        numberOfWriteResults));

        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(new TableBucket(tableId, 1), 1L);
        expectedLogEndOffsets.put(new TableBucket(tableId, 2), 2L);
        Map<TableBucket, Long> expectedMaxTimestamps = new HashMap<>();
        expectedMaxTimestamps.put(new TableBucket(tableId, 1), 1L);
        expectedMaxTimestamps.put(new TableBucket(tableId, 2), 2L);
        verifyLakeSnapshot(tablePath1, tableId, 1, expectedLogEndOffsets, expectedMaxTimestamps);
    }

    @Test
    void testTableCommitWhenFlussMissingLakeSnapshot() throws Exception {
        CommittedLakeSnapshot mockCommittedSnapshot =
                mockCommittedLakeSnapshot(Collections.singletonList(null), 2);
        TestingLakeTieringFactory.TestingLakeCommitter testingLakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter(mockCommittedSnapshot);
        committerOperator =
                new TieringCommitOperator<>(
                        parameters,
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        new TestingLakeTieringFactory(testingLakeCommitter));
        committerOperator.open();

        TablePath tablePath = TablePath.of("fluss", "test_commit_when_fluss_missing_lake_snapshot");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numberOfWriteResults = 3;

        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath, tableBucket, 3, 3, 3L, numberOfWriteResults));
        }

        verifyLakeSnapshot(
                tablePath,
                tableId,
                2,
                getExpectedLogEndOffsets(tableId, mockCommittedSnapshot),
                getExpectedMaxTimestamps(tableId, mockCommittedSnapshot),
                mockCommittedSnapshot.getQualifiedPartitionNameById(),
                String.format(
                        "The current Fluss's lake snapshot %s is less than lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d},"
                                + " missing snapshot: %s.",
                        null,
                        mockCommittedSnapshot.getLakeSnapshotId(),
                        tablePath,
                        tableId,
                        mockCommittedSnapshot));

        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        Map<TableBucket, Long> expectedMaxTimestamps = new HashMap<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            long offset = bucket * bucket;
            long timestamp = bucket * bucket;
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath, tableBucket, 3, offset, timestamp, numberOfWriteResults));
            expectedLogEndOffsets.put(tableBucket, offset);
            expectedMaxTimestamps.put(tableBucket, timestamp);
        }

        verifyLakeSnapshot(tablePath, tableId, 3, expectedLogEndOffsets, expectedMaxTimestamps);
    }

    @Test
    void testPartitionedTableCommitWhenFlussMissingLakeSnapshot() throws Exception {
        TablePath tablePath =
                TablePath.of(
                        "fluss", "test_commit_partitioned_table_when_fluss_missing_lake_snapshot");
        long tableId = createTable(tablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR);

        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);

        CommittedLakeSnapshot mockCommittedSnapshot =
                mockCommittedLakeSnapshot(Collections.singletonList(null), 3);
        TestingLakeTieringFactory.TestingLakeCommitter testingLakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter(mockCommittedSnapshot);
        committerOperator =
                new TieringCommitOperator<>(
                        parameters,
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        new TestingLakeTieringFactory(testingLakeCommitter));
        committerOperator.open();

        int numberOfWriteResults = 6;

        for (int bucket = 0; bucket < 3; bucket++) {
            for (String partitionName : partitionIdByNames.keySet()) {
                long partitionId = partitionIdByNames.get(partitionName);
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
                committerOperator.processElement(
                        createTableBucketWriteResultStreamRecord(
                                tablePath,
                                tableBucket,
                                partitionName,
                                3,
                                3,
                                3L,
                                numberOfWriteResults));
            }
        }

        verifyLakeSnapshot(
                tablePath,
                tableId,
                3,
                getExpectedLogEndOffsets(tableId, mockCommittedSnapshot),
                getExpectedMaxTimestamps(tableId, mockCommittedSnapshot),
                mockCommittedSnapshot.getQualifiedPartitionNameById(),
                String.format(
                        "The current Fluss's lake snapshot %s is less than lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d}, missing snapshot: %s.",
                        null,
                        mockCommittedSnapshot.getLakeSnapshotId(),
                        tablePath,
                        tableId,
                        mockCommittedSnapshot));
    }

    private CommittedLakeSnapshot mockCommittedLakeSnapshot(List<Long> partitions, int snapshotId) {
        CommittedLakeSnapshot mockCommittedSnapshot = new CommittedLakeSnapshot(snapshotId);
        for (Long partition : partitions) {
            for (int bucket = 0; bucket < DEFAULT_BUCKET_NUM; bucket++) {
                if (partition == null) {
                    mockCommittedSnapshot.addBucket(bucket, bucket + 1);
                } else {
                    mockCommittedSnapshot.addPartitionBucket(
                            partition,
                            ResolvedPartitionSpec.fromPartitionValue(
                                            "partition_key", "partition-" + partition)
                                    .getPartitionQualifiedName(),
                            bucket,
                            bucket + 1);
                }
            }
        }
        return mockCommittedSnapshot;
    }

    private Map<TableBucket, Long> getExpectedLogEndOffsets(
            long tableId, CommittedLakeSnapshot committedLakeSnapshot) {
        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        for (Map.Entry<Tuple2<Long, Integer>, Long> entry :
                committedLakeSnapshot.getLogEndOffsets().entrySet()) {
            Tuple2<Long, Integer> partitionBucket = entry.getKey();
            if (partitionBucket.f0 == null) {
                expectedLogEndOffsets.put(
                        new TableBucket(tableId, partitionBucket.f1), entry.getValue());
            } else {
                expectedLogEndOffsets.put(
                        new TableBucket(tableId, partitionBucket.f0, partitionBucket.f1),
                        entry.getValue());
            }
        }
        return expectedLogEndOffsets;
    }

    private Map<TableBucket, Long> getExpectedMaxTimestamps(
            long tableId, CommittedLakeSnapshot committedLakeSnapshot) {
        Map<TableBucket, Long> expectedMaxTimestamps = new HashMap<>();
        for (Map.Entry<Tuple2<Long, Integer>, Long> entry :
                committedLakeSnapshot.getLogEndOffsets().entrySet()) {
            Tuple2<Long, Integer> partitionBucket = entry.getKey();
            if (partitionBucket.f0 == null) {
                expectedMaxTimestamps.put(new TableBucket(tableId, partitionBucket.f1), -1L);
            } else {
                expectedMaxTimestamps.put(
                        new TableBucket(tableId, partitionBucket.f0, partitionBucket.f1), -1L);
            }
        }
        return expectedMaxTimestamps;
    }

    private StreamRecord<TableBucketWriteResult<TestingWriteResult>>
            createTableBucketWriteResultStreamRecord(
                    TablePath tablePath,
                    TableBucket tableBucket,
                    @Nullable Integer writeResult,
                    long logEndOffset,
                    long maxTimestamp,
                    int numberOfWriteResults) {
        return createTableBucketWriteResultStreamRecord(
                tablePath,
                tableBucket,
                null,
                writeResult,
                logEndOffset,
                maxTimestamp,
                numberOfWriteResults);
    }

    private StreamRecord<TableBucketWriteResult<TestingWriteResult>>
            createTableBucketWriteResultStreamRecord(
                    TablePath tablePath,
                    TableBucket tableBucket,
                    @Nullable String partitionName,
                    @Nullable Integer writeResult,
                    long logEndOffset,
                    long maxTimestamp,
                    int numberOfWriteResults) {
        TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath,
                        tableBucket,
                        partitionName,
                        writeResult == null ? null : new TestingWriteResult(writeResult),
                        logEndOffset,
                        maxTimestamp,
                        numberOfWriteResults);
        return new StreamRecord<>(tableBucketWriteResult);
    }

    private void verifyNoLakeSnapshot(TablePath tablePath) {
        assertThatThrownBy(() -> admin.getLatestLakeSnapshot(tablePath).get())
                .cause()
                .isInstanceOf(LakeTableSnapshotNotExistException.class);
    }

    private void verifyLakeSnapshot(
            TablePath tablePath,
            long tableId,
            long expectedSnapshotId,
            Map<TableBucket, Long> expectedLogEndOffsets,
            Map<TableBucket, Long> expectedMaxTimestamp)
            throws Exception {
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(expectedSnapshotId);
        assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedLogEndOffsets);

        // TODO: replace with LakeSnapshot when support max timestamp
        verifyLakeSnapshotMaxTimestamp(tableId, expectedMaxTimestamp);

        // check the tableId has been sent to mark finished
        List<OperatorEvent> operatorEvents = mockOperatorEventGateway.getEventsSent();
        SourceEventWrapper sourceEventWrapper =
                (SourceEventWrapper) operatorEvents.get(operatorEvents.size() - 1);
        FinishedTieringEvent finishedTieringEvent =
                (FinishedTieringEvent) sourceEventWrapper.getSourceEvent();
        assertThat(finishedTieringEvent.getTableId()).isEqualTo(tableId);
    }

    private void verifyLakeSnapshot(
            TablePath tablePath,
            long tableId,
            long expectedSnapshotId,
            Map<TableBucket, Long> expectedLogEndOffsets,
            Map<TableBucket, Long> expectedMaxTimestamp,
            Map<Long, String> expectedPartitionIdByName,
            String failedReason)
            throws Exception {
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(expectedSnapshotId);
        assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedLogEndOffsets);
        assertThat(lakeSnapshot.getPartitionNameById()).isEqualTo(expectedPartitionIdByName);

        // TODO: replace with LakeSnapshot when support max timestamp
        verifyLakeSnapshotMaxTimestamp(tableId, expectedMaxTimestamp);

        // check the tableId has been sent to mark failed
        List<OperatorEvent> operatorEvents = mockOperatorEventGateway.getEventsSent();
        SourceEventWrapper sourceEventWrapper =
                (SourceEventWrapper) operatorEvents.get(operatorEvents.size() - 1);
        FailedTieringEvent finishTieringEvent =
                (FailedTieringEvent) sourceEventWrapper.getSourceEvent();
        assertThat(finishTieringEvent.getTableId()).isEqualTo(tableId);
        assertThat(finishTieringEvent.failReason()).contains(failedReason);
    }

    private void verifyLakeSnapshotMaxTimestamp(
            long tableId, Map<TableBucket, Long> expectedMaxTimestamp) throws Exception {
        ZooKeeperClient zkClient = FLUSS_CLUSTER_EXTENSION.getZooKeeperClient();
        Optional<LakeTableSnapshot> lakeTableSnapshotOpt = zkClient.getLakeTableSnapshot(tableId);
        assertThat(lakeTableSnapshotOpt).isNotEmpty();
        LakeTableSnapshot lakeTableSnapshot = lakeTableSnapshotOpt.get();
        assertThat(lakeTableSnapshot.getBucketMaxTimestamp()).isEqualTo(expectedMaxTimestamp);
    }

    private static class MockOperatorEventDispatcher implements OperatorEventDispatcher {

        private final OperatorEventGateway operatorEventGateway;

        public MockOperatorEventDispatcher(MockOperatorEventGateway mockOperatorEventGateway) {
            this.operatorEventGateway = mockOperatorEventGateway;
        }

        @Override
        public void registerEventHandler(
                OperatorID operatorID, OperatorEventHandler operatorEventHandler) {
            // do nothing
        }

        @Override
        public OperatorEventGateway getOperatorEventGateway(OperatorID operatorID) {
            return operatorEventGateway;
        }
    }
}
