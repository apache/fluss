/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.exception.LakeTableSnapshotNotExistException;
import com.alibaba.fluss.flink.tiering.TestingLakeTieringFactory;
import com.alibaba.fluss.flink.tiering.TestingWriteResult;
import com.alibaba.fluss.flink.tiering.event.FailedTieringEvent;
import com.alibaba.fluss.flink.tiering.event.FinishedTieringEvent;
import com.alibaba.fluss.flink.tiering.source.TableBucketWriteResult;
import com.alibaba.fluss.flink.utils.FlinkTestBase;
import com.alibaba.fluss.lake.committer.CommittedLakeSnapshot;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.types.Tuple2;

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

import static com.alibaba.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
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
                        tablePath1, t1b0, 1, 11, numberOfWriteResults));
        // table1, bucket 1
        TableBucket t1b1 = new TableBucket(tableId1, 1);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath1, t1b1, 2, 12, numberOfWriteResults));
        verifyNoLakeSnapshot(tablePath1);

        // table2, bucket0
        TableBucket t2b0 = new TableBucket(tableId2, 0);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath2, t2b0, 1, 21, numberOfWriteResults));

        // table2, bucket1
        TableBucket t2b1 = new TableBucket(tableId2, 1);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath2, t2b1, 2, 22, numberOfWriteResults));
        verifyNoLakeSnapshot(tablePath2);

        // add table1, bucket2
        TableBucket t1b2 = new TableBucket(tableId1, 2);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath1, t1b2, 3, 13, numberOfWriteResults));
        // verify lake snapshot
        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(t1b0, 11L);
        expectedLogEndOffsets.put(t1b1, 12L);
        expectedLogEndOffsets.put(t1b2, 13L);
        verifyLakeSnapshot(tablePath1, tableId1, 1, expectedLogEndOffsets);

        // add table2, bucket2
        TableBucket t2b2 = new TableBucket(tableId2, 2);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath2, t2b2, 4, 23, numberOfWriteResults));
        expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(t2b0, 21L);
        expectedLogEndOffsets.put(t2b1, 22L);
        expectedLogEndOffsets.put(t2b2, 23L);
        verifyLakeSnapshot(tablePath2, tableId2, 1, expectedLogEndOffsets);

        // let's process one round of TableBucketWriteResult again
        expectedLogEndOffsets = new HashMap<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId1, bucket);
            long offset = bucket * bucket;
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath1, tableBucket, bucket, offset, numberOfWriteResults));
            expectedLogEndOffsets.put(tableBucket, offset);
        }
        verifyLakeSnapshot(tablePath1, tableId1, 1, expectedLogEndOffsets);
    }

    @Test
    void testCommitPartitionedTable() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "partitioned_table");
        long tableId = createTable(tablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR);
        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);
        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        int numberOfWriteResults = 3 * partitionIdByNames.size();
        long offset = 0;
        for (int bucket = 0; bucket < 3; bucket++) {
            for (Map.Entry<String, Long> partitionIdAndNameEntry : partitionIdByNames.entrySet()) {
                long partitionId = partitionIdAndNameEntry.getValue();
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
                long currentOffset = offset++;
                committerOperator.processElement(
                        createTableBucketWriteResultStreamRecord(
                                tablePath, tableBucket, 1, currentOffset, numberOfWriteResults));
                expectedLogEndOffsets.put(tableBucket, currentOffset);
            }
            if (bucket == 2) {
                verifyLakeSnapshot(tablePath, tableId, 1, expectedLogEndOffsets);
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
                            tablePath1, tableBucket, null, 3, numberOfWriteResults));
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
                            numberOfWriteResults));
        }
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath1, new TableBucket(tableId, 0), null, 3, numberOfWriteResults));

        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(new TableBucket(tableId, 1), 1L);
        expectedLogEndOffsets.put(new TableBucket(tableId, 2), 2L);
        verifyLakeSnapshot(tablePath1, tableId, 1, expectedLogEndOffsets);
    }

    @Test
    void testTableCommitWhenFlussMissingLakeSnapshot() throws Exception {
        CommittedLakeSnapshot mockCommitedSnapshot =
                mockCommittedLakeSnapshot(Collections.singletonList(null), 2);
        TestingLakeTieringFactory.TestingLakeCommitter testingLakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter(mockCommitedSnapshot);
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
                            tablePath, tableBucket, 3, 3, numberOfWriteResults));
        }

        verifyLakeSnapshot(
                tablePath,
                tableId,
                2,
                getExpectedLogEndOffsets(tableId, mockCommitedSnapshot, Collections.emptyMap()),
                String.format(
                        "The current Fluss's lake snapshot %d is less than lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d},"
                                + " missing snapshot: %s.",
                        null,
                        mockCommitedSnapshot.getLakeSnapshotId(),
                        tablePath,
                        tableId,
                        mockCommitedSnapshot));

        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            long offset = bucket * bucket;
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath, tableBucket, 3, offset, numberOfWriteResults));
            expectedLogEndOffsets.put(tableBucket, offset);
        }

        verifyLakeSnapshot(tablePath, tableId, 3, expectedLogEndOffsets);
    }

    @Test
    void testPartitionedTableCommitWhenFlussMissingLakeSnapshot() throws Exception {
        TablePath tablePath =
                TablePath.of(
                        "fluss", "test_commit_partitioned_table_when_fluss_missing_lake_snapshot");
        long tableId = createTable(tablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR);

        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);

        CommittedLakeSnapshot mockCommitedSnapshot =
                mockCommittedLakeSnapshot(Collections.singletonList(null), 3);
        TestingLakeTieringFactory.TestingLakeCommitter testingLakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter(mockCommitedSnapshot);
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
                                tablePath, tableBucket, 3, 3, numberOfWriteResults));
            }
        }

        verifyLakeSnapshot(
                tablePath,
                tableId,
                3,
                getExpectedLogEndOffsets(tableId, mockCommitedSnapshot, Collections.emptyMap()),
                String.format(
                        "The current Fluss's lake snapshot %d is less than lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d}, missing snapshot: %s.",
                        null,
                        mockCommitedSnapshot.getLakeSnapshotId(),
                        tablePath,
                        tableId,
                        mockCommitedSnapshot));
    }

    private CommittedLakeSnapshot mockCommittedLakeSnapshot(
            List<String> partitions, int snapshotId) {
        CommittedLakeSnapshot mockCommittedSnapshot = new CommittedLakeSnapshot(snapshotId);
        for (String partition : partitions) {
            for (int bucket = 0; bucket < DEFAULT_BUCKET_NUM; bucket++) {
                if (partition == null) {
                    mockCommittedSnapshot.addBucket(bucket, bucket + 1);
                } else {
                    mockCommittedSnapshot.addPartitionBucket(partition, bucket, bucket + 1);
                }
            }
        }
        return mockCommittedSnapshot;
    }

    private Map<TableBucket, Long> getExpectedLogEndOffsets(
            long tableId,
            CommittedLakeSnapshot committedLakeSnapshot,
            Map<String, Long> partitionIdByName) {
        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        for (Map.Entry<Tuple2<String, Integer>, Long> entry :
                committedLakeSnapshot.getLogEndOffsets().entrySet()) {
            Tuple2<String, Integer> partitionBucket = entry.getKey();
            if (partitionBucket.f0 == null) {
                expectedLogEndOffsets.put(
                        new TableBucket(tableId, partitionBucket.f1), entry.getValue());
            } else {
                expectedLogEndOffsets.put(
                        new TableBucket(
                                tableId,
                                partitionIdByName.get(partitionBucket.f0),
                                partitionBucket.f1),
                        entry.getValue());
            }
        }
        return expectedLogEndOffsets;
    }

    private StreamRecord<TableBucketWriteResult<TestingWriteResult>>
            createTableBucketWriteResultStreamRecord(
                    TablePath tablePath,
                    TableBucket tableBucket,
                    @Nullable Integer writeResult,
                    long logEndOffset,
                    int numberOfWriteResults) {
        TableBucketWriteResult<TestingWriteResult> tableBucketWriteResult =
                new TableBucketWriteResult<>(
                        tablePath,
                        tableBucket,
                        writeResult == null ? null : new TestingWriteResult(writeResult),
                        logEndOffset,
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
            Map<TableBucket, Long> expectedLogEndOffsets)
            throws Exception {
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(expectedSnapshotId);
        assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedLogEndOffsets);

        // check the tableId has been send to mark finished
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
            String failedReason)
            throws Exception {
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(expectedSnapshotId);
        assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedLogEndOffsets);

        // check the tableId has been send to mark failed
        List<OperatorEvent> operatorEvents = mockOperatorEventGateway.getEventsSent();
        SourceEventWrapper sourceEventWrapper =
                (SourceEventWrapper) operatorEvents.get(operatorEvents.size() - 1);
        FailedTieringEvent finishTieringEvent =
                (FailedTieringEvent) sourceEventWrapper.getSourceEvent();
        assertThat(finishTieringEvent.getTableId()).isEqualTo(tableId);
        assertThat(finishTieringEvent.failReason()).contains(failedReason);
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
