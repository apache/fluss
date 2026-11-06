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
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.adapter.StreamOperatorParametersAdapter;
import org.apache.fluss.flink.tiering.TestingLakeTieringFactory;
import org.apache.fluss.flink.tiering.TestingWriteResult;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.utils.FlinkTestBase;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.CommitterInitContext;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.lake.committer.PartitionMarkDoneCommitter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.testutils.common.CheckedThread;
import org.apache.fluss.testutils.common.CommonTestUtils;
import org.apache.fluss.testutils.common.MultiVersionTest;
import org.apache.fluss.utils.FlussPaths;

import org.apache.flink.api.common.operators.MailboxExecutor;
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
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.Mail;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.streaming.util.MockOutput;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.fluss.config.ConfigOptions.LAKE_TIERING_COMMIT_THREAD_NUM;
import static org.apache.fluss.flink.tiering.committer.TieringCommitOperator.CONSECUTIVE_FAILURES_TO_LOG_ERROR;
import static org.apache.fluss.lake.committer.LakeCommitter.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
import static org.apache.fluss.record.TestData.DATA1_PARTITIONED_TABLE_DESCRIPTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** UT for {@link TieringCommitOperator}. */
@MultiVersionTest
class TieringCommitOperatorTest extends FlinkTestBase {

    private TieringCommitOperator<TestingWriteResult, TestingCommittable> committerOperator;
    private MockOperatorEventGateway mockOperatorEventGateway;
    private StreamOperatorParameters<CommittableMessage<TestingCommittable>> parameters;
    private List<CommittableMessage<TestingCommittable>> output;
    private TaskMailboxImpl mailbox;
    private MailboxExecutor mailboxExecutor;
    private CompletableFuture<Throwable> taskFailure;
    private boolean expectTaskFailure;

    @BeforeEach
    public void beforeEach() throws Exception {
        output = new ArrayList<>();
        taskFailure = new CompletableFuture<>();
        expectTaskFailure = false;
        mockOperatorEventGateway = new MockOperatorEventGateway();
        MockOperatorEventDispatcher mockOperatorEventDispatcher =
                new MockOperatorEventDispatcher(mockOperatorEventGateway);
        parameters =
                StreamOperatorParametersAdapter.create(
                        new SourceOperatorStreamTask<String>(
                                new DummyEnvironment() {
                                    @Override
                                    public void failExternally(Throwable cause) {
                                        taskFailure.complete(cause);
                                    }
                                }),
                        new MockStreamConfig(new org.apache.flink.configuration.Configuration(), 1),
                        new MockOutput<>(output),
                        null,
                        mockOperatorEventDispatcher,
                        null);
        mailbox = new TaskMailboxImpl();
        mailboxExecutor = new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);

        reopenCommitter(new TestingLakeTieringFactory(), new Configuration());
    }

    @AfterEach
    void afterEach() throws Exception {
        try {
            committerOperator.close();
        } finally {
            mailbox.close();
        }
        if (!expectTaskFailure) {
            assertThat(taskFailure).isNotDone();
        }
    }

    @Test
    void testQuiescedMailboxIgnoresLateCommit() {
        mailbox.quiesce();
        committerOperator.postCompletion(1L, () -> fail("Quiesced mailbox ran the completion"));
        assertThat(taskFailure).isNotDone();
        assertThat(mailbox.hasMail()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("mailboxFailures")
    void testMailboxDispatchFailureFailsTask(RuntimeException failure) throws Exception {
        TablePath table = TablePath.of("fluss", "async_failure");
        long tableId = createTable(table, DEFAULT_PK_TABLE_DESCRIPTOR);
        mailbox.close();
        mailbox =
                new TaskMailboxImpl() {
                    @Override
                    public void put(Mail mail) {
                        throw failure;
                    }
                };
        mailboxExecutor = new MailboxExecutorImpl(mailbox, 0, StreamTaskActionExecutor.IMMEDIATE);
        reopenCommitter(new TestingLakeTieringFactory(), 1);
        expectTaskFailure = true;
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        table, new TableBucket(tableId, 0), 1, 1, 1, 1));
        assertThat(taskFailure.get(10, TimeUnit.SECONDS)).isSameAs(failure);
        assertThat(mockOperatorEventGateway.getEventsSent()).isEmpty();
    }

    @Test
    void testCompletionFailurePropagatesFromMailbox() throws Exception {
        TablePath table = TablePath.of("fluss", "completion_failure");
        long tableId = createTable(table, DEFAULT_PK_TABLE_DESCRIPTOR);
        parameters =
                StreamOperatorParametersAdapter.create(
                        parameters.getContainingTask(),
                        parameters.getStreamConfig(),
                        new MockOutput<>(Collections.unmodifiableList(output)),
                        null,
                        new MockOperatorEventDispatcher(mockOperatorEventGateway),
                        null);
        reopenCommitter(new TestingLakeTieringFactory(), 1);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        table, new TableBucket(tableId, 0), 1, 1, 1, 1));

        assertThatThrownBy(this::awaitCommitEvent)
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(mockOperatorEventGateway.getEventsSent()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitErrorFailsTask(boolean duringClose) throws Exception {
        TablePath table = TablePath.of("fluss", "close_failure");
        long tableId = createTable(table, DEFAULT_PK_TABLE_DESCRIPTOR);
        AssertionError failure = new AssertionError("Commit failed");
        BlockingLakeTieringFactory factory =
                new BlockingLakeTieringFactory(table) {
                    @Override
                    public PartitionMarkDoneCommitter<TestingWriteResult, TestingCommittable>
                            createLakeCommitter(CommitterInitContext context) throws IOException {
                        try {
                            return super.createLakeCommitter(context);
                        } catch (IOException e) {
                            throw failure;
                        }
                    }
                };
        reopenCommitter(factory, 1);
        expectTaskFailure = true;
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        table, new TableBucket(tableId, 0), 1, 1, 1, 1));
        factory.started.get(10, TimeUnit.SECONDS);
        if (duringClose) {
            committerOperator.close();
        } else {
            factory.release.completeExceptionally(failure);
        }
        assertThat(taskFailure.get(10, TimeUnit.SECONDS)).isSameAs(failure);
        assertThat(mockOperatorEventGateway.getEventsSent()).isEmpty();
    }

    @Test
    void testInterruptedCloseWaitsForCommit() throws Exception {
        TablePath table = TablePath.of("fluss", "interrupted_close");
        long tableId = createTable(table, DEFAULT_PK_TABLE_DESCRIPTOR);
        CompletableFuture<Void> started = new CompletableFuture<>();
        CompletableFuture<Void> release = new CompletableFuture<>();
        TestingLakeTieringFactory factory =
                new TestingLakeTieringFactory() {
                    @Override
                    public PartitionMarkDoneCommitter<TestingWriteResult, TestingCommittable>
                            createLakeCommitter(CommitterInitContext context) throws IOException {
                        started.complete(null);
                        release.join();
                        // Simulate a storage call that consumes the interrupt.
                        Thread.interrupted();
                        return super.createLakeCommitter(context);
                    }
                };
        reopenCommitter(factory, 1);
        CheckedThread closer =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        interrupt();
                        committerOperator.close();
                        assertThat(release).isCompleted();
                        assertThat(isInterrupted()).isTrue();
                    }
                };
        try {
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            table, new TableBucket(tableId, 0), 1, 1, 1, 1));
            started.get(10, TimeUnit.SECONDS);
            closer.start();
            CommonTestUtils.waitUntil(
                    () ->
                            !closer.isAlive()
                                    || (closer.getState() == Thread.State.TIMED_WAITING
                                            && Stream.of(closer.getStackTrace())
                                                    .map(StackTraceElement::getMethodName)
                                                    .anyMatch("awaitTermination"::equals)),
                    Duration.ofSeconds(10),
                    "Close did not wait for the commit");
        } finally {
            release.complete(null);
            closer.sync(10_000);
        }
        assertThat(admin.getLatestLakeSnapshot(table).get().getSnapshotId()).isEqualTo(1);
        assertThat(mockOperatorEventGateway.getEventsSent()).isEmpty();
    }

    @Test
    void testSlowCommitDoesNotBlockOtherTablesOrCheckpoint() throws Exception {
        TablePath slowTable = TablePath.of("fluss", "slow_commit");
        TablePath fastTable = TablePath.of("fluss", "fast_commit");
        long slowTableId = createTable(slowTable, DEFAULT_PK_TABLE_DESCRIPTOR);
        long fastTableId = createTable(fastTable, DEFAULT_PK_TABLE_DESCRIPTOR);
        BlockingLakeTieringFactory factory = new BlockingLakeTieringFactory(slowTable);
        reopenCommitter(factory, 2);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        slowTable, new TableBucket(slowTableId, 0), 1, 1, 1, 1));
        factory.started.get(10, TimeUnit.SECONDS);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        fastTable, new TableBucket(fastTableId, 0), 1, 1, 1, 1));
        awaitCommitEvent();
        assertThat(mockOperatorEventGateway.getEventsSent())
                .singleElement()
                .extracting(event -> ((SourceEventWrapper) event).getSourceEvent())
                .isInstanceOfSatisfying(
                        FinishedTieringEvent.class,
                        event -> assertThat(event.getTableId()).isEqualTo(fastTableId));
        mailboxExecutor.execute(() -> factory.release.complete(null), "Release slow commit");
        committerOperator.prepareSnapshotPreBarrier(1);
        assertThat(factory.release).isNotDone();
        awaitCommitEvent();
        assertThat(admin.getLatestLakeSnapshot(slowTable).get().getSnapshotId()).isEqualTo(1);
    }

    @Test
    void testCountsConsecutiveCommitFailuresPerTable() throws Exception {
        TablePath failingTable = TablePath.of("fluss", "consecutive_commit_failures");
        TablePath healthyTable = TablePath.of("fluss", "consecutive_commit_failures_healthy");
        long failingTableId = createTable(failingTable, DEFAULT_PK_TABLE_DESCRIPTOR);
        long healthyTableId = createTable(healthyTable, DEFAULT_PK_TABLE_DESCRIPTOR);
        TestingLakeTieringFactory.TestingLakeCommitter lakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter();
        reopenCommitter(new TestingLakeTieringFactory(lakeCommitter), new Configuration());

        // Every round fails, as with a permanent conflict in the lake.
        lakeCommitter.setCommitFailure(new IOException("Lake commit rejected"));
        for (int round = 1; round <= CONSECUTIVE_FAILURES_TO_LOG_ERROR; round++) {
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            failingTable, new TableBucket(failingTableId, 0), 1, 1, 1, 1));
            awaitCommitEvent();
            assertThat(lastSourceEvent()).isInstanceOf(FailedTieringEvent.class);
            assertThat(committerOperator.getConsecutiveCommitFailures(failingTableId))
                    .isEqualTo(round);
        }

        // A successful round of another table does not reset the count.
        lakeCommitter.setCommitFailure(null);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        healthyTable, new TableBucket(healthyTableId, 0), 1, 1, 1, 1));
        awaitCommitEvent();
        assertThat(lastSourceEvent()).isInstanceOf(FinishedTieringEvent.class);
        assertThat(committerOperator.getConsecutiveCommitFailures(healthyTableId)).isZero();
        assertThat(committerOperator.getConsecutiveCommitFailures(failingTableId))
                .isEqualTo(CONSECUTIVE_FAILURES_TO_LOG_ERROR);

        // A successful round of the table itself resets it.
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        failingTable, new TableBucket(failingTableId, 0), 1, 1, 1, 1));
        awaitCommitEvent();
        assertThat(lastSourceEvent()).isInstanceOf(FinishedTieringEvent.class);
        assertThat(committerOperator.getConsecutiveCommitFailures(failingTableId)).isZero();
    }

    @Test
    void testRemovesCommitFailuresOfDroppedTables() throws Exception {
        TablePath droppedTable = TablePath.of("fluss", "commit_failures_dropped");
        TablePath recreatedTable = TablePath.of("fluss", "commit_failures_recreated");
        TablePath failingTable = TablePath.of("fluss", "commit_failures_still_failing");
        long droppedTableId = createTable(droppedTable, DEFAULT_PK_TABLE_DESCRIPTOR);
        long recreatedTableId = createTable(recreatedTable, DEFAULT_PK_TABLE_DESCRIPTOR);
        long failingTableId = createTable(failingTable, DEFAULT_PK_TABLE_DESCRIPTOR);
        TestingLakeTieringFactory.TestingLakeCommitter lakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter();
        reopenCommitter(new TestingLakeTieringFactory(lakeCommitter), new Configuration());
        lakeCommitter.setCommitFailure(new IOException("Lake commit rejected"));
        failRound(droppedTable, droppedTableId);
        failRound(recreatedTable, recreatedTableId);

        // Neither table gets another round to reset its count once dropped, so the next table
        // that starts failing triggers the cleanup.
        admin.dropTable(droppedTable, false).get();
        admin.dropTable(recreatedTable, false).get();
        createTable(recreatedTable, DEFAULT_PK_TABLE_DESCRIPTOR);
        failRound(failingTable, failingTableId);
        CommonTestUtils.waitUntil(
                () -> {
                    while (mailboxExecutor.tryYield()) {}
                    return committerOperator.getConsecutiveCommitFailures(droppedTableId) == 0
                            && committerOperator.getConsecutiveCommitFailures(recreatedTableId)
                                    == 0;
                },
                Duration.ofSeconds(10),
                "Failure counts of dropped tables were not removed.");
        assertThat(committerOperator.getConsecutiveCommitFailures(failingTableId)).isOne();
    }

    @Test
    void testCommitWaitsForCapacity() throws Exception {
        TablePath first = TablePath.of("fluss", "first_commit");
        TablePath second = TablePath.of("fluss", "second_commit");
        long firstId = createTable(first, DEFAULT_PK_TABLE_DESCRIPTOR);
        long secondId = createTable(second, DEFAULT_PK_TABLE_DESCRIPTOR);
        BlockingLakeTieringFactory factory = new BlockingLakeTieringFactory(first);
        reopenCommitter(factory, 1);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        first, new TableBucket(firstId, 0), 1, 1, 1, 1));
        factory.started.get(10, TimeUnit.SECONDS);
        mailboxExecutor.execute(() -> factory.release.complete(null), "Release first commit");
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        second, new TableBucket(secondId, 0), 2, 2, 2, 1));
        assertThat(factory.release).isCompleted();
        awaitCommitEvent();
        assertThat(output).hasSize(2);
    }

    @Test
    void testRejectsDuplicateTableCommit() throws Exception {
        TablePath table = TablePath.of("fluss", "duplicate_commit");
        long tableId = createTable(table, DEFAULT_PK_TABLE_DESCRIPTOR);
        TableBucket bucket = new TableBucket(tableId, 0);
        BlockingLakeTieringFactory factory = new BlockingLakeTieringFactory(table);
        reopenCommitter(factory, 2);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(table, bucket, 1, 1, 1, 1));
        factory.started.get(10, TimeUnit.SECONDS);
        assertThatThrownBy(
                        () ->
                                committerOperator.processElement(
                                        createTableBucketWriteResultStreamRecord(
                                                table, bucket, 2, 2, 2, 1)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Table %s is already committing.", tableId);
        factory.release.complete(null);
        awaitCommitEvent();
        assertThat(output).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCloseAfterCheckpoint(boolean callbackQueued) throws Exception {
        TablePath table = TablePath.of("fluss", "cancel_commit");
        long tableId = createTable(table, DEFAULT_PK_TABLE_DESCRIPTOR);
        BlockingLakeTieringFactory factory = new BlockingLakeTieringFactory(table);
        reopenCommitter(factory, 1);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        table, new TableBucket(tableId, 0), 1, 1, 1, 1));
        factory.started.get(10, TimeUnit.SECONDS);
        committerOperator.prepareSnapshotPreBarrier(1);
        assertThat(factory.release).isNotDone();
        if (callbackQueued) {
            factory.release.complete(null);
            CommonTestUtils.waitUntil(
                    mailbox::hasMail, Duration.ofSeconds(10), "Commit callback was not queued.");
        }
        committerOperator.close();
        assertThat(factory.stopped).isCompleted();
        while (mailboxExecutor.tryYield()) {}
        assertThat(output).isEmpty();
        assertThat(mockOperatorEventGateway.getEventsSent()).isEmpty();
        if (!callbackQueued) {
            // Regenerate the interrupted round without restoring Flink state.
            factory.release.complete(null);
            reopenCommitter(factory, 1);
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            table, new TableBucket(tableId, 0), 1, 1, 1, 1));
            verifyLakeSnapshot(
                    table, tableId, 1, Collections.singletonMap(new TableBucket(tableId, 0), 1L));
        }
    }

    @Test
    void testDuplicateBucketWriteResult() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "duplicate_bucket");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        TableBucket tableBucket = new TableBucket(tableId, 0);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(tablePath, tableBucket, 1, 1, 1, 2));

        // The duplicate would reach the expected count, but must fail instead of committing.
        assertThatThrownBy(
                        () ->
                                committerOperator.processElement(
                                        createTableBucketWriteResultStreamRecord(
                                                tablePath, tableBucket, 2, 2, 2, 2)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Found duplicate write results for bucket %s of table %s.",
                        tableBucket, tableId);
        verifyNoLakeSnapshot(tablePath);
    }

    @Test
    void testInconsistentWriteResultCounts() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "inconsistent_result_counts");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath, new TableBucket(tableId, 0), 1, 1, 1, 2));

        assertThatThrownBy(
                        () ->
                                committerOperator.processElement(
                                        createTableBucketWriteResultStreamRecord(
                                                tablePath,
                                                new TableBucket(tableId, 1),
                                                2,
                                                2,
                                                2,
                                                3)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "numberOfWriteResults is not same across TableBucketWriteResults for table %s, got %s and %s.",
                        tableId, 2, 3);
        verifyNoLakeSnapshot(tablePath);
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

        // addColumn table1, bucket2
        TableBucket t1b2 = new TableBucket(tableId1, 2);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath1, t1b2, 3, 13, 23L, numberOfWriteResults));
        // verify lake snapshot
        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(t1b0, 11L);
        expectedLogEndOffsets.put(t1b1, 12L);
        expectedLogEndOffsets.put(t1b2, 13L);
        verifyLakeSnapshot(tablePath1, tableId1, 1, expectedLogEndOffsets);

        // addColumn table2, bucket2
        TableBucket t2b2 = new TableBucket(tableId2, 2);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath2, t2b2, 4, 23, 33L, numberOfWriteResults));
        expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(t2b0, 21L);
        expectedLogEndOffsets.put(t2b1, 22L);
        expectedLogEndOffsets.put(t2b2, 23L);
        verifyLakeSnapshot(tablePath2, tableId2, 2, expectedLogEndOffsets);

        // let's process one round of TableBucketWriteResult again
        expectedLogEndOffsets = new HashMap<>();
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
        }
        verifyLakeSnapshot(tablePath1, tableId1, 3, expectedLogEndOffsets);
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

        // all buckets empty but offsets advanced: an empty lake snapshot is committed
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath1, tableBucket, null, 3, 6L, numberOfWriteResults));
        }

        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(new TableBucket(tableId, 0), 3L);
        expectedLogEndOffsets.put(new TableBucket(tableId, 1), 3L);
        expectedLogEndOffsets.put(new TableBucket(tableId, 2), 3L);
        verifyLakeSnapshot(tablePath1, tableId, 1, expectedLogEndOffsets);

        // one bucket empty with a valid offset: committed together with non-empty buckets
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
                        4,
                        6L,
                        numberOfWriteResults));

        expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(new TableBucket(tableId, 0), 4L);
        expectedLogEndOffsets.put(new TableBucket(tableId, 1), 1L);
        expectedLogEndOffsets.put(new TableBucket(tableId, 2), 2L);
        verifyLakeSnapshot(tablePath1, tableId, 2, expectedLogEndOffsets);

        // one bucket skipped (unknown offset): its previously committed offset is retained
        for (int bucket = 1; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath1,
                            tableBucket,
                            bucket,
                            bucket * 10L,
                            bucket * 10L,
                            numberOfWriteResults));
        }
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath1,
                        new TableBucket(tableId, 0),
                        null,
                        // unknown bucket offset and timestamp
                        -1,
                        -1L,
                        numberOfWriteResults));

        expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(new TableBucket(tableId, 0), 4L);
        expectedLogEndOffsets.put(new TableBucket(tableId, 1), 10L);
        expectedLogEndOffsets.put(new TableBucket(tableId, 2), 20L);
        verifyLakeSnapshot(tablePath1, tableId, 3, expectedLogEndOffsets);

        // all buckets skipped (unknown offsets): nothing is committed
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath1, tableBucket, null, -1, -1L, numberOfWriteResults));
        }
        verifyLakeSnapshot(tablePath1, tableId, 3, expectedLogEndOffsets);
    }

    @Test
    void testTableCommitWhenFlussMissingLakeSnapshot() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_commit_when_fluss_missing_lake_snapshot");
        long tableId = createTable(tablePath, DEFAULT_PK_TABLE_DESCRIPTOR);
        int numberOfWriteResults = 2;

        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            expectedLogEndOffsets.put(tableBucket, 3L);
        }

        CommittedLakeSnapshot mockMissingCommittedLakeSnapshot =
                mockCommittedLakeSnapshot(tableId, tablePath, 0, expectedLogEndOffsets);
        TestingLakeTieringFactory.TestingLakeCommitter testingLakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter(
                        mockMissingCommittedLakeSnapshot);
        testingLakeCommitter.enablePartitionMarkDone();
        Configuration tieringConfig = new Configuration();
        tieringConfig.set(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED, true);
        reopenCommitter(new TestingLakeTieringFactory(testingLakeCommitter), tieringConfig);

        for (int bucket = 0; bucket < numberOfWriteResults; bucket++) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath, tableBucket, 3, 3, 3L, numberOfWriteResults));
        }

        verifyLakeSnapshot(
                tablePath,
                tableId,
                0,
                expectedLogEndOffsets,
                String.format(
                        "The current Fluss's lake snapshot %s is less than lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d},"
                                + " missing snapshot: %s.",
                        null,
                        mockMissingCommittedLakeSnapshot.getLakeSnapshotId(),
                        tablePath,
                        tableId,
                        mockMissingCommittedLakeSnapshot));
        assertThat(output).isEmpty();
        assertThat(testingLakeCommitter.getMarkDonePreparations()).isZero();

        // Retry with a different count and arrival order to verify the failed round was cleared.
        numberOfWriteResults = 3;
        expectedLogEndOffsets = new HashMap<>();
        for (int bucket : new int[] {2, 0, 1}) {
            TableBucket tableBucket = new TableBucket(tableId, bucket);
            long offset = bucket * bucket;
            long timestamp = bucket * bucket;
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath,
                            tableBucket,
                            bucket,
                            offset,
                            timestamp,
                            numberOfWriteResults));
            expectedLogEndOffsets.put(tableBucket, offset);
        }

        verifyLakeSnapshot(tablePath, tableId, 1, expectedLogEndOffsets);
        assertThat(testingLakeCommitter.getMarkDonePreparations()).isEqualTo(1);
        assertThat(output)
                .singleElement()
                .satisfies(
                        message ->
                                assertThat(message.committable().writeResults())
                                        .containsExactly(2, 0, 1));
    }

    @ParameterizedTest
    @ValueSource(strings = {"file:///unused", "s3://bucket/fluss"})
    void testIgnoreMissingLakeSnapshotFromPreviousTable(String remoteDataDir) throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test-ignore-previous-table-lake-snapshot");
        long oldTableId = createTable(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR);
        // Only construct the path: recovery must identify the old table without accessing
        // the offsets file or requiring a filesystem plugin for its URI scheme.
        String oldOffsetsPath =
                FlussPaths.remoteLakeTableSnapshotOffsetPath(remoteDataDir, tablePath, oldTableId)
                        .toString();
        CommittedLakeSnapshot oldSnapshot =
                new CommittedLakeSnapshot(
                        5,
                        Collections.singletonMap(
                                FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, oldOffsetsPath));

        admin.dropTable(tablePath, false).get();
        long tableId = createTable(tablePath, DEFAULT_LOG_TABLE_DESCRIPTOR);
        assertThat(tableId).isNotEqualTo(oldTableId);
        TestingLakeTieringFactory.TestingLakeCommitter lakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter(oldSnapshot);
        lakeCommitter.setCommitResult(LakeCommitResult.committedIsReadable(6));
        reopenCommitter(new TestingLakeTieringFactory(lakeCommitter), new Configuration());

        // An empty round must not attach the previous table's snapshot to the new table.
        TableBucket bucket = new TableBucket(tableId, 0);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(tablePath, bucket, null, -1, -1L, 1));
        awaitCommitEvent();
        verifyNoLakeSnapshot(tablePath);
        assertThat(output).isEmpty();

        // The next round must commit normally with the new table's offsets, without first
        // recovering the old offsets or failing the round for a retry.
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(tablePath, bucket, 1, 100, 1L, 1));
        verifyLakeSnapshot(tablePath, tableId, 6, Collections.singletonMap(bucket, 100L));
        assertThat(output)
                .singleElement()
                .satisfies(
                        message ->
                                assertThat(message.committable().writeResults())
                                        .containsExactly(1));
        assertThat(mockOperatorEventGateway.getEventsSent())
                .hasSize(2)
                .allSatisfy(
                        event ->
                                assertThat(((SourceEventWrapper) event).getSourceEvent())
                                        .isInstanceOf(FinishedTieringEvent.class));
    }

    @Test
    void testPartitionedTableCommitWhenFlussMissingLakeSnapshot() throws Exception {
        TablePath tablePath =
                TablePath.of(
                        "fluss", "test_commit_partitioned_table_when_fluss_missing_lake_snapshot");
        long tableId = createTable(tablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR);

        Map<String, Long> partitionIdByNames =
                FLUSS_CLUSTER_EXTENSION.waitUntilPartitionAllReady(tablePath);

        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        for (int bucket = 0; bucket < 3; bucket++) {
            for (String partitionName : partitionIdByNames.keySet()) {
                long partitionId = partitionIdByNames.get(partitionName);
                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);
                expectedLogEndOffsets.put(tableBucket, 3L);
            }
        }

        CommittedLakeSnapshot mockMissingCommittedLakeSnapshot =
                mockCommittedLakeSnapshot(tableId, tablePath, 0, expectedLogEndOffsets);

        TestingLakeTieringFactory.TestingLakeCommitter testingLakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter(
                        mockMissingCommittedLakeSnapshot);
        reopenCommitter(new TestingLakeTieringFactory(testingLakeCommitter), new Configuration());

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
                0,
                expectedLogEndOffsets,
                String.format(
                        "The current Fluss's lake snapshot %s is less than lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d}, missing snapshot: %s.",
                        null,
                        mockMissingCommittedLakeSnapshot.getLakeSnapshotId(),
                        tablePath,
                        tableId,
                        mockMissingCommittedLakeSnapshot));
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

    private void awaitCommitEvent() throws Exception {
        int previous = mockOperatorEventGateway.getEventsSent().size();
        while (mockOperatorEventGateway.getEventsSent().size() == previous) {
            CommonTestUtils.waitUntil(
                    mailbox::hasMail, Duration.ofSeconds(10), "Missing commit callback");
            mailboxExecutor.yield();
        }
    }

    private void failRound(TablePath tablePath, long tableId) throws Exception {
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath, new TableBucket(tableId, 0), 1, 1, 1, 1));
        awaitCommitEvent();
        assertThat(lastSourceEvent()).isInstanceOf(FailedTieringEvent.class);
    }

    private Object lastSourceEvent() {
        List<OperatorEvent> operatorEvents = mockOperatorEventGateway.getEventsSent();
        return ((SourceEventWrapper) operatorEvents.get(operatorEvents.size() - 1))
                .getSourceEvent();
    }

    private void verifyNoLakeSnapshot(TablePath tablePath) throws Exception {
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
        awaitCommitEvent();
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(expectedSnapshotId);
        assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedLogEndOffsets);

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
            String failedReason)
            throws Exception {
        awaitCommitEvent();
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(expectedSnapshotId);
        assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedLogEndOffsets);

        // check the tableId has been sent to mark failed
        List<OperatorEvent> operatorEvents = mockOperatorEventGateway.getEventsSent();
        SourceEventWrapper sourceEventWrapper =
                (SourceEventWrapper) operatorEvents.get(operatorEvents.size() - 1);
        FailedTieringEvent failedTieringEvent =
                (FailedTieringEvent) sourceEventWrapper.getSourceEvent();
        assertThat(failedTieringEvent.getTableId()).isEqualTo(tableId);
        assertThat(failedTieringEvent.failReason()).contains(failedReason);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCommitFailsWhenTableRecreated(boolean hasTieringProgress) throws Exception {
        if (!hasTieringProgress) {
            Configuration tieringConfig = new Configuration();
            tieringConfig.set(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED, true);
            reopenCommitter(new TestingLakeTieringFactory(), tieringConfig);
        }
        TablePath tablePath = TablePath.of("fluss", "test_commit_fails_when_table_recreated");
        long originalTableId =
                createTable(
                        tablePath,
                        hasTieringProgress
                                ? DEFAULT_PK_TABLE_DESCRIPTOR
                                : DATA1_PARTITIONED_TABLE_DESCRIPTOR);
        int numberOfWriteResults = 3;

        // Send write results for the first bucket
        TableBucket tableBucket = new TableBucket(originalTableId, 0);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath,
                        tableBucket,
                        hasTieringProgress ? 1 : null,
                        hasTieringProgress ? 1 : -1,
                        hasTieringProgress ? 1L : -1L,
                        numberOfWriteResults));

        // Drop and recreate the table with the same path
        admin.dropTable(tablePath, true).get();
        long newTableId =
                createTable(
                        tablePath,
                        hasTieringProgress
                                ? DEFAULT_PK_TABLE_DESCRIPTOR
                                : DATA1_PARTITIONED_TABLE_DESCRIPTOR);

        // Verify that the table id has changed
        assertThat(newTableId).isNotEqualTo(originalTableId);

        // Try to commit the remaining write results - should fail because table was recreated
        for (int bucket = 1; bucket < numberOfWriteResults; bucket++) {
            tableBucket = new TableBucket(originalTableId, bucket);
            committerOperator.processElement(
                    createTableBucketWriteResultStreamRecord(
                            tablePath,
                            tableBucket,
                            hasTieringProgress ? bucket : null,
                            hasTieringProgress ? bucket : -1,
                            hasTieringProgress ? (long) bucket : -1L,
                            numberOfWriteResults));
        }

        // Verify that a FailedTieringEvent was sent with the expected error message
        awaitCommitEvent();
        List<OperatorEvent> operatorEvents = mockOperatorEventGateway.getEventsSent();
        SourceEventWrapper sourceEventWrapper =
                (SourceEventWrapper) operatorEvents.get(operatorEvents.size() - 1);
        FailedTieringEvent failedTieringEvent =
                (FailedTieringEvent) sourceEventWrapper.getSourceEvent();
        assertThat(failedTieringEvent.getTableId()).isEqualTo(originalTableId);
        assertThat(failedTieringEvent.failReason())
                .contains("different from the table id")
                .contains("dropped and recreated during tiering");
        assertThat(output).isEmpty();
        verifyNoLakeSnapshot(tablePath);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEmptyRoundRecoversMissingLakeSnapshot(boolean jobMarkDoneEnabled) throws Exception {
        TablePath tablePath =
                TablePath.of("fluss", "test_empty_round_recovers_missing_lake_snapshot");
        long tableId = createTable(tablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR);

        // mimic a previous round that committed snapshot 5 to the lake but failed to commit
        // to Fluss
        Map<TableBucket, Long> expectedLogEndOffsets = new HashMap<>();
        expectedLogEndOffsets.put(new TableBucket(tableId, 0), 3L);
        CommittedLakeSnapshot mockMissingCommittedLakeSnapshot =
                mockCommittedLakeSnapshot(tableId, tablePath, 5, expectedLogEndOffsets);
        TestingLakeTieringFactory.TestingLakeCommitter testingLakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter(
                        mockMissingCommittedLakeSnapshot);
        TestingLakeTieringFactory lakeTieringFactory =
                new TestingLakeTieringFactory(testingLakeCommitter);
        // Recover the missing snapshot even when mark-done is disabled at the job or table level.
        Configuration lakeTieringConfig = new Configuration();
        lakeTieringConfig.set(
                ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED, jobMarkDoneEnabled);
        reopenCommitter(lakeTieringFactory, lakeTieringConfig);

        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath, new TableBucket(tableId, 0), null, null, -1, -1, 1));
        int expectedPreparations = jobMarkDoneEnabled ? 1 : 0;
        verifyLakeSnapshot(tablePath, tableId, 5, expectedLogEndOffsets);
        assertThat(testingLakeCommitter.getMarkDonePreparations()).isEqualTo(expectedPreparations);

        lakeTieringConfig.set(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED, true);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath, new TableBucket(tableId, 0), null, null, -1, -1, 1));
        verifyLakeSnapshot(tablePath, tableId, 5, expectedLogEndOffsets);
        assertThat(testingLakeCommitter.getMarkDonePreparations())
                .isEqualTo(expectedPreparations + 1);

        testingLakeCommitter.enablePartitionMarkDone();
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(
                        tablePath, new TableBucket(tableId, 0), null, null, -1, -1, 1));
        awaitCommitEvent();
        assertThat(testingLakeCommitter.getMarkDonePreparations())
                .isEqualTo(expectedPreparations + 2);
        LakeSnapshot lakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(lakeSnapshot.getSnapshotId()).isEqualTo(5);
        assertThat(lakeSnapshot.getTableBucketsOffset()).isEqualTo(expectedLogEndOffsets);
        assertThat(mockOperatorEventGateway.getEventsSent())
                .hasSize(3)
                .allSatisfy(
                        event ->
                                assertThat(((SourceEventWrapper) event).getSourceEvent())
                                        .isInstanceOf(FinishedTieringEvent.class));
        assertThat(output).isEmpty();
    }

    @Test
    void testReadableSnapshotsAfterPartitionMarkDone() throws Exception {
        TablePath tablePath = TablePath.of("fluss", "test_readable_snapshots_after_mark_done");
        long tableId = createTable(tablePath, DATA1_PARTITIONED_TABLE_DESCRIPTOR);
        TableBucket bucket = new TableBucket(tableId, 0);
        Map<TableBucket, Long> tieredOffsets = Collections.singletonMap(bucket, 100L);
        TestingLakeTieringFactory.TestingLakeCommitter lakeCommitter =
                new TestingLakeTieringFactory.TestingLakeCommitter();
        TestingLakeTieringFactory factory = new TestingLakeTieringFactory(lakeCommitter);
        lakeCommitter.enablePartitionMarkDone();
        Configuration tieringConfig = new Configuration();
        tieringConfig.set(ConfigOptions.LAKE_TIERING_PARTITION_MARK_DONE_ENABLED, true);
        reopenCommitter(factory, tieringConfig);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(tablePath, bucket, null, null, -1, -1, 1));
        awaitCommitEvent();
        verifyNoLakeSnapshot(tablePath);
        assertThat(lakeCommitter.getMarkDonePreparations()).isZero();
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(tablePath, bucket, null, 100, 1L, 1));
        awaitCommitEvent();
        assertThat(admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId()).isEqualTo(1);
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(tablePath, bucket, 1, 100, 1L, 1));
        awaitCommitEvent();
        assertThat(admin.getReadableLakeSnapshot(tablePath).get().getSnapshotId()).isEqualTo(2);
        assertThat(lakeCommitter.getMarkDonePreparations()).isEqualTo(2);

        // Non-DV partition mark-done advances the readable snapshot without advancing offsets.
        lakeCommitter.setCommitResult(LakeCommitResult.committedIsReadable(3));
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(tablePath, bucket, null, null, -1, -1, 1));
        awaitCommitEvent();
        LakeSnapshot readable = admin.getReadableLakeSnapshot(tablePath).get();
        assertThat(readable.getSnapshotId()).isEqualTo(3);
        assertThat(readable.getTableBucketsOffset()).isEqualTo(tieredOffsets);

        // DV partition mark-done must use the compacted snapshot's readable offsets, not the
        // mark-done snapshot's newer tiered offsets.
        Map<TableBucket, Long> readableOffsets = Collections.singletonMap(bucket, 60L);
        lakeCommitter.setCommitResult(
                LakeCommitResult.withReadableSnapshot(
                        5, 4, tieredOffsets, readableOffsets, LakeCommitResult.KEEP_ALL_PREVIOUS));
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(tablePath, bucket, null, null, -1, -1, 1));
        awaitCommitEvent();
        readable = admin.getReadableLakeSnapshot(tablePath).get();
        assertThat(readable.getSnapshotId()).isEqualTo(4);
        assertThat(readable.getTableBucketsOffset()).isEqualTo(readableOffsets);
        LakeSnapshot tiered = admin.getLatestLakeSnapshot(tablePath).get();
        assertThat(tiered.getSnapshotId()).isEqualTo(5);
        assertThat(tiered.getTableBucketsOffset()).isEqualTo(tieredOffsets);

        // With no new readable result, preserve the existing readable boundary.
        lakeCommitter.setCommitResult(LakeCommitResult.unknownReadableSnapshot(6));
        committerOperator.processElement(
                createTableBucketWriteResultStreamRecord(tablePath, bucket, null, null, -1, -1, 1));
        awaitCommitEvent();
        readable = admin.getReadableLakeSnapshot(tablePath).get();
        assertThat(readable.getSnapshotId()).isEqualTo(4);
        assertThat(readable.getTableBucketsOffset()).isEqualTo(readableOffsets);
        assertThat(admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId()).isEqualTo(6);
        assertThat(lakeCommitter.getMarkDonePreparations()).isEqualTo(5);
    }

    private CommittedLakeSnapshot mockCommittedLakeSnapshot(
            long tableId, TablePath tablePath, int snapshotId, Map<TableBucket, Long> logEndOffsets)
            throws Exception {
        try (FlussTableLakeSnapshotCommitter lakeSnapshotCommitter =
                new FlussTableLakeSnapshotCommitter(FLUSS_CLUSTER_EXTENSION.getClientConfig())) {
            lakeSnapshotCommitter.open();
            String lakeSnapshotFile =
                    lakeSnapshotCommitter.prepareLakeSnapshot(tableId, tablePath, logEndOffsets);
            return new CommittedLakeSnapshot(
                    snapshotId,
                    Collections.singletonMap(
                            FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, lakeSnapshotFile));
        }
    }

    private void reopenCommitter(TestingLakeTieringFactory factory, int commitThreads)
            throws Exception {
        reopenCommitter(
                factory, new Configuration().set(LAKE_TIERING_COMMIT_THREAD_NUM, commitThreads));
    }

    private void reopenCommitter(TestingLakeTieringFactory factory, Configuration config)
            throws Exception {
        if (committerOperator != null) {
            committerOperator.close();
        }
        committerOperator =
                new TieringCommitOperator<>(
                        parameters,
                        mailboxExecutor,
                        FLUSS_CLUSTER_EXTENSION.getClientConfig(),
                        config,
                        factory);
        committerOperator.open();
    }

    private static Stream<RuntimeException> mailboxFailures() {
        return Stream.of(
                new IllegalStateException("Mailbox dispatch failed"),
                new RejectedExecutionException("Mailbox rejected completion"));
    }

    private static class BlockingLakeTieringFactory extends TestingLakeTieringFactory {

        private final TablePath blockedTable;
        private final CompletableFuture<Void> started = new CompletableFuture<>();
        private final CompletableFuture<Void> release = new CompletableFuture<>();
        private final CompletableFuture<Void> stopped = new CompletableFuture<>();
        private final Map<TablePath, TestingLakeCommitter> committers = new ConcurrentHashMap<>();

        private BlockingLakeTieringFactory(TablePath blockedTable) {
            this.blockedTable = blockedTable;
        }

        @Override
        public PartitionMarkDoneCommitter<TestingWriteResult, TestingCommittable>
                createLakeCommitter(CommitterInitContext context) throws IOException {
            if (context.tablePath().equals(blockedTable)) {
                started.complete(null);
                try {
                    release.get(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                    throw new IOException(e);
                } finally {
                    stopped.complete(null);
                }
            }
            return committers.computeIfAbsent(
                    context.tablePath(), ignored -> new TestingLakeCommitter());
        }
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
