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

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.tiering.FlussTableLakeSnapshotCommitter;
import org.apache.fluss.client.tiering.TableBucketWriteResult;
import org.apache.fluss.client.tiering.TieringCommitResult;
import org.apache.fluss.client.tiering.TieringCommitter;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.source.TieringSource;
import org.apache.fluss.flink.tiering.source.metrics.TieringMetrics;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox.MailboxClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.flink.runtime.util.Runnables.withUncaughtExceptionHandler;
import static org.apache.fluss.config.ConfigOptions.LAKE_TIERING_COMMIT_THREAD_NUM;
import static org.apache.fluss.shaded.guava32.com.google.common.util.concurrent.Uninterruptibles.awaitTerminationUninterruptibly;
import static org.apache.fluss.utils.IOUtils.closeAll;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * A Flink operator to aggregate {@link WriteResult}s by table to {@link Committable} which will
 * then be committed to lake & Fluss cluster.
 *
 * <p>It will collect all {@link TableBucketWriteResult}s which wraps {@link WriteResult} written by
 * {@link LakeWriter} in {@link TieringSource} operator.
 *
 * <p>When it collects all {@link TableBucketWriteResult}s of a round of tiering for a table, it
 * will combine all the {@link WriteResult}s to {@link Committable} via method {@link
 * LakeCommitter#toCommittable(List)}, and then call method {@link LakeCommitter#commit(Object,
 * LakeCommitter.CommitContext)} to commit to lake.
 *
 * <p>Finally, it will also commit the committed lake snapshot to Fluss cluster to make Fluss aware
 * of the tiering progress.
 */
public class TieringCommitOperator<WriteResult, Committable>
        extends AbstractStreamOperator<CommittableMessage<Committable>>
        implements OneInputStreamOperator<
                TableBucketWriteResult<WriteResult>, CommittableMessage<Committable>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(TieringCommitOperator.class);

    /** Consecutive failed rounds of a table from which its commit failures are logged at ERROR. */
    @VisibleForTesting static final int CONSECUTIVE_FAILURES_TO_LOG_ERROR = 3;

    private final Configuration flussConfig;
    private final Configuration lakeTieringConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;
    private final MailboxExecutor mailboxExecutor;
    private final int commitThreads;
    // Mailbox-thread confined, including completed commits whose callbacks have not run yet.
    private final Set<Long> committingTables = new HashSet<>();
    private ExecutorService commitExecutor;
    private boolean closed;
    private TieringCommitter<WriteResult, Committable> tieringCommitter;
    private Connection connection;
    private Admin admin;

    // gateway to send event to flink source coordinator
    private final OperatorEventGateway operatorEventGateway;

    // Table ID -> accumulator for the current tiering round.
    private final Map<Long, TableWriteResultAccumulator<WriteResult>> tableWriteResultAccumulators;

    // Table path -> consecutive failed commit rounds. Updated on the mailbox thread, read by the
    // metric reporters.
    private final Map<TablePath, Integer> consecutiveCommitFailures = new ConcurrentHashMap<>();
    // Mailbox-thread confined; a table's metric is registered on its first failed round.
    private final Set<TablePath> tablesWithFailureMetric = new HashSet<>();

    /** Creates an operator that collects and commits table tiering rounds. */
    public TieringCommitOperator(
            StreamOperatorParameters<CommittableMessage<Committable>> parameters,
            MailboxExecutor mailboxExecutor,
            Configuration flussConf,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.flussTableLakeSnapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConf);
        this.tableWriteResultAccumulators = new HashMap<>();
        this.flussConfig = flussConf;
        this.lakeTieringConfig = lakeTieringConfig;
        this.mailboxExecutor = mailboxExecutor;
        this.commitThreads = lakeTieringConfig.get(LAKE_TIERING_COMMIT_THREAD_NUM);
        checkArgument(
                commitThreads > 0, "%s must be positive.", LAKE_TIERING_COMMIT_THREAD_NUM.key());
        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        this.operatorEventGateway =
                parameters
                        .getOperatorEventDispatcher()
                        .getOperatorEventGateway(TieringSource.TIERING_SOURCE_OPERATOR_UID);
    }

    @Override
    public void open() {
        flussTableLakeSnapshotCommitter.open();
        connection = ConnectionFactory.createConnection(flussConfig);
        admin = connection.getAdmin();
        tieringCommitter =
                new TieringCommitter<>(
                        admin,
                        flussConfig,
                        lakeTieringConfig,
                        lakeTieringFactory,
                        flussTableLakeSnapshotCommitter);
        commitExecutor =
                Executors.newFixedThreadPool(
                        commitThreads,
                        new ExecutorThreadFactory("tiering-committer", getUserCodeClassloader()));
    }

    @Override
    public void processElement(StreamRecord<TableBucketWriteResult<WriteResult>> streamRecord)
            throws Exception {
        TableBucketWriteResult<WriteResult> tableBucketWriteResult = streamRecord.getValue();
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        long tableId = tableBucket.getTableId();
        TableWriteResultAccumulator<WriteResult> accumulator =
                tableWriteResultAccumulators.computeIfAbsent(
                        tableId,
                        ignored ->
                                new TableWriteResultAccumulator<>(
                                        tableBucketWriteResult.numberOfWriteResults()));
        accumulator.add(tableBucketWriteResult);

        if (accumulator.isComplete()) {
            // Bound submitted rounds as well as threads; yield so completions can free capacity.
            while (committingTables.size() >= commitThreads) {
                mailboxExecutor.yield();
            }
            tableWriteResultAccumulators.remove(tableId);
            checkState(committingTables.add(tableId), "Table %s is already committing.", tableId);
            commitAsync(
                    tableId, tableBucketWriteResult.tablePath(), accumulator.bucketWriteResults);
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        if (commitExecutor != null) {
            commitExecutor.shutdownNow();
            awaitTerminationUninterruptibly(commitExecutor);
        }
        // Release the clients before restoring a pending cancellation interrupt.
        boolean interrupted = Thread.interrupted();
        try {
            closeAll(flussTableLakeSnapshotCommitter, admin, connection, super::close);
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void commitAsync(
            long tableId, TablePath tablePath, List<TableBucketWriteResult<WriteResult>> results) {
        Runnable commit =
                () -> {
                    TieringCommitResult<Committable> result;
                    try {
                        result = tieringCommitter.commitWriteResults(tableId, tablePath, results);
                    } catch (Exception e) {
                        postCompletion(tableId, () -> reportFailed(tableId, tablePath, e));
                        return;
                    }
                    postCompletion(tableId, () -> reportFinished(tableId, tablePath, result));
                };
        commitExecutor.execute(
                withUncaughtExceptionHandler(
                        commit,
                        (thread, error) ->
                                getContainingTask().getEnvironment().failExternally(error)));
    }

    @VisibleForTesting
    void postCompletion(long tableId, Runnable completion) {
        try {
            mailboxExecutor.execute(
                    () -> {
                        if (!closed) {
                            committingTables.remove(tableId);
                            completion.run();
                        }
                    },
                    "Complete tiering commit for table %s",
                    tableId);
        } catch (RejectedExecutionException e) {
            if (!(e.getCause() instanceof MailboxClosedException)) {
                throw e;
            }
        }
    }

    @VisibleForTesting
    int getConsecutiveCommitFailures(TablePath tablePath) {
        return consecutiveCommitFailures.getOrDefault(tablePath, 0);
    }

    private void reportFinished(
            long tableId, TablePath tablePath, TieringCommitResult<Committable> result) {
        consecutiveCommitFailures.remove(tablePath);
        if (result.getCommittable() != null) {
            output.collect(new StreamRecord<>(new CommittableMessage<>(result.getCommittable())));
        }
        operatorEventGateway.sendEventToCoordinator(
                new SourceEventWrapper(new FinishedTieringEvent(tableId, result.getStats())));
    }

    private void reportFailed(long tableId, TablePath tablePath, Exception cause) {
        operatorEventGateway.sendEventToCoordinator(
                new SourceEventWrapper(
                        new FailedTieringEvent(tableId, ExceptionUtils.stringifyException(cause))));
        int failures = consecutiveCommitFailures.merge(tablePath, 1, Integer::sum);
        if (tablesWithFailureMetric.add(tablePath)) {
            TieringMetrics.registerConsecutiveCommitFailures(
                    getMetricGroup(), tablePath, () -> getConsecutiveCommitFailures(tablePath));
        }
        // A permanent failure retries forever without failing the job, so escalate it to make a
        // table that stopped tiering visible.
        if (failures >= CONSECUTIVE_FAILURES_TO_LOG_ERROR) {
            LOG.error(
                    "Failed to commit the tiering round of table {} (id {}) {} times in a row, "
                            + "its lake data does not advance until a commit succeeds. "
                            + "Will try to tier it again in the next round.",
                    tablePath,
                    tableId,
                    failures,
                    cause);
        } else {
            LOG.warn(
                    "Failed to commit the tiering round of table {} (id {}), "
                            + "will try to tier it again in the next round.",
                    tablePath,
                    tableId,
                    cause);
        }
    }

    /** Collects one table's tiering round with constant-time validation of each result. */
    private static final class TableWriteResultAccumulator<WriteResult> {
        private final int expectedNumberOfWriteResults;
        private final Set<TableBucket> collectedBuckets = new HashSet<>();
        private final List<TableBucketWriteResult<WriteResult>> bucketWriteResults =
                new ArrayList<>();

        private TableWriteResultAccumulator(int expectedNumberOfWriteResults) {
            this.expectedNumberOfWriteResults = expectedNumberOfWriteResults;
        }

        private void add(TableBucketWriteResult<WriteResult> bucketWriteResult) {
            TableBucket tableBucket = bucketWriteResult.tableBucket();
            checkState(
                    !collectedBuckets.contains(tableBucket),
                    "Found duplicate write results for bucket %s of table %s.",
                    tableBucket,
                    tableBucket.getTableId());
            checkState(
                    expectedNumberOfWriteResults == bucketWriteResult.numberOfWriteResults(),
                    "numberOfWriteResults is not same across TableBucketWriteResults for table %s, got %s and %s.",
                    tableBucket.getTableId(),
                    expectedNumberOfWriteResults,
                    bucketWriteResult.numberOfWriteResults());
            collectedBuckets.add(tableBucket);
            bucketWriteResults.add(bucketWriteResult);
        }

        private boolean isComplete() {
            return bucketWriteResults.size() == expectedNumberOfWriteResults;
        }
    }
}
