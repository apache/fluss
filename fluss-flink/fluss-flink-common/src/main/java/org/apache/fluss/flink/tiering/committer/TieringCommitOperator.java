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
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.ExceptionUtils;

import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private final Configuration flussConfig;
    private final Configuration lakeTieringConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;
    private TieringCommitter<WriteResult, Committable> tieringCommitter;
    private Connection connection;
    private Admin admin;

    // gateway to send event to flink source coordinator
    private final OperatorEventGateway operatorEventGateway;

    // Table ID -> accumulator for the current tiering round.
    private final Map<Long, TableWriteResultAccumulator<WriteResult>> tableWriteResultAccumulators;

    /** Creates an operator that collects and commits table tiering rounds. */
    public TieringCommitOperator(
            StreamOperatorParameters<CommittableMessage<Committable>> parameters,
            Configuration flussConf,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.flussTableLakeSnapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConf);
        this.tableWriteResultAccumulators = new HashMap<>();
        this.flussConfig = flussConf;
        this.lakeTieringConfig = lakeTieringConfig;
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
            try {
                TieringCommitResult<Committable> commitResult =
                        tieringCommitter.commitWriteResults(
                                tableId,
                                tableBucketWriteResult.tablePath(),
                                accumulator.bucketWriteResults);
                // only emit downstream when actual data was written
                if (commitResult.getCommittable() != null) {
                    output.collect(
                            new StreamRecord<>(
                                    new CommittableMessage<>(commitResult.getCommittable())));
                }
                // notify that the table id has been finished tier
                operatorEventGateway.sendEventToCoordinator(
                        new SourceEventWrapper(
                                new FinishedTieringEvent(tableId, commitResult.getStats())));
            } catch (Exception e) {
                // if any exception happens, send to source coordinator to mark it as failed
                operatorEventGateway.sendEventToCoordinator(
                        new SourceEventWrapper(
                                new FailedTieringEvent(
                                        tableId, ExceptionUtils.stringifyException(e))));
                LOG.warn(
                        "Fail to commit tiering write result, will try to tier again in next round.",
                        e);
            } finally {
                tableWriteResultAccumulators.remove(tableId);
            }
        }
    }

    @Override
    public void close() throws Exception {
        flussTableLakeSnapshotCommitter.close();
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
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
