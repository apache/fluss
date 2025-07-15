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

package com.alibaba.fluss.flink.tiering.committer;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.LakeTableSnapshotNotExistException;
import com.alibaba.fluss.flink.tiering.event.FailedTieringEvent;
import com.alibaba.fluss.flink.tiering.event.FinishedTieringEvent;
import com.alibaba.fluss.flink.tiering.source.TableBucketWriteResult;
import com.alibaba.fluss.flink.tiering.source.TieringSource;
import com.alibaba.fluss.lake.committer.CommittedLakeSnapshot;
import com.alibaba.fluss.lake.committer.LakeCommitter;
import com.alibaba.fluss.lake.writer.LakeTieringFactory;
import com.alibaba.fluss.lake.writer.LakeWriter;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.ExceptionUtils;

import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * A Flink operator to aggregate {@link WriteResult}s by table to {@link Committable} which will
 * then be committed to lake & Fluss cluster.
 *
 * <p>It will collect all {@link TableBucketWriteResult}s which wraps {@link WriteResult} written by
 * {@link LakeWriter} in {@link TieringSource} operator.
 *
 * <p>When it collects all {@link TableBucketWriteResult}s of a round of tiering for a table, it
 * will combine all the {@link WriteResult}s to {@link Committable} via method {@link
 * LakeCommitter#toCommittable(List)}, and then call method {@link LakeCommitter#commit(Object)} to
 * commit to lake.
 *
 * <p>Finally, it will also commit the commited lake snapshot to Fluss cluster to make Fluss aware
 * of the tiering progress.
 */
public class TieringCommitOperator<WriteResult, Committable>
        extends AbstractStreamOperator<CommittableMessage<Committable>>
        implements OneInputStreamOperator<
                TableBucketWriteResult<WriteResult>, CommittableMessage<Committable>> {

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;
    private Connection connection;
    private Admin admin;

    // gateway to send event to flink source coordinator
    private final OperatorEventGateway operatorEventGateway;

    // tableid -> write results
    private final Map<Long, List<TableBucketWriteResult<WriteResult>>>
            collectedTableBucketWriteResults;

    public TieringCommitOperator(
            StreamOperatorParameters<CommittableMessage<Committable>> parameters,
            Configuration flussConf,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.flussTableLakeSnapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConf);
        this.collectedTableBucketWriteResults = new HashMap<>();
        this.flussConfig = flussConf;
        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        operatorEventGateway =
                parameters
                        .getOperatorEventDispatcher()
                        .getOperatorEventGateway(TieringSource.TIERING_SOURCE_OPERATOR_UID);
    }

    @Override
    public void open() {
        flussTableLakeSnapshotCommitter.open();
        connection = ConnectionFactory.createConnection(flussConfig);
        admin = connection.getAdmin();
    }

    @Override
    public void processElement(StreamRecord<TableBucketWriteResult<WriteResult>> streamRecord)
            throws Exception {
        TableBucketWriteResult<WriteResult> tableBucketWriteResult = streamRecord.getValue();
        TableBucket tableBucket = tableBucketWriteResult.tableBucket();
        long tableId = tableBucket.getTableId();
        registerTableBucketWriteResult(tableId, tableBucketWriteResult);

        // may collect all write results for the table
        List<TableBucketWriteResult<WriteResult>> committableWriteResults =
                collectTableAllBucketWriteResult(tableId);

        if (committableWriteResults != null) {
            try {
                Committable committable =
                        commitWriteResults(
                                tableId,
                                tableBucketWriteResult.tablePath(),
                                committableWriteResults);
                // only emit when committable is not-null
                if (committable != null) {
                    output.collect(new StreamRecord<>(new CommittableMessage<>(committable)));
                }
                // notify that the table id has been finished tier
                operatorEventGateway.sendEventToCoordinator(
                        new SourceEventWrapper(new FinishedTieringEvent(tableId)));
            } catch (Exception e) {
                // if any exception happens, send to source coordinator to mark it as failed
                operatorEventGateway.sendEventToCoordinator(
                        new SourceEventWrapper(
                                new FailedTieringEvent(
                                        tableId, ExceptionUtils.stringifyException(e))));
            } finally {
                collectedTableBucketWriteResults.remove(tableId);
            }
        }
    }

    @Nullable
    private Committable commitWriteResults(
            long tableId,
            TablePath tablePath,
            List<TableBucketWriteResult<WriteResult>> committableWriteResults)
            throws Exception {
        // filter out non-null write result
        committableWriteResults =
                committableWriteResults.stream()
                        .filter(
                                writeResultTableBucketWriteResult ->
                                        writeResultTableBucketWriteResult.writeResult() != null)
                        .collect(Collectors.toList());

        // empty, means all write result is null, which is a empty commit,
        // return null to skip the empty commit
        if (committableWriteResults.isEmpty()) {
            return null;
        }
        try (LakeCommitter<WriteResult, Committable> lakeCommitter =
                lakeTieringFactory.createLakeCommitter(
                        new TieringCommitterInitContext(tablePath))) {
            List<WriteResult> writeResults =
                    committableWriteResults.stream()
                            .map(TableBucketWriteResult::writeResult)
                            .collect(Collectors.toList());
            // to committable
            Committable committable = lakeCommitter.toCommittable(writeResults);
            // before commit to lake, check fluss not missing any lake snapshot commited by fluss
            checkFlussNotMissingLakeSnapshot(tablePath, lakeCommitter, committable);
            long commitedSnapshotId = lakeCommitter.commit(committable);
            // commit to fluss
            Map<TableBucket, Long> logEndOffsets = new HashMap<>();
            for (TableBucketWriteResult<WriteResult> writeResult : committableWriteResults) {
                logEndOffsets.put(writeResult.tableBucket(), writeResult.logEndOffset());
            }
            flussTableLakeSnapshotCommitter.commit(
                    new FlussTableLakeSnapshot(tableId, commitedSnapshotId, logEndOffsets));
            return committable;
        }
    }

    private void checkFlussNotMissingLakeSnapshot(
            TablePath tablePath,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            Committable committable)
            throws Exception {
        Long flussCurrentLakeSnapshot;
        try {
            flussCurrentLakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get().getSnapshotId();
        } catch (Exception e) {
            Throwable throwable = e.getCause();
            if (throwable instanceof LakeTableSnapshotNotExistException) {
                // do-nothing
                flussCurrentLakeSnapshot = null;
            } else {
                throw e;
            }
        }

        // get Fluss missing lake snapshot in Lake
        CommittedLakeSnapshot missingCommittedSnapshot =
                lakeCommitter.getMissingLakeSnapshot(flussCurrentLakeSnapshot);

        // fluss's known snapshot is less than lake snapshot committed by fluss
        // fail this commit since the data is read from the log end-offset of a invalid fluss
        // known lake snapshot, which means the data already has been committed to lake,
        // not to commit to lake to avoid data duplicated
        if (missingCommittedSnapshot != null) {
            // commit this missing snapshot to fluss
            TableInfo tableInfo = admin.getTableInfo(tablePath).get();
            Map<String, Long> partitionIdByName = null;
            if (tableInfo.isPartitioned()) {
                partitionIdByName =
                        admin.listPartitionInfos(tablePath).get().stream()
                                .collect(
                                        Collectors.toMap(
                                                PartitionInfo::getPartitionName,
                                                PartitionInfo::getPartitionId));
            }
            flussTableLakeSnapshotCommitter.commit(
                    tableInfo.getTableId(), partitionIdByName, missingCommittedSnapshot);
            // abort this committable to delete the written files
            lakeCommitter.abort(committable);
            throw new IllegalStateException(
                    String.format(
                            "The current Fluss's lake snapshot %d is less than"
                                    + " lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d},"
                                    + " missing snapshot: %s.",
                            flussCurrentLakeSnapshot,
                            missingCommittedSnapshot.getLakeSnapshotId(),
                            tableInfo.getTablePath(),
                            tableInfo.getTableId(),
                            missingCommittedSnapshot));
        }
    }

    private void registerTableBucketWriteResult(
            long tableId, TableBucketWriteResult<WriteResult> tableBucketWriteResult) {
        collectedTableBucketWriteResults
                .computeIfAbsent(tableId, k -> new ArrayList<>())
                .add(tableBucketWriteResult);
    }

    @Nullable
    private List<TableBucketWriteResult<WriteResult>> collectTableAllBucketWriteResult(
            long tableId) {
        Set<TableBucket> collectedBuckets = new HashSet<>();
        Integer numberOfWriteResults = null;
        List<TableBucketWriteResult<WriteResult>> writeResults = new ArrayList<>();
        for (TableBucketWriteResult<WriteResult> tableBucketWriteResult :
                collectedTableBucketWriteResults.get(tableId)) {
            if (!collectedBuckets.add(tableBucketWriteResult.tableBucket())) {
                // it means the write results contain more than two write result
                // for same table, it shouldn't happen, let's throw exception to
                // avoid unexpected behavior
                throw new IllegalStateException(
                        String.format(
                                "Found duplicate write results for bucket %s of table %s.",
                                tableBucketWriteResult.tableBucket(), tableId));
            }
            if (numberOfWriteResults == null) {
                numberOfWriteResults = tableBucketWriteResult.numberOfWriteResults();
            } else {
                // the numberOfWriteResults must be same across tableBucketWriteResults
                checkState(
                        numberOfWriteResults == tableBucketWriteResult.numberOfWriteResults(),
                        "numberOfWriteResults is not same across TableBucketWriteResults for table %s, got %s and %s.",
                        tableId,
                        numberOfWriteResults,
                        tableBucketWriteResult.numberOfWriteResults());
            }
            writeResults.add(tableBucketWriteResult);
        }

        if (numberOfWriteResults != null && writeResults.size() == numberOfWriteResults) {
            return writeResults;
        } else {
            return null;
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
}
