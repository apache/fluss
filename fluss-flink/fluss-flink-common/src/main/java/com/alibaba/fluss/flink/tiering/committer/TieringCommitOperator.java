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

import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.flink.tiering.event.FinishTieringEvent;
import com.alibaba.fluss.flink.tiering.source.TableBucketWriteResult;
import com.alibaba.fluss.flink.tiering.source.TieringSource;
import com.alibaba.fluss.lakehouse.committer.LakeCommitter;
import com.alibaba.fluss.lakehouse.writer.LakeTieringFactory;
import com.alibaba.fluss.lakehouse.writer.LakeWriter;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

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
 * LakeCommitter#toCommitable(List)}, and then call method {@link LakeCommitter#commit(Object)} to
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

    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;

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
            Committable committable =
                    commitWriteResults(
                            tableId, tableBucketWriteResult.tablePath(), committableWriteResults);
            collectedTableBucketWriteResults.remove(tableId);
            // notify that the table id has been finished tier
            operatorEventGateway.sendEventToCoordinator(
                    new SourceEventWrapper(new FinishTieringEvent(tableId)));
            // only emit when committable is not-null
            if (committable != null) {
                output.collect(new StreamRecord<>(new CommittableMessage<>(committable)));
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
            Committable committable = lakeCommitter.toCommitable(writeResults);
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
    }
}
