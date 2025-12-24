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
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.flink.adapter.RuntimeContextAdapter;
import org.apache.fluss.flink.tiering.event.FailedTieringEvent;
import org.apache.fluss.flink.tiering.event.FinishedTieringEvent;
import org.apache.fluss.flink.tiering.event.TieringFailOverEvent;
import org.apache.fluss.flink.tiering.source.TableBucketWriteResult;
import org.apache.fluss.flink.tiering.source.TieringSource;
import org.apache.fluss.lake.committer.BucketOffset;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.lake.committer.LakeCommitter;
import org.apache.fluss.lake.writer.LakeTieringFactory;
import org.apache.fluss.lake.writer.LakeWriter;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.fluss.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.json.BucketOffsetJsonSerde;

import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.lake.committer.BucketOffset.FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY;
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
 * Map)} to commit to lake.
 *
 * <p>Finally, it will also commit the committed lake snapshot to Fluss cluster to make Fluss aware
 * of the tiering progress.
 */
public class TieringCommitOperator<WriteResult, Committable>
        extends AbstractStreamOperator<CommittableMessage<Committable>>
        implements OneInputStreamOperator<
                TableBucketWriteResult<WriteResult>, CommittableMessage<Committable>> {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final long serialVersionUID = 1L;

    private final Configuration flussConfig;
    private final Configuration lakeTieringConfig;
    private final LakeTieringFactory<WriteResult, Committable> lakeTieringFactory;
    private final FlussTableLakeSnapshotCommitter flussTableLakeSnapshotCommitter;
    private Connection connection;
    private Admin admin;
    private static final JsonFactory JACKSON_FACTORY = new JsonFactory();

    // gateway to send event to flink source coordinator
    private final OperatorEventGateway operatorEventGateway;

    // tableid -> write results
    private final Map<Long, List<TableBucketWriteResult<WriteResult>>>
            collectedTableBucketWriteResults;

    public TieringCommitOperator(
            StreamOperatorParameters<CommittableMessage<Committable>> parameters,
            Configuration flussConf,
            Configuration lakeTieringConfig,
            LakeTieringFactory<WriteResult, Committable> lakeTieringFactory) {
        this.lakeTieringFactory = lakeTieringFactory;
        this.flussTableLakeSnapshotCommitter = new FlussTableLakeSnapshotCommitter(flussConf);
        this.collectedTableBucketWriteResults = new HashMap<>();
        this.flussConfig = flussConf;
        this.lakeTieringConfig = lakeTieringConfig;
        this.operatorEventGateway =
                parameters
                        .getOperatorEventDispatcher()
                        .getOperatorEventGateway(TieringSource.TIERING_SOURCE_OPERATOR_UID);
        this.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<CommittableMessage<Committable>>> output) {
        super.setup(containingTask, config, output);
        int attemptNumber = RuntimeContextAdapter.getAttemptNumber(getRuntimeContext());
        if (attemptNumber > 0) {
            LOG.info("Send TieringFailoverEvent, current attempt number: {}", attemptNumber);
            // attempt number is greater than zero, the job must failover
            operatorEventGateway.sendEventToCoordinator(
                    new SourceEventWrapper(new TieringFailOverEvent()));
        }
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
                LOG.warn(
                        "Fail to commit tiering write result, will try to tier again in next round.",
                        e);
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
                        new TieringCommitterInitContext(
                                tablePath,
                                admin.getTableInfo(tablePath).get(),
                                lakeTieringConfig))) {
            List<WriteResult> writeResults =
                    committableWriteResults.stream()
                            .map(TableBucketWriteResult::writeResult)
                            .collect(Collectors.toList());

            LakeSnapshot flussCurrentLakeSnapshot = getLatestLakeSnapshot(tablePath);
            Map<String, String> logOffsetsProperty =
                    toBucketOffsetsProperty(flussCurrentLakeSnapshot, committableWriteResults);
            // to committable
            Committable committable = lakeCommitter.toCommittable(writeResults);
            // before commit to lake, check fluss not missing any lake snapshot committed by fluss
            checkFlussNotMissingLakeSnapshot(
                    tableId,
                    tablePath,
                    lakeCommitter,
                    committable,
                    flussCurrentLakeSnapshot == null
                            ? null
                            : flussCurrentLakeSnapshot.getSnapshotId());
            long committedSnapshotId = lakeCommitter.commit(committable, logOffsetsProperty);
            // commit to fluss
            FlussTableLakeSnapshot flussTableLakeSnapshot =
                    new FlussTableLakeSnapshot(tableId, committedSnapshotId);
            for (TableBucketWriteResult<WriteResult> writeResult : committableWriteResults) {
                TableBucket tableBucket = writeResult.tableBucket();
                flussTableLakeSnapshot.addBucketOffset(tableBucket, writeResult.logEndOffset());
            }
            flussTableLakeSnapshotCommitter.commit(flussTableLakeSnapshot);
            return committable;
        }
    }

    /**
     * Merge the log offsets of latest snapshot with current written bucket offsets to get full log
     * offsets.
     */
    private Map<String, String> toBucketOffsetsProperty(
            @Nullable LakeSnapshot latestLakeSnapshot,
            List<TableBucketWriteResult<WriteResult>> currentWriteResults)
            throws Exception {
        // first of all, we need to merge latest lake snapshot with current write results
        Map<TableBucket, Long> tableBucketOffsets = new HashMap<>();
        if (latestLakeSnapshot != null) {
            tableBucketOffsets = new HashMap<>(latestLakeSnapshot.getTableBucketsOffset());
        }

        for (TableBucketWriteResult<WriteResult> tableBucketWriteResult : currentWriteResults) {
            tableBucketOffsets.put(
                    tableBucketWriteResult.tableBucket(), tableBucketWriteResult.logEndOffset());
        }

        // then, serialize the bucket offsets, partition name by id
        return toBucketOffsetsProperty(tableBucketOffsets);
    }

    public static Map<String, String> toBucketOffsetsProperty(
            Map<TableBucket, Long> tableBucketOffsets) throws IOException {
        StringWriter sw = new StringWriter();
        try (JsonGenerator gen = JACKSON_FACTORY.createGenerator(sw)) {
            gen.writeStartArray();
            for (Map.Entry<TableBucket, Long> entry : tableBucketOffsets.entrySet()) {
                Long partitionId = entry.getKey().getPartitionId();
                BucketOffsetJsonSerde.INSTANCE.serialize(
                        new BucketOffset(entry.getValue(), entry.getKey().getBucket(), partitionId),
                        gen);
            }
            gen.writeEndArray();
        }
        return new HashMap<String, String>() {
            {
                put(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY, sw.toString());
            }
        };
    }

    @Nullable
    private LakeSnapshot getLatestLakeSnapshot(TablePath tablePath) throws Exception {
        LakeSnapshot flussCurrentLakeSnapshot;
        try {
            flussCurrentLakeSnapshot = admin.getLatestLakeSnapshot(tablePath).get();
        } catch (Exception e) {
            Throwable throwable = e.getCause();
            if (throwable instanceof LakeTableSnapshotNotExistException) {
                // do-nothing
                flussCurrentLakeSnapshot = null;
            } else {
                throw e;
            }
        }
        return flussCurrentLakeSnapshot;
    }

    private void checkFlussNotMissingLakeSnapshot(
            long tableId,
            TablePath tablePath,
            LakeCommitter<WriteResult, Committable> lakeCommitter,
            Committable committable,
            Long flussCurrentLakeSnapshot)
            throws Exception {

        // get Fluss missing lake snapshot in Lake
        CommittedLakeSnapshot missingCommittedSnapshot =
                lakeCommitter.getMissingLakeSnapshot(flussCurrentLakeSnapshot);

        // fluss's known snapshot is less than lake snapshot committed by fluss
        // fail this commit since the data is read from the log end-offset of a invalid fluss
        // known lake snapshot, which means the data already has been committed to lake,
        // not to commit to lake to avoid data duplicated
        if (missingCommittedSnapshot != null) {
            if (missingCommittedSnapshot.getSnapshotProperties() == null
                    || missingCommittedSnapshot
                                    .getSnapshotProperties()
                                    .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY)
                            == null) {
                throw new IllegalStateException(
                        String.format(
                                "Missing required log offsets property '%s' in lake snapshot %d for table: ‘tablePath=%s, tableId=%d’. "
                                        + "This property is required to commit the missing snapshot to Fluss. "
                                        + "The snapshot may have been created by an older version of Fluss that did not store this information, "
                                        + "or the snapshot properties may be corrupted.",
                                FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY,
                                missingCommittedSnapshot.getLakeSnapshotId(),
                                tablePath,
                                tableId));
            }

            String logOffsetsProperty =
                    missingCommittedSnapshot
                            .getSnapshotProperties()
                            .get(FLUSS_LAKE_SNAP_BUCKET_OFFSET_PROPERTY);

            // commit this missing snapshot to fluss
            flussTableLakeSnapshotCommitter.commit(
                    tableId,
                    missingCommittedSnapshot.getLakeSnapshotId(),
                    fromLogOffsetProperty(tableId, logOffsetsProperty));
            // abort this committable to delete the written files
            lakeCommitter.abort(committable);
            throw new IllegalStateException(
                    String.format(
                            "The current Fluss's lake snapshot %d is less than"
                                    + " lake actual snapshot %d committed by Fluss for table: {tablePath=%s, tableId=%d},"
                                    + " missing snapshot: %s.",
                            flussCurrentLakeSnapshot,
                            missingCommittedSnapshot.getLakeSnapshotId(),
                            tablePath,
                            tableId,
                            missingCommittedSnapshot));
        }
    }

    public static Map<TableBucket, Long> fromLogOffsetProperty(
            long tableId, String logOffsetsProperty) throws IOException {
        Map<TableBucket, Long> logEndOffsets = new HashMap<>();
        for (JsonNode node : OBJECT_MAPPER.readTree(logOffsetsProperty)) {
            BucketOffset bucketOffset = BucketOffsetJsonSerde.INSTANCE.deserialize(node);
            TableBucket tableBucket =
                    new TableBucket(
                            tableId, bucketOffset.getPartitionId(), bucketOffset.getBucket());
            logEndOffsets.put(tableBucket, bucketOffset.getLogOffset());
        }
        return logEndOffsets;
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
