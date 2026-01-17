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
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.LakeCommitResult;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PbBucketOffset;
import org.apache.fluss.rpc.messages.PbCommitLakeTableSnapshotRespForTable;
import org.apache.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotMetadata;
import org.apache.fluss.rpc.messages.PbPrepareLakeTableRespForTable;
import org.apache.fluss.rpc.messages.PbTableOffsets;
import org.apache.fluss.rpc.messages.PrepareLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PrepareLakeTableSnapshotResponse;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.utils.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Committer to commit lake table snapshots to Fluss cluster.
 *
 * <p>This committer implements a two-phase commit protocol to record lake table snapshot
 * information in Fluss:
 *
 * <ul>
 *   <li><b>Prepare phase</b> ({@link #prepareLakeSnapshot}): Sends log end offsets to the Fluss
 *       cluster, which merges them with the previous log end offsets and stores the merged snapshot
 *       data in a file. Returns the file path where the snapshot metadata is stored.
 *   <li><b>Commit phase</b> ({@link #commit}): Sends the lake snapshot metadata (including snapshot
 *       ID and file paths) to the coordinator to finalize the commit. Also includes log end offsets
 *       and max tiered timestamps for metrics reporting to tablet servers.
 * </ul>
 */
public class FlussTableLakeSnapshotCommitter implements AutoCloseable {

    private final Configuration flussConf;

    private CoordinatorGateway coordinatorGateway;
    private RpcClient rpcClient;

    public FlussTableLakeSnapshotCommitter(Configuration flussConf) {
        this.flussConf = flussConf;
    }

    public void open() {
        // init coordinator gateway
        String clientId = flussConf.getString(ConfigOptions.CLIENT_ID);
        MetricRegistry metricRegistry = MetricRegistry.create(flussConf, null);
        // don't care about metrics, but pass a ClientMetricGroup to make compiler happy
        rpcClient =
                RpcClient.create(flussConf, new ClientMetricGroup(metricRegistry, clientId), false);
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        this.coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
    }

    @VisibleForTesting
    public String prepareLakeSnapshot(
            long tableId, TablePath tablePath, Map<TableBucket, Long> logEndOffsets)
            throws IOException {
        PbPrepareLakeTableRespForTable prepareResp;
        try {
            PrepareLakeTableSnapshotRequest prepareLakeTableSnapshotRequest =
                    toPrepareLakeTableSnapshotRequest(tableId, tablePath, logEndOffsets);
            PrepareLakeTableSnapshotResponse prepareLakeTableSnapshotResponse =
                    coordinatorGateway
                            .prepareLakeTableSnapshot(prepareLakeTableSnapshotRequest)
                            .get();
            List<PbPrepareLakeTableRespForTable> pbPrepareLakeTableRespForTables =
                    prepareLakeTableSnapshotResponse.getPrepareLakeTableRespsList();
            checkState(pbPrepareLakeTableRespForTables.size() == 1);
            prepareResp = pbPrepareLakeTableRespForTables.get(0);
            checkState(
                    prepareResp.getTableId() == tableId,
                    "TableId does not match, table id in request is %s, but got %s in response.",
                    tableId,
                    prepareResp.getTableId());
            if (prepareResp.hasErrorCode()) {
                throw ApiError.fromErrorMessage(prepareResp).exception();
            } else {
                return checkNotNull(prepareResp).getLakeTableOffsetsPath();
            }
        } catch (Exception e) {
            throw new IOException(
                    String.format(
                            "Fail to prepare commit table lake snapshot for %s to Fluss.",
                            tablePath),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    public void commit(
            long tableId,
            TablePath tablePath,
            LakeCommitResult lakeCommitResult,
            String lakeBucketTieredOffsetsPath,
            Map<TableBucket, Long> logEndOffsets,
            Map<TableBucket, Long> logMaxTieredTimestamps)
            throws IOException {
        Long earliestSnapshotIDToKeep = lakeCommitResult.getEarliestSnapshotIDToKeep();
        if (lakeCommitResult.committedIsReadable()) {
            commit(
                    tableId,
                    lakeCommitResult.getCommittedSnapshotId(),
                    lakeBucketTieredOffsetsPath,
                    lakeBucketTieredOffsetsPath,
                    logEndOffsets,
                    logMaxTieredTimestamps,
                    earliestSnapshotIDToKeep);
        } else {
            LakeCommitResult.ReadableSnapshot readableSnapshot =
                    lakeCommitResult.getReadableSnapshot();
            if (readableSnapshot == null) {
                // readable snapshot is unknown
                commit(
                        tableId,
                        lakeCommitResult.getCommittedSnapshotId(),
                        lakeBucketTieredOffsetsPath,
                        null,
                        logEndOffsets,
                        logMaxTieredTimestamps,
                        earliestSnapshotIDToKeep);
            } else {
                // readable snapshot is known, we will first commit a snapshot with readable bucket
                // offset
                // prepare a readable bucket offset file for the readable snapshot
                String readableSnapshotReadableOffsetsPath =
                        prepareLakeSnapshot(
                                tableId, tablePath, readableSnapshot.getReadableLogEndOffsets());

                // prepare a tiered bucket offset file for the readable snapshot
                String readableSnapshotTieredOffsetsPath =
                        prepareLakeSnapshot(
                                tableId, tablePath, readableSnapshot.getTieredLogEndOffsets());
                // commit the readable snapshot
                commit(
                        tableId,
                        readableSnapshot.getReadableSnapshotId(),
                        readableSnapshotTieredOffsetsPath,
                        readableSnapshotReadableOffsetsPath,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        earliestSnapshotIDToKeep);

                // commit the tiered snapshot
                commit(
                        tableId,
                        lakeCommitResult.getCommittedSnapshotId(),
                        lakeBucketTieredOffsetsPath,
                        // no readable snapshot offset path
                        null,
                        logEndOffsets,
                        logMaxTieredTimestamps,
                        earliestSnapshotIDToKeep);
            }
        }
    }

    void commit(
            long tableId,
            long snapshotId,
            String lakeBucketTieredOffsetsPath,
            @Nullable String readableLakeBucketTieredOffsetsPath,
            Map<TableBucket, Long> logEndOffsets,
            Map<TableBucket, Long> logMaxTieredTimestamps,
            @Nullable Long earliestSnapshotIDToKeep)
            throws IOException {
        try {
            CommitLakeTableSnapshotRequest request =
                    toCommitLakeTableSnapshotRequest(
                            tableId,
                            snapshotId,
                            lakeBucketTieredOffsetsPath,
                            readableLakeBucketTieredOffsetsPath,
                            logEndOffsets,
                            logMaxTieredTimestamps,
                            earliestSnapshotIDToKeep);
            List<PbCommitLakeTableSnapshotRespForTable> commitLakeTableSnapshotRespForTables =
                    coordinatorGateway.commitLakeTableSnapshot(request).get().getTableRespsList();
            checkState(commitLakeTableSnapshotRespForTables.size() == 1);
            PbCommitLakeTableSnapshotRespForTable commitLakeTableSnapshotRes =
                    commitLakeTableSnapshotRespForTables.get(0);
            if (commitLakeTableSnapshotRes.hasErrorCode()) {
                throw ApiError.fromErrorMessage(commitLakeTableSnapshotRes).exception();
            }
        } catch (Exception exception) {
            throw new IOException(
                    String.format(
                            "Fail to commit table lake snapshot id %d of table %d to Fluss.",
                            snapshotId, tableId),
                    ExceptionUtils.stripExecutionException(exception));
        }
    }

    /**
     * Converts the prepare commit parameters to a {@link PrepareLakeTableSnapshotRequest}.
     *
     * @param tableId the table ID
     * @param tablePath the table path
     * @param logEndOffsets the log end offsets for each bucket
     * @return the prepared commit request
     */
    private PrepareLakeTableSnapshotRequest toPrepareLakeTableSnapshotRequest(
            long tableId, TablePath tablePath, Map<TableBucket, Long> logEndOffsets) {
        PrepareLakeTableSnapshotRequest prepareLakeTableSnapshotRequest =
                new PrepareLakeTableSnapshotRequest();
        PbTableOffsets pbTableOffsets = prepareLakeTableSnapshotRequest.addBucketOffset();
        pbTableOffsets.setTableId(tableId);
        pbTableOffsets
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());

        for (Map.Entry<TableBucket, Long> logEndOffsetEntry : logEndOffsets.entrySet()) {
            PbBucketOffset pbBucketOffset = pbTableOffsets.addBucketOffset();
            TableBucket tableBucket = logEndOffsetEntry.getKey();
            if (tableBucket.getPartitionId() != null) {
                pbBucketOffset.setPartitionId(tableBucket.getPartitionId());
            }
            pbBucketOffset.setBucketId(tableBucket.getBucket());
            pbBucketOffset.setLogEndOffset(logEndOffsetEntry.getValue());
        }
        return prepareLakeTableSnapshotRequest;
    }

    /**
     * Converts the commit parameters to a {@link CommitLakeTableSnapshotRequest}.
     *
     * <p>This method creates a request that includes:
     *
     * <ul>
     *   <li>Lake table snapshot metadata (snapshot ID, table ID, file paths)
     *   <li>PbLakeTableSnapshotInfo for metrics reporting (log end offsets and max tiered
     *       timestamps)
     * </ul>
     *
     * @param tableId the table ID
     * @param snapshotId the lake snapshot ID
     * @param tieredBucketOffsetsPath the file path where the tiered bucket offsets is stored
     * @param readableBucketTieredOffsetsPath the file path where the readable bucket offsets is
     *     stored
     * @param logEndOffsets the log end offsets for each bucket
     * @param logMaxTieredTimestamps the max tiered timestamps for each bucket
     * @param earliestSnapshotIDToKeep the earliest snapshot id to keep
     * @return the commit request
     */
    private CommitLakeTableSnapshotRequest toCommitLakeTableSnapshotRequest(
            long tableId,
            long snapshotId,
            String tieredBucketOffsetsPath,
            @Nullable String readableBucketTieredOffsetsPath,
            Map<TableBucket, Long> logEndOffsets,
            Map<TableBucket, Long> logMaxTieredTimestamps,
            @Nullable Long earliestSnapshotIDToKeep) {
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                new CommitLakeTableSnapshotRequest();

        // Add lake table snapshot metadata
        PbLakeTableSnapshotMetadata pbLakeTableSnapshotMetadata =
                commitLakeTableSnapshotRequest.addLakeTableSnapshotMetadata();
        pbLakeTableSnapshotMetadata.setSnapshotId(snapshotId);
        pbLakeTableSnapshotMetadata.setTableId(tableId);
        // tiered snapshot file path is equal to readable snapshot currently
        pbLakeTableSnapshotMetadata.setTieredBucketOffsetsFilePath(tieredBucketOffsetsPath);
        if (readableBucketTieredOffsetsPath != null) {
            pbLakeTableSnapshotMetadata.setReadableBucketOffsetsFilePath(
                    readableBucketTieredOffsetsPath);
        }
        if (earliestSnapshotIDToKeep != null) {
            pbLakeTableSnapshotMetadata.setEarliestSnapshotIdToKeep(earliestSnapshotIDToKeep);
        }

        // Add PbLakeTableSnapshotInfo for metrics reporting (to notify tablet servers about
        // synchronized log end offsets and max timestamps)
        if (!logEndOffsets.isEmpty()) {
            commitLakeTableSnapshotRequest =
                    addLogEndOffsets(
                            commitLakeTableSnapshotRequest,
                            tableId,
                            snapshotId,
                            logEndOffsets,
                            logMaxTieredTimestamps);
        }
        return commitLakeTableSnapshotRequest;
    }

    @VisibleForTesting
    protected CommitLakeTableSnapshotRequest addLogEndOffsets(
            CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest,
            long tableId,
            long snapshotId,
            Map<TableBucket, Long> logEndOffsets,
            Map<TableBucket, Long> logMaxTieredTimestamps) {
        PbLakeTableSnapshotInfo pbLakeTableSnapshotInfo =
                commitLakeTableSnapshotRequest.addTablesReq();
        pbLakeTableSnapshotInfo.setTableId(tableId);
        pbLakeTableSnapshotInfo.setSnapshotId(snapshotId);
        for (Map.Entry<TableBucket, Long> logEndOffsetEntry : logEndOffsets.entrySet()) {
            TableBucket tableBucket = logEndOffsetEntry.getKey();
            PbLakeTableOffsetForBucket pbLakeTableOffsetForBucket =
                    pbLakeTableSnapshotInfo.addBucketsReq();

            if (tableBucket.getPartitionId() != null) {
                pbLakeTableOffsetForBucket.setPartitionId(tableBucket.getPartitionId());
            }
            pbLakeTableOffsetForBucket.setBucketId(tableBucket.getBucket());
            pbLakeTableOffsetForBucket.setLogEndOffset(logEndOffsetEntry.getValue());

            Long maxTimestamp = logMaxTieredTimestamps.get(tableBucket);
            if (maxTimestamp != null) {
                pbLakeTableOffsetForBucket.setMaxTimestamp(maxTimestamp);
            }
        }
        return commitLakeTableSnapshotRequest;
    }

    @VisibleForTesting
    CoordinatorGateway getCoordinatorGateway() {
        return coordinatorGateway;
    }

    @Override
    public void close() throws Exception {
        if (rpcClient != null) {
            rpcClient.close();
        }
    }
}
