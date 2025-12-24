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
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PbCommitLakeTableSnapshotRespForTable;
import org.apache.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotMetadata;
import org.apache.fluss.rpc.messages.PbPrepareCommitLakeTableRespForTable;
import org.apache.fluss.rpc.messages.PrepareCommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PrepareCommitLakeTableSnapshotResponse;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.utils.ExceptionUtils;

import java.io.IOException;
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
 *   <li><b>Prepare phase</b> ({@link #prepareCommit}): Sends log end offsets to the FLuss cluster,
 *       which merges them with the previous log end offsets and stores the merged snapshot data in
 *       a file. Returns the file path where the snapshot metadata is stored.
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

    String prepareCommit(long tableId, TablePath tablePath, Map<TableBucket, Long> logEndOffsets)
            throws IOException {
        PbPrepareCommitLakeTableRespForTable prepareCommitResp = null;
        Exception exception = null;
        try {
            PrepareCommitLakeTableSnapshotRequest prepareCommitLakeTableSnapshotRequest =
                    toPrepareCommitLakeTableSnapshotRequest(tableId, tablePath, logEndOffsets);
            PrepareCommitLakeTableSnapshotResponse prepareCommitLakeTableSnapshotResponse =
                    coordinatorGateway
                            .prepareCommitLakeTableSnapshot(prepareCommitLakeTableSnapshotRequest)
                            .get();
            List<PbPrepareCommitLakeTableRespForTable> pbPrepareCommitLakeTableRespForTables =
                    prepareCommitLakeTableSnapshotResponse.getPrepareCommitLakeTableRespsList();
            checkState(pbPrepareCommitLakeTableRespForTables.size() == 1);
            prepareCommitResp = pbPrepareCommitLakeTableRespForTables.get(0);
            if (prepareCommitResp.hasErrorCode()) {
                exception = ApiError.fromErrorMessage(prepareCommitResp).exception();
            }
        } catch (Exception e) {
            exception = e;
        }

        if (exception != null) {
            throw new IOException(
                    String.format(
                            "Fail to prepare commit table lake snapshot for %s to Fluss.",
                            tablePath),
                    ExceptionUtils.stripExecutionException(exception));
        }
        return checkNotNull(prepareCommitResp).getLakeTableSnapshotFilePath();
    }

    void commit(
            long tableId,
            long lakeSnapshotId,
            String lakeSnapshotPath,
            Map<TableBucket, Long> logEndOffsets,
            Map<TableBucket, Long> logMaxTieredTimestamps)
            throws IOException {
        Exception exception = null;
        try {
            CommitLakeTableSnapshotRequest request =
                    toCommitLakeTableSnapshotRequest(
                            tableId,
                            lakeSnapshotId,
                            lakeSnapshotPath,
                            logEndOffsets,
                            logMaxTieredTimestamps);
            List<PbCommitLakeTableSnapshotRespForTable> commitLakeTableSnapshotRespForTables =
                    coordinatorGateway.commitLakeTableSnapshot(request).get().getTableRespsList();
            checkState(commitLakeTableSnapshotRespForTables.size() == 1);
            PbCommitLakeTableSnapshotRespForTable commitLakeTableSnapshotRes =
                    commitLakeTableSnapshotRespForTables.get(0);
            if (commitLakeTableSnapshotRes.hasErrorCode()) {
                exception = ApiError.fromErrorMessage(commitLakeTableSnapshotRes).exception();
            }
        } catch (Exception e) {
            exception = e;
        }

        if (exception != null) {
            throw new IOException(
                    String.format(
                            "Fail to commit table lake snapshot id %d of table %d to Fluss.",
                            lakeSnapshotId, tableId),
                    ExceptionUtils.stripExecutionException(exception));
        }
    }

    /**
     * Converts the prepare commit parameters to a {@link PrepareCommitLakeTableSnapshotRequest}.
     *
     * @param tableId the table ID
     * @param tablePath the table path
     * @param logEndOffsets the log end offsets for each bucket
     * @return the prepared commit request
     */
    private PrepareCommitLakeTableSnapshotRequest toPrepareCommitLakeTableSnapshotRequest(
            long tableId, TablePath tablePath, Map<TableBucket, Long> logEndOffsets) {
        PrepareCommitLakeTableSnapshotRequest prepareCommitLakeTableSnapshotRequest =
                new PrepareCommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotInfo pbLakeTableSnapshotInfo =
                prepareCommitLakeTableSnapshotRequest.addTablesReq();
        pbLakeTableSnapshotInfo.setTableId(tableId);

        // in prepare phase, we don't know the snapshot id,
        // set -1 since the field is required
        pbLakeTableSnapshotInfo.setSnapshotId(-1L);
        for (Map.Entry<TableBucket, Long> logEndOffsetEntry : logEndOffsets.entrySet()) {
            PbLakeTableOffsetForBucket pbLakeTableOffsetForBucket =
                    pbLakeTableSnapshotInfo.addBucketsReq();
            TableBucket tableBucket = logEndOffsetEntry.getKey();
            pbLakeTableSnapshotInfo
                    .setTablePath()
                    .setDatabaseName(tablePath.getDatabaseName())
                    .setTableName(tablePath.getTableName());
            if (tableBucket.getPartitionId() != null) {
                pbLakeTableOffsetForBucket.setPartitionId(tableBucket.getPartitionId());
            }
            pbLakeTableOffsetForBucket.setBucketId(tableBucket.getBucket());
            pbLakeTableOffsetForBucket.setLogEndOffset(logEndOffsetEntry.getValue());
        }
        return prepareCommitLakeTableSnapshotRequest;
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
     * @param lakeSnapshotPath the file path where the snapshot metadata is stored
     * @param logEndOffsets the log end offsets for each bucket
     * @param logMaxTieredTimestamps the max tiered timestamps for each bucket
     * @return the commit request
     */
    private CommitLakeTableSnapshotRequest toCommitLakeTableSnapshotRequest(
            long tableId,
            long snapshotId,
            String lakeSnapshotPath,
            Map<TableBucket, Long> logEndOffsets,
            Map<TableBucket, Long> logMaxTieredTimestamps) {
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                new CommitLakeTableSnapshotRequest();

        // Add lake table snapshot metadata
        PbLakeTableSnapshotMetadata pbLakeTableSnapshotMetadata =
                commitLakeTableSnapshotRequest.addLakeTableSnapshotMetadata();
        pbLakeTableSnapshotMetadata.setSnapshotId(snapshotId);
        pbLakeTableSnapshotMetadata.setTableId(tableId);
        // tiered snapshot file path is equal to readable snapshot currently
        pbLakeTableSnapshotMetadata.setTieredSnapshotFilePath(lakeSnapshotPath);
        pbLakeTableSnapshotMetadata.setReadableSnapshotFilePath(lakeSnapshotPath);

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
