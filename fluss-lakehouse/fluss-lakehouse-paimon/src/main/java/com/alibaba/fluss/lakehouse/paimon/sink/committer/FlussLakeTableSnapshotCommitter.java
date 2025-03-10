/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lakehouse.paimon.sink.committer;

import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.lakehouse.paimon.source.metrics.FlinkMetricRegistry;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import com.alibaba.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.utils.ExceptionUtils;

import org.apache.flink.metrics.MetricGroup;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** A {@link LakeTableSnapshotCommitter} to commit snapshot of tables in lake to Fluss. */
public class FlussLakeTableSnapshotCommitter implements LakeTableSnapshotCommitter {

    private static final long serialVersionUID = 1L;

    private final CoordinatorGateway coordinatorGateway;
    private final RpcClient rpcClient;

    public FlussLakeTableSnapshotCommitter(Configuration flussClientConf, MetricGroup metricGroup) {
        FlinkMetricRegistry metricRegistry = new FlinkMetricRegistry(metricGroup);
        ClientMetricGroup clientMetricGroup =
                new ClientMetricGroup(metricRegistry, "logOffsetCommiter");
        rpcClient = RpcClient.create(flussClientConf, clientMetricGroup);
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussClientConf, rpcClient);
        this.coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
    }

    @Override
    public void commit(LakeTableSnapshotInfo lakeTableSnapshotInfo) throws IOException {
        Map<Long, Map<TableBucket, Long>> logEndOffsetByTableId =
                lakeTableSnapshotInfo.getLogEndOffsetByTableId();
        // no log end offset to commit, return directly
        if (logEndOffsetByTableId.isEmpty()) {
            return;
        }

        CommitLakeTableSnapshotRequest request =
                toCommitLakeTableSnapshotRequest(lakeTableSnapshotInfo);
        try {
            // if fail to commit lakehouse data to fluss, throw exception directly to make the
            // sink job restore
            coordinatorGateway.commitLakeTableSnapshot(request).get();
        } catch (Exception e) {
            throw new IOException(
                    "Error committing data lake tiered %s to Fluss",
                    ExceptionUtils.stripCompletionException(e));
        }
    }

    @Override
    public void close() throws Exception {
        if (rpcClient != null) {
            rpcClient.close();
        }
    }

    private CommitLakeTableSnapshotRequest toCommitLakeTableSnapshotRequest(
            LakeTableSnapshotInfo lakeTableSnapshotInfo) {
        CommitLakeTableSnapshotRequest request = new CommitLakeTableSnapshotRequest();

        Map<Long, Map<TableBucket, Long>> logEndOffsetByTableId =
                lakeTableSnapshotInfo.getLogEndOffsetByTableId();

        Map<Long, Map<TableBucket, Long>> logStartOffsetByTableId =
                lakeTableSnapshotInfo.getLogStartOffsetByTableId();

        Map<Long, Long> snapshotIdByTableId = lakeTableSnapshotInfo.getSnapshotIdByTableId();

        Map<Long, PbLakeTableSnapshotInfo> reqForTables = new HashMap<>();
        for (Map.Entry<Long, Map<TableBucket, Long>> logEndOffsetEntry :
                logEndOffsetByTableId.entrySet()) {

            // set committed lakehouse data for each table
            long tableId = logEndOffsetEntry.getKey();
            PbLakeTableSnapshotInfo reqForTable =
                    reqForTables.computeIfAbsent(
                            tableId,
                            (id) ->
                                    request.addTablesReq()
                                            .setTableId(id)
                                            .setSnapshotId(snapshotIdByTableId.get(tableId)));

            // get log end offset for each bucket
            Map<TableBucket, Long> bucketLogEndOffset = logEndOffsetEntry.getValue();
            for (Map.Entry<TableBucket, Long> bucketEndOffsetEntry :
                    bucketLogEndOffset.entrySet()) {
                TableBucket tableBucket = bucketEndOffsetEntry.getKey();
                int bucketId = tableBucket.getBucket();
                PbLakeTableOffsetForBucket lakeTableOffsetForBucket = reqForTable.addBucketsReq();
                lakeTableOffsetForBucket.setBucketId(bucketId);
                if (tableBucket.getPartitionId() != null) {
                    lakeTableOffsetForBucket.setPartitionId(tableBucket.getPartitionId());
                }

                long logEndOffset = bucketEndOffsetEntry.getValue();
                // when it's still reading pk table's snapshot, we have no log end offset
                if (logEndOffset >= 0) {
                    lakeTableOffsetForBucket.setLogEndOffset(
                            // we plus 1 to align with Fluss's end log offset
                            bucketEndOffsetEntry.getValue() + 1);
                }

                // may set start log start offset
                Long logStartOffset =
                        logStartOffsetByTableId
                                .getOrDefault(tableId, Collections.emptyMap())
                                .get(tableBucket);
                if (logStartOffset != null && logStartOffset >= 0) {
                    lakeTableOffsetForBucket.setLogStartOffset(logStartOffset);
                }
            }
        }

        return request;
    }
}
