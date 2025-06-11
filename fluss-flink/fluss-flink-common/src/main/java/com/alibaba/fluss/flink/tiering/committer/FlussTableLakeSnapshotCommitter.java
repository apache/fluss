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

import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metrics.registry.MetricRegistry;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.CoordinatorGateway;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import com.alibaba.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.utils.ExceptionUtils;

import java.io.IOException;
import java.util.Map;

/** Committer to commit {@link FlussTableLakeSnapshot} of lake to Fluss. */
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
        rpcClient = RpcClient.create(flussConf, new ClientMetricGroup(metricRegistry, clientId));
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        this.coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
    }

    public void commit(FlussTableLakeSnapshot flussTableLakeSnapshot) throws IOException {
        try {
            CommitLakeTableSnapshotRequest request =
                    toCommitLakeTableSnapshotRequest(flussTableLakeSnapshot);
            coordinatorGateway.commitLakeTableSnapshot(request).get();
        } catch (Exception e) {
            throw new IOException(
                    String.format(
                            "Fail to commit table lake snapshot %s to Fluss.",
                            flussTableLakeSnapshot),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    private CommitLakeTableSnapshotRequest toCommitLakeTableSnapshotRequest(
            FlussTableLakeSnapshot flussTableLakeSnapshot) {
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotInfo pbLakeTableSnapshotInfo =
                commitLakeTableSnapshotRequest.addTablesReq();

        pbLakeTableSnapshotInfo.setTableId(flussTableLakeSnapshot.tableId());
        pbLakeTableSnapshotInfo.setSnapshotId(flussTableLakeSnapshot.lakeSnapshotId());
        for (Map.Entry<TableBucket, Long> bucketEndOffsetEntry :
                flussTableLakeSnapshot.logEndOffsets().entrySet()) {
            PbLakeTableOffsetForBucket pbLakeTableOffsetForBucket =
                    pbLakeTableSnapshotInfo.addBucketsReq();
            TableBucket tableBucket = bucketEndOffsetEntry.getKey();
            long endOffset = bucketEndOffsetEntry.getValue();
            if (tableBucket.getPartitionId() != null) {
                pbLakeTableOffsetForBucket.setPartitionId(tableBucket.getPartitionId());
            }
            pbLakeTableOffsetForBucket.setBucketId(tableBucket.getBucket());
            pbLakeTableOffsetForBucket.setLogEndOffset(endOffset);
        }
        return commitLakeTableSnapshotRequest;
    }

    @Override
    public void close() throws Exception {
        if (rpcClient != null) {
            rpcClient.close();
        }
    }
}
