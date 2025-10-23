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

import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.lake.committer.CommittedLakeSnapshot;
import org.apache.fluss.metadata.ResolvedPartitionSpec;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metrics.registry.MetricRegistry;
import org.apache.fluss.rpc.GatewayClientProxy;
import org.apache.fluss.rpc.RpcClient;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import org.apache.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import org.apache.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import org.apache.fluss.rpc.metrics.ClientMetricGroup;
import org.apache.fluss.utils.ExceptionUtils;
import org.apache.fluss.utils.types.Tuple2;

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
        rpcClient =
                RpcClient.create(flussConf, new ClientMetricGroup(metricRegistry, clientId), false);
        MetadataUpdater metadataUpdater = new MetadataUpdater(flussConf, rpcClient);
        this.coordinatorGateway =
                GatewayClientProxy.createGatewayProxy(
                        metadataUpdater::getCoordinatorServer, rpcClient, CoordinatorGateway.class);
    }

    void commit(FlussTableLakeSnapshot flussTableLakeSnapshot) throws IOException {
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

    public void commit(long tableId, CommittedLakeSnapshot committedLakeSnapshot)
            throws IOException {
        // construct lake snapshot to commit to Fluss
        FlussTableLakeSnapshot flussTableLakeSnapshot =
                new FlussTableLakeSnapshot(tableId, committedLakeSnapshot.getLakeSnapshotId());
        for (Map.Entry<Tuple2<Long, Integer>, Long> entry :
                committedLakeSnapshot.getLogEndOffsets().entrySet()) {
            Tuple2<Long, Integer> partitionBucket = entry.getKey();
            TableBucket tableBucket;
            Long partitionId = partitionBucket.f0;
            if (partitionId == null) {
                tableBucket = new TableBucket(tableId, partitionBucket.f1);
                // we use -1 since we don't store timestamp in lake snapshot property for
                // simplicity, it may cause the timestamp to be -1 during constructing lake
                // snapshot to commit to Fluss.
                // But it should happen rarely and should be a normal value after next tiering.
                flussTableLakeSnapshot.addBucketOffsetAndTimestamp(
                        tableBucket, entry.getValue(), -1);
            } else {
                tableBucket = new TableBucket(tableId, partitionId, partitionBucket.f1);
                // the partition name is qualified partition name in format:
                // key1=value1/key2=value2.
                // we need to convert to partition name in format: value1$value2$
                String qualifiedPartitionName =
                        committedLakeSnapshot.getQualifiedPartitionNameById().get(partitionId);
                ResolvedPartitionSpec resolvedPartitionSpec =
                        ResolvedPartitionSpec.fromPartitionQualifiedName(qualifiedPartitionName);
                flussTableLakeSnapshot.addPartitionBucketOffsetAndTimestamp(
                        tableBucket,
                        resolvedPartitionSpec.getPartitionName(),
                        entry.getValue(),
                        -1);
            }
        }
        commit(flussTableLakeSnapshot);
    }

    private CommitLakeTableSnapshotRequest toCommitLakeTableSnapshotRequest(
            FlussTableLakeSnapshot flussTableLakeSnapshot) {
        CommitLakeTableSnapshotRequest commitLakeTableSnapshotRequest =
                new CommitLakeTableSnapshotRequest();
        PbLakeTableSnapshotInfo pbLakeTableSnapshotInfo =
                commitLakeTableSnapshotRequest.addTablesReq();

        pbLakeTableSnapshotInfo.setTableId(flussTableLakeSnapshot.tableId());
        pbLakeTableSnapshotInfo.setSnapshotId(flussTableLakeSnapshot.lakeSnapshotId());
        for (Tuple2<TableBucket, String> bucketPartition :
                flussTableLakeSnapshot.tablePartitionBuckets()) {
            PbLakeTableOffsetForBucket pbLakeTableOffsetForBucket =
                    pbLakeTableSnapshotInfo.addBucketsReq();
            TableBucket tableBucket = bucketPartition.f0;
            String partitionName = bucketPartition.f1;
            long endOffset = flussTableLakeSnapshot.getLogEndOffset(bucketPartition);
            long maxTimestamp = flussTableLakeSnapshot.getMaxTimestamp(bucketPartition);
            if (tableBucket.getPartitionId() != null) {
                pbLakeTableOffsetForBucket.setPartitionId(tableBucket.getPartitionId());
            }
            if (partitionName != null) {
                pbLakeTableOffsetForBucket.setPartitionName(partitionName);
            }
            pbLakeTableOffsetForBucket.setBucketId(tableBucket.getBucket());
            pbLakeTableOffsetForBucket.setLogEndOffset(endOffset);
            pbLakeTableOffsetForBucket.setMaxTimestamp(maxTimestamp);
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
