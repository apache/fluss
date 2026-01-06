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

package org.apache.fluss.server.coordinator;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableBucketReplica;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePartition;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import org.apache.fluss.rpc.messages.PbNotifyLakeTableOffsetReqForBucket;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.PbStopReplicaReqForBucket;
import org.apache.fluss.rpc.messages.PbStopReplicaRespForBucket;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.server.coordinator.event.DeleteReplicaResponseReceivedEvent;
import org.apache.fluss.server.coordinator.event.EventManager;
import org.apache.fluss.server.coordinator.event.NotifyLeaderAndIsrResponseReceivedEvent;
import org.apache.fluss.server.entity.DeleteReplicaResultForBucket;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.metadata.BucketMetadata;
import org.apache.fluss.server.metadata.PartitionMetadata;
import org.apache.fluss.server.metadata.TableMetadata;
import org.apache.fluss.server.zk.data.LeaderAndIsr;
import org.apache.fluss.server.zk.data.lake.LakeTableSnapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_ID;
import static org.apache.fluss.server.metadata.PartitionMetadata.DELETED_PARTITION_NAME;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_ID;
import static org.apache.fluss.server.metadata.TableMetadata.DELETED_TABLE_PATH;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getNotifyLeaderAndIsrResponseData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyBucketLeaderAndIsr;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyKvSnapshotOffsetRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLakeTableOffsetForBucket;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyRemoteLogOffsetsRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeStopBucketReplica;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeUpdateMetadataRequest;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toTableBucket;

/** A request sender for coordinator server to request to tablet server by batch. */
public class CoordinatorRequestBatch {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorRequestBatch.class);

    private static final Schema EMPTY_SCHEMA = Schema.newBuilder().build();
    private static final TableDescriptor EMPTY_TABLE_DESCRIPTOR =
            TableDescriptor.builder().schema(EMPTY_SCHEMA).distributedBy(0).build();

    // a map from tablet server to notify the leader and isr for each bucket.
    private final Map<Integer, Map<TableBucket, PbNotifyLeaderAndIsrReqForBucket>>
            notifyLeaderAndIsrRequestMap = new HashMap<>();
    // a map from tablet server to stop replica for each bucket.
    private final Map<Integer, Map<TableBucket, PbStopReplicaReqForBucket>> stopReplicaRequestMap =
            new HashMap<>();

    // a set of tabletServers to send update metadata request.
    private final Set<Integer> updateMetadataRequestTabletServerSet = new HashSet<>();
    // a map from tableId to bucket metadata to update.
    private final Map<Long, List<BucketMetadata>> updateMetadataRequestBucketMap = new HashMap<>();
    // a map from tableId to (a map from partitionId to bucket metadata) to update.
    private final Map<Long, Map<Long, List<BucketMetadata>>> updateMetadataRequestPartitionMap =
            new HashMap<>();

    // a map from tablet server to notify remote log offsets request.
    private final Map<Integer, NotifyRemoteLogOffsetsRequest> notifyRemoteLogOffsetsRequestMap =
            new HashMap<>();
    // a map from tablet server to notify kv snapshot offset request.
    private final Map<Integer, NotifyKvSnapshotOffsetRequest> notifyKvSnapshotOffsetRequestMap =
            new HashMap<>();

    private final Map<Integer, Map<TableBucket, PbNotifyLakeTableOffsetReqForBucket>>
            notifyLakeTableOffsetRequestMap = new HashMap<>();

    private final CoordinatorChannelManager coordinatorChannelManager;
    private final EventManager eventManager;
    private final CoordinatorContext coordinatorContext;

    public CoordinatorRequestBatch(
            CoordinatorChannelManager coordinatorChannelManager,
            EventManager eventManager,
            CoordinatorContext coordinatorContext) {
        this.coordinatorChannelManager = coordinatorChannelManager;
        this.eventManager = eventManager;
        this.coordinatorContext = coordinatorContext;
    }

    public void newBatch() {
        if (!notifyLeaderAndIsrRequestMap.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The NotifyLeaderAndIsr batch request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some NotifyLeaderAndIsr request in %s might be lost.",
                            notifyLeaderAndIsrRequestMap));
        }
        if (!stopReplicaRequestMap.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The StopReplica batch request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some StopReplica request in %s might be lost.",
                            stopReplicaRequestMap));
        }
        if (!updateMetadataRequestTabletServerSet.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The UpdateMetadata request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some UpdateMetadata request in %s might be lost.",
                            updateMetadataRequestTabletServerSet));
        }

        if (!notifyRemoteLogOffsetsRequestMap.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The DeleteLogSegments request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some DeleteLogSegments request in %s might be lost.",
                            notifyRemoteLogOffsetsRequestMap));
        }

        if (!notifyLakeTableOffsetRequestMap.isEmpty()) {
            throw new IllegalStateException(
                    String.format(
                            "The NotifyLakeTableOffset request from coordinator to tablet server is not empty while creating "
                                    + "a new one. Some NotifyLakeTableOffset request in %s might be lost.",
                            notifyLakeTableOffsetRequestMap));
        }
    }

    public void sendRequestToTabletServers(int coordinatorEpoch) {
        try {
            sendNotifyLeaderAndIsrRequest(coordinatorEpoch);
            sendUpdateMetadataRequest();
            sendNotifyRemoteLogOffsetsRequest(coordinatorEpoch);
            sendNotifyKvSnapshotOffsetRequest(coordinatorEpoch);
            sendStopRequest(coordinatorEpoch);
        } catch (Throwable t) {
            if (!notifyLeaderAndIsrRequestMap.isEmpty()) {
                LOG.error(
                        "Haven't been able to send notify leader and isr requests, current state of the map is {}.",
                        notifyLeaderAndIsrRequestMap,
                        t);
            }
            if (!updateMetadataRequestTabletServerSet.isEmpty()) {
                LOG.error(
                        "Haven't been able to send update metadata requests, current state of the map is {}.",
                        updateMetadataRequestTabletServerSet,
                        t);
            }
            if (!stopReplicaRequestMap.isEmpty()) {
                LOG.error(
                        "Haven't been able to send stop replica requests, current state of the map is {}.",
                        stopReplicaRequestMap,
                        t);
            }
            if (!notifyRemoteLogOffsetsRequestMap.isEmpty()) {
                LOG.error(
                        "Haven't been able to send delete log segments requests, current state of the map is {}.",
                        notifyRemoteLogOffsetsRequestMap,
                        t);
            }
            if (!notifyLakeTableOffsetRequestMap.isEmpty()) {
                LOG.error(
                        "Haven't been able to send notify lake house data requests, current state of the map is {}.",
                        notifyLakeTableOffsetRequestMap,
                        t);
            }
            throw new IllegalStateException(t);
        }
    }

    public void addNotifyLeaderRequestForTabletServers(
            Set<Integer> tabletServers,
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            List<Integer> bucketReplicas,
            LeaderAndIsr leaderAndIsr) {
        tabletServers.stream()
                .filter(s -> s >= 0 && !coordinatorContext.shuttingDownTabletServers().contains(s))
                .forEach(
                        id -> {
                            Map<TableBucket, PbNotifyLeaderAndIsrReqForBucket>
                                    notifyBucketLeaderAndIsr =
                                            notifyLeaderAndIsrRequestMap.computeIfAbsent(
                                                    id, k -> new HashMap<>());
                            PbNotifyLeaderAndIsrReqForBucket notifyLeaderAndIsrForBucket =
                                    makeNotifyBucketLeaderAndIsr(
                                            new NotifyLeaderAndIsrData(
                                                    tablePath,
                                                    tableBucket,
                                                    bucketReplicas,
                                                    leaderAndIsr));
                            notifyBucketLeaderAndIsr.put(tableBucket, notifyLeaderAndIsrForBucket);
                        });

        // TODO for these cases, we can send NotifyLeaderAndIsrRequest instead of another
        // updateMetadata request, trace by: https://github.com/apache/fluss/issues/983
        addUpdateMetadataRequestForTabletServers(
                coordinatorContext.getLiveTabletServers().keySet(),
                null,
                null,
                Collections.singleton(tableBucket));
    }

    public void addStopReplicaRequestForTabletServers(
            Set<Integer> tabletServers,
            TableBucket tableBucket,
            boolean isDelete,
            int leaderEpoch) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(
                        id -> {
                            Map<TableBucket, PbStopReplicaReqForBucket> stopBucketReplica =
                                    stopReplicaRequestMap.computeIfAbsent(id, k -> new HashMap<>());
                            // reduce delete flag, if it has been marked as deleted,
                            // we will set it as delete replica
                            boolean alreadyDelete =
                                    stopBucketReplica.get(tableBucket) != null
                                            && stopBucketReplica.get(tableBucket).isDelete();
                            PbStopReplicaReqForBucket protoStopReplicaForBucket =
                                    makeStopBucketReplica(
                                            tableBucket, alreadyDelete || isDelete, leaderEpoch);
                            stopBucketReplica.put(tableBucket, protoStopReplicaForBucket);
                        });
    }

    /**
     * Add updateMetadata request for tabletServers when these cases happen:
     *
     * <ol>
     *   <li>case1: coordinatorServer re-start to re-initial coordinatorContext
     *   <li>case2: Table create and bucketAssignment generated, case will happen for new created
     *       none-partitioned table
     *   <li>case3: Table create and bucketAssignment don't generated, case will happen for new
     *       created partitioned table
     *   <li>case4: Table is queued for deletion, in this case we will set a empty tableBucket set
     *       and tableId set to {@link TableMetadata#DELETED_TABLE_ID} to avoid send unless info to
     *       tabletServer
     *   <li>case5: Partition create and bucketAssignment of this partition generated.
     *   <li>case6: Partition is queued for deletion, in this case we will set a empty tableBucket
     *       set and partitionId set to {@link PartitionMetadata#DELETED_PARTITION_ID } to avoid
     *       send unless info to tabletServer
     *   <li>case7: Leader and isr is changed for these input tableBuckets
     *   <li>case8: One newly tabletServer added into cluster
     *   <li>case9: One tabletServer is removed from cluster
     *   <li>case10: schemaId is changed after table is created.
     * </ol>
     */
    // todo: improve this with different phase enum.
    public void addUpdateMetadataRequestForTabletServers(
            Set<Integer> tabletServers,
            @Nullable Long tableId,
            @Nullable Long partitionId,
            Set<TableBucket> tableBuckets) {
        // case9:
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(updateMetadataRequestTabletServerSet::add);

        if (tableId != null) {
            if (partitionId != null) {
                // case6
                updateMetadataRequestPartitionMap
                        .computeIfAbsent(tableId, k -> new HashMap<>())
                        .put(partitionId, Collections.emptyList());
            } else {
                // case3, case4, case10
                updateMetadataRequestBucketMap.put(tableId, Collections.emptyList());
            }
        } else {
            // case1, case2, case5, case7, case8
            for (TableBucket tableBucket : tableBuckets) {
                long currentTableId = tableBucket.getTableId();
                Long currentPartitionId = tableBucket.getPartitionId();
                // case1, case2
                Optional<LeaderAndIsr> bucketLeaderAndIsr =
                        coordinatorContext.getBucketLeaderAndIsr(tableBucket);
                Integer leaderEpoch =
                        bucketLeaderAndIsr.map(LeaderAndIsr::leaderEpoch).orElse(null);
                Integer leader = bucketLeaderAndIsr.map(LeaderAndIsr::leader).orElse(null);
                if (currentPartitionId == null) {
                    Map<Integer, List<Integer>> tableAssignment =
                            coordinatorContext.getTableAssignment(currentTableId);
                    BucketMetadata bucketMetadata =
                            new BucketMetadata(
                                    tableBucket.getBucket(),
                                    leader,
                                    leaderEpoch,
                                    tableAssignment.get(tableBucket.getBucket()));
                    updateMetadataRequestBucketMap
                            .computeIfAbsent(currentTableId, k -> new ArrayList<>())
                            .add(bucketMetadata);
                } else {
                    TablePartition tablePartition =
                            new TablePartition(currentTableId, currentPartitionId);
                    Map<Integer, List<Integer>> partitionAssignment =
                            coordinatorContext.getPartitionAssignment(tablePartition);
                    BucketMetadata bucketMetadata =
                            new BucketMetadata(
                                    tableBucket.getBucket(),
                                    leader,
                                    leaderEpoch,
                                    partitionAssignment.get(tableBucket.getBucket()));
                    updateMetadataRequestPartitionMap
                            .computeIfAbsent(currentTableId, k -> new HashMap<>())
                            .computeIfAbsent(tableBucket.getPartitionId(), k -> new ArrayList<>())
                            .add(bucketMetadata);
                }
            }
        }
    }

    public void addNotifyRemoteLogOffsetsRequestForTabletServers(
            List<Integer> tabletServers,
            TableBucket tableBucket,
            long remoteLogStartOffset,
            long remoteLogEndOffset) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(
                        id ->
                                notifyRemoteLogOffsetsRequestMap.put(
                                        id,
                                        makeNotifyRemoteLogOffsetsRequest(
                                                tableBucket,
                                                remoteLogStartOffset,
                                                remoteLogEndOffset)));
    }

    public void addNotifyKvSnapshotOffsetRequestForTabletServers(
            List<Integer> tabletServers, TableBucket tableBucket, long minRetainOffset) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(
                        id ->
                                notifyKvSnapshotOffsetRequestMap.put(
                                        id,
                                        makeNotifyKvSnapshotOffsetRequest(
                                                tableBucket, minRetainOffset)));
    }

    public void addNotifyLakeTableOffsetRequestForTableServers(
            List<Integer> tabletServers,
            TableBucket tableBucket,
            LakeTableSnapshot lakeTableSnapshot,
            @Nullable Long maxTieredTimestamp) {
        tabletServers.stream()
                .filter(s -> s >= 0)
                .forEach(
                        id -> {
                            Map<TableBucket, PbNotifyLakeTableOffsetReqForBucket>
                                    notifyLakeTableOffsetReqForBucketMap =
                                            notifyLakeTableOffsetRequestMap.computeIfAbsent(
                                                    id, k -> new HashMap<>());
                            notifyLakeTableOffsetReqForBucketMap.put(
                                    tableBucket,
                                    makeNotifyLakeTableOffsetForBucket(
                                            tableBucket, lakeTableSnapshot, maxTieredTimestamp));
                        });
    }

    private void sendNotifyLeaderAndIsrRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, Map<TableBucket, PbNotifyLeaderAndIsrReqForBucket>>
                notifyRequestEntry : notifyLeaderAndIsrRequestMap.entrySet()) {
            // send request for each tablet server
            Integer serverId = notifyRequestEntry.getKey();
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest =
                    makeNotifyLeaderAndIsrRequest(
                            coordinatorEpoch, notifyRequestEntry.getValue().values());

            coordinatorChannelManager.sendBucketLeaderAndIsrRequest(
                    serverId,
                    notifyLeaderAndIsrRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.warn(
                                    "Failed to send notify leader and isr request to tablet server {}.",
                                    serverId,
                                    throwable);
                            // todo: in FLUSS-55886145, we will introduce a sender thread to send
                            // the request, and retry if encounter any error; It may happens that
                            // the tablet server is offline and will always got error. But,
                            // coordinator will remove the sender for the tablet server and mark all
                            // replica in the tablet server as offline. so, in here, if encounter
                            // any error, we just ignore it.
                            return;
                        }
                        // put the response receive event into the event manager
                        eventManager.put(
                                new NotifyLeaderAndIsrResponseReceivedEvent(
                                        getNotifyLeaderAndIsrResponseData(response), serverId));
                    });
        }
        notifyLeaderAndIsrRequestMap.clear();
    }

    private void sendStopRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, Map<TableBucket, PbStopReplicaReqForBucket>> stopReplciaEntry :
                stopReplicaRequestMap.entrySet()) {
            // send request for each tablet server
            Integer serverId = stopReplciaEntry.getKey();

            // construct the stop replica request
            Map<TableBucket, PbStopReplicaReqForBucket> stopReplicas = stopReplciaEntry.getValue();
            StopReplicaRequest stopReplicaRequest = new StopReplicaRequest();
            stopReplicaRequest
                    .setCoordinatorEpoch(coordinatorEpoch)
                    .addAllStopReplicasReqs(stopReplicas.values());

            // we collect the buckets whose replica is to be deleted
            Set<TableBucket> deletedReplicaBuckets =
                    stopReplicas.values().stream()
                            .filter(PbStopReplicaReqForBucket::isDelete)
                            .map(t -> toTableBucket(t.getTableBucket()))
                            .collect(Collectors.toSet());

            coordinatorChannelManager.sendStopBucketReplicaRequest(
                    serverId,
                    stopReplicaRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            // todo: in FLUSS-55886145, we will introduce a sender thread to send
                            // the request.
                            // in here, we just ignore the error.
                            LOG.warn(
                                    "Failed to send stop replica request to tablet server {}.",
                                    serverId,
                                    throwable);
                            return;
                        }
                        // handle the response
                        List<DeleteReplicaResultForBucket> deleteReplicaResultForBuckets =
                                new ArrayList<>();
                        List<PbStopReplicaRespForBucket> stopReplicasResps =
                                response.getStopReplicasRespsList();
                        // construct the result for stop replica
                        // for each replica
                        for (PbStopReplicaRespForBucket stopReplicaRespForBucket :
                                stopReplicasResps) {
                            TableBucket tableBucket =
                                    toTableBucket(stopReplicaRespForBucket.getTableBucket());

                            // now, for stop replica(delete=false), it's best effort without any
                            // error handling.
                            // currently, it only happens in the two case:
                            // 1. send stop replica(delete = false) for table deletion, if it fails,
                            // the following step will trigger replica to ReplicaDeletionStarted
                            // will send stop replica(delete =true) which will retry if fail.
                            // 2. send notify leader and isr request to tablet server, but the
                            // tablet server fail to init a replica
                            // then, it'll send stop replica(delete = false) to the tablet server to
                            // make the tablet server can stop the replica; It's still fine if
                            // sending stop replica fail.
                            // todo: let's revisit here to see whether we can
                            // really ignore the error after
                            // we finish the logic of tablet server.

                            // but for stop replica(delete=true), we need to handle the error and
                            // retry deletion.

                            // filter out the  error response for replica deletion.
                            if (deletedReplicaBuckets.contains(tableBucket)) {
                                DeleteReplicaResultForBucket deleteReplicaResultForBucket;
                                TableBucketReplica tableBucketReplica =
                                        new TableBucketReplica(tableBucket, serverId);
                                // if fail;
                                if (stopReplicaRespForBucket.hasErrorCode()) {
                                    deleteReplicaResultForBucket =
                                            new DeleteReplicaResultForBucket(
                                                    tableBucketReplica.getTableBucket(),
                                                    serverId,
                                                    ApiError.fromErrorMessage(
                                                            stopReplicaRespForBucket));
                                } else {
                                    deleteReplicaResultForBucket =
                                            new DeleteReplicaResultForBucket(tableBucket, serverId);
                                }
                                deleteReplicaResultForBuckets.add(deleteReplicaResultForBucket);
                            }
                        }
                        // if there are any deleted replicas, construct
                        // the DeleteReplicaResponseReceivedEvent and put into event manager
                        if (!deleteReplicaResultForBuckets.isEmpty()) {
                            DeleteReplicaResponseReceivedEvent deleteReplicaResponseReceivedEvent =
                                    new DeleteReplicaResponseReceivedEvent(
                                            deleteReplicaResultForBuckets);
                            eventManager.put(deleteReplicaResponseReceivedEvent);
                        }
                    });
        }
        stopReplicaRequestMap.clear();
    }

    public void sendUpdateMetadataRequest() {
        // Build updateMetadataRequest.
        UpdateMetadataRequest updateMetadataRequest = buildUpdateMetadataRequest();
        for (Integer serverId : updateMetadataRequestTabletServerSet) {
            coordinatorChannelManager.sendUpdateMetadataRequest(
                    serverId,
                    updateMetadataRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.debug("Failed to send update metadata request.", throwable);
                        } else {
                            LOG.debug("Update metadata for server {} success.", serverId);
                        }
                    });
        }
        updateMetadataRequestTabletServerSet.clear();
        updateMetadataRequestBucketMap.clear();
        updateMetadataRequestPartitionMap.clear();
    }

    public void sendNotifyRemoteLogOffsetsRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, NotifyRemoteLogOffsetsRequest> notifyRemoteLogOffsetsRequestEntry :
                notifyRemoteLogOffsetsRequestMap.entrySet()) {
            Integer serverId = notifyRemoteLogOffsetsRequestEntry.getKey();
            NotifyRemoteLogOffsetsRequest notifyRemoteLogOffsetsRequest =
                    notifyRemoteLogOffsetsRequestEntry.getValue();
            notifyRemoteLogOffsetsRequest.setCoordinatorEpoch(coordinatorEpoch);
            coordinatorChannelManager.sendNotifyRemoteLogOffsetsRequest(
                    serverId,
                    notifyRemoteLogOffsetsRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.warn(
                                    "Failed to send notify remote log offsets request.", throwable);
                        } else {
                            LOG.debug("Notify remote log offsets for server {} success.", serverId);
                        }
                    });
        }
        notifyRemoteLogOffsetsRequestMap.clear();
    }

    public void sendNotifyKvSnapshotOffsetRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, NotifyKvSnapshotOffsetRequest> notifySnapshotOffsetRequestEntry :
                notifyKvSnapshotOffsetRequestMap.entrySet()) {
            Integer serverId = notifySnapshotOffsetRequestEntry.getKey();
            NotifyKvSnapshotOffsetRequest notifySnapshotOffsetRequest =
                    notifySnapshotOffsetRequestEntry.getValue();
            notifySnapshotOffsetRequest.setCoordinatorEpoch(coordinatorEpoch);
            coordinatorChannelManager.sendNotifyKvSnapshotOffsetRequest(
                    serverId,
                    notifySnapshotOffsetRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.warn("Failed to send notify snapshot offset request.", throwable);
                        } else {
                            LOG.debug("Notify snapshot offset for server {} success.", serverId);
                        }
                    });
        }
        notifyKvSnapshotOffsetRequestMap.clear();
    }

    public void sendNotifyLakeTableOffsetRequest(int coordinatorEpoch) {
        for (Map.Entry<Integer, Map<TableBucket, PbNotifyLakeTableOffsetReqForBucket>>
                notifyLakeTableOffsetEntry : notifyLakeTableOffsetRequestMap.entrySet()) {
            Integer serverId = notifyLakeTableOffsetEntry.getKey();
            Map<TableBucket, PbNotifyLakeTableOffsetReqForBucket> notifyLogOffsets =
                    notifyLakeTableOffsetEntry.getValue();

            NotifyLakeTableOffsetRequest notifyLakeTableOffsetRequest =
                    new NotifyLakeTableOffsetRequest()
                            .setCoordinatorEpoch(coordinatorEpoch)
                            .addAllNotifyBucketsReqs(notifyLogOffsets.values());

            coordinatorChannelManager.sendNotifyLakeTableOffsetRequest(
                    serverId,
                    notifyLakeTableOffsetRequest,
                    (response, throwable) -> {
                        if (throwable != null) {
                            LOG.warn("Failed to send notify lake table offset.", throwable);
                        } else {
                            LOG.debug("Notify lake table offset for server {} success.", serverId);
                        }
                    });
        }
        notifyLakeTableOffsetRequestMap.clear();
    }

    private UpdateMetadataRequest buildUpdateMetadataRequest() {
        List<TableMetadata> tableMetadataList = new ArrayList<>();
        updateMetadataRequestBucketMap.forEach(
                (tableId, bucketMetadataList) -> {
                    TableInfo tableInfo = getTableInfo(tableId);
                    if (tableInfo != null) {
                        tableMetadataList.add(new TableMetadata(tableInfo, bucketMetadataList));
                    }
                });

        List<PartitionMetadata> partitionMetadataList = new ArrayList<>();
        updateMetadataRequestPartitionMap.forEach(
                (tableId, partitionIdToBucketMetadataMap) -> {
                    for (Map.Entry<Long, List<BucketMetadata>> kvEntry :
                            partitionIdToBucketMetadataMap.entrySet()) {
                        Long partitionId = kvEntry.getKey();
                        boolean partitionQueuedForDeletion =
                                coordinatorContext.isPartitionQueuedForDeletion(
                                        new TablePartition(tableId, partitionId));
                        String partitionName = coordinatorContext.getPartitionName(partitionId);
                        PartitionMetadata partitionMetadata;
                        if (partitionName == null) {
                            if (partitionQueuedForDeletion) {
                                partitionMetadata =
                                        new PartitionMetadata(
                                                tableId,
                                                DELETED_PARTITION_NAME,
                                                partitionId,
                                                kvEntry.getValue());
                            } else {
                                throw new IllegalStateException(
                                        "Partition name is null for partition " + partitionId);
                            }
                        } else {
                            partitionMetadata =
                                    new PartitionMetadata(
                                            tableId,
                                            partitionName,
                                            partitionQueuedForDeletion
                                                    ? DELETED_PARTITION_ID
                                                    : partitionId,
                                            kvEntry.getValue());
                        }
                        // table
                        partitionMetadataList.add(partitionMetadata);
                    }
                    // no bucket metadata, use empty metadata list
                    TableInfo tableInfo = getTableInfo(tableId);
                    if (tableInfo != null) {
                        tableMetadataList.add(
                                new TableMetadata(getTableInfo(tableId), Collections.emptyList()));
                    }
                });

        // TODO Todo Distinguish which tablet servers need to be updated instead of sending all live
        // tablet servers.
        return makeUpdateMetadataRequest(
                coordinatorContext.getCoordinatorServerInfo(),
                coordinatorContext.getCoordinatorEpoch(),
                new HashSet<>(coordinatorContext.getLiveTabletServers().values()),
                tableMetadataList,
                partitionMetadataList);
    }

    @Nullable
    private TableInfo getTableInfo(long tableId) {
        TableInfo tableInfo = coordinatorContext.getTableInfoById(tableId);
        boolean tableQueuedForDeletion = coordinatorContext.isTableQueuedForDeletion(tableId);
        if (tableInfo == null) {
            if (tableQueuedForDeletion) {
                return TableInfo.of(
                        DELETED_TABLE_PATH, tableId, 0, EMPTY_TABLE_DESCRIPTOR, -1L, -1L);
            } else {
                // it may happen that the table is dropped, but the partition still exists
                // when coordinator restarts, it won't consider it as deleted table,
                // and will still send partition bucket metadata to tablet server after startup,
                // which will fail into this code patch, not throw exception, just return null.
                // TODO: FIX ME, it shouldn't come into here
                return null;
            }
        } else {
            return tableQueuedForDeletion
                    ? TableInfo.of(
                            tableInfo.getTablePath(),
                            DELETED_TABLE_ID,
                            0,
                            EMPTY_TABLE_DESCRIPTOR,
                            -1L,
                            -1L)
                    : tableInfo;
        }
    }
}
