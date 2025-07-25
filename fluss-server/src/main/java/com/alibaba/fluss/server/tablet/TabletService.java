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

package com.alibaba.fluss.server.tablet;

import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.exception.AuthorizationException;
import com.alibaba.fluss.exception.UnknownTableOrBucketException;
import com.alibaba.fluss.fs.FileSystem;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.entity.LookupResultForBucket;
import com.alibaba.fluss.rpc.entity.PrefixLookupResultForBucket;
import com.alibaba.fluss.rpc.entity.ResultForBucket;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.messages.InitWriterResponse;
import com.alibaba.fluss.rpc.messages.LimitScanRequest;
import com.alibaba.fluss.rpc.messages.LimitScanResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListOffsetsResponse;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.LookupResponse;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.MetadataResponse;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetResponse;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsResponse;
import com.alibaba.fluss.rpc.messages.PrefixLookupRequest;
import com.alibaba.fluss.rpc.messages.PrefixLookupResponse;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.rpc.messages.PutKvResponse;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.messages.StopReplicaResponse;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.rpc.messages.UpdateMetadataResponse;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.Errors;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.server.RpcServiceBase;
import com.alibaba.fluss.server.authorizer.Authorizer;
import com.alibaba.fluss.server.coordinator.MetadataManager;
import com.alibaba.fluss.server.entity.FetchReqInfo;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.log.FetchParams;
import com.alibaba.fluss.server.log.ListOffsetsParam;
import com.alibaba.fluss.server.metadata.TabletServerMetadataCache;
import com.alibaba.fluss.server.replica.ReplicaManager;
import com.alibaba.fluss.server.utils.ServerRpcMessageUtils;
import com.alibaba.fluss.server.zk.ZooKeeperClient;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.alibaba.fluss.security.acl.OperationType.READ;
import static com.alibaba.fluss.security.acl.OperationType.WRITE;
import static com.alibaba.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static com.alibaba.fluss.server.log.FetchParams.DEFAULT_MAX_WAIT_MS_WHEN_MIN_BYTES_ENABLE;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getListOffsetsData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifyLakeTableOffset;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifyLeaderAndIsrRequestData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifyRemoteLogOffsetsData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getNotifySnapshotOffsetData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getProduceLogData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getPutKvData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getStopReplicaData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getTargetColumns;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.getUpdateMetadataRequestData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeInitWriterResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeLimitScanResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeListOffsetsResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeLookupResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makePrefixLookupResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makePutKvResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.makeStopReplicaResponse;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toLookupData;
import static com.alibaba.fluss.server.utils.ServerRpcMessageUtils.toPrefixLookupData;

/** An RPC Gateway service for tablet server. */
public final class TabletService extends RpcServiceBase implements TabletServerGateway {

    private final String serviceName;
    private final ReplicaManager replicaManager;
    private final TabletServerMetadataCache metadataCache;

    public TabletService(
            int serverId,
            FileSystem remoteFileSystem,
            ZooKeeperClient zkClient,
            ReplicaManager replicaManager,
            TabletServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            @Nullable Authorizer authorizer) {
        super(remoteFileSystem, ServerType.TABLET_SERVER, zkClient, metadataManager, authorizer);
        this.serviceName = "server-" + serverId;
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
    }

    @Override
    public String name() {
        return serviceName;
    }

    @Override
    public void shutdown() {}

    @Override
    public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
        authorizeTable(WRITE, request.getTableId());
        CompletableFuture<ProduceLogResponse> response = new CompletableFuture<>();
        Map<TableBucket, MemoryLogRecords> produceLogData = getProduceLogData(request);
        replicaManager.appendRecordsToLog(
                request.getTimeoutMs(),
                request.getAcks(),
                produceLogData,
                bucketResponseMap -> response.complete(makeProduceLogResponse(bucketResponseMap)));
        return response;
    }

    @Override
    public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
        Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(request);
        Map<TableBucket, FetchLogResultForBucket> errorResponseMap = new HashMap<>();
        Map<TableBucket, FetchReqInfo> interesting =
                // TODO: we should also authorize for follower, otherwise, users can mock follower
                //  to skip the authorization.
                authorizer != null && request.getFollowerServerId() < 0
                        ? authorizeRequestData(
                                READ, fetchLogData, errorResponseMap, FetchLogResultForBucket::new)
                        : fetchLogData;
        if (interesting.isEmpty()) {
            return CompletableFuture.completedFuture(makeFetchLogResponse(errorResponseMap));
        }

        CompletableFuture<FetchLogResponse> response = new CompletableFuture<>();
        FetchParams fetchParams = getFetchParams(request);
        replicaManager.fetchLogRecords(
                fetchParams,
                interesting,
                fetchResponseMap ->
                        response.complete(
                                makeFetchLogResponse(fetchResponseMap, errorResponseMap)));
        return response;
    }

    private static FetchParams getFetchParams(FetchLogRequest request) {
        FetchParams fetchParams;
        if (request.hasMinBytes()) {
            fetchParams =
                    new FetchParams(
                            request.getFollowerServerId(),
                            request.getMaxBytes(),
                            request.getMinBytes(),
                            request.hasMaxWaitMs()
                                    ? request.getMaxWaitMs()
                                    : DEFAULT_MAX_WAIT_MS_WHEN_MIN_BYTES_ENABLE);
        } else {
            fetchParams = new FetchParams(request.getFollowerServerId(), request.getMaxBytes());
        }
        return fetchParams;
    }

    @Override
    public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
        authorizeTable(WRITE, request.getTableId());

        Map<TableBucket, KvRecordBatch> putKvData = getPutKvData(request);
        CompletableFuture<PutKvResponse> response = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                request.getTimeoutMs(),
                request.getAcks(),
                putKvData,
                getTargetColumns(request),
                bucketResponse -> response.complete(makePutKvResponse(bucketResponse)));
        return response;
    }

    @Override
    public CompletableFuture<LookupResponse> lookup(LookupRequest request) {
        Map<TableBucket, List<byte[]>> lookupData = toLookupData(request);
        Map<TableBucket, LookupResultForBucket> errorResponseMap = new HashMap<>();
        Map<TableBucket, List<byte[]>> interesting =
                authorizeRequestData(
                        READ, lookupData, errorResponseMap, LookupResultForBucket::new);
        if (interesting.isEmpty()) {
            return CompletableFuture.completedFuture(makeLookupResponse(errorResponseMap));
        }

        CompletableFuture<LookupResponse> response = new CompletableFuture<>();
        replicaManager.lookups(
                lookupData,
                value -> response.complete(makeLookupResponse(value, errorResponseMap)));
        return response;
    }

    @Override
    public CompletableFuture<PrefixLookupResponse> prefixLookup(PrefixLookupRequest request) {
        Map<TableBucket, List<byte[]>> prefixLookupData = toPrefixLookupData(request);
        Map<TableBucket, PrefixLookupResultForBucket> errorResponseMap = new HashMap<>();
        Map<TableBucket, List<byte[]>> interesting =
                authorizeRequestData(
                        READ, prefixLookupData, errorResponseMap, PrefixLookupResultForBucket::new);
        if (interesting.isEmpty()) {
            return CompletableFuture.completedFuture(makePrefixLookupResponse(errorResponseMap));
        }

        CompletableFuture<PrefixLookupResponse> response = new CompletableFuture<>();
        replicaManager.prefixLookups(
                prefixLookupData,
                value -> response.complete(makePrefixLookupResponse(value, errorResponseMap)));
        return response;
    }

    @Override
    public CompletableFuture<LimitScanResponse> limitScan(LimitScanRequest request) {
        authorizeTable(READ, request.getTableId());

        CompletableFuture<LimitScanResponse> response = new CompletableFuture<>();
        replicaManager.limitScan(
                new TableBucket(
                        request.getTableId(),
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId()),
                request.getLimit(),
                value -> response.complete(makeLimitScanResponse(value)));
        return response;
    }

    @Override
    public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest) {
        CompletableFuture<NotifyLeaderAndIsrResponse> response = new CompletableFuture<>();
        List<NotifyLeaderAndIsrData> notifyLeaderAndIsrRequestData =
                getNotifyLeaderAndIsrRequestData(notifyLeaderAndIsrRequest);
        replicaManager.becomeLeaderOrFollower(
                notifyLeaderAndIsrRequest.getCoordinatorEpoch(),
                notifyLeaderAndIsrRequestData,
                result -> response.complete(makeNotifyLeaderAndIsrResponse(result)));
        return response;
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        return CompletableFuture.completedFuture(
                makeMetadataResponse(
                        request,
                        currentListenerName(),
                        currentSession(),
                        authorizer,
                        metadataCache,
                        metadataCache::getTableMetadata,
                        metadataCache::getPhysicalTablePath,
                        metadataCache::getPartitionMetadata));
    }

    @Override
    public CompletableFuture<UpdateMetadataResponse> updateMetadata(UpdateMetadataRequest request) {
        int coordinatorEpoch =
                request.hasCoordinatorEpoch()
                        ? request.getCoordinatorEpoch()
                        : INITIAL_COORDINATOR_EPOCH;
        replicaManager.maybeUpdateMetadataCache(
                coordinatorEpoch, getUpdateMetadataRequestData(request));
        return CompletableFuture.completedFuture(new UpdateMetadataResponse());
    }

    @Override
    public CompletableFuture<StopReplicaResponse> stopReplica(
            StopReplicaRequest stopReplicaRequest) {
        CompletableFuture<StopReplicaResponse> response = new CompletableFuture<>();
        replicaManager.stopReplicas(
                stopReplicaRequest.getCoordinatorEpoch(),
                getStopReplicaData(stopReplicaRequest),
                result -> response.complete(makeStopReplicaResponse(result)));
        return response;
    }

    @Override
    public CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request) {
        // TODO: authorize DESCRIBE permission
        CompletableFuture<ListOffsetsResponse> response = new CompletableFuture<>();
        Set<TableBucket> tableBuckets = getListOffsetsData(request);
        replicaManager.listOffsets(
                new ListOffsetsParam(
                        request.getFollowerServerId(),
                        request.hasOffsetType() ? request.getOffsetType() : null,
                        request.hasStartTimestamp() ? request.getStartTimestamp() : null),
                tableBuckets,
                (responseList) -> response.complete(makeListOffsetsResponse(responseList)));
        return response;
    }

    @Override
    public CompletableFuture<InitWriterResponse> initWriter(InitWriterRequest request) {
        List<TablePath> tablePathsList =
                request.getTablePathsList().stream()
                        .map(ServerRpcMessageUtils::toTablePath)
                        .collect(Collectors.toList());
        authorizeAnyTable(WRITE, tablePathsList);
        CompletableFuture<InitWriterResponse> response = new CompletableFuture<>();
        response.complete(makeInitWriterResponse(metadataManager.initWriterId()));
        return response;
    }

    @Override
    public CompletableFuture<NotifyRemoteLogOffsetsResponse> notifyRemoteLogOffsets(
            NotifyRemoteLogOffsetsRequest request) {
        CompletableFuture<NotifyRemoteLogOffsetsResponse> response = new CompletableFuture<>();
        replicaManager.notifyRemoteLogOffsets(
                getNotifyRemoteLogOffsetsData(request), response::complete);
        return response;
    }

    @Override
    public CompletableFuture<NotifyKvSnapshotOffsetResponse> notifyKvSnapshotOffset(
            NotifyKvSnapshotOffsetRequest request) {
        CompletableFuture<NotifyKvSnapshotOffsetResponse> response = new CompletableFuture<>();
        replicaManager.notifyKvSnapshotOffset(
                getNotifySnapshotOffsetData(request), response::complete);
        return response;
    }

    @Override
    public CompletableFuture<NotifyLakeTableOffsetResponse> notifyLakeTableOffset(
            NotifyLakeTableOffsetRequest request) {
        CompletableFuture<NotifyLakeTableOffsetResponse> response = new CompletableFuture<>();
        replicaManager.notifyLakeTableOffset(getNotifyLakeTableOffset(request), response::complete);
        return response;
    }

    private void authorizeTable(OperationType operationType, long tableId) {
        if (authorizer != null) {
            TablePath tablePath = metadataCache.getTablePath(tableId).orElse(null);
            if (tablePath == null) {
                throw new UnknownTableOrBucketException(
                        String.format(
                                "This server %s does not know this table ID %s. This may happen when the table "
                                        + "metadata cache in the server is not updated yet.",
                                serviceName, tableId));
            }
            if (!authorizer.isAuthorized(
                    currentSession(), operationType, Resource.table(tablePath))) {
                throw new AuthorizationException(
                        String.format(
                                "No permission to %s table %s in database %s",
                                operationType,
                                tablePath.getTableName(),
                                tablePath.getDatabaseName()));
            }
        }
    }

    private void authorizeAnyTable(OperationType operationType, List<TablePath> tablePaths) {
        if (authorizer != null) {
            if (tablePaths.isEmpty()) {
                throw new AuthorizationException(
                        "The request of InitWriter requires non empty table paths for authorization.");
            }

            for (TablePath tablePath : tablePaths) {
                Resource tableResource = Resource.table(tablePath);
                if (authorizer.isAuthorized(currentSession(), operationType, tableResource)) {
                    // authorized success if one of the tables has the permission
                    return;
                }
            }
            throw new AuthorizationException(
                    String.format(
                            "No %s permission among all the tables: %s",
                            operationType, tablePaths));
        }
    }

    /**
     * Authorize the given request data for each table bucket, and return the successfully
     * authorized request data. The failed authorization will be put into the errorResponseMap.
     */
    private <T, K extends ResultForBucket> Map<TableBucket, T> authorizeRequestData(
            OperationType operationType,
            Map<TableBucket, T> requestData,
            Map<TableBucket, K> errorResponseMap,
            BiFunction<TableBucket, ApiError, K> resultCreator) {
        if (authorizer == null) {
            // return all request data if authorization is disabled.
            return requestData;
        }

        Map<TableBucket, T> interesting = new HashMap<>();
        Set<Long> filteredTableIds = filterAuthorizedTables(requestData.keySet(), operationType);
        requestData.forEach(
                (tableBucket, bucketData) -> {
                    long tableId = tableBucket.getTableId();
                    Optional<TablePath> tablePathOpt = metadataCache.getTablePath(tableId);
                    if (!tablePathOpt.isPresent()) {
                        errorResponseMap.put(
                                tableBucket,
                                resultCreator.apply(
                                        tableBucket,
                                        new ApiError(
                                                Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION,
                                                String.format(
                                                        "This server %s does not know this table ID %s. "
                                                                + "This may happen when the table metadata cache in the server is not updated yet.",
                                                        serviceName, tableId))));
                    } else if (!filteredTableIds.contains(tableId)) {
                        TablePath tablePath = tablePathOpt.get();
                        errorResponseMap.put(
                                tableBucket,
                                resultCreator.apply(
                                        tableBucket,
                                        new ApiError(
                                                Errors.AUTHORIZATION_EXCEPTION,
                                                String.format(
                                                        "No permission to %s table %s in database %s",
                                                        operationType,
                                                        tablePath.getTableName(),
                                                        tablePath.getDatabaseName()))));
                    } else {
                        interesting.put(tableBucket, bucketData);
                    }
                });
        return interesting;
    }

    private Set<Long> filterAuthorizedTables(
            Collection<TableBucket> tableBuckets, OperationType operationType) {
        return tableBuckets.stream()
                .map(TableBucket::getTableId)
                .distinct()
                .filter(
                        tableId -> {
                            Optional<TablePath> tablePathOpt = metadataCache.getTablePath(tableId);
                            return tablePathOpt.isPresent()
                                    && authorizer != null
                                    && authorizer.isAuthorized(
                                            currentSession(),
                                            operationType,
                                            Resource.table(tablePathOpt.get()));
                        })
                .collect(Collectors.toSet());
    }
}
