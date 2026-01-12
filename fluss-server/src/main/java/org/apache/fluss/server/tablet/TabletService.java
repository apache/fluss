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

package org.apache.fluss.server.tablet;

import org.apache.fluss.cluster.ServerType;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PrefixLookupResultForBucket;
import org.apache.fluss.rpc.entity.ResultForBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.InitWriterRequest;
import org.apache.fluss.rpc.messages.InitWriterResponse;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.LookupResponse;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.MetadataResponse;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyKvSnapshotOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import org.apache.fluss.rpc.messages.NotifyLakeTableOffsetResponse;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import org.apache.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import org.apache.fluss.rpc.messages.NotifyRemoteLogOffsetsResponse;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.DynamicConfigManager;
import org.apache.fluss.server.RpcServiceBase;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.UserContext;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.ListOffsetsParam;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.replica.ReplicaManager;
import org.apache.fluss.server.utils.ServerRpcMessageUtils;
import org.apache.fluss.server.zk.ZooKeeperClient;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.fluss.security.acl.OperationType.READ;
import static org.apache.fluss.security.acl.OperationType.WRITE;
import static org.apache.fluss.server.coordinator.CoordinatorContext.INITIAL_COORDINATOR_EPOCH;
import static org.apache.fluss.server.log.FetchParams.DEFAULT_MAX_WAIT_MS_WHEN_MIN_BYTES_ENABLE;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getListOffsetsData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getNotifyLakeTableOffset;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getNotifyLeaderAndIsrRequestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getNotifyRemoteLogOffsetsData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getNotifySnapshotOffsetData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getProduceLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getPutKvData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getStopReplicaData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getTargetColumns;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getUpdateMetadataRequestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeInitWriterResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeLimitScanResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeListOffsetsResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeLookupResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeNotifyLeaderAndIsrResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makePrefixLookupResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeProduceLogResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makePutKvResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeStopReplicaResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toLookupData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.toPrefixLookupData;

/** An RPC Gateway service for tablet server. */
public final class TabletService extends RpcServiceBase implements TabletServerGateway {

    private final String serviceName;
    private final ReplicaManager replicaManager;
    private final TabletServerMetadataCache metadataCache;
    private final TabletServerMetadataProvider metadataFunctionProvider;

    public TabletService(
            int serverId,
            FileSystem remoteFileSystem,
            ZooKeeperClient zkClient,
            ReplicaManager replicaManager,
            TabletServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            @Nullable Authorizer authorizer,
            DynamicConfigManager dynamicConfigManager,
            ExecutorService ioExecutor) {
        super(
                remoteFileSystem,
                ServerType.TABLET_SERVER,
                zkClient,
                metadataManager,
                authorizer,
                dynamicConfigManager,
                ioExecutor);
        this.serviceName = "server-" + serverId;
        this.replicaManager = replicaManager;
        this.metadataCache = metadataCache;
        this.metadataFunctionProvider =
                new TabletServerMetadataProvider(zkClient, metadataManager, metadataCache);
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
                new UserContext(currentSession().getPrincipal()),
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
                new UserContext(currentSession().getPrincipal()),
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
        MetadataResponse metadataResponse =
                processMetadataRequest(
                        request,
                        currentListenerName(),
                        currentSession(),
                        authorizer,
                        metadataCache,
                        metadataFunctionProvider);
        return CompletableFuture.completedFuture(metadataResponse);
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

    @Override
    public void authorizeTable(OperationType operationType, long tableId) {
        if (authorizer != null) {
            TablePath tablePath = metadataCache.getTablePath(tableId).orElse(null);
            if (tablePath == null) {
                throw new UnknownTableOrBucketException(
                        String.format(
                                "This server %s does not know this table ID %s. This may happen when the table "
                                        + "metadata cache in the server is not updated yet.",
                                serviceName, tableId));
            }
            authorizeTable(operationType, tablePath);
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
