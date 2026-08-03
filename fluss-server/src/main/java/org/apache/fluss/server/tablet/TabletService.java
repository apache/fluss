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
import org.apache.fluss.exception.InvalidScanRequestException;
import org.apache.fluss.exception.NotLeaderOrFollowerException;
import org.apache.fluss.exception.ScannerExpiredException;
import org.apache.fluss.exception.StaleMetadataException;
import org.apache.fluss.exception.UnknownScannerIdException;
import org.apache.fluss.exception.UnknownTableOrBucketException;
import org.apache.fluss.fs.FileSystem;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.DefaultValueRecordBatch;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.entity.LookupResultForBucket;
import org.apache.fluss.rpc.entity.PrefixLookupResultForBucket;
import org.apache.fluss.rpc.entity.ResultForBucket;
import org.apache.fluss.rpc.gateway.CoordinatorGateway;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.GetClusterHealthRequest;
import org.apache.fluss.rpc.messages.GetClusterHealthResponse;
import org.apache.fluss.rpc.messages.GetTableStatsRequest;
import org.apache.fluss.rpc.messages.GetTableStatsResponse;
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
import org.apache.fluss.rpc.messages.PbScanReqForBucket;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.messages.ScanKvRequest;
import org.apache.fluss.rpc.messages.ScanKvResponse;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.rpc.protocol.FetchLogReadPreference;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.security.acl.OperationType;
import org.apache.fluss.security.acl.Resource;
import org.apache.fluss.server.DynamicConfigManager;
import org.apache.fluss.server.RpcServiceBase;
import org.apache.fluss.server.authorizer.Authorizer;
import org.apache.fluss.server.coordinator.MetadataManager;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.server.entity.NotifyKvSnapshotOffsetData;
import org.apache.fluss.server.entity.NotifyLakeTableOffsetData;
import org.apache.fluss.server.entity.NotifyLeaderAndIsrData;
import org.apache.fluss.server.entity.NotifyRemoteLogOffsetsData;
import org.apache.fluss.server.entity.StopReplicaData;
import org.apache.fluss.server.entity.UserContext;
import org.apache.fluss.server.kv.scan.OpenScanResult;
import org.apache.fluss.server.kv.scan.ScannerContext;
import org.apache.fluss.server.kv.scan.ScannerManager;
import org.apache.fluss.server.log.FetchParams;
import org.apache.fluss.server.log.FetchParamsBuilder;
import org.apache.fluss.server.log.FilterInfo;
import org.apache.fluss.server.log.ListOffsetsParam;
import org.apache.fluss.server.metadata.ClusterMetadata;
import org.apache.fluss.server.metadata.TabletServerMetadataCache;
import org.apache.fluss.server.metadata.TabletServerMetadataProvider;
import org.apache.fluss.server.replica.Replica;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.fluss.security.acl.OperationType.DESCRIBE;
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
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getTableFilterInfoMap;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getTableStatsRequestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getTargetColumns;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getUpdateMetadataRequestData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeGetTableStatsResponse;
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
    private final ScannerManager scannerManager;
    private final CoordinatorGateway coordinatorGateway;
    private final String interListenerName;
    private final ExecutorService replicaStateChangeExecutor;

    public TabletService(
            int serverId,
            FileSystem remoteFileSystem,
            ZooKeeperClient zkClient,
            ReplicaManager replicaManager,
            TabletServerMetadataCache metadataCache,
            MetadataManager metadataManager,
            @Nullable Authorizer authorizer,
            DynamicConfigManager dynamicConfigManager,
            ExecutorService ioExecutor,
            ExecutorService replicaStateChangeExecutor,
            ScannerManager scannerManager,
            CoordinatorGateway coordinatorGateway,
            String interListenerName) {
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
        this.scannerManager = scannerManager;
        this.coordinatorGateway = coordinatorGateway;
        this.interListenerName = interListenerName;
        this.replicaStateChangeExecutor = replicaStateChangeExecutor;
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
        Map<Long, FilterInfo> tableFilterInfoMap = getTableFilterInfoMap(request);
        FetchLogReadPreference readPreference =
                request.hasReadPreference()
                        ? FetchLogReadPreference.from(request.getReadPreference())
                        : FetchLogReadPreference.LOCAL_FIRST;
        if (request.hasMinBytes()) {
            fetchParams =
                    new FetchParamsBuilder(request.getFollowerServerId(), request.getMaxBytes())
                            .withMinFetchBytes(request.getMinBytes())
                            .withMaxWaitMs(
                                    request.hasMaxWaitMs()
                                            ? request.getMaxWaitMs()
                                            : DEFAULT_MAX_WAIT_MS_WHEN_MIN_BYTES_ENABLE)
                            .withTableFilterInfoMap(tableFilterInfoMap)
                            .withReadPreference(readPreference)
                            .build();
        } else {
            fetchParams =
                    new FetchParamsBuilder(request.getFollowerServerId(), request.getMaxBytes())
                            .withTableFilterInfoMap(tableFilterInfoMap)
                            .withReadPreference(readPreference)
                            .build();
        }

        return fetchParams;
    }

    @Override
    public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
        authorizeTable(WRITE, request.getTableId());

        Map<TableBucket, KvRecordBatch> putKvData = getPutKvData(request);
        // Get mergeMode from request, default to DEFAULT if not set
        MergeMode mergeMode =
                request.hasAggMode()
                        ? MergeMode.fromValue(request.getAggMode())
                        : MergeMode.DEFAULT;
        CompletableFuture<PutKvResponse> response = new CompletableFuture<>();
        replicaManager.putRecordsToKv(
                request.getTimeoutMs(),
                request.getAcks(),
                putKvData,
                getTargetColumns(request),
                mergeMode,
                currentSession().getApiVersion(),
                bucketResponse -> response.complete(makePutKvResponse(bucketResponse)));
        return response;
    }

    @Override
    public CompletableFuture<LookupResponse> lookup(LookupRequest request) {
        Map<TableBucket, List<byte[]>> lookupData = toLookupData(request);
        Map<TableBucket, LookupResultForBucket> errorResponseMap = new HashMap<>();
        CompletableFuture<LookupResponse> response = new CompletableFuture<>();

        if (request.hasInsertIfNotExists() && request.isInsertIfNotExists()) {
            authorizeTable(WRITE, request.getTableId());
            replicaManager.lookups(
                    request.isInsertIfNotExists(),
                    request.getTimeoutMs(),
                    request.getAcks(),
                    lookupData,
                    currentSession().getApiVersion(),
                    value -> response.complete(makeLookupResponse(value, errorResponseMap)));
        } else {
            Map<TableBucket, List<byte[]>> interesting =
                    authorizeRequestData(
                            READ, lookupData, errorResponseMap, LookupResultForBucket::new);
            if (interesting.isEmpty()) {
                return CompletableFuture.completedFuture(makeLookupResponse(errorResponseMap));
            }
            replicaManager.lookups(
                    lookupData,
                    currentSession().getApiVersion(),
                    value -> response.complete(makeLookupResponse(value, errorResponseMap)));
        }
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
                currentSession().getApiVersion(),
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
    public CompletableFuture<GetTableStatsResponse> getTableStats(GetTableStatsRequest request) {
        authorizeTable(READ, request.getTableId());

        CompletableFuture<GetTableStatsResponse> response = new CompletableFuture<>();
        replicaManager.getTableStats(
                getTableStatsRequestData(request),
                result -> response.complete(makeGetTableStatsResponse(result)));
        return response;
    }

    @Override
    public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest) {
        CompletableFuture<NotifyLeaderAndIsrResponse> response = new CompletableFuture<>();
        int coordinatorEpoch = notifyLeaderAndIsrRequest.getCoordinatorEpoch();
        List<NotifyLeaderAndIsrData> notifyLeaderAndIsrRequestData =
                getNotifyLeaderAndIsrRequestData(notifyLeaderAndIsrRequest);
        return submitReplicaStateChange(
                response,
                result ->
                        replicaManager.becomeLeaderOrFollower(
                                coordinatorEpoch,
                                notifyLeaderAndIsrRequestData,
                                value -> result.complete(makeNotifyLeaderAndIsrResponse(value))));
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
        ClusterMetadata clusterMetadata = getUpdateMetadataRequestData(request);
        CompletableFuture<UpdateMetadataResponse> response = new CompletableFuture<>();
        return submitReplicaStateChange(
                response,
                result -> {
                    replicaManager.maybeUpdateMetadataCache(coordinatorEpoch, clusterMetadata);
                    result.complete(new UpdateMetadataResponse());
                });
    }

    @Override
    public CompletableFuture<StopReplicaResponse> stopReplica(
            StopReplicaRequest stopReplicaRequest) {
        CompletableFuture<StopReplicaResponse> response = new CompletableFuture<>();
        int coordinatorEpoch = stopReplicaRequest.getCoordinatorEpoch();
        List<StopReplicaData> stopReplicaData = getStopReplicaData(stopReplicaRequest);
        return submitReplicaStateChange(
                response,
                result ->
                        replicaManager.stopReplicas(
                                coordinatorEpoch,
                                stopReplicaData,
                                value -> result.complete(makeStopReplicaResponse(value))));
    }

    @Override
    public CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request) {
        authorizeTable(DESCRIBE, request.getTableId());
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
        // This is an internal-only RPC, reject all external sessions
        if (!currentSession().isInternal()) {
            CompletableFuture<NotifyRemoteLogOffsetsResponse> failedFuture =
                    new CompletableFuture<>();
            failedFuture.completeExceptionally(
                    new AuthorizationException(
                            "NotifyRemoteLogOffsets is an internal RPC and cannot be called by external clients"));
            return failedFuture;
        }
        CompletableFuture<NotifyRemoteLogOffsetsResponse> response = new CompletableFuture<>();
        NotifyRemoteLogOffsetsData notifyRemoteLogOffsetsData =
                getNotifyRemoteLogOffsetsData(request);
        return submitReplicaStateChange(
                response,
                result ->
                        replicaManager.notifyRemoteLogOffsets(
                                notifyRemoteLogOffsetsData, result::complete));
    }

    @Override
    public CompletableFuture<NotifyKvSnapshotOffsetResponse> notifyKvSnapshotOffset(
            NotifyKvSnapshotOffsetRequest request) {
        CompletableFuture<NotifyKvSnapshotOffsetResponse> response = new CompletableFuture<>();
        NotifyKvSnapshotOffsetData notifyKvSnapshotOffsetData =
                getNotifySnapshotOffsetData(request);
        return submitReplicaStateChange(
                response,
                result ->
                        replicaManager.notifyKvSnapshotOffset(
                                notifyKvSnapshotOffsetData, result::complete));
    }

    @Override
    public CompletableFuture<NotifyLakeTableOffsetResponse> notifyLakeTableOffset(
            NotifyLakeTableOffsetRequest request) {
        CompletableFuture<NotifyLakeTableOffsetResponse> response = new CompletableFuture<>();
        NotifyLakeTableOffsetData notifyLakeTableOffsetData = getNotifyLakeTableOffset(request);
        return submitReplicaStateChange(
                response,
                result ->
                        replicaManager.notifyLakeTableOffset(
                                notifyLakeTableOffsetData, result::complete));
    }

    @Override
    public CompletableFuture<GetClusterHealthResponse> getClusterHealth(
            GetClusterHealthRequest request) {
        // Tablet servers don't own the cluster-wide health view; we forward the call to the
        // coordinator over the internal listener.
        if (authorizer != null) {
            authorizer.authorize(currentSession(), OperationType.DESCRIBE, Resource.cluster());
        }

        if (metadataCache.getCoordinatorServer(interListenerName) == null) {
            // Fail fast during the startup window before the tablet has received its first
            // UpdateMetadataRequest from the coordinator. The supplier inside coordinatorGateway
            // would otherwise block-and-retry, which is not what readiness probes want.
            CompletableFuture<GetClusterHealthResponse> failed = new CompletableFuture<>();
            failed.completeExceptionally(
                    new StaleMetadataException(
                            "Tablet server has not yet received coordinator metadata; cluster"
                                    + " health is unavailable."));
            return failed;
        }
        return coordinatorGateway.getClusterHealth(request);
    }

    @Override
    public CompletableFuture<ScanKvResponse> scanKv(ScanKvRequest request) {
        ScanKvResponse response = new ScanKvResponse();
        ScannerContext openedContext = null;
        ScannerContext acquiredContext = null;
        try {
            if (request.hasBucketScanReq() && request.hasScannerId()) {
                throw new InvalidScanRequestException(
                        "ScanKvRequest must not set both bucket_scan_req and scanner_id.");
            }
            if (request.hasBucketScanReq()
                    && request.hasCloseScanner()
                    && request.isCloseScanner()) {
                throw new InvalidScanRequestException(
                        "ScanKvRequest must not set close_scanner together with bucket_scan_req.");
            }
            if (!request.hasBucketScanReq() && !request.hasScannerId()) {
                throw new InvalidScanRequestException(
                        "ScanKvRequest must have either bucket_scan_req (new scan) "
                                + "or scanner_id (continuation).");
            }

            boolean isCloseRequest = request.hasCloseScanner() && request.isCloseScanner();

            // Validate batch_size_bytes up-front so malformed requests never open a snapshot.
            int effectiveBatchSize = 0;
            if (!isCloseRequest) {
                if (!request.hasBatchSizeBytes()) {
                    throw new InvalidScanRequestException(
                            "batch_size_bytes is required for data-fetching scan requests.");
                }
                int requestedBatchSize = request.getBatchSizeBytes();
                if (requestedBatchSize <= 0) {
                    throw new InvalidScanRequestException(
                            "batch_size_bytes must be greater than 0.");
                }
                effectiveBatchSize =
                        Math.min(requestedBatchSize, scannerManager.getMaxBatchSizeBytes());
            }

            ScannerContext context;

            boolean isNewScan = false;
            long initialLogOffset = 0L;

            if (request.hasBucketScanReq()) {
                PbScanReqForBucket bucketReq = request.getBucketScanReq();
                long tableId = bucketReq.getTableId();
                authorizeTable(READ, tableId);

                TableBucket tableBucket =
                        new TableBucket(
                                tableId,
                                bucketReq.hasPartitionId() ? bucketReq.getPartitionId() : null,
                                bucketReq.getBucketId());
                Long limit = bucketReq.hasLimit() ? bucketReq.getLimit() : null;

                OpenScanResult openResult =
                        scannerManager.createScanner(
                                replicaManager.getReplicaOrException(tableBucket), limit);
                isNewScan = true;
                initialLogOffset = openResult.getLogOffset();

                context = openResult.getContext();
                if (context == null) {
                    // Empty bucket: no session registered; still return the captured offset.
                    response.setHasMoreResults(false);
                    response.setLogOffset(initialLogOffset);
                    return CompletableFuture.completedFuture(response);
                }
                openedContext = context;
            } else {
                byte[] scannerId = request.getScannerId();
                context = scannerManager.getScanner(scannerId);
                if (context == null) {
                    if (isCloseRequest) {
                        response.setScannerId(scannerId);
                        response.setHasMoreResults(false);
                        return CompletableFuture.completedFuture(response);
                    }
                    if (scannerManager.isRecentlyExpired(scannerId)) {
                        throw new ScannerExpiredException(
                                "Scanner session has expired due to inactivity. "
                                        + "Please start a new scan.");
                    } else {
                        throw new UnknownScannerIdException(
                                "Unknown scanner ID. The session may have expired or "
                                        + "never existed.");
                    }
                }
                openedContext = context;
            }

            // Cursor-exclusion CAS: serialises concurrent same-scannerId RPCs and rejects if
            // close() has begun.
            if (!context.tryAcquireForUse()) {
                throw new InvalidScanRequestException(
                        String.format(
                                "Concurrent scan request on scanner ID for bucket %s, or session "
                                        + "is closing; only one in-flight scanKv RPC per scanner "
                                        + "is allowed.",
                                context.getTableBucket()));
            }
            acquiredContext = context;

            // Honour close even on a non-leader: the local session is still ours to release.
            // Close requests are exempt from callSeqId validation.
            if (isCloseRequest) {
                response.setScannerId(context.getScannerId());
                response.setHasMoreResults(false);
                return CompletableFuture.completedFuture(response);
            }

            // Validate callSeqId for all data-fetching requests (open + continuation).
            if (request.hasCallSeqId()) {
                int expectedSeqId = context.getCallSeqId() + 1;
                int requestSeqId = request.getCallSeqId();
                if (requestSeqId != expectedSeqId) {
                    throw new InvalidScanRequestException(
                            String.format(
                                    "Out-of-order scan request: expected callSeqId=%d but got %d.",
                                    expectedSeqId, requestSeqId));
                }
            } else {
                throw new InvalidScanRequestException(
                        "call_seq_id is required for ScanKV requests to ensure "
                                + "in-order processing and at-least-once semantics.");
            }

            // Catch a leadership flip ahead of the eventual closeScannersForBucket callback so
            // the client can redirect rather than consume a stale snapshot.
            if (!request.hasBucketScanReq()) {
                Replica replica = replicaManager.getReplicaOrException(context.getTableBucket());
                if (!replica.isLeader()) {
                    throw new NotLeaderOrFollowerException(
                            String.format(
                                    "Leader is no longer local for bucket %s; client should "
                                            + "restart the scan against the new leader.",
                                    context.getTableBucket()));
                }
            }

            // Refresh TTL only now that the request is fully validated.
            scannerManager.markAccessed(context);

            // Gate on builder.sizeInBytes() (not raw bytes) so the threshold reflects the
            // serialised batch. Always append at least one record so a tiny effectiveBatchSize
            // cannot produce an empty has_more=true response.
            DefaultValueRecordBatch.Builder builder = DefaultValueRecordBatch.builder();
            boolean appendedAny = false;
            while (context.isValid()
                    && (!appendedAny || builder.sizeInBytes() < effectiveBatchSize)) {
                builder.append(context.currentValue());
                context.advance();
                appendedAny = true;
            }

            boolean hasMore = context.isValid();
            if (!hasMore) {
                // RocksIterator.next() does not throw on internal errors; an unchecked status
                // would silently truncate the scan and report has_more=false to the client.
                context.checkIteratorStatus();
            }
            DefaultValueRecordBatch batch = builder.build();

            response.setScannerId(context.getScannerId());
            response.setHasMoreResults(hasMore);
            if (batch.sizeInBytes() > 0) {
                response.setRecords(batch.getSegment(), batch.getPosition(), batch.sizeInBytes());
            }
            if (isNewScan) {
                response.setLogOffset(initialLogOffset);
            }

            // Update callSeqId AFTER the response is prepared so a duplicate retry can be
            // detected via the in-order check.
            if (request.hasCallSeqId()) {
                context.setCallSeqId(request.getCallSeqId());
            }

            // Keep the session alive only if there's more to read; otherwise leave openedContext
            // set so finally drains it.
            if (hasMore) {
                openedContext = null;
            }

        } catch (Exception e) {
            if (e instanceof InterruptedException || e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            ApiError apiError = ApiError.fromThrowable(e);
            response.setErrorCode(apiError.error().code());
            response.setErrorMessage(apiError.message() != null ? apiError.message() : "");
        } finally {
            if (acquiredContext != null) {
                acquiredContext.releaseAfterUse();
            }
            if (openedContext != null) {
                scannerManager.removeScanner(openedContext);
            }
        }

        return CompletableFuture.completedFuture(response);
    }

    private <T> CompletableFuture<T> submitReplicaStateChange(
            CompletableFuture<T> response, Consumer<CompletableFuture<T>> action) {
        try {
            replicaStateChangeExecutor.execute(
                    () -> {
                        try {
                            action.accept(response);
                        } catch (Throwable t) {
                            response.completeExceptionally(t);
                        }
                    });
        } catch (Throwable t) {
            response.completeExceptionally(t);
        }
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
