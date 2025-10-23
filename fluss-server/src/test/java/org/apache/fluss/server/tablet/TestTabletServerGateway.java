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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.ApiMessage;
import org.apache.fluss.rpc.messages.ApiVersionsRequest;
import org.apache.fluss.rpc.messages.ApiVersionsResponse;
import org.apache.fluss.rpc.messages.DatabaseExistsRequest;
import org.apache.fluss.rpc.messages.DatabaseExistsResponse;
import org.apache.fluss.rpc.messages.DescribeClusterConfigsRequest;
import org.apache.fluss.rpc.messages.DescribeClusterConfigsResponse;
import org.apache.fluss.rpc.messages.FetchLogRequest;
import org.apache.fluss.rpc.messages.FetchLogResponse;
import org.apache.fluss.rpc.messages.GetDatabaseInfoRequest;
import org.apache.fluss.rpc.messages.GetDatabaseInfoResponse;
import org.apache.fluss.rpc.messages.GetFileSystemSecurityTokenRequest;
import org.apache.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import org.apache.fluss.rpc.messages.GetKvSnapshotMetadataRequest;
import org.apache.fluss.rpc.messages.GetKvSnapshotMetadataResponse;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsRequest;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.GetLatestLakeSnapshotRequest;
import org.apache.fluss.rpc.messages.GetLatestLakeSnapshotResponse;
import org.apache.fluss.rpc.messages.GetTableInfoRequest;
import org.apache.fluss.rpc.messages.GetTableInfoResponse;
import org.apache.fluss.rpc.messages.GetTableSchemaRequest;
import org.apache.fluss.rpc.messages.GetTableSchemaResponse;
import org.apache.fluss.rpc.messages.InitWriterRequest;
import org.apache.fluss.rpc.messages.InitWriterResponse;
import org.apache.fluss.rpc.messages.LimitScanRequest;
import org.apache.fluss.rpc.messages.LimitScanResponse;
import org.apache.fluss.rpc.messages.ListAclsRequest;
import org.apache.fluss.rpc.messages.ListAclsResponse;
import org.apache.fluss.rpc.messages.ListDatabasesRequest;
import org.apache.fluss.rpc.messages.ListDatabasesResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListOffsetsResponse;
import org.apache.fluss.rpc.messages.ListPartitionInfosRequest;
import org.apache.fluss.rpc.messages.ListPartitionInfosResponse;
import org.apache.fluss.rpc.messages.ListTablesRequest;
import org.apache.fluss.rpc.messages.ListTablesResponse;
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
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import org.apache.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import org.apache.fluss.rpc.messages.PbStopReplicaReqForBucket;
import org.apache.fluss.rpc.messages.PbStopReplicaRespForBucket;
import org.apache.fluss.rpc.messages.PbTableBucket;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.PrefixLookupResponse;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.ProduceLogResponse;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.rpc.messages.PutKvResponse;
import org.apache.fluss.rpc.messages.StopReplicaRequest;
import org.apache.fluss.rpc.messages.StopReplicaResponse;
import org.apache.fluss.rpc.messages.TableExistsRequest;
import org.apache.fluss.rpc.messages.TableExistsResponse;
import org.apache.fluss.rpc.messages.UpdateMetadataRequest;
import org.apache.fluss.rpc.messages.UpdateMetadataResponse;
import org.apache.fluss.server.entity.FetchReqInfo;
import org.apache.fluss.utils.types.Tuple2;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.fluss.server.utils.ServerRpcMessageUtils.getFetchLogData;
import static org.apache.fluss.server.utils.ServerRpcMessageUtils.makeFetchLogResponse;

/** A {@link TabletServerGateway} for test purpose. */
public class TestTabletServerGateway implements TabletServerGateway {

    private final boolean alwaysFail;
    private final AtomicLong writerId = new AtomicLong(0);

    // Use concurrent queue for storing request and related completable future response so that
    // requests may be queried from a different thread.
    private final Queue<Tuple2<ApiMessage, CompletableFuture<?>>> requests =
            new ConcurrentLinkedDeque<>();

    public TestTabletServerGateway(boolean alwaysFail) {
        this.alwaysFail = alwaysFail;
    }

    @Override
    public CompletableFuture<UpdateMetadataResponse> updateMetadata(UpdateMetadataRequest request) {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<GetLatestKvSnapshotsResponse> getLatestKvSnapshots(
            GetLatestKvSnapshotsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetKvSnapshotMetadataResponse> getKvSnapshotMetadata(
            GetKvSnapshotMetadataRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetFileSystemSecurityTokenResponse> getFileSystemSecurityToken(
            GetFileSystemSecurityTokenRequest request) {
        return CompletableFuture.completedFuture(new GetFileSystemSecurityTokenResponse());
    }

    @Override
    public CompletableFuture<ListPartitionInfosResponse> listPartitionInfos(
            ListPartitionInfosRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetLatestLakeSnapshotResponse> getLatestLakeSnapshot(
            GetLatestLakeSnapshotRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ProduceLogResponse> produceLog(ProduceLogRequest request) {
        CompletableFuture<ProduceLogResponse> response = new CompletableFuture<>();
        requests.add(Tuple2.of(request, response));
        return response;
    }

    @Override
    public CompletableFuture<FetchLogResponse> fetchLog(FetchLogRequest request) {
        Map<TableBucket, FetchReqInfo> fetchLogData = getFetchLogData(request);
        Map<TableBucket, FetchLogResultForBucket> resultForBucketMap = new HashMap<>();
        fetchLogData.forEach(
                (tableBucket, fetchData) -> {
                    FetchLogResultForBucket fetchLogResultForBucket =
                            new FetchLogResultForBucket(tableBucket, MemoryLogRecords.EMPTY, 0L);
                    resultForBucketMap.put(tableBucket, fetchLogResultForBucket);
                });
        return CompletableFuture.completedFuture(makeFetchLogResponse(resultForBucketMap));
    }

    @Override
    public CompletableFuture<PutKvResponse> putKv(PutKvRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<LookupResponse> lookup(LookupRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<PrefixLookupResponse> prefixLookup(PrefixLookupRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<LimitScanResponse> limitScan(LimitScanRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ListOffsetsResponse> listOffsets(ListOffsetsRequest request) {
        return null;
    }

    @Override
    public CompletableFuture<ApiVersionsResponse> apiVersions(ApiVersionsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListDatabasesResponse> listDatabases(ListDatabasesRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetDatabaseInfoResponse> getDatabaseInfo(
            GetDatabaseInfoRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DatabaseExistsResponse> databaseExists(DatabaseExistsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListTablesResponse> listTables(ListTablesRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetTableInfoResponse> getTableInfo(GetTableInfoRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetTableSchemaResponse> getTableSchema(GetTableSchemaRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<TableExistsResponse> tableExists(TableExistsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<NotifyLeaderAndIsrResponse> notifyLeaderAndIsr(
            NotifyLeaderAndIsrRequest notifyLeaderAndIsrRequest) {
        if (alwaysFail) {
            List<PbNotifyLeaderAndIsrRespForBucket> bucketsResps = new ArrayList<>();
            for (PbNotifyLeaderAndIsrReqForBucket pbNotifyLeaderForBucket :
                    notifyLeaderAndIsrRequest.getNotifyBucketsLeaderReqsList()) {
                PbNotifyLeaderAndIsrRespForBucket pbNotifyLeaderRespForBucket =
                        new PbNotifyLeaderAndIsrRespForBucket();
                pbNotifyLeaderRespForBucket
                        .setTableBucket()
                        .setTableId(pbNotifyLeaderForBucket.getTableBucket().getTableId())
                        .setBucketId(pbNotifyLeaderForBucket.getTableBucket().getBucketId());
                pbNotifyLeaderRespForBucket.setErrorCode(1);
                pbNotifyLeaderRespForBucket.setErrorMessage(
                        "mock notifyLeaderAndIsr fail for test purpose.");
                bucketsResps.add(pbNotifyLeaderRespForBucket);
            }
            NotifyLeaderAndIsrResponse notifyLeaderAndIsrResponse =
                    new NotifyLeaderAndIsrResponse();
            notifyLeaderAndIsrResponse.addAllNotifyBucketsLeaderResps(bucketsResps);
            return CompletableFuture.completedFuture(notifyLeaderAndIsrResponse);
        } else {
            return CompletableFuture.completedFuture(new NotifyLeaderAndIsrResponse());
        }
    }

    @Override
    public CompletableFuture<StopReplicaResponse> stopReplica(
            StopReplicaRequest stopReplicaRequest) {
        StopReplicaResponse stopReplicaResponse;
        if (alwaysFail) {
            stopReplicaResponse =
                    mockStopReplicaResponse(
                            stopReplicaRequest, 1, "mock stopReplica fail for test purpose.");
        } else {
            stopReplicaResponse = mockStopReplicaResponse(stopReplicaRequest, null, null);
        }
        return CompletableFuture.completedFuture(stopReplicaResponse);
    }

    @Override
    public CompletableFuture<InitWriterResponse> initWriter(InitWriterRequest request) {
        return CompletableFuture.completedFuture(
                new InitWriterResponse().setWriterId(writerId.getAndIncrement()));
    }

    @Override
    public CompletableFuture<NotifyRemoteLogOffsetsResponse> notifyRemoteLogOffsets(
            NotifyRemoteLogOffsetsRequest request) {
        CompletableFuture<NotifyRemoteLogOffsetsResponse> response = new CompletableFuture<>();
        requests.add(Tuple2.of(request, response));
        return response;
    }

    @Override
    public CompletableFuture<NotifyKvSnapshotOffsetResponse> notifyKvSnapshotOffset(
            NotifyKvSnapshotOffsetRequest request) {
        CompletableFuture<NotifyKvSnapshotOffsetResponse> response = new CompletableFuture<>();
        requests.add(Tuple2.of(request, response));
        return response;
    }

    @Override
    public CompletableFuture<NotifyLakeTableOffsetResponse> notifyLakeTableOffset(
            NotifyLakeTableOffsetRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<ListAclsResponse> listAcls(ListAclsRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DescribeClusterConfigsResponse> describeClusterConfigs(
            DescribeClusterConfigsRequest request) {
        throw new UnsupportedOperationException();
    }

    public int pendingRequestSize() {
        return requests.size();
    }

    public ApiMessage getRequest(int index) {
        if (requests.isEmpty()) {
            throw new IllegalStateException("No requests pending for inbound response.");
        }

        // Index out of bounds check.
        if (index >= requests.size()) {
            throw new IllegalArgumentException(
                    "Index " + index + " is out of bounds for requests queue.");
        }

        if (index == 0) {
            return requests.peek().f0;
        } else {
            int currentIndex = 0;
            for (Tuple2<ApiMessage, CompletableFuture<?>> tuple : requests) {
                if (currentIndex == index) {
                    return tuple.f0;
                }
                currentIndex++;
            }
        }
        return null;
    }

    public void response(int index, ApiMessage response) {
        if (requests.isEmpty()) {
            throw new IllegalStateException("No requests pending for inbound response.");
        }

        // Index out of bounds check.
        if (index >= requests.size()) {
            throw new IllegalArgumentException(
                    "Index " + index + " is out of bounds for requests queue.");
        }

        CompletableFuture<ApiMessage> result = null;
        int currentIndex = 0;
        for (Iterator<Tuple2<ApiMessage, CompletableFuture<?>>> it = requests.iterator();
                it.hasNext(); ) {
            Tuple2<ApiMessage, CompletableFuture<?>> tuple = it.next();
            if (currentIndex == index) {
                result = (CompletableFuture<ApiMessage>) tuple.f1;
                it.remove();
                break;
            }
            currentIndex++;
        }

        if (result != null) {
            result.complete(response);
        } else {
            throw new IllegalStateException(
                    "The future to complete was not found at index " + index);
        }
    }

    private StopReplicaResponse mockStopReplicaResponse(
            StopReplicaRequest stopReplicaRequest,
            @Nullable Integer errCode,
            @Nullable String errMsg) {
        List<PbStopReplicaRespForBucket> protoStopReplicaRespForBuckets = new ArrayList<>();
        for (PbStopReplicaReqForBucket protoStopReplicaForBucket :
                stopReplicaRequest.getStopReplicasReqsList()) {
            PbStopReplicaRespForBucket pbStopReplicaRespForBucket =
                    new PbStopReplicaRespForBucket();
            PbTableBucket pbTableBucket =
                    pbStopReplicaRespForBucket
                            .setTableBucket()
                            .setTableId(protoStopReplicaForBucket.getTableBucket().getTableId())
                            .setBucketId(protoStopReplicaForBucket.getTableBucket().getBucketId());
            if (protoStopReplicaForBucket.getTableBucket().hasPartitionId()) {
                pbTableBucket.setPartitionId(
                        protoStopReplicaForBucket.getTableBucket().getPartitionId());
            }
            protoStopReplicaRespForBuckets.add(pbStopReplicaRespForBucket);
            if (errCode != null) {
                pbStopReplicaRespForBucket.setErrorCode(errCode);
                pbStopReplicaRespForBucket.setErrorMessage(errMsg);
            }
        }
        StopReplicaResponse stopReplicaResponse = new StopReplicaResponse();
        stopReplicaResponse.addAllStopReplicasResps(protoStopReplicaRespForBuckets);
        return stopReplicaResponse;
    }
}
