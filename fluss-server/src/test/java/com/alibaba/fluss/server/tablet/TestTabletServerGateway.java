/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
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

package com.alibaba.fluss.server.tablet;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.ApiMessage;
import com.alibaba.fluss.rpc.messages.ApiVersionsRequest;
import com.alibaba.fluss.rpc.messages.ApiVersionsResponse;
import com.alibaba.fluss.rpc.messages.DatabaseExistsRequest;
import com.alibaba.fluss.rpc.messages.DatabaseExistsResponse;
import com.alibaba.fluss.rpc.messages.DescribeLakeStorageRequest;
import com.alibaba.fluss.rpc.messages.DescribeLakeStorageResponse;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.rpc.messages.GetDatabaseRequest;
import com.alibaba.fluss.rpc.messages.GetDatabaseResponse;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenRequest;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotResponse;
import com.alibaba.fluss.rpc.messages.GetLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetLakeTableSnapshotResponse;
import com.alibaba.fluss.rpc.messages.GetPartitionSnapshotRequest;
import com.alibaba.fluss.rpc.messages.GetPartitionSnapshotResponse;
import com.alibaba.fluss.rpc.messages.GetTableRequest;
import com.alibaba.fluss.rpc.messages.GetTableResponse;
import com.alibaba.fluss.rpc.messages.GetTableSchemaRequest;
import com.alibaba.fluss.rpc.messages.GetTableSchemaResponse;
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.messages.InitWriterResponse;
import com.alibaba.fluss.rpc.messages.LimitScanRequest;
import com.alibaba.fluss.rpc.messages.LimitScanResponse;
import com.alibaba.fluss.rpc.messages.ListDatabasesRequest;
import com.alibaba.fluss.rpc.messages.ListDatabasesResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListOffsetsResponse;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosRequest;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosResponse;
import com.alibaba.fluss.rpc.messages.ListTablesRequest;
import com.alibaba.fluss.rpc.messages.ListTablesResponse;
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
import com.alibaba.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import com.alibaba.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import com.alibaba.fluss.rpc.messages.PbStopReplicaReqForBucket;
import com.alibaba.fluss.rpc.messages.PbStopReplicaRespForBucket;
import com.alibaba.fluss.rpc.messages.PbTableBucket;
import com.alibaba.fluss.rpc.messages.PrefixLookupRequest;
import com.alibaba.fluss.rpc.messages.PrefixLookupResponse;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.rpc.messages.PutKvResponse;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.messages.StopReplicaResponse;
import com.alibaba.fluss.rpc.messages.TableExistsRequest;
import com.alibaba.fluss.rpc.messages.TableExistsResponse;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.rpc.messages.UpdateMetadataResponse;
import com.alibaba.fluss.server.entity.FetchData;
import com.alibaba.fluss.utils.types.Tuple2;

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

import static com.alibaba.fluss.server.utils.RpcMessageUtils.getFetchLogData;
import static com.alibaba.fluss.server.utils.RpcMessageUtils.makeFetchLogResponse;

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
    public CompletableFuture<GetKvSnapshotResponse> getKvSnapshot(GetKvSnapshotRequest request) {
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
    public CompletableFuture<GetPartitionSnapshotResponse> getPartitionSnapshot(
            GetPartitionSnapshotRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<DescribeLakeStorageResponse> describeLakeStorage(
            DescribeLakeStorageRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<GetLakeTableSnapshotResponse> getLakeTableSnapshot(
            GetLakeTableSnapshotRequest request) {
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
        Map<TableBucket, FetchData> fetchLogData = getFetchLogData(request);
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
    public CompletableFuture<GetDatabaseResponse> getDatabase(GetDatabaseRequest request) {
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
    public CompletableFuture<GetTableResponse> getTable(GetTableRequest request) {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<NotifyKvSnapshotOffsetResponse> notifyKvSnapshotOffset(
            NotifyKvSnapshotOffsetRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<NotifyLakeTableOffsetResponse> notifyLakeTableOffset(
            NotifyLakeTableOffsetRequest request) {
        throw new UnsupportedOperationException();
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
