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

package com.alibaba.fluss.server.utils;

import com.alibaba.fluss.cluster.ServerNode;
import com.alibaba.fluss.cluster.ServerType;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.ResolvedPartitionSpec;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.BytesViewLogRecords;
import com.alibaba.fluss.record.DefaultKvRecordBatch;
import com.alibaba.fluss.record.DefaultValueRecordBatch;
import com.alibaba.fluss.record.FileChannelChunk;
import com.alibaba.fluss.record.FileLogRecords;
import com.alibaba.fluss.record.KvRecordBatch;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.remote.RemoteLogFetchInfo;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.entity.LimitScanResultForBucket;
import com.alibaba.fluss.rpc.entity.ListOffsetsResultForBucket;
import com.alibaba.fluss.rpc.entity.LookupResultForBucket;
import com.alibaba.fluss.rpc.entity.PrefixLookupResultForBucket;
import com.alibaba.fluss.rpc.entity.ProduceLogResultForBucket;
import com.alibaba.fluss.rpc.entity.PutKvResultForBucket;
import com.alibaba.fluss.rpc.messages.AdjustIsrRequest;
import com.alibaba.fluss.rpc.messages.AdjustIsrResponse;
import com.alibaba.fluss.rpc.messages.CommitKvSnapshotRequest;
import com.alibaba.fluss.rpc.messages.CommitLakeTableSnapshotRequest;
import com.alibaba.fluss.rpc.messages.CommitRemoteLogManifestRequest;
import com.alibaba.fluss.rpc.messages.FetchLogRequest;
import com.alibaba.fluss.rpc.messages.FetchLogResponse;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotMetadataResponse;
import com.alibaba.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import com.alibaba.fluss.rpc.messages.GetLatestLakeSnapshotResponse;
import com.alibaba.fluss.rpc.messages.InitWriterResponse;
import com.alibaba.fluss.rpc.messages.LimitScanResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListOffsetsResponse;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosResponse;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.LookupResponse;
import com.alibaba.fluss.rpc.messages.NotifyKvSnapshotOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyLakeTableOffsetRequest;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrRequest;
import com.alibaba.fluss.rpc.messages.NotifyLeaderAndIsrResponse;
import com.alibaba.fluss.rpc.messages.NotifyRemoteLogOffsetsRequest;
import com.alibaba.fluss.rpc.messages.PbAdjustIsrReqForBucket;
import com.alibaba.fluss.rpc.messages.PbAdjustIsrReqForTable;
import com.alibaba.fluss.rpc.messages.PbAdjustIsrRespForBucket;
import com.alibaba.fluss.rpc.messages.PbAdjustIsrRespForTable;
import com.alibaba.fluss.rpc.messages.PbFetchLogReqForBucket;
import com.alibaba.fluss.rpc.messages.PbFetchLogReqForTable;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForTable;
import com.alibaba.fluss.rpc.messages.PbKeyValue;
import com.alibaba.fluss.rpc.messages.PbKvSnapshot;
import com.alibaba.fluss.rpc.messages.PbLakeSnapshotForBucket;
import com.alibaba.fluss.rpc.messages.PbLakeTableOffsetForBucket;
import com.alibaba.fluss.rpc.messages.PbLakeTableSnapshotInfo;
import com.alibaba.fluss.rpc.messages.PbListOffsetsRespForBucket;
import com.alibaba.fluss.rpc.messages.PbLookupReqForBucket;
import com.alibaba.fluss.rpc.messages.PbLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbNotifyLakeTableOffsetReqForBucket;
import com.alibaba.fluss.rpc.messages.PbNotifyLeaderAndIsrReqForBucket;
import com.alibaba.fluss.rpc.messages.PbNotifyLeaderAndIsrRespForBucket;
import com.alibaba.fluss.rpc.messages.PbPartitionSpec;
import com.alibaba.fluss.rpc.messages.PbPhysicalTablePath;
import com.alibaba.fluss.rpc.messages.PbPrefixLookupReqForBucket;
import com.alibaba.fluss.rpc.messages.PbPrefixLookupRespForBucket;
import com.alibaba.fluss.rpc.messages.PbProduceLogReqForBucket;
import com.alibaba.fluss.rpc.messages.PbProduceLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbPutKvReqForBucket;
import com.alibaba.fluss.rpc.messages.PbPutKvRespForBucket;
import com.alibaba.fluss.rpc.messages.PbRemoteLogSegment;
import com.alibaba.fluss.rpc.messages.PbRemotePathAndLocalFile;
import com.alibaba.fluss.rpc.messages.PbServerNode;
import com.alibaba.fluss.rpc.messages.PbStopReplicaReqForBucket;
import com.alibaba.fluss.rpc.messages.PbStopReplicaRespForBucket;
import com.alibaba.fluss.rpc.messages.PbTableBucket;
import com.alibaba.fluss.rpc.messages.PbTablePath;
import com.alibaba.fluss.rpc.messages.PbValue;
import com.alibaba.fluss.rpc.messages.PbValueList;
import com.alibaba.fluss.rpc.messages.PrefixLookupRequest;
import com.alibaba.fluss.rpc.messages.PrefixLookupResponse;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.messages.ProduceLogResponse;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.rpc.messages.PutKvResponse;
import com.alibaba.fluss.rpc.messages.StopReplicaRequest;
import com.alibaba.fluss.rpc.messages.StopReplicaResponse;
import com.alibaba.fluss.rpc.messages.UpdateMetadataRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.server.entity.AdjustIsrResultForBucket;
import com.alibaba.fluss.server.entity.CommitLakeTableSnapshotData;
import com.alibaba.fluss.server.entity.CommitRemoteLogManifestData;
import com.alibaba.fluss.server.entity.FetchData;
import com.alibaba.fluss.server.entity.LakeBucketOffset;
import com.alibaba.fluss.server.entity.NotifyKvSnapshotOffsetData;
import com.alibaba.fluss.server.entity.NotifyLakeTableOffsetData;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrData;
import com.alibaba.fluss.server.entity.NotifyLeaderAndIsrResultForBucket;
import com.alibaba.fluss.server.entity.NotifyRemoteLogOffsetsData;
import com.alibaba.fluss.server.entity.StopReplicaData;
import com.alibaba.fluss.server.entity.StopReplicaResultForBucket;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshot;
import com.alibaba.fluss.server.kv.snapshot.CompletedSnapshotJsonSerde;
import com.alibaba.fluss.server.kv.snapshot.KvSnapshotHandle;
import com.alibaba.fluss.server.zk.data.BucketSnapshot;
import com.alibaba.fluss.server.zk.data.LakeTableSnapshot;
import com.alibaba.fluss.server.zk.data.LeaderAndIsr;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.fluss.utils.Preconditions.checkNotNull;

/**
 * Utils for making rpc request/response from inner object or convert inner class to rpc
 * request/response.
 */
public class RpcMessageUtils {

    public static ByteBuffer toByteBuffer(ByteBuf buf) {
        if (buf.isDirect()) {
            return buf.nioBuffer();
        } else if (buf.hasArray()) {
            int offset = buf.arrayOffset() + buf.readerIndex();
            int length = buf.readableBytes();
            return ByteBuffer.wrap(buf.array(), offset, length);
        } else {
            // fallback to deep copy
            byte[] bytes = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), bytes);
            return ByteBuffer.wrap(bytes);
        }
    }

    public static TablePath toTablePath(PbTablePath pbTablePath) {
        return new TablePath(pbTablePath.getDatabaseName(), pbTablePath.getTableName());
    }

    public static PhysicalTablePath toPhysicalTablePath(PbPhysicalTablePath pbPhysicalPath) {
        return PhysicalTablePath.of(
                pbPhysicalPath.getDatabaseName(),
                pbPhysicalPath.getTableName(),
                pbPhysicalPath.hasPartitionName() ? pbPhysicalPath.getPartitionName() : null);
    }

    public static PbPhysicalTablePath fromPhysicalTablePath(PhysicalTablePath physicalPath) {
        PbPhysicalTablePath pbPath =
                new PbPhysicalTablePath()
                        .setDatabaseName(physicalPath.getDatabaseName())
                        .setTableName(physicalPath.getTableName());
        if (physicalPath.getPartitionName() != null) {
            pbPath.setPartitionName(physicalPath.getPartitionName());
        }
        return pbPath;
    }

    public static TableBucket toTableBucket(PbTableBucket protoTableBucket) {
        return new TableBucket(
                protoTableBucket.getTableId(),
                protoTableBucket.hasPartitionId() ? protoTableBucket.getPartitionId() : null,
                protoTableBucket.getBucketId());
    }

    public static ServerNode toServerNode(PbServerNode pbServerNode, ServerType serverType) {
        return new ServerNode(
                pbServerNode.getNodeId(),
                pbServerNode.getHost(),
                pbServerNode.getPort(),
                serverType);
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static UpdateMetadataRequest makeUpdateMetadataRequest(
            Optional<ServerNode> coordinatorServer, Set<ServerNode> aliveTableServers) {
        UpdateMetadataRequest updateMetadataRequest = new UpdateMetadataRequest();
        Set<PbServerNode> aliveTableServerNodes = new HashSet<>();
        for (ServerNode serverNode : aliveTableServers) {
            aliveTableServerNodes.add(
                    new PbServerNode()
                            .setNodeId(serverNode.id())
                            .setHost(serverNode.host())
                            .setPort(serverNode.port()));
        }
        updateMetadataRequest.addAllTabletServers(aliveTableServerNodes);
        coordinatorServer.map(
                node ->
                        updateMetadataRequest
                                .setCoordinatorServer()
                                .setNodeId(node.id())
                                .setHost(node.host())
                                .setPort(node.port()));
        return updateMetadataRequest;
    }

    public static PbNotifyLeaderAndIsrReqForBucket makeNotifyBucketLeaderAndIsr(
            NotifyLeaderAndIsrData notifyLeaderAndIsrData) {
        PbNotifyLeaderAndIsrReqForBucket reqForBucket =
                new PbNotifyLeaderAndIsrReqForBucket()
                        .setLeader(notifyLeaderAndIsrData.getLeader())
                        .setLeaderEpoch(notifyLeaderAndIsrData.getLeaderEpoch())
                        .setBucketEpoch(notifyLeaderAndIsrData.getBucketEpoch());

        TableBucket tb = notifyLeaderAndIsrData.getTableBucket();
        PbTableBucket pbTableBucket =
                reqForBucket
                        .setTableBucket()
                        .setTableId(tb.getTableId())
                        .setBucketId(tb.getBucket());
        if (tb.getPartitionId() != null) {
            pbTableBucket.setPartitionId(tb.getPartitionId());
        }

        PhysicalTablePath physicalTablePath = notifyLeaderAndIsrData.getPhysicalTablePath();
        reqForBucket
                .setPhysicalTablePath(fromPhysicalTablePath(physicalTablePath))
                .setReplicas(notifyLeaderAndIsrData.getReplicasArray())
                .setIsrs(notifyLeaderAndIsrData.getIsrArray());

        return reqForBucket;
    }

    public static List<NotifyLeaderAndIsrData> getNotifyLeaderAndIsrData(
            NotifyLeaderAndIsrRequest request) {
        List<NotifyLeaderAndIsrData> notifyLeaderAndIsrDataList = new ArrayList<>();
        for (PbNotifyLeaderAndIsrReqForBucket reqForBucket :
                request.getNotifyBucketsLeaderReqsList()) {
            List<Integer> replicas = new ArrayList<>();
            for (int i = 0; i < reqForBucket.getReplicasCount(); i++) {
                replicas.add(reqForBucket.getReplicaAt(i));
            }

            List<Integer> isr = new ArrayList<>();
            for (int i = 0; i < reqForBucket.getIsrsCount(); i++) {
                isr.add(reqForBucket.getIsrAt(i));
            }

            PbTableBucket pbTableBucket = reqForBucket.getTableBucket();
            notifyLeaderAndIsrDataList.add(
                    new NotifyLeaderAndIsrData(
                            toPhysicalTablePath(reqForBucket.getPhysicalTablePath()),
                            toTableBucket(pbTableBucket),
                            replicas,
                            new LeaderAndIsr(
                                    reqForBucket.getLeader(),
                                    reqForBucket.getLeaderEpoch(),
                                    isr,
                                    request.getCoordinatorEpoch(),
                                    reqForBucket.getBucketEpoch())));
        }
        return notifyLeaderAndIsrDataList;
    }

    public static NotifyLeaderAndIsrResponse makeNotifyLeaderAndIsrResponse(
            List<NotifyLeaderAndIsrResultForBucket> bucketsResult) {
        NotifyLeaderAndIsrResponse notifyLeaderAndIsrResponse = new NotifyLeaderAndIsrResponse();
        List<PbNotifyLeaderAndIsrRespForBucket> respForBuckets = new ArrayList<>();
        for (NotifyLeaderAndIsrResultForBucket bucketResult : bucketsResult) {
            PbNotifyLeaderAndIsrRespForBucket respForBucket =
                    new PbNotifyLeaderAndIsrRespForBucket();
            TableBucket tableBucket = bucketResult.getTableBucket();
            PbTableBucket pbTableBucket =
                    respForBucket
                            .setTableBucket()
                            .setTableId(tableBucket.getTableId())
                            .setBucketId(tableBucket.getBucket());
            if (tableBucket.getPartitionId() != null) {
                pbTableBucket.setPartitionId(tableBucket.getPartitionId());
            }
            if (bucketResult.failed()) {
                respForBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            }
            respForBuckets.add(respForBucket);
        }
        notifyLeaderAndIsrResponse.addAllNotifyBucketsLeaderResps(respForBuckets);
        return notifyLeaderAndIsrResponse;
    }

    public static PbStopReplicaReqForBucket makeStopBucketReplica(
            TableBucket tableBucket, boolean isDelete, int leaderEpoch) {
        PbStopReplicaReqForBucket stopBucketReplicaRequest = new PbStopReplicaReqForBucket();
        PbTableBucket pbTableBucket =
                stopBucketReplicaRequest
                        .setDelete(isDelete)
                        .setLeaderEpoch(leaderEpoch)
                        .setTableBucket()
                        .setBucketId(tableBucket.getBucket())
                        .setTableId(tableBucket.getTableId());
        if (tableBucket.getPartitionId() != null) {
            pbTableBucket.setPartitionId(tableBucket.getPartitionId());
        }
        return stopBucketReplicaRequest;
    }

    public static List<StopReplicaData> getStopReplicaData(StopReplicaRequest request) {
        List<StopReplicaData> stopReplicaDataList = new ArrayList<>();
        for (PbStopReplicaReqForBucket reqForBucket : request.getStopReplicasReqsList()) {
            PbTableBucket tableBucket = reqForBucket.getTableBucket();
            stopReplicaDataList.add(
                    new StopReplicaData(
                            toTableBucket(tableBucket),
                            reqForBucket.isDelete(),
                            request.getCoordinatorEpoch(),
                            reqForBucket.getLeaderEpoch()));
        }

        return stopReplicaDataList;
    }

    public static StopReplicaResponse makeStopReplicaResponse(
            List<StopReplicaResultForBucket> resultForBuckets) {
        StopReplicaResponse stopReplicaResponse = new StopReplicaResponse();
        List<PbStopReplicaRespForBucket> stopReplicaRespForBucketList = new ArrayList<>();
        for (StopReplicaResultForBucket bucketResult : resultForBuckets) {
            PbStopReplicaRespForBucket respForBucket = new PbStopReplicaRespForBucket();
            PbTableBucket pbTableBucket =
                    respForBucket
                            .setTableBucket()
                            .setTableId(bucketResult.getTableId())
                            .setBucketId(bucketResult.getBucketId());
            if (bucketResult.getTableBucket().getPartitionId() != null) {
                pbTableBucket.setPartitionId(bucketResult.getTableBucket().getPartitionId());
            }
            if (bucketResult.failed()) {
                respForBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            }
            stopReplicaRespForBucketList.add(respForBucket);
        }
        stopReplicaResponse.addAllStopReplicasResps(stopReplicaRespForBucketList);
        return stopReplicaResponse;
    }

    public static Map<TableBucket, MemoryLogRecords> getProduceLogData(
            ProduceLogRequest produceRequest) {
        long tableId = produceRequest.getTableId();
        Map<TableBucket, MemoryLogRecords> produceEntryData = new HashMap<>();
        for (PbProduceLogReqForBucket produceLogReqForBucket :
                produceRequest.getBucketsReqsList()) {
            ByteBuffer recordBuffer = toByteBuffer(produceLogReqForBucket.getRecordsSlice());
            MemoryLogRecords logRecords = MemoryLogRecords.pointToByteBuffer(recordBuffer);
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            produceLogReqForBucket.hasPartitionId()
                                    ? produceLogReqForBucket.getPartitionId()
                                    : null,
                            produceLogReqForBucket.getBucketId());
            produceEntryData.put(tb, logRecords);
        }
        return produceEntryData;
    }

    public static ProduceLogResponse makeProduceLogResponse(
            List<ProduceLogResultForBucket> appendLogResultForBucketList) {
        ProduceLogResponse produceResponse = new ProduceLogResponse();
        List<PbProduceLogRespForBucket> produceLogRespForBucketList = new ArrayList<>();
        for (ProduceLogResultForBucket bucketResult : appendLogResultForBucketList) {
            PbProduceLogRespForBucket producedBucket =
                    new PbProduceLogRespForBucket().setBucketId(bucketResult.getBucketId());
            TableBucket tableBucket = bucketResult.getTableBucket();
            if (tableBucket.getPartitionId() != null) {
                producedBucket.setPartitionId(tableBucket.getPartitionId());
            }

            if (bucketResult.failed()) {
                producedBucket.setError(
                        bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                producedBucket.setBaseOffset(bucketResult.getBaseOffset());
            }
            produceLogRespForBucketList.add(producedBucket);
        }
        produceResponse.addAllBucketsResps(produceLogRespForBucketList);
        return produceResponse;
    }

    public static Map<TableBucket, FetchData> getFetchLogData(FetchLogRequest request) {
        Map<TableBucket, FetchData> fetchDataMap = new HashMap<>();
        for (PbFetchLogReqForTable fetchLogReqForTable : request.getTablesReqsList()) {
            long tableId = fetchLogReqForTable.getTableId();
            final int[] projectionFields;
            if (fetchLogReqForTable.isProjectionPushdownEnabled()) {
                projectionFields = fetchLogReqForTable.getProjectedFields();
            } else {
                projectionFields = null;
            }

            List<PbFetchLogReqForBucket> bucketsReqsList = fetchLogReqForTable.getBucketsReqsList();
            for (PbFetchLogReqForBucket fetchLogReqForBucket : bucketsReqsList) {
                int bucketId = fetchLogReqForBucket.getBucketId();
                fetchDataMap.put(
                        new TableBucket(
                                tableId,
                                fetchLogReqForBucket.hasPartitionId()
                                        ? fetchLogReqForBucket.getPartitionId()
                                        : null,
                                bucketId),
                        new FetchData(
                                tableId,
                                fetchLogReqForBucket.getFetchOffset(),
                                fetchLogReqForBucket.getMaxFetchBytes(),
                                projectionFields));
            }
        }

        return fetchDataMap;
    }

    public static FetchLogResponse makeFetchLogResponse(
            Map<TableBucket, FetchLogResultForBucket> fetchLogResult) {
        Map<Long, List<PbFetchLogRespForBucket>> fetchLogRespMap = new HashMap<>();
        for (Map.Entry<TableBucket, FetchLogResultForBucket> entry : fetchLogResult.entrySet()) {
            TableBucket tb = entry.getKey();
            FetchLogResultForBucket bucketResult = entry.getValue();
            PbFetchLogRespForBucket fetchLogRespForBucket =
                    new PbFetchLogRespForBucket().setBucketId(tb.getBucket());
            if (tb.getPartitionId() != null) {
                fetchLogRespForBucket.setPartitionId(tb.getPartitionId());
            }
            if (bucketResult.failed()) {
                fetchLogRespForBucket.setError(
                        bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                fetchLogRespForBucket
                        .setHighWatermark(bucketResult.getHighWatermark())
                        // TODO: set log start offset here if we support log clean.
                        .setLogStartOffset(0L);

                if (bucketResult.fetchFromRemote()) {
                    // set remote log fetch info.
                    RemoteLogFetchInfo rlfInfo = bucketResult.remoteLogFetchInfo();
                    checkNotNull(rlfInfo, "Remote log fetch info is null.");
                    List<PbRemoteLogSegment> remoteLogSegmentList = new ArrayList<>();
                    for (RemoteLogSegment logSegment : rlfInfo.remoteLogSegmentList()) {
                        PbRemoteLogSegment pbRemoteLogSegment =
                                new PbRemoteLogSegment()
                                        .setRemoteLogStartOffset(logSegment.remoteLogStartOffset())
                                        .setRemoteLogSegmentId(
                                                logSegment.remoteLogSegmentId().toString())
                                        .setRemoteLogEndOffset(logSegment.remoteLogEndOffset())
                                        .setSegmentSizeInBytes(logSegment.segmentSizeInBytes());
                        remoteLogSegmentList.add(pbRemoteLogSegment);
                    }
                    fetchLogRespForBucket
                            .setRemoteLogFetchInfo()
                            .setRemoteLogTabletDir(rlfInfo.remoteLogTabletDir())
                            .addAllRemoteLogSegments(remoteLogSegmentList)
                            .setFirstStartPos(rlfInfo.firstStartPos());
                    if (rlfInfo.partitionName() != null) {
                        fetchLogRespForBucket
                                .setRemoteLogFetchInfo()
                                .setPartitionName(rlfInfo.partitionName());
                    }
                } else {
                    // set records
                    LogRecords records = bucketResult.recordsOrEmpty();
                    if (records instanceof FileLogRecords) {
                        FileChannelChunk chunk = ((FileLogRecords) records).toChunk();
                        // zero-copy optimization for file channel
                        fetchLogRespForBucket.setRecords(
                                chunk.getFileChannel(), chunk.getPosition(), chunk.getSize());
                    } else if (records instanceof MemoryLogRecords) {
                        // this should never happen, but we still support fetch memory log records.
                        if (records == MemoryLogRecords.EMPTY) {
                            fetchLogRespForBucket.setRecords(new byte[0]);
                        } else {
                            MemoryLogRecords logRecords = (MemoryLogRecords) records;
                            fetchLogRespForBucket.setRecords(
                                    logRecords.getMemorySegment(),
                                    logRecords.getPosition(),
                                    logRecords.sizeInBytes());
                        }
                    } else if (records instanceof BytesViewLogRecords) {
                        // zero-copy for project push down.
                        fetchLogRespForBucket.setRecordsBytesView(
                                ((BytesViewLogRecords) records).getBytesView());
                    } else {
                        throw new UnsupportedOperationException(
                                "Not supported log records type: " + records.getClass().getName());
                    }
                }
            }
            if (fetchLogRespMap.containsKey(tb.getTableId())) {
                fetchLogRespMap.get(tb.getTableId()).add(fetchLogRespForBucket);
            } else {
                List<PbFetchLogRespForBucket> fetchLogRespForBuckets = new ArrayList<>();
                fetchLogRespForBuckets.add(fetchLogRespForBucket);
                fetchLogRespMap.put(tb.getTableId(), fetchLogRespForBuckets);
            }
        }

        List<PbFetchLogRespForTable> fetchLogRespForTables = new ArrayList<>();
        for (Map.Entry<Long, List<PbFetchLogRespForBucket>> entry : fetchLogRespMap.entrySet()) {
            PbFetchLogRespForTable fetchLogRespForTable = new PbFetchLogRespForTable();
            fetchLogRespForTable.setTableId(entry.getKey());
            fetchLogRespForTable.addAllBucketsResps(entry.getValue());
            fetchLogRespForTables.add(fetchLogRespForTable);
        }

        FetchLogResponse fetchLogResponse = new FetchLogResponse();
        fetchLogResponse.addAllTablesResps(fetchLogRespForTables);
        return fetchLogResponse;
    }

    public static Map<TableBucket, FetchLogResultForBucket> getFetchLogResult(
            FetchLogResponse fetchLogResponse) {
        Map<TableBucket, FetchLogResultForBucket> fetchLogResultMap = new HashMap<>();
        List<PbFetchLogRespForTable> tablesRespList = fetchLogResponse.getTablesRespsList();
        for (PbFetchLogRespForTable tableResp : tablesRespList) {
            long tableId = tableResp.getTableId();
            List<PbFetchLogRespForBucket> bucketsRespList = tableResp.getBucketsRespsList();
            for (PbFetchLogRespForBucket bucketResp : bucketsRespList) {
                TableBucket tableBucket =
                        new TableBucket(
                                tableId,
                                bucketResp.hasPartitionId() ? bucketResp.getPartitionId() : null,
                                bucketResp.getBucketId());
                if (bucketResp.hasErrorCode()) {
                    fetchLogResultMap.put(
                            tableBucket,
                            new FetchLogResultForBucket(
                                    tableBucket, ApiError.fromErrorMessage(bucketResp)));
                } else {
                    ByteBuffer recordsBuffer = toByteBuffer(bucketResp.getRecordsSlice());
                    MemoryLogRecords logRecords = MemoryLogRecords.pointToByteBuffer(recordsBuffer);
                    fetchLogResultMap.put(
                            tableBucket,
                            new FetchLogResultForBucket(
                                    tableBucket, logRecords, bucketResp.getHighWatermark()));
                }
            }
        }

        return fetchLogResultMap;
    }

    public static Map<TableBucket, KvRecordBatch> getPutKvData(PutKvRequest putKvRequest) {
        long tableId = putKvRequest.getTableId();
        Map<TableBucket, KvRecordBatch> produceEntryData = new HashMap<>();
        for (PbPutKvReqForBucket putKvReqForBucket : putKvRequest.getBucketsReqsList()) {
            ByteBuffer recordsBuffer = toByteBuffer(putKvReqForBucket.getRecordsSlice());
            DefaultKvRecordBatch kvRecords = DefaultKvRecordBatch.pointToByteBuffer(recordsBuffer);
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            putKvReqForBucket.hasPartitionId()
                                    ? putKvReqForBucket.getPartitionId()
                                    : null,
                            putKvReqForBucket.getBucketId());
            produceEntryData.put(tb, kvRecords);
        }
        return produceEntryData;
    }

    public static Map<TableBucket, List<byte[]>> toLookupData(LookupRequest lookupRequest) {
        long tableId = lookupRequest.getTableId();
        Map<TableBucket, List<byte[]>> lookupEntryData = new HashMap<>();
        for (PbLookupReqForBucket lookupReqForBucket : lookupRequest.getBucketsReqsList()) {
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            lookupReqForBucket.hasPartitionId()
                                    ? lookupReqForBucket.getPartitionId()
                                    : null,
                            lookupReqForBucket.getBucketId());
            List<byte[]> keys = new ArrayList<>(lookupReqForBucket.getKeysCount());
            for (int i = 0; i < lookupReqForBucket.getKeysCount(); i++) {
                keys.add(lookupReqForBucket.getKeyAt(i));
            }
            lookupEntryData.put(tb, keys);
        }
        return lookupEntryData;
    }

    public static Map<TableBucket, List<byte[]>> toPrefixLookupData(
            PrefixLookupRequest prefixLookupRequest) {
        long tableId = prefixLookupRequest.getTableId();
        Map<TableBucket, List<byte[]>> lookupEntryData = new HashMap<>();
        for (PbPrefixLookupReqForBucket lookupReqForBucket :
                prefixLookupRequest.getBucketsReqsList()) {
            TableBucket tb =
                    new TableBucket(
                            tableId,
                            lookupReqForBucket.hasPartitionId()
                                    ? lookupReqForBucket.getPartitionId()
                                    : null,
                            lookupReqForBucket.getBucketId());
            List<byte[]> keys = new ArrayList<>(lookupReqForBucket.getKeysCount());
            for (int i = 0; i < lookupReqForBucket.getKeysCount(); i++) {
                keys.add(lookupReqForBucket.getKeyAt(i));
            }
            lookupEntryData.put(tb, keys);
        }
        return lookupEntryData;
    }

    public static @Nullable int[] getTargetColumns(PutKvRequest putKvRequest) {
        int[] targetColumns = putKvRequest.getTargetColumns();
        return targetColumns.length == 0 ? null : targetColumns;
    }

    public static PutKvResponse makePutKvResponse(List<PutKvResultForBucket> kvPutResult) {
        PutKvResponse putKvResponse = new PutKvResponse();
        List<PbPutKvRespForBucket> putKvRespForBucketList = new ArrayList<>();
        for (PutKvResultForBucket bucketResult : kvPutResult) {
            PbPutKvRespForBucket putKvBucket =
                    new PbPutKvRespForBucket().setBucketId(bucketResult.getBucketId());
            TableBucket tableBucket = bucketResult.getTableBucket();
            if (tableBucket.getPartitionId() != null) {
                putKvBucket.setPartitionId(tableBucket.getPartitionId());
            }

            if (bucketResult.failed()) {
                putKvBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            }
            putKvRespForBucketList.add(putKvBucket);
        }
        putKvResponse.addAllBucketsResps(putKvRespForBucketList);
        return putKvResponse;
    }

    public static LimitScanResponse makeLimitScanResponse(LimitScanResultForBucket bucketResult) {
        LimitScanResponse limitScanResponse = new LimitScanResponse();

        if (bucketResult.failed()) {
            limitScanResponse.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
        } else {

            Boolean isLogTable = bucketResult.isLogTable();
            if (isLogTable != null) {
                limitScanResponse.setIsLogTable(isLogTable);
            }

            DefaultValueRecordBatch valueRecords = bucketResult.getValues();
            if (valueRecords != null) {
                limitScanResponse.setRecords(
                        valueRecords.getSegment(),
                        valueRecords.getPosition(),
                        valueRecords.sizeInBytes());
            }

            LogRecords logRecords = bucketResult.getRecords();
            if (logRecords != null) {
                // TODO: code below is duplicated with FetchLogResponse, we should refactor it.
                if (logRecords instanceof FileLogRecords) {
                    FileChannelChunk chunk = ((FileLogRecords) logRecords).toChunk();
                    // zero-copy optimization for file channel
                    limitScanResponse.setRecords(
                            chunk.getFileChannel(), chunk.getPosition(), chunk.getSize());
                } else if (logRecords instanceof MemoryLogRecords) {
                    // this should never happen, but we still support fetch memory log records.
                    if (logRecords == MemoryLogRecords.EMPTY) {
                        limitScanResponse.setRecords(new byte[0]);
                    } else {
                        MemoryLogRecords records = (MemoryLogRecords) logRecords;
                        limitScanResponse.setRecords(
                                records.getMemorySegment(),
                                records.getPosition(),
                                records.sizeInBytes());
                    }
                } else if (logRecords instanceof BytesViewLogRecords) {
                    // zero-copy for project push down.
                    limitScanResponse.setRecordsBytesView(
                            ((BytesViewLogRecords) logRecords).getBytesView());
                } else {
                    throw new UnsupportedOperationException(
                            "Not supported log records type: " + logRecords.getClass().getName());
                }
            }
        }

        return limitScanResponse;
    }

    public static LookupResponse makeLookupResponse(
            Map<TableBucket, LookupResultForBucket> lookupResult) {
        LookupResponse lookupResponse = new LookupResponse();
        for (Map.Entry<TableBucket, LookupResultForBucket> entry : lookupResult.entrySet()) {
            TableBucket tb = entry.getKey();
            LookupResultForBucket bucketResult = entry.getValue();
            PbLookupRespForBucket lookupRespForBucket = lookupResponse.addBucketsResp();
            lookupRespForBucket.setBucketId(tb.getBucket());
            if (tb.getPartitionId() != null) {
                lookupRespForBucket.setPartitionId(tb.getPartitionId());
            }
            if (bucketResult.failed()) {
                lookupRespForBucket.setError(
                        bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                for (byte[] value : bucketResult.lookupValues()) {
                    PbValue pbValue = lookupRespForBucket.addValue();
                    if (value != null) {
                        pbValue.setValues(value);
                    }
                }
            }
        }
        return lookupResponse;
    }

    public static PrefixLookupResponse makePrefixLookupResponse(
            Map<TableBucket, PrefixLookupResultForBucket> prefixLookupResult) {
        PrefixLookupResponse prefixLookupResponse = new PrefixLookupResponse();
        List<PbPrefixLookupRespForBucket> resultForAll = new ArrayList<>();
        for (Map.Entry<TableBucket, PrefixLookupResultForBucket> entry :
                prefixLookupResult.entrySet()) {
            PbPrefixLookupRespForBucket respForBucket = new PbPrefixLookupRespForBucket();
            TableBucket tb = entry.getKey();
            respForBucket.setBucketId(tb.getBucket());
            if (tb.getPartitionId() != null) {
                respForBucket.setPartitionId(tb.getPartitionId());
            }

            PrefixLookupResultForBucket bucketResult = entry.getValue();
            if (bucketResult.failed()) {
                respForBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                List<PbValueList> keyResultList = new ArrayList<>();
                for (List<byte[]> res : bucketResult.prefixLookupValues()) {
                    PbValueList pbValueList = new PbValueList();
                    for (byte[] bytes : res) {
                        pbValueList.addValue(bytes);
                    }
                    keyResultList.add(pbValueList);
                }
                respForBucket.addAllValueLists(keyResultList);
            }
            resultForAll.add(respForBucket);
        }
        prefixLookupResponse.addAllBucketsResps(resultForAll);
        return prefixLookupResponse;
    }

    public static AdjustIsrRequest makeAdjustIsrRequest(
            int serverId, Map<TableBucket, LeaderAndIsr> leaderAndIsrMap) {
        // group by table id.
        Map<Long, List<PbAdjustIsrReqForBucket>> reqForBucketByTableId = new HashMap<>();
        leaderAndIsrMap.forEach(
                ((tb, leaderAndIsr) -> {
                    PbAdjustIsrReqForBucket reqForBucket =
                            new PbAdjustIsrReqForBucket()
                                    .setBucketId(tb.getBucket())
                                    .setBucketEpoch(leaderAndIsr.bucketEpoch())
                                    .setCoordinatorEpoch(leaderAndIsr.coordinatorEpoch())
                                    .setLeaderEpoch(leaderAndIsr.leaderEpoch());
                    if (tb.getPartitionId() != null) {
                        reqForBucket.setPartitionId(tb.getPartitionId());
                    }
                    leaderAndIsr.isr().forEach(reqForBucket::addNewIsr);
                    if (reqForBucketByTableId.containsKey(tb.getTableId())) {
                        reqForBucketByTableId.get(tb.getTableId()).add(reqForBucket);
                    } else {
                        List<PbAdjustIsrReqForBucket> list = new ArrayList<>();
                        list.add(reqForBucket);
                        reqForBucketByTableId.put(tb.getTableId(), list);
                    }
                }));

        // make request.
        AdjustIsrRequest adjustIsrRequest = new AdjustIsrRequest().setServerId(serverId);
        List<PbAdjustIsrReqForTable> tablesReqs = new ArrayList<>();
        for (Map.Entry<Long, List<PbAdjustIsrReqForBucket>> entry :
                reqForBucketByTableId.entrySet()) {
            PbAdjustIsrReqForTable reqForTable =
                    new PbAdjustIsrReqForTable().setTableId(entry.getKey());
            reqForTable.addAllBucketsReqs(entry.getValue());
            tablesReqs.add(reqForTable);
        }
        adjustIsrRequest.addAllTablesReqs(tablesReqs);
        return adjustIsrRequest;
    }

    public static Map<TableBucket, LeaderAndIsr> getAdjustIsrData(AdjustIsrRequest request) {
        int leaderId = request.getServerId();
        Map<TableBucket, LeaderAndIsr> leaderAndIsrMap = new HashMap<>();
        for (PbAdjustIsrReqForTable reqForTable : request.getTablesReqsList()) {
            long tableId = reqForTable.getTableId();
            List<PbAdjustIsrReqForBucket> bucketsReqsList = reqForTable.getBucketsReqsList();
            for (PbAdjustIsrReqForBucket reqForBucket : bucketsReqsList) {
                TableBucket tb =
                        new TableBucket(
                                tableId,
                                reqForBucket.hasPartitionId()
                                        ? reqForBucket.getPartitionId()
                                        : null,
                                reqForBucket.getBucketId());
                List<Integer> newIsr = new ArrayList<>();
                for (int i = 0; i < reqForBucket.getNewIsrsCount(); i++) {
                    newIsr.add(reqForBucket.getNewIsrAt(i));
                }
                leaderAndIsrMap.put(
                        tb,
                        new LeaderAndIsr(
                                leaderId,
                                reqForBucket.getLeaderEpoch(),
                                newIsr,
                                reqForBucket.getCoordinatorEpoch(),
                                reqForBucket.getBucketEpoch()));
            }
        }
        return leaderAndIsrMap;
    }

    public static AdjustIsrResponse makeAdjustIsrResponse(
            List<AdjustIsrResultForBucket> resultForBuckets) {
        Map<Long, List<PbAdjustIsrRespForBucket>> respMap = new HashMap<>();
        for (AdjustIsrResultForBucket bucketResult : resultForBuckets) {
            TableBucket tb = bucketResult.getTableBucket();
            PbAdjustIsrRespForBucket respForBucket =
                    new PbAdjustIsrRespForBucket().setBucketId(tb.getBucket());
            if (tb.getPartitionId() != null) {
                respForBucket.setPartitionId(tb.getPartitionId());
            }
            if (bucketResult.failed()) {
                respForBucket.setError(bucketResult.getErrorCode(), bucketResult.getErrorMessage());
            } else {
                LeaderAndIsr leaderAndIsr = bucketResult.leaderAndIsr();
                respForBucket
                        .setLeaderId(leaderAndIsr.leader())
                        .setLeaderEpoch(leaderAndIsr.leaderEpoch())
                        .setCoordinatorEpoch(leaderAndIsr.coordinatorEpoch())
                        .setBucketEpoch(leaderAndIsr.bucketEpoch())
                        .setIsrs(leaderAndIsr.isrArray());
            }

            if (respMap.containsKey(tb.getTableId())) {
                respMap.get(tb.getTableId()).add(respForBucket);
            } else {
                List<PbAdjustIsrRespForBucket> list = new ArrayList<>();
                list.add(respForBucket);
                respMap.put(tb.getTableId(), list);
            }
        }

        AdjustIsrResponse adjustIsrResponse = new AdjustIsrResponse();
        List<PbAdjustIsrRespForTable> respForTables = new ArrayList<>();
        for (Map.Entry<Long, List<PbAdjustIsrRespForBucket>> entry : respMap.entrySet()) {
            PbAdjustIsrRespForTable respForTable = new PbAdjustIsrRespForTable();
            respForTable.setTableId(entry.getKey());
            respForTable.addAllBucketsResps(entry.getValue());
            respForTables.add(respForTable);
        }
        adjustIsrResponse.addAllTablesResps(respForTables);
        return adjustIsrResponse;
    }

    public static Map<TableBucket, AdjustIsrResultForBucket> getAdjustIsrResponseData(
            AdjustIsrResponse response) {
        Map<TableBucket, AdjustIsrResultForBucket> adjustIsrResult = new HashMap<>();
        for (PbAdjustIsrRespForTable respForTable : response.getTablesRespsList()) {
            long tableId = respForTable.getTableId();
            for (PbAdjustIsrRespForBucket respForBucket : respForTable.getBucketsRespsList()) {
                TableBucket tb =
                        new TableBucket(
                                tableId,
                                respForBucket.hasPartitionId()
                                        ? respForBucket.getPartitionId()
                                        : null,
                                respForBucket.getBucketId());
                if (respForBucket.hasErrorCode()) {
                    adjustIsrResult.put(
                            tb,
                            new AdjustIsrResultForBucket(
                                    tb, ApiError.fromErrorMessage(respForBucket)));
                    continue;
                }

                List<Integer> isr = new ArrayList<>();
                for (int i = 0; i < respForBucket.getIsrsCount(); i++) {
                    isr.add(respForBucket.getIsrAt(i));
                }
                adjustIsrResult.put(
                        tb,
                        new AdjustIsrResultForBucket(
                                tb,
                                new LeaderAndIsr(
                                        respForBucket.getLeaderId(),
                                        respForBucket.getLeaderEpoch(),
                                        isr,
                                        respForBucket.getCoordinatorEpoch(),
                                        respForBucket.getBucketEpoch())));
            }
        }
        return adjustIsrResult;
    }

    public static Set<TableBucket> getListOffsetsData(ListOffsetsRequest request) {
        Set<TableBucket> tableBuckets = new HashSet<>();
        long tableId = request.getTableId();
        Long partitionId = request.hasPartitionId() ? request.getPartitionId() : null;
        for (int i = 0; i < request.getBucketIdsCount(); i++) {
            tableBuckets.add(new TableBucket(tableId, partitionId, request.getBucketIdAt(i)));
        }
        return tableBuckets;
    }

    public static ListOffsetsResponse makeListOffsetsResponse(
            List<ListOffsetsResultForBucket> results) {
        ListOffsetsResponse listOffsetsResponse = new ListOffsetsResponse();
        List<PbListOffsetsRespForBucket> respForBucketList = new ArrayList<>();
        for (ListOffsetsResultForBucket result : results) {
            PbListOffsetsRespForBucket respForBucket =
                    new PbListOffsetsRespForBucket().setBucketId(result.getBucketId());
            if (result.failed()) {
                respForBucket.setError(result.getErrorCode(), result.getErrorMessage());
            } else {
                respForBucket.setOffset(result.getOffset());
            }
            respForBucketList.add(respForBucket);
        }
        listOffsetsResponse.addAllBucketsResps(respForBucketList);
        return listOffsetsResponse;
    }

    public static ListOffsetsRequest makeListOffsetsRequest(
            int followerServerId,
            int offsetType,
            long tableId,
            @Nullable Long partitionId,
            int bucketId) {
        ListOffsetsRequest listOffsetsRequest = new ListOffsetsRequest();
        listOffsetsRequest
                .setFollowerServerId(followerServerId)
                .setOffsetType(offsetType)
                .setTableId(tableId)
                .setBucketIds(new int[] {bucketId});
        if (partitionId != null) {
            listOffsetsRequest.setPartitionId(partitionId);
        }
        return listOffsetsRequest;
    }

    public static CommitKvSnapshotRequest makeCommitKvSnapshotRequest(
            CompletedSnapshot completedSnapshot, int coordinatorEpoch, int bucketLeaderEpoch) {
        CommitKvSnapshotRequest request = new CommitKvSnapshotRequest();
        byte[] completedSnapshotBytes = CompletedSnapshotJsonSerde.toJson(completedSnapshot);
        request.setCompletedSnapshot(completedSnapshotBytes)
                .setCoordinatorEpoch(coordinatorEpoch)
                .setBucketLeaderEpoch(bucketLeaderEpoch);
        return request;
    }

    public static GetLatestKvSnapshotsResponse makeGetLatestKvSnapshotsResponse(
            long tableId,
            @Nullable Long partitionId,
            Map<Integer, Optional<BucketSnapshot>> latestSnapshots,
            int numBuckets) {
        List<PbKvSnapshot> pbSnapshots = makePbKvSnapshots(latestSnapshots, numBuckets);
        GetLatestKvSnapshotsResponse response =
                new GetLatestKvSnapshotsResponse()
                        .setTableId(tableId)
                        .addAllLatestSnapshots(pbSnapshots);
        if (partitionId != null) {
            response.setPartitionId(partitionId);
        }
        return response;
    }

    private static List<PbKvSnapshot> makePbKvSnapshots(
            Map<Integer, Optional<BucketSnapshot>> snapshots, int numBuckets) {
        List<PbKvSnapshot> result = new ArrayList<>();
        for (int bucket = 0; bucket < numBuckets; bucket++) {
            Optional<BucketSnapshot> snapshot = snapshots.get(bucket);
            PbKvSnapshot pbKvSnapshot = new PbKvSnapshot().setBucketId(bucket);
            if (snapshot != null && snapshot.isPresent()) {
                pbKvSnapshot
                        .setSnapshotId(snapshot.get().getSnapshotId())
                        .setLogOffset(snapshot.get().getLogOffset());
            }
            result.add(pbKvSnapshot);
        }
        return result;
    }

    public static GetKvSnapshotMetadataResponse makeKvSnapshotMetadataResponse(
            CompletedSnapshot completedSnapshot) {
        return new GetKvSnapshotMetadataResponse()
                .setLogOffset(completedSnapshot.getLogOffset())
                .addAllSnapshotFiles(toPbSnapshotFileHandles(completedSnapshot));
    }

    public static InitWriterResponse makeInitWriterResponse(long writerId) {
        return new InitWriterResponse().setWriterId(writerId);
    }

    private static List<PbRemotePathAndLocalFile> toPbSnapshotFileHandles(
            CompletedSnapshot completedSnapshot) {
        KvSnapshotHandle kvSnapshotHandle = completedSnapshot.getKvSnapshotHandle();
        return Stream.concat(
                        kvSnapshotHandle.getPrivateFileHandles().stream(),
                        kvSnapshotHandle.getSharedKvFileHandles().stream())
                .map(
                        kvFileHandleAndLocalPath -> {
                            // get the remote file path
                            String filePath =
                                    kvFileHandleAndLocalPath
                                            .getKvFileHandle()
                                            .getFilePath()
                                            .toString();
                            // get the local name for the file
                            String localPath = kvFileHandleAndLocalPath.getLocalPath();
                            return new PbRemotePathAndLocalFile()
                                    .setRemotePath(filePath)
                                    .setLocalFileName(localPath);
                        })
                .collect(Collectors.toList());
    }

    public static GetFileSystemSecurityTokenResponse toGetFileSystemSecurityTokenResponse(
            String filesystemSchema, ObtainedSecurityToken obtainedSecurityToken) {
        GetFileSystemSecurityTokenResponse getFileSystemSecurityTokenResponse =
                new GetFileSystemSecurityTokenResponse()
                        .setToken(obtainedSecurityToken.getToken())
                        .setSchema(filesystemSchema);

        obtainedSecurityToken
                .getValidUntil()
                .ifPresent(getFileSystemSecurityTokenResponse::setExpirationTime);

        List<PbKeyValue> pbKeyValues =
                new ArrayList<>(obtainedSecurityToken.getAdditionInfos().size());
        for (Map.Entry<String, String> entry :
                obtainedSecurityToken.getAdditionInfos().entrySet()) {
            pbKeyValues.add(new PbKeyValue().setKey(entry.getKey()).setValue(entry.getValue()));
        }
        getFileSystemSecurityTokenResponse.addAllAdditionInfos(pbKeyValues);
        return getFileSystemSecurityTokenResponse;
    }

    public static CommitRemoteLogManifestData getCommitRemoteLogManifestData(
            CommitRemoteLogManifestRequest request) {
        return new CommitRemoteLogManifestData(
                new TableBucket(
                        request.getTableId(),
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId()),
                new FsPath(request.getRemoteLogManifestPath()),
                request.getRemoteLogStartOffset(),
                request.getRemoteLogEndOffset(),
                request.getCoordinatorEpoch(),
                request.getBucketLeaderEpoch());
    }

    public static CommitRemoteLogManifestRequest makeCommitRemoteLogManifestRequest(
            CommitRemoteLogManifestData commitRemoteLogManifestData) {
        CommitRemoteLogManifestRequest request = new CommitRemoteLogManifestRequest();
        TableBucket tb = commitRemoteLogManifestData.getTableBucket();
        if (tb.getPartitionId() != null) {
            request.setPartitionId(tb.getPartitionId());
        }
        request.setTableId(tb.getTableId())
                .setBucketId(tb.getBucket())
                .setRemoteLogManifestPath(
                        commitRemoteLogManifestData.getRemoteLogManifestPath().toString())
                .setRemoteLogStartOffset(commitRemoteLogManifestData.getRemoteLogStartOffset())
                .setRemoteLogEndOffset(commitRemoteLogManifestData.getRemoteLogEndOffset())
                .setCoordinatorEpoch(commitRemoteLogManifestData.getCoordinatorEpoch())
                .setBucketLeaderEpoch(commitRemoteLogManifestData.getBucketLeaderEpoch());
        return request;
    }

    public static NotifyRemoteLogOffsetsRequest makeNotifyRemoteLogOffsetsRequest(
            TableBucket tableBucket, long remoteLogStartOffset, long remoteLogEndOffset) {
        NotifyRemoteLogOffsetsRequest request = new NotifyRemoteLogOffsetsRequest();
        if (tableBucket.getPartitionId() != null) {
            request.setPartitionId(tableBucket.getPartitionId());
        }
        request.setTableId(tableBucket.getTableId())
                .setBucketId(tableBucket.getBucket())
                .setRemoteStartOffset(remoteLogStartOffset)
                .setRemoteEndOffset(remoteLogEndOffset);
        return request;
    }

    public static NotifyRemoteLogOffsetsData getNotifyRemoteLogOffsetsData(
            NotifyRemoteLogOffsetsRequest request) {
        return new NotifyRemoteLogOffsetsData(
                new TableBucket(
                        request.getTableId(),
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId()),
                request.getRemoteStartOffset(),
                request.getRemoteEndOffset(),
                request.getCoordinatorEpoch());
    }

    public static NotifyKvSnapshotOffsetData getNotifySnapshotOffsetData(
            NotifyKvSnapshotOffsetRequest request) {
        return new NotifyKvSnapshotOffsetData(
                new TableBucket(
                        request.getTableId(),
                        request.hasPartitionId() ? request.getPartitionId() : null,
                        request.getBucketId()),
                request.getMinRetainOffset(),
                request.getCoordinatorEpoch());
    }

    public static NotifyKvSnapshotOffsetRequest makeNotifyKvSnapshotOffsetRequest(
            TableBucket tableBucket, long minRetainOffset) {
        NotifyKvSnapshotOffsetRequest request = new NotifyKvSnapshotOffsetRequest();
        if (tableBucket.getPartitionId() != null) {
            request.setPartitionId(tableBucket.getPartitionId());
        }
        request.setTableId(tableBucket.getTableId())
                .setBucketId(tableBucket.getBucket())
                .setMinRetainOffset(minRetainOffset);
        return request;
    }

    public static ListPartitionInfosResponse toListPartitionInfosResponse(
            List<String> partitionKeys, Map<String, Long> partitionNameAndIds) {
        ListPartitionInfosResponse listPartitionsResponse = new ListPartitionInfosResponse();
        for (Map.Entry<String, Long> partitionNameAndId : partitionNameAndIds.entrySet()) {
            ResolvedPartitionSpec spec =
                    ResolvedPartitionSpec.fromPartitionName(
                            partitionKeys, partitionNameAndId.getKey());
            listPartitionsResponse
                    .addPartitionsInfo()
                    .setPartitionId(partitionNameAndId.getValue())
                    .setPartitionSpec(makePbPartitionSpec(spec));
        }
        return listPartitionsResponse;
    }

    public static PbPartitionSpec makePbPartitionSpec(ResolvedPartitionSpec spec) {
        PbPartitionSpec pbPartitionSpec = new PbPartitionSpec();
        for (int i = 0; i < spec.getPartitionKeys().size(); i++) {
            pbPartitionSpec
                    .addPartitionKeyValue()
                    .setKey(spec.getPartitionKeys().get(i))
                    .setValue(spec.getPartitionValues().get(i));
        }
        return pbPartitionSpec;
    }

    public static CommitLakeTableSnapshotData getCommitLakeTableSnapshotData(
            CommitLakeTableSnapshotRequest request) {
        Map<Long, LakeTableSnapshot> lakeTableInfoByTableId = new HashMap<>();
        for (PbLakeTableSnapshotInfo pdLakeTableSnapshotInfo : request.getTablesReqsList()) {
            long tableId = pdLakeTableSnapshotInfo.getTableId();
            long snapshotId = pdLakeTableSnapshotInfo.getSnapshotId();
            Map<TableBucket, Long> bucketLogStartOffset = new HashMap<>();
            Map<TableBucket, Long> bucketLogEndOffset = new HashMap<>();

            for (PbLakeTableOffsetForBucket lakeTableOffsetForBucket :
                    pdLakeTableSnapshotInfo.getBucketsReqsList()) {
                Long partitionId =
                        lakeTableOffsetForBucket.hasPartitionId()
                                ? lakeTableOffsetForBucket.getPartitionId()
                                : null;
                int bucketId = lakeTableOffsetForBucket.getBucketId();

                TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
                Long logStartOffset =
                        lakeTableOffsetForBucket.hasLogStartOffset()
                                ? lakeTableOffsetForBucket.getLogStartOffset()
                                : null;
                Long logEndOffset =
                        lakeTableOffsetForBucket.hasLogEndOffset()
                                ? lakeTableOffsetForBucket.getLogEndOffset()
                                : null;
                bucketLogStartOffset.put(tableBucket, logStartOffset);
                bucketLogEndOffset.put(tableBucket, logEndOffset);
            }
            lakeTableInfoByTableId.put(
                    tableId,
                    new LakeTableSnapshot(
                            snapshotId, tableId, bucketLogStartOffset, bucketLogEndOffset));
        }
        return new CommitLakeTableSnapshotData(lakeTableInfoByTableId);
    }

    public static PbNotifyLakeTableOffsetReqForBucket makeNotifyLakeTableOffsetForBucket(
            TableBucket tableBucket, LakeTableSnapshot lakeTableSnapshot) {
        PbNotifyLakeTableOffsetReqForBucket reqForBucket =
                new PbNotifyLakeTableOffsetReqForBucket();
        if (tableBucket.getPartitionId() != null) {
            reqForBucket.setPartitionId(tableBucket.getPartitionId());
        }
        reqForBucket
                .setTableId(tableBucket.getTableId())
                .setBucketId(tableBucket.getBucket())
                .setSnapshotId(lakeTableSnapshot.getSnapshotId());

        lakeTableSnapshot.getLogStartOffset(tableBucket).ifPresent(reqForBucket::setLogStartOffset);

        lakeTableSnapshot.getLogEndOffset(tableBucket).ifPresent(reqForBucket::setLogEndOffset);

        return reqForBucket;
    }

    public static NotifyLakeTableOffsetData getNotifyLakeTableOffset(
            NotifyLakeTableOffsetRequest notifyLakeTableOffsetRequest) {
        Map<TableBucket, LakeBucketOffset> lakeBucketOffsetMap = new HashMap<>();
        for (PbNotifyLakeTableOffsetReqForBucket reqForBucket :
                notifyLakeTableOffsetRequest.getNotifyBucketsReqsList()) {
            long tableId = reqForBucket.getTableId();
            Long partitionId = reqForBucket.hasPartitionId() ? reqForBucket.getPartitionId() : null;
            int bucket = reqForBucket.getBucketId();

            TableBucket tableBucket = new TableBucket(tableId, partitionId, bucket);

            long snapshotId = reqForBucket.getSnapshotId();
            Long logStartOffset =
                    reqForBucket.hasLogStartOffset() ? reqForBucket.getLogStartOffset() : null;
            Long logEndOffset =
                    reqForBucket.hasLogEndOffset() ? reqForBucket.getLogEndOffset() : null;
            lakeBucketOffsetMap.put(
                    tableBucket, new LakeBucketOffset(snapshotId, logStartOffset, logEndOffset));
        }

        return new NotifyLakeTableOffsetData(
                notifyLakeTableOffsetRequest.getCoordinatorEpoch(), lakeBucketOffsetMap);
    }

    public static GetLatestLakeSnapshotResponse makeGetLatestLakeSnapshotResponse(
            long tableId, LakeTableSnapshot lakeTableSnapshot) {
        GetLatestLakeSnapshotResponse getLakeTableSnapshotResponse =
                new GetLatestLakeSnapshotResponse();

        getLakeTableSnapshotResponse.setTableId(tableId);
        getLakeTableSnapshotResponse.setSnapshotId(lakeTableSnapshot.getSnapshotId());

        for (Map.Entry<TableBucket, Long> logEndLogOffsetEntry :
                lakeTableSnapshot.getBucketLogEndOffset().entrySet()) {
            PbLakeSnapshotForBucket pbLakeSnapshotForBucket =
                    getLakeTableSnapshotResponse.addBucketSnapshot();
            TableBucket tableBucket = logEndLogOffsetEntry.getKey();
            pbLakeSnapshotForBucket
                    .setBucketId(tableBucket.getBucket())
                    .setLogOffset(logEndLogOffsetEntry.getValue());
            if (tableBucket.getPartitionId() != null) {
                pbLakeSnapshotForBucket.setPartitionId(tableBucket.getPartitionId());
            }
        }

        return getLakeTableSnapshotResponse;
    }

    public static PartitionSpec getPartitionSpec(PbPartitionSpec pbPartitionSpec) {
        Map<String, String> partitionKeyAndValues = new HashMap<>();
        for (int i = 0; i < pbPartitionSpec.getPartitionKeyValuesCount(); i++) {
            PbKeyValue pbKeyValue = pbPartitionSpec.getPartitionKeyValueAt(i);
            partitionKeyAndValues.put(pbKeyValue.getKey(), pbKeyValue.getValue());
        }
        return new PartitionSpec(partitionKeyAndValues);
    }
}
