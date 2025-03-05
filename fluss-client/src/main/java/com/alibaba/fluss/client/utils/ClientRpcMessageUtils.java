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

package com.alibaba.fluss.client.utils;

import com.alibaba.fluss.client.admin.OffsetSpec;
import com.alibaba.fluss.client.lookup.LookupBatch;
import com.alibaba.fluss.client.lookup.PrefixLookupBatch;
import com.alibaba.fluss.client.metadata.KvSnapshotMetadata;
import com.alibaba.fluss.client.metadata.KvSnapshots;
import com.alibaba.fluss.client.metadata.LakeSnapshot;
import com.alibaba.fluss.client.write.KvWriteBatch;
import com.alibaba.fluss.client.write.WriteBatch;
import com.alibaba.fluss.fs.FsPath;
import com.alibaba.fluss.fs.FsPathAndFileName;
import com.alibaba.fluss.fs.token.ObtainedSecurityToken;
import com.alibaba.fluss.metadata.PartitionInfo;
import com.alibaba.fluss.metadata.PartitionSpec;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.remote.RemoteLogFetchInfo;
import com.alibaba.fluss.remote.RemoteLogSegment;
import com.alibaba.fluss.rpc.entity.FetchLogResultForBucket;
import com.alibaba.fluss.rpc.messages.CreatePartitionRequest;
import com.alibaba.fluss.rpc.messages.DropPartitionRequest;
import com.alibaba.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import com.alibaba.fluss.rpc.messages.GetKvSnapshotMetadataResponse;
import com.alibaba.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import com.alibaba.fluss.rpc.messages.GetLatestLakeSnapshotResponse;
import com.alibaba.fluss.rpc.messages.ListOffsetsRequest;
import com.alibaba.fluss.rpc.messages.ListPartitionInfosResponse;
import com.alibaba.fluss.rpc.messages.LookupRequest;
import com.alibaba.fluss.rpc.messages.MetadataRequest;
import com.alibaba.fluss.rpc.messages.PbFetchLogRespForBucket;
import com.alibaba.fluss.rpc.messages.PbKeyValue;
import com.alibaba.fluss.rpc.messages.PbKvSnapshot;
import com.alibaba.fluss.rpc.messages.PbLakeSnapshotForBucket;
import com.alibaba.fluss.rpc.messages.PbLookupReqForBucket;
import com.alibaba.fluss.rpc.messages.PbPhysicalTablePath;
import com.alibaba.fluss.rpc.messages.PbPrefixLookupReqForBucket;
import com.alibaba.fluss.rpc.messages.PbProduceLogReqForBucket;
import com.alibaba.fluss.rpc.messages.PbPutKvReqForBucket;
import com.alibaba.fluss.rpc.messages.PbRemoteLogFetchInfo;
import com.alibaba.fluss.rpc.messages.PbRemoteLogSegment;
import com.alibaba.fluss.rpc.messages.PbRemotePathAndLocalFile;
import com.alibaba.fluss.rpc.messages.PrefixLookupRequest;
import com.alibaba.fluss.rpc.messages.ProduceLogRequest;
import com.alibaba.fluss.rpc.messages.PutKvRequest;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.shaded.netty4.io.netty.buffer.ByteBuf;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkState;

/**
 * Utils for making rpc request/response from inner object or convert inner class to rpc
 * request/response for client.
 */
public class ClientRpcMessageUtils {

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

    public static ProduceLogRequest makeProduceLogRequest(
            long tableId, int acks, int maxRequestTimeoutMs, List<WriteBatch> batches) {
        ProduceLogRequest request =
                new ProduceLogRequest()
                        .setTableId(tableId)
                        .setAcks(acks)
                        .setTimeoutMs(maxRequestTimeoutMs);
        batches.forEach(
                batch -> {
                    PbProduceLogReqForBucket pbProduceLogReqForBucket =
                            request.addBucketsReq()
                                    .setBucketId(batch.tableBucket().getBucket())
                                    .setRecordsBytesView(batch.build());
                    TableBucket tableBucket = batch.tableBucket();
                    if (tableBucket.getPartitionId() != null) {
                        pbProduceLogReqForBucket.setPartitionId(tableBucket.getPartitionId());
                    }
                });
        return request;
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

    public static PutKvRequest makePutKvRequest(
            long tableId, int acks, int maxRequestTimeoutMs, List<WriteBatch> batches) {
        PutKvRequest request =
                new PutKvRequest()
                        .setTableId(tableId)
                        .setAcks(acks)
                        .setTimeoutMs(maxRequestTimeoutMs);
        // check the target columns in the batch list should be the same. If not same,
        // we throw exception directly currently.
        int[] targetColumns = ((KvWriteBatch) batches.get(0)).getTargetColumns();
        for (int i = 1; i < batches.size(); i++) {
            int[] currentBatchTargetColumns = ((KvWriteBatch) batches.get(i)).getTargetColumns();
            if (!Arrays.equals(targetColumns, currentBatchTargetColumns)) {
                throw new IllegalStateException(
                        String.format(
                                "All the write batches to make put kv request should have the same target columns, "
                                        + "but got %s and %s.",
                                Arrays.toString(targetColumns),
                                Arrays.toString(currentBatchTargetColumns)));
            }
        }
        if (targetColumns != null) {
            request.setTargetColumns(targetColumns);
        }
        batches.forEach(
                batch -> {
                    PbPutKvReqForBucket pbPutKvReqForBucket =
                            request.addBucketsReq()
                                    .setBucketId(batch.tableBucket().getBucket())
                                    .setRecordsBytesView(batch.build());
                    TableBucket tableBucket = batch.tableBucket();
                    if (tableBucket.getPartitionId() != null) {
                        pbPutKvReqForBucket.setPartitionId(tableBucket.getPartitionId());
                    }
                });
        return request;
    }

    public static LookupRequest makeLookupRequest(
            long tableId, Collection<LookupBatch> lookupBatches) {
        LookupRequest request = new LookupRequest().setTableId(tableId);
        lookupBatches.forEach(
                (batch) -> {
                    TableBucket tb = batch.tableBucket();
                    PbLookupReqForBucket pbLookupReqForBucket =
                            request.addBucketsReq().setBucketId(tb.getBucket());
                    if (tb.getPartitionId() != null) {
                        pbLookupReqForBucket.setPartitionId(tb.getPartitionId());
                    }
                    batch.lookups().forEach(get -> pbLookupReqForBucket.addKey(get.key()));
                });
        return request;
    }

    public static PrefixLookupRequest makePrefixLookupRequest(
            long tableId, Collection<PrefixLookupBatch> lookupBatches) {
        PrefixLookupRequest request = new PrefixLookupRequest().setTableId(tableId);
        lookupBatches.forEach(
                (batch) -> {
                    TableBucket tb = batch.tableBucket();
                    PbPrefixLookupReqForBucket pbPrefixLookupReqForBucket =
                            request.addBucketsReq().setBucketId(tb.getBucket());
                    if (tb.getPartitionId() != null) {
                        pbPrefixLookupReqForBucket.setPartitionId(tb.getPartitionId());
                    }
                    batch.lookups().forEach(get -> pbPrefixLookupReqForBucket.addKey(get.key()));
                });
        return request;
    }

    public static FetchLogResultForBucket getFetchLogResultForBucket(
            TableBucket tb, TablePath tp, PbFetchLogRespForBucket respForBucket) {
        FetchLogResultForBucket fetchLogResultForBucket;
        if (respForBucket.hasErrorCode()) {
            fetchLogResultForBucket =
                    new FetchLogResultForBucket(tb, ApiError.fromErrorMessage(respForBucket));
        } else {
            if (respForBucket.hasRemoteLogFetchInfo()) {
                PbRemoteLogFetchInfo pbRlfInfo = respForBucket.getRemoteLogFetchInfo();
                String partitionName =
                        pbRlfInfo.hasPartitionName() ? pbRlfInfo.getPartitionName() : null;
                PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tp, partitionName);
                List<RemoteLogSegment> remoteLogSegmentList = new ArrayList<>();
                for (PbRemoteLogSegment pbRemoteLogSegment : pbRlfInfo.getRemoteLogSegmentsList()) {
                    RemoteLogSegment remoteLogSegment =
                            RemoteLogSegment.Builder.builder()
                                    .tableBucket(tb)
                                    .physicalTablePath(physicalTablePath)
                                    .remoteLogSegmentId(
                                            UUID.fromString(
                                                    pbRemoteLogSegment.getRemoteLogSegmentId()))
                                    .remoteLogEndOffset(pbRemoteLogSegment.getRemoteLogEndOffset())
                                    .remoteLogStartOffset(
                                            pbRemoteLogSegment.getRemoteLogStartOffset())
                                    .segmentSizeInBytes(pbRemoteLogSegment.getSegmentSizeInBytes())
                                    .maxTimestamp(-1L) // not use.
                                    .build();
                    remoteLogSegmentList.add(remoteLogSegment);
                }
                RemoteLogFetchInfo rlFetchInfo =
                        new RemoteLogFetchInfo(
                                pbRlfInfo.getRemoteLogTabletDir(),
                                pbRlfInfo.hasPartitionName() ? pbRlfInfo.getPartitionName() : null,
                                remoteLogSegmentList,
                                pbRlfInfo.getFirstStartPos());
                fetchLogResultForBucket =
                        new FetchLogResultForBucket(
                                tb, rlFetchInfo, respForBucket.getHighWatermark());
            } else {
                ByteBuffer recordsBuffer = toByteBuffer(respForBucket.getRecordsSlice());
                LogRecords records =
                        respForBucket.hasRecords()
                                ? MemoryLogRecords.pointToByteBuffer(recordsBuffer)
                                : MemoryLogRecords.EMPTY;
                fetchLogResultForBucket =
                        new FetchLogResultForBucket(tb, records, respForBucket.getHighWatermark());
            }
        }

        return fetchLogResultForBucket;
    }

    public static KvSnapshots toKvSnapshots(GetLatestKvSnapshotsResponse response) {
        long tableId = response.getTableId();
        Long partitionId = response.hasPartitionId() ? response.getPartitionId() : null;
        Map<Integer, Long> snapshotIds = new HashMap<>();
        Map<Integer, Long> logOffsets = new HashMap<>();
        for (PbKvSnapshot pbKvSnapshot : response.getLatestSnapshotsList()) {
            int bucketId = pbKvSnapshot.getBucketId();
            Long snapshotId = pbKvSnapshot.hasSnapshotId() ? pbKvSnapshot.getSnapshotId() : null;
            snapshotIds.put(bucketId, snapshotId);
            Long logOffset = pbKvSnapshot.hasLogOffset() ? pbKvSnapshot.getLogOffset() : null;
            logOffsets.put(bucketId, logOffset);
            boolean bothNull = snapshotId == null && logOffset == null;
            boolean bothNotNull = snapshotId != null && logOffset != null;
            checkState(
                    bothNull || bothNotNull,
                    "snapshotId and logOffset should be both null or not null");
        }
        return new KvSnapshots(tableId, partitionId, snapshotIds, logOffsets);
    }

    public static KvSnapshotMetadata toKvSnapshotMetadata(GetKvSnapshotMetadataResponse response) {
        return new KvSnapshotMetadata(
                toFsPathAndFileName(response.getSnapshotFilesList()), response.getLogOffset());
    }

    public static LakeSnapshot toLakeTableSnapshotInfo(GetLatestLakeSnapshotResponse response) {
        long tableId = response.getTableId();
        long snapshotId = response.getSnapshotId();
        Map<TableBucket, Long> tableBucketsOffset =
                new HashMap<>(response.getBucketSnapshotsCount());
        for (PbLakeSnapshotForBucket pbLakeSnapshotForBucket : response.getBucketSnapshotsList()) {
            Long partitionId =
                    pbLakeSnapshotForBucket.hasPartitionId()
                            ? pbLakeSnapshotForBucket.getPartitionId()
                            : null;
            TableBucket tableBucket =
                    new TableBucket(tableId, partitionId, pbLakeSnapshotForBucket.getBucketId());
            tableBucketsOffset.put(tableBucket, pbLakeSnapshotForBucket.getLogOffset());
        }
        return new LakeSnapshot(snapshotId, tableBucketsOffset);
    }

    public static List<FsPathAndFileName> toFsPathAndFileName(
            List<PbRemotePathAndLocalFile> pbFileHandles) {
        return pbFileHandles.stream()
                .map(
                        pathAndName ->
                                new FsPathAndFileName(
                                        new FsPath(pathAndName.getRemotePath()),
                                        pathAndName.getLocalFileName()))
                .collect(Collectors.toList());
    }

    public static ObtainedSecurityToken toSecurityToken(
            GetFileSystemSecurityTokenResponse response) {
        String scheme = response.getSchema();
        byte[] tokens = response.getToken();
        Long validUntil = response.hasExpirationTime() ? response.getExpirationTime() : null;

        Map<String, String> additionInfo = toKeyValueMap(response.getAdditionInfosList());
        return new ObtainedSecurityToken(scheme, tokens, validUntil, additionInfo);
    }

    public static MetadataRequest makeMetadataRequest(
            @Nullable Set<TablePath> tablePaths,
            @Nullable Collection<PhysicalTablePath> tablePathPartitionNames,
            @Nullable Collection<Long> tablePathPartitionIds) {
        MetadataRequest metadataRequest = new MetadataRequest();
        if (tablePaths != null) {
            for (TablePath tablePath : tablePaths) {
                metadataRequest
                        .addTablePath()
                        .setDatabaseName(tablePath.getDatabaseName())
                        .setTableName(tablePath.getTableName());
            }
        }
        if (tablePathPartitionNames != null) {
            tablePathPartitionNames.forEach(
                    tablePathPartitionName ->
                            metadataRequest
                                    .addPartitionsPath()
                                    .setDatabaseName(tablePathPartitionName.getDatabaseName())
                                    .setTableName(tablePathPartitionName.getTableName())
                                    .setPartitionName(tablePathPartitionName.getPartitionName()));
        }

        if (tablePathPartitionIds != null) {
            tablePathPartitionIds.forEach(metadataRequest::addPartitionsId);
        }

        return metadataRequest;
    }

    public static ListOffsetsRequest makeListOffsetsRequest(
            long tableId,
            @Nullable Long partitionId,
            List<Integer> bucketIdList,
            OffsetSpec offsetSpec) {
        ListOffsetsRequest listOffsetsRequest = new ListOffsetsRequest();
        listOffsetsRequest
                .setFollowerServerId(-1) // -1 indicate the request from client.
                .setTableId(tableId)
                .setBucketIds(bucketIdList.stream().mapToInt(Integer::intValue).toArray());
        if (partitionId != null) {
            listOffsetsRequest.setPartitionId(partitionId);
        }

        if (offsetSpec instanceof OffsetSpec.EarliestSpec) {
            listOffsetsRequest.setOffsetType(OffsetSpec.LIST_EARLIEST_OFFSET);
        } else if (offsetSpec instanceof OffsetSpec.LatestSpec) {
            listOffsetsRequest.setOffsetType(OffsetSpec.LIST_LATEST_OFFSET);
        } else if (offsetSpec instanceof OffsetSpec.TimestampSpec) {
            listOffsetsRequest.setOffsetType(OffsetSpec.LIST_OFFSET_FROM_TIMESTAMP);
            listOffsetsRequest.setStartTimestamp(
                    ((OffsetSpec.TimestampSpec) offsetSpec).getTimestamp());
        } else {
            throw new IllegalArgumentException("Unsupported offset spec: " + offsetSpec);
        }
        return listOffsetsRequest;
    }

    public static CreatePartitionRequest makeCreatePartitionRequest(
            TablePath tablePath, PartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        CreatePartitionRequest createPartitionRequest =
                new CreatePartitionRequest().setIgnoreIfNotExists(ignoreIfNotExists);
        createPartitionRequest
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        List<PbKeyValue> pbPartitionKeyAndValues = new ArrayList<>();
        partitionSpec
                .getPartitionSpec()
                .forEach(
                        (partitionKey, value) ->
                                pbPartitionKeyAndValues.add(
                                        new PbKeyValue().setKey(partitionKey).setValue(value)));
        createPartitionRequest.setPartitionSpec().addAllPartitionKeyValues(pbPartitionKeyAndValues);
        return createPartitionRequest;
    }

    public static DropPartitionRequest makeDropPartitionRequest(
            TablePath tablePath, PartitionSpec partitionSpec, boolean ignoreIfNotExists) {
        DropPartitionRequest dropPartitionRequest =
                new DropPartitionRequest().setIgnoreIfNotExists(ignoreIfNotExists);
        dropPartitionRequest
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());
        List<PbKeyValue> pbPartitionKeyAndValues = new ArrayList<>();
        partitionSpec
                .getPartitionSpec()
                .forEach(
                        (partitionKey, value) ->
                                pbPartitionKeyAndValues.add(
                                        new PbKeyValue().setKey(partitionKey).setValue(value)));
        dropPartitionRequest.setPartitionSpec().addAllPartitionKeyValues(pbPartitionKeyAndValues);
        return dropPartitionRequest;
    }

    public static List<PartitionInfo> toPartitionInfos(ListPartitionInfosResponse response) {
        return response.getPartitionsInfosList().stream()
                .map(
                        pbPartitionInfo ->
                                new PartitionInfo(
                                        pbPartitionInfo.getPartitionId(),
                                        pbPartitionInfo.getPartitionName()))
                .collect(Collectors.toList());
    }

    public static Map<String, String> toKeyValueMap(List<PbKeyValue> pbKeyValues) {
        return pbKeyValues.stream()
                .collect(
                        java.util.stream.Collectors.toMap(
                                PbKeyValue::getKey, PbKeyValue::getValue));
    }
}
