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

package org.apache.fluss.client.utils;

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.lookup.LookupBatch;
import org.apache.fluss.client.lookup.PrefixLookupBatch;
import org.apache.fluss.client.metadata.KvSnapshotMetadata;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.write.KvWriteBatch;
import org.apache.fluss.client.write.ReadyWriteBatch;
import org.apache.fluss.cluster.rebalance.RebalancePlanForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceProgress;
import org.apache.fluss.cluster.rebalance.RebalanceResultForBucket;
import org.apache.fluss.cluster.rebalance.RebalanceStatus;
import org.apache.fluss.config.cluster.AlterConfigOpType;
import org.apache.fluss.config.cluster.ColumnPositionType;
import org.apache.fluss.config.cluster.ConfigEntry;
import org.apache.fluss.fs.FsPath;
import org.apache.fluss.fs.FsPathAndFileName;
import org.apache.fluss.fs.token.ObtainedSecurityToken;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableChange;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.messages.AlterTableRequest;
import org.apache.fluss.rpc.messages.CreatePartitionRequest;
import org.apache.fluss.rpc.messages.DropPartitionRequest;
import org.apache.fluss.rpc.messages.GetFileSystemSecurityTokenResponse;
import org.apache.fluss.rpc.messages.GetKvSnapshotMetadataResponse;
import org.apache.fluss.rpc.messages.GetLatestKvSnapshotsResponse;
import org.apache.fluss.rpc.messages.GetLatestLakeSnapshotResponse;
import org.apache.fluss.rpc.messages.ListOffsetsRequest;
import org.apache.fluss.rpc.messages.ListPartitionInfosResponse;
import org.apache.fluss.rpc.messages.ListRebalanceProgressResponse;
import org.apache.fluss.rpc.messages.LookupRequest;
import org.apache.fluss.rpc.messages.MetadataRequest;
import org.apache.fluss.rpc.messages.PbAddColumn;
import org.apache.fluss.rpc.messages.PbAlterConfig;
import org.apache.fluss.rpc.messages.PbDescribeConfig;
import org.apache.fluss.rpc.messages.PbDropColumn;
import org.apache.fluss.rpc.messages.PbKeyValue;
import org.apache.fluss.rpc.messages.PbKvSnapshot;
import org.apache.fluss.rpc.messages.PbLakeSnapshotForBucket;
import org.apache.fluss.rpc.messages.PbLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbModifyColumn;
import org.apache.fluss.rpc.messages.PbPartitionSpec;
import org.apache.fluss.rpc.messages.PbPrefixLookupReqForBucket;
import org.apache.fluss.rpc.messages.PbProduceLogReqForBucket;
import org.apache.fluss.rpc.messages.PbPutKvReqForBucket;
import org.apache.fluss.rpc.messages.PbRebalancePlanForBucket;
import org.apache.fluss.rpc.messages.PbRebalanceProgressForBucket;
import org.apache.fluss.rpc.messages.PbRebalanceProgressForTable;
import org.apache.fluss.rpc.messages.PbRemotePathAndLocalFile;
import org.apache.fluss.rpc.messages.PbRenameColumn;
import org.apache.fluss.rpc.messages.PrefixLookupRequest;
import org.apache.fluss.rpc.messages.ProduceLogRequest;
import org.apache.fluss.rpc.messages.PutKvRequest;
import org.apache.fluss.utils.json.DataTypeJsonSerde;
import org.apache.fluss.utils.json.JsonSerdeUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.cluster.rebalance.RebalanceStatus.FINAL_STATUSES;
import static org.apache.fluss.rpc.util.CommonRpcMessageUtils.toResolvedPartitionSpec;
import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkState;

/**
 * Utils for making rpc request/response from inner object or convert inner class to rpc
 * request/response for client.
 */
public class ClientRpcMessageUtils {

    public static ProduceLogRequest makeProduceLogRequest(
            long tableId, int acks, int maxRequestTimeoutMs, List<ReadyWriteBatch> readyBatches) {
        ProduceLogRequest request =
                new ProduceLogRequest()
                        .setTableId(tableId)
                        .setAcks(acks)
                        .setTimeoutMs(maxRequestTimeoutMs);
        readyBatches.forEach(
                readyBatch -> {
                    TableBucket tableBucket = readyBatch.tableBucket();
                    PbProduceLogReqForBucket pbProduceLogReqForBucket =
                            request.addBucketsReq()
                                    .setBucketId(tableBucket.getBucket())
                                    .setRecordsBytesView(readyBatch.writeBatch().build());
                    if (tableBucket.getPartitionId() != null) {
                        pbProduceLogReqForBucket.setPartitionId(tableBucket.getPartitionId());
                    }
                });
        return request;
    }

    public static PutKvRequest makePutKvRequest(
            long tableId,
            int acks,
            int maxRequestTimeoutMs,
            List<ReadyWriteBatch> readyWriteBatches) {
        PutKvRequest request =
                new PutKvRequest()
                        .setTableId(tableId)
                        .setAcks(acks)
                        .setTimeoutMs(maxRequestTimeoutMs);
        // check the target columns in the batch list should be the same. If not same,
        // we throw exception directly currently.
        int[] targetColumns =
                ((KvWriteBatch) readyWriteBatches.get(0).writeBatch()).getTargetColumns();
        for (int i = 1; i < readyWriteBatches.size(); i++) {
            int[] currentBatchTargetColumns =
                    ((KvWriteBatch) readyWriteBatches.get(i).writeBatch()).getTargetColumns();
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
        readyWriteBatches.forEach(
                readyBatch -> {
                    TableBucket tableBucket = readyBatch.tableBucket();
                    PbPutKvReqForBucket pbPutKvReqForBucket =
                            request.addBucketsReq()
                                    .setBucketId(tableBucket.getBucket())
                                    .setRecordsBytesView(readyBatch.writeBatch().build());
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
        PbPartitionSpec pbPartitionSpec = makePbPartitionSpec(partitionSpec);
        createPartitionRequest.setPartitionSpec(pbPartitionSpec);
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
        PbPartitionSpec pbPartitionSpec = makePbPartitionSpec(partitionSpec);
        dropPartitionRequest.setPartitionSpec(pbPartitionSpec);
        return dropPartitionRequest;
    }

    public static AlterTableRequest makeAlterTableRequest(
            TablePath tablePath, List<TableChange> tableChanges, boolean ignoreIfNotExists) {
        AlterTableRequest request = new AlterTableRequest();
        request.setIgnoreIfNotExists(ignoreIfNotExists)
                .setTablePath()
                .setDatabaseName(tablePath.getDatabaseName())
                .setTableName(tablePath.getTableName());

        List<PbAddColumn> addColumns = new ArrayList<>();
        List<PbDropColumn> dropColumns = new ArrayList<>();
        List<PbRenameColumn> renameColumns = new ArrayList<>();
        List<PbModifyColumn> modifyColumns = new ArrayList<>();
        List<PbAlterConfig> alterConfigs = new ArrayList<>();
        for (TableChange tableChange : tableChanges) {
            if (tableChange instanceof TableChange.AddColumn) {
                addColumns.add(toPbAddColumn((TableChange.AddColumn) tableChange));
            } else if (tableChange instanceof TableChange.DropColumn) {
                dropColumns.add(toPbDropColumn((TableChange.DropColumn) tableChange));
            } else if (tableChange instanceof TableChange.RenameColumn) {
                renameColumns.add(toPbRenameColumn((TableChange.RenameColumn) tableChange));
            } else if (tableChange instanceof TableChange.ModifyColumn) {
                modifyColumns.add(toPbModifyColumn((TableChange.ModifyColumn) tableChange));
            } else if (tableChange instanceof TableChange.SetOption
                    || tableChange instanceof TableChange.ResetOption) {
                alterConfigs.add(toPbAlterConfigs(tableChange));
            } else {
                throw new IllegalArgumentException(
                        "Unsupported table change: " + tableChange.getClass());
            }
        }
        request.addAllConfigChanges(alterConfigs)
                .addAllAddColumns(addColumns)
                .addAllDropColumns(dropColumns)
                .addAllRenameColumns(renameColumns)
                .addAllModifyColumns(modifyColumns);
        return request;
    }

    public static Optional<RebalanceProgress> toRebalanceProgress(
            ListRebalanceProgressResponse response) {
        if (!response.hasRebalanceId()) {
            return Optional.empty();
        }

        checkArgument(response.hasRebalanceStatus(), "Rebalance status is not set");
        RebalanceStatus totalRebalanceStatus = RebalanceStatus.of(response.getRebalanceStatus());
        int totalTask = 0;
        int finishedTask = 0;
        Map<TableBucket, RebalanceResultForBucket> rebalanceProgress = new HashMap<>();
        for (PbRebalanceProgressForTable pbTable : response.getTableProgressesList()) {
            long tableId = pbTable.getTableId();
            for (PbRebalanceProgressForBucket pbBucket : pbTable.getBucketsProgressesList()) {
                RebalanceStatus bucketStatus = RebalanceStatus.of(pbBucket.getRebalanceStatus());
                RebalancePlanForBucket planForBucket =
                        toRebalancePlanForBucket(tableId, pbBucket.getRebalancePlan());
                rebalanceProgress.put(
                        planForBucket.getTableBucket(),
                        new RebalanceResultForBucket(planForBucket, bucketStatus));
                if (FINAL_STATUSES.contains(bucketStatus)) {
                    finishedTask++;
                }
                totalTask++;
            }
        }

        // For these rebalance task without only bucket level rebalance tasks, we return -1 as
        // progress.
        double progress = -1d;
        if (totalTask != 0) {
            progress = (double) finishedTask / totalTask;
        }

        return Optional.of(
                new RebalanceProgress(
                        response.getRebalanceId(),
                        totalRebalanceStatus,
                        progress,
                        rebalanceProgress));
    }

    private static RebalancePlanForBucket toRebalancePlanForBucket(
            long tableId, PbRebalancePlanForBucket rebalancePlan) {
        TableBucket tableBucket =
                new TableBucket(
                        tableId,
                        rebalancePlan.hasPartitionId() ? rebalancePlan.getPartitionId() : null,
                        rebalancePlan.getBucketId());
        return new RebalancePlanForBucket(
                tableBucket,
                rebalancePlan.getOriginalLeader(),
                rebalancePlan.getNewLeader(),
                Arrays.stream(rebalancePlan.getOriginalReplicas())
                        .boxed()
                        .collect(Collectors.toList()),
                Arrays.stream(rebalancePlan.getNewReplicas()).boxed().collect(Collectors.toList()));
    }

    public static List<PartitionInfo> toPartitionInfos(ListPartitionInfosResponse response) {
        return response.getPartitionsInfosList().stream()
                .map(
                        pbPartitionInfo ->
                                new PartitionInfo(
                                        pbPartitionInfo.getPartitionId(),
                                        toResolvedPartitionSpec(
                                                pbPartitionInfo.getPartitionSpec())))
                .collect(Collectors.toList());
    }

    public static Map<String, String> toKeyValueMap(List<PbKeyValue> pbKeyValues) {
        return pbKeyValues.stream()
                .collect(
                        java.util.stream.Collectors.toMap(
                                PbKeyValue::getKey, PbKeyValue::getValue));
    }

    public static PbPartitionSpec makePbPartitionSpec(PartitionSpec partitionSpec) {
        Map<String, String> partitionSpecMap = partitionSpec.getSpecMap();
        List<PbKeyValue> pbKeyValues = new ArrayList<>(partitionSpecMap.size());
        partitionSpecMap.forEach(
                (key, value) -> pbKeyValues.add(new PbKeyValue().setKey(key).setValue(value)));
        return new PbPartitionSpec().addAllPartitionKeyValues(pbKeyValues);
    }

    public static PbAlterConfig toPbAlterConfigs(TableChange tableChange) {
        PbAlterConfig info = new PbAlterConfig();
        if (tableChange instanceof TableChange.SetOption) {
            TableChange.SetOption setOption = (TableChange.SetOption) tableChange;
            info.setConfigKey(setOption.getKey());
            info.setConfigValue(setOption.getValue());
            info.setOpType(AlterConfigOpType.SET.value());
        } else if (tableChange instanceof TableChange.ResetOption) {
            TableChange.ResetOption resetOption = (TableChange.ResetOption) tableChange;
            info.setConfigKey(resetOption.getKey());
            info.setOpType(AlterConfigOpType.DELETE.value());
        } else {
            throw new IllegalArgumentException(
                    "Unsupported table change: " + tableChange.getClass());
        }
        return info;
    }

    public static PbAddColumn toPbAddColumn(TableChange.AddColumn addColumn) {
        ColumnPositionType columnPositionType = ColumnPositionType.from(addColumn.getPosition());

        PbAddColumn pbAddColumn =
                new PbAddColumn()
                        .setColumnName(addColumn.getName())
                        .setDataTypeJson(
                                JsonSerdeUtils.writeValueAsBytes(
                                        addColumn.getDataType(), DataTypeJsonSerde.INSTANCE))
                        .setColumnPositionType(columnPositionType.value());
        if (addColumn.getComment() != null) {
            pbAddColumn.setComment(addColumn.getComment());
        }

        return pbAddColumn;
    }

    public static PbDropColumn toPbDropColumn(TableChange.DropColumn dropColumn) {
        return new PbDropColumn().setColumnName(dropColumn.getName());
    }

    public static PbRenameColumn toPbRenameColumn(TableChange.RenameColumn dropColumn) {
        return new PbRenameColumn()
                .setOldColumnName(dropColumn.getOldColumnName())
                .setNewColumnName(dropColumn.getNewColumnName());
    }

    public static PbModifyColumn toPbModifyColumn(TableChange.ModifyColumn modifyColumn) {
        PbModifyColumn pbModifyColumn =
                new PbModifyColumn()
                        .setColumnName(modifyColumn.getName())
                        .setDataTypeJson(
                                JsonSerdeUtils.writeValueAsBytes(
                                        modifyColumn.getDataType(), DataTypeJsonSerde.INSTANCE));
        if (modifyColumn.getNewPosition() != null) {
            ColumnPositionType columnPositionType =
                    ColumnPositionType.from(modifyColumn.getNewPosition());
            pbModifyColumn.setColumnPositionType(columnPositionType.value());
        }
        if (modifyColumn.getComment() != null) {
            pbModifyColumn.setComment(modifyColumn.getComment());
        }
        return pbModifyColumn;
    }

    public static List<ConfigEntry> toConfigEntries(List<PbDescribeConfig> pbDescribeConfigs) {
        return pbDescribeConfigs.stream()
                .map(
                        pbDescribeConfig ->
                                new ConfigEntry(
                                        pbDescribeConfig.getConfigKey(),
                                        pbDescribeConfig.hasConfigValue()
                                                ? pbDescribeConfig.getConfigValue()
                                                : null,
                                        ConfigEntry.ConfigSource.valueOf(
                                                pbDescribeConfig.getConfigSource())))
                .collect(Collectors.toList());
    }
}
