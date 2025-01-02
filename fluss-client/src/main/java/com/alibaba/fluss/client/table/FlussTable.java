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

package com.alibaba.fluss.client.table;

import com.alibaba.fluss.annotation.PublicEvolving;
import com.alibaba.fluss.client.lakehouse.LakeTableBucketAssigner;
import com.alibaba.fluss.client.lookup.LookupClient;
import com.alibaba.fluss.client.lookup.LookupResult;
import com.alibaba.fluss.client.lookup.PrefixLookupResult;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.scanner.RemoteFileDownloader;
import com.alibaba.fluss.client.scanner.ScanRecord;
import com.alibaba.fluss.client.scanner.log.FlussLogScanner;
import com.alibaba.fluss.client.scanner.log.LogScan;
import com.alibaba.fluss.client.scanner.log.LogScanner;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScan;
import com.alibaba.fluss.client.scanner.snapshot.SnapshotScanner;
import com.alibaba.fluss.client.table.getter.PartitionGetter;
import com.alibaba.fluss.client.table.writer.AppendWriter;
import com.alibaba.fluss.client.table.writer.UpsertWrite;
import com.alibaba.fluss.client.table.writer.UpsertWriter;
import com.alibaba.fluss.client.token.DefaultSecurityTokenManager;
import com.alibaba.fluss.client.token.DefaultSecurityTokenProvider;
import com.alibaba.fluss.client.token.SecurityTokenManager;
import com.alibaba.fluss.client.token.SecurityTokenProvider;
import com.alibaba.fluss.client.write.HashBucketAssigner;
import com.alibaba.fluss.client.write.WriterClient;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
import com.alibaba.fluss.exception.PartitionNotExistException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TableInfo;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.DefaultValueRecordBatch;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.record.LogRecordReadContext;
import com.alibaba.fluss.record.LogRecords;
import com.alibaba.fluss.record.MemoryLogRecords;
import com.alibaba.fluss.record.ValueRecord;
import com.alibaba.fluss.record.ValueRecordReadContext;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.row.ProjectedRow;
import com.alibaba.fluss.row.decode.RowDecoder;
import com.alibaba.fluss.row.encode.KeyEncoder;
import com.alibaba.fluss.row.encode.ValueDecoder;
import com.alibaba.fluss.rpc.GatewayClientProxy;
import com.alibaba.fluss.rpc.RpcClient;
import com.alibaba.fluss.rpc.gateway.AdminReadOnlyGateway;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.LimitScanRequest;
import com.alibaba.fluss.rpc.messages.LimitScanResponse;
import com.alibaba.fluss.rpc.metrics.ClientMetricGroup;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.types.DataField;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;
import com.alibaba.fluss.utils.Preconditions;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.alibaba.fluss.client.utils.MetadataUtils.getOneAvailableTabletServerNode;

/**
 * The base impl of {@link Table}.
 *
 * @since 0.1
 */
@PublicEvolving
public class FlussTable implements Table {

    private final Configuration conf;
    private final long tableId;
    private final TablePath tablePath;
    private final RpcClient rpcClient;
    private final MetadataUpdater metadataUpdater;
    private final TableInfo tableInfo;
    private final boolean hasPrimaryKey;
    private final int numBuckets;
    private final RowType keyRowType;
    // decode the lookup bytes to result row
    private final ValueDecoder kvValueDecoder;
    // a getter to extract partition from key row, null when it's not a partitioned primary key
    // table
    private final @Nullable PartitionGetter keyRowPartitionGetter;

    private final KeyEncoder bucketKeyEncoder;
    private final KeyEncoder primaryKeyEncoder;

    private final Supplier<WriterClient> writerSupplier;
    private final Supplier<LookupClient> lookupClientSupplier;
    private final AtomicBoolean closed;
    // metrics related.
    private final ClientMetricGroup clientMetricGroup;

    private volatile RemoteFileDownloader remoteFileDownloader;
    private volatile SecurityTokenManager securityTokenManager;

    private @Nullable LakeTableBucketAssigner lakeTableBucketAssigner;

    public FlussTable(
            Configuration conf,
            TablePath tablePath,
            RpcClient rpcClient,
            MetadataUpdater metadataUpdater,
            Supplier<WriterClient> writerSupplier,
            Supplier<LookupClient> lookupClientSupplier,
            ClientMetricGroup clientMetricGroup) {
        this.conf = conf;
        this.tablePath = tablePath;
        this.rpcClient = rpcClient;
        this.writerSupplier = writerSupplier;
        this.lookupClientSupplier = lookupClientSupplier;
        this.metadataUpdater = metadataUpdater;
        this.clientMetricGroup = clientMetricGroup;

        metadataUpdater.checkAndUpdateTableMetadata(Collections.singleton(tablePath));

        this.tableInfo = metadataUpdater.getTableInfoOrElseThrow(tablePath);
        this.tableId = tableInfo.getTableId();
        TableDescriptor tableDescriptor = tableInfo.getTableDescriptor();
        Schema schema = tableDescriptor.getSchema();
        RowType rowType = schema.toRowType();
        this.hasPrimaryKey = tableDescriptor.hasPrimaryKey();
        this.numBuckets = metadataUpdater.getBucketCount(tablePath);
        this.keyRowType = getKeyRowType(schema, schema.getPrimaryKeyIndexes());
        this.keyRowPartitionGetter =
                tableDescriptor.isPartitioned() && tableDescriptor.hasPrimaryKey()
                        ? new PartitionGetter(keyRowType, tableDescriptor.getPartitionKeys())
                        : null;
        this.closed = new AtomicBoolean(false);
        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableDescriptor.getKvFormat(),
                                rowType.getChildren().toArray(new DataType[0])));

        this.primaryKeyEncoder =
                KeyEncoder.createKeyEncoder(
                        keyRowType, keyRowType.getFieldNames(), tableDescriptor.getPartitionKeys());
        int[] bucketKeyIndexes = tableDescriptor.getBucketKeyIndexes();
        if (bucketKeyIndexes.length != 0) {
            this.bucketKeyEncoder = new KeyEncoder(getKeyRowType(schema, bucketKeyIndexes));
        } else {
            this.bucketKeyEncoder = primaryKeyEncoder;
        }
    }

    @Override
    public TableDescriptor getDescriptor() {
        return tableInfo.getTableDescriptor();
    }

    @Override
    public CompletableFuture<LookupResult> lookup(InternalRow key) {
        if (!hasPrimaryKey) {
            throw new FlussRuntimeException(
                    String.format("none-pk table %s not support lookup()", tablePath));
        }
        // encoding the key row using a compacted way consisted with how the key is encoded when put
        // a row
        byte[] pkBytes = primaryKeyEncoder.encode(key);
        byte[] bkBytes = bucketKeyEncoder.encode(key);
        Long partitionId = keyRowPartitionGetter == null ? null : getPartitionId(key);
        int bucketId = getBucketId(bkBytes, key);
        TableBucket tableBucket = new TableBucket(tableId, partitionId, bucketId);
        return lookupClientSupplier
                .get()
                .lookup(tableBucket, pkBytes)
                .thenApply(
                        valueBytes -> {
                            InternalRow row =
                                    valueBytes == null
                                            ? null
                                            : kvValueDecoder.decodeValue(valueBytes).row;
                            return new LookupResult(row);
                        });
    }

    @Override
    public CompletableFuture<PrefixLookupResult> prefixLookup(InternalRow bucketKey) {
        if (!hasPrimaryKey) {
            throw new FlussRuntimeException(
                    String.format("None-pk table %s don't support prefix lookup", tablePath));
        }
        // TODO: add checks the bucket key is prefix of primary key

        byte[] prefixKeyBytes = bucketKeyEncoder.encode(bucketKey);
        int bucketId = getBucketId(prefixKeyBytes, bucketKey);
        return lookupClientSupplier
                .get()
                .prefixLookup(tableId, bucketId, prefixKeyBytes)
                .thenApply(
                        result -> {
                            List<InternalRow> rowList = new ArrayList<>();
                            for (byte[] valueBytes : result) {
                                rowList.add(
                                        valueBytes == null
                                                ? null
                                                : kvValueDecoder.decodeValue(valueBytes).row);
                            }
                            return new PrefixLookupResult(rowList);
                        });
    }

    private int getBucketId(byte[] keyBytes, InternalRow key) {
        if (!tableInfo.getTableDescriptor().isDataLakeEnabled()) {
            return HashBucketAssigner.bucketForRowKey(keyBytes, numBuckets);
        } else {
            if (lakeTableBucketAssigner == null) {
                lakeTableBucketAssigner =
                        new LakeTableBucketAssigner(
                                keyRowType,
                                tableInfo.getTableDescriptor().getBucketKey(),
                                numBuckets);
            }
            return lakeTableBucketAssigner.assignBucket(
                    keyBytes, key, metadataUpdater.getCluster());
        }
    }

    @Override
    public CompletableFuture<List<ScanRecord>> limitScan(
            TableBucket tableBucket, int limit, @Nullable int[] projectedFields) {

        LimitScanRequest limitScanRequest =
                new LimitScanRequest()
                        .setTableId(tableBucket.getTableId())
                        .setBucketId(tableBucket.getBucket())
                        .setLimit(limit);

        if (tableBucket.getPartitionId() != null) {
            limitScanRequest.setPartitionId(tableBucket.getPartitionId());
            metadataUpdater.checkAndUpdateMetadata(tablePath, tableBucket);
        }

        // because that rocksdb is not suitable to projection, thus do it in client.
        int leader = metadataUpdater.leaderFor(tableBucket);
        TabletServerGateway gateway = metadataUpdater.newTabletServerClientForNode(leader);
        RowType rowType = tableInfo.getTableDescriptor().getSchema().toRowType();
        InternalRow.FieldGetter[] fieldGetters =
                new InternalRow.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            fieldGetters[i] = InternalRow.createFieldGetter(rowType.getTypeAt(i), i);
        }

        CompletableFuture<List<ScanRecord>> future = new CompletableFuture<>();
        gateway.limitScan(limitScanRequest)
                .thenAccept(
                        limitScantResponse -> {
                            if (!limitScantResponse.hasErrorCode()) {
                                future.complete(
                                        parseLimitScanResponse(
                                                limit,
                                                limitScantResponse,
                                                projectedFields,
                                                hasPrimaryKey,
                                                fieldGetters));
                            } else {
                                throw ApiError.fromErrorMessage(limitScantResponse).exception();
                            }
                        })
                .exceptionally(
                        throwable -> {
                            future.completeExceptionally(throwable);
                            return null;
                        });
        return future;
    }

    private List<ScanRecord> parseLimitScanResponse(
            int limit,
            LimitScanResponse limitScanResponse,
            @Nullable int[] projectedFields,
            boolean hasPrimaryKey,
            InternalRow.FieldGetter[] fieldGetters) {
        List<ScanRecord> scanRecordList = new ArrayList<>();
        if (!limitScanResponse.hasRecords()) {
            return scanRecordList;
        }
        ByteBuffer recordsBuffer = ByteBuffer.wrap(limitScanResponse.getRecords());
        if (hasPrimaryKey) {
            DefaultValueRecordBatch valueRecords =
                    DefaultValueRecordBatch.pointToByteBuffer(recordsBuffer);
            ValueRecordReadContext readContext =
                    new ValueRecordReadContext(kvValueDecoder.getRowDecoder());
            for (ValueRecord record : valueRecords.records(readContext)) {
                addScanRecord(projectedFields, scanRecordList, record.getRow(), fieldGetters);
            }
        } else {
            LogRecordReadContext readContext =
                    LogRecordReadContext.createReadContext(tableInfo, false, null);
            LogRecords records = MemoryLogRecords.pointToByteBuffer(recordsBuffer);
            for (LogRecordBatch logRecordBatch : records.batches()) {
                // A batch of log record maybe little more than limit, thus we need slice the
                // last limit number.
                try (CloseableIterator<LogRecord> logRecordIterator =
                        logRecordBatch.records(readContext)) {
                    while (logRecordIterator.hasNext()) {
                        addScanRecord(
                                projectedFields,
                                scanRecordList,
                                logRecordIterator.next().getRow(),
                                fieldGetters);
                    }
                }
            }
            if (scanRecordList.size() > limit) {
                scanRecordList =
                        scanRecordList.subList(
                                scanRecordList.size() - limit, scanRecordList.size());
            }
        }

        return scanRecordList;
    }

    private void addScanRecord(
            @Nullable int[] projectedFields,
            List<ScanRecord> scanRecordList,
            InternalRow originRow,
            InternalRow.FieldGetter[] fieldGetters) {
        GenericRow newRow = new GenericRow(fieldGetters.length);
        for (int i = 0; i < fieldGetters.length; i++) {
            newRow.setField(i, fieldGetters[i].getFieldOrNull(originRow));
        }
        if (projectedFields != null) {
            ProjectedRow row = ProjectedRow.from(projectedFields);
            row.replaceRow(newRow);
            scanRecordList.add(new ScanRecord(row));
        } else {
            scanRecordList.add(new ScanRecord(newRow));
        }
    }

    /**
     * Return the id of the partition the row belongs to. It'll try to update the metadata if the
     * partition doesn't exist. If the partition doesn't exist yet after update metadata, it'll
     * throw {@link PartitionNotExistException}.
     */
    private Long getPartitionId(InternalRow row) {
        Preconditions.checkNotNull(keyRowPartitionGetter, "partitionGetter shouldn't be null.");
        String partitionName = keyRowPartitionGetter.getPartition(row);
        PhysicalTablePath physicalTablePath = PhysicalTablePath.of(tablePath, partitionName);
        metadataUpdater.checkAndUpdatePartitionMetadata(physicalTablePath);
        return metadataUpdater.getCluster().getPartitionIdOrElseThrow(physicalTablePath);
    }

    private RowType getKeyRowType(Schema schema, int[] keyIndexes) {
        List<DataField> keyRowFields = new ArrayList<>(keyIndexes.length);
        List<DataField> rowFields = schema.toRowType().getFields();
        for (int index : keyIndexes) {
            keyRowFields.add(rowFields.get(index));
        }
        return new RowType(keyRowFields);
    }

    @Override
    public AppendWriter getAppendWriter() {
        if (hasPrimaryKey) {
            throw new FlussRuntimeException(
                    String.format("Can't get a LogWriter for PrimaryKey table %s", tablePath));
        }
        return new AppendWriter(
                tablePath, tableInfo.getTableDescriptor(), metadataUpdater, writerSupplier.get());
    }

    @Override
    public UpsertWriter getUpsertWriter(UpsertWrite upsertWrite) {
        if (!hasPrimaryKey) {
            throw new FlussRuntimeException(
                    String.format("Can't get a KvWriter for Log table %s", tablePath));
        }
        return new UpsertWriter(
                tablePath,
                tableInfo.getTableDescriptor(),
                upsertWrite,
                writerSupplier.get(),
                metadataUpdater);
    }

    @Override
    public UpsertWriter getUpsertWriter() {
        return getUpsertWriter(new UpsertWrite());
    }

    @Override
    public LogScanner getLogScanner(LogScan logScan) {
        mayPrepareSecurityTokeResource();
        mayPrepareRemoteFileDownloader();

        return new FlussLogScanner(
                conf,
                tableInfo,
                rpcClient,
                metadataUpdater,
                logScan,
                clientMetricGroup,
                remoteFileDownloader);
    }

    @Override
    public SnapshotScanner getSnapshotScanner(SnapshotScan snapshotScan) {
        mayPrepareSecurityTokeResource();
        mayPrepareRemoteFileDownloader();

        return new SnapshotScanner(
                conf,
                tableInfo.getTableDescriptor().getKvFormat(),
                remoteFileDownloader,
                snapshotScan);
    }

    @Override
    public void close() throws Exception {
        if (closed.compareAndSet(false, true)) {
            if (remoteFileDownloader != null) {
                remoteFileDownloader.close();
            }
            if (securityTokenManager != null) {
                // todo: FLUSS-56910234 we don't have to wait until close fluss table
                // to stop securityTokenManager
                securityTokenManager.stop();
            }
        }
    }

    private void mayPrepareSecurityTokeResource() {
        if (securityTokenManager == null) {
            synchronized (this) {
                if (securityTokenManager == null) {
                    // prepare security token manager
                    // create the admin read only gateway
                    // todo: may add retry logic when no any available tablet server?
                    AdminReadOnlyGateway gateway =
                            GatewayClientProxy.createGatewayProxy(
                                    () ->
                                            getOneAvailableTabletServerNode(
                                                    metadataUpdater.getCluster()),
                                    rpcClient,
                                    AdminReadOnlyGateway.class);
                    SecurityTokenProvider securityTokenProvider =
                            new DefaultSecurityTokenProvider(gateway);
                    securityTokenManager =
                            new DefaultSecurityTokenManager(conf, securityTokenProvider);
                    try {
                        securityTokenManager.start();
                    } catch (Exception e) {
                        throw new FlussRuntimeException("start security token manager failed", e);
                    }
                }
            }
        }
    }

    private void mayPrepareRemoteFileDownloader() {
        if (remoteFileDownloader == null) {
            synchronized (this) {
                if (remoteFileDownloader == null) {
                    remoteFileDownloader =
                            new RemoteFileDownloader(
                                    conf.getInt(ConfigOptions.REMOTE_FILE_DOWNLOAD_THREAD_NUM));
                }
            }
        }
    }
}
