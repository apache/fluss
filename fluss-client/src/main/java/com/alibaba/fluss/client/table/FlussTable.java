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
import com.alibaba.fluss.client.lookup.FlussLookuper;
import com.alibaba.fluss.client.lookup.FlussPrefixLookuper;
import com.alibaba.fluss.client.lookup.LookupClient;
import com.alibaba.fluss.client.lookup.Lookuper;
import com.alibaba.fluss.client.lookup.PrefixLookup;
import com.alibaba.fluss.client.lookup.PrefixLookuper;
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
import com.alibaba.fluss.client.write.WriterClient;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.FlussRuntimeException;
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
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.RowType;
import com.alibaba.fluss.utils.CloseableIterator;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static com.alibaba.fluss.client.utils.MetadataUtils.getOneAvailableTabletServerNode;
import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/**
 * The base impl of {@link Table}.
 *
 * @since 0.1
 */
@PublicEvolving
public class FlussTable implements Table {

    private final Configuration conf;
    private final TablePath tablePath;
    private final RpcClient rpcClient;
    private final MetadataUpdater metadataUpdater;
    private final TableInfo tableInfo;
    private final boolean hasPrimaryKey;
    private final int numBuckets;
    private final Schema schema;
    private final TableDescriptor tableDescriptor;
    private final RowType rowType;
    private final RowType primaryKeyRowType;
    private final @Nullable RowType bucketKeyRowType;
    // decode the lookup bytes to result row
    private final ValueDecoder kvValueDecoder;
    private final List<String> partitionKeyNames;

    private final Supplier<WriterClient> writerSupplier;
    private final Supplier<LookupClient> lookupClientSupplier;
    private final AtomicBoolean closed;
    // metrics related.
    private final ClientMetricGroup clientMetricGroup;

    private volatile RemoteFileDownloader remoteFileDownloader;
    private volatile SecurityTokenManager securityTokenManager;
    private volatile LakeTableBucketAssigner lakeTableBucketAssigner;

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
        this.tableDescriptor = tableInfo.getTableDescriptor();
        this.schema = tableDescriptor.getSchema();
        this.rowType = schema.toRowType();
        this.hasPrimaryKey = tableDescriptor.hasPrimaryKey();
        this.numBuckets = metadataUpdater.getBucketCount(tablePath);
        this.primaryKeyRowType = rowType.project(schema.getPrimaryKeyIndexes());
        this.closed = new AtomicBoolean(false);
        this.kvValueDecoder =
                new ValueDecoder(
                        RowDecoder.create(
                                tableDescriptor.getKvFormat(),
                                rowType.getChildren().toArray(new DataType[0])));

        int[] bucketKeyIndexes = tableDescriptor.getBucketKeyIndexes();
        if (bucketKeyIndexes.length != 0) {
            this.bucketKeyRowType = rowType.project(bucketKeyIndexes);
        } else {
            this.bucketKeyRowType = null;
        }

        this.partitionKeyNames =
                tableDescriptor.isPartitioned()
                        ? tableDescriptor.getPartitionKeys()
                        : new ArrayList<>();
    }

    @Override
    public TableDescriptor getDescriptor() {
        return tableInfo.getTableDescriptor();
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
    public Lookuper getLookuper() {
        // lookup keys equals with primary keys.
        if (!hasPrimaryKey) {
            throw new FlussRuntimeException(
                    String.format("none-pk table %s not support lookup()", tablePath));
        }
        maybeCreateLakeTableBucketAssigner();

        PartitionGetter partitionGetter =
                tableDescriptor.isPartitioned() && tableDescriptor.hasPrimaryKey()
                        ? new PartitionGetter(primaryKeyRowType, tableDescriptor.getPartitionKeys())
                        : null;
        KeyEncoder primaryKeyEncoder =
                KeyEncoder.createKeyEncoder(
                        primaryKeyRowType,
                        primaryKeyRowType.getFieldNames(),
                        tableDescriptor.getPartitionKeys());
        checkArgument(bucketKeyRowType != null, "bucketKeyRowType shouldn't be null.");
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.createKeyEncoder(
                        primaryKeyRowType, bucketKeyRowType.getFieldNames(), partitionKeyNames);
        return new FlussLookuper(
                tableInfo,
                numBuckets,
                metadataUpdater,
                lookupClientSupplier.get(),
                primaryKeyEncoder,
                bucketKeyEncoder,
                lakeTableBucketAssigner,
                partitionGetter,
                kvValueDecoder);
    }

    @Override
    public PrefixLookuper getPrefixLookuper(PrefixLookup prefixLookup) {
        validatePrefixLookup(prefixLookup);
        maybeCreateLakeTableBucketAssigner();

        RowType prefixKeyRowType =
                rowType.project(
                        schema.getColumnIndexes(
                                Arrays.asList(prefixLookup.getLookupColumnNames())));
        PartitionGetter partitionGetter =
                partitionKeyNames.size() > 0
                        ? new PartitionGetter(prefixKeyRowType, partitionKeyNames)
                        : null;
        checkArgument(bucketKeyRowType != null, "bucketKeyRowType shouldn't be null.");
        KeyEncoder bucketKeyEncoder =
                KeyEncoder.createKeyEncoder(
                        prefixKeyRowType, bucketKeyRowType.getFieldNames(), partitionKeyNames);
        return new FlussPrefixLookuper(
                tableInfo,
                numBuckets,
                metadataUpdater,
                lookupClientSupplier.get(),
                bucketKeyEncoder,
                lakeTableBucketAssigner,
                partitionGetter,
                kvValueDecoder);
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

    private void maybeCreateLakeTableBucketAssigner() {
        if (lakeTableBucketAssigner == null) {
            synchronized (this) {
                if (lakeTableBucketAssigner == null) {
                    lakeTableBucketAssigner =
                            new LakeTableBucketAssigner(
                                    primaryKeyRowType,
                                    tableInfo.getTableDescriptor().getBucketKey(),
                                    numBuckets);
                }
            }
        }
    }

    private void validatePrefixLookup(PrefixLookup prefixLookup) {
        // 1. verify table descriptor.
        if (!hasPrimaryKey) {
            throw new FlussRuntimeException(
                    String.format("None-pk table %s don't support prefix lookup", tablePath));
        }

        checkArgument(bucketKeyRowType != null, "bucketKeyRowType shouldn't be null.");
        List<String> pkRemovePartitionFields = primaryKeyRowType.getFieldNames();
        pkRemovePartitionFields.removeAll(partitionKeyNames);
        List<String> bucketKeyNames = bucketKeyRowType.getFieldNames();

        for (int i = 0; i < bucketKeyNames.size(); i++) {
            if (!bucketKeyNames.get(i).equals(pkRemovePartitionFields.get(i))) {
                throw new FlussRuntimeException(
                        String.format(
                                "To do prefix lookup, the bucket keys must be the prefix subset of "
                                        + "primary keys exclude partition fields (if partition table), but "
                                        + "the bucket keys are %s and the primary keys are %s and the primary "
                                        + "key exclude partition fields are %s for table %s",
                                bucketKeyNames,
                                primaryKeyRowType.getFieldNames(),
                                pkRemovePartitionFields,
                                tablePath));
            }
        }

        // verify PrefixLookup.
        String[] prefixLookupColumns = prefixLookup.getLookupColumnNames();
        List<String> prefixLookupColumnList = new ArrayList<>(Arrays.asList(prefixLookupColumns));
        if (!partitionKeyNames.isEmpty()) {
            for (int i = 0; i < partitionKeyNames.size(); i++) {
                if (!prefixLookupColumnList.contains(partitionKeyNames.get(i))) {
                    throw new FlussRuntimeException(
                            String.format(
                                    "To do prefix lookup for partitioned primary key table, the "
                                            + "partition keys must be in lookup columns, but the lookup "
                                            + "columns are %s and the partition keys are %s for table %s",
                                    prefixLookupColumnList, partitionKeyNames, tablePath));
                }
            }
            prefixLookupColumnList.removeAll(partitionKeyNames);
        }
        checkArgument(
                prefixLookupColumnList.size() == bucketKeyNames.size()
                        && Arrays.equals(
                                prefixLookupColumnList.toArray(new String[0]),
                                bucketKeyNames.toArray(new String[0])),
                "To do prefix lookup, the lookup columns must be the bucket key with "
                        + "partition fields (if partition table), but the lookup columns are %s and the "
                        + "bucket keys are %s for table %s",
                Arrays.asList(prefixLookup.getLookupColumnNames()),
                bucketKeyNames,
                tablePath);
    }
}
