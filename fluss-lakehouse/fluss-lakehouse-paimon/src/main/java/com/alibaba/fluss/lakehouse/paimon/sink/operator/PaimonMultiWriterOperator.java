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

package com.alibaba.fluss.lakehouse.paimon.sink.operator;

import com.alibaba.fluss.lakehouse.paimon.record.CdcRecord;
import com.alibaba.fluss.lakehouse.paimon.record.MultiplexCdcRecord;
import com.alibaba.fluss.lakehouse.paimon.sink.committable.FlussLogOffsetCommittable;
import com.alibaba.fluss.lakehouse.paimon.sink.row.CdcRecordWrapper;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.utils.concurrent.ExecutorThreadFactory;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.PrepareCommitOperator;
import org.apache.paimon.flink.sink.StateUtils;
import org.apache.paimon.flink.sink.StoreSinkWrite;
import org.apache.paimon.flink.sink.StoreSinkWriteState;
import org.apache.paimon.flink.sink.StoreSinkWriteStateImpl;
import org.apache.paimon.flink.sink.cdc.CdcRecordStoreMultiWriteOperator;
import org.apache.paimon.memory.HeapMemorySegmentPool;
import org.apache.paimon.memory.MemoryPoolFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.alibaba.fluss.utils.Preconditions.checkArgument;

/* This file is based on source code of Apache Paimon Project (https://paimon.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * Most is coped from {@link CdcRecordStoreMultiWriteOperator}, but this writer operator can record
 * the log start/end offset of the record written and take it as committable.
 */
public class PaimonMultiWriterOperator
        extends PrepareCommitOperator<MultiplexCdcRecord, MultiTableCommittable> {

    private static final long serialVersionUID = 1L;

    private final StoreSinkWrite.WithWriteBufferProvider storeSinkWriteProvider;
    private final String initialCommitUser;
    private final CatalogLoader catalogLoader;

    private MemoryPoolFactory memoryPoolFactory;

    private String commitUser;

    private Catalog catalog;
    // fluss table id -> paimon table
    private Map<Long, FileStoreTable> tables;
    // fluss table id -> paimon table identifier
    private Map<Long, Identifier> tablePathById;

    private StoreSinkWriteState state;
    // fluss table id -> paimon store sink write
    private Map<Long, StoreSinkWrite> writes;

    private Map<Long, String> partitionNameById;
    private Map<Long, Map<TableBucket, Long>> currentLogEndOffsets;

    private ExecutorService compactExecutor;

    public PaimonMultiWriterOperator(
            CatalogLoader catalogLoader,
            StoreSinkWrite.WithWriteBufferProvider storeSinkWriteProvider,
            String initialCommitUser,
            StreamOperatorParameters<MultiTableCommittable> parameters,
            Options options) {
        super(parameters, options);
        this.catalogLoader = catalogLoader;
        this.storeSinkWriteProvider = storeSinkWriteProvider;
        this.initialCommitUser = initialCommitUser;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        catalog = catalogLoader.load();

        // Each job can only have one user name and this name must be consistent across restarts.
        // We cannot use job id as commit user name here because user may change job id by creating
        // a savepoint, stop the job and then resume from savepoint.
        commitUser =
                StateUtils.getSingleValueFromState(
                        context, "commit_user_state", String.class, initialCommitUser);

        // TODO: should use CdcRecordMultiChannelComputer to filter
        state = new StoreSinkWriteStateImpl(context, (tableName, partition, bucket) -> true);
        tables = new HashMap<>();
        writes = new HashMap<>();
        tablePathById = new HashMap<>();

        currentLogEndOffsets = new HashMap<>();
        partitionNameById = new HashMap<>();

        compactExecutor =
                Executors.newSingleThreadScheduledExecutor(
                        new ExecutorThreadFactory(
                                Thread.currentThread().getName() + "-MultiWrite-Compaction"));
    }

    @Override
    public void processElement(StreamRecord<MultiplexCdcRecord> streamRecord) throws Exception {
        MultiplexCdcRecord multiplexCdcRecord = streamRecord.getValue();

        TableBucket tableBucket = multiplexCdcRecord.getTableBucket();

        long tableId = tableBucket.getTableId();
        int bucket = tableBucket.getBucket();

        CdcRecord cdcRecord = multiplexCdcRecord.getCdcRecord();

        FileStoreTable table = getTable(tableId, multiplexCdcRecord.getTablePath());

        // all table write should share one write buffer so that writers can preempt memory
        // from those of other tables
        if (memoryPoolFactory == null) {
            memoryPoolFactory =
                    new MemoryPoolFactory(
                            memoryPool != null
                                    ? memoryPool
                                    // currently, the options of all tables are the same in CDC
                                    : new HeapMemorySegmentPool(
                                            table.coreOptions().writeBufferSize(),
                                            table.coreOptions().pageSize()));
        }

        StoreSinkWrite write =
                writes.computeIfAbsent(
                        tableId,
                        id ->
                                storeSinkWriteProvider.provide(
                                        table,
                                        commitUser,
                                        state,
                                        getContainingTask().getEnvironment().getIOManager(),
                                        memoryPoolFactory,
                                        getMetricGroup()));

        updateCurrentLogOffset(multiplexCdcRecord);
        try {
            InternalRow paimonRow = toPaimonRow(cdcRecord);
            if (table.bucketMode() == BucketMode.BUCKET_UNAWARE) {
                // unaware bucket mode
                write.write(paimonRow);
            } else {
                // write to specific bucket
                write.write(paimonRow, bucket);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void updateCurrentLogOffset(MultiplexCdcRecord multiplexCdcRecord) throws Exception {
        TableBucket tableBucket = multiplexCdcRecord.getTableBucket();
        TablePath tablePath = multiplexCdcRecord.getTablePath();
        long offset = multiplexCdcRecord.getOffset();
        // we always assume the offset is in asc order
        // so, log end offset should be updated always,
        // but log start offset should be updated only once
        Map<TableBucket, Long> bucketLogEndOffset =
                currentLogEndOffsets.computeIfAbsent(
                        tableBucket.getTableId(), id -> new HashMap<>());
        bucketLogEndOffset.put(tableBucket, offset);

        // update partition name by id
        if (tableBucket.getPartitionId() != null) {
            Long partitionId = tableBucket.getPartitionId();
            if (!partitionNameById.containsKey(partitionId)) {
                // try to get partition name
                FileStoreTable fileStoreTable = getTable(tableBucket.getTableId(), tablePath);
                TableSchema schema = fileStoreTable.schema();
                List<String> partitionKeys = schema.partitionKeys();
                checkArgument(
                        partitionKeys.size() == 1,
                        String.format(
                                "Paimon table: %s must be with only 1 partition key, but got %s, the partitions keys are: %s.",
                                tablePath, partitionKeys.size(), partitionKeys));
                String partition = partitionKeys.get(0);
                int partitionColIndex = schema.fieldNames().indexOf(partition);
                checkArgument(
                        partitionColIndex >= 0,
                        String.format(
                                "Partition column %s not found in table %s, the columns of the table is %s.",
                                partition, tablePath, schema.fieldNames()));
                RowData rowData = multiplexCdcRecord.getCdcRecord().getRowData();
                // currently, only partition column with string type is supported, so must be string
                partitionNameById.put(partitionId, rowData.getString(partitionColIndex).toString());
            }
        }
    }

    @Override
    protected List<MultiTableCommittable> prepareCommit(boolean waitCompaction, long checkpointId)
            throws IOException {
        List<MultiTableCommittable> committables = new LinkedList<>();
        for (Map.Entry<Long, StoreSinkWrite> entry : writes.entrySet()) {
            Identifier tableIdentifier = getIdentifier(entry.getKey());
            StoreSinkWrite write = entry.getValue();
            List<MultiTableCommittable> perTableCommittable =
                    write.prepareCommit(waitCompaction, checkpointId).stream()
                            .map(
                                    committable ->
                                            MultiTableCommittable.fromCommittable(
                                                    tableIdentifier, committable))
                            .collect(Collectors.toList());
            committables.addAll(perTableCommittable);
        }

        // collect start offset & end offset to commit
        for (Map.Entry<Long, Map<TableBucket, Long>> logEndOffsetEntry :
                currentLogEndOffsets.entrySet()) {
            long tableId = logEndOffsetEntry.getKey();
            Identifier paimonIdentifier = getIdentifier(tableId);
            Map<TableBucket, Long> bucketLogEndOffsets =
                    new HashMap<>(logEndOffsetEntry.getValue());
            Map<Long, String> currentPartitionNameById = new HashMap<>();

            // if the table is partitioned, need to pass the mapping
            // from partition id to partition name to committer
            if (!tables.get(tableId).partitionKeys().isEmpty()) {
                bucketLogEndOffsets
                        .keySet()
                        .forEach(
                                bucket ->
                                        currentPartitionNameById.put(
                                                bucket.getPartitionId(),
                                                partitionNameById.get(bucket.getPartitionId())));
            }
            committables.add(
                    MultiTableCommittable.fromCommittable(
                            paimonIdentifier,
                            new Committable(
                                    checkpointId,
                                    Committable.Kind.LOG_OFFSET,
                                    new FlussLogOffsetCommittable(
                                            tableId,
                                            currentPartitionNameById,
                                            bucketLogEndOffsets))));
        }

        currentLogEndOffsets.clear();
        return committables;
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        for (StoreSinkWrite write : writes.values()) {
            write.snapshotState();
        }

        state.snapshotState();
    }

    @Override
    public void close() throws Exception {
        super.close();
        for (StoreSinkWrite write : writes.values()) {
            write.close();
        }
        if (compactExecutor != null) {
            compactExecutor.shutdownNow();
        }
    }

    private FileStoreTable getTable(long tableId, TablePath tablePath) throws Exception {
        FileStoreTable table = tables.get(tableId);
        if (table == null) {
            Identifier paimonIdentifier = toIdentifier(tablePath);
            table = (FileStoreTable) catalog.getTable(paimonIdentifier);
            tables.put(tableId, table);
            tablePathById.put(tableId, paimonIdentifier);
        }
        return table;
    }

    private InternalRow toPaimonRow(CdcRecord cdcRecord) {
        return new CdcRecordWrapper(cdcRecord);
    }

    private Identifier getIdentifier(long tableId) {
        return tablePathById.get(tableId);
    }

    private Identifier toIdentifier(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }
}
