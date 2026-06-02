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

package org.apache.fluss.client.table.writer;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.client.table.getter.PartitionGetter;
import org.apache.fluss.client.write.WriteFormat;
import org.apache.fluss.client.write.WriteRecord;
import org.apache.fluss.client.write.WriterClient;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.CompactedRowEncoder;
import org.apache.fluss.row.encode.IndexedRowEncoder;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.fluss.utils.Preconditions.checkNotNull;
import static org.apache.fluss.utils.Preconditions.checkState;

/** Default implementation of {@link MultiTableWriter}. */
@Internal
class MultiTableWriterImpl implements MultiTableWriter {

    private final MetadataUpdater metadataUpdater;
    private final Admin admin;
    /** Shared with other writers on the same {@link org.apache.fluss.client.FlussConnection}. */
    private final WriterClient writerClient;

    /** Per-table encoder states keyed by schemaId, supporting writes against historical schemas. */
    private final Map<TablePath, Map<Integer, TableWriteState>> states = new HashMap<>();

    /** The latest known schemaId per table, for fast-path lookup of the current encoder state. */
    private final Map<TablePath, Integer> latestSchemaIdByPath = new HashMap<>();

    private volatile boolean closed;

    MultiTableWriterImpl(MetadataUpdater metadataUpdater, Admin admin, WriterClient writerClient) {
        this.metadataUpdater = checkNotNull(metadataUpdater, "metadataUpdater");
        this.admin = checkNotNull(admin, "admin");
        this.writerClient = checkNotNull(writerClient, "writerClient");
    }

    @Override
    public CompletableFuture<WriteResult> write(MultiTableWriteRecord record) {
        checkNotNull(record, "record");
        checkState(!closed, "MultiTableWriter is already closed.");

        TablePath tablePath = record.getTablePath();
        ChangeType changeType = record.getChangeType();
        InternalRow row = record.getRow();

        // UPDATE_BEFORE is a no-op (consistent with Flink changelog semantics).
        if (changeType == ChangeType.UPDATE_BEFORE) {
            return CompletableFuture.completedFuture(new WriteResult());
        }

        final WriteRecord writeRecord;
        try {
            // refresh schema if record's schemaId changed.
            TableWriteState state =
                    resolveStateAndRefreshSchemaIfChanged(tablePath, record.getSchemaId());
            state.checkFieldCount(row);
            writeRecord = state.build(changeType, row);
        } catch (Exception e) {
            CompletableFuture<WriteResult> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }

        CompletableFuture<WriteResult> future = new CompletableFuture<>();
        writerClient.send(
                writeRecord,
                (bucket, logEndOffset, exception) -> {
                    if (exception == null) {
                        future.complete(new WriteResult());
                    } else {
                        future.completeExceptionally(exception);
                    }
                });
        return future;
    }

    @Override
    public void flush() {
        writerClient.flush();
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }

        try {
            writerClient.flush();
        } finally {
            // writerClient is a connection-level shared resource and is NOT closed here.
            // Drop per-table encoder state to help GC on long-lived connections that may
            // create and close many MultiTableWriter instances.
            states.clear();
            latestSchemaIdByPath.clear();
            closed = true;
        }
    }

    /**
     * Resolve the per-table encoding state, honoring the schemaId carried by the record.
     *
     * <ul>
     *   <li>If the record's schemaId matches the cached latest schema, return that state directly.
     *   <li>Otherwise {@link WriterClient#flush() flush} all pending records (encoded against the
     *       old schema) to the server, then refresh metadata to learn the current latest schema.
     *   <li>If the record's schemaId is still ahead of the refreshed latest schemaId (i.e., the
     *       caller is referencing a future / unknown schema), fail fast.
     *   <li>If the record's schemaId equals the refreshed latest schemaId, use the latest state.
     *   <li>If the record's schemaId is behind the refreshed latest schemaId, build (or reuse) an
     *       encoding state for that historical schema so the row is encoded and sent against the
     *       exact schema it was produced with.
     * </ul>
     */
    private TableWriteState resolveStateAndRefreshSchemaIfChanged(
            TablePath tablePath, int recordSchemaId) {
        TableWriteState latest = resolveLatestState(tablePath, false);
        if (recordSchemaId == latest.tableInfo.getSchemaId()) {
            return latest;
        }

        writerClient.flush();
        latest = resolveLatestState(tablePath, true);
        int latestSchemaId = latest.tableInfo.getSchemaId();
        if (recordSchemaId > latestSchemaId) {
            throw new IllegalArgumentException(
                    "SchemaId mismatch for table "
                            + tablePath
                            + ": record schemaId="
                            + recordSchemaId
                            + " is ahead of the current table schemaId="
                            + latestSchemaId
                            + " (after metadata refresh).");
        }
        if (recordSchemaId == latestSchemaId) {
            return latest;
        }
        // recordSchemaId < latestSchemaId: encode against the historical schema the row was
        // produced with.
        return resolveHistoricalState(tablePath, latest.tableInfo, recordSchemaId);
    }

    /**
     * Resolve the encoding state for the table's latest schema. When {@code forceUpdate} is true,
     * force-refresh metadata to avoid stale schema, matching {@code FlussConnection.getTable(...)}.
     */
    private TableWriteState resolveLatestState(TablePath tablePath, boolean forceUpdate) {
        Integer latestSchemaId = latestSchemaIdByPath.get(tablePath);
        if (latestSchemaId != null && !forceUpdate) {
            return states.get(tablePath).get(latestSchemaId);
        }
        metadataUpdater.updateTableOrPartitionMetadata(tablePath, null);
        TableInfo info = getTableInfo(tablePath);
        TableWriteState state = TableWriteState.forTable(tablePath, info);
        cacheState(tablePath, info.getSchemaId(), state);
        latestSchemaIdByPath.put(tablePath, info.getSchemaId());
        return state;
    }

    /**
     * Resolve (or build and cache) the encoding state for a historical {@code schemaId}. The
     * historical {@link TableInfo} reuses every non-schema attribute of the latest table info and
     * only swaps in the historical schema / schemaId, so the produced {@link WriteRecord} carries
     * the record's original schemaId (see {@link WriteRecord#getSchemaId()}).
     */
    private TableWriteState resolveHistoricalState(
            TablePath tablePath, TableInfo latestTableInfo, int schemaId) {
        Map<Integer, TableWriteState> bySchema = states.get(tablePath);
        if (bySchema != null) {
            TableWriteState cached = bySchema.get(schemaId);
            if (cached != null) {
                return cached;
            }
        }
        Schema historicalSchema = getTableSchema(tablePath, schemaId);
        TableInfo historicalTableInfo = withSchema(latestTableInfo, schemaId, historicalSchema);
        TableWriteState state = TableWriteState.forTable(tablePath, historicalTableInfo);
        cacheState(tablePath, schemaId, state);
        return state;
    }

    private void cacheState(TablePath tablePath, int schemaId, TableWriteState state) {
        states.computeIfAbsent(tablePath, k -> new HashMap<>()).put(schemaId, state);
    }

    /**
     * Build a {@link TableInfo} for a historical schema by reusing every non-schema attribute of
     * {@code base}. This assumes schema evolution does not change bucket keys / partition keys /
     * bucket count (compatible evolution such as adding columns); otherwise encoding would fail
     * fast downstream.
     */
    private static TableInfo withSchema(TableInfo base, int schemaId, Schema schema) {
        return new TableInfo(
                base.getTablePath(),
                base.getTableId(),
                schemaId,
                schema,
                base.getBucketKeys(),
                base.getPartitionKeys(),
                base.getNumBuckets(),
                base.getProperties(),
                base.getCustomProperties(),
                base.getRemoteDataDir(),
                base.getComment().orElse(null),
                base.getCreatedTime(),
                base.getModifiedTime());
    }

    private TableInfo getTableInfo(TablePath tablePath) {
        try {
            return admin.getTableInfo(tablePath).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while resolving table info: " + tablePath, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException("Failed to resolve table info: " + tablePath, cause);
        }
    }

    private Schema getTableSchema(TablePath tablePath, int schemaId) {
        try {
            return admin.getTableSchema(tablePath, schemaId).get().getSchema();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while resolving table schema: " + tablePath, e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException("Failed to resolve table schema: " + tablePath, cause);
        }
    }

    /**
     * Per-table encoding state, lazily constructed on first write. Mirrors the encoder setup done
     * in the constructors of {@link AppendWriterImpl} and {@link UpsertWriterImpl}. Sub-classed for
     * log tables ({@link LogTableWriteState}) and primary-key tables ({@link
     * PrimaryTableWriteState}) so that each path only carries the encoders it actually uses, and
     * the {@link ChangeType} dispatch is owned by the table-shape that supports it.
     */
    private abstract static class TableWriteState {
        final TablePath tablePath;
        final TableInfo tableInfo;
        final int fieldCount;
        @Nullable final PartitionGetter partitionGetter;
        final FieldGetter[] fieldGetters;

        TableWriteState(TablePath tablePath, TableInfo tableInfo) {
            this.tablePath = tablePath;
            this.tableInfo = tableInfo;
            RowType rowType = tableInfo.getRowType();
            this.fieldCount = rowType.getFieldCount();
            this.partitionGetter =
                    tableInfo.isPartitioned()
                            ? new PartitionGetter(rowType, tableInfo.getPartitionKeys())
                            : null;
            this.fieldGetters = InternalRow.createFieldGetters(rowType);
        }

        /** Factory: pick the right shape based on whether the table has a primary key. */
        static TableWriteState forTable(TablePath tablePath, TableInfo tableInfo) {
            return tableInfo.hasPrimaryKey()
                    ? new PrimaryTableWriteState(tablePath, tableInfo)
                    : new LogTableWriteState(tablePath, tableInfo);
        }

        void checkFieldCount(InternalRow row) {
            if (row.getFieldCount() != fieldCount) {
                throw new IllegalArgumentException(
                        "The field count of the row does not match the table schema for "
                                + tablePath
                                + ". Expected: "
                                + fieldCount
                                + ", Actual: "
                                + row.getFieldCount());
            }
        }

        PhysicalTablePath getPhysicalPath(InternalRow row) {
            if (partitionGetter == null) {
                return PhysicalTablePath.of(tablePath);
            }
            return PhysicalTablePath.of(tablePath, partitionGetter.getPartition(row));
        }

        /**
         * Encode the row into a {@link WriteRecord} for the given {@link ChangeType}. Throws {@link
         * IllegalArgumentException} if the change type is not supported by the underlying table
         * shape.
         */
        abstract WriteRecord build(ChangeType changeType, InternalRow row);
    }

    /** Encoding state for a log (non-PK) table; only supports APPEND_ONLY / INSERT. */
    private static final class LogTableWriteState extends TableWriteState {
        final LogFormat logFormat;
        /**
         * Per-row encoder for the {@link #logFormat}. Only initialized for {@link
         * LogFormat#INDEXED} and {@link LogFormat#COMPACTED}; {@code null} for {@link
         * LogFormat#ARROW} which accepts a general {@link InternalRow} directly.
         */
        @Nullable final RowEncoder rowEncoder;

        @Nullable final KeyEncoder logBucketKeyEncoder;

        LogTableWriteState(TablePath tablePath, TableInfo tableInfo) {
            super(tablePath, tableInfo);
            RowType rowType = tableInfo.getRowType();
            if (tableInfo.getBucketKeys().isEmpty()) {
                this.logBucketKeyEncoder = null;
            } else {
                this.logBucketKeyEncoder =
                        KeyEncoder.ofBucketKeyEncoder(
                                rowType,
                                tableInfo.getBucketKeys(),
                                tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
            }
            this.logFormat = tableInfo.getTableConfig().getLogFormat();
            if (logFormat == LogFormat.INDEXED) {
                this.rowEncoder = new IndexedRowEncoder(rowType);
            } else if (logFormat == LogFormat.COMPACTED) {
                DataType[] fieldDataTypes =
                        tableInfo.getSchema().getRowType().getChildren().toArray(new DataType[0]);
                this.rowEncoder = new CompactedRowEncoder(fieldDataTypes);
            } else {
                // ARROW format accepts a general InternalRow; no per-row encoder needed.
                this.rowEncoder = null;
            }
        }

        @Override
        WriteRecord build(ChangeType changeType, InternalRow row) {
            switch (changeType) {
                case APPEND_ONLY:
                case INSERT:
                    return buildAppendRecord(row);
                default:
                    throw new IllegalArgumentException(
                            "ChangeType "
                                    + changeType
                                    + " is not supported for log table "
                                    + tablePath
                                    + "; only APPEND_ONLY and INSERT are allowed.");
            }
        }

        WriteRecord buildAppendRecord(InternalRow row) {
            PhysicalTablePath physicalPath = getPhysicalPath(row);
            byte[] bucketKey =
                    logBucketKeyEncoder != null ? logBucketKeyEncoder.encodeKey(row) : null;
            if (logFormat == LogFormat.INDEXED) {
                IndexedRow indexedRow =
                        row instanceof IndexedRow ? (IndexedRow) row : (IndexedRow) encodeRow(row);
                return WriteRecord.forIndexedAppend(tableInfo, physicalPath, indexedRow, bucketKey);
            } else if (logFormat == LogFormat.COMPACTED) {
                CompactedRow compactedRow =
                        row instanceof CompactedRow
                                ? (CompactedRow) row
                                : (CompactedRow) encodeRow(row);
                return WriteRecord.forCompactedAppend(
                        tableInfo, physicalPath, compactedRow, bucketKey);
            } else {
                // ARROW format accepts a general InternalRow.
                return WriteRecord.forArrowAppend(tableInfo, physicalPath, row, bucketKey);
            }
        }

        private BinaryRow encodeRow(InternalRow row) {
            assert rowEncoder != null;
            rowEncoder.startNewRow();
            for (int i = 0; i < fieldCount; i++) {
                rowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
            }
            return rowEncoder.finishRow();
        }
    }

    /**
     * Encoding state for a primary-key (KV) table; supports INSERT / UPDATE_AFTER / DELETE.
     * UPDATE_BEFORE is filtered out by {@link MultiTableWriterImpl#write(MultiTableWriteRecord)}
     * before reaching this class.
     */
    private static final class PrimaryTableWriteState extends TableWriteState {
        final KeyEncoder primaryKeyEncoder;
        final KeyEncoder kvBucketKeyEncoder;
        final KvFormat kvFormat;
        final WriteFormat kvWriteFormat;
        final RowEncoder kvRowEncoder;

        PrimaryTableWriteState(TablePath tablePath, TableInfo tableInfo) {
            super(tablePath, tableInfo);
            RowType rowType = tableInfo.getRowType();
            this.primaryKeyEncoder =
                    KeyEncoder.ofPrimaryKeyEncoder(
                            rowType,
                            tableInfo.getPhysicalPrimaryKeys(),
                            tableInfo.getTableConfig(),
                            tableInfo.isDefaultBucketKey());
            this.kvBucketKeyEncoder =
                    tableInfo.isDefaultBucketKey()
                            ? this.primaryKeyEncoder
                            : KeyEncoder.ofBucketKeyEncoder(
                                    rowType,
                                    tableInfo.getBucketKeys(),
                                    tableInfo.getTableConfig().getDataLakeFormat().orElse(null));
            this.kvFormat = tableInfo.getTableConfig().getKvFormat();
            this.kvWriteFormat = WriteFormat.fromKvFormat(this.kvFormat);
            this.kvRowEncoder = RowEncoder.create(this.kvFormat, rowType);
        }

        @Override
        WriteRecord build(ChangeType changeType, InternalRow row) {
            switch (changeType) {
                case APPEND_ONLY:
                case INSERT:
                case UPDATE_AFTER:
                    return buildUpsertRecord(row);
                case DELETE:
                    return buildDeleteRecord(row);
                default:
                    // UPDATE_BEFORE has already been handled by the writer entry point.
                    throw new IllegalArgumentException(
                            "Unsupported ChangeType "
                                    + changeType
                                    + " for primary-key table "
                                    + tablePath);
            }
        }

        WriteRecord buildUpsertRecord(InternalRow row) {
            byte[] key = primaryKeyEncoder.encodeKey(row);
            return WriteRecord.forUpsert(
                    tableInfo,
                    getPhysicalPath(row),
                    encodeKvRow(row),
                    key,
                    encodeBucketKey(row, key),
                    kvWriteFormat,
                    null /*targetColumns*/,
                    MergeMode.DEFAULT);
        }

        WriteRecord buildDeleteRecord(InternalRow row) {
            byte[] key = primaryKeyEncoder.encodeKey(row);
            return WriteRecord.forDelete(
                    tableInfo,
                    getPhysicalPath(row),
                    key,
                    encodeBucketKey(row, key),
                    kvWriteFormat,
                    null /*targetColumns*/,
                    MergeMode.DEFAULT);
        }

        /**
         * When the bucket key equals the primary key (default-bucket-key tables), reuse the already
         * encoded primary key bytes to avoid a second encode pass.
         */
        private byte[] encodeBucketKey(InternalRow row, byte[] primaryKey) {
            return kvBucketKeyEncoder == primaryKeyEncoder
                    ? primaryKey
                    : kvBucketKeyEncoder.encodeKey(row);
        }

        private BinaryRow encodeKvRow(InternalRow row) {
            if (kvFormat == KvFormat.INDEXED && row instanceof IndexedRow) {
                return (IndexedRow) row;
            } else if (kvFormat == KvFormat.COMPACTED && row instanceof CompactedRow) {
                return (CompactedRow) row;
            }
            kvRowEncoder.startNewRow();
            for (int i = 0; i < fieldCount; i++) {
                kvRowEncoder.encodeField(i, fieldGetters[i].getFieldOrNull(row));
            }
            return kvRowEncoder.finishRow();
        }
    }

    // ----- test hooks -----

    /** For tests: number of resolved tables (lazy registration count). */
    int resolvedTableCount() {
        return states.size();
    }
}
