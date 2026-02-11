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

package org.apache.fluss.server.kv;

import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.record.LogRecords;
import org.apache.fluss.record.MemoryLogRecords;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.encode.ValueEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.server.kv.autoinc.AutoIncIDRange;
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

import static org.apache.fluss.server.TabletManagerBase.getTableInfo;

/** A helper for recovering Kv from log. */
public class KvRecoverHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KvRecoverHelper.class);

    private final KvTablet kvTablet;
    private final LogTablet logTablet;
    private final long recoverPointOffset;
    @Nullable private final Long recoverPointRowCount;
    @Nullable private final AutoIncIDRange autoIncRange;
    private final KvRecoverContext recoverContext;
    private final KvFormat kvFormat;
    private final LogFormat logFormat;

    // will be initialized when first encounter a log record during recovering from log
    private Integer currentSchemaId;
    private RowType currentRowType;

    private KeyEncoder keyEncoder;
    private RowEncoder rowEncoder;
    private final SchemaGetter schemaGetter;

    private InternalRow.FieldGetter[] currentFieldGetters;

    public KvRecoverHelper(
            KvTablet kvTablet,
            LogTablet logTablet,
            long recoverPointOffset,
            @Nullable Long recoverPointRowCount,
            @Nullable AutoIncIDRange autoIncRange,
            KvRecoverContext recoverContext,
            KvFormat kvFormat,
            LogFormat logFormat,
            SchemaGetter schemaGetter) {
        this.kvTablet = kvTablet;
        this.logTablet = logTablet;
        this.recoverPointOffset = recoverPointOffset;
        this.recoverPointRowCount = recoverPointRowCount;
        this.autoIncRange = autoIncRange;
        this.recoverContext = recoverContext;
        this.kvFormat = kvFormat;
        this.logFormat = logFormat;
        this.schemaGetter = schemaGetter;
    }

    public void recover() throws Exception {
        // first step: read to high watermark and apply them to kv directly; that
        // 's for the data acked

        // second step: read from high watermark to log end offset which is not acked, and write
        // them into pre-write buffer to make the data in kv(underlying kv + pre-write buffer)
        // align with the local log;
        // the data in pre-write will be flush
        // after the corresponding log offset is acked(when high watermark is advanced to the
        // offset)

        initSchema(schemaGetter.getLatestSchemaInfo().getSchemaId());

        Schema schema = schemaGetter.getLatestSchemaInfo().getSchema();

        long nextLogOffset = recoverPointOffset;
        RowCountUpdater rowCountUpdater =
                recoverPointRowCount != null
                        ? new RowCountUpdaterImpl(recoverPointRowCount)
                        : new NoOpRowCountUpdater();
        AutoIncIDRangeUpdater autoIncIdRangeUpdater =
                autoIncRange != null
                        ? new AutoIncIDRangeUpdaterImpl(
                                schema, autoIncRange, kvTablet.getAutoIncrementCacheSize())
                        : new NoOpAutoIncIDRangeUpdater();
        // read to high watermark
        try (KvBatchWriter kvBatchWriter = kvTablet.createKvBatchWriter()) {
            ThrowingConsumer<KeyValueAndLogOffset, Exception> resumeRecordApplier =
                    (resumeRecord) -> {
                        if (resumeRecord.value == null) {
                            kvBatchWriter.delete(resumeRecord.key);
                        } else {
                            kvBatchWriter.put(resumeRecord.key, resumeRecord.value);
                        }
                    };

            nextLogOffset =
                    readLogRecordsAndApply(
                            nextLogOffset,
                            rowCountUpdater,
                            autoIncIdRangeUpdater,
                            FetchIsolation.HIGH_WATERMARK,
                            resumeRecordApplier);
        }

        // the all data up to nextLogOffset has been flush into kv
        kvTablet.setFlushedLogOffset(nextLogOffset);
        // if we have valid row count (means this table supports row count),
        // update the row count in kv tablet
        if (recoverPointRowCount != null) {
            kvTablet.setRowCount(rowCountUpdater.getRowCount());
            LOG.info(
                    "Updated row count to {} for table '{}' after recovering from log",
                    rowCountUpdater.getRowCount(),
                    kvTablet.getTablePath());
        } else {
            LOG.info(
                    "Skipping row count update after recovering from log, because this table '{}' doesn't support row count.",
                    kvTablet.getTablePath());
        }

        // read to log end offset
        ThrowingConsumer<KeyValueAndLogOffset, Exception> resumeRecordApplier =
                (resumeRecord) ->
                        kvTablet.putToPreWriteBuffer(
                                resumeRecord.key, resumeRecord.value, resumeRecord.logOffset);
        readLogRecordsAndApply(
                nextLogOffset,
                // records in pre-write-buffer shouldn't affect the row count, the high-watermark
                // of pre-write-buffer records will update the row-count async
                new NoOpRowCountUpdater(),
                // we should update auto-inc-id for pre-write-buffer, because id has been used.
                autoIncIdRangeUpdater,
                FetchIsolation.LOG_END,
                resumeRecordApplier);

        if (autoIncRange != null) {
            AutoIncIDRange newRange = autoIncIdRangeUpdater.getNewRange();
            kvTablet.updateAutoIncrementIDRange(newRange);
            LOG.info(
                    "Updated auto inc id range to [{}, {}] for table '{}' after recovering from log",
                    newRange.getStart(),
                    newRange.getEnd(),
                    kvTablet.getTablePath());
        }
    }

    private long readLogRecordsAndApply(
            long startFetchOffset,
            RowCountUpdater rowCountUpdater,
            AutoIncIDRangeUpdater autoIncIdRangeUpdater,
            FetchIsolation fetchIsolation,
            ThrowingConsumer<KeyValueAndLogOffset, Exception> resumeRecordConsumer)
            throws Exception {
        try (LogRecordReadContext readContext = createLogRecordReadContext()) {
            long nextFetchOffset = startFetchOffset;
            while (true) {
                LogRecords logRecords =
                        logTablet
                                .read(
                                        nextFetchOffset,
                                        recoverContext.maxFetchLogSizeInRecoverKv,
                                        fetchIsolation,
                                        true,
                                        null)
                                .getRecords();
                if (logRecords == MemoryLogRecords.EMPTY) {
                    break;
                }

                for (LogRecordBatch logRecordBatch : logRecords.batches()) {
                    try (CloseableIterator<LogRecord> logRecordIter =
                            logRecordBatch.records(readContext)) {
                        while (logRecordIter.hasNext()) {
                            LogRecord logRecord = logRecordIter.next();
                            ChangeType changeType = logRecord.getChangeType();
                            rowCountUpdater.applyChange(changeType);

                            if (changeType != ChangeType.UPDATE_BEFORE) {
                                InternalRow logRow = logRecord.getRow();
                                byte[] key = keyEncoder.encodeKey(logRow);
                                byte[] value = null;
                                if (changeType != ChangeType.DELETE) {
                                    // the log row format may not compatible with kv row format,
                                    // e.g, arrow vs. compacted, thus needs a conversion here.
                                    BinaryRow row = toKvRow(logRow);
                                    value =
                                            ValueEncoder.encodeValue(
                                                    currentSchemaId.shortValue(), row);
                                }
                                resumeRecordConsumer.accept(
                                        new KeyValueAndLogOffset(
                                                key, value, logRecord.logOffset()));

                                // reuse the logRow instance which is usually a CompactedRow which
                                // has been deserialized during toKvRow(..)
                                autoIncIdRangeUpdater.applyRecord(changeType, logRow);
                            }
                        }
                    }
                    nextFetchOffset = logRecordBatch.nextLogOffset();
                }
            }
            return nextFetchOffset;
        }
    }

    private LogRecordReadContext createLogRecordReadContext() {
        if (logFormat == LogFormat.ARROW) {
            return LogRecordReadContext.createArrowReadContext(
                    currentRowType, currentSchemaId, schemaGetter);
        } else if (logFormat == LogFormat.COMPACTED) {
            return LogRecordReadContext.createCompactedRowReadContext(
                    currentRowType, currentSchemaId);
        } else {
            throw new UnsupportedOperationException("Unsupported log format: " + logFormat);
        }
    }

    // TODO: this is very in-efficient, because the conversion is CPU heavy. Should be optimized in
    //  the future.
    private BinaryRow toKvRow(InternalRow originalRow) {
        if (kvFormat == KvFormat.INDEXED) {
            // if the row is in indexed row format, just return the original row directly
            if (originalRow instanceof IndexedRow) {
                return (IndexedRow) originalRow;
            }
        }

        // then, we need to reconstruct the row
        rowEncoder.startNewRow();
        for (int i = 0; i < currentRowType.getFieldCount(); i++) {
            rowEncoder.encodeField(i, currentFieldGetters[i].getFieldOrNull(originalRow));
        }
        return rowEncoder.finishRow();
    }

    private void initSchema(int schemaId) throws Exception {
        // todo, may need a cache,
        // but now, we get the schema from zk
        TableInfo tableInfo = getTableInfo(recoverContext.zkClient, recoverContext.tablePath);
        // todo: we need to check the schema's table id is equal to the
        // kv tablet's table id or not. If not equal, it means other table with same
        // table path has been created, so the kv tablet's table is consider to be
        // deleted. We can ignore the restore operation
        currentRowType = schemaGetter.getSchema(schemaId).getRowType();
        DataType[] dataTypes = currentRowType.getChildren().toArray(new DataType[0]);
        currentSchemaId = schemaId;

        keyEncoder =
                KeyEncoder.ofPrimaryKeyEncoder(
                        tableInfo.getRowType(),
                        tableInfo.getPhysicalPrimaryKeys(),
                        tableInfo.getTableConfig(),
                        tableInfo.isDefaultBucketKey());
        rowEncoder = RowEncoder.create(kvFormat, dataTypes);
        currentFieldGetters = new InternalRow.FieldGetter[currentRowType.getFieldCount()];
        for (int i = 0; i < currentRowType.getFieldCount(); i++) {
            currentFieldGetters[i] = InternalRow.createFieldGetter(currentRowType.getTypeAt(i), i);
        }
    }

    private static final class KeyValueAndLogOffset {
        private final byte[] key;
        private final @Nullable byte[] value;
        private final long logOffset;

        public KeyValueAndLogOffset(byte[] key, byte[] value, long logOffset) {
            this.key = key;
            this.value = value;
            this.logOffset = logOffset;
        }
    }

    /** A context to provide necessary objects for kv recovering. */
    public static class KvRecoverContext {

        private final TablePath tablePath;

        private final ZooKeeperClient zkClient;
        private final int maxFetchLogSizeInRecoverKv;

        public KvRecoverContext(
                TablePath tablePath, ZooKeeperClient zkClient, int maxFetchLogSizeInRecoverKv) {
            this.tablePath = tablePath;
            this.zkClient = zkClient;
            this.maxFetchLogSizeInRecoverKv = maxFetchLogSizeInRecoverKv;
        }
    }

    // ------------------------------------------------------------------------------------------
    // Below are some helpers for recovering tablet state from log
    // ------------------------------------------------------------------------------------------

    /** A helper to update the latest row count during recovering from log. */
    private interface RowCountUpdater {

        /** Apply the change to the row count according to the change type. */
        void applyChange(ChangeType changeType);

        /** Get the latest row count. Returns -1 if this table doesn't support row count. */
        long getRowCount();
    }

    /**
     * A simple implementation of {@link RowCountUpdater} which maintains a row count by applying
     * the change type of each log record.
     */
    private static class RowCountUpdaterImpl implements RowCountUpdater {
        private long rowCount;

        public RowCountUpdaterImpl(long initialRowCount) {
            this.rowCount = initialRowCount;
        }

        @Override
        public void applyChange(ChangeType changeType) {
            if (changeType == ChangeType.INSERT || changeType == ChangeType.UPDATE_AFTER) {
                rowCount++;
            } else if (changeType == ChangeType.DELETE || changeType == ChangeType.UPDATE_BEFORE) {
                rowCount--;
            }
        }

        @Override
        public long getRowCount() {
            return rowCount;
        }
    }

    /**
     * A no-op implementation of {@link RowCountUpdater} for the table which doesn't support row
     * count.
     */
    private static class NoOpRowCountUpdater implements RowCountUpdater {

        @Override
        public void applyChange(ChangeType changeType) {
            // do nothing
        }

        @Override
        public long getRowCount() {
            return -1;
        }
    }

    /** A helper to update the auto inc id range during recovering from log. */
    private interface AutoIncIDRangeUpdater {
        /**
         * Apply the change to the auto inc id range according to the change type and log record.
         */
        void applyRecord(ChangeType changeType, InternalRow changelog);

        /**
         * Get a new auto inc id range for the next pre-write buffer. Returns null if this table
         * doesn't support auto inc id.
         */
        AutoIncIDRange getNewRange();
    }

    /**
     * A simple implementation of {@link AutoIncIDRangeUpdater} which maintains a auto inc id range
     * by applying the change type and log record of each log record. It assumes the auto inc column
     * is always increasing when new record inserted, and the auto inc column's value won't be
     * updated once it's inserted.
     */
    private static class AutoIncIDRangeUpdaterImpl implements AutoIncIDRangeUpdater {
        private final long autoIncrementCacheSize;
        private final boolean isLongType;
        private final int columnIndex;
        private final int columnId;
        private final long end;

        private long current;

        private AutoIncIDRangeUpdaterImpl(
                Schema schema, AutoIncIDRange autoIncRange, long autoIncrementCacheSize) {
            this.autoIncrementCacheSize = autoIncrementCacheSize;
            this.end = autoIncRange.getEnd();
            this.columnId = autoIncRange.getColumnId();
            this.columnIndex = autoIncColumnIndex(schema, autoIncRange.getColumnId());
            this.isLongType =
                    schema.getColumns().get(columnIndex).getDataType().is(DataTypeRoot.BIGINT);
            // initialize current to be one less than the start of the range
            this.current = autoIncRange.getStart() - 1;
        }

        private int autoIncColumnIndex(Schema schema, int columnId) {
            List<Schema.Column> columns = schema.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                if (columns.get(i).getColumnId() == columnId) {
                    return i;
                }
            }
            throw new IllegalArgumentException(
                    "Auto inc column with column id " + columnId + " not found in schema");
        }

        @Override
        public void applyRecord(ChangeType changeType, InternalRow changelog) {
            // only INSERT records will have new sequence id, and it must be not null
            if (changeType == ChangeType.INSERT) {
                if (isLongType) {
                    current = changelog.getLong(columnIndex);
                } else {
                    // currently, we only support long and int type for auto inc column, and they
                    // must be not null
                    current = changelog.getInt(columnIndex);
                }
            }
        }

        @Override
        public AutoIncIDRange getNewRange() {
            long newStart = current + 1;
            // id range is starting from [1, end], so if newStart is greater than end, it means we
            // have exhausted the id range, and the kv tablet has allocate a new range after the
            // snapshot, so we calculate the new range based on the current new start
            long newEnd = end;
            while (newStart > newEnd) {
                newEnd = newEnd + autoIncrementCacheSize;
            }
            return new AutoIncIDRange(columnId, newStart, newEnd);
        }
    }

    /**
     * A no-op implementation of {@link AutoIncIDRangeUpdater} for the table which doesn't support
     * auto inc column.
     */
    private static class NoOpAutoIncIDRangeUpdater implements AutoIncIDRangeUpdater {

        @Override
        public void applyRecord(ChangeType changeType, InternalRow changelog) {
            // do nothing
        }

        @Override
        public AutoIncIDRange getNewRange() {
            return null;
        }
    }
}
