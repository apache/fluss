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

import org.apache.fluss.exception.KvStorageException;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.TableInfo;
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
import org.apache.fluss.server.log.FetchIsolation;
import org.apache.fluss.server.replica.Replica;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.function.ThrowingConsumer;

import javax.annotation.Nullable;

import static org.apache.fluss.server.TabletManagerBase.getTableInfo;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** A helper for kv storage to apply log. */
public class KvApplyLogHelper {

    private final ZooKeeperClient zkClient;
    private final Replica replica;

    // will be initialized when first encounter a log record during recovering from log
    private Integer currentSchemaId;
    private RowType currentRowType;

    private KeyEncoder keyEncoder;
    private RowEncoder rowEncoder;

    private InternalRow.FieldGetter[] currentFieldGetters;

    public KvApplyLogHelper(Replica replica, ZooKeeperClient zkClient) {
        this.replica = replica;
        this.zkClient = zkClient;
    }

    public void recover(long recoverPointOffset, int maxFetchLogSize) throws Exception {
        // first step: read to high watermark and apply them to kv directly; that
        // 's for the data acked

        // second step: read from high watermark to log end offset which is not acked, and write
        // them into pre-write buffer to make the data in kv(underlying kv + pre-write buffer)
        // align with the local log;
        // the data in pre-write will be flush
        // after the corresponding log offset is acked(when high watermark is advanced to the
        // offset)

        long nextLogOffset = recoverPointOffset;
        // read to high watermark
        KvTablet kvTablet = replica.getKvTablet();
        checkNotNull(kvTablet);
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
                            FetchIsolation.HIGH_WATERMARK,
                            resumeRecordApplier,
                            maxFetchLogSize);
        }

        // the all data up to nextLogOffset has been flush into kv
        kvTablet.setFlushedLogOffset(nextLogOffset);

        // read to log end offset
        ThrowingConsumer<KeyValueAndLogOffset, Exception> resumeRecordApplier =
                (resumeRecord) ->
                        kvTablet.putToPreWriteBuffer(
                                resumeRecord.key, resumeRecord.value, resumeRecord.logOffset);
        readLogRecordsAndApply(
                nextLogOffset, FetchIsolation.LOG_END, resumeRecordApplier, maxFetchLogSize);
    }

    private long readLogRecordsAndApply(
            long startFetchOffset,
            FetchIsolation fetchIsolation,
            ThrowingConsumer<KeyValueAndLogOffset, Exception> resumeRecordConsumer,
            int maxFetchLogSize)
            throws Exception {
        long nextFetchOffset = startFetchOffset;
        while (true) {
            LogRecords logRecords =
                    replica.getLogTablet()
                            .read(nextFetchOffset, maxFetchLogSize, fetchIsolation, true, null)
                            .getRecords();
            if (logRecords == MemoryLogRecords.EMPTY) {
                break;
            }

            nextFetchOffset = applyLogRecords(logRecords, nextFetchOffset, resumeRecordConsumer);
        }

        return nextFetchOffset;
    }

    /**
     * Apply log records to kv.
     *
     * @param logRecords log records to apply.
     * @param startFetchOffset start fetch offset.
     * @return next fetch offset.
     */
    public long applyLogRecords(LogRecords logRecords, long startFetchOffset) throws Exception {
        ThrowingConsumer<KeyValueAndLogOffset, Exception> resumeRecordApplier =
                (resumeRecord) -> {
                    KvTablet kvTablet = replica.getKvTablet();
                    checkNotNull(kvTablet);
                    kvTablet.putToPreWriteBuffer(
                            resumeRecord.key, resumeRecord.value, resumeRecord.logOffset);
                };
        return applyLogRecords(logRecords, startFetchOffset, resumeRecordApplier);
    }

    /**
     * Apply log records to kv.
     *
     * @param logRecords log records to apply.
     * @param startFetchOffset start fetch offset.
     * @param resumeRecordConsumer resume record consumer.
     * @return next fetch offset.
     */
    private long applyLogRecords(
            LogRecords logRecords,
            long startFetchOffset,
            ThrowingConsumer<KeyValueAndLogOffset, Exception> resumeRecordConsumer)
            throws Exception {
        long nextFetchOffset = startFetchOffset;
        for (LogRecordBatch logRecordBatch : logRecords.batches()) {
            short schemaId = logRecordBatch.schemaId();
            if (currentSchemaId == null) {
                initSchema(schemaId);
            } else if (currentSchemaId != schemaId) {
                throw new KvStorageException(
                        String.format(
                                "Can't recover kv tablet for table bucket from log %s since the schema changes from schema id %d to schema id %d. "
                                        + "Currently, schema change is not supported.",
                                replica.getTableBucket(), currentSchemaId, schemaId));
            }

            try (LogRecordReadContext readContext =
                            LogRecordReadContext.createArrowReadContext(
                                    currentRowType, currentSchemaId);
                    CloseableIterator<LogRecord> logRecordIter =
                            logRecordBatch.records(readContext)) {
                while (logRecordIter.hasNext()) {
                    LogRecord logRecord = logRecordIter.next();
                    if (logRecord.getChangeType() != ChangeType.UPDATE_BEFORE) {
                        InternalRow logRow = logRecord.getRow();
                        byte[] key = keyEncoder.encodeKey(logRow);
                        byte[] value = null;
                        if (logRecord.getChangeType() != ChangeType.DELETE) {
                            // the log row format may not compatible with kv row format,
                            // e.g, arrow vs. compacted, thus needs a conversion here.
                            BinaryRow row = toKvRow(logRecord.getRow());
                            value = ValueEncoder.encodeValue(schemaId, row);
                        }
                        resumeRecordConsumer.accept(
                                new KeyValueAndLogOffset(key, value, logRecord.logOffset()));
                    }
                }
            }
            nextFetchOffset = logRecordBatch.nextLogOffset();
        }
        return nextFetchOffset;
    }

    // TODO: this is very in-efficient, because the conversion is CPU heavy. Should be optimized in
    //  the future.
    private BinaryRow toKvRow(InternalRow originalRow) {
        if (replica.getKvFormat() == KvFormat.INDEXED) {
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
        TableInfo tableInfo = getTableInfo(zkClient, replica.getTablePath());
        // todo: we need to check the schema's table id is equal to the
        // kv tablet's table id or not. If not equal, it means other table with same
        // table path has been created, so the kv tablet's table is consider to be
        // deleted. We can ignore the restore operation
        currentRowType = tableInfo.getRowType();
        DataType[] dataTypes = currentRowType.getChildren().toArray(new DataType[0]);
        currentSchemaId = schemaId;

        DataLakeFormat lakeFormat = tableInfo.getTableConfig().getDataLakeFormat().orElse(null);
        keyEncoder = KeyEncoder.of(currentRowType, tableInfo.getPhysicalPrimaryKeys(), lakeFormat);
        rowEncoder = RowEncoder.create(replica.getKvFormat(), dataTypes);
        currentFieldGetters = new InternalRow.FieldGetter[currentRowType.getFieldCount()];
        for (int i = 0; i < currentRowType.getFieldCount(); i++) {
            currentFieldGetters[i] = InternalRow.createFieldGetter(currentRowType.getTypeAt(i), i);
        }
    }

    private static final class KeyValueAndLogOffset {
        private final byte[] key;
        private final @Nullable byte[] value;
        private final long logOffset;

        public KeyValueAndLogOffset(byte[] key, @Nullable byte[] value, long logOffset) {
            this.key = key;
            this.value = value;
            this.logOffset = logOffset;
        }
    }
}
