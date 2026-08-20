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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.exception.DeletionDisabledException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.memory.MemorySegmentPool;
import org.apache.fluss.metadata.ChangelogImage;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaGetter;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.KvRecord;
import org.apache.fluss.record.KvRecordBatch;
import org.apache.fluss.record.KvRecordReadContext;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.PaddingRow;
import org.apache.fluss.row.arrow.ArrowWriterProvider;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.AutoIncrementUpdater;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer;
import org.apache.fluss.server.kv.prewrite.KvPreWriteBuffer.TruncateReason;
import org.apache.fluss.server.kv.rowmerger.DefaultRowMerger;
import org.apache.fluss.server.kv.rowmerger.RowMerger;
import org.apache.fluss.server.kv.wal.ArrowWalBuilder;
import org.apache.fluss.server.kv.wal.CompactedWalBuilder;
import org.apache.fluss.server.kv.wal.IndexWalBuilder;
import org.apache.fluss.server.kv.wal.WalBuilder;
import org.apache.fluss.server.log.LogAppendInfo;
import org.apache.fluss.server.log.LogTablet;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.BytesUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Processes a KV record batch into local state mutations and the corresponding WAL records.
 *
 * <p>For each input record, this processor reads the current value through {@link KvStateAccessor},
 * applies the configured row-merge semantics, stages the resulting mutation in the state pre-write
 * buffer, and appends the generated changelog to the {@link LogTablet}. Staged mutations are
 * truncated when the append fails or is detected as a duplicate.
 *
 * <p>The supplied {@link KvStateAccessor} defines how keys and state are accessed. Normal writes
 * use the original primary key and local state, while historical writes use partition-scoped keys
 * and may fall back to lake storage on a local miss. The merge and WAL generation path is shared by
 * both write kinds.
 *
 * <p>One instance belongs to one {@link KvTablet} and is invoked while that tablet's write lock is
 * held.
 */
@Internal
@NotThreadSafe
public final class KvWriteProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(KvWriteProcessor.class);

    private final TableBucket tableBucket;
    private final LogTablet logTablet;
    private final ArrowWriterProvider arrowWriterProvider;
    private final MemorySegmentPool memorySegmentPool;
    private final LogFormat logFormat;
    private final KvFormat kvFormat;
    // defines how to merge rows on the same primary key
    private final RowMerger rowMerger;
    // Pre-created DefaultRowMerger for OVERWRITE mode (undo recovery scenarios)
    // This avoids creating a new instance on every putAsLeader call
    private final RowMerger overwriteRowMerger;
    private final ArrowCompressionInfo arrowCompressionInfo;
    private final SchemaGetter schemaGetter;
    // the changelog image mode for this tablet
    private final ChangelogImage changelogImage;
    private final AutoIncrementManager autoIncrementManager;

    /** Creates a KV write processor. */
    public KvWriteProcessor(
            TableBucket tableBucket,
            LogTablet logTablet,
            ArrowWriterProvider arrowWriterProvider,
            MemorySegmentPool memorySegmentPool,
            KvFormat kvFormat,
            RowMerger rowMerger,
            ArrowCompressionInfo arrowCompressionInfo,
            SchemaGetter schemaGetter,
            ChangelogImage changelogImage,
            AutoIncrementManager autoIncrementManager) {
        this.tableBucket = tableBucket;
        this.logTablet = logTablet;
        this.arrowWriterProvider = arrowWriterProvider;
        this.memorySegmentPool = memorySegmentPool;
        this.logFormat = logTablet.getLogFormat();
        this.kvFormat = kvFormat;
        this.rowMerger = rowMerger;
        // Pre-create DefaultRowMerger for OVERWRITE mode to avoid creating new instances
        // on every putAsLeader call. Used for undo recovery scenarios.
        this.overwriteRowMerger = new DefaultRowMerger(kvFormat, DeleteBehavior.ALLOW);
        this.arrowCompressionInfo = arrowCompressionInfo;
        this.schemaGetter = schemaGetter;
        this.changelogImage = changelogImage;
        this.autoIncrementManager = autoIncrementManager;
    }

    /** Processes a KV batch against the supplied state and appends its WAL. */
    public LogAppendInfo putAsLeader(
            KvRecordBatch kvRecords,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            KvStateAccessor stateAccessor)
            throws Exception {
        SchemaInfo schemaInfo = schemaGetter.getLatestSchemaInfo();
        Schema latestSchema = schemaInfo.getSchema();
        short latestSchemaId = (short) schemaInfo.getSchemaId();
        validateSchemaId(kvRecords.schemaId(), latestSchemaId);

        AutoIncrementUpdater currentAutoIncrementUpdater =
                autoIncrementManager.getUpdaterForSchema(kvFormat, latestSchemaId);

        // Validate targetColumns doesn't contain auto-increment column
        currentAutoIncrementUpdater.validateTargetColumns(targetColumns);

        // Determine the row merger based on mergeMode:
        // - DEFAULT: Use the configured merge engine (rowMerger)
        // - OVERWRITE: Bypass merge engine, use pre-created overwriteRowMerger
        //   to directly replace values (for undo recovery scenarios)
        // We only support ADD COLUMN, so targetColumns is fine to be used directly.
        RowMerger currentMerger =
                (mergeMode == MergeMode.OVERWRITE)
                        ? overwriteRowMerger.configureTargetColumns(
                                targetColumns, latestSchemaId, latestSchema)
                        : rowMerger.configureTargetColumns(
                                targetColumns, latestSchemaId, latestSchema);

        RowType latestRowType = latestSchema.getRowType();
        WalBuilder walBuilder = createWalBuilder(latestSchemaId, latestRowType);
        walBuilder.setWriterState(kvRecords.writerId(), kvRecords.batchSequence());
        // we only support ADD COLUMN LAST, so the BinaryRow after RowMerger is
        // only has fewer ending columns than latest schema, so we pad nulls to
        // the end of the BinaryRow to get the latest schema row.
        PaddingRow latestSchemaRow = new PaddingRow(latestRowType.getFieldCount());
        // get offset to track the offset corresponded to the kv record
        long logEndOffsetOfPrevBatch = logTablet.localLogEndOffset();

        try {
            processKvRecords(
                    kvRecords,
                    kvRecords.schemaId(),
                    currentMerger,
                    currentAutoIncrementUpdater,
                    walBuilder,
                    latestSchemaRow,
                    logEndOffsetOfPrevBatch,
                    stateAccessor);

            // There will be a situation that these batches of kvRecordBatch have not
            // generated any CDC logs, for example, when client attempts to delete
            // some non-existent keys or MergeEngineType set to FIRST_ROW. In this case,
            // we cannot simply return, as doing so would cause a
            // OutOfOrderSequenceException problem. Therefore, here we will build an
            // empty batch with lastLogOffset to 0L as the baseLogOffset is 0L. As doing
            // that, the logOffsetDelta in logRecordBatch will be set to 0L. So, we will
            // put a batch into file with recordCount 0 and offset plus 1L, it will
            // update the batchSequence corresponding to the writerId and also increment
            // the CDC log offset by 1.
            LogAppendInfo logAppendInfo = logTablet.appendAsLeader(walBuilder.build());

            // if the batch is duplicated, we should truncate the state pre-write
            // buffer already written.
            if (logAppendInfo.duplicated()) {
                stateAccessor.truncateTo(logEndOffsetOfPrevBatch, TruncateReason.DUPLICATED);
            }
            return logAppendInfo;
        } catch (Throwable t) {
            // While encounter error here, the CDC logs may fail writing to disk,
            // and the client probably will resend the batch. If we do not remove the
            // values generated by the erroneous batch from the state pre-write buffer,
            // the retry-send batch will produce incorrect CDC logs.
            // TODO for some errors, the cdc logs may already be written to disk, for
            //  those errors, we should not truncate the state pre-write buffer.
            stateAccessor.truncateTo(logEndOffsetOfPrevBatch, TruncateReason.ERROR);
            throw t;
        } finally {
            // deallocate the memory and arrow writer used by the wal builder
            walBuilder.deallocate();
        }
    }

    private void validateSchemaId(short schemaIdOfNewData, short latestSchemaId) {
        if (schemaIdOfNewData > latestSchemaId || schemaIdOfNewData < 0) {
            throw new SchemaNotExistException(
                    "Invalid schema id: "
                            + schemaIdOfNewData
                            + ", latest schema id: "
                            + latestSchemaId);
        }
    }

    private void processKvRecords(
            KvRecordBatch kvRecords,
            short schemaIdOfNewData,
            RowMerger currentMerger,
            AutoIncrementUpdater autoIncrementUpdater,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long startLogOffset,
            KvStateAccessor stateAccessor)
            throws Exception {
        long logOffset = startLogOffset;

        // TODO: reuse the read context and decoder
        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(kvFormat, schemaGetter);
        ValueDecoder valueDecoder = new ValueDecoder(schemaGetter, kvFormat);

        for (KvRecord kvRecord : kvRecords.records(readContext)) {
            byte[] keyBytes = BytesUtils.toArray(kvRecord.getKey());
            KvPreWriteBuffer.Key key = stateAccessor.encodeKey(keyBytes);
            BinaryRow row = kvRecord.getRow();
            BinaryValue currentValue = row == null ? null : new BinaryValue(schemaIdOfNewData, row);

            if (currentValue == null) {
                logOffset =
                        processDeletion(
                                key,
                                currentMerger,
                                valueDecoder,
                                walBuilder,
                                latestSchemaRow,
                                logOffset,
                                stateAccessor);
            } else {
                logOffset =
                        processUpsert(
                                key,
                                currentValue,
                                currentMerger,
                                autoIncrementUpdater,
                                valueDecoder,
                                walBuilder,
                                latestSchemaRow,
                                logOffset,
                                stateAccessor);
            }
        }
    }

    private long processDeletion(
            KvPreWriteBuffer.Key key,
            RowMerger currentMerger,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            KvStateAccessor stateAccessor)
            throws Exception {
        DeleteBehavior deleteBehavior = currentMerger.deleteBehavior();
        if (deleteBehavior == DeleteBehavior.IGNORE) {
            // skip delete rows if the merger doesn't support yet
            return logOffset;
        } else if (deleteBehavior == DeleteBehavior.DISABLE) {
            throw new DeletionDisabledException(
                    "Delete operations are disabled for this table. "
                            + "The table.delete.behavior is set to 'disable'.");
        }

        byte[] oldValueBytes = getFromState(key, stateAccessor);
        if (oldValueBytes == null) {
            LOG.debug(
                    "The specific key can't be found in kv tablet although the kv record is for deletion, "
                            + "ignore it directly as it doesn't exist in the kv tablet yet.");
            return logOffset;
        }

        BinaryValue oldValue = valueDecoder.decodeValue(oldValueBytes);
        BinaryValue newValue = currentMerger.delete(oldValue);

        // if newValue is null, it means the row should be deleted
        if (newValue == null) {
            return applyDelete(
                    key, oldValue, walBuilder, latestSchemaRow, logOffset, stateAccessor);
        } else {
            return applyUpdate(
                    key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset, stateAccessor);
        }
    }

    private long processUpsert(
            KvPreWriteBuffer.Key key,
            BinaryValue currentValue,
            RowMerger currentMerger,
            AutoIncrementUpdater autoIncrementUpdater,
            ValueDecoder valueDecoder,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            KvStateAccessor stateAccessor)
            throws Exception {
        // Optimization: IN WAL mode，when using DefaultRowMerger (full update, not partial update)
        // and there is no auto-increment column, we can skip fetching old value for better
        // performance since the result always reflects the new value. In this case, both INSERT and
        // UPDATE will produce UPDATE_AFTER.
        if (changelogImage == ChangelogImage.WAL
                && !autoIncrementUpdater.hasAutoIncrement()
                && currentMerger instanceof DefaultRowMerger) {
            return applyUpdate(
                    key, null, currentValue, walBuilder, latestSchemaRow, logOffset, stateAccessor);
        }

        byte[] oldValueBytes = getFromState(key, stateAccessor);
        if (oldValueBytes == null) {
            BinaryValue valueToInsert = currentMerger.merge(null, currentValue);
            return applyInsert(
                    key,
                    valueToInsert,
                    walBuilder,
                    latestSchemaRow,
                    logOffset,
                    autoIncrementUpdater,
                    stateAccessor);
        }

        BinaryValue oldValue = valueDecoder.decodeValue(oldValueBytes);
        BinaryValue newValue = currentMerger.merge(oldValue, currentValue);

        if (newValue == oldValue) {
            // no actual change, skip this record
            return logOffset;
        }

        return applyUpdate(
                key, oldValue, newValue, walBuilder, latestSchemaRow, logOffset, stateAccessor);
    }

    private long applyDelete(
            KvPreWriteBuffer.Key key,
            BinaryValue oldValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            KvStateAccessor stateAccessor)
            throws Exception {
        walBuilder.append(ChangeType.DELETE, latestSchemaRow.replaceRow(oldValue.row));
        stateAccessor.delete(key, logOffset);
        return logOffset + 1;
    }

    private long applyInsert(
            KvPreWriteBuffer.Key key,
            BinaryValue currentValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            AutoIncrementUpdater autoIncrementUpdater,
            KvStateAccessor stateAccessor)
            throws Exception {
        BinaryValue newValue = autoIncrementUpdater.updateAutoIncrementColumns(currentValue);
        walBuilder.append(ChangeType.INSERT, latestSchemaRow.replaceRow(newValue.row));
        stateAccessor.insert(key, newValue.encodeValue(), logOffset);
        return logOffset + 1;
    }

    private long applyUpdate(
            KvPreWriteBuffer.Key key,
            @Nullable BinaryValue oldValue,
            BinaryValue newValue,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            KvStateAccessor stateAccessor)
            throws Exception {
        if (changelogImage == ChangelogImage.WAL) {
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            stateAccessor.update(key, newValue.encodeValue(), logOffset);
            return logOffset + 1;
        } else {
            walBuilder.append(ChangeType.UPDATE_BEFORE, latestSchemaRow.replaceRow(oldValue.row));
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            stateAccessor.update(key, newValue.encodeValue(), logOffset + 1);
            return logOffset + 2;
        }
    }

    private byte[] getFromState(
            KvPreWriteBuffer.Key encodedPrimaryKey, KvStateAccessor stateAccessor)
            throws Exception {
        return stateAccessor.lookup(encodedPrimaryKey).value();
    }

    private WalBuilder createWalBuilder(int schemaId, RowType rowType) throws Exception {
        switch (logFormat) {
            case INDEXED:
                if (kvFormat == KvFormat.COMPACTED) {
                    // convert from compacted row to indexed row is time cost, and gain
                    // less benefits, currently we won't support compacted as kv format and
                    // indexed as cdc log format.
                    // so in here we throw exception directly
                    throw new IllegalArgumentException(
                            "Primary Key Table with COMPACTED kv format doesn't support INDEXED cdc log format.");
                }
                return new IndexWalBuilder(schemaId, memorySegmentPool);
            case COMPACTED:
                return new CompactedWalBuilder(schemaId, rowType, memorySegmentPool);
            case ARROW:
                return new ArrowWalBuilder(
                        schemaId,
                        arrowWriterProvider.getOrCreateWriter(
                                tableBucket.getTableId(),
                                schemaId,
                                // we don't limit size of the arrow batch, because all the
                                // changelogs should be in a single batch
                                Integer.MAX_VALUE,
                                rowType,
                                arrowCompressionInfo),
                        memorySegmentPool);
            default:
                throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }
}
