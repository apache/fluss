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
import org.apache.fluss.exception.InvalidRecordException;
import org.apache.fluss.exception.InvalidTableException;
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
import org.apache.fluss.record.MutationType;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.PaddingRow;
import org.apache.fluss.row.arrow.ArrowWriterProvider;
import org.apache.fluss.row.encode.KvValueLayout;
import org.apache.fluss.row.encode.ValueDecoder;
import org.apache.fluss.rpc.protocol.MergeMode;
import org.apache.fluss.server.kv.autoinc.AutoIncrementManager;
import org.apache.fluss.server.kv.autoinc.AutoIncrementUpdater;
import org.apache.fluss.server.kv.historical.HistoricalValueLookup;
import org.apache.fluss.server.kv.historical.LocalValueLookupResult;
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
import org.apache.fluss.utils.ByteArrayWrapper;
import org.apache.fluss.utils.BytesUtils;
import org.apache.fluss.utils.clock.Clock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Processes a KV record batch into local state mutations and the corresponding WAL records.
 *
 * <p>For each input record, this processor reads the current value through {@link KvStateAccessor},
 * applies the configured row-merge semantics, stages the resulting mutation in the state pre-write
 * buffer, and appends the generated changelog to the {@link LogTablet}. Staged mutations are
 * truncated when the append fails or is detected as a duplicate.
 *
 * <p>The supplied {@link KvStateAccessor} defines how keys and state are accessed. Normal writes
 * use the original primary key and local state, while historical writes use partition-scoped keys.
 * Historical writes supply previous values saved for the current request as the accessor's
 * fallback. The saved values include local probe results, which remain available after compaction,
 * and lake results resolved by the caller outside the tablet lock. The merge and WAL generation
 * path is shared by both write kinds.
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
    private final KvStateValueEncoder stateValueEncoder;
    private final ValueDecoder valueDecoder;
    private final ValueDecoder lakeValueDecoder;
    @Nullable private final RowTtlTimestampProvider rowTtlTimestampProvider;
    private final Clock clock;
    private final boolean skipUnchangedUpsert;

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
            AutoIncrementManager autoIncrementManager,
            KvStateValueEncoder stateValueEncoder,
            ValueDecoder valueDecoder,
            @Nullable RowTtlTimestampProvider rowTtlTimestampProvider,
            Clock clock,
            boolean skipUnchangedUpsert) {
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
        this.stateValueEncoder = stateValueEncoder;
        this.valueDecoder = valueDecoder;
        this.lakeValueDecoder = new ValueDecoder(schemaGetter, kvFormat, KvValueLayout.PLAIN);
        this.rowTtlTimestampProvider = rowTtlTimestampProvider;
        this.clock = clock;
        this.skipUnchangedUpsert = skipUnchangedUpsert;
    }

    /** Processes a KV batch against the supplied state and appends its WAL. */
    public LogAppendInfo putAsLeader(
            KvRecordBatch kvRecords,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            KvStateAccessor stateAccessor,
            @Nullable String originalPartitionName,
            @Nullable HistoricalValueLookup historicalValueLookup)
            throws Exception {
        WriteContext writeContext = createWriteContext(kvRecords, targetColumns, mergeMode);
        RowType latestRowType = writeContext.latestSchema.getRowType();
        WalBuilder walBuilder = createWalBuilder(writeContext.latestSchemaId, latestRowType);
        walBuilder.setWriterState(kvRecords.writerId(), kvRecords.batchSequence());
        // we only support ADD COLUMN LAST, so the BinaryRow after RowMerger is
        // only has fewer ending columns than latest schema, so we pad nulls to
        // the end of the BinaryRow to get the latest schema row.
        PaddingRow latestSchemaRow = new PaddingRow(latestRowType.getFieldCount());
        // get offset to track the offset corresponded to the kv record
        long logEndOffsetOfPrevBatch = logTablet.localLogEndOffset();

        try {
            if (rowTtlTimestampProvider != null) {
                rowTtlTimestampProvider.prepareForBatch(clock.milliseconds());
            }
            processKvRecords(
                    kvRecords,
                    kvRecords.schemaId(),
                    writeContext.rowMerger,
                    writeContext.autoIncrementUpdater,
                    walBuilder,
                    latestSchemaRow,
                    logEndOffsetOfPrevBatch,
                    stateAccessor,
                    originalPartitionName,
                    historicalValueLookup);

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

    /**
     * Probes the local previous values required by a historical write batch.
     *
     * <p>Every key whose previous value is required is probed at most once. Local values and
     * deletes are decoded into the returned collection, while true local misses are exposed for
     * lake lookup. Records that establish their result without a previous value are skipped.
     */
    LocalValueLookupResult probeLocalPreviousValues(
            KvRecordBatch kvRecords,
            @Nullable int[] targetColumns,
            MergeMode mergeMode,
            KvStateAccessor stateAccessor,
            String originalPartitionName)
            throws Exception {
        WriteContext writeContext = createWriteContext(kvRecords, targetColumns, mergeMode);

        LocalValueLookupResult localLookupResult =
                new LocalValueLookupResult(valueDecoder, lakeValueDecoder);
        Set<ByteArrayWrapper> probedKeys = new HashSet<>();
        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(kvFormat, schemaGetter);
        for (KvRecord kvRecord : kvRecords.records(readContext)) {
            byte[] primaryKey = BytesUtils.toArray(kvRecord.getKey());
            ByteArrayWrapper wrappedKey = new ByteArrayWrapper(primaryKey);
            if (kvRecord.getRow() == null) {
                if (shouldIgnoreDeletion(writeContext.rowMerger)) {
                    continue;
                }
            } else if (canSkipOldValueLookup(
                    writeContext.rowMerger, writeContext.autoIncrementUpdater)) {
                // A full-row WAL upsert establishes the state without reading its previous value.
                probedKeys.add(wrappedKey);
                continue;
            }

            // Probe local state only for the first record that needs a previous value. A true local
            // miss schedules one lake lookup shared by all records for this key in the batch.
            if (probedKeys.add(wrappedKey)) {
                localLookupResult.add(
                        primaryKey,
                        stateAccessor.lookupLocal(
                                stateAccessor.encodeKey(primaryKey, originalPartitionName)));
            }
        }
        return localLookupResult;
    }

    private WriteContext createWriteContext(
            KvRecordBatch kvRecords, @Nullable int[] targetColumns, MergeMode mergeMode) {
        if (skipUnchangedUpsert && mergeMode == MergeMode.OVERWRITE) {
            throw new InvalidTableException(
                    "MergeMode.OVERWRITE cannot be used with table.upsert.skip-unchanged enabled.");
        }

        SchemaInfo schemaInfo = schemaGetter.getLatestSchemaInfo();
        Schema latestSchema = schemaInfo.getSchema();
        short latestSchemaId = (short) schemaInfo.getSchemaId();
        validateSchemaId(kvRecords.schemaId(), latestSchemaId);

        AutoIncrementUpdater autoIncrementUpdater =
                autoIncrementManager.getUpdaterForSchema(kvFormat, latestSchemaId);
        autoIncrementUpdater.validateTargetColumns(targetColumns);

        // OVERWRITE bypasses the configured merge engine for undo recovery.
        RowMerger configuredRowMerger =
                (mergeMode == MergeMode.OVERWRITE)
                        ? overwriteRowMerger.configureTargetColumns(
                                targetColumns, latestSchemaId, latestSchema)
                        : rowMerger.configureTargetColumns(
                                targetColumns, latestSchemaId, latestSchema);
        return new WriteContext(
                latestSchema, latestSchemaId, autoIncrementUpdater, configuredRowMerger);
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
            KvStateAccessor stateAccessor,
            @Nullable String originalPartitionName,
            @Nullable HistoricalValueLookup historicalValueLookup)
            throws Exception {
        if (kvRecords.isV2Format()) {
            processV2KvRecords(
                    kvRecords,
                    schemaIdOfNewData,
                    currentMerger,
                    autoIncrementUpdater,
                    walBuilder,
                    latestSchemaRow,
                    startLogOffset,
                    stateAccessor,
                    originalPartitionName,
                    historicalValueLookup);
            return;
        }
        long logOffset = startLogOffset;

        // TODO: reuse the read context
        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(kvFormat, schemaGetter);

        for (KvRecord kvRecord : kvRecords.records(readContext)) {
            byte[] keyBytes = BytesUtils.toArray(kvRecord.getKey());
            KvPreWriteBuffer.Key key = stateAccessor.encodeKey(keyBytes, originalPartitionName);
            BinaryRow row = kvRecord.getRow();
            BinaryValue currentValue = row == null ? null : new BinaryValue(schemaIdOfNewData, row);

            if (currentValue == null) {
                logOffset =
                        processDeletion(
                                key,
                                currentMerger,
                                walBuilder,
                                latestSchemaRow,
                                logOffset,
                                stateAccessor,
                                keyBytes,
                                historicalValueLookup);
            } else {
                logOffset =
                        processUpsert(
                                key,
                                currentValue,
                                currentMerger,
                                autoIncrementUpdater,
                                walBuilder,
                                latestSchemaRow,
                                logOffset,
                                stateAccessor,
                                keyBytes,
                                historicalValueLookup);
            }
        }
    }

    private void processV2KvRecords(
            KvRecordBatch kvRecords,
            short schemaIdOfNewData,
            RowMerger currentMerger,
            AutoIncrementUpdater autoIncrementUpdater,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long startLogOffset,
            KvStateAccessor stateAccessor,
            @Nullable String originalPartitionName,
            @Nullable HistoricalValueLookup historicalValueLookup)
            throws Exception {
        long logOffset = startLogOffset;
        KvRecordBatch.ReadContext readContext =
                KvRecordReadContext.createReadContext(kvFormat, schemaGetter);
        // V2 path: uses PeekableKvMutationIterator for RETRACT+UPSERT pairing optimization
        Iterator<KvRecord> iter = kvRecords.records(readContext).iterator();
        PeekableKvMutationIterator peekableIter =
                new PeekableKvMutationIterator(
                        iter, schemaIdOfNewData, stateAccessor, originalPartitionName);
        while (peekableIter.hasNext()) {
            KvRecord kvRecord = peekableIter.next();
            NormalizedKvMutation mutation = peekableIter.nextMutation(kvRecord);
            MutationType mutationType = kvRecord.getMutationType();

            switch (mutationType) {
                case UPSERT:
                    {
                        if (mutation.isDelete()) {
                            throw new InvalidRecordException(
                                    "V2 UPSERT record must carry a non-null row value.");
                        }
                        logOffset =
                                dispatchMutation(
                                        mutation,
                                        currentMerger,
                                        autoIncrementUpdater,
                                        walBuilder,
                                        latestSchemaRow,
                                        logOffset,
                                        stateAccessor,
                                        originalPartitionName,
                                        historicalValueLookup);
                        break;
                    }
                case DELETE:
                    {
                        logOffset =
                                dispatchMutation(
                                        mutation,
                                        currentMerger,
                                        autoIncrementUpdater,
                                        walBuilder,
                                        latestSchemaRow,
                                        logOffset,
                                        stateAccessor,
                                        originalPartitionName,
                                        historicalValueLookup);
                        break;
                    }
                case RETRACT:
                    {
                        if (!currentMerger.supportsRetract()) {
                            throw new InvalidRecordException(
                                    "RETRACT records are only supported for aggregation merge "
                                            + "engine tables with retract-safe functions.");
                        }
                        if (mutation.isDelete()) {
                            throw new InvalidRecordException(
                                    "RETRACT record must carry a non-null row value.");
                        }

                        // Snapshot key/value from the reusable NormalizedKvMutation to decouple
                        // from the reusable object's lifecycle during multi-step retract
                        // processing.
                        final KvPreWriteBuffer.Key retractKey = mutation.key;
                        final BinaryValue retractValue = mutation.value;

                        BinaryValue oldValue =
                                getPreviousValue(
                                        retractKey,
                                        mutation.primaryKey,
                                        stateAccessor,
                                        historicalValueLookup);
                        if (oldValue == null) {
                            // Retract on non-existent key — nothing to undo, safe to skip.
                            LOG.debug(
                                    "Retract on non-existent key in kv tablet for {}, "
                                            + "ignoring as the key does not exist.",
                                    tableBucket);
                            break;
                        }

                        // Best-effort merge optimization: peek next record for same key
                        KvRecord nextRecord = peekableIter.peek();
                        if (nextRecord != null) {
                            NormalizedKvMutation nextMutation = peekableIter.peekMutation();
                            if (nextMutation.key.equals(retractKey)
                                    && nextRecord.getMutationType() == MutationType.UPSERT
                                    && !nextMutation.isDelete()) {
                                // Consume the peeked record — it's part of this merged pair
                                peekableIter.consumePeeked();
                                // Merged retract+upsert pair
                                BinaryValue intermediate =
                                        currentMerger.retract(oldValue, retractValue);
                                BinaryValue newValue;
                                if (intermediate == null) {
                                    // Retract fully removed the row (intermediate == null).
                                    // Treat the subsequent upsert as a fresh insert by merging
                                    // with null accumulator — this is the intended semantic.
                                    newValue = currentMerger.merge(null, nextMutation.value);
                                } else {
                                    newValue =
                                            currentMerger.merge(intermediate, nextMutation.value);
                                }

                                if (newValue == null) {
                                    DeleteBehavior deleteBehavior = currentMerger.deleteBehavior();
                                    if (deleteBehavior == DeleteBehavior.IGNORE) {
                                        // Merged retract+upsert produced null but delete is
                                        // ignored.
                                        // The old value remains in the KV store — this is
                                        // intentional:
                                        // IGNORE means "do not delete keys", so the pre-retract
                                        // value
                                        // is preserved. Subsequent lookups will return the old
                                        // value.
                                        // No CDC output is emitted for this no-op.
                                    } else if (deleteBehavior == DeleteBehavior.DISABLE) {
                                        throw new DeletionDisabledException(
                                                "Delete operations are disabled for this table."
                                                        + " The table.delete.behavior is set to 'disable'.");
                                    } else {
                                        logOffset =
                                                applyDelete(
                                                        retractKey,
                                                        oldValue,
                                                        walBuilder,
                                                        latestSchemaRow,
                                                        logOffset,
                                                        stateAccessor);
                                    }
                                }
                                // Use value equality (not reference equality) because
                                // retract()+merge() always creates new BinaryValue instances
                                // even when the result is logically unchanged.
                                else if (newValue.equals(oldValue)) {
                                    // Merged retract+upsert produced same value as before:
                                    // intentionally no CDC output and no offset consumed.
                                } else {
                                    logOffset =
                                            applyUpdate(
                                                    retractKey,
                                                    oldValue,
                                                    newValue,
                                                    walBuilder,
                                                    latestSchemaRow,
                                                    logOffset,
                                                    stateAccessor);
                                }
                                break;
                            }
                            // Non-matching peeked record stays in the iterator for the
                            // next loop iteration — no need to manually save it.
                        }

                        // Independent retract (no matching upsert follows)
                        logOffset =
                                processIndependentRetract(
                                        retractKey,
                                        retractValue,
                                        oldValue,
                                        currentMerger,
                                        walBuilder,
                                        latestSchemaRow,
                                        logOffset,
                                        stateAccessor);
                        break;
                    }
                default:
                    // Defensive: should be unreachable; MutationType is validated
                    // during deserialization.
                    throw new InvalidRecordException("Unknown MutationType: " + mutationType);
            }
        }
    }

    /**
     * Process an independent retract (not paired with a subsequent upsert). Retracts the given
     * value from the old value and applies the result.
     */
    private long processIndependentRetract(
            KvPreWriteBuffer.Key key,
            BinaryValue retractValue,
            BinaryValue oldValue,
            RowMerger currentMerger,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            KvStateAccessor stateAccessor)
            throws Exception {
        BinaryValue intermediate = currentMerger.retract(oldValue, retractValue);

        if (intermediate == null) {
            // Retract resulted in removal
            DeleteBehavior deleteBehavior = currentMerger.deleteBehavior();
            if (deleteBehavior == DeleteBehavior.IGNORE) {
                // Retract produced null but delete is ignored.
                // The old value remains in the KV store — this is intentional:
                // IGNORE means "do not delete keys", so the pre-retract value
                // is preserved. Subsequent lookups will return the old value.
                return logOffset;
            } else if (deleteBehavior == DeleteBehavior.DISABLE) {
                throw new DeletionDisabledException(
                        "Delete operations are disabled for this table."
                                + " The table.delete.behavior is set to 'disable'.");
            } else {
                return applyDelete(
                        key, oldValue, walBuilder, latestSchemaRow, logOffset, stateAccessor);
            }
        }
        // Use value equality (not reference equality) because retract()+merge() always
        // creates new BinaryValue instances even when the result is logically unchanged.
        else if (intermediate.equals(oldValue)) {
            return logOffset; // no change, no offset consumed
        } else {
            return applyUpdate(
                    key,
                    oldValue,
                    intermediate,
                    walBuilder,
                    latestSchemaRow,
                    logOffset,
                    stateAccessor);
        }
    }

    private long dispatchMutation(
            NormalizedKvMutation mutation,
            RowMerger currentMerger,
            AutoIncrementUpdater autoIncrementUpdater,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            KvStateAccessor stateAccessor,
            @Nullable String originalPartitionName,
            @Nullable HistoricalValueLookup historicalValueLookup)
            throws Exception {
        if (mutation.isDelete()) {
            return processDeletion(
                    mutation.key,
                    currentMerger,
                    walBuilder,
                    latestSchemaRow,
                    logOffset,
                    stateAccessor,
                    mutation.primaryKey,
                    historicalValueLookup);
        } else {
            return processUpsert(
                    mutation.key,
                    mutation.value,
                    currentMerger,
                    autoIncrementUpdater,
                    walBuilder,
                    latestSchemaRow,
                    logOffset,
                    stateAccessor,
                    mutation.primaryKey,
                    historicalValueLookup);
        }
    }

    /**
     * A peekable iterator over KvRecords that pre-computes {@link NormalizedKvMutation} for each
     * record. Supports one-element lookahead via {@link #peek()} / {@link #peekMutation()} without
     * consuming the record, eliminating the need for manual pendingRecord/pendingMutation state
     * management in the caller.
     */
    private static final class PeekableKvMutationIterator {
        private final Iterator<KvRecord> inner;
        private final short schemaIdOfNewData;
        private final KvStateAccessor stateAccessor;
        @Nullable private final String originalPartitionName;

        private @Nullable KvRecord peekedRecord;
        private @Nullable NormalizedKvMutation peekedMutation;
        // Holds the mutation from a consumed peeked record so nextMutation() can return
        // it without recomputing.
        private @Nullable NormalizedKvMutation lastConsumedMutation;
        // Reusable mutation object for the non-peeked hot path to avoid per-record allocation.
        private final NormalizedKvMutation reusable;

        PeekableKvMutationIterator(
                Iterator<KvRecord> inner,
                short schemaIdOfNewData,
                KvStateAccessor stateAccessor,
                @Nullable String originalPartitionName) {
            this.inner = inner;
            this.stateAccessor = stateAccessor;
            this.originalPartitionName = originalPartitionName;
            this.schemaIdOfNewData = schemaIdOfNewData;
            this.reusable = new NormalizedKvMutation(null, null);
        }

        boolean hasNext() {
            return peekedRecord != null || inner.hasNext();
        }

        /** Returns the next record, consuming it. */
        KvRecord next() {
            if (peekedRecord != null) {
                KvRecord record = peekedRecord;
                lastConsumedMutation = peekedMutation;
                peekedRecord = null;
                peekedMutation = null;
                return record;
            }
            lastConsumedMutation = null;
            return inner.next();
        }

        /** Returns the mutation for the record returned by the last {@link #next()} call. */
        NormalizedKvMutation nextMutation(KvRecord kvRecord) {
            if (lastConsumedMutation != null) {
                NormalizedKvMutation m = lastConsumedMutation;
                lastConsumedMutation = null;
                return m;
            }
            // Non-peeked hot path: reuse the mutation object to avoid allocation.
            byte[] primaryKey = BytesUtils.toArray(kvRecord.getKey());
            KvPreWriteBuffer.Key key = stateAccessor.encodeKey(primaryKey, originalPartitionName);
            BinaryRow row = kvRecord.getRow();
            BinaryValue value = row == null ? null : new BinaryValue(schemaIdOfNewData, row);
            reusable.reset(key, value);
            reusable.primaryKey = primaryKey;
            return reusable;
        }

        /** Peeks at the next record without consuming it. Returns null if no more records. */
        @Nullable
        KvRecord peek() {
            if (peekedRecord == null && inner.hasNext()) {
                peekedRecord = inner.next();
                peekedMutation = normalize(peekedRecord);
            }
            return peekedRecord;
        }

        /** Returns the pre-computed mutation for the peeked record. */
        @Nullable
        NormalizedKvMutation peekMutation() {
            return peekedMutation;
        }

        /** Consumes the peeked record (must be called after peek()). */
        void consumePeeked() {
            peekedRecord = null;
            peekedMutation = null;
        }

        private NormalizedKvMutation normalize(KvRecord kvRecord) {
            byte[] primaryKey = BytesUtils.toArray(kvRecord.getKey());
            KvPreWriteBuffer.Key key = stateAccessor.encodeKey(primaryKey, originalPartitionName);
            BinaryRow row = kvRecord.getRow();
            BinaryValue value = row == null ? null : new BinaryValue(schemaIdOfNewData, row);
            NormalizedKvMutation mutation = new NormalizedKvMutation(key, value);
            mutation.primaryKey = primaryKey;
            return mutation;
        }
    }

    private static final class NormalizedKvMutation {
        private KvPreWriteBuffer.Key key;
        private byte[] primaryKey;
        private @Nullable BinaryValue value;

        private NormalizedKvMutation(KvPreWriteBuffer.Key key, @Nullable BinaryValue value) {
            this.key = key;
            this.value = value;
        }

        void reset(KvPreWriteBuffer.Key key, @Nullable BinaryValue value) {
            this.key = key;
            this.value = value;
        }

        /** A delete mutation has null value. */
        boolean isDelete() {
            return value == null;
        }
    }

    private long processDeletion(
            KvPreWriteBuffer.Key key,
            RowMerger currentMerger,
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            KvStateAccessor stateAccessor,
            byte[] primaryKey,
            @Nullable HistoricalValueLookup historicalValueLookup)
            throws Exception {
        if (shouldIgnoreDeletion(currentMerger)) {
            return logOffset;
        }

        BinaryValue oldValue =
                getPreviousValue(key, primaryKey, stateAccessor, historicalValueLookup);
        if (oldValue == null) {
            LOG.debug(
                    "The specific key can't be found in kv tablet although the kv record is for deletion, "
                            + "ignore it directly as it doesn't exist in the kv tablet yet.");
            return logOffset;
        }

        BinaryValue newValue = currentMerger.delete(oldValue);

        boolean skipWrite =
                skipUnchangedUpsert
                        && newValue != null
                        && BytewiseRowEqualizer.equals(oldValue, newValue);
        if (skipWrite) {
            return logOffset;
        }

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
            WalBuilder walBuilder,
            PaddingRow latestSchemaRow,
            long logOffset,
            KvStateAccessor stateAccessor,
            byte[] primaryKey,
            @Nullable HistoricalValueLookup historicalValueLookup)
            throws Exception {
        if (canSkipOldValueLookup(currentMerger, autoIncrementUpdater)) {
            return applyUpdate(
                    key, null, currentValue, walBuilder, latestSchemaRow, logOffset, stateAccessor);
        }

        BinaryValue oldValue =
                getPreviousValue(key, primaryKey, stateAccessor, historicalValueLookup);
        if (oldValue == null) {
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

        BinaryValue newValue = currentMerger.merge(oldValue, currentValue);

        boolean skipWrite =
                skipUnchangedUpsert
                        ? BytewiseRowEqualizer.equals(oldValue, newValue)
                        : newValue == oldValue;
        if (skipWrite) {
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
        stateAccessor.insert(key, stateValueEncoder.encodeValue(newValue, logOffset), logOffset);
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
            stateAccessor.update(
                    key, stateValueEncoder.encodeValue(newValue, logOffset), logOffset);
            return logOffset + 1;
        } else {
            walBuilder.append(ChangeType.UPDATE_BEFORE, latestSchemaRow.replaceRow(oldValue.row));
            walBuilder.append(ChangeType.UPDATE_AFTER, latestSchemaRow.replaceRow(newValue.row));
            long updateAfterOffset = logOffset + 1;
            stateAccessor.update(
                    key,
                    stateValueEncoder.encodeValue(newValue, updateAfterOffset),
                    updateAfterOffset);
            return logOffset + 2;
        }
    }

    /**
     * Returns the previous value through the state accessor, using saved results as the fallback
     * for historical writes.
     *
     * @param localStateKey the key used by the local prewrite buffer and RocksDB; it wraps {@code
     *     primaryKey} for a normal partition and adds the original partition namespace for a
     *     historical partition
     * @param primaryKey the encoded bytes of the logical primary key, without the historical
     *     partition namespace
     * @return the previous value, or null if the key is absent or locally marked as deleted
     */
    private BinaryValue getPreviousValue(
            KvPreWriteBuffer.Key localStateKey,
            byte[] primaryKey,
            KvStateAccessor stateAccessor,
            @Nullable HistoricalValueLookup historicalValueLookup)
            throws Exception {
        return stateAccessor.lookup(
                localStateKey,
                valueDecoder,
                historicalValueLookup == null
                        ? null
                        : () -> historicalValueLookup.lookup(primaryKey));
    }

    private boolean canSkipOldValueLookup(
            RowMerger currentMerger, AutoIncrementUpdater autoIncrementUpdater) {
        // A full-row WAL upsert does not need the old value: its result is always the incoming row,
        // and WAL mode emits only UPDATE_AFTER. Partial updates and auto-increment writes still
        // need the old value to construct the final row or distinguish an insert from an update.
        return changelogImage == ChangelogImage.WAL
                && !autoIncrementUpdater.hasAutoIncrement()
                && currentMerger instanceof DefaultRowMerger;
    }

    private static boolean shouldIgnoreDeletion(RowMerger currentMerger) {
        DeleteBehavior deleteBehavior = currentMerger.deleteBehavior();
        if (deleteBehavior == DeleteBehavior.IGNORE) {
            return true;
        } else if (deleteBehavior == DeleteBehavior.DISABLE) {
            throw new DeletionDisabledException(
                    "Delete operations are disabled for this table. "
                            + "The table.delete.behavior is set to 'disable'.");
        }
        return false;
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
                        memorySegmentPool,
                        false);
            default:
                throw new IllegalArgumentException("Unsupported log format: " + logFormat);
        }
    }

    private static final class WriteContext {
        private final Schema latestSchema;
        private final short latestSchemaId;
        private final AutoIncrementUpdater autoIncrementUpdater;
        private final RowMerger rowMerger;

        private WriteContext(
                Schema latestSchema,
                short latestSchemaId,
                AutoIncrementUpdater autoIncrementUpdater,
                RowMerger rowMerger) {
            this.latestSchema = latestSchema;
            this.latestSchemaId = latestSchemaId;
            this.autoIncrementUpdater = autoIncrementUpdater;
            this.rowMerger = rowMerger;
        }
    }
}
