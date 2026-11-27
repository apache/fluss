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

package org.apache.fluss.client.table.scanner.log;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.exception.CorruptMessageException;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LogDecodeException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ArrowBatchData;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.record.CompactedLogRecord;
import org.apache.fluss.record.IndexedLogRecord;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.OutOfMemoryException;
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Predicate;

import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.utils.ExceptionUtils.findThrowable;
import static org.apache.fluss.utils.Preconditions.checkArgument;

/**
 * {@link CompletedFetch} represents the result that was returned from the tablet server via a fetch
 * log request, which can be a {@link LogRecordBatch} or remote log segments path. It contains logic
 * to maintain state between calls to {@link #fetchRecords(int)}.
 */
@Internal
public abstract class CompletedFetch {
    static final Logger LOG = LoggerFactory.getLogger(CompletedFetch.class);
    static final long NO_FILTERED_END_OFFSET = -1L;

    final TableBucket tableBucket;
    final TablePath tablePath;
    final ApiError error;
    final int sizeInBytes;
    final long highWatermark;
    private final long fetchOffset;
    private final long filteredEndOffset;

    private final boolean checkBatchCrc;
    private final Iterator<LogRecordBatch> batches;
    private final LogScannerStatus logScannerStatus;
    protected final LogRecordReadContext readContext;

    private LogRecordBatch currentBatch;
    private long previousBatchEndOffset = -1L;
    private int currentBatchSchemaId = -1;
    private LogRecord lastRecord;
    private CloseableIterator<LogRecord> records;
    private int recordsRead = 0;
    private FetchException cachedRecordException;
    private boolean recordReadFailed;
    private long nextFetchOffset;
    private boolean isConsumed = false;
    private boolean initialized = false;
    private int lastFetchRecordCount;

    public CompletedFetch(
            TableBucket tableBucket,
            TablePath tablePath,
            ApiError error,
            int sizeInBytes,
            long highWatermark,
            Iterator<LogRecordBatch> batches,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean checkCrcs,
            long fetchOffset,
            long filteredEndOffset) {
        this.tableBucket = tableBucket;
        this.tablePath = tablePath;
        this.error = error;
        this.sizeInBytes = sizeInBytes;
        this.highWatermark = highWatermark;
        this.batches = batches;
        this.readContext = readContext;
        this.checkBatchCrc = checkCrcs && !readContext.isProjectionPushDowned();
        this.logScannerStatus = logScannerStatus;
        this.fetchOffset = fetchOffset;
        checkArgument(
                filteredEndOffset == NO_FILTERED_END_OFFSET || filteredEndOffset >= fetchOffset,
                "filteredEndOffset (%s) must be %s (NO_FILTERED_END_OFFSET) or >= fetchOffset (%s)"
                        + " for bucket %s.",
                filteredEndOffset,
                NO_FILTERED_END_OFFSET,
                fetchOffset,
                tableBucket);
        this.filteredEndOffset = filteredEndOffset;
        this.nextFetchOffset = fetchOffset;
    }

    // TODO: optimize this to avoid deep copying the record.
    //  refactor #fetchRecords to return an iterator which lazily deserialize
    //  from underlying record stream and arrow buffer.
    ScanRecord toScanRecord(LogRecord record) {
        long tableId = tableBucket.getTableId();
        int schemaId = currentBatchSchemaId;
        InternalRow.FieldGetter[] selectedFieldGetters =
                readContext.getSelectedFieldGetters(schemaId);

        try {
            GenericRow newRow = new GenericRow(selectedFieldGetters.length);
            InternalRow internalRow = record.getRow();
            for (int i = 0; i < selectedFieldGetters.length; i++) {
                newRow.setField(i, selectedFieldGetters[i].getFieldOrNull(internalRow));
            }

            return new ScanRecord(
                    tableId,
                    schemaId,
                    record.logOffset(),
                    record.timestamp(),
                    record.getChangeType(),
                    newRow,
                    getRecordSizeInBytes(record));
        } catch (RuntimeException e) {
            throw batchDecodeException(e, currentBatch);
        }
    }

    /**
     * Returns an approximate size in bytes for the given record, used for metrics and flow control.
     *
     * <p>For {@link IndexedLogRecord} and {@link CompactedLogRecord}, the size is read directly
     * from the record's serialized framing (includes length header and attributes overhead). For
     * other record types (e.g., arrow-format or projection-pushdown records backed by {@link
     * org.apache.fluss.record.GenericRecord}), per-record size is unavailable, so the batch-level
     * average ({@link LogRecordBatch#sizeInBytes()} / record count) is used as a fallback. Returns
     * {@code -1} if no estimate is available.
     */
    private int getRecordSizeInBytes(LogRecord record) {
        if (record instanceof IndexedLogRecord) {
            return ((IndexedLogRecord) record).getSizeInBytes();
        } else if (record instanceof CompactedLogRecord) {
            return ((CompactedLogRecord) record).getSizeInBytes();
        }
        // For GenericRecord (arrow/projected), use batch-level average
        if (currentBatch != null && currentBatch.getRecordCount() > 0) {
            return currentBatch.sizeInBytes() / currentBatch.getRecordCount();
        }
        return ScanRecord.UNKNOWN_SIZE_IN_BYTES;
    }

    boolean isConsumed() {
        return isConsumed;
    }

    boolean isInitialized() {
        return initialized;
    }

    long fetchOffset() {
        return fetchOffset;
    }

    long nextFetchOffset() {
        return nextFetchOffset;
    }

    void setInitialized() {
        this.initialized = true;
    }

    /**
     * Draining a {@link CompletedFetch} will signal that the data has been consumed and the
     * underlying resources are closed. This is somewhat analogous to {@link Closeable#close()
     * closing}, though no error will result if a caller invokes {@link #fetchRecords(int)}; an
     * empty {@link List list} will be returned instead.
     */
    void drain() {
        if (!isConsumed) {
            maybeCloseRecordStream();
            cachedRecordException = null;
            recordReadFailed = false;
            isConsumed = true;

            // we move the bucket to the end if we received some bytes.
            if (recordsRead > 0) {
                logScannerStatus.moveBucketToEnd(tableBucket);
            }
        }
    }

    /**
     * The {@link LogRecordBatch batch} of {@link LogRecord records} is converted to a {@link List
     * list} of {@link ScanRecord scan records} and returned.
     *
     * @param maxRecords The number of records to return; the number returned may be {@code 0 <=
     *     maxRecords}
     * @return {@link ScanRecord scan records}
     */
    public List<ScanRecord> fetchRecords(int maxRecords) {
        return fetchRecords(maxRecords, null);
    }

    int lastFetchRecordCount() {
        return lastFetchRecordCount;
    }

    List<ScanRecord> fetchRecords(int maxRecords, @Nullable Predicate<LogRecord> recordFilter) {
        lastFetchRecordCount = 0;
        if (recordReadFailed || cachedRecordException instanceof LogDecodeException) {
            throw cachedRecordException;
        }

        if (isConsumed) {
            return Collections.emptyList();
        }

        List<ScanRecord> scanRecords = new ArrayList<>();
        try {
            for (int i = 0; i < maxRecords; i++) {
                fetchRecord(scanRecords, recordFilter);
                if (lastRecord == null) {
                    break;
                }
            }

            // Guarantee that UPDATE_BEFORE (-U) and UPDATE_AFTER (+U) are never split
            // across two consecutive poll batches. If the last fetched record is an
            // UPDATE_BEFORE, fetch one more record so the matching UPDATE_AFTER is
            // included in the same batch. This prevents downstream converters (e.g.,
            // BinlogRowConverter) from seeing an orphaned -U when records from multiple
            // buckets are interleaved across polls.
            if (lastRecord != null && lastRecord.getChangeType() == ChangeType.UPDATE_BEFORE) {
                fetchRecord(scanRecords, recordFilter);
            }
        } catch (Exception e) {
            cachedRecordException = fetchException(e);
            if (scanRecords.isEmpty()) {
                throw cachedRecordException;
            }
        }

        return scanRecords;
    }

    private void fetchRecord(
            List<ScanRecord> scanRecords, @Nullable Predicate<LogRecord> recordFilter) {
        // Only move to next record if there was no exception in the last fetch.
        if (cachedRecordException == null) {
            recordReadFailed = true;
            lastRecord = nextFetchedRecord();
            recordReadFailed = false;
        }

        if (lastRecord == null) {
            return;
        }

        if (recordFilter == null || recordFilter.test(lastRecord)) {
            ScanRecord record = toScanRecord(lastRecord);
            scanRecords.add(record);
        }
        lastFetchRecordCount++;
        recordsRead++;
        // Per-record offset is a best-effort value; the authoritative offset
        // comes from the batch's nextLogOffset once the batch is fully consumed.
        nextFetchOffset = lastRecord.logOffset() + 1;
        cachedRecordException = null;
    }

    /**
     * The {@link LogRecordBatch batches} are loaded as {@link ArrowBatchData Arrow batches} and
     * returned.
     *
     * @param maxRecords A soft upper bound on the number of records to return. Because batches are
     *     returned whole (never split), the actual number of records may exceed this value. At
     *     least one batch is always returned if available, even if it alone exceeds the limit.
     * @return {@link ArrowBatchData Arrow batches}
     */
    List<ArrowBatchData> fetchArrowBatches(int maxRecords) {
        if (cachedRecordException != null) {
            throw cachedRecordException;
        }

        if (isConsumed) {
            return Collections.emptyList();
        }

        List<ArrowBatchData> arrowBatches = new ArrayList<>();
        int recordsFetched = 0;
        try {
            while (recordsFetched < maxRecords || arrowBatches.isEmpty()) {
                LogRecordBatch batch = nextFetchedBatch();
                if (batch == null) {
                    break;
                }

                readContext.resolveBatchSchema(batch.schemaId());
                ArrowBatchData arrowBatchData;
                try {
                    arrowBatchData = batch.loadArrowBatch(readContext);
                } catch (RuntimeException e) {
                    if (findThrowable(e, org.apache.arrow.memory.OutOfMemoryException.class)
                            .isPresent()) {
                        throw fetchException(e);
                    }
                    throw batchDecodeException(e, batch);
                }
                if (arrowBatchData.getRecordCount() == 0) {
                    arrowBatchData.close();
                    continue;
                }

                // Skip records that are before nextFetchOffset, analogous to the
                // record-level filtering in nextFetchedRecord() for the row-based path.
                long batchBaseOffset = arrowBatchData.getBaseLogOffset();
                if (batchBaseOffset < nextFetchOffset) {
                    int skipRows = (int) (nextFetchOffset - batchBaseOffset);
                    if (skipRows >= arrowBatchData.getRecordCount()) {
                        arrowBatchData.close();
                        continue;
                    }
                    arrowBatchData = arrowBatchData.sliceAndTransferOwnership(skipRows);
                }

                arrowBatches.add(arrowBatchData);
                recordsRead += arrowBatchData.getRecordCount();
                recordsFetched += arrowBatchData.getRecordCount();
                nextFetchOffset = batch.nextLogOffset();
            }
        } catch (Exception e) {
            // Deliver partial results when possible, mirroring fetchRecords() semantics.
            // The batch iterator is single-pass (batches.next()), so already-consumed
            // batches cannot be re-read. Rolling back nextFetchOffset would leave it
            // pointing at batches the iterator has already passed, causing a different
            // kind of inconsistency. Delivering partial results keeps offsets, recordsRead,
            // and the iterator position all in sync.
            cachedRecordException = fetchException(e);
            if (arrowBatches.isEmpty()) {
                throw cachedRecordException;
            }
        }

        return arrowBatches;
    }

    private LogRecord nextFetchedRecord() {
        while (true) {
            if (records != null) {
                try {
                    if (records.hasNext()) {
                        LogRecord record = records.next();
                        if (record.logOffset() >= nextFetchOffset) {
                            return record;
                        }
                        continue;
                    }
                } catch (RuntimeException e) {
                    throw batchDecodeException(e, currentBatch);
                }
            }

            LogRecordBatch batch = nextFetchedBatch();
            if (batch == null) {
                return null;
            }
            currentBatchSchemaId = batch.schemaId();
            if (batch.getRecordCount() != 0) {
                readContext.resolveBatchSchema(currentBatchSchemaId);
            }
            try {
                records = batch.records(readContext);
            } catch (RuntimeException e) {
                throw batchDecodeException(e, batch);
            }
        }
    }

    private LogRecordBatch nextFetchedBatch() {
        maybeCloseRecordStream();
        LogRecordBatch batch;
        try {
            batch = batches.hasNext() ? batches.next() : null;
        } catch (CorruptMessageException e) {
            throw decodeException(e, nextFetchOffset, OptionalLong.empty());
        }
        if (batch == null) {
            finishFetchedBatches();
            return null;
        }

        try {
            ensureHeaderComplete(batch);
            if (checkBatchCrc) {
                batch.ensureValid();
            }
            ensureValidOffsetRange(batch);
        } catch (CorruptMessageException | IllegalArgumentException e) {
            throw decodeException(e, batch.baseLogOffset(), OptionalLong.empty());
        }
        if (currentBatch != null) {
            previousBatchEndOffset = currentBatch.nextLogOffset();
        }
        currentBatch = batch;
        return batch;
    }

    private FetchException fetchException(Exception cause) {
        return cause instanceof FetchException
                ? (FetchException) cause
                : new FetchException(
                        "Failed to fetch log data for bucket " + tableBucket + ".", cause);
    }

    private FetchException batchDecodeException(RuntimeException cause, LogRecordBatch batch) {
        // File-backed reads wrap I/O failures in FlussRuntimeException.
        if ((cause instanceof FlussRuntimeException && cause.getCause() instanceof IOException)
                || findThrowable(cause, OutOfMemoryException.class).isPresent()) {
            return fetchException(cause);
        }
        long offset = batch.baseLogOffset();
        return decodeException(cause, offset, trustedNextOffset(batch, offset));
    }

    private OptionalLong trustedNextOffset(LogRecordBatch batch, long offset) {
        try {
            long nextOffset = batch.nextLogOffset();
            if (offset < 0 || nextOffset <= Math.max(offset, nextFetchOffset)) {
                return OptionalLong.empty();
            }
            // This failed fetch will be discarded, so advancing the iterator is safe.
            boolean hasFollowingBatch = batches.hasNext();
            if (hasFollowingBatch) {
                LogRecordBatch followingBatch = batches.next();
                ensureHeaderComplete(followingBatch);
                if (followingBatch.baseLogOffset() != nextOffset) {
                    return OptionalLong.empty();
                }
            } else if (previousBatchEndOffset != offset) {
                // The CRC starts at the schema ID, so the last batch needs a separate base check.
                return OptionalLong.empty();
            }
            boolean trusted =
                    (readContext.isProjectionPushDowned() && hasFollowingBatch)
                            || checkBatchCrc
                            || batch.isValid();
            return trusted ? OptionalLong.of(nextOffset) : OptionalLong.empty();
        } catch (RuntimeException e) {
            LOG.debug(
                    "Cannot verify the offset after the failed batch of bucket {} at offset {}.",
                    tableBucket,
                    offset,
                    e);
            return OptionalLong.empty();
        }
    }

    private LogDecodeException decodeException(
            Exception cause, long offset, OptionalLong nextOffset) {
        String message =
                String.format(
                        "Failed to decode the log batch of bucket %s at offset %s. %s",
                        tableBucket,
                        offset,
                        nextOffset.isPresent()
                                ? "Subscribe the bucket at offset "
                                        + nextOffset.getAsLong()
                                        + " to skip the batch."
                                : "Resume offset unavailable.");
        return new LogDecodeException(message, cause, tableBucket, offset, nextOffset);
    }

    private void finishFetchedBatches() {
        // Batch boundaries include batches with no visible rows. The server can also advance
        // the cursor past trailing batches pruned by a filter.
        if (currentBatch != null) {
            nextFetchOffset = Math.max(currentBatch.nextLogOffset(), filteredEndOffset);
        } else if (filteredEndOffset != NO_FILTERED_END_OFFSET) {
            nextFetchOffset = filteredEndOffset;
        }
        drain();
    }

    // An undersized batch can otherwise read header fields from the following batch.
    private static void ensureHeaderComplete(LogRecordBatch batch) {
        int headerSize = recordBatchHeaderSize(batch.magic());
        int sizeInBytes = batch.sizeInBytes();
        if (sizeInBytes < headerSize) {
            throw new CorruptMessageException(
                    String.format(
                            "Record batch is corrupt (the size %s is smaller than its header size"
                                    + " %s)",
                            sizeInBytes, headerSize));
        }
    }

    private static void ensureValidOffsetRange(LogRecordBatch batch) {
        long baseOffset = batch.baseLogOffset();
        long nextOffset = batch.nextLogOffset();
        int recordCount = batch.getRecordCount();
        // Empty CDC batches can occupy an offset without emitting a record.
        if (baseOffset < 0
                || nextOffset <= baseOffset
                || recordCount < 0
                || recordCount > nextOffset - baseOffset) {
            throw new CorruptMessageException("Invalid log batch offset range at " + baseOffset);
        }
    }

    private void maybeCloseRecordStream() {
        if (records != null) {
            // release underlying resources
            records.close();
            records = null;
        }
    }
}
