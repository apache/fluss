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
import org.apache.fluss.utils.CloseableIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.Predicate;

import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
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

    private final boolean isCheckCrcs;
    private final Iterator<LogRecordBatch> batches;
    private final LogScannerStatus logScannerStatus;
    protected final LogRecordReadContext readContext;

    private LogRecordBatch currentBatch;
    // The batch a decode failure is attributed to; null while the next batch is being read from
    // the fetched data, so such a failure is not blamed on the previous batch.
    @Nullable private LogRecordBatch decodingBatch;
    // Whether decodingBatch passed ensureHeaderComplete and maybeEnsureValid, so its end can be
    // reported. A skipped CRC check leaves a complete header unverified, as decoding trusts it.
    private boolean decodingBatchHeaderTrusted;
    private int currentBatchSchemaId = -1;
    private LogRecord lastRecord;
    private CloseableIterator<LogRecord> records;
    private int recordsRead = 0;
    private Exception cachedRecordException = null;
    private boolean corruptLastRecord = false;
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
            boolean isCheckCrcs,
            long fetchOffset,
            long filteredEndOffset) {
        this.tableBucket = tableBucket;
        this.tablePath = tablePath;
        this.error = error;
        this.sizeInBytes = sizeInBytes;
        this.highWatermark = highWatermark;
        this.batches = batches;
        this.readContext = readContext;
        this.isCheckCrcs = isCheckCrcs;
        this.logScannerStatus = logScannerStatus;
        this.fetchOffset = fetchOffset;
        checkArgument(
                filteredEndOffset == NO_FILTERED_END_OFFSET || filteredEndOffset >= fetchOffset,
                "filteredEndOffset (%s) must be %s (NO_FILTERED_END_OFFSET) or >= fetchOffset (%s) for bucket %s.",
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
        if (corruptLastRecord) {
            throw decodeException(cachedRecordException);
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
            cachedRecordException = e;
            if (scanRecords.isEmpty()) {
                throw decodeException(e);
            }
        }

        return scanRecords;
    }

    private void fetchRecord(
            List<ScanRecord> scanRecords, @Nullable Predicate<LogRecord> recordFilter)
            throws Exception {
        // Only move to next record if there was no exception in the last fetch.
        if (cachedRecordException == null) {
            corruptLastRecord = true;
            lastRecord = nextFetchedRecord();
            corruptLastRecord = false;
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
            throw decodeException(cachedRecordException);
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

                ArrowBatchData arrowBatchData = batch.loadArrowBatch(readContext);
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
            cachedRecordException = e;
            if (arrowBatches.isEmpty()) {
                throw decodeException(e);
            }
        }

        return arrowBatches;
    }

    private LogRecord nextFetchedRecord() throws Exception {
        while (true) {
            if (records == null || !records.hasNext()) {
                LogRecordBatch batch = nextFetchedBatch();
                if (batch == null) {
                    return null;
                }

                currentBatchSchemaId = batch.schemaId();
                records = batch.records(readContext);
            } else {
                LogRecord record = records.next();
                // skip any records out of range.
                if (record.logOffset() >= nextFetchOffset) {
                    return record;
                }
            }
        }
    }

    private LogRecordBatch nextFetchedBatch() {
        maybeCloseRecordStream();
        decodingBatch = null;
        if (!batches.hasNext()) {
            finishFetchedBatches();
            return null;
        }

        currentBatch = batches.next();
        decodingBatch = currentBatch;
        decodingBatchHeaderTrusted = false;
        // TODO get last epoch.
        ensureHeaderComplete(currentBatch);
        maybeEnsureValid(currentBatch);
        decodingBatchHeaderTrusted = true;
        return currentBatch;
    }

    /**
     * Builds the exception for a decode failure. A failed fetch does not advance, so repeated calls
     * for the same cached failure describe the same batch.
     */
    private LogDecodeException decodeException(Exception cause) {
        long offset = decodingBatch == null ? nextFetchOffset : decodingBatch.baseLogOffset();
        OptionalLong nextOffset = OptionalLong.empty();
        // A negative base offset is never valid; resuming before 0 would mean resuming from the
        // earliest offset rather than skipping the batch.
        if (decodingBatch != null && decodingBatchHeaderTrusted && offset >= 0) {
            try {
                long batchEnd = decodingBatch.nextLogOffset();
                // A garbled header may put the end at or before the start. With a non-negative
                // base and an int offset delta, an overflowing end wraps negative, so this check
                // also rejects it.
                if (batchEnd > offset) {
                    nextOffset = OptionalLong.of(batchEnd);
                }
            } catch (RuntimeException e) {
                // E.g. the header of a file-backed batch cannot be loaded: report the original
                // failure without the end.
                LOG.debug(
                        "Cannot read the offset after the failed batch of bucket {} at offset {}.",
                        tableBucket,
                        offset,
                        e);
            }
        }
        String message =
                String.format(
                        "Failed to decode the log batch of bucket %s at offset %s. %s",
                        tableBucket,
                        offset,
                        nextOffset.isPresent()
                                ? "Subscribe the bucket at offset "
                                        + nextOffset.getAsLong()
                                        + " to skip the batch."
                                : "The offset after the failed batch is unknown.");
        return new LogDecodeException(message, cause, tableBucket, offset, nextOffset);
    }

    private void finishFetchedBatches() {
        // In batch, we preserve the last offset in a batch. By using the next offset
        // computed from the last offset in the batch, we ensure that the offset of the
        // next fetch will point to the next batch, which avoids unnecessary re-fetching
        // of the same batch (in the worst case, the scanner could get stuck fetching
        // the same batch repeatedly).
        // When filteredEndOffset is set, use the max of the batch-derived offset and
        // filteredEndOffset to skip already-scanned-and-filtered trailing batches.
        if (currentBatch != null) {
            nextFetchOffset = Math.max(currentBatch.nextLogOffset(), filteredEndOffset);
        } else if (filteredEndOffset != NO_FILTERED_END_OFFSET) {
            nextFetchOffset = filteredEndOffset;
        }
        drain();
    }

    /**
     * Checks that the batch is large enough to hold its header. Unlike the CRC check, this always
     * runs: the header of a batch smaller than it overlaps the bytes after the batch, so decoding
     * would read garbage and the batch end could point into or past healthy batches.
     */
    private static void ensureHeaderComplete(LogRecordBatch batch) {
        // Throws for an unsupported magic, whose header layout is unknown.
        int headerSize = recordBatchHeaderSize(batch.magic());
        int sizeInBytes = batch.sizeInBytes();
        if (sizeInBytes < headerSize) {
            throw new CorruptMessageException(
                    String.format(
                            "Record batch is corrupt (the size %s is smaller than its header size %s)",
                            sizeInBytes, headerSize));
        }
    }

    private void maybeEnsureValid(LogRecordBatch batch) {
        if (isCheckCrcs) {
            if (readContext.isProjectionPushDowned()) {
                LOG.debug("Skipping CRC check for column projected log record batch.");
                return;
            }
            batch.ensureValid();
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
