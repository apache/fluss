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
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;

import org.slf4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.utils.Preconditions.checkState;

/** Shared implementation for polling completed fetches into scanner results. */
@ThreadSafe
@Internal
abstract class AbstractLogFetchCollector<T, R> {
    protected final Logger log;
    protected final LogScannerStatus logScannerStatus;
    private final int maxPollRecords;
    private final MetadataUpdater metadataUpdater;

    protected AbstractLogFetchCollector(
            Logger log,
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater) {
        this.log = log;
        this.logScannerStatus = logScannerStatus;
        this.maxPollRecords = conf.getInt(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS);
        this.metadataUpdater = metadataUpdater;
    }

    /**
     * Return the fetched log records, empty the record buffer and update the consumed position.
     *
     * <p>NOTE: empty record lists may still advance the consumed position.
     *
     * @return The fetched records per partition
     * @throws FetchException If there is OffsetOutOfRange error in fetchResponse and the
     *     defaultResetPolicy is NONE
     */
    public R collectFetch(final LogFetchBuffer logFetchBuffer) {
        PollAccumulator result = new PollAccumulator();

        try {
            while (result.recordsRemaining > 0) {
                CompletedFetch nextInLineFetch = logFetchBuffer.nextInLineFetch();
                if (nextInLineFetch == null || nextInLineFetch.isConsumed()) {
                    CompletedFetch completedFetch = logFetchBuffer.peek();
                    if (completedFetch == null) {
                        break;
                    }

                    if (!completedFetch.isInitialized()) {
                        try {
                            CompletedFetch initialized = initialize(completedFetch, result);
                            logFetchBuffer.setNextInLineFetch(initialized);
                            if (initialized == null) {
                                completedFetch.drain();
                            }
                        } catch (Exception e) {
                            // Deferred errors remain queued. Immediate failures are discarded only
                            // when the failure policy says this response must not be retried.
                            if (result.shouldPropagateImmediately(e)
                                    && result.shouldDiscardFailedFetch(e, completedFetch)) {
                                CompletedFetch removed = logFetchBuffer.poll();
                                checkState(
                                        removed == completedFetch,
                                        "Expected failed fetch %s at the head of the buffer, but found %s.",
                                        completedFetch,
                                        removed);
                                try {
                                    completedFetch.drain();
                                } catch (RuntimeException cleanupException) {
                                    e.addSuppressed(cleanupException);
                                }
                            }
                            throw e;
                        }
                    } else {
                        logFetchBuffer.setNextInLineFetch(completedFetch);
                    }

                    logFetchBuffer.poll();
                } else {
                    TableBucket tableBucket = nextInLineFetch.tableBucket;
                    long stoppingOffset = logScannerStatus.getBucketStoppingOffset(tableBucket);
                    boolean bounded = stoppingOffset != LogScanner.NO_STOPPING_OFFSET;
                    Long offsetBeforeFetch =
                            bounded ? logScannerStatus.getBucketOffset(tableBucket) : null;
                    List<T> records = fetchRecords(nextInLineFetch, result.recordsRemaining);

                    if (bounded) {
                        Long offsetAfterFetch = logScannerStatus.getBucketOffset(tableBucket);
                        if (offsetBeforeFetch != null
                                && offsetAfterFetch != null
                                && !offsetBeforeFetch.equals(offsetAfterFetch)) {
                            result.recordProgress(tableBucket, offsetAfterFetch, true);
                        }
                    } else {
                        // Preserve the existing unbounded behavior: every consumed fetch
                        // contributes its next fetch offset to the poll result.
                        result.recordProgress(
                                tableBucket, nextInLineFetch.nextFetchOffset(), false);
                    }

                    result.addRecords(tableBucket, records, bounded);
                }
            }
        } catch (Exception e) {
            if (result.shouldPropagateImmediately(e)) {
                // Release any off-heap resources held by accumulated records. Pending completion
                // events remain in scanner status until a poll successfully returns them.
                closeFetchedRecords(result.fetched);
                throw e;
            }
        }

        Set<TableBucket> finishedBuckets = logScannerStatus.drainFinishedBuckets();
        return toResult(result.fetched, result.consumedUpToOffsets, finishedBuckets);
    }

    /** Initialize a {@link CompletedFetch} object. */
    @Nullable
    private CompletedFetch initialize(CompletedFetch completedFetch, PollAccumulator result) {
        TableBucket tb = completedFetch.tableBucket;
        if (logScannerStatus.hasReachedStoppingOffset(tb)) {
            log.trace("Discarding fetch response for finished bounded bucket {}.", tb);
            return null;
        }
        ApiError error = completedFetch.error;

        try {
            if (error.isSuccess()) {
                return handleInitializeSuccess(completedFetch, result);
            } else {
                handleInitializeErrors(
                        completedFetch,
                        error.error(),
                        error.messageWithFallback(),
                        completedFetch.tablePath);
                return null;
            }
        } finally {
            if (error.isFailure()) {
                // we move the bucket to the end if there was an error. This way,
                // it's more likely that buckets for the same table can remain together
                // (allowing for more efficient serialization).
                logScannerStatus.moveBucketToEnd(tb);
            }
        }
    }

    private @Nullable CompletedFetch handleInitializeSuccess(
            CompletedFetch completedFetch, PollAccumulator result) {
        TableBucket tb = completedFetch.tableBucket;
        long requestedFetchOffset = completedFetch.requestedFetchOffset();

        // we are interested in this fetch only if the beginning offset matches the
        // current consumed position.
        Long currentOffset = logScannerStatus.getBucketOffset(tb);
        if (currentOffset == null) {
            log.debug(
                    "Discarding stale fetch response for bucket {} since the expected offset is null which means the bucket has been unsubscribed.",
                    tb);
            return null;
        }
        if (currentOffset != requestedFetchOffset) {
            log.warn(
                    "Discarding stale fetch response for bucket {} since its offset {} does not match the expected offset {}.",
                    tb,
                    requestedFetchOffset,
                    currentOffset);
            return null;
        }

        long stoppingOffset = logScannerStatus.getBucketStoppingOffset(tb);
        boolean bounded = stoppingOffset != LogScanner.NO_STOPPING_OFFSET;

        if (bounded && requestedFetchOffset == LogScanner.EARLIEST_OFFSET) {
            long resolvedEarliestOffset = completedFetch.resolvedEarliestOffset();
            if (resolvedEarliestOffset < 0) {
                throw new UnsupportedBoundedEarliestException(
                        "Bounded scanning from EARLIEST_OFFSET requires a server that reports resolved_earliest_offset."
                                + " Upgrade the server or use an explicit starting offset.");
            }
            // Physical cursor used while reading this CompletedFetch.
            completedFetch.applyResolvedEarliestOffset();
            // Logical bounded cursor owned by LogScannerStatus.
            Long logicalOffset =
                    logScannerStatus.resolveBoundedStartingOffset(
                            tb, requestedFetchOffset, resolvedEarliestOffset);
            if (logicalOffset == null) {
                return null;
            }
            result.recordProgress(tb, logicalOffset, true);
            if (logScannerStatus.hasReachedStoppingOffset(tb)) {
                return null;
            }
        }

        long highWatermark = completedFetch.highWatermark;
        if (highWatermark >= 0) {
            log.trace("Updating high watermark for bucket {} to {}.", tb, highWatermark);
            logScannerStatus.updateHighWatermark(tb, highWatermark);
        }

        completedFetch.setInitialized();
        return completedFetch;
    }

    private void handleInitializeErrors(
            CompletedFetch completedFetch, Errors error, String errorMessage, TablePath tablePath) {
        TableBucket tb = completedFetch.tableBucket;
        long requestedFetchOffset = completedFetch.requestedFetchOffset();
        if (error == Errors.NOT_LEADER_OR_FOLLOWER
                || error == Errors.LOG_STORAGE_EXCEPTION
                || error == Errors.KV_STORAGE_EXCEPTION
                || error == Errors.STORAGE_EXCEPTION
                || error == Errors.FENCED_LEADER_EPOCH_EXCEPTION) {
            log.debug(
                    "Error in fetch for bucket {}: {}:{}",
                    tb,
                    error.exceptionName(),
                    error.exception(errorMessage));
            metadataUpdater.checkAndUpdateMetadata(tablePath, tb);
        } else if (error == Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION) {
            log.warn("Received unknown table or bucket error in fetch for bucket {}", tb);
            metadataUpdater.checkAndUpdateMetadata(tablePath, tb);
        } else if (error == Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION) {
            throw new FetchException(
                    String.format(
                            "The fetching offset %s is out of range: %s",
                            requestedFetchOffset, error.exception(errorMessage)));
        } else if (error == Errors.AUTHORIZATION_EXCEPTION) {
            throw new AuthorizationException(errorMessage);
        } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
            log.warn(
                    "Unknown server error while fetching offset {} for bucket {}: {}",
                    requestedFetchOffset,
                    tb,
                    error.exception(errorMessage));
        } else if (error == Errors.CORRUPT_MESSAGE) {
            throw new FetchException(
                    String.format(
                            "Encountered corrupt message when fetching offset %s for bucket %s: %s",
                            requestedFetchOffset, tb, error.exception(errorMessage)));
        } else {
            throw new FetchException(
                    String.format(
                            "Unexpected error code %s while fetching at offset %s from bucket %s: %s",
                            error, requestedFetchOffset, tb, error.exception(errorMessage)));
        }
    }

    protected List<T> fetchRecords(CompletedFetch nextInLineFetch, int maxRecords) {
        TableBucket tb = nextInLineFetch.tableBucket;
        Long offset = logScannerStatus.getBucketOffset(tb);
        if (offset == null) {
            log.debug(
                    "Ignoring fetched records for {} at offset {} since the current offset is null which means the bucket has been unsubscribed.",
                    tb,
                    nextInLineFetch.requestedFetchOffset());
        } else if (logScannerStatus.hasReachedStoppingOffset(tb)) {
            log.trace("Ignoring fetched records for finished bounded bucket {}.", tb);
        } else if (nextInLineFetch.nextFetchOffset() == offset) {
            long stoppingOffset = logScannerStatus.getBucketStoppingOffset(tb);
            boolean bounded = stoppingOffset != LogScanner.NO_STOPPING_OFFSET;

            List<T> records = doFetchRecords(nextInLineFetch, maxRecords);
            long rawConsumedUpToOffset = nextInLineFetch.nextFetchOffset();

            if (bounded && !records.isEmpty()) {
                records = trimFetchedRecords(records, stoppingOffset);
            }

            log.trace(
                    "Returning {} fetched records at offset {} for assigned bucket {}.",
                    records.size(),
                    offset,
                    tb);

            long consumedUpToOffset =
                    bounded
                            ? Math.min(rawConsumedUpToOffset, stoppingOffset)
                            : rawConsumedUpToOffset;

            if (consumedUpToOffset > offset) {
                log.trace(
                        "Updating fetch offset from {} to {} for bucket {} and returning {} records "
                                + "from poll()",
                        offset,
                        consumedUpToOffset,
                        tb,
                        records.size());

                logScannerStatus.updateOffset(tb, consumedUpToOffset);
            }

            if (bounded && rawConsumedUpToOffset >= stoppingOffset) {
                log.trace(
                        "Reached stopping offset {} for bucket {}, draining remaining fetched records.",
                        stoppingOffset,
                        tb);
                nextInLineFetch.drain();
            }

            return records;
        } else {
            // these records aren't next in line based on the last consumed offset, ignore them
            // they must be from an obsolete request
            log.warn(
                    "Ignoring fetched records for {} at offset {} since the current offset is {}",
                    nextInLineFetch.tableBucket,
                    nextInLineFetch.nextFetchOffset(),
                    offset);
        }

        log.trace("Draining fetched records for bucket {}", nextInLineFetch.tableBucket);
        nextInLineFetch.drain();
        return Collections.emptyList();
    }

    /**
     * Fetch records from the given {@link CompletedFetch}. Subclasses implement this to call the
     * appropriate method on {@link CompletedFetch} for their record type.
     */
    protected abstract List<T> doFetchRecords(CompletedFetch nextInLineFetch, int maxRecords);

    protected abstract int recordCount(List<T> fetchedRecords);

    protected abstract R toResult(
            Map<TableBucket, List<T>> fetchedRecords,
            Map<TableBucket, Long> consumedUpToOffsets,
            Set<TableBucket> finishedBuckets);

    protected abstract List<T> trimFetchedRecords(List<T> fetchedRecords, long stoppingOffset);

    /**
     * Release resources held by fetched records on failure. The default implementation is a no-op,
     * suitable for record types that are plain Java objects. Subclasses whose record types hold
     * off-heap resources (e.g. Arrow buffers) should override this to close them.
     */
    protected void closeFetchedRecords(Map<TableBucket, List<T>> fetched) {}

    private final class PollAccumulator {
        private final Map<TableBucket, List<T>> fetched = new HashMap<>();
        private final Map<TableBucket, Long> consumedUpToOffsets = new HashMap<>();

        private int recordsRemaining = maxPollRecords;
        private boolean hasBoundedAccumulatedResult;

        private boolean hasDeliverableResult() {
            return !fetched.isEmpty()
                    || !consumedUpToOffsets.isEmpty()
                    || logScannerStatus.hasPendingFinishedBuckets();
        }

        private boolean shouldPropagateImmediately(Exception exception) {
            if (exception instanceof FetchException) {
                // Preserve the existing FetchException behavior: return any already
                // accumulated records, progress, or bounded completion first.
                return !hasDeliverableResult();
            }

            // Non-FetchException historically propagates immediately for unbounded
            // scans. Defer it only when this poll has already consumed bounded data
            // or progress that must be delivered before the error.
            return !hasBoundedAccumulatedResult;
        }

        private boolean shouldDiscardFailedFetch(
                Exception exception, CompletedFetch completedFetch) {
            if (exception instanceof UnsupportedBoundedEarliestException) {
                return true;
            }

            return fetched.isEmpty()
                    && consumedUpToOffsets.isEmpty()
                    && completedFetch.sizeInBytes == 0;
        }

        private void recordProgress(TableBucket tableBucket, long offset, boolean bounded) {
            consumedUpToOffsets.merge(tableBucket, offset, Math::max);
            hasBoundedAccumulatedResult |= bounded;
        }

        private void addRecords(TableBucket tableBucket, List<T> records, boolean bounded) {
            if (records.isEmpty()) {
                return;
            }
            List<T> current = fetched.get(tableBucket);
            if (current == null) {
                fetched.put(tableBucket, records);
            } else {
                List<T> merged = new ArrayList<>(current.size() + records.size());
                merged.addAll(current);
                merged.addAll(records);
                fetched.put(tableBucket, merged);
            }
            recordsRemaining -= recordCount(records);
            hasBoundedAccumulatedResult |= bounded;
        }
    }

    private static final class UnsupportedBoundedEarliestException
            extends UnsupportedOperationException {

        private UnsupportedBoundedEarliestException(String message) {
            super(message);
        }
    }
}
