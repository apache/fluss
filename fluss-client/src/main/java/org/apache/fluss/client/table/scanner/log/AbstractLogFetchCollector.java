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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Shared implementation for polling completed fetches into scanner results. */
@ThreadSafe
@Internal
abstract class AbstractLogFetchCollector<T, R> {
    protected final Logger log;
    protected final TablePath tablePath;
    protected final LogScannerStatus logScannerStatus;
    private final int maxPollRecords;
    private final MetadataUpdater metadataUpdater;

    protected AbstractLogFetchCollector(
            Logger log,
            TablePath tablePath,
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater) {
        this.log = log;
        this.tablePath = tablePath;
        this.logScannerStatus = logScannerStatus;
        this.maxPollRecords = conf.getInt(ConfigOptions.CLIENT_SCANNER_LOG_MAX_POLL_RECORDS);
        this.metadataUpdater = metadataUpdater;
    }

    /**
     * Return the fetched log records, empty the record buffer and update the consumed position.
     *
     * <p>NOTE: returning empty records guarantees the consumed position are NOT updated.
     *
     * @return The fetched records per partition
     * @throws FetchException If there is OffsetOutOfRange error in fetchResponse and the
     *     defaultResetPolicy is NONE
     */
    public R collectFetch(final LogFetchBuffer logFetchBuffer) {
        Map<TableBucket, List<T>> fetched = new HashMap<>();
        int recordsRemaining = maxPollRecords;

        try {
            while (recordsRemaining > 0) {
                CompletedFetch nextInLineFetch = logFetchBuffer.nextInLineFetch();
                if (nextInLineFetch == null || nextInLineFetch.isConsumed()) {
                    CompletedFetch completedFetch = logFetchBuffer.peek();
                    if (completedFetch == null) {
                        break;
                    }

                    if (!completedFetch.isInitialized()) {
                        try {
                            logFetchBuffer.setNextInLineFetch(initialize(completedFetch));
                        } catch (Exception e) {
                            // Remove a completedFetch upon a parse with exception if
                            // (1) it contains no records, and
                            // (2) there are no fetched records with actual content
                            // preceding this exception.
                            if (fetched.isEmpty() && completedFetch.sizeInBytes == 0) {
                                logFetchBuffer.poll();
                            }
                            throw e;
                        }
                    } else {
                        logFetchBuffer.setNextInLineFetch(completedFetch);
                    }

                    logFetchBuffer.poll();
                } else {
                    List<T> records = fetchRecords(nextInLineFetch, recordsRemaining);
                    if (!records.isEmpty()) {
                        TableBucket tableBucket = nextInLineFetch.tableBucket;
                        List<T> currentRecords = fetched.get(tableBucket);
                        if (currentRecords == null) {
                            fetched.put(tableBucket, records);
                        } else {
                            // this case shouldn't usually happen because we only send one fetch
                            // at a time per bucket, but it might conceivably happen in some rare
                            // cases (such as bucket leader changes). we have to copy to a new list
                            // because the old one may be immutable
                            List<T> mergedRecords =
                                    new ArrayList<>(records.size() + currentRecords.size());
                            mergedRecords.addAll(currentRecords);
                            mergedRecords.addAll(records);
                            fetched.put(tableBucket, mergedRecords);
                        }

                        recordsRemaining -= recordCount(records);
                    }
                }
            }
        } catch (FetchException e) {
            if (fetched.isEmpty()) {
                throw e;
            }
        }

        return toResult(fetched);
    }

    /** Initialize a {@link CompletedFetch} object. */
    @Nullable
    private CompletedFetch initialize(CompletedFetch completedFetch) {
        TableBucket tb = completedFetch.tableBucket;
        ApiError error = completedFetch.error;

        try {
            if (error.isSuccess()) {
                return handleInitializeSuccess(completedFetch);
            } else {
                handleInitializeErrors(completedFetch, error.error(), error.messageWithFallback());
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

    private @Nullable CompletedFetch handleInitializeSuccess(CompletedFetch completedFetch) {
        TableBucket tb = completedFetch.tableBucket;
        long fetchOffset = completedFetch.nextFetchOffset();

        Long offset = logScannerStatus.getBucketOffset(tb);
        if (offset == null) {
            log.debug(
                    "Discarding stale fetch response for bucket {} since the expected offset is null which means the bucket has been unsubscribed.",
                    tb);
            return null;
        }
        if (offset != fetchOffset) {
            log.warn(
                    "Discarding stale fetch response for bucket {} since its offset {} does not match the expected offset {}.",
                    tb,
                    fetchOffset,
                    offset);
            return null;
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
            CompletedFetch completedFetch, Errors error, String errorMessage) {
        TableBucket tb = completedFetch.tableBucket;
        long fetchOffset = completedFetch.nextFetchOffset();
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
                            fetchOffset, error.exception(errorMessage)));
        } else if (error == Errors.AUTHORIZATION_EXCEPTION) {
            throw new AuthorizationException(errorMessage);
        } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
            log.warn(
                    "Unknown server error while fetching offset {} for bucket {}: {}",
                    fetchOffset,
                    tb,
                    error.exception(errorMessage));
        } else if (error == Errors.CORRUPT_MESSAGE) {
            throw new FetchException(
                    String.format(
                            "Encountered corrupt message when fetching offset %s for bucket %s: %s",
                            fetchOffset, tb, error.exception(errorMessage)));
        } else {
            throw new FetchException(
                    String.format(
                            "Unexpected error code %s while fetching at offset %s from bucket %s: %s",
                            error, fetchOffset, tb, error.exception(errorMessage)));
        }
    }

    protected abstract List<T> fetchRecords(CompletedFetch nextInLineFetch, int maxRecords);

    protected abstract int recordCount(List<T> fetchedRecords);

    protected abstract R toResult(Map<TableBucket, List<T>> fetchedRecords);
}
