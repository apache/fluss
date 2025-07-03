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

package com.alibaba.fluss.client.table.scanner.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.client.metadata.MetadataUpdater;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.config.ConfigOptions;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.exception.AuthorizationException;
import com.alibaba.fluss.exception.FetchException;
import com.alibaba.fluss.exception.LogOffsetOutOfRangeException;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecord;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.rpc.protocol.ApiError;
import com.alibaba.fluss.rpc.protocol.Errors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * {@link LogFetchCollector} operates at the {@link LogRecordBatch} level, as that is what is stored
 * in the {@link LogFetchBuffer}. Each {@link LogRecord} in the {@link LogRecordBatch} is converted
 * to a {@link ScanRecord} and added to the returned {@link LogFetcher}.
 */
@ThreadSafe
@Internal
public class LogFetchCollector {
    private static final Logger LOG = LoggerFactory.getLogger(LogFetchCollector.class);

    private final TablePath tablePath;
    private final LogScannerStatus logScannerStatus;
    private final int maxPollRecords;
    private final MetadataUpdater metadataUpdater;

    public LogFetchCollector(
            TablePath tablePath,
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater) {
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
     * @throws LogOffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *     the defaultResetPolicy is NONE
     */
    public Map<TableBucket, List<ScanRecord>> collectFetch(final LogFetchBuffer logFetchBuffer) {
        Map<TableBucket, List<ScanRecord>> fetched = new HashMap<>();
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
                            // (2) there are no fetched records with actual content preceding this
                            // exception.
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
                    List<ScanRecord> records = fetchRecords(nextInLineFetch, recordsRemaining);
                    if (!records.isEmpty()) {
                        TableBucket tableBucket = nextInLineFetch.tableBucket;
                        List<ScanRecord> currentRecords = fetched.get(tableBucket);
                        if (currentRecords == null) {
                            fetched.put(tableBucket, records);
                        } else {
                            // this case shouldn't usually happen because we only send one fetch at
                            // a time per bucket, but it might conceivably happen in some rare
                            // cases (such as bucket leader changes). we have to copy to a new list
                            // because the old one may be immutable
                            List<ScanRecord> newScanRecords =
                                    new ArrayList<>(records.size() + currentRecords.size());
                            newScanRecords.addAll(currentRecords);
                            newScanRecords.addAll(records);
                            fetched.put(tableBucket, newScanRecords);
                        }

                        recordsRemaining -= records.size();
                    }
                }
            }
        } catch (FetchException e) {
            if (fetched.isEmpty()) {
                throw e;
            }
        }

        return fetched;
    }

    private List<ScanRecord> fetchRecords(CompletedFetch nextInLineFetch, int maxRecords) {
        TableBucket tb = nextInLineFetch.tableBucket;
        Long offset = logScannerStatus.getBucketOffset(tb);
        if (offset == null) {
            LOG.debug(
                    "Ignoring fetched records for {} at offset {} since the current offset is null which means the "
                            + "bucket has been unsubscribe.",
                    tb,
                    nextInLineFetch.nextFetchOffset());
        } else {
            if (nextInLineFetch.nextFetchOffset() == offset) {
                List<ScanRecord> records = nextInLineFetch.fetchRecords(maxRecords);
                LOG.trace(
                        "Returning {} fetched records at offset {} for assigned bucket {}.",
                        records.size(),
                        offset,
                        tb);

                if (nextInLineFetch.nextFetchOffset() > offset) {
                    LOG.trace(
                            "Updating fetch offset from {} to {} for bucket {} and returning {} records from poll()",
                            offset,
                            nextInLineFetch.nextFetchOffset(),
                            tb,
                            records.size());
                    logScannerStatus.updateOffset(tb, nextInLineFetch.nextFetchOffset());
                }
                return records;
            } else {
                // these records aren't next in line based on the last consumed offset, ignore them
                // they must be from an obsolete request
                LOG.debug(
                        "Ignoring fetched records for {} at offset {} since the current offset is {}",
                        nextInLineFetch.tableBucket,
                        nextInLineFetch.nextFetchOffset(),
                        offset);
            }
        }

        LOG.trace("Draining fetched records for bucket {}", nextInLineFetch.tableBucket);
        nextInLineFetch.drain();

        return Collections.emptyList();
    }

    /** Initialize a {@link CompletedFetch} object. */
    private @Nullable CompletedFetch initialize(CompletedFetch completedFetch) {
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

        // we are interested in this fetch only if the beginning offset matches the
        // current consumed position.
        Long offset = logScannerStatus.getBucketOffset(tb);
        if (offset == null) {
            LOG.debug(
                    "Discarding stale fetch response for bucket {} since the expected offset is null which means the bucket has been "
                            + "unsubscribed.",
                    tb);
            return null;
        }
        if (offset != fetchOffset) {
            LOG.debug(
                    "Discarding stale fetch response for bucket {} since its offset {} does not match the expected offset {}.",
                    tb,
                    fetchOffset,
                    offset);
            return null;
        }

        long highWatermark = completedFetch.highWatermark;
        if (highWatermark >= 0) {
            LOG.trace("Updating high watermark for bucket {} to {}.", tb, highWatermark);
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
            LOG.debug(
                    "Error in fetch for bucket {}: {}:{}", tb, error.exceptionName(), errorMessage);
            metadataUpdater.checkAndUpdateMetadata(tablePath, tb);
        } else if (error == Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION) {
            LOG.warn("Received unknown table or bucket error in fetch for bucket {}", tb);
            metadataUpdater.checkAndUpdateMetadata(tablePath, tb);
        } else if (error == Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION) {
            throw new FetchException(
                    String.format("The fetching offset %s is out of range", fetchOffset),
                    error.exception());
        } else if (error == Errors.AUTHORIZATION_EXCEPTION) {
            throw new AuthorizationException(errorMessage);
        } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
            LOG.warn(
                    "Unknown server error while fetching offset {} for bucket {}: {}",
                    fetchOffset,
                    tb,
                    errorMessage);
        } else if (error == Errors.CORRUPT_MESSAGE) {
            throw new FetchException(
                    String.format(
                            "Encountered corrupt message when fetching offset %s for bucket %s: %s",
                            fetchOffset, tb, errorMessage));
        } else {
            throw new FetchException(
                    String.format(
                            "Unexpected error code %s while fetching at offset %s from bucket %s: %s",
                            error, fetchOffset, tb, errorMessage));
        }
    }
}
