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
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.FetchException;
import org.apache.fluss.exception.LogOffsetOutOfRangeException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecord;
import org.apache.fluss.record.LogRecordBatch;
import org.apache.fluss.rpc.protocol.ApiError;
import org.apache.fluss.rpc.protocol.Errors;

import org.apache.fluss.utils.AbstractIterator;
import org.apache.fluss.utils.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.LinkedList;
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
    private final MetadataUpdater metadataUpdater;

    public LogFetchCollector(
            TablePath tablePath,
            LogScannerStatus logScannerStatus,
            Configuration conf,
            MetadataUpdater metadataUpdater) {
        this.tablePath = tablePath;
        this.logScannerStatus = logScannerStatus;
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
    public Map<TableBucket, CloseableIterator<ScanRecord>> collectFetch(final LogFetchBuffer logFetchBuffer) {
        Map<TableBucket, List<CloseableIterator<ScanRecord>>> fetched = new HashMap<>();

        while (!logFetchBuffer.isEmpty()) {
            CompletedFetch completedFetch = logFetchBuffer.peek();
            logFetchBuffer.setNextInLineFetch(completedFetch);

            if (completedFetch == null) {
                break;
            }

            // On error, return successfully fetched records and let next collect surface error
            if (completedFetch.error.isFailure() && !fetched.isEmpty()) {
                break;
            }

            logFetchBuffer.poll();

            // Filters out unsubscribed bucket
            if (logScannerStatus.getBucketOffset(completedFetch.tableBucket) == null) {
                continue;
            }

            if (completedFetch.error.isFailure()) {
                // Initialize to trigger error handling
                initialize(completedFetch);
                break;
            }

            fetched.computeIfAbsent(completedFetch.tableBucket, tb -> new LinkedList<>())
                    .add(fetchRecords(completedFetch));
        }

        Map<TableBucket, CloseableIterator<ScanRecord>> output = new HashMap<>();
        for (Map.Entry<TableBucket, List<CloseableIterator<ScanRecord>>> entry: fetched.entrySet()) {
            output.put(entry.getKey(), CloseableIterator.concatenate(entry.getValue()));
        }

        return output;
    }

    private CloseableIterator<ScanRecord> fetchRecords(CompletedFetch nextInLineFetch) {
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
                LOG.trace(
                        "Returning fetched records iterator at offset {} for assigned bucket {}.",
                        offset,
                        tb);

                return new ScanRecordIterator(nextInLineFetch, logScannerStatus);
            } else {
                // these records aren't next in line based on the last consumed offset, ignore them
                // they must be from an obsolete request
                LOG.warn(
                        "Ignoring fetched records for {} at offset {} since the current offset is {}",
                        nextInLineFetch.tableBucket,
                        nextInLineFetch.nextFetchOffset(),
                        offset);
            }
        }

        LOG.trace("Draining fetched records for bucket {}", nextInLineFetch.tableBucket);
        nextInLineFetch.drain();

        return CloseableIterator.emptyIterator();
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
            LOG.warn(
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
                    "Error in fetch for bucket {}: {}:{}",
                    tb,
                    error.exceptionName(),
                    error.exception(errorMessage));
            metadataUpdater.checkAndUpdateMetadata(tablePath, tb);
        } else if (error == Errors.UNKNOWN_TABLE_OR_BUCKET_EXCEPTION) {
            LOG.warn("Received unknown table or bucket error in fetch for bucket {}", tb);
            metadataUpdater.checkAndUpdateMetadata(tablePath, tb);
        } else if (error == Errors.LOG_OFFSET_OUT_OF_RANGE_EXCEPTION) {
            throw new FetchException(
                    String.format(
                            "The fetching offset %s is out of range: %s",
                            fetchOffset, error.exception(errorMessage)));
        } else if (error == Errors.AUTHORIZATION_EXCEPTION) {
            throw new AuthorizationException(errorMessage);
        } else if (error == Errors.UNKNOWN_SERVER_ERROR) {
            LOG.warn(
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

    private class ScanRecordIterator extends AbstractIterator<ScanRecord> implements CloseableIterator<ScanRecord> {
        final CompletedFetch completedFetch;
        final LogScannerStatus logScannerStatus;

        ScanRecordIterator(CompletedFetch completedFetch, LogScannerStatus logScannerStatus) {
            this.completedFetch = completedFetch;
            this.logScannerStatus = logScannerStatus;
        }

        @Override
        public void close() {
            allDone();
            completedFetch.drain();
        }

        @Override
        public ScanRecord next() {
            Long offset = logScannerStatus.getBucketOffset(completedFetch.tableBucket);
            if (completedFetch.nextFetchOffset() > offset) {
                LOG.trace(
                        "Updating fetch offset from {} to {} for bucket {} and returning records iterator",
                        offset,
                        completedFetch.nextFetchOffset(),
                        completedFetch.tableBucket);
                logScannerStatus.updateOffset(completedFetch.tableBucket, completedFetch.nextFetchOffset());
            }

            return super.next();
        }

        @Override
        protected ScanRecord makeNext() {
            if (isUnsubscribed()) {
                close();
                return null;
            }

            ScanRecord scanRecord = completedFetch.fetchRecord();

            if (scanRecord == null) {
                close();
            }

            return scanRecord;
        }

        @Override
        protected boolean initialize() {
            return LogFetchCollector.this.initialize(completedFetch) != null;
        }

        private boolean isUnsubscribed() {
            return logScannerStatus.getBucketOffset(completedFetch.tableBucket) == null;
        }
    }
}
