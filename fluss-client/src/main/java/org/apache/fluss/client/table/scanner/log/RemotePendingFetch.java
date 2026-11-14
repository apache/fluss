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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.FileLogRecords;
import org.apache.fluss.record.LogRecordReadContext;
import org.apache.fluss.remote.RemoteLogSegment;
import org.apache.fluss.rpc.entity.FetchLogResultForBucket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * {@link RemotePendingFetch} is a {@link PendingFetch} that represents a pending fetch that waiting
 * for the remote log file fetched to local disk.
 */
class RemotePendingFetch implements PendingFetch {
    private static final Logger LOG = LoggerFactory.getLogger(RemotePendingFetch.class);

    final RemoteLogSegment remoteLogSegment;

    private final RemoteLogDownloadFuture downloadFuture;
    private final TablePath tablePath;

    private final int posInLogSegment;
    private final long fetchOffset;
    private final long highWatermark;
    private final LogRecordReadContext readContext;
    private final LogScannerStatus logScannerStatus;
    private final boolean isCheckCrc;

    private final LogScannerStatus.SubscriptionSnapshot subscription;

    private Throwable failure;
    private volatile boolean callbackCompleted;

    RemotePendingFetch(
            RemoteLogSegment remoteLogSegment,
            RemoteLogDownloadFuture downloadFuture,
            TablePath tablePath,
            int posInLogSegment,
            long fetchOffset,
            long highWatermark,
            LogRecordReadContext readContext,
            LogScannerStatus logScannerStatus,
            boolean isCheckCrc,
            LogScannerStatus.SubscriptionSnapshot subscription) {
        this.remoteLogSegment = remoteLogSegment;
        this.downloadFuture = downloadFuture;
        this.tablePath = tablePath;
        this.posInLogSegment = posInLogSegment;
        this.fetchOffset = fetchOffset;
        this.highWatermark = highWatermark;
        this.readContext = readContext;
        this.logScannerStatus = logScannerStatus;
        this.isCheckCrc = isCheckCrc;
        this.subscription = subscription;
    }

    void complete(Throwable error) {
        this.failure = error;
        this.callbackCompleted = true;
    }

    boolean isBounded() {
        return subscription != null && subscription.isBounded();
    }

    @Override
    public TableBucket tableBucket() {
        return remoteLogSegment.tableBucket();
    }

    @Override
    public boolean isObsolete() {
        if (subscription == null) {
            return false;
        }
        LogScannerStatus.SubscriptionSnapshot current =
                logScannerStatus.subscriptionSnapshot(tableBucket());

        return current == null
                || current.stoppingOffset != subscription.stoppingOffset
                || (current.isBounded()
                        && (current.stoppingOffset == 0
                                || current.offset >= current.stoppingOffset));
    }

    @Override
    public boolean isCompleted() {
        return callbackCompleted;
    }

    @Override
    public CompletedFetch toCompletedFetch() {
        if (failure != null) {
            discard();
            return failedFetch(failure);
        }
        FileLogRecords records;
        try {
            records = downloadFuture.getFileLogRecords(posInLogSegment);
        } catch (RuntimeException e) {
            discard();
            return failedFetch(e);
        }
        try {
            CompletedFetch completed =
                    new RemoteCompletedFetch(
                            tableBucket(),
                            tablePath,
                            records,
                            highWatermark,
                            readContext,
                            logScannerStatus,
                            isCheckCrc,
                            fetchOffset,
                            downloadFuture.getRecycleCallback());
            if (subscription != null) {
                completed.bindSubscription(subscription, -1L);
            }
            return completed;
        } catch (RuntimeException e) {
            try {
                records.closeHandlers();
            } catch (IOException closeError) {
                LOG.warn("Failed to close remote log records for {}.", tableBucket(), closeError);
            }
            discard();
            return failedFetch(e);
        }
    }

    private CompletedFetch failedFetch(Throwable cause) {
        CompletedFetch completed =
                new DefaultCompletedFetch(
                        tableBucket(),
                        tablePath,
                        FetchLogResultForBucket.empty(tableBucket(), highWatermark, -1L),
                        readContext,
                        logScannerStatus,
                        isCheckCrc,
                        fetchOffset,
                        null);

        if (subscription != null) {
            completed.bindSubscription(subscription, -1L);
        }
        completed.setFailure(cause);
        return completed;
    }

    @Override
    public void discard() {
        downloadFuture.discard();
    }

    @Override
    public String toString() {
        return "RemotePendingFetch{"
                + "remoteLogSegment="
                + remoteLogSegment
                + ", fetchOffset="
                + fetchOffset
                + ", posInLogSegment="
                + posInLogSegment
                + ", highWatermark="
                + highWatermark
                + '}';
    }
}
