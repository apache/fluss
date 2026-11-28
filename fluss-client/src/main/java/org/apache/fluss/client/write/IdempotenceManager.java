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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.client.metadata.MetadataUpdater;
import org.apache.fluss.exception.ApiException;
import org.apache.fluss.exception.AuthorizationException;
import org.apache.fluss.exception.InvalidMetadataException;
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.exception.UnknownWriterIdException;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.rpc.gateway.TabletServerGateway;
import org.apache.fluss.rpc.messages.InitWriterRequest;
import org.apache.fluss.rpc.protocol.Errors;
import org.apache.fluss.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;
import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * A class which manages the idempotence in writer keeps the state necessary to ensure idempotent of
 * writer.
 */
@Internal
@ThreadSafe
public class IdempotenceManager {
    private static final Logger LOG = LoggerFactory.getLogger(IdempotenceManager.class);

    // retry config for requesting writerId, maybe make it as configurable
    private static final int RETRY_TIMES = 3;
    private static final int RETRY_INTERVAL_MS = 100;

    private final boolean idempotenceEnabled;
    private final boolean recoverOnOutOfOrderSequence;
    private final IdempotenceBucketMap idempotenceBucketMap;
    private final int maxInflightRequestsPerBucket;
    private final TabletServerGateway tabletServerGateway;
    private final MetadataUpdater metadataUpdater;

    private volatile long writerId;
    @Nullable private volatile ApiException recoveryCause;
    private volatile boolean hasUnresolvedRequest;

    public IdempotenceManager(
            boolean idempotenceEnabled,
            int maxInflightRequestsPerBucket,
            TabletServerGateway tabletServerGateway,
            MetadataUpdater metadataUpdater,
            boolean recoverOnOutOfOrderSequence) {
        this.idempotenceEnabled = idempotenceEnabled;
        this.recoverOnOutOfOrderSequence = recoverOnOutOfOrderSequence;
        this.maxInflightRequestsPerBucket = maxInflightRequestsPerBucket;
        this.idempotenceBucketMap = new IdempotenceBucketMap();
        this.tabletServerGateway = tabletServerGateway;
        this.writerId = NO_WRITER_ID;
        this.metadataUpdater = metadataUpdater;
    }

    boolean idempotenceEnabled() {
        return idempotenceEnabled;
    }

    boolean recoveryEnabled() {
        return idempotenceEnabled && recoverOnOutOfOrderSequence;
    }

    boolean isRecoveringWriterId() {
        return recoveryCause != null;
    }

    void recordUncertainAttempt(WriteBatch batch, boolean requestMayStillArrive) {
        batch.markDeliveryUncertain();
        if (requestMayStillArrive) {
            // The original RPC can still overwrite a newer update after its retry succeeds.
            preventWriterIdRecovery();
        }
    }

    void preventWriterIdRecovery() {
        hasUnresolvedRequest = true;
    }

    synchronized boolean requestWriterIdRecovery(ReadyWriteBatch ready, ApiException cause) {
        WriteBatch batch = ready.writeBatch();
        if (!recoveryEnabled()
                || hasUnresolvedRequest
                || !hasWriterId(batch.writerId())
                || batch.batchSequence() <= 0
                || !isNextSequence(ready.tableBucket(), batch.batchSequence())
                || batch.hasUncertainDelivery()) {
            return false;
        }
        if (recoveryCause == null) {
            recoveryCause = cause;
        }
        return true;
    }

    /** The sender must wait for all in-flight write callbacks before resetting sequences. */
    void recoverWriterId(Set<PhysicalTablePath> tables) throws Throwable {
        synchronized (this) {
            if (hasUnresolvedRequest || !idempotenceBucketMap.canRecoverWriterId()) {
                throw recoveryCause;
            }
        }
        long newWriterId = requestWriterId(tables);
        LOG.info(
                "Recovering writer {} with new writer ID {} after {}.",
                writerId,
                newWriterId,
                recoveryCause.getClass().getSimpleName());
        synchronized (this) {
            idempotenceBucketMap.restartSequences(newWriterId);
            setWriterId(newWriterId);
            recoveryCause = null;
        }
    }

    boolean hasWriterId(long writerId) {
        return this.writerId == writerId;
    }

    long writerId() {
        return writerId;
    }

    void setWriterId(long writerId) {
        LOG.info("WriterId set to {}", writerId);
        this.writerId = writerId;
    }

    boolean isWriterIdValid() {
        return writerId > NO_WRITER_ID;
    }

    synchronized void maybeUpdateWriterId(TableBucket tableBucket) {
        if (hasStaleWriterId(tableBucket) && !hasInflightBatches(tableBucket)) {
            // If the batch was on a different id and all its in-flight batches have completed,
            // reset the bucket batch sequence so that the next batch (with the new epoch) starts
            // from 0.
            idempotenceBucketMap.startBatchSequencesAtBeginning(tableBucket, writerId);
            LOG.debug(
                    "Writer id of bucket {} set to {}. Reinitialize batch sequence at beginning.",
                    tableBucket,
                    writerId);
        }
    }

    /**
     * This method is used when writer needs to reset its internal state because of an irrecoverable
     * exception from the tablet server.
     *
     * <p>We need to reset writer id and associated state when we have sent a batch to the tablet
     * server, but we either get a non-retriable exception or we run out of retries, or the batch
     * expired in writer queue after it was already sent to the tablet server.
     *
     * <p>In all of these cases, we don't know whether batch was actually committed on the tablet
     * server, and hence whether the batch sequence was actually updated. If we don't reset the
     * writer state, we risk the chance that all future messages will return an {@link
     * OutOfOrderSequenceException}.
     */
    synchronized void resetWriterId() {
        setWriterId(NO_WRITER_ID);
        this.idempotenceBucketMap.reset();
    }

    synchronized boolean hasStaleWriterId(TableBucket tableBucket) {
        return writerId != idempotenceBucketMap.getOrCreate(tableBucket).writerId();
    }

    synchronized int nextSequence(TableBucket tableBucket) {
        return idempotenceBucketMap.getOrCreate(tableBucket).nextSequence();
    }

    synchronized boolean tryAssignSequence(WriteBatch batch, TableBucket tableBucket) {
        if (!isWriterIdValid() || isRecoveringWriterId()) {
            return false;
        }
        maybeUpdateWriterId(tableBucket);
        IdempotenceBucketEntry entry = idempotenceBucketMap.get(tableBucket);
        batch.setWriterState(writerId, entry.nextSequence());
        entry.incrementSequence();
        entry.addInflightBatch(batch);
        LOG.debug(
                "Assigned writer ID {} and sequence {} to batch for {}",
                writerId,
                batch.batchSequence(),
                tableBucket);
        return true;
    }

    synchronized WriteBatch nextBatchBySequence(TableBucket tableBucket) {
        return idempotenceBucketMap.nextBatchBySequence(tableBucket);
    }

    synchronized boolean isNextSequence(TableBucket tableBucket, int sequence) {
        return sequence
                        - lastAckedBatchSequence(tableBucket)
                                .orElse(IdempotenceBucketEntry.NO_LAST_ACKED_BATCH_SEQUENCE)
                == 1;
    }

    synchronized Optional<Integer> lastAckedBatchSequence(TableBucket tableBucket) {
        return idempotenceBucketMap.lastAckedBatchSequence(tableBucket);
    }

    synchronized boolean tryReclaimRejectedSequence(ReadyWriteBatch ready) {
        WriteBatch batch = ready.writeBatch();
        return hasWriterId(batch.writerId())
                && !batch.hasUncertainDelivery()
                && idempotenceBucketMap.get(ready.tableBucket()).removeRejectedTail(batch);
    }

    synchronized void removeInFlightBatch(ReadyWriteBatch batch) {
        if (hasInflightBatches(batch.tableBucket())) {
            idempotenceBucketMap.removeInFlightBatch(batch);
        }
    }

    /**
     * Returns the first inflight batch sequence for a given bucket. This is the base sequence of an
     * inflight batch with the lowest batch sequence.
     *
     * @return the lowest inflight batch sequence if the idempotence manager is tracking inflight
     *     requests for this bucket. If there are no inflight requests being tracked for this
     *     bucket, this method will return LogRecordBatch.NO_BATCH_SEQUENCE.
     */
    synchronized int firstInFlightBatchSequence(TableBucket tableBucket) {
        if (!hasInflightBatches(tableBucket)) {
            return NO_BATCH_SEQUENCE;
        }
        WriteBatch batch = nextBatchBySequence(tableBucket);
        return batch == null ? NO_BATCH_SEQUENCE : batch.batchSequence();
    }

    synchronized void handleCompletedBatch(ReadyWriteBatch readyWriteBatch) {
        TableBucket tableBucket = readyWriteBatch.tableBucket();
        WriteBatch batch = readyWriteBatch.writeBatch();
        if (!hasWriterId(batch.writerId())) {
            LOG.debug(
                    "Ignoring completed batch {} with writer id {}, and batch sequence {} "
                            + "since the writer id has been reset internally",
                    batch,
                    batch.writerId(),
                    batch.batchSequence());
            return;
        }

        int lastAckedSequence = maybeUpdateLastAckedSequence(tableBucket, batch.batchSequence());
        LOG.debug(
                "Writer id: {}; Set last ack'd batch sequence for table-bucket {} to {}",
                batch.writerId(),
                tableBucket,
                lastAckedSequence);
        removeInFlightBatch(readyWriteBatch);
    }

    synchronized void handleFailedBatch(
            ReadyWriteBatch readyWriteBatch, Exception exception, boolean adjustSequenceNumbers) {
        WriteBatch batch = readyWriteBatch.writeBatch();
        if (!hasWriterId(batch.writerId())) {
            LOG.debug(
                    "Ignoring failed batch {} with writer id {}, and batch sequence {} "
                            + "since the writer id has been reset internally",
                    batch,
                    batch.writerId(),
                    batch.batchSequence(),
                    exception);
            return;
        }

        if (exception instanceof OutOfOrderSequenceException
                || exception instanceof UnknownWriterIdException) {
            LOG.error(
                    "The server returned {} for table-bucket {} with writer id {} and batch sequence {}.",
                    exception,
                    readyWriteBatch.tableBucket(),
                    batch.writerId(),
                    batch.batchSequence());
            if (isRecoveringWriterId()) {
                // Resetting here would discard the batches waiting for recovery.
                preventWriterIdRecovery();
            } else {
                resetWriterId();
            }
        } else {
            removeInFlightBatch(readyWriteBatch);
            if (adjustSequenceNumbers) {
                idempotenceBucketMap.adjustSequencesDueToFailedBatch(readyWriteBatch);
            }
        }
    }

    synchronized boolean hasInflightBatches(TableBucket tableBucket) {
        return idempotenceBucketMap.getOrCreate(tableBucket).hasInflightBatches();
    }

    synchronized boolean canSendMoreRequests(TableBucket tableBucket) {
        return inflightBatchSize(tableBucket) < maxInflightRequestsPerBucket;
    }

    @VisibleForTesting
    synchronized int inflightBatchSize(TableBucket tableBucket) {
        return idempotenceBucketMap.getOrCreate(tableBucket).inflightBatchSize();
    }

    synchronized boolean canRetry(WriteBatch batch, TableBucket tableBucket, Errors error) {
        if (!isWriterIdValid()) {
            return false;
        }

        if (error == Errors.OUT_OF_ORDER_SEQUENCE_EXCEPTION
                && (batch.sequenceHasBeenReset()
                        || !isNextSequence(tableBucket, batch.batchSequence())
                        || lastAckedBatchSequence(tableBucket)
                                        .orElse(IdempotenceBucketEntry.NO_LAST_ACKED_BATCH_SEQUENCE)
                                > batch.lastAckedSequenceAtSend())) {
            // We should retry the OutOfOrderSequenceException if the batch is not the next batch,
            // i.e. its batch sequence isn't the lastAckedBatchSequence + 1. However, if the first
            // in flight batch fails fatally, we will adjust the batch sequences of the other
            // inflight batches to account for the 'loss' of the sequence range in the batch which
            // failed. In this case, an inflight batch will have a batch sequence which is the
            // lastAckedSequence + 1 after adjustment. When this batch fails with an
            // OutOfOrderSequence, we want to retry it. To account for the latter case, we check
            // whether the sequence has been reset since the last drain. If it has, we will retry it
            // anyway.
            //
            // We should also retry if the acked sequence has advanced since this batch was sent
            // (lastAckedBatchSequence > lastAckedSequenceAtSend). That means a predecessor was
            // acknowledged while this batch was in flight, so this OutOfOrderSequence reflects a
            // superseded (stale) server state that a response reordering surfaced late; a retry
            // will observe the advanced state and succeed. A genuine loss shows no such progress
            // and still resets.
            return true;
        }

        if (error == Errors.UNKNOWN_WRITER_ID_EXCEPTION) {
            // When the first inflight batch fails due to the truncation case, then the
            // sequences of all the other in flight batches would have been restarted from the
            // beginning. However, when those responses come back from the tablet server, they
            // would also come with an UNKNOWN_WRITER_ID error. In this case, we should not
            // reset the batch sequence to the beginning.
            return batch.sequenceHasBeenReset();
        }

        return false;
    }

    synchronized boolean hasCoveringAck(WriteBatch batch, TableBucket tableBucket) {
        Optional<Integer> lastAcked = lastAckedBatchSequence(tableBucket);
        return hasWriterId(batch.writerId())
                && lastAcked.isPresent()
                && batch.batchSequence() <= lastAcked.get();
    }

    void maybeWaitForWriterId(Set<PhysicalTablePath> tablePaths) throws Throwable {
        if (!isWriterIdValid()) {
            setWriterId(requestWriterId(tablePaths));
        }
    }

    private long requestWriterId(Set<PhysicalTablePath> tablePaths) throws Throwable {
        int retryCount = 0;
        while (true) {
            try {
                return tabletServerGateway
                        .initWriter(prepareInitWriterRequest(tablePaths))
                        .get()
                        .getWriterId();
            } catch (Exception e) {
                Throwable t = ExceptionUtils.stripExecutionException(e);
                if (t instanceof AuthorizationException || retryCount >= RETRY_TIMES) {
                    throw t;
                } else {
                    if (t instanceof InvalidMetadataException) {
                        metadataUpdater.updatePhysicalTableMetadata(tablePaths);
                    }
                    LOG.warn(
                            "Failed to init writer id, the retry count: {}, error message: {}",
                            retryCount,
                            t.getMessage());
                    retryCount++;
                    long delayMs = (long) (RETRY_INTERVAL_MS * Math.pow(2, retryCount));
                    Thread.sleep(delayMs);
                }
            }
        }
    }

    InitWriterRequest prepareInitWriterRequest(Set<PhysicalTablePath> physicalTables) {
        InitWriterRequest initWriterRequest = new InitWriterRequest();
        Set<TablePath> tables =
                physicalTables.stream()
                        .map(PhysicalTablePath::getTablePath)
                        .collect(Collectors.toSet());
        for (TablePath tablePath : tables) {
            initWriterRequest
                    .addTablePath()
                    .setDatabaseName(tablePath.getDatabaseName())
                    .setTableName(tablePath.getTableName());
        }
        return initWriterRequest;
    }

    private int maybeUpdateLastAckedSequence(TableBucket tableBucket, int sequence) {
        return idempotenceBucketMap.maybeUpdateLastAckedSequence(tableBucket, sequence);
    }
}
