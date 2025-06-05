/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.client.write;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.annotation.VisibleForTesting;
import com.alibaba.fluss.exception.OutOfOrderSequenceException;
import com.alibaba.fluss.exception.UnknownWriterIdException;
import com.alibaba.fluss.metadata.PhysicalTablePath;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.record.LogRecordBatch;
import com.alibaba.fluss.rpc.gateway.TabletServerGateway;
import com.alibaba.fluss.rpc.messages.InitWriterRequest;
import com.alibaba.fluss.rpc.protocol.Errors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.alibaba.fluss.record.LogRecordBatch.NO_WRITER_ID;

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

    private final boolean idempotenceEnabled;
    private final IdempotenceBucketMap idempotenceBucketMap;
    private final int maxInflightRequestsPerBucket;
    private final TabletServerGateway tabletServerGateway;

    private volatile long writerId;

    public IdempotenceManager(
            boolean idempotenceEnabled,
            int maxInflightRequestsPerBucket,
            TabletServerGateway tabletServerGateway) {
        this.idempotenceEnabled = idempotenceEnabled;
        this.maxInflightRequestsPerBucket = maxInflightRequestsPerBucket;
        this.idempotenceBucketMap = new IdempotenceBucketMap();
        this.tabletServerGateway = tabletServerGateway;
        this.writerId = NO_WRITER_ID;
    }

    boolean idempotenceEnabled() {
        return idempotenceEnabled;
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

    synchronized void incrementBatchSequence(TableBucket tableBucket) {
        idempotenceBucketMap.get(tableBucket).incrementSequence();
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

    synchronized void addInFlightBatch(WriteBatch batch, TableBucket tableBucket) {
        if (!batch.hasBatchSequence()) {
            throw new IllegalStateException(
                    "Can't track batch for bucket "
                            + tableBucket
                            + " when batch sequence is not set.");
        }
        idempotenceBucketMap.get(tableBucket).addInflightBatch(batch);
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
            return LogRecordBatch.NO_BATCH_SEQUENCE;
        }
        WriteBatch batch = nextBatchBySequence(tableBucket);
        return batch == null ? LogRecordBatch.NO_BATCH_SEQUENCE : batch.batchSequence();
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
            // Reset the writer state since we have hit an irrecoverable exception and cannot make
            // any guarantees about the previously committed message. Note that this will discard
            // the writer id and batch sequence for all existing buckets.
            resetWriterId();
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
                        || !isNextSequence(tableBucket, batch.batchSequence()))) {
            // We should retry the OutOfOrderSequenceException if the batch is not the next batch,
            // i.e. its batch sequence isn't the lastAckedBatchSequence + 1. However, if the first
            // in flight batch fails fatally, we will adjust the batch sequences of the other
            // inflight batches to account for the 'loss' of the sequence range in the batch which
            // failed. In this case, an inflight batch will have a batch sequence which is the
            // lastAckedSequence + 1 after adjustment. When this batch fails with an
            // OutOfOrderSequence, we want to retry it. To account for the latter case, we check
            // whether the sequence has been reset since the last drain. If it has, we will retry it
            // anyway.
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

    void maybeWaitForWriterId(Set<PhysicalTablePath> tablePaths)
            throws ExecutionException, InterruptedException {
        if (!isWriterIdValid()) {
            tabletServerGateway
                    .initWriter(prepareInitWriterRequest(tablePaths))
                    .thenAccept(response -> setWriterId(response.getWriterId()))
                    .get(); // TODO: can optimize into async response handling.
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
