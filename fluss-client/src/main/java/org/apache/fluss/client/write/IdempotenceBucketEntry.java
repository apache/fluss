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
import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.Comparator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Consumer;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_WRITER_ID;

/** Entry to store the idempotence information of each table-bucket. */
@Internal
public class IdempotenceBucketEntry {
    static final int NO_LAST_ACKED_BATCH_SEQUENCE = -1;

    private final TableBucket tableBucket;

    /** Writer id used for a given table bucket. */
    private long writerId;

    /** The batch sequence of next bound for a given table bucket. */
    private int nextSequence;

    /**
     * The batch sequence of the last ack'd batch from the given table bucket. When there are no in
     * flight requests for a bucket, the lastAckedSequence(tableBucket) == nextSequence(tableBucket)
     * - 1.
     */
    private int lastAckedSequence;

    /**
     * Keep track of the in flight batches bound for a bucket, ordered by sequence. This helps us to
     * ensure that we continue to order batches by the sequence numbers even when the responses come
     * back out of order during leader failover. We add a batch to the queue when it is drained, and
     * remove it when the batch completes (either successfully or through a fatal failure).
     */
    private SortedSet<WriteBatch> inflightBatchesBySequence;

    /**
     * `inflightBatchesBySequence` should only have batches with the same writer id, but there is an
     * edge case where we may remove the wrong batch if the comparator only takes `baseSequence`
     * into account.
     */
    private static final Comparator<WriteBatch> WRITE_BATCH_COMPARATOR =
            Comparator.comparingLong(WriteBatch::writerId)
                    .thenComparingInt(WriteBatch::batchSequence);

    IdempotenceBucketEntry(TableBucket tableBucket) {
        this.tableBucket = tableBucket;
        this.writerId = NO_WRITER_ID;
        this.nextSequence = 0;
        this.lastAckedSequence = NO_LAST_ACKED_BATCH_SEQUENCE;
        this.inflightBatchesBySequence = new TreeSet<>(WRITE_BATCH_COMPARATOR);
    }

    long writerId() {
        return writerId;
    }

    int nextSequence() {
        return nextSequence;
    }

    Optional<Integer> lastAckedBatchSequence() {
        return lastAckedSequence == NO_LAST_ACKED_BATCH_SEQUENCE
                ? Optional.empty()
                : Optional.of(lastAckedSequence);
    }

    void startBatchSequencesAtBeginning(long newWriterId) {
        final int[] sequence = {0};
        resetSequenceNumbers(
                inFlightBatch -> {
                    inFlightBatch.resetWriterState(newWriterId, sequence[0]);
                    sequence[0] += 1;
                });
        writerId = newWriterId;
        nextSequence = sequence[0];
        lastAckedSequence = NO_LAST_ACKED_BATCH_SEQUENCE;
    }

    boolean hasInflightBatches() {
        return !inflightBatchesBySequence.isEmpty();
    }

    int inflightBatchSize() {
        return inflightBatchesBySequence.size();
    }

    @Nullable
    WriteBatch nextBatchBySequence() {
        return inflightBatchesBySequence.isEmpty() ? null : inflightBatchesBySequence.first();
    }

    void incrementSequence() {
        this.nextSequence += 1;
    }

    void addInflightBatch(WriteBatch batch) {
        inflightBatchesBySequence.add(batch);
    }

    int maybeUpdateLastAckedSequence(int sequence) {
        if (sequence > lastAckedSequence) {
            lastAckedSequence = sequence;
            return sequence;
        }
        return lastAckedSequence;
    }

    void removeInFlightBatch(WriteBatch batch) {
        inflightBatchesBySequence.remove(batch);
    }

    /**
     * If a batch is failed fatally, the batch sequence for future batches bound for the bucket must
     * be adjusted so that they don't fail with the {@link OutOfOrderSequenceException}.
     *
     * <p>This method must only be called when we know that the batch is question has been
     * unequivocally failed by the tablet serve, i.e. it has received a confirmed fatal status code
     * like 'Message Too Large' or something similar.
     */
    void adjustSequencesDueToFailedBatch(WriteBatch batch) {
        decrementSequence();
        resetSequenceNumbers(
                inFlightBatch -> {
                    int inFlightBatchSequence = inFlightBatch.batchSequence();
                    if (inFlightBatchSequence < batch.batchSequence()) {
                        return;
                    }

                    int newSequence = inFlightBatchSequence - 1;
                    if (newSequence < 0) {
                        throw new IllegalStateException(
                                "Batch sequence for batch with sequence "
                                        + inFlightBatchSequence
                                        + " for table bucket "
                                        + tableBucket
                                        + " is going to become negative :"
                                        + newSequence);
                    }
                    inFlightBatch.resetWriterState(writerId, newSequence);
                });
    }

    private void resetSequenceNumbers(Consumer<WriteBatch> resetSequence) {
        TreeSet<WriteBatch> newInflights = new TreeSet<>(WRITE_BATCH_COMPARATOR);
        for (WriteBatch inflightBatch : inflightBatchesBySequence) {
            resetSequence.accept(inflightBatch);
            newInflights.add(inflightBatch);
        }
        inflightBatchesBySequence = newInflights;
    }

    private void decrementSequence() {
        int updatedSequence = nextSequence;
        updatedSequence -= 1;
        if (updatedSequence < 0) {
            throw new IllegalStateException(
                    "Sequence number for table bucket "
                            + tableBucket
                            + " is going to become negative: "
                            + updatedSequence);
        }
        this.nextSequence = updatedSequence;
    }
}
