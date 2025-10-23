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

package org.apache.fluss.server.log;

import org.apache.fluss.exception.OutOfOrderSequenceException;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.record.LogRecordBatch;

import static org.apache.fluss.record.LogRecordBatchFormat.NO_BATCH_SEQUENCE;

/**
 * This class is used to validate the records appended by a given writer before they are written to
 * log. It's initialized with writer's state after the last successful append.
 */
public class WriterAppendInfo {
    private final long writerId;
    private final TableBucket tableBucket;
    private final WriterStateEntry currentEntry;
    private final WriterStateEntry updatedEntry;

    public WriterAppendInfo(long writerId, TableBucket tableBucket, WriterStateEntry currentEntry) {
        this.writerId = writerId;
        this.tableBucket = tableBucket;
        this.currentEntry = currentEntry;
        this.updatedEntry = currentEntry.withWriterIdAndBatchMetadata(writerId, null);
    }

    public long writerId() {
        return writerId;
    }

    public void append(
            LogRecordBatch batch, boolean isWriterInBatchExpired, boolean isAppendAsLeader) {
        LogOffsetMetadata firstOffsetMetadata = new LogOffsetMetadata(batch.baseLogOffset());
        appendDataBatch(
                batch.batchSequence(),
                firstOffsetMetadata,
                batch.lastLogOffset(),
                isWriterInBatchExpired,
                isAppendAsLeader,
                batch.commitTimestamp());
    }

    public void appendDataBatch(
            int batchSequence,
            LogOffsetMetadata firstOffsetMetadata,
            long lastOffset,
            boolean isWriterInBatchExpired,
            boolean isAppendAsLeader,
            long batchTimestamp) {
        maybeValidateDataBatch(batchSequence, isWriterInBatchExpired, lastOffset, isAppendAsLeader);
        updatedEntry.addBath(
                batchSequence,
                lastOffset,
                (int) (lastOffset - firstOffsetMetadata.getMessageOffset()),
                batchTimestamp);
    }

    private void maybeValidateDataBatch(
            int appendFirstSeq,
            boolean isWriterInBatchExpired,
            long lastOffset,
            boolean isAppendAsLeader) {
        int currentLastSeq =
                !updatedEntry.isEmpty()
                        ? updatedEntry.lastBatchSequence()
                        : currentEntry.lastBatchSequence();
        // must be in sequence, even for the first batch should start from 0
        if (!inSequence(currentLastSeq, appendFirstSeq, isWriterInBatchExpired, isAppendAsLeader)) {
            throw new OutOfOrderSequenceException(
                    String.format(
                            "Out of order batch sequence for writer %s at offset %s in "
                                    + "table-bucket %s : %s (incoming batch seq.), %s (current batch seq.)",
                            writerId, lastOffset, tableBucket, appendFirstSeq, currentLastSeq));
        }
    }

    public WriterStateEntry toEntry() {
        return updatedEntry;
    }

    /**
     * Check if the next batch sequence is in sequence with the last batch sequence. The following
     * three scenarios will be judged as in sequence:
     *
     * <ul>
     *   <li>1. If lastBatchSeq equals NO_BATCH_SEQUENCE, the following two scenarios will be judged
     *       as in sequence:
     *       <ul>
     *         <li>1.1 If the committed timestamp of the next batch under the current writerId has
     *             expired, we consider this a special case caused by writerId expiration, for this
     *             case, to ensure the correctness of follower sync, we still treat it as in
     *             sequence.
     *         <li>1.2 If the append request is from the follower, we consider this is a special
     *             case caused by inconsistent expiration of writerId between the leader and
     *             follower. To prevent continuous fetch failures on the follower side, we still
     *             treat it as in sequence.
     *       </ul>
     *   <li>2. nextBatchSeq == lastBatchSeq + 1L
     *   <li>3. lastBatchSeq reaches its maximum value
     * </ul>
     *
     * <p>For case 1.2, here is a detailed example: The expiration of a writer is triggered
     * asynchronously by the {@code PeriodicWriterIdExpirationCheck} thread at intervals defined by
     * {@code server.writer-id.expiration-check-interval}, which can result in slight differences in
     * the actual expiration times of the same writer on the leader replica and follower replicas.
     * This slight difference leads to a dreadful corner case. Imagine the following scenario(set
     * {@code server.writer-id.expiration-check-interval}: 10min, {@code
     * server.writer-id.expiration-time}: 12h):
     *
     * <pre>{@code
     * Step     Time         Action of Leader                  Action of Follower
     * 1        00:03:38     receive batch 0 of writer 101
     * 2        00:03:38                                       fetch batch 0 of writer 101
     * 3        12:05:00                                       remove state of writer 101
     * 4        12:10:02     receive batch 1 of writer 101
     * 5        12:10:02                                       fetch batch 0 of writer 101
     * 6        12:11:00     remove state of writer 101
     * }</pre>
     *
     * <p>In step 3, the follower removes the state of writer 101 first, since it has been more than
     * 12 hours since writer 101's last batch write, making it safe to remove. However, since the
     * expiration of writer 101 has not yet occurred on the leader, and a new batch 1 is received at
     * this time, it is successfully written on the leader. At this point, the fetcher pulls batch 1
     * from the leader, but since the state of writer 101 has already been cleaned up, an {@link
     * OutOfOrderSequenceException} will occur during to write if we don't treat it as in sequence.
     */
    private boolean inSequence(
            int lastBatchSeq,
            int nextBatchSeq,
            boolean isWriterInBatchExpired,
            boolean isAppendAsLeader) {
        return (lastBatchSeq == NO_BATCH_SEQUENCE && (isWriterInBatchExpired || !isAppendAsLeader))
                || nextBatchSeq == lastBatchSeq + 1L
                || (nextBatchSeq == 0 && lastBatchSeq == Integer.MAX_VALUE);
    }
}
