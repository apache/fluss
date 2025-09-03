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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.fluss.client.write.IdempotenceBucketEntry.NO_LAST_ACKED_BATCH_SEQUENCE;

/** Map to manage {@link IdempotenceBucketEntry} of all table-bucket. */
@Internal
public class IdempotenceBucketMap {
    private static final Logger LOG = LoggerFactory.getLogger(IdempotenceBucketMap.class);

    private final Map<TableBucket, IdempotenceBucketEntry> tableBuckets = new HashMap<>();

    IdempotenceBucketEntry get(TableBucket tableBucket) {
        IdempotenceBucketEntry entry = tableBuckets.get(tableBucket);
        if (entry == null) {
            throw new IllegalStateException(
                    "Try to get idempotence entry for table bucket "
                            + tableBucket
                            + ", but it was never set for this bucket.");
        }
        return entry;
    }

    IdempotenceBucketEntry getOrCreate(TableBucket tableBucket) {
        return tableBuckets.computeIfAbsent(
                tableBucket, k -> new IdempotenceBucketEntry(tableBucket));
    }

    boolean contains(TableBucket tableBucket) {
        return tableBuckets.containsKey(tableBucket);
    }

    void reset() {
        tableBuckets.clear();
    }

    Optional<Integer> lastAckedBatchSequence(TableBucket tableBucket) {
        IdempotenceBucketEntry entry = tableBuckets.get(tableBucket);
        if (entry != null) {
            return entry.lastAckedBatchSequence();
        }
        return Optional.empty();
    }

    void startBatchSequencesAtBeginning(TableBucket tableBucket, long writerId) {
        IdempotenceBucketEntry entry = get(tableBucket);
        if (entry != null) {
            entry.startBatchSequencesAtBeginning(writerId);
        }
    }

    /**
     * If a batch is failed fatally, the batch sequence for future batches bound for the bucket must
     * be adjusted so that they don't fail with the {@link OutOfOrderSequenceException}.
     *
     * <p>This method must only be called when we know that the batch is question has been
     * unequivocally failed by the tablet serve, i.e. it has received a confirmed fatal status code
     * like 'Message Too Large' or something similar.
     */
    void adjustSequencesDueToFailedBatch(ReadyWriteBatch readyBatch) {
        TableBucket tableBucket = readyBatch.tableBucket();
        WriteBatch batch = readyBatch.writeBatch();
        if (!contains(tableBucket)) {
            // Batch sequence are not being tracked for this bucket. This could happen if the
            // writer id was just reset due to a previous OutOfOrderSequenceException.
            return;
        }
        LOG.debug(
                "writerId: {}, send to bucket {} failed fatally. Reducing future batch sequence numbers",
                batch.writerId(),
                tableBucket);
        get(tableBucket).adjustSequencesDueToFailedBatch(batch);
    }

    int maybeUpdateLastAckedSequence(TableBucket tableBucket, int sequence) {
        IdempotenceBucketEntry entry = tableBuckets.get(tableBucket);
        if (entry != null) {
            return entry.maybeUpdateLastAckedSequence(sequence);
        }
        return NO_LAST_ACKED_BATCH_SEQUENCE;
    }

    WriteBatch nextBatchBySequence(TableBucket tableBucket) {
        return get(tableBucket).nextBatchBySequence();
    }

    void removeInFlightBatch(ReadyWriteBatch readyBatch) {
        get(readyBatch.tableBucket()).removeInFlightBatch(readyBatch.writeBatch());
    }
}
