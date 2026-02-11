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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.ByteArrayWrapper;

import java.util.HashSet;
import java.util.Set;

/**
 * Encapsulates the recovery state for a single bucket.
 *
 * <p>This class tracks:
 *
 * <ul>
 *   <li>The bucket being recovered
 *   <li>The checkpoint offset (start point for reading changelog)
 *   <li>The log end offset (end point for reading changelog)
 *   <li>Processed primary keys for deduplication (streaming execution)
 *   <li>Progress tracking during changelog scanning
 * </ul>
 */
public class BucketRecoveryContext {

    private final TableBucket bucket;
    private final long checkpointOffset;
    private final long logEndOffset;

    private final Set<ByteArrayWrapper> processedKeys;
    private long lastProcessedOffset;
    private int totalRecordsProcessed;

    public BucketRecoveryContext(TableBucket bucket, long checkpointOffset, long logEndOffset) {
        this.bucket = bucket;
        this.checkpointOffset = checkpointOffset;
        this.logEndOffset = logEndOffset;
        this.processedKeys = new HashSet<>();
        this.lastProcessedOffset = checkpointOffset - 1;
        this.totalRecordsProcessed = 0;
    }

    public TableBucket getBucket() {
        return bucket;
    }

    public long getCheckpointOffset() {
        return checkpointOffset;
    }

    public long getLogEndOffset() {
        return logEndOffset;
    }

    public Set<ByteArrayWrapper> getProcessedKeys() {
        return processedKeys;
    }

    /**
     * Checks if this bucket needs recovery.
     *
     * @return true if checkpoint offset is less than log end offset
     */
    public boolean needsRecovery() {
        return checkpointOffset < logEndOffset;
    }

    /**
     * Checks if changelog scanning is complete for this bucket.
     *
     * <p>Complete means either:
     *
     * <ul>
     *   <li>No recovery is needed (checkpointOffset >= logEndOffset), or
     *   <li>The last processed offset has reached or passed logEndOffset - 1 (lastProcessedOffset
     *       >= logEndOffset - 1)
     * </ul>
     *
     * <p>TODO: This offset-based completion detection cannot handle the case where all
     * LogRecordBatches between checkpointOffset and logEndOffset are empty (contain no user
     * records). In that scenario, lastProcessedOffset will never advance and isComplete() will
     * never return true, causing the recovery to rely on the timeout mechanism in
     * UndoRecoveryExecutor. A future improvement is to implement bounded subscription mode in
     * LogScanner, which will allow the scanner to signal completion directly, and this logic should
     * be refactored accordingly.
     *
     * @return true if changelog scanning is complete
     */
    public boolean isComplete() {
        if (!needsRecovery()) {
            return true;
        }
        return lastProcessedOffset >= logEndOffset - 1;
    }

    /**
     * Records that a changelog record has been processed.
     *
     * @param offset the offset of the processed record
     */
    public void recordProcessed(long offset) {
        lastProcessedOffset = offset;
        totalRecordsProcessed++;
    }

    public int getTotalRecordsProcessed() {
        return totalRecordsProcessed;
    }

    public long getLastProcessedOffset() {
        return lastProcessedOffset;
    }

    @Override
    public String toString() {
        return "BucketRecoveryContext{"
                + "bucket="
                + bucket
                + ", checkpointOffset="
                + checkpointOffset
                + ", logEndOffset="
                + logEndOffset
                + ", processedKeys="
                + processedKeys.size()
                + ", complete="
                + isComplete()
                + '}';
    }
}
