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

/** Bucket scan status for log fetch. */
@Internal
class BucketScanStatus {
    private long offset; // last consumed position
    private long highWatermark = -1L; // the high watermark from last fetch, -1 if never fetched
    // the stopping offset for this bucket, NO_STOPPING_OFFSET if not set
    private long stoppingOffset = LogScanner.NO_STOPPING_OFFSET;
    // TODO add resetStrategy and nextAllowedRetryTimeMs.

    public BucketScanStatus() {
        this.offset = 0L;
    }

    public BucketScanStatus(Long position) {
        this.offset = position;
    }

    public long getOffset() {
        return offset;
    }

    public long getHighWatermark() {
        return highWatermark;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public void setHighWatermark(Long highWatermark) {
        this.highWatermark = highWatermark;
    }

    public long getStoppingOffset() {
        return stoppingOffset;
    }

    public void setStoppingOffset(long stoppingOffset) {
        this.stoppingOffset = stoppingOffset;
    }

    public boolean isBounded() {
        return stoppingOffset != LogScanner.NO_STOPPING_OFFSET;
    }

    /**
     * Returns whether this subscription has reached its exclusive stopping offset or has an empty
     * range. Unbounded subscriptions always return {@code false}.
     */
    public boolean hasReachedStoppingOffset() {
        return isBounded() && (stoppingOffset == 0 || offset >= stoppingOffset);
    }

    /**
     * Returns the number of log records that have not been fetched for this bucket, or 0 if the lag
     * is unknown, i.e. the offset is still a sentinel offset (like {@link
     * LogScanner#EARLIEST_OFFSET}) not resolved by any fetch yet, or no high watermark has been
     * returned by the server yet. The high watermark can also be staler than the offset, in which
     * case the lag is 0 as well.
     */
    long recordsLag() {
        if (offset < 0 || highWatermark < 0) {
            return 0L;
        }
        long effectiveHighWatermark =
                isBounded() ? Math.min(highWatermark, stoppingOffset) : highWatermark;
        return Math.max(effectiveHighWatermark - offset, 0L);
    }
}
