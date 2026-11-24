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

import javax.annotation.Nullable;

import static org.apache.fluss.utils.ExceptionUtils.rethrow;

/** Bucket scan status for log fetch. */
@Internal
class BucketScanStatus {
    enum FetchState {
        AWAITING_COMMITTED_OFFSET,
        AWAITING_RESET,
        FETCHING
    }

    private final long assignmentVersion;
    private FetchState state;
    private long offset;
    private long highWatermark = -1L;
    // Long.MAX_VALUE reserves this reset for the outstanding ListOffsets request.
    private long nextAllowedResetMs;
    @Nullable private Throwable resetError;

    BucketScanStatus(long assignmentVersion) {
        this.assignmentVersion = assignmentVersion;
        state = FetchState.AWAITING_COMMITTED_OFFSET;
    }

    BucketScanStatus(long assignmentVersion, long offset) {
        this(assignmentVersion);
        setOffset(offset);
    }

    long assignmentVersion() {
        return assignmentVersion;
    }

    FetchState state() {
        return state;
    }

    long getOffset() {
        return offset;
    }

    void setOffset(long offset) {
        this.offset = offset;
        // server can handle LogScanner.EARLIEST_OFFSET, so we ignore the case here.
        state =
                offset == LogScanner.LATEST_OFFSET
                        ? FetchState.AWAITING_RESET
                        : FetchState.FETCHING;
    }

    void setHighWatermark(long highWatermark) {
        this.highWatermark = highWatermark;
    }

    long nextAllowedResetMs() {
        return nextAllowedResetMs;
    }

    void markResetPending() {
        nextAllowedResetMs = Long.MAX_VALUE;
    }

    void resetFailed(@Nullable Throwable error, long nextAllowedResetMs) {
        this.resetError = error;
        this.nextAllowedResetMs = nextAllowedResetMs;
    }

    void maybeThrowResetError() {
        if (resetError != null) {
            Throwable error = resetError;
            resetError = null;
            rethrow(error);
        }
    }

    /**
     * Returns the number of log records that have not been fetched for this bucket, or 0 if the lag
     * is unknown, i.e. the offset is still a sentinel offset (like {@link
     * LogScanner#EARLIEST_OFFSET}) not resolved by any fetch yet, or no high watermark has been
     * returned by the server yet. The high watermark can also be staler than the offset, in which
     * case the lag is 0 as well.
     */
    long recordsLag() {
        if (state != FetchState.FETCHING || offset < 0 || highWatermark < 0) {
            return 0L;
        }
        return Math.max(highWatermark - offset, 0L);
    }
}
