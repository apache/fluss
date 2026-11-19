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

package org.apache.fluss.server.index;

import org.apache.fluss.utils.ExponentialBackoff;
import org.apache.fluss.utils.clock.Clock;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.TimeUnit;

/**
 * Monotonic retry state for a source bucket or a single index.
 *
 * <p>Retained WAL and replay progress allow window payloads to be rebuilt after yielding shared
 * capacity. Retry history belongs to the replication task and survives payload replacement.
 * Successful window progress confirms recovery.
 *
 * <p>Short failures reuse staged payloads. During sustained failures, the fault age and remaining
 * retry delay determine when a window should yield its payload budget. Waiting after yielding gives
 * other tasks an opportunity to acquire capacity before this task rebuilds.
 */
@ThreadSafe
final class IndexReplicationRetryState {
    private static final long CAPACITY_RETRY_NANOS = TimeUnit.MILLISECONDS.toNanos(50);

    private final ExponentialBackoff backoff;
    private final Clock clock;
    private long attempts;
    private long failureStartNanos;
    private volatile long retryDeadlineNanos;

    IndexReplicationRetryState(ExponentialBackoff backoff, Clock clock) {
        this.backoff = backoff;
        this.clock = clock;
        this.retryDeadlineNanos = clock.nanoseconds();
    }

    /** Failures arriving during the same backoff share its deadline and retry count. */
    synchronized void onFailure() {
        if (attempts > 0 && remainingDelayNanos() > 0) {
            return;
        }
        long now = clock.nanoseconds();
        if (attempts == 0) {
            failureStartNanos = now;
        }
        retryDeadlineNanos = now + TimeUnit.MILLISECONDS.toNanos(backoff.backoff(attempts++));
    }

    /**
     * Paces construction contention without recording a replication fault or losing retry history.
     * Releasing partial construction can wake the same worker. A short eligibility delay prevents
     * repeated read, build and release cycles while capacity remains unavailable.
     */
    synchronized void onCapacityUnavailable() {
        retryDeadlineNanos =
                clock.nanoseconds() + Math.max(remainingDelayNanos(), CAPACITY_RETRY_NANOS);
    }

    long remainingDelayNanos() {
        return Math.max(0, retryDeadlineNanos - clock.nanoseconds());
    }

    synchronized boolean hasFailure() {
        return attempts > 0;
    }

    /**
     * Checks whether failure age plus remaining retry delay reaches the soft payload-retention
     * target, allowing a failed window to yield before holding capacity through a long backoff.
     */
    synchronized boolean shouldYieldPayload(long retentionNanos) {
        return attempts > 0
                && clock.nanoseconds() - failureStartNanos
                        >= retentionNanos - remainingDelayNanos();
    }

    /**
     * Leaves a fresh competition opportunity after yielding. Retaining fault age and retry stage
     * prevents payload rebuilding from restarting fast retries during the same failure period.
     */
    synchronized void deferRetryAfterYield() {
        long delayNanos = TimeUnit.MILLISECONDS.toNanos(backoff.backoff(attempts - 1));
        retryDeadlineNanos = clock.nanoseconds() + Math.max(remainingDelayNanos(), delayNanos);
    }

    /** Clears the failure period when completed window progress confirms recovery. */
    synchronized void reset() {
        attempts = 0;
        failureStartNanos = 0;
        retryDeadlineNanos = clock.nanoseconds();
    }
}
