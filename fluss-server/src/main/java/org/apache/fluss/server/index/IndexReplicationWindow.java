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

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.Set;

/**
 * A deterministic replication window produced by a single {@link IndexReplicator#poll()} cycle.
 *
 * <p>The offset is a property of the window, not of any individual {@link IndexBatch}. A window
 * covers the half-open WAL offset range {@code [windowStart, windowEndOffset)} for exactly one
 * secondary index and fans out into {@code remaining} index write batches across one or more target
 * index buckets. Only when every one of those batches has been acknowledged does the window advance
 * that index's pushed offset to {@link #windowEndOffset}.
 *
 * <p>Window boundaries may differ after failover. The source advances progress only after every
 * batch in a window completes and replays from its persisted progress after recovery. Together with
 * the target rejecting requests behind its stored writer progress, this makes different source-side
 * window boundaries safe.
 *
 * <p>Retry pacing belongs to the logical index progress and survives replacement of this window.
 * Yielding makes the window terminal so late completions cannot advance its owner's progress.
 */
@Internal
final class IndexReplicationWindow {

    private final String indexName;
    private final long windowEndOffset;
    private final int expectedBatchCount;
    private int remaining;
    private final IndexReplicator owner;
    private final IndexReplicationRetryState retry;
    private Set<TableBucket> targetBuckets = Collections.emptySet();
    private volatile boolean admitted;
    private long admittedPayloadBytes;
    private volatile boolean terminal;
    private volatile boolean failed;

    IndexReplicationWindow(
            String indexName, long windowEndOffset, int batchCount, IndexReplicator owner) {
        this.indexName = indexName;
        this.windowEndOffset = windowEndOffset;
        this.expectedBatchCount = batchCount;
        this.remaining = batchCount;
        this.owner = owner;
        this.retry = owner.retryState(indexName);
        this.admitted = false;
    }

    String indexName() {
        return indexName;
    }

    long windowEndOffset() {
        return windowEndOffset;
    }

    /** The source main-table bucket of the producing replicator; the buffer's accounting key. */
    TableBucket sourceBucket() {
        return owner.sourceBucket();
    }

    /** Whether the producing replicator has been closed. */
    boolean isOwnerClosed() {
        return owner.isClosed();
    }

    /**
     * Retires this window without notifying its owner, when its owner is closed or construction is
     * abandoned or its retained payload is yielded after sustained failures.
     */
    boolean tryRetire() {
        synchronized (this) {
            if (terminal) {
                return false;
            }
            terminal = true;
            return true;
        }
    }

    synchronized void onFailure() {
        if (!terminal && !owner.isClosed()) {
            failed = true;
            retry.onFailure();
        }
    }

    boolean hasFailure() {
        return failed;
    }

    boolean isReadyForSend() {
        return retry.remainingDelayNanos() == 0;
    }

    boolean isActive() {
        return !terminal;
    }

    boolean isAdmitted() {
        return admitted;
    }

    synchronized void markAdmitted(long payloadBytes, Set<TableBucket> targetBuckets) {
        this.targetBuckets = targetBuckets;
        admittedPayloadBytes = payloadBytes;
        admitted = true;
    }

    Iterable<TableBucket> targetBuckets() {
        return targetBuckets;
    }

    int expectedBatchCount() {
        return expectedBatchCount;
    }

    /**
     * Acknowledge one batch belonging to this window. When the last outstanding batch is
     * acknowledged, the window is complete and the owning replicator's pushed offset is advanced.
     */
    void onBatchAcked() {
        boolean completed = false;
        long completedBytes = 0L;
        synchronized (this) {
            if (terminal) {
                return;
            }
            if (--remaining == 0) {
                completedBytes = admittedPayloadBytes;
                terminal = true;
                completed = true;
            }
        }
        if (completed) {
            owner.onWindowComplete(this, completedBytes);
        }
    }
}
