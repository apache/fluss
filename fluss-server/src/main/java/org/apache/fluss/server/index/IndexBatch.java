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
import org.apache.fluss.record.bytesview.BytesView;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A single pre-encoded index write unit targeting one index-table bucket.
 *
 * <p>A batch carries only a reference to the {@link IndexReplicationWindow} it belongs to; the WAL
 * offset it advances is a property of that window, not of the batch. On successful acknowledgement
 * the batch notifies its window via {@link IndexReplicationWindow#onBatchAcked()}; on failure it is
 * re-enqueued for unlimited retry without advancing any offset.
 *
 * <p>The {@link IndexSendBuffer} owns the batch from admission through its exact-claim terminal
 * transition, so a late or duplicate sender completion cannot acknowledge the window twice.
 */
@Internal
final class IndexBatch {

    private final TableBucket targetBucket;
    private final TableBucket sourceBucket;
    private final long sourceEndOffset;
    private final byte[] progressKey;
    private final BytesView encoded;
    private final long retainedBytes;
    private final IndexReplicationWindow window;

    private int attempts;

    /** Earliest wall-clock time this batch may be re-sent; set on retry to enforce backoff. */
    private volatile long readyAtMs;

    /** One-shot guard ensuring pending-byte accounting is released at most once. */
    private final AtomicBoolean released = new AtomicBoolean(false);

    IndexBatch(
            TableBucket targetBucket,
            TableBucket sourceBucket,
            long sourceEndOffset,
            byte[] progressKey,
            BytesView encoded,
            long retainedBytes,
            IndexReplicationWindow window) {
        this.targetBucket = checkNotNull(targetBucket, "targetBucket");
        this.sourceBucket = checkNotNull(sourceBucket, "sourceBucket");
        checkArgument(sourceEndOffset >= 0, "sourceEndOffset must not be negative");
        this.sourceEndOffset = sourceEndOffset;
        this.progressKey = checkNotNull(progressKey, "progressKey");
        this.encoded = checkNotNull(encoded, "encoded");
        checkArgument(
                retainedBytes >= encoded.getBytesLength(),
                "retainedBytes must cover the encoded payload");
        this.retainedBytes = retainedBytes;
        this.window = checkNotNull(window, "window");
        this.attempts = 0;
        this.readyAtMs = 0L;
    }

    TableBucket targetBucket() {
        return targetBucket;
    }

    TableBucket sourceBucket() {
        return sourceBucket;
    }

    long sourceEndOffset() {
        return sourceEndOffset;
    }

    byte[] progressKey() {
        return progressKey;
    }

    BytesView encoded() {
        return encoded;
    }

    long retainedBytes() {
        return retainedBytes;
    }

    IndexReplicationWindow window() {
        return window;
    }

    int attempts() {
        return attempts;
    }

    void reEnqueued() {
        attempts++;
    }

    long readyAtMs() {
        return readyAtMs;
    }

    void setReadyAtMs(long readyAtMs) {
        this.readyAtMs = readyAtMs;
    }

    boolean ownerActive() {
        return window.isActive() && !window.isOwnerClosed();
    }

    boolean markReleased() {
        return released.compareAndSet(false, true);
    }

    boolean isReleased() {
        return released.get();
    }
}
