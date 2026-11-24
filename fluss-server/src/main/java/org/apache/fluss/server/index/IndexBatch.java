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

import static org.apache.fluss.utils.Preconditions.checkArgument;
import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * A single pre-encoded index write unit targeting one index-table bucket.
 *
 * <p>The window owns the source WAL boundary. The sender notifies the window after the send buffer
 * accepts completion of its claim. Failed batches can be requeued until the window retires and is
 * rebuilt from WAL.
 */
@Internal
final class IndexBatch {

    private final TableBucket targetBucket;
    private final byte[] progressKey;
    private final BytesView encoded;
    private final long retainedBytes;
    private final IndexReplicationWindow window;

    private int requeueCount;

    IndexBatch(
            TableBucket targetBucket,
            byte[] progressKey,
            BytesView encoded,
            long retainedBytes,
            IndexReplicationWindow window) {
        this.targetBucket = checkNotNull(targetBucket, "targetBucket");
        this.progressKey = checkNotNull(progressKey, "progressKey");
        this.encoded = checkNotNull(encoded, "encoded");
        checkArgument(
                retainedBytes >= encoded.getBytesLength(),
                "retainedBytes must cover the encoded payload");
        this.retainedBytes = retainedBytes;
        this.window = checkNotNull(window, "window");
        checkArgument(window.windowEndOffset() >= 0, "sourceEndOffset must not be negative");
        this.requeueCount = 0;
    }

    TableBucket targetBucket() {
        return targetBucket;
    }

    TableBucket sourceBucket() {
        return window.sourceBucket();
    }

    long sourceEndOffset() {
        return window.windowEndOffset();
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

    int requeueCount() {
        return requeueCount;
    }

    void reEnqueued() {
        requeueCount++;
    }
}
