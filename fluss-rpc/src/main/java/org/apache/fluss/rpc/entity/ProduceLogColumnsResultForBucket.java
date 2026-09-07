/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.rpc.entity;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.rpc.protocol.ApiError;

/** Result of a produce-log-columns (FIP-45 column-group append) request for one bucket. */
@Internal
public class ProduceLogColumnsResultForBucket extends ResultForBucket {
    private final long logEndOffset;
    private final long highWatermark;
    private final long expectedSourceOffset;

    public ProduceLogColumnsResultForBucket(
            TableBucket tableBucket, long logEndOffset, long highWatermark) {
        this(tableBucket, logEndOffset, highWatermark, -1L, ApiError.NONE);
    }

    public ProduceLogColumnsResultForBucket(TableBucket tableBucket, ApiError error) {
        this(tableBucket, -1L, -1L, -1L, error);
    }

    public ProduceLogColumnsResultForBucket(
            TableBucket tableBucket, ApiError error, long expectedSourceOffset) {
        this(tableBucket, -1L, -1L, expectedSourceOffset, error);
    }

    private ProduceLogColumnsResultForBucket(
            TableBucket tableBucket,
            long logEndOffset,
            long highWatermark,
            long expectedSourceOffset,
            ApiError error) {
        super(tableBucket, error);
        this.logEndOffset = logEndOffset;
        this.highWatermark = highWatermark;
        this.expectedSourceOffset = expectedSourceOffset;
    }

    /** The column group's log end offset (enrichment watermark) after the append. */
    public long getLogEndOffset() {
        return logEndOffset;
    }

    /** The column group's high watermark (committed enrichment watermark) after the append. */
    public long getHighWatermark() {
        return highWatermark;
    }

    /** The source offset the server expected, or -1 if not applicable. */
    public long getExpectedSourceOffset() {
        return expectedSourceOffset;
    }
}
