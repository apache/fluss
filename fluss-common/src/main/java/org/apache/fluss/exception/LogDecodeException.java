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

package org.apache.fluss.exception;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import javax.annotation.Nullable;

import java.util.OptionalLong;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/**
 * Thrown by a log scan when it cannot decode a log record batch of a bucket.
 *
 * <p>The failed fetch stays buffered until the bucket is unsubscribed or subscribed at a different
 * offset. To skip its remaining records, subscribe the bucket at the verified offset from {@link
 * #getNextOffset()}, when available.
 *
 * @since 1.1
 */
@PublicEvolving
public class LogDecodeException extends FetchException {

    private static final long serialVersionUID = 1L;

    private final TableBucket tableBucket;
    private final long offset;
    // A boxed long keeps this field serializable.
    @Nullable private final Long nextOffset;

    /**
     * Creates the exception for a batch that could not be decoded.
     *
     * @param message the detail message
     * @param cause the failure that stopped decoding
     * @param tableBucket the bucket whose log could not be decoded
     * @param offset the base offset of the failed batch, or the offset the scan stopped at if the
     *     batch could not be read at all
     * @param nextOffset the verified offset after the failed batch, or empty when unavailable
     */
    public LogDecodeException(
            String message,
            Throwable cause,
            TableBucket tableBucket,
            long offset,
            OptionalLong nextOffset) {
        super(message, cause);
        this.tableBucket = checkNotNull(tableBucket, "tableBucket must not be null");
        this.offset = offset;
        this.nextOffset = nextOffset.isPresent() ? nextOffset.getAsLong() : null;
    }

    /** Returns the bucket whose log could not be decoded. */
    public TableBucket getTableBucket() {
        return tableBucket;
    }

    /**
     * Returns the failed batch's base offset when available. Otherwise returns the scan position,
     * which may be inside a batch.
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Returns the verified offset after the failed batch when it is ahead of the scan position.
     * When another batch follows in the same fetch, its base offset must match the resume offset.
     * Otherwise, the failed batch must start at the end of the preceding decoded batch in the
     * fetch. The failed batch must pass its CRC check unless it was projected by the server and the
     * following batch confirms the boundary.
     */
    public OptionalLong getNextOffset() {
        return nextOffset == null ? OptionalLong.empty() : OptionalLong.of(nextOffset);
    }
}
