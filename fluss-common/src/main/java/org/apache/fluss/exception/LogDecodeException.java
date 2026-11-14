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
 * <p>The scan does not move past the failed batch, so every poll throws this exception again until
 * the bucket is subscribed at another offset or unsubscribed. A scan decodes whole batches: if the
 * failed batch holds more than one record, subscribing at {@code getOffset() + 1} lands in it
 * again. To skip the batch, subscribe the bucket at {@link #getNextOffset()}.
 *
 * <p>The failure may surface after earlier polls already returned some records of the failed batch,
 * for example when a record in the middle of the batch cannot be decoded. Skipping the batch gives
 * up its remaining records, which are never returned.
 *
 * <pre>{@code
 * try {
 *     ScanRecords records = scanner.poll(timeout);
 *     // process records
 * } catch (LogDecodeException e) {
 *     if (!e.getNextOffset().isPresent()) {
 *         throw e;
 *     }
 *     TableBucket bucket = e.getTableBucket();
 *     long nextOffset = e.getNextOffset().getAsLong();
 *     if (bucket.getPartitionId() != null) {
 *         scanner.subscribe(bucket.getPartitionId(), bucket.getBucket(), nextOffset);
 *     } else {
 *         scanner.subscribe(bucket.getBucket(), nextOffset);
 *     }
 * }
 * }</pre>
 *
 * @since 1.1
 */
@PublicEvolving
public class LogDecodeException extends FetchException {

    private static final long serialVersionUID = 1L;

    private final TableBucket tableBucket;
    private final long offset;
    // Nullable instead of OptionalLong, which is not serializable.
    @Nullable private final Long nextOffset;

    /**
     * Creates the exception for a batch that could not be decoded.
     *
     * @param message the detail message
     * @param cause the failure that stopped decoding
     * @param tableBucket the bucket whose log could not be decoded
     * @param offset the base offset of the failed batch, or the offset the scan stopped at if the
     *     batch could not be read at all
     * @param nextOffset the offset after the failed batch, or empty if it is unknown
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
     * Returns the base offset of the batch that could not be decoded. If the batch could not be
     * read from the fetched data at all, returns the offset the scan stopped at instead.
     */
    public long getOffset() {
        return offset;
    }

    /**
     * Returns the offset after the failed batch, which is where to subscribe the bucket to skip it.
     * Empty when the end of the batch is unknown: the batch could not be read, or its header cannot
     * be trusted because it is incomplete, failed its CRC check, has an unsupported magic, has a
     * negative base offset, or puts the batch end at or before its start or past {@link
     * Long#MAX_VALUE}. When the CRC check is skipped ({@code client.scanner.log.check-crc} is
     * disabled, or the server projected the batch for a scan with a column projection), a complete
     * header is trusted without verification.
     */
    public OptionalLong getNextOffset() {
        return nextOffset == null ? OptionalLong.empty() : OptionalLong.of(nextOffset);
    }
}
