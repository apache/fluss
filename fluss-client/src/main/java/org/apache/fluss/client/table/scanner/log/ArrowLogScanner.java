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

import org.apache.fluss.annotation.PublicEvolving;

import java.time.Duration;

/**
 * A log scanner that returns log data as Arrow record batches.
 *
 * <p>The caller is responsible for closing every returned batch.
 *
 * @since 0.10
 */
@PublicEvolving
public interface ArrowLogScanner extends AutoCloseable {

    /** The earliest offset to fetch from. */
    long EARLIEST_OFFSET = LogScanner.EARLIEST_OFFSET;

    /** The latest offset to fetch to. */
    long NO_STOPPING_OFFSET = LogScanner.NO_STOPPING_OFFSET;

    /** Poll Arrow log batches from the tablet server. */
    ArrowScanRecords poll(Duration timeout);

    /** Subscribe to the given bucket with the given offset. */
    void subscribe(int bucket, long offset);

    /** Subscribe to the given bucket from the beginning. */
    default void subscribeFromBeginning(int bucket) {
        subscribe(bucket, EARLIEST_OFFSET);
    }

    /** Subscribe to the given partitioned bucket with the given offset. */
    void subscribe(long partitionId, int bucket, long offset);

    /** Unsubscribe from the given partitioned bucket. */
    void unsubscribe(long partitionId, int bucket);

    /** Unsubscribe from the given bucket. */
    void unsubscribe(int bucket);

    /** Subscribe to the given partitioned bucket from the beginning. */
    default void subscribeFromBeginning(long partitionId, int bucket) {
        subscribe(partitionId, bucket, EARLIEST_OFFSET);
    }

    /** Wake up the log scanner. */
    void wakeup();
}
