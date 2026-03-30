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

package org.apache.fluss.flink.sink.undo;

import org.apache.fluss.metadata.TableBucket;

/**
 * An interface for the SinkWriter to report bucket offsets back to the upstream
 * UndoRecoveryOperator.
 *
 * <p>This interface enables the communication of written offsets from the downstream SinkWriter to
 * the upstream operator that manages the undo recovery state. The operator uses these offsets to
 * track the latest written positions for each bucket, which is essential for proper undo recovery
 * during checkpoint restoration.
 *
 * <p><b>Thread-Safety Requirements:</b> Implementations of this interface MUST be thread-safe, as
 * offset reports may be called from async write callbacks running on different threads. The typical
 * implementation should use thread-safe data structures (e.g., ConcurrentHashMap) and atomic
 * operations to handle concurrent updates.
 *
 * <p><b>Usage:</b> The SinkWriter calls {@link #reportOffset(TableBucket, long)} after each
 * successful write operation to report the bucket and offset of the written record. The
 * implementing operator should track the maximum offset for each bucket to ensure correct state
 * during checkpointing.
 */
public interface ProducerOffsetReporter {

    /**
     * Reports a written offset for a bucket.
     *
     * <p>This method is called by the SinkWriter after a successful write operation to report the
     * bucket and offset of the written record. Implementations should track the maximum offset for
     * each bucket, as multiple writes to the same bucket may occur concurrently.
     *
     * <p>This method must be thread-safe to handle async write callbacks from multiple threads.
     *
     * @param bucket The bucket that was written to
     * @param offset The offset of the written record
     */
    void reportOffset(TableBucket bucket, long offset);
}
