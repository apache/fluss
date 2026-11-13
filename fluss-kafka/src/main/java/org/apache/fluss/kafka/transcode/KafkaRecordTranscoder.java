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

package org.apache.fluss.kafka.transcode;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.kafka.backend.produce.KafkaProduceCommand.Record;
import org.apache.fluss.metadata.TableInfo;

import java.util.List;

/**
 * Converts borrowed Kafka records into the native Fluss log representation. The owner must close
 * the transcoder when it is no longer needed.
 */
@Internal
public interface KafkaRecordTranscoder extends AutoCloseable {
    /**
     * Transcodes records from the requested Kafka partition according to the target Fluss table
     * schema and log format.
     *
     * <p>The caller supplies the current table metadata and treats it as an immutable snapshot.
     * Implementations may cache conversion plans by table ID and schema ID until invalidation or
     * expiration.
     *
     * <p>The caller keeps input storage alive and unchanged until this method returns or throws.
     * The output is independent of the input buffers, but retains pooled memory. The caller must
     * invoke {@link EncodedRecords#deallocate()} after the log append completes or fails, and
     * before the owner closes the shared memory segment pool.
     */
    EncodedRecords transcode(int partitionId, List<Record> records, TableInfo tableInfo)
            throws Exception;

    /** Releases resources owned by the transcoder. */
    @Override
    void close() throws Exception;
}
