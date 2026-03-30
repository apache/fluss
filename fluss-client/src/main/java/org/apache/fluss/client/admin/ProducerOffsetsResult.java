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

package org.apache.fluss.client.admin;

import org.apache.fluss.annotation.PublicEvolving;
import org.apache.fluss.metadata.TableBucket;

import java.util.Collections;
import java.util.Map;

/**
 * Result containing producer offset snapshot data.
 *
 * <p>This class holds the offset snapshot for a producer, which is used for undo recovery when a
 * Flink job fails over before completing its first checkpoint.
 *
 * <p>The snapshot contains bucket offsets organized by table ID, allowing the Flink Operator
 * Coordinator to coordinate undo recovery across all subtasks.
 *
 * @since 0.9
 */
@PublicEvolving
public class ProducerOffsetsResult {

    private final String producerId;
    private final Map<Long, Map<TableBucket, Long>> tableOffsets;
    private final long expirationTime;

    /**
     * Creates a new ProducerOffsetsResult.
     *
     * @param producerId the producer ID (typically Flink job ID)
     * @param tableOffsets map of table ID to bucket offsets
     * @param expirationTime the expiration timestamp in milliseconds
     */
    public ProducerOffsetsResult(
            String producerId,
            Map<Long, Map<TableBucket, Long>> tableOffsets,
            long expirationTime) {
        this.producerId = producerId;
        this.tableOffsets = Collections.unmodifiableMap(tableOffsets);
        this.expirationTime = expirationTime;
    }

    /**
     * Get the producer ID.
     *
     * @return the producer ID
     */
    public String getProducerId() {
        return producerId;
    }

    /**
     * Get the offset snapshot for all tables.
     *
     * @return unmodifiable map of table ID to bucket offsets
     */
    public Map<Long, Map<TableBucket, Long>> getTableOffsets() {
        return tableOffsets;
    }

    /**
     * Get the expiration timestamp.
     *
     * @return the expiration timestamp in milliseconds since epoch
     */
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public String toString() {
        return "ProducerOffsetsResult{"
                + "producerId='"
                + producerId
                + '\''
                + ", tableCount="
                + tableOffsets.size()
                + ", expirationTime="
                + expirationTime
                + '}';
    }
}
