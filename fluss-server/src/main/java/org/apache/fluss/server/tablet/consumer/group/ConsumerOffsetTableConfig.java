/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.tablet.consumer.group;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;

import static org.apache.fluss.utils.Preconditions.checkNotNull;

/** Identity, layout and write configuration of the consumer-offset system table. */
@Internal
public final class ConsumerOffsetTableConfig {

    /** Database containing Fluss internal system tables. */
    public static final String SYSTEM_DATABASE = "sys";

    /** Name of the consumer-offset system table. */
    public static final String CONSUMER_OFFSET_TABLE_NAME = "consumer_offsets";

    /** Sentinel indicating that the table id has not been resolved. */
    public static final long UNRESOLVED_TABLE_ID = -1L;

    /** Sentinel indicating that the schema id has not been resolved. */
    public static final int UNRESOLVED_SCHEMA_ID = -1;

    /**
     * Offset commits always require the full ISR to acknowledge the write. A successful commit must
     * be durably persisted, so this value is intentionally not configurable.
     */
    public static final short OFFSET_COMMIT_REQUIRED_ACKS = -1;

    private final int maxMetadataSize;
    private final long offsetsRetentionMs;
    private final int offsetsTableNumBuckets;
    private final int offsetsTableReplicationFactor;
    private final int offsetCommitTimeoutMs;

    /** Creates a consumer-offset table configuration. */
    public ConsumerOffsetTableConfig(
            int maxMetadataSize,
            long offsetsRetentionMs,
            int offsetsTableNumBuckets,
            int offsetsTableReplicationFactor,
            int offsetCommitTimeoutMs) {
        this.maxMetadataSize = maxMetadataSize;
        this.offsetsRetentionMs = offsetsRetentionMs;
        this.offsetsTableNumBuckets = offsetsTableNumBuckets;
        this.offsetsTableReplicationFactor = offsetsTableReplicationFactor;
        this.offsetCommitTimeoutMs = offsetCommitTimeoutMs;
    }

    /** Creates the table configuration from the canonical {@link ConfigOptions} definitions. */
    public static ConsumerOffsetTableConfig from(Configuration configuration) {
        checkNotNull(configuration, "configuration must not be null");
        return new ConsumerOffsetTableConfig(
                Math.toIntExact(
                        configuration.get(ConfigOptions.OFFSET_METADATA_MAX_BYTES).getBytes()),
                configuration.get(ConfigOptions.OFFSETS_RETENTION_MS).toMillis(),
                configuration.getInt(ConfigOptions.OFFSETS_TABLE_NUM_BUCKETS),
                configuration.getInt(ConfigOptions.OFFSETS_TABLE_REPLICATION_FACTOR),
                Math.toIntExact(
                        configuration.get(ConfigOptions.OFFSET_COMMIT_TIMEOUT_MS).toMillis()));
    }

    /** Returns the maximum number of bytes allowed for offset metadata. */
    public int getMaxMetadataSize() {
        return maxMetadataSize;
    }

    /** Returns the retention period for inactive committed offsets. */
    public long getOffsetsRetentionMs() {
        return offsetsRetentionMs;
    }

    /** Returns the number of buckets in the consumer-offset table. */
    public int getOffsetsTableNumBuckets() {
        return offsetsTableNumBuckets;
    }

    /** Returns the replication factor of the consumer-offset table. */
    public int getOffsetsTableReplicationFactor() {
        return offsetsTableReplicationFactor;
    }

    /** Returns the timeout for committing consumer offsets. */
    public int getOffsetCommitTimeoutMs() {
        return offsetCommitTimeoutMs;
    }

    @Override
    public String toString() {
        return "ConsumerOffsetTableConfig{"
                + "maxMetadataSize="
                + maxMetadataSize
                + ", offsetsRetentionMs="
                + offsetsRetentionMs
                + ", offsetsTableNumBuckets="
                + offsetsTableNumBuckets
                + ", offsetsTableReplicationFactor="
                + offsetsTableReplicationFactor
                + ", offsetCommitTimeoutMs="
                + offsetCommitTimeoutMs
                + '}';
    }
}
