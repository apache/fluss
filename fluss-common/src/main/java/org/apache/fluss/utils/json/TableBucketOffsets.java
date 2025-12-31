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

package org.apache.fluss.utils.json;

import org.apache.fluss.metadata.TableBucket;

import java.util.Map;
import java.util.Objects;

/**
 * Represents the offsets for all buckets of a table. This class stores the mapping from {@link
 * TableBucket} to their corresponding offsets.
 *
 * <p>This class is used to track the log end offsets for each bucket in a table. It supports both
 * non-partitioned tables (where buckets are identified only by bucket id) and partitioned tables
 * (where buckets are identified by partition id and bucket id).
 *
 * <p>The offsets map contains entries for each bucket that has a valid offset. Missing buckets are
 * not included in the map.
 *
 * @see TableBucketOffsetsJsonSerde for JSON serialization and deserialization.
 */
public class TableBucketOffsets {

    /** The table ID that all buckets belong to. */
    private final long tableId;

    /**
     * The mapping from {@link TableBucket} to their offsets. The map contains entries only for
     * buckets that have valid offsets.
     */
    private final Map<TableBucket, Long> offsets;

    /**
     * Creates a new {@link TableBucketOffsets} instance.
     *
     * @param tableId the table ID that all buckets belong to
     * @param offsets the mapping from {@link TableBucket} to their offsets
     */
    public TableBucketOffsets(long tableId, Map<TableBucket, Long> offsets) {
        this.tableId = tableId;
        this.offsets = offsets;
    }

    /**
     * Returns the table ID that all buckets belong to.
     *
     * @return the table ID
     */
    public long getTableId() {
        return tableId;
    }

    /**
     * Returns the mapping from {@link TableBucket} to their offsets.
     *
     * @return the offsets map
     */
    public Map<TableBucket, Long> getOffsets() {
        return offsets;
    }

    /**
     * Serialize to a JSON byte array.
     *
     * @see TableBucketOffsetsJsonSerde
     */
    public byte[] toJsonBytes() {
        return JsonSerdeUtils.writeValueAsBytes(this, TableBucketOffsetsJsonSerde.INSTANCE);
    }

    /**
     * Deserialize from JSON byte array to an instance of {@link TableBucketOffsets}.
     *
     * @see TableBucketOffsets
     */
    public static TableBucketOffsets fromJsonBytes(byte[] json) {
        return JsonSerdeUtils.readValue(json, TableBucketOffsetsJsonSerde.INSTANCE);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableBucketOffsets that = (TableBucketOffsets) o;
        return tableId == that.tableId && Objects.equals(offsets, that.offsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableId, offsets);
    }

    @Override
    public String toString() {
        return "TableBucketOffsets{" + "tableId=" + tableId + ", offsets=" + offsets + '}';
    }
}
