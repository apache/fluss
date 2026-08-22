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

package org.apache.fluss.flink.sink.state;

import org.apache.fluss.metadata.TableBucket;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * State of the writer for checkpoint.
 *
 * <p>This state stores the last successfully written changelog offset for each bucket that this
 * writer is responsible for. During failover recovery, these offsets are used to generate undo logs
 * and rollback to the checkpoint state.
 *
 * <p>V2 state is a complete but sparse recovery baseline. It stores only positive offsets; a live
 * bucket absent from the map has an explicit baseline of zero. The state element itself, including
 * an element with an empty map, proves that the checkpoint was produced with this completeness
 * guarantee.
 *
 * <p>This class uses {@link TableBucket} as the key to support partitioned tables. Each bucket is
 * uniquely identified by its table ID, partition ID (if applicable), and bucket ID.
 */
public class WriterState implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final long NO_TABLE_ID = -1L;

    /** The semantic guarantees carried by the serialized writer state. */
    public enum StateFormat {
        /** Legacy state whose bucket map may be sparse. */
        V1_LEGACY,

        /** Complete state for the table identified by {@link WriterState#getTableId()}. */
        V2_COMPLETE
    }

    private final StateFormat stateFormat;
    private final long tableId;

    /**
     * Map from TableBucket to the last successfully written changelog offset.
     *
     * <p>For each bucket, the offset represents the last log offset that was successfully
     * acknowledged by the Fluss server when this checkpoint was taken.
     *
     * <p>Using TableBucket as key ensures correct handling of partitioned tables, where different
     * partitions may have buckets with the same bucket ID.
     */
    private final Map<TableBucket, Long> bucketOffsets;

    /** Creates legacy V1 state whose bucket map must not be treated as complete. */
    public WriterState(Map<TableBucket, Long> bucketOffsets) {
        this(StateFormat.V1_LEGACY, NO_TABLE_ID, bucketOffsets);
    }

    private WriterState(
            StateFormat stateFormat, long tableId, Map<TableBucket, Long> bucketOffsets) {
        if (bucketOffsets == null) {
            throw new IllegalArgumentException("bucketOffsets must not be null");
        }
        if (stateFormat == null) {
            throw new IllegalArgumentException("stateFormat must not be null");
        }
        if (stateFormat == StateFormat.V2_COMPLETE && tableId < 0) {
            throw new IllegalArgumentException("tableId must not be negative: " + tableId);
        }
        for (Map.Entry<TableBucket, Long> entry : bucketOffsets.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("TableBucket in bucketOffsets must not be null");
            }
            if (entry.getValue() == null || entry.getValue() < 0) {
                throw new IllegalArgumentException(
                        "Invalid offset for bucket " + entry.getKey() + ": " + entry.getValue());
            }
            if (stateFormat == StateFormat.V2_COMPLETE && entry.getKey().getTableId() != tableId) {
                throw new IllegalArgumentException(
                        "Bucket table ID "
                                + entry.getKey().getTableId()
                                + " does not match complete state table ID "
                                + tableId);
            }
        }
        this.stateFormat = stateFormat;
        this.tableId = tableId;
        this.bucketOffsets = Collections.unmodifiableMap(new HashMap<>(bucketOffsets));
    }

    /** Creates complete V2 state for the given table. */
    public static WriterState complete(long tableId, Map<TableBucket, Long> bucketOffsets) {
        return new WriterState(StateFormat.V2_COMPLETE, tableId, bucketOffsets);
    }

    /** Returns the state format and its completeness guarantee. */
    public StateFormat getStateFormat() {
        return stateFormat;
    }

    /**
     * Returns the table ID carried by complete V2 state.
     *
     * @throws IllegalStateException if this is legacy state
     */
    public long getTableId() {
        if (stateFormat != StateFormat.V2_COMPLETE) {
            throw new IllegalStateException("Legacy writer state does not carry a table ID");
        }
        return tableId;
    }

    /** Returns the immutable bucket-to-offset map. */
    public Map<TableBucket, Long> getBucketOffsets() {
        return bucketOffsets;
    }

    /** Returns the offset for the bucket, or {@code null} when it is absent. */
    public Long getOffsetForBucket(TableBucket tableBucket) {
        return bucketOffsets.get(tableBucket);
    }

    /**
     * Create an empty writer state.
     *
     * @return an empty writer state
     */
    public static WriterState empty() {
        return new WriterState(Collections.emptyMap());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WriterState that = (WriterState) o;
        return tableId == that.tableId
                && stateFormat == that.stateFormat
                && Objects.equals(bucketOffsets, that.bucketOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stateFormat, tableId, bucketOffsets);
    }

    @Override
    public String toString() {
        return "WriterState{"
                + "stateFormat="
                + stateFormat
                + ", tableId="
                + tableId
                + ", bucketOffsets="
                + bucketOffsets
                + '}';
    }
}
