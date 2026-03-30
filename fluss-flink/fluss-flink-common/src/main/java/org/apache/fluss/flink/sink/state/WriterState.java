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
 * <p>This class uses {@link TableBucket} as the key to support partitioned tables. Each bucket is
 * uniquely identified by its table ID, partition ID (if applicable), and bucket ID.
 */
public class WriterState implements Serializable {

    private static final long serialVersionUID = 1L;

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

    public WriterState(Map<TableBucket, Long> bucketOffsets) {
        if (bucketOffsets == null) {
            throw new IllegalArgumentException("bucketOffsets must not be null");
        }
        // Validate no null or negative offsets
        // Note: offset=0 is valid and means the bucket was empty at checkpoint time
        // (no records have been written to this bucket yet)
        for (Map.Entry<TableBucket, Long> entry : bucketOffsets.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("TableBucket in bucketOffsets must not be null");
            }
            if (entry.getValue() == null || entry.getValue() < 0) {
                throw new IllegalArgumentException(
                        "Invalid offset for bucket " + entry.getKey() + ": " + entry.getValue());
            }
        }
        this.bucketOffsets = Collections.unmodifiableMap(new HashMap<>(bucketOffsets));
    }

    public Map<TableBucket, Long> getBucketOffsets() {
        return bucketOffsets;
    }

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
        return Objects.equals(bucketOffsets, that.bucketOffsets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketOffsets);
    }

    @Override
    public String toString() {
        return "WriterState{" + "bucketOffsets=" + bucketOffsets + '}';
    }
}
