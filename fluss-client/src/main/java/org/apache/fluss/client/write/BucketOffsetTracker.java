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

package org.apache.fluss.client.write;

import org.apache.fluss.annotation.Internal;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.utils.MapUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Tracks the latest acknowledged offset for each bucket written by the writer.
 *
 * <p>This class maintains a map from TableBucket to the last successfully acknowledged changelog
 * offset. It is used for exactly-once semantics support and checkpoint/failover recovery.
 */
@Internal
public class BucketOffsetTracker {

    private final Map<TableBucket, Long> bucketOffsets = MapUtils.newConcurrentHashMap();

    /**
     * Update the offset for a bucket. Only updates if the new offset is greater than the current
     * offset.
     *
     * @param tableBucket the table bucket
     * @param offset the new offset
     */
    public void updateOffset(TableBucket tableBucket, long offset) {
        if (offset < 0) {
            // Invalid offset, ignore
            return;
        }
        bucketOffsets.compute(tableBucket, (k, v) -> v == null ? offset : Math.max(v, offset));
    }

    /**
     * Get the offset for a specific bucket.
     *
     * @param tableBucket the table bucket
     * @return the offset for the bucket, or null if not tracked
     */
    public Long getOffset(TableBucket tableBucket) {
        return bucketOffsets.get(tableBucket);
    }

    /**
     * Get a snapshot of all tracked bucket offsets.
     *
     * @return an unmodifiable map from TableBucket to offset
     */
    public Map<TableBucket, Long> getAllOffsets() {
        return Collections.unmodifiableMap(new HashMap<>(bucketOffsets));
    }

    /** Clear all tracked offsets. */
    public void clear() {
        bucketOffsets.clear();
    }
}
