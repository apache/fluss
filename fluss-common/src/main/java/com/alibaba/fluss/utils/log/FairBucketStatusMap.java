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

package com.alibaba.fluss.utils.log;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.metadata.TableBucket;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * This class is a useful building block for doing fetch requests where table bucket have to be
 * rotated via round-robin to ensure fairness and some level of determinism given the existence of a
 * limit on the fetch response size. Because the serialization of fetch requests is more efficient
 * if all buckets for the same table are grouped together, we do such grouping in the method `set`.
 *
 * <p>As buckets are moved to the end, the same table may be repeated more than once. In the optimal
 * case, a single table would "wrap around" and appear twice. However, as buckets are fetched in
 * different orders and buckets leadership changes, we will deviate from the optimal. If this turns
 * out to be an issue in practice, we can improve it by tracking the buckets per node or calling
 * `set` every so often.
 *
 * <p>Note that this class is not thread-safe except {@link #size()} which returns the number of
 * partitions currently tracked.
 */
@NotThreadSafe
@Internal
public final class FairBucketStatusMap<S> {

    private final Map<TableBucket, S> map = new LinkedHashMap<>();
    private final Set<TableBucket> bucketSetView = Collections.unmodifiableSet(map.keySet());

    /** The number of buckets that are currently assigned available in a thread safe manner. */
    private volatile int size = 0;

    public FairBucketStatusMap() {}

    public void moveToEnd(TableBucket tableBucket) {
        S status = map.remove(tableBucket);
        if (status != null) {
            map.put(tableBucket, status);
        }
    }

    public void updateAndMoveToEnd(TableBucket tableBucket, S status) {
        map.remove(tableBucket);
        map.put(tableBucket, status);
        updateSize();
    }

    public void update(TableBucket tableBucket, S status) {
        map.put(tableBucket, status);
        updateSize();
    }

    public void remove(TableBucket tableBucket) {
        map.remove(tableBucket);
        updateSize();
    }

    /**
     * Returns an unmodifiable view of the bucket in random order. changes to this bucketStatusMap
     * instance will be reflected in this view.
     */
    public Set<TableBucket> bucketSet() {
        return bucketSetView;
    }

    public void clear() {
        map.clear();
        updateSize();
    }

    public boolean contains(TableBucket tableBucket) {
        return map.containsKey(tableBucket);
    }

    public Map<TableBucket, S> bucketStatusMap() {
        return Collections.unmodifiableMap(map);
    }

    /** Returns the bucket status values in order. */
    public List<S> bucketStatusValues() {
        return new ArrayList<>(map.values());
    }

    public S statusValue(TableBucket tableBucket) {
        return map.get(tableBucket);
    }

    public void forEach(BiConsumer<TableBucket, S> biConsumer) {
        map.forEach(biConsumer);
    }

    /** Get the number of buckets that are currently being tracked. This is thread-safe. */
    public int size() {
        return size;
    }

    /**
     * Update the builder to have the received map as its status (i.e. the previous status is
     * cleared). The builder will "batch by table", so if we have a, b and c, each with two buckets,
     * we may end up with something like the following (the order of tables and buckets within
     * tables is dependent on the iteration order of the received map): a0, a1, b1, b0, c0, c1.
     */
    public void set(Map<TableBucket, S> bucketToStatus) {
        map.clear();
        update(bucketToStatus);
        updateSize();
    }

    private void updateSize() {
        size = map.size();
    }

    private void update(Map<TableBucket, S> bucketToStatus) {
        LinkedHashMap<Long, List<TableBucket>> tableToBuckets = new LinkedHashMap<>();
        for (TableBucket tableBucket : bucketToStatus.keySet()) {
            List<TableBucket> buckets =
                    tableToBuckets.computeIfAbsent(
                            tableBucket.getTableId(), k -> new ArrayList<>());
            buckets.add(tableBucket);
        }
        for (Map.Entry<Long, List<TableBucket>> entry : tableToBuckets.entrySet()) {
            for (TableBucket tableBucket : entry.getValue()) {
                S status = bucketToStatus.get(tableBucket);
                map.put(tableBucket, status);
            }
        }
    }
}
