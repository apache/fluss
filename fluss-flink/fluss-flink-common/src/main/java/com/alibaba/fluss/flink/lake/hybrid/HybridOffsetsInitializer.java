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

package com.alibaba.fluss.flink.lake.hybrid;

import com.alibaba.fluss.flink.source.enumerator.initializer.OffsetsInitializer;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link OffsetsInitializer} to initialize the offsets for Hybrid Fluss and
 * Lake.
 */
public class HybridOffsetsInitializer implements OffsetsInitializer {

    private final Map<Integer, Long> offsets;

    public HybridOffsetsInitializer(Map<Integer, Long> offsets) {
        this.offsets = offsets;
    }

    @Override
    public Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever) {

        Map<Integer, Long> bucketOffsets = new HashMap<>(offsets.size());
        for (Integer bucket : buckets) {
            bucketOffsets.put(bucket, offsets.get(bucket));
        }

        return bucketOffsets;
    }
}
