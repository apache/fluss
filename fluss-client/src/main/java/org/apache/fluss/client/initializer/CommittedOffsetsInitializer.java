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

package org.apache.fluss.client.initializer;

import org.apache.fluss.config.ConsumerOffsetReset;
import org.apache.fluss.metadata.GroupId;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An initializer that starts each bucket from the last offset committed by the given consumer
 * group. Buckets that have no committed offset for the group follow the configured {@link
 * ConsumerOffsetReset}.
 *
 * <p>Package private and should be instantiated via {@link OffsetsInitializer#committed(String,
 * ConsumerOffsetReset)}.
 */
class CommittedOffsetsInitializer implements OffsetsInitializer {
    private static final long serialVersionUID = 1L;

    private final String groupId;
    private final ConsumerOffsetReset resetPolicy;

    CommittedOffsetsInitializer(String groupId, ConsumerOffsetReset resetPolicy) {
        this.groupId = GroupId.of(groupId).getGroupId();
        this.resetPolicy = resetPolicy;
    }

    @Override
    public Map<Integer, Long> getBucketOffsets(
            @Nullable String partitionName,
            Collection<Integer> buckets,
            BucketOffsetsRetriever bucketOffsetsRetriever) {
        Map<Integer, Long> committed =
                bucketOffsetsRetriever.committedOffsets(groupId, partitionName, buckets);

        // Buckets whose committed offset is missing (never committed) or negative follow the
        // configured reset policy.
        List<Integer> missing = new ArrayList<>();
        Map<Integer, Long> initial = new HashMap<>(buckets.size());
        for (Integer bucket : buckets) {
            Long offset = committed.get(bucket);
            if (offset == null || offset < 0L) {
                missing.add(bucket);
            } else {
                initial.put(bucket, offset);
            }
        }
        if (!missing.isEmpty()) {
            if (resetPolicy == ConsumerOffsetReset.EARLIEST) {
                initial.putAll(
                        OffsetsInitializer.earliest()
                                .getBucketOffsets(partitionName, missing, bucketOffsetsRetriever));
            } else if (resetPolicy == ConsumerOffsetReset.LATEST) {
                initial.putAll(
                        OffsetsInitializer.latest()
                                .getBucketOffsets(partitionName, missing, bucketOffsetsRetriever));
            } else {
                throw new IllegalStateException(
                        "No committed offset for buckets "
                                + missing
                                + " and the reset policy is none");
            }
        }
        return initial;
    }
}
