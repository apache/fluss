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
import org.apache.fluss.bucketing.FlussBucketingFunction;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;

/** Validates consumer group ids and maps them to consumer-offset table buckets. */
@Internal
public final class GroupManager {

    private final ConsumerOffsetTableConfig consumerOffsetTableConfig;

    public GroupManager(ConsumerOffsetTableConfig consumerOffsetTableConfig) {
        this.consumerOffsetTableConfig = consumerOffsetTableConfig;
    }

    public static GroupManager from(Configuration configuration) {
        return new GroupManager(ConsumerOffsetTableConfig.from(configuration));
    }

    public ConsumerOffsetTableConfig consumerOffsetTableConfig() {
        return consumerOffsetTableConfig;
    }

    /** Maps a group id to a bucket using the actual system-table bucket count. */
    public int bucketFor(String groupId, int bucketCount) {
        validateGroupId(groupId);
        byte[] bucketKey = ConsumerOffsetSchema.encodeGroupPrefix(groupId);
        return FlussBucketingFunction.bucketForRowKey(bucketKey, bucketCount);
    }

    /** Validates a consumer group id before routing or accessing its offsets. */
    public void validateGroupId(String groupId) {
        if (groupId == null || groupId.isEmpty()) {
            throw new InvalidConsumerGroupIdException(
                    "Consumer group id must not be null or empty.");
        }
    }
}
