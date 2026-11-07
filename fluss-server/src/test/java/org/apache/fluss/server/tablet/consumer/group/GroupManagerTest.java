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

import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.InvalidConsumerGroupIdException;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GroupManagerTest {

    private final GroupManager groupManager = GroupManager.from(new Configuration());

    @Test
    void testStableBucketRouting() {
        int bucketCount = 50;
        int bucket = groupManager.bucketFor("group-a", bucketCount);

        assertThat(groupManager.bucketFor("group-a", bucketCount)).isEqualTo(bucket);
        assertThat(bucket).isBetween(0, bucketCount - 1);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                ".",
                "..",
                " ",
                "default.consumer",
                "default/consumer",
                "default:consumer",
                "消费组",
                "__internal",
                "default-*",
                "*"
            })
    void testInvalidGroupId(String groupId) {
        assertThatThrownBy(() -> groupManager.bucketFor(groupId, 50))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessageContaining("Consumer group id");
    }

    @ParameterizedTest
    @ValueSource(strings = {"Default_consumer-1", "0", "_consumer", "consumer__1"})
    void testValidGroupId(String groupId) {
        assertThat(groupManager.bucketFor(groupId, 50)).isBetween(0, 49);
    }

    @Test
    void testGroupIdLengthLimit() {
        assertThat(groupManager.bucketFor(StringUtils.repeat("a", 200), 50)).isBetween(0, 49);
        assertThatThrownBy(() -> groupManager.bucketFor(StringUtils.repeat("a", 201), 50))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessageContaining("200");
    }
}
