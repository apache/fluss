/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.fluss.metadata;

import org.apache.fluss.exception.InvalidConsumerGroupIdException;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link GroupId}. */
class GroupIdTest {

    @ParameterizedTest
    @ValueSource(
            strings = {"a", "group1", "default-consumer", "Default_consumer-1", "fluss-cluster"})
    void testValidGroupId(String groupId) {
        GroupId id = GroupId.of(groupId);
        assertThat(id.getGroupId()).isEqualTo(groupId);
        assertThat(id).isEqualTo(GroupId.of(groupId));
        assertThat(id).hasSameHashCodeAs(GroupId.of(groupId));
        assertThat(id).hasToString(groupId);
    }

    @Test
    void testNullGroupIdIsRejected() {
        assertThatThrownBy(() -> GroupId.of(null))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessageContaining("must not be null or empty");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "__internal",
                "default.consumer",
                "default/consumer",
                "default:consumer",
                "消费组",
                "default-*",
                "a*b"
            })
    void testInvalidGroupId(String groupId) {
        assertThatThrownBy(() -> GroupId.of(groupId))
                .isInstanceOf(InvalidConsumerGroupIdException.class);
    }

    @Test
    void testTooLongGroupIdIsRejected() {
        String groupId = StringUtils.repeat("a", 201);
        assertThatThrownBy(() -> GroupId.of(groupId))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessageContaining("must not be longer than 200 characters");
    }

    @Test
    void testMaxLengthGroupIdIsAccepted() {
        String groupId = StringUtils.repeat("a", 200);
        assertThat(GroupId.of(groupId).getGroupId()).isEqualTo(groupId);
    }
}
