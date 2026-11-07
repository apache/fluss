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

package org.apache.fluss.utils;

import org.apache.fluss.exception.InvalidConsumerGroupIdException;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.fluss.utils.ConsumerGroupUtils.validateGroupId;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for consumer-group naming rules and error messages. */
class ConsumerGroupUtilsTest {

    @ParameterizedTest
    @NullAndEmptySource
    void testMissingGroupId(String groupId) {
        assertThatThrownBy(() -> validateGroupId(groupId))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessage("Consumer group id must not be null or empty.");
    }

    @Test
    void testReservedPrefix() {
        assertThatThrownBy(() -> validateGroupId("__internal"))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessage("Consumer group id must not start with '__'.");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                ".",
                "..",
                " ",
                "default.consumer",
                "default/consumer",
                "default:consumer",
                "消费组",
                "default-*",
                "*"
            })
    void testInvalidCharacters(String groupId) {
        assertThatThrownBy(() -> validateGroupId(groupId))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessage(
                        "Consumer group id may contain only ASCII letters, digits, '_' and '-'.");
    }

    @Test
    void testLengthLimit() {
        assertThatCode(() -> validateGroupId(StringUtils.repeat("a", 200)))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> validateGroupId(StringUtils.repeat("a", 201)))
                .isInstanceOf(InvalidConsumerGroupIdException.class)
                .hasMessage("Consumer group id must not exceed 200 characters.");
    }

    @ParameterizedTest
    @ValueSource(strings = {"default-consumer", "Consumer_1", "0", "_", "-", "consumer__1"})
    void testValidGroupId(String groupId) {
        assertThatCode(() -> validateGroupId(groupId)).doesNotThrowAnyException();
    }
}
