/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.fluss.security.acl;

import org.apache.fluss.exception.InvalidConsumerGroupIdException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EmptySource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for ACL resource identity, filtering, and wire compatibility. */
class ResourceTest {

    @ParameterizedTest
    @CsvSource({"ANY,1", "CLUSTER,2", "DATABASE,3", "TABLE,4", "GROUP,5"})
    void testStableResourceTypeCodes(ResourceType type, byte code) {
        assertThat(type.getCode()).isEqualTo(code);
        assertThat(ResourceType.fromCode(code)).isEqualTo(type);
    }

    @Test
    void testGroupIdentityAndFiltering() {
        String groupId = "default-consumer";
        Resource group = Resource.group(groupId);
        assertThat(Resource.of("group", groupId)).isEqualTo(group);
        ResourceFilter filter = new ResourceFilter(ResourceType.GROUP, groupId);
        assertThat(filter.matches(group)).isTrue();
        assertThat(filter.matches(Resource.group("other-group"))).isFalse();
        assertThat(filter.matches(new Resource(ResourceType.TABLE, groupId))).isFalse();
        assertThat(new ResourceFilter(ResourceType.GROUP, null).matches(group)).isTrue();
        assertThat(ResourceFilter.ANY.matches(group)).isTrue();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(
            strings = {
                "default/consumer",
                "default:consumer",
                "default.consumer",
                "消费组",
                "default-*",
                "__internal",
                " "
            })
    void testInvalidGroupResourceName(String name) {
        assertThatThrownBy(() -> new Resource(ResourceType.GROUP, name))
                .isInstanceOf(InvalidConsumerGroupIdException.class);
        assertThatThrownBy(() -> Resource.group(name))
                .isInstanceOf(InvalidConsumerGroupIdException.class);
        assertThatThrownBy(() -> Resource.of("group", name))
                .isInstanceOf(InvalidConsumerGroupIdException.class);
    }

    @ParameterizedTest
    @EmptySource
    @ValueSource(
            strings = {
                "default/consumer",
                "default:consumer",
                "default.consumer",
                "消费组",
                "default-*",
                "__internal",
                " "
            })
    void testGroupFilterWithInvalidNameDoesNotMatch(String name) {
        ResourceFilter filter = new ResourceFilter(ResourceType.GROUP, name);
        assertThat(filter.matches(Resource.group("default-consumer"))).isFalse();
        assertThat(filter.matches(Resource.group("*"))).isFalse();
    }

    @Test
    void testGroupWildcardResourceAndFilter() {
        Resource wildcard = Resource.group("*");
        assertThat(wildcard.getType()).isEqualTo(ResourceType.GROUP);
        assertThat(wildcard.getName()).isEqualTo("*");
        ResourceFilter filter = new ResourceFilter(ResourceType.GROUP, "*");
        assertThat(filter.matches(wildcard)).isTrue();
        assertThat(filter.matches(Resource.group("default-consumer"))).isFalse();
    }
}
