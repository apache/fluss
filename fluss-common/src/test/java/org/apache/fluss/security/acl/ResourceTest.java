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

package org.apache.fluss.security.acl;

import org.apache.fluss.exception.InvalidConsumerGroupIdException;
import org.apache.fluss.metadata.GroupId;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
        GroupId groupId = GroupId.of("default-consumer");
        Resource group = Resource.group(groupId);
        assertThat(Resource.of("GROUP", "default-consumer")).isEqualTo(group);
        ResourceFilter filter = new ResourceFilter(ResourceType.GROUP, "default-consumer");
        assertThat(filter.matches(group)).isTrue();
        assertThat(filter.matches(Resource.group(GroupId.of("other-group")))).isFalse();
        assertThat(filter.matches(new Resource(ResourceType.TABLE, "default-consumer"))).isFalse();
        assertThat(new ResourceFilter(ResourceType.GROUP, null).matches(group)).isTrue();
        assertThat(ResourceFilter.ANY.matches(group)).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"default-consumer", "Default_consumer-1", "group1", "fluss-cluster"})
    void testGroupResourceName(String name) {
        Resource resource = Resource.group(GroupId.of(name));
        assertThat(resource).isEqualTo(new Resource(ResourceType.GROUP, name));
        assertThat(Resource.of("GROUP", name)).isEqualTo(resource);
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
                "a*b",
                "*"
            })
    void testGroupResourceRejectsInvalidGroupId(String groupId) {
        assertThatThrownBy(() -> Resource.group(GroupId.of(groupId)))
                .isInstanceOf(InvalidConsumerGroupIdException.class);
    }

    @Test
    void testGroupWildcardResourceAndFilter() {
        Resource wildcard = Resource.groupWildcard();
        assertThat(wildcard.getType()).isEqualTo(ResourceType.GROUP);
        assertThat(wildcard.getName()).isEqualTo("*");
        assertThat(Resource.of("GROUP", "*")).isEqualTo(wildcard);
        ResourceFilter filter = ResourceFilter.of(ResourceType.GROUP, "*");
        assertThat(filter.matches(wildcard)).isTrue();
        assertThat(filter.matches(Resource.group(GroupId.of("default-consumer")))).isFalse();
    }

    @Test
    void testResourceFilterFactory() {
        ResourceFilter groupFilter = ResourceFilter.of(ResourceType.GROUP, "default-consumer");
        assertThat(groupFilter.getType()).isEqualTo(ResourceType.GROUP);
        assertThat(groupFilter.getName()).isEqualTo("default-consumer");
        assertThat(groupFilter)
                .isEqualTo(new ResourceFilter(ResourceType.GROUP, "default-consumer"));

        ResourceFilter anyGroupFilter = ResourceFilter.of(ResourceType.GROUP, null);
        assertThat(anyGroupFilter.matches(Resource.group(GroupId.of("default-consumer")))).isTrue();
        assertThat(anyGroupFilter.matches(Resource.group(GroupId.of("other-group")))).isTrue();
        assertThat(anyGroupFilter.matches(new Resource(ResourceType.TABLE, "default-consumer")))
                .isFalse();

        ResourceFilter tableFilter = ResourceFilter.of(ResourceType.TABLE, "db.table");
        assertThat(tableFilter.getName()).isEqualTo("db.table");
        assertThat(tableFilter.matches(new Resource(ResourceType.TABLE, "db.table"))).isTrue();
    }

    @Test
    void testResourceFilterValueContract() {
        ResourceFilter filter = new ResourceFilter(ResourceType.GROUP, "default-consumer");
        ResourceFilter sameFilter = new ResourceFilter(ResourceType.GROUP, "default-consumer");

        assertThat(filter).isEqualTo(sameFilter);
        assertThat(filter.hashCode()).isEqualTo(sameFilter.hashCode());
        assertThat(filter).isNotEqualTo(new ResourceFilter(ResourceType.TABLE, "default-consumer"));
        assertThat(filter).isNotEqualTo(new ResourceFilter(ResourceType.GROUP, "other-group"));
        assertThat(filter).isNotEqualTo(null);
        assertThat(filter).isNotEqualTo("default-consumer");

        assertThat(filter.toString()).isEqualTo("(resourceType=GROUP, name=default-consumer)");
        assertThat(ResourceFilter.ANY.toString()).isEqualTo("(resourceType=ANY, name=<any>)");
    }
}
