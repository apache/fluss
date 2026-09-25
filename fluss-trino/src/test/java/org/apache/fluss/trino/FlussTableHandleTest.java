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

package org.apache.fluss.trino;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests table identity used for metadata lookup and stale-handle detection. */
final class FlussTableHandleTest {
    @Test
    void testTopologyIsPartOfIdentity() {
        FlussTableHandle handle =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, 4, 0);
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, 5, 0));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, 4, 1));
        assertThatThrownBy(
                        () -> new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucketCount");
        assertThatThrownBy(
                        () ->
                                new FlussTableHandle(
                                        "sales", "users", "Sales", "Users", 42, 3, 4, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucketCountEpoch");
    }

    @Test
    void testJsonRoundTrip() {
        FlussTableHandle handle =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, 4, Long.MAX_VALUE);
        JsonCodec<FlussTableHandle> codec = jsonCodec(FlussTableHandle.class);
        assertThat(codec.fromJson(codec.toJson(handle))).isEqualTo(handle);
    }

    @Test
    void testEqualityIncludesLogicalAndPhysicalIdentity() {
        FlussTableHandle handle =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, 4, 0);
        FlussTableHandle same =
                new FlussTableHandle("sales", "users", "Sales", "Users", 42, 3, 4, 0);
        assertThat(handle).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle("other", "users", "Sales", "Users", 42, 3, 4, 0));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle("sales", "other", "Sales", "Users", 42, 3, 4, 0));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle("sales", "users", "sales", "Users", 42, 3, 4, 0));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle("sales", "users", "Sales", "users", 42, 3, 4, 0));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle("sales", "users", "Sales", "Users", 43, 3, 4, 0));
        assertThat(handle)
                .isNotEqualTo(
                        new FlussTableHandle("sales", "users", "Sales", "Users", 42, 4, 4, 0));
    }

    @Test
    void testRejectNegativeIdentifiers() {
        assertThatThrownBy(
                        () -> new FlussTableHandle("sales", "users", "Sales", "Users", -1, 0, 4, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("tableId");
        assertThatThrownBy(
                        () -> new FlussTableHandle("sales", "users", "Sales", "Users", 0, -1, 4, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schemaId");
    }

    @Test
    void testRejectMissingPhysicalName() {
        assertThatThrownBy(() -> new FlussTableHandle("sales", "users", null, "Users", 42, 3, 4, 0))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("flussDatabaseName");
        assertThatThrownBy(() -> new FlussTableHandle("sales", "users", "Sales", null, 42, 3, 4, 0))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("flussTableName");
    }
}
