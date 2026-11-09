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

/** Tests physical column identity and ordinal validation. */
final class FlussColumnHandleTest {
    @Test
    void testJsonRoundTrip() {
        FlussColumnHandle handle = new FlussColumnHandle("MixedCase", 2);
        JsonCodec<FlussColumnHandle> codec = jsonCodec(FlussColumnHandle.class);
        assertThat(codec.fromJson(codec.toJson(handle))).isEqualTo(handle);
    }

    @Test
    void testEqualityIncludesCaseSensitiveNameAndOrdinal() {
        FlussColumnHandle handle = new FlussColumnHandle("ID", 0);
        FlussColumnHandle same = new FlussColumnHandle("ID", 0);
        assertThat(handle).isEqualTo(same).hasSameHashCodeAs(same);
        assertThat(handle).isNotEqualTo(new FlussColumnHandle("id", 0));
        assertThat(handle).isNotEqualTo(new FlussColumnHandle("ID", 1));
    }

    @Test
    void testRejectInvalidColumnIdentity() {
        assertThatThrownBy(() -> new FlussColumnHandle(null, 0))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("name");
        assertThatThrownBy(() -> new FlussColumnHandle("ID", -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ordinalPosition");
    }
}
