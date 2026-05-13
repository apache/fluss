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

package org.apache.fluss.server.kv;

import org.apache.fluss.utils.types.Tuple2;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CompositeKeyEncoder}. */
class CompositeKeyEncoderTest {

    @Test
    void testEncodeDecodeRoundTrip() {
        String partitionName = "20240101";
        byte[] originalKey = "user-123".getBytes(StandardCharsets.UTF_8);

        byte[] composite = CompositeKeyEncoder.encode(partitionName, originalKey);
        Tuple2<String, byte[]> decoded = CompositeKeyEncoder.decode(composite);

        assertThat(decoded.f0).isEqualTo(partitionName);
        assertThat(decoded.f1).isEqualTo(originalKey);
    }

    @Test
    void testEncodeDecodeWithEmptyOriginalKey() {
        String partitionName = "20240101";
        byte[] originalKey = new byte[0];

        byte[] composite = CompositeKeyEncoder.encode(partitionName, originalKey);
        Tuple2<String, byte[]> decoded = CompositeKeyEncoder.decode(composite);

        assertThat(decoded.f0).isEqualTo(partitionName);
        assertThat(decoded.f1).isEqualTo(originalKey);
    }

    @Test
    void testEncodeDecodeWithEmptyPartitionName() {
        String partitionName = "";
        byte[] originalKey = "key-data".getBytes(StandardCharsets.UTF_8);

        byte[] composite = CompositeKeyEncoder.encode(partitionName, originalKey);
        Tuple2<String, byte[]> decoded = CompositeKeyEncoder.decode(composite);

        assertThat(decoded.f0).isEqualTo(partitionName);
        assertThat(decoded.f1).isEqualTo(originalKey);
    }

    @Test
    void testEncodeDecodeWithSpecialCharacters() {
        String partitionName = "日期$2024/01|01";
        byte[] originalKey = "key\u0000with\tnulls".getBytes(StandardCharsets.UTF_8);

        byte[] composite = CompositeKeyEncoder.encode(partitionName, originalKey);
        Tuple2<String, byte[]> decoded = CompositeKeyEncoder.decode(composite);

        assertThat(decoded.f0).isEqualTo(partitionName);
        assertThat(decoded.f1).isEqualTo(originalKey);
    }

    @Test
    void testDifferentPartitionNamesDifferentKeys() {
        byte[] originalKey = "same-key".getBytes(StandardCharsets.UTF_8);

        byte[] composite1 = CompositeKeyEncoder.encode("partition-a", originalKey);
        byte[] composite2 = CompositeKeyEncoder.encode("partition-b", originalKey);

        assertThat(composite1).isNotEqualTo(composite2);

        Tuple2<String, byte[]> decoded1 = CompositeKeyEncoder.decode(composite1);
        Tuple2<String, byte[]> decoded2 = CompositeKeyEncoder.decode(composite2);

        assertThat(decoded1.f0).isEqualTo("partition-a");
        assertThat(decoded2.f0).isEqualTo("partition-b");
        assertThat(decoded1.f1).isEqualTo(originalKey);
        assertThat(decoded2.f1).isEqualTo(originalKey);
    }
}
