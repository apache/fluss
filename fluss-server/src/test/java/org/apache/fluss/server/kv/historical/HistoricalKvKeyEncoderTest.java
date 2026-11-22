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

package org.apache.fluss.server.kv.historical;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HistoricalKvKeyEncoder}. */
class HistoricalKvKeyEncoderTest {

    @Test
    void testEncodeKey() {
        assertEncodedKey("dt=2026-07-12", new byte[] {1, 2, 3});
        assertEncodedKey("地区=杭州", new byte[] {0, 1, -1});
        assertEncodedKey("dt=2026-07-12/region=cn", new byte[0]);
    }

    @Test
    void testEncodingHasUnambiguousPartitionBoundary() {
        byte[] first = HistoricalKvKeyEncoder.encode("ab", "c".getBytes(StandardCharsets.UTF_8));
        byte[] second = HistoricalKvKeyEncoder.encode("a", "bc".getBytes(StandardCharsets.UTF_8));

        assertThat(first).isNotEqualTo(second);
        assertThat(HistoricalKvKeyEncoder.encode("p1", new byte[] {1}))
                .isNotEqualTo(HistoricalKvKeyEncoder.encode("p2", new byte[] {1}));
        assertThat(HistoricalKvKeyEncoder.encode("p1", new byte[] {1}))
                .isEqualTo(HistoricalKvKeyEncoder.encode("p1", new byte[] {1}));
    }

    @Test
    void testRejectInvalidInput() {
        assertThatThrownBy(() -> HistoricalKvKeyEncoder.encode(null, new byte[0]))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> HistoricalKvKeyEncoder.encode("", new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> HistoricalKvKeyEncoder.encode("p", null))
                .isInstanceOf(NullPointerException.class);
    }

    private static void assertEncodedKey(String partitionName, byte[] primaryKey) {
        byte[] partitionNameBytes = partitionName.getBytes(StandardCharsets.UTF_8);
        byte[] encoded = HistoricalKvKeyEncoder.encode(partitionName, primaryKey);
        ByteBuffer buffer = ByteBuffer.wrap(encoded);

        int partitionNameLength = buffer.getInt();
        byte[] actualPartitionName = new byte[partitionNameLength];
        buffer.get(actualPartitionName);
        byte[] actualPrimaryKey = new byte[buffer.remaining()];
        buffer.get(actualPrimaryKey);

        assertThat(partitionNameLength).isEqualTo(partitionNameBytes.length);
        assertThat(actualPartitionName).isEqualTo(partitionNameBytes);
        assertThat(actualPrimaryKey).isEqualTo(primaryKey);
        assertThat(HistoricalKvKeyEncoder.extractOriginalPrimaryKey(encoded)).isEqualTo(primaryKey);
        assertThat(encoded).hasSize(Integer.BYTES + partitionNameBytes.length + primaryKey.length);
        assertThat(Arrays.copyOfRange(encoded, Integer.BYTES, encoded.length))
                .startsWith(partitionNameBytes);
    }
}
