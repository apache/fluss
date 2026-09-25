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

/** Tests the bounded bucket range sent to workers. */
final class FlussSplitTest {
    @Test
    void testJsonRoundTrip() {
        FlussSplit split = new FlussSplit(2, 11, Long.MAX_VALUE);
        JsonCodec<FlussSplit> codec = jsonCodec(FlussSplit.class);
        FlussSplit copy = codec.fromJson(codec.toJson(split));
        assertThat(copy.getBucketId()).isEqualTo(2);
        assertThat(copy.getStartOffset()).isEqualTo(11);
        assertThat(copy.getStoppingOffset()).isEqualTo(Long.MAX_VALUE);
        assertThat(copy.isRemotelyAccessible()).isTrue();
        assertThat(copy.getAddresses()).isEmpty();
    }

    @Test
    void testEmptyRangeIsValid() {
        FlussSplit split = new FlussSplit(0, 5, 5);
        assertThat(split.getStartOffset()).isEqualTo(split.getStoppingOffset());
    }

    @Test
    void testInvalidRanges() {
        assertThatThrownBy(() -> new FlussSplit(-1, 0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucketId");
        assertThatThrownBy(() -> new FlussSplit(0, -1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("startOffset");
        assertThatThrownBy(() -> new FlussSplit(0, 2, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stoppingOffset");
    }
}
