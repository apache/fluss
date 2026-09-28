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

import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests exclusive-end log boundaries. */
final class FlussLogRangeTest {
    @Test
    void testRoundTripAndEmptyRange() {
        FlussLogRange range = new FlussLogRange(11, Long.MAX_VALUE);
        FlussLogRange copy =
                jsonCodec(FlussLogRange.class)
                        .fromJson(jsonCodec(FlussLogRange.class).toJson(range));
        assertThat(copy).isEqualTo(range).hasSameHashCodeAs(range);
        assertThat(copy.getStartOffset()).isEqualTo(11);
        assertThat(copy.getStoppingOffset()).isEqualTo(Long.MAX_VALUE);
        assertThat(copy.isEmpty()).isFalse();
        assertThat(new FlussLogRange(1000, 1000).isEmpty()).isTrue();
        assertThat(range).isNotEqualTo(new FlussLogRange(12, Long.MAX_VALUE));
        assertThat(range).isNotEqualTo(new FlussLogRange(11, 12));
    }

    @Test
    void testInvalidRanges() {
        assertThatThrownBy(() -> new FlussLogRange(-1, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FlussLogRange(2, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
