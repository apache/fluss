/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MaskedValueUtilsTest {

    @Test
    void defaultMaskingRevealsFirstThreeChars() {
        String key = "AS7124FSDFER";
        String masked = MaskedValue.of(key).toString();
        assertThat(masked).isEqualTo("AS7*********");
    }

    @Test
    void shortValueStillMasksWithFixedStars() {
        String key = "AB";
        String masked = MaskedValue.of(key).toString();
        assertThat(masked).isEqualTo("AB");
    }

    @Test
    void nullOrEmptyReturnsFixedStars() {
        assertThat(MaskedValue.of(null).toString()).isEqualTo("");
        assertThat(MaskedValue.of("").toString()).isEqualTo("");
    }

    @Test
    void customPrefixVisibleWorks() {
        String key = "ACCESSKEY123";
        String masked = MaskedValue.of(key, 2).toString();
        assertThat(masked).isEqualTo("AC**********");
    }

    @Test
    void equalsAndHashCodeBasedOnRealValueOnly() {
        MaskedValue a = MaskedValue.of("SAMEVALUE", 1);
        MaskedValue b = MaskedValue.of("SAMEVALUE", 5);
        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }
}
