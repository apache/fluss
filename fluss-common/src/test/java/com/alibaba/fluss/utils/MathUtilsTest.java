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

package com.alibaba.fluss.utils;

import org.junit.jupiter.api.Test;

import static com.alibaba.fluss.utils.MathUtils.log2strict;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test of {@link com.alibaba.fluss.utils.MathUtils}. */
public class MathUtilsTest {

    @Test
    void testLog2strict() {
        assertThatThrownBy(() -> log2strict(0))
                .isInstanceOf(ArithmeticException.class)
                .hasMessageContaining("Logarithm of zero is undefined.");

        assertThatThrownBy(() -> log2strict(10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a power of two.");

        assertThat(log2strict(16)).isEqualTo(4);
    }

    @Test
    void testCeilDiv() {
        assertThat(MathUtils.ceilDiv(10, 3)).isEqualTo(4);
        assertThat(MathUtils.ceilDiv(10, 10)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 1)).isEqualTo(10);
        assertThat(MathUtils.ceilDiv(10, 5)).isEqualTo(2);
        assertThat(MathUtils.ceilDiv(10, 6)).isEqualTo(2);
        assertThat(MathUtils.ceilDiv(10, 7)).isEqualTo(2);
        assertThat(MathUtils.ceilDiv(10, 8)).isEqualTo(2);
        assertThat(MathUtils.ceilDiv(10, 9)).isEqualTo(2);
        assertThat(MathUtils.ceilDiv(10, 10)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 11)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 12)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 13)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 14)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 15)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 16)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 17)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 18)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 19)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 20)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 21)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 22)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 23)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 24)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 25)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 26)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 27)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 28)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 29)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 30)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 31)).isEqualTo(1);
        assertThat(MathUtils.ceilDiv(10, 32)).isEqualTo(1);
    }
}
