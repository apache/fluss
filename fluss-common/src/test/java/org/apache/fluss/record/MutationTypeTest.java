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

package org.apache.fluss.record;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MutationTypeTest {

    @Test
    void testEnumValues() {
        assertThat(MutationType.UPSERT.getValue()).isEqualTo((byte) 0);
        assertThat(MutationType.DELETE.getValue()).isEqualTo((byte) 1);
        assertThat(MutationType.RETRACT.getValue()).isEqualTo((byte) 2);
    }

    @Test
    void testFromValue() {
        assertThat(MutationType.fromValue((byte) 0)).isEqualTo(MutationType.UPSERT);
        assertThat(MutationType.fromValue((byte) 1)).isEqualTo(MutationType.DELETE);
        assertThat(MutationType.fromValue((byte) 2)).isEqualTo(MutationType.RETRACT);
    }

    @Test
    void testFromValueUnknownThrows() {
        assertThatThrownBy(() -> MutationType.fromValue((byte) 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown MutationType");
        assertThatThrownBy(() -> MutationType.fromValue((byte) -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> MutationType.fromValue(Byte.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testValuesCount() {
        assertThat(MutationType.values()).hasSize(3);
    }
}
