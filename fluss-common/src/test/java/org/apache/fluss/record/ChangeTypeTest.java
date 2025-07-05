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

/** Tests for the {@link ChangeType} class. */
public class ChangeTypeTest {

    @Test
    void testGetSortString() {
        assertThat(ChangeType.APPEND_ONLY.shortString()).isEqualTo("+A");
        assertThat(ChangeType.INSERT.shortString()).isEqualTo("+I");
        assertThat(ChangeType.UPDATE_BEFORE.shortString()).isEqualTo("-U");
        assertThat(ChangeType.UPDATE_AFTER.shortString()).isEqualTo("+U");
        assertThat(ChangeType.DELETE.shortString()).isEqualTo("-D");
    }

    @Test
    void testToByteValue() {
        assertThat(ChangeType.APPEND_ONLY.toByteValue()).isEqualTo((byte) 0);
        assertThat(ChangeType.INSERT.toByteValue()).isEqualTo((byte) 1);
        assertThat(ChangeType.UPDATE_BEFORE.toByteValue()).isEqualTo((byte) 2);
        assertThat(ChangeType.UPDATE_AFTER.toByteValue()).isEqualTo((byte) 3);
        assertThat(ChangeType.DELETE.toByteValue()).isEqualTo((byte) 4);
    }

    @Test
    void testFromByteValue() {
        assertThat(ChangeType.fromByteValue((byte) 0)).isEqualTo(ChangeType.APPEND_ONLY);
        assertThat(ChangeType.fromByteValue((byte) 1)).isEqualTo(ChangeType.INSERT);
        assertThat(ChangeType.fromByteValue((byte) 2)).isEqualTo(ChangeType.UPDATE_BEFORE);
        assertThat(ChangeType.fromByteValue((byte) 3)).isEqualTo(ChangeType.UPDATE_AFTER);
        assertThat(ChangeType.fromByteValue((byte) 4)).isEqualTo(ChangeType.DELETE);

        assertThatThrownBy(() -> ChangeType.fromByteValue((byte) 5))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("Unsupported byte value");
    }
}
