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

package org.apache.fluss.metadata;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AggFunctionType}. */
class AggFunctionTypeTest {

    @ParameterizedTest(name = "{0}.supportsRetract()")
    @EnumSource(AggFunctionType.class)
    void testSupportsRetract(AggFunctionType type) {
        if (type == AggFunctionType.SUM
                || type == AggFunctionType.LAST_VALUE
                || type == AggFunctionType.LAST_VALUE_IGNORE_NULLS) {
            assertThat(type.supportsRetract()).as("%s should support retract", type).isTrue();
        } else {
            assertThat(type.supportsRetract()).as("%s should NOT support retract", type).isFalse();
        }
    }
}
