/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.row.serializer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link Serializer} interface. */
class SerializerTest {

    @Test
    void testDefaultSerializeToString() {
        TestSerializer serializer = new TestSerializer();
        assertThatThrownBy(() -> serializer.serializeToString("test"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("serialize test to string is unsupported");
    }

    @Test
    void testDefaultDeserializeFromString() {
        TestSerializer serializer = new TestSerializer();
        assertThatThrownBy(() -> serializer.deserializeFromString("test"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("deserialize test from string is unsupported");
    }

    private static class TestSerializer implements Serializer<String> {
        @Override
        public Serializer<String> duplicate() {
            return this;
        }

        @Override
        public String copy(String from) {
            return from;
        }

        @Override
        public void serialize(String record, com.alibaba.fluss.memory.OutputView target) {
            // Not implemented for this test
        }

        @Override
        public String deserialize(com.alibaba.fluss.memory.InputView source) {
            return null;
        }
    }
}
