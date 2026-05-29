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

package org.apache.fluss.flink.adapter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link MultipleParameterToolAdapter}. */
abstract class FlinkMultipleParameterToolTest {

    @Test
    public void testToMap() {
        String[] args =
                new String[] {
                    "--multi1", "multiValue1", "--multi2", "multiValue2", "--multi1", "multiValue3"
                };

        MultipleParameterToolAdapter adapter = MultipleParameterToolAdapter.fromArgs(args);

        // The last value is used.
        assertThat(adapter.toMap()).containsEntry("multi1", "multiValue3");
        assertThat(adapter.toMap()).containsEntry("multi2", "multiValue2");
    }

    @Test
    public void testHas() {
        String[] args = new String[] {"--key1", "value1", "--key2", "value2"};
        MultipleParameterToolAdapter adapter = MultipleParameterToolAdapter.fromArgs(args);

        assertThat(adapter.has("key1")).isTrue();
        assertThat(adapter.has("key2")).isTrue();
        assertThat(adapter.has("nonexistent")).isFalse();
    }

    @Test
    public void testGet() {
        String[] args = new String[] {"--key1", "value1", "--key2", "value2"};
        MultipleParameterToolAdapter adapter = MultipleParameterToolAdapter.fromArgs(args);

        assertThat(adapter.get("key1")).isEqualTo("value1");
        assertThat(adapter.get("key2")).isEqualTo("value2");
        assertThat(adapter.get("nonexistent")).isNull();
    }

    @Test
    public void testGetMultiParameter() {
        String[] args = new String[] {"--multi", "val1", "--multi", "val2", "--single", "only"};
        MultipleParameterToolAdapter adapter = MultipleParameterToolAdapter.fromArgs(args);

        assertThat(adapter.getMultiParameter("multi")).containsExactly("val1", "val2");
        assertThat(adapter.getMultiParameter("single")).containsExactly("only");
    }
}
