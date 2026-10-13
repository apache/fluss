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

package org.apache.fluss.flink.sink;

import org.apache.fluss.flink.adapter.SinkAdapter;
import org.apache.fluss.testutils.common.MultiVersionTest;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link FlinkSink} is compiled against the common {@link SinkAdapter} but packaged against this
 * module's one. A signature drift between them only surfaces as an {@code AbstractMethodError} when
 * a writer is created at runtime, so assert the override resolves here.
 */
@MultiVersionTest
class Flink23SinkAdapterTest {

    @Test
    void testFlinkSinkImplementsEveryAbstractSinkAdapterMethod() throws Exception {
        assertThat(Modifier.isAbstract(FlinkSink.class.getModifiers())).isFalse();
        for (Method method : SinkAdapter.class.getDeclaredMethods()) {
            if (!Modifier.isAbstract(method.getModifiers())) {
                continue;
            }
            Method override =
                    FlinkSink.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
            assertThat(Modifier.isAbstract(override.getModifiers())).isFalse();
        }
    }
}
