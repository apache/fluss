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

package org.apache.fluss.lake.hudi.tiering;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HudiWriteResultSerializer}. */
class HudiWriteResultSerializerTest {

    @Test
    void testSerializeAndDeserializeEmptyWriteResult() throws Exception {
        HudiWriteResultSerializer serializer = new HudiWriteResultSerializer();
        HudiWriteResult writeResult =
                new HudiWriteResult(Collections.emptyMap(), Collections.emptyMap());

        HudiWriteResult deserialized =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(writeResult));

        assertThat(deserialized.getWriteStatuses()).isEmpty();
        assertThat(deserialized.getCompactionWriteStatuses()).isEmpty();
    }

    @Test
    void testRejectUnsupportedVersion() {
        HudiWriteResultSerializer serializer = new HudiWriteResultSerializer();

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported HudiWriteResult version");
    }
}
