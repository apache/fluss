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

package org.apache.fluss.lake.hudi.source;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HudiSplitSerializer}. */
class HudiSplitSerializerTest {

    @Test
    void testSerializeAndDeserialize() throws Exception {
        HudiSplitSerializer serializer = new HudiSplitSerializer();
        HudiSplit split =
                new HudiSplit(
                        new FileSlice(
                                new HoodieFileGroupId(
                                        "dt=20260608", "00000000-0000-0000-0000-000000000001"),
                                "20260608010101000"),
                        1,
                        Collections.singletonList("20260608"));

        HudiSplit deserialized =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(split));

        assertThat(deserialized).isEqualTo(split);
        assertThat(deserialized.getFileSlice().getFileGroupId())
                .isEqualTo(split.getFileSlice().getFileGroupId());
    }

    @Test
    void testDeserializeRejectsUnknownVersion() {
        HudiSplitSerializer serializer = new HudiSplitSerializer();

        assertThatThrownBy(() -> serializer.deserialize(2, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported HudiSplit serialization version");
    }
}
