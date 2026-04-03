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

package org.apache.fluss.lake.lance.source;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Test class for {@link LanceSplitSerializer}. */
class LanceSplitSerializerTest {


    @Test
    void testSerializeDeserializeWithFullFields() throws IOException {
        LanceSplit split =
                new LanceSplit(12, 101L, 999L, 77L, 3, Arrays.asList("p0", "p1"));

        LanceSplitSerializer serializer = new LanceSplitSerializer();
        byte[] bytes = serializer.serialize(split);
        LanceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restored.fragmentId()).isEqualTo(12);
        assertThat(restored.snapshotId()).isEqualTo(101L);
        assertThat(restored.fragmentRows()).isEqualTo(999L);
        assertThat(restored.scanLimit()).isEqualTo(77L);
        assertThat(restored.bucket()).isEqualTo(3);
        assertThat(restored.partition()).containsExactly("p0", "p1");
    }

    @Test
    void testSerializeDeserializeWithEmptyPartition() throws IOException {
        LanceSplit split = new LanceSplit(1, 2L, 3L, -1L, -1, Collections.<String>emptyList());

        LanceSplitSerializer serializer = new LanceSplitSerializer();
        byte[] bytes = serializer.serialize(split);
        LanceSplit restored = serializer.deserialize(serializer.getVersion(), bytes);

        assertThat(restored.partition()).isEmpty();
        assertThat(restored.scanLimit()).isEqualTo(-1L);
    }
}
