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

package org.apache.fluss.lake.source;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LakeSplit} Java serialization. */
class LakeSplitSerializationTest {

    @Test
    void testTestingLakeSplitRoundTrip() throws Exception {
        TestingLakeSplit original = new TestingLakeSplit(3, Arrays.asList("20250101", "12"));

        // Serialize
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }
        byte[] bytes = baos.toByteArray();

        // Deserialize
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            TestingLakeSplit deserialized = (TestingLakeSplit) ois.readObject();

            assertThat(deserialized.bucket()).isEqualTo(3);
            assertThat(deserialized.partition()).containsExactly("20250101", "12");
        }
    }

    @Test
    void testLakeSplitInterfaceIsSerializable() {
        assertThat(LakeSplit.class).isAssignableTo(java.io.Serializable.class);
    }
}
