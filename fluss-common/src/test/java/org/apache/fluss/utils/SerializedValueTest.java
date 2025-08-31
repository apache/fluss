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

package org.apache.fluss.utils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.fluss.utils.SerializedValue}. */
class SerializedValueTest {

    @Test
    void testSerializedValue() throws Exception {
        String value = "string-value";
        SerializedValue<String> serializedValue = new SerializedValue<>(value);

        // get the bytes of serializedValue
        byte[] serializedValueBytes = serializedValue.getByteArray();
        // verify the serializedBytes
        assertThat(serializedValueBytes).isEqualTo(InstantiationUtils.serializeObject(value));

        String deserializeValue =
                (String)
                        SerializedValue.fromBytes(serializedValueBytes)
                                .deserializeValue(Thread.currentThread().getContextClassLoader());
        // verify the deserializeValue
        assertThat(deserializeValue).isEqualTo(value);

        // verify a serializedValue created from object is equal to the serializedValue created
        // from the serialized bytes
        assertThat(serializedValue).isEqualTo(new SerializedValue<>(serializedValueBytes));
    }
}
