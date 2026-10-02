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

package org.apache.fluss.lake.paimon.source;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for {@link PaimonSplitSerializer.RelocatingObjectInputStream}. */
class RelocatingObjectInputStreamTest {

    private static final String ORIGINAL_PREFIX =
            org.apache.fluss.lake.paimon.source.original.Probe.class.getPackage().getName() + ".";
    private static final String RELOCATED_PREFIX =
            org.apache.fluss.lake.paimon.source.relocated.Probe.class.getPackage().getName() + ".";

    @Test
    void testResolveClass() throws Exception {
        // the stream always carries the original class name, as written by a non-relocated build
        byte[] bytes =
                javaSerialize(new org.apache.fluss.lake.paimon.source.original.Probe("hello"));

        // relocated build: remapped to the relocated class, field values preserved
        Object remapped = deserialize(bytes, ORIGINAL_PREFIX, RELOCATED_PREFIX);
        assertThat(remapped)
                .isInstanceOf(org.apache.fluss.lake.paimon.source.relocated.Probe.class);
        assertThat(((org.apache.fluss.lake.paimon.source.relocated.Probe) remapped).value())
                .isEqualTo("hello");

        // non-relocated build (equal prefixes): remapping is a no-op
        assertThat(deserialize(bytes, ORIGINAL_PREFIX, ORIGINAL_PREFIX))
                .isInstanceOf(org.apache.fluss.lake.paimon.source.original.Probe.class);

        // remapped class missing: falls back to default resolution by the original name
        assertThat(deserialize(bytes, ORIGINAL_PREFIX, ORIGINAL_PREFIX + "nonexistent."))
                .isInstanceOf(org.apache.fluss.lake.paimon.source.original.Probe.class);
    }

    private Object deserialize(byte[] bytes, String originalPrefix, String actualPrefix)
            throws Exception {
        try (PaimonSplitSerializer.RelocatingObjectInputStream in =
                new PaimonSplitSerializer.RelocatingObjectInputStream(
                        new ByteArrayInputStream(bytes),
                        getClass().getClassLoader(),
                        originalPrefix,
                        actualPrefix)) {
            return in.readObject();
        }
    }

    private static byte[] javaSerialize(Object object) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
        }
        return baos.toByteArray();
    }
}
