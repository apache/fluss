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

import org.apache.fluss.utils.InstantiationUtils;

import org.apache.hudi.client.WriteStatus;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HudiCommittableSerializer}. */
class HudiCommittableSerializerTest {

    @Test
    void testSerializeAndDeserializeCommittable() throws Exception {
        HudiCommittableSerializer serializer = new HudiCommittableSerializer();
        HudiCommittable committable =
                new HudiCommittable(writeStatuses("20260622000100000"), Collections.emptyMap());

        HudiCommittable deserialized =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(deserialized.getWriteStatuses())
                .containsOnlyKeys("20260622000100000")
                .hasSize(1);
        assertThat(deserialized.getWriteStatuses().get("20260622000100000")).hasSize(2);
        assertThat(deserialized.getCompactionWriteStatuses()).isEmpty();
    }

    @Test
    void testBuilderAggregatesStatusesByInstant() {
        HudiCommittable committable =
                HudiCommittable.builder()
                        .addWriteStatuses(writeStatuses("20260622000100000"))
                        .addWriteStatuses(writeStatuses("20260622000100000"))
                        .addCompactionWriteStatuses(writeStatuses("20260622000200000"))
                        .build();

        assertThat(committable.getWriteStatuses().get("20260622000100000")).hasSize(4);
        assertThat(committable.getCompactionWriteStatuses().get("20260622000200000")).hasSize(2);
    }

    @Test
    void testRejectUnsupportedVersion() {
        HudiCommittableSerializer serializer = new HudiCommittableSerializer();

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported HudiCommittable version");
    }

    @Test
    void testRejectCorruptedLength() throws Exception {
        HudiCommittableSerializer serializer = new HudiCommittableSerializer();
        byte[] emptyMapBytes = InstantiationUtils.serializeObject(Collections.emptyMap());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(emptyMapBytes.length);
            dos.write(emptyMapBytes);
            dos.writeInt(emptyMapBytes.length);
        }

        assertThatThrownBy(
                        () -> serializer.deserialize(serializer.getVersion(), baos.toByteArray()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Corrupted serialization: invalid CompactionWriteStatuses");
    }

    private static Map<String, java.util.List<WriteStatus>> writeStatuses(String instant) {
        Map<String, java.util.List<WriteStatus>> writeStatuses = new HashMap<>();
        writeStatuses.put(
                instant, Arrays.asList(new WriteStatus(false, 0.0), new WriteStatus(false, 0.0)));
        return writeStatuses;
    }
}
