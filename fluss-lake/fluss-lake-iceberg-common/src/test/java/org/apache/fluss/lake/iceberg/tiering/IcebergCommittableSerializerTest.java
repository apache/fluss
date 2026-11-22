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

package org.apache.fluss.lake.iceberg.tiering;

import org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.COMMITTABLE_DIR;
import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.assertCommittableEquals;
import static org.apache.fluss.lake.iceberg.utils.TestingGoldenFiles.goldenCommittable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/** UT for {@link IcebergCommittableSerializer}. */
class IcebergCommittableSerializerTest {

    private final IcebergCommittableSerializer serializer = new IcebergCommittableSerializer();

    @Test
    void testSerializeAndDeserialize() throws Exception {
        IcebergCommittable committable = goldenCommittable();
        IcebergCommittable deserialized =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));
        assertCommittableEquals(deserialized, committable);
        assertThat(deserialized.specs()).isEqualTo(committable.specs());
    }

    @Test
    void testSerializeEmptyCommittable() throws Exception {
        IcebergCommittable committable =
                IcebergCommittable.builder().addSpecs(TestingGoldenFiles.SPECS).build();
        IcebergCommittable deserialized =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));
        assertCommittableEquals(deserialized, committable);
    }

    @Test
    void testDeserializeGoldenFiles() throws Exception {
        IcebergCommittable expected = goldenCommittable();
        for (int version = TestingGoldenFiles.firstReadableVersion();
                version <= serializer.getVersion();
                version++) {
            IcebergCommittable actual =
                    serializer.deserialize(
                            version, TestingGoldenFiles.read(COMMITTABLE_DIR, version));
            assertCommittableEquals(actual, expected);
        }
    }

    @Test
    void testCurrentVersionMatchesGoldenFile() throws Exception {
        // other Iceberg versions write the Iceberg objects in the JSON differently
        assumeThat(TestingGoldenFiles.isGoldenIcebergVersion())
                .as("the golden files were written with another Iceberg version")
                .isTrue();
        // the current format must not change without bumping the version
        assertThat(serializer.serialize(goldenCommittable()))
                .isEqualTo(TestingGoldenFiles.read(COMMITTABLE_DIR, serializer.getVersion()));
    }

    /**
     * Regenerates the golden file of the current version. The golden file of version 1 was
     * generated with {@link IcebergCommittable} as it was before version 2, so it cannot be
     * regenerated.
     */
    @Test
    void testRegenerateGoldenFiles() throws Exception {
        assumeThat(TestingGoldenFiles.shouldRegenerate())
                .as("golden files are only regenerated on request")
                .isTrue();
        TestingGoldenFiles.write(
                COMMITTABLE_DIR,
                serializer.getVersion(),
                serializer.serialize(goldenCommittable()));
    }

    @Test
    void testDeserializeInvalidData() throws Exception {
        byte[] serialized = serializer.serialize(goldenCommittable());
        assertThatThrownBy(() -> serializer.deserialize(2, "{}".getBytes()))
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining("Missing field");
        assertThatThrownBy(() -> serializer.deserialize(2, "invalid".getBytes()))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> serializer.deserialize(3, serialized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported IcebergCommittable serializer version 3");
    }
}
