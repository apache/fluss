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

import org.apache.hudi.common.model.HoodieWriteStat;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HudiCommittableSerializer}. */
class HudiCommittableSerializerTest {

    @Test
    void testSerializeAndDeserializeCommittable() throws Exception {
        HudiCommittableSerializer serializer = new HudiCommittableSerializer();
        HudiCommittable committable =
                new HudiCommittable(writeStats("20260622000100000"), Collections.emptyMap());

        HudiCommittable deserialized =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(committable));

        assertThat(deserialized.getWriteStats()).containsOnlyKeys("20260622000100000").hasSize(1);
        HudiWriteStats writeStats = deserialized.getWriteStats().get("20260622000100000");
        assertThat(writeStats.getWriteStats()).hasSize(2);
        assertThat(writeStats.getWriteStats())
                .extracting(HoodieWriteStat::getFileId)
                .containsExactly("file-1", "file-2");
        assertThat(writeStats.getTotalErrorRecords()).isEqualTo(3L);
        assertThat(deserialized.getCompactionWriteStats()).isEmpty();
    }

    @Test
    void testBuilderAggregatesStatsByInstant() {
        HudiCommittable committable =
                HudiCommittable.builder()
                        .addWriteStats(writeStats("20260622000100000"))
                        .addWriteStats(writeStats("20260622000100000"))
                        .addCompactionWriteStats(writeStats("20260622000200000"))
                        .build();

        HudiWriteStats writeStats = committable.getWriteStats().get("20260622000100000");
        assertThat(writeStats.getWriteStats()).hasSize(4);
        assertThat(writeStats.getTotalErrorRecords()).isEqualTo(6L);
        assertThat(committable.getCompactionWriteStats().get("20260622000200000").getWriteStats())
                .hasSize(2);
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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(0);
            dos.writeInt(1);
            dos.writeInt(1024);
        }

        assertThatThrownBy(
                        () -> serializer.deserialize(serializer.getVersion(), baos.toByteArray()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Corrupted serialization: invalid CompactionWriteStats");
    }

    private static Map<String, HudiWriteStats> writeStats(String instant) {
        Map<String, HudiWriteStats> writeStats = new HashMap<>();
        writeStats.put(instant, new HudiWriteStats(writeStats(), 3L));
        return writeStats;
    }

    private static List<HoodieWriteStat> writeStats() {
        return Arrays.asList(writeStat("file-1"), writeStat("file-2"));
    }

    private static HoodieWriteStat writeStat(String fileId) {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setFileId(fileId);
        writeStat.setPartitionPath("partition");
        writeStat.setPath("partition/" + fileId + ".parquet");
        writeStat.setNumWrites(1L);
        return writeStat;
    }
}
