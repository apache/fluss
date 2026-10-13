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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;
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

        assertThat(deserialized.getWriteStats()).isEmpty();
        assertThat(deserialized.getCompactionWriteStats()).isEmpty();
    }

    @Test
    void testSerializeAndDeserializeWriteResult() throws Exception {
        HudiWriteResultSerializer serializer = new HudiWriteResultSerializer();
        HudiWriteResult writeResult =
                new HudiWriteResult(
                        writeStats("20260622000100000", "file-1", 2L), Collections.emptyMap());

        HudiWriteResult deserialized =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(writeResult));

        HudiWriteStats writeStats = deserialized.getWriteStats().get("20260622000100000");
        assertThat(writeStats.getWriteStats()).hasSize(1);
        assertThat(writeStats.getWriteStats().get(0).getFileId()).isEqualTo("file-1");
        assertThat(writeStats.getTotalErrorRecords()).isEqualTo(2L);
    }

    @Test
    void testRejectUnsupportedVersion() {
        HudiWriteResultSerializer serializer = new HudiWriteResultSerializer();

        assertThatThrownBy(() -> serializer.deserialize(serializer.getVersion() + 1, new byte[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported HudiWriteResult version");
    }

    @Test
    void testRejectCorruptedLength() throws Exception {
        HudiWriteResultSerializer serializer = new HudiWriteResultSerializer();

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

    private static Map<String, HudiWriteStats> writeStats(
            String instant, String fileId, long totalErrorRecords) {
        Map<String, HudiWriteStats> writeStats = new HashMap<>();
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setFileId(fileId);
        writeStat.setPartitionPath("partition");
        writeStat.setPath("partition/" + fileId + ".parquet");
        writeStat.setNumWrites(1L);
        writeStats.put(instant, new HudiWriteStats(singletonList(writeStat), totalErrorRecords));
        return writeStats;
    }
}
