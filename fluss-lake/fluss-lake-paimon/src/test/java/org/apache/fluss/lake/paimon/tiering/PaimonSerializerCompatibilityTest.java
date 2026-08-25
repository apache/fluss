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

package org.apache.fluss.lake.paimon.tiering;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.index.GlobalIndexMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests compatibility of Paimon tiering state across Paimon upgrades. */
class PaimonSerializerCompatibilityTest {

    private static final byte[] PAIMON_1_4_WRITE_RESULT =
            Base64.getDecoder()
                    .decode(
                            "AAAADAAAAAAAAAAAAAAAAAAAAAMBAAAACAAAAAAAAAAAAAAAAAAAAAEAAACQADAAAAAAAABH"
                                    + "TE9CQUwAhgoAAABAAAAAewAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAUAAA"
                                    + "AGluZGV4LWZpbGUAAAAAAAAAAAAAAAAAAAoAAAAAAAAAFAAAAAAAAAABAAAAAAAAABAAAAAw"
                                    + "AAAABAUAAAAAAIICAAAAAAAAAAIAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");

    private static final byte[] PAIMON_1_4_COMMITTABLE =
            Base64.getDecoder()
                    .decode(
                            "AAAAAAAAACoAAAAAAAAAAGMAAAABAAFrAAF2AAAACwAAAAEAAAAMAAAAAAAAAAAAAAAAAAAA"
                                    + "AwEAAAAIAAAAAAAAAAAAAAAAAAAAAQAAAJAAMAAAAAAAAEdMT0JBTACGCgAAAEAAAAB7AAAA"
                                    + "AAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEAAAABQAAAAaW5kZXgtZmlsZQAAAAAAAAAA"
                                    + "AAAAAAAACgAAAAAAAAAUAAAAAAAAAAEAAAAAAAAAEAAAADAAAAAEBQAAAAAAggIAAAAAAAAA"
                                    + "AgAAAAMAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");

    @Test
    void testWriteResultRoundTrip() throws Exception {
        PaimonWriteResultSerializer serializer = new PaimonWriteResultSerializer();
        PaimonWriteResult original = createWriteResult();
        byte[] serialized = serializer.serialize(original);
        PaimonWriteResult restored = serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(serializer.getVersion()).isEqualTo(2);
        assertThat(serialized)
                .isEqualTo(new CommitMessageSerializer().serialize(original.commitMessage()));
        assertThat(globalIndexMeta(restored).sourceMeta()).containsExactly((byte) 6, (byte) 7);
    }

    @Test
    void testDeserializePaimon14WriteResult() throws Exception {
        PaimonWriteResult restored =
                new PaimonWriteResultSerializer().deserialize(1, PAIMON_1_4_WRITE_RESULT);

        GlobalIndexMeta globalIndexMeta = globalIndexMeta(restored);
        assertThat(globalIndexMeta.rowRangeStart()).isEqualTo(10L);
        assertThat(globalIndexMeta.rowRangeEnd()).isEqualTo(20L);
        assertThat(globalIndexMeta.sourceMeta()).isNull();
    }

    @Test
    void testDeserializePaimon14Committable() throws Exception {
        PaimonCommittable restored =
                new PaimonCommittableSerializer().deserialize(1, PAIMON_1_4_COMMITTABLE);

        ManifestCommittable manifestCommittable = restored.manifestCommittable();
        assertThat(manifestCommittable.identifier()).isEqualTo(42L);
        assertThat(manifestCommittable.watermark()).isEqualTo(99L);
        assertThat(manifestCommittable.properties()).containsEntry("k", "v");
        CommitMessageImpl commitMessage =
                (CommitMessageImpl) manifestCommittable.fileCommittables().get(0);
        assertThat(
                        commitMessage
                                .newFilesIncrement()
                                .newIndexFiles()
                                .get(0)
                                .globalIndexMeta()
                                .sourceMeta())
                .isNull();
    }

    @Test
    void testRejectUnsupportedWriteResultVersion() throws Exception {
        PaimonWriteResultSerializer serializer = new PaimonWriteResultSerializer();

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        serializer.getVersion() + 1, PAIMON_1_4_WRITE_RESULT))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unsupported PaimonWriteResult version");
    }

    private PaimonWriteResult createWriteResult() {
        GlobalIndexMeta globalIndexMeta =
                new GlobalIndexMeta(
                        10L, 20L, 1, new int[] {2, 3}, new byte[] {4, 5}, new byte[] {6, 7});
        IndexFileMeta indexFileMeta =
                new IndexFileMeta("GLOBAL", "index-file", 123L, 4L, globalIndexMeta, null);
        DataIncrement dataIncrement =
                new DataIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(indexFileMeta),
                        Collections.emptyList());
        return new PaimonWriteResult(
                new CommitMessageImpl(
                        BinaryRow.EMPTY_ROW,
                        3,
                        8,
                        dataIncrement,
                        CompactIncrement.emptyIncrement()));
    }

    private GlobalIndexMeta globalIndexMeta(PaimonWriteResult writeResult) {
        CommitMessageImpl commitMessage = (CommitMessageImpl) writeResult.commitMessage();
        return commitMessage.newFilesIncrement().newIndexFiles().get(0).globalIndexMeta();
    }
}
