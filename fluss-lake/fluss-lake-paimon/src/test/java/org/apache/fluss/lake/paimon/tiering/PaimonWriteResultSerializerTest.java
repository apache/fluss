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
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PaimonWriteResultSerializer}. */
class PaimonWriteResultSerializerTest {

    @Test
    void testSerializeAndDeserializeV2() throws IOException {
        PaimonWriteResultSerializer serializer = new PaimonWriteResultSerializer();
        CommitMessage commitMessage = createEmptyCommitMessage();

        // positive watermark
        PaimonWriteResult original = new PaimonWriteResult(commitMessage, 12345L);
        byte[] serialized = serializer.serialize(original);
        PaimonWriteResult deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserialized.getWatermark()).isEqualTo(12345L);

        // negative watermark
        original = new PaimonWriteResult(commitMessage, -12345L);
        serialized = serializer.serialize(original);
        deserialized = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserialized.getWatermark()).isEqualTo(-12345L);

        // null watermark
        original = new PaimonWriteResult(commitMessage, null);
        serialized = serializer.serialize(original);
        deserialized = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserialized.getWatermark()).isNull();
    }

    @Test
    void testDeserializeV1BackwardCompatibility() throws IOException {
        PaimonWriteResultSerializer serializer = new PaimonWriteResultSerializer();
        CommitMessage commitMessage = createEmptyCommitMessage();

        CommitMessageSerializer messageSer = new CommitMessageSerializer();
        byte[] v1Serialized = messageSer.serialize(commitMessage);

        PaimonWriteResult deserialized = serializer.deserialize(1, v1Serialized);
        assertThat(deserialized.getWatermark()).isNull();
    }

    private static CommitMessage createEmptyCommitMessage() {
        DataIncrement dataIncrement =
                new DataIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        CompactIncrement compactIncrement =
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
        return new CommitMessageImpl(BinaryRow.EMPTY_ROW, 0, null, dataIncrement, compactIncrement);
    }
}
