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

package org.apache.fluss.flink.lake;

import org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit;
import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit.LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test case for {@link LakeSplitSerializer}. */
class LakeSplitSerializerTest {
    private static final byte LAKE_SNAPSHOT_SPLIT_KIND = -1;

    private static final int SERIALIZER_VERSION = 3;

    private static final byte[] TEST_DATA = "test-lake-split".getBytes();

    private static final int STOPPING_OFFSET = 1024;

    private static final LakeSplit LAKE_SPLIT =
            new TestLakeSplit(0, Collections.singletonList("2025-08-18"));

    private final SimpleVersionedSerializer<LakeSplit> sourceSplitSerializer =
            new TestSimpleVersionedSerializer();

    private TableBucket tableBucket = new TableBucket(0, 1L, 0);

    private final LakeSplitSerializer serializer = new LakeSplitSerializer(sourceSplitSerializer);

    @Test
    void testSerializeAndDeserializeLakeSnapshotSplit() throws IOException {
        // Prepare test data
        LakeSnapshotSplit originalSplit =
                new LakeSnapshotSplit(tableBucket, "2025-08-18", LAKE_SPLIT);

        DataOutputSerializer output = new DataOutputSerializer(STOPPING_OFFSET);
        serializer.serialize(output, originalSplit);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(
                        LAKE_SNAPSHOT_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(deserializedSplit instanceof LakeSnapshotSplit).isTrue();
        LakeSnapshotSplit result = (LakeSnapshotSplit) deserializedSplit;

        assertThat(tableBucket).isEqualTo(result.getTableBucket());
        assertThat("2025-08-18").isEqualTo(result.getPartitionName());
        assertThat(LAKE_SPLIT).isEqualTo(result.getLakeSplit());
    }

    @Test
    void testSerializeAndDeserializeLakeSnapshotAndFlussLogSplit() throws IOException {
        LakeSnapshotAndFlussLogSplit originalSplit =
                new LakeSnapshotAndFlussLogSplit(
                        tableBucket,
                        "2025-08-18",
                        Collections.singletonList(LAKE_SPLIT),
                        EARLIEST_OFFSET,
                        STOPPING_OFFSET);

        DataOutputSerializer output = new DataOutputSerializer(STOPPING_OFFSET);
        serializer.serialize(output, originalSplit);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(
                        LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(output.getCopyOfBuffer()));

        assertThat(deserializedSplit instanceof LakeSnapshotAndFlussLogSplit).isTrue();
        LakeSnapshotAndFlussLogSplit result = (LakeSnapshotAndFlussLogSplit) deserializedSplit;

        assertThat(tableBucket).isEqualTo(result.getTableBucket());
        assertThat("2025-08-18").isEqualTo(result.getPartitionName());
        assertThat(Collections.singletonList(LAKE_SPLIT)).isEqualTo(result.getLakeSplits());
        assertThat(EARLIEST_OFFSET).isEqualTo(result.getStartingOffset());
        assertThat((long) STOPPING_OFFSET).isEqualTo(result.getStoppingOffset().get());
    }

    @Test
    void testDeserializeWithWrongSplitKind() throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(1024);
        output.writeInt(0);

        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        (byte) 99,
                                        tableBucket,
                                        "2023-10-01",
                                        new DataInputDeserializer(output.getCopyOfBuffer())))
                .withFailMessage(() -> "Unsupported split kind: ")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static class TestSimpleVersionedSerializer
            implements SimpleVersionedSerializer<LakeSplit> {

        @Override
        public byte[] serialize(LakeSplit split) throws IOException {
            return TEST_DATA;
        }

        @Override
        public LakeSplit deserialize(int version, byte[] serialized) throws IOException {
            return LAKE_SPLIT;
        }

        @Override
        public int getVersion() {
            return SERIALIZER_VERSION;
        }
    }

    private static class TestLakeSplit implements LakeSplit {

        private int bucket;
        private List<String> partition;

        public TestLakeSplit(int bucket, List<String> partition) {
            this.bucket = bucket;
            this.partition = partition;
        }

        @Override
        public String toString() {
            return "TestLakeSplit";
        }

        @Override
        public int bucket() {
            return bucket;
        }

        @Override
        public List<String> partition() {
            return partition;
        }
    }
}
