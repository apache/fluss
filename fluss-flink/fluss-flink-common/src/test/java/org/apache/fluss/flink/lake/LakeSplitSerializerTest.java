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
import org.apache.fluss.flink.source.split.LogSplit;
import org.apache.fluss.flink.source.split.SourceSplitBase;
import org.apache.fluss.lake.serializer.SimpleVersionedSerializer;
import org.apache.fluss.lake.source.LakeSplit;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.fluss.client.table.scanner.log.LogScanner.EARLIEST_OFFSET;
import static org.apache.fluss.flink.lake.split.LakeSnapshotAndFlussLogSplit.LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test case for {@link LakeSplitSerializer}. */
class LakeSplitSerializerTest {
    private static final byte LAKE_SNAPSHOT_SPLIT_KIND = -1;

    private static final byte[] TEST_DATA = "test-lake-split".getBytes();

    private static final int STOPPING_OFFSET = 1024;

    private static final LakeSplit LAKE_SPLIT =
            new TestingLakeSplit(0, Collections.singletonList("2025-08-18"));

    private final SimpleVersionedSerializer<LakeSplit> sourceSplitSerializer =
            new TestSimpleVersionedSerializer();

    private final TableBucket tableBucket = new TableBucket(0, 1L, 0);

    private final LakeSplitSerializer serializer = new LakeSplitSerializer(sourceSplitSerializer);

    @Test
    void testSerializeAndDeserializeLakeSnapshotSplit() throws IOException {
        // Prepare test data
        int splitIndex = 1;
        LakeSnapshotSplit originalSplit =
                new LakeSnapshotSplit(tableBucket, "2025-08-18", LAKE_SPLIT, splitIndex);

        DataOutputSerializer output = new DataOutputSerializer(STOPPING_OFFSET);
        serializer.serialize(output, originalSplit);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(
                        LAKE_SNAPSHOT_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(output.getCopyOfBuffer()),
                        true);

        assertThat(deserializedSplit instanceof LakeSnapshotSplit).isTrue();
        LakeSnapshotSplit result = (LakeSnapshotSplit) deserializedSplit;

        assertThat(tableBucket).isEqualTo(result.getTableBucket());
        assertThat("2025-08-18").isEqualTo(result.getPartitionName());
        assertThat(LAKE_SPLIT).isEqualTo(result.getLakeSplit());
        assertThat(splitIndex).isEqualTo(result.getSplitIndex());
    }

    @Test
    void testSerializeAndDeserializeLakeSnapshotSplitWithRecordsToSkip() throws IOException {
        LakeSnapshotSplit originalSplit =
                new LakeSnapshotSplit(tableBucket, "2025-08-18", LAKE_SPLIT, 3, 42L);

        DataOutputSerializer output = new DataOutputSerializer(STOPPING_OFFSET);
        serializer.serialize(output, originalSplit);
        DataInputDeserializer input = new DataInputDeserializer(output.getCopyOfBuffer());

        LakeSnapshotSplit result =
                (LakeSnapshotSplit)
                        serializer.deserialize(
                                LAKE_SNAPSHOT_SPLIT_KIND, tableBucket, "2025-08-18", input, true);

        assertThat(result.getSplitIndex()).isEqualTo(3);
        assertThat(result.getRecordsToSkip()).isEqualTo(42L);
        assertThat(result.getLakeSplit()).isEqualTo(LAKE_SPLIT);
        // the whole lake split section is consumed
        assertThat(input.available()).isZero();
    }

    @Test
    void testSerializeAndDeserializeLakeSnapshotSplitBackwardCompatibility() throws IOException {
        SimpleVersionedSerializer<LakeSplit> sourceSplitSerializerV1 =
                new TestSimpleVersionedSerializer();
        SimpleVersionedSerializer<LakeSplit> sourceSplitSerializerV2 =
                new TestSimpleVersionedSerializerV2();
        LakeSplitSerializer serializerV1 = new LakeSplitSerializer(sourceSplitSerializerV1);
        LakeSplitSerializer serializerV2 = new LakeSplitSerializer(sourceSplitSerializerV2);

        // Prepare test data
        int splitIndex = 1;
        LakeSnapshotSplit originalSplit =
                new LakeSnapshotSplit(tableBucket, "2025-08-18", LAKE_SPLIT, splitIndex);

        DataOutputSerializer output = new DataOutputSerializer(STOPPING_OFFSET);
        serializerV1.serialize(output, originalSplit);

        SourceSplitBase deserializedSplit =
                serializerV2.deserialize(
                        LAKE_SNAPSHOT_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(output.getCopyOfBuffer()),
                        true);

        assertThat(deserializedSplit instanceof LakeSnapshotSplit).isTrue();
        LakeSnapshotSplit result = (LakeSnapshotSplit) deserializedSplit;

        assertThat(tableBucket).isEqualTo(result.getTableBucket());
        assertThat("2025-08-18").isEqualTo(result.getPartitionName());
        assertThat(LAKE_SPLIT).isEqualTo(result.getLakeSplit());
        assertThat(splitIndex).isEqualTo(result.getSplitIndex());
    }

    @Test
    void testSerializeAndDeserializeLakeSnapshotAndFlussLogSplit() throws IOException {
        LakeSnapshotAndFlussLogSplit originalSplit =
                new LakeSnapshotAndFlussLogSplit(
                        tableBucket,
                        "2025-08-18",
                        Collections.singletonList(LAKE_SPLIT),
                        EARLIEST_OFFSET,
                        STOPPING_OFFSET,
                        2,
                        1,
                        true);

        DataOutputSerializer output = new DataOutputSerializer(STOPPING_OFFSET);
        serializer.serialize(output, originalSplit);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(
                        LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(output.getCopyOfBuffer()),
                        true);

        assertThat(deserializedSplit instanceof LakeSnapshotAndFlussLogSplit).isTrue();
        LakeSnapshotAndFlussLogSplit result = (LakeSnapshotAndFlussLogSplit) deserializedSplit;

        assertThat(result.getTableBucket()).isEqualTo(tableBucket);
        assertThat(result.getPartitionName()).isEqualTo("2025-08-18");
        assertThat(result.getLakeSplits()).isEqualTo(Collections.singletonList(LAKE_SPLIT));
        assertThat(result.getStartingOffset()).isEqualTo(EARLIEST_OFFSET);
        assertThat(result.getStoppingOffset().get()).isEqualTo(STOPPING_OFFSET);
        assertThat(result.getCurrentLakeSplitIndex()).isEqualTo(1);
        assertThat(result.getRecordsToSkip()).isEqualTo(2);
        assertThat(result.isLakeSplitFinished()).isEqualTo(true);
    }

    @Test
    void testUnfinishedStreamingHybridSplitKeepsLakeMetadata() throws IOException {
        LakeSnapshotAndFlussLogSplit original =
                new LakeSnapshotAndFlussLogSplit(
                        tableBucket,
                        "2025-08-18",
                        Collections.singletonList(LAKE_SPLIT),
                        EARLIEST_OFFSET,
                        LogSplit.NO_STOPPING_OFFSET,
                        2L,
                        0,
                        false);

        DataOutputSerializer output = new DataOutputSerializer(64);
        serializer.serialize(output, original);

        SourceSplitBase decoded =
                serializer.deserialize(
                        LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(output.getCopyOfBuffer()),
                        true);

        LakeSnapshotAndFlussLogSplit restored = (LakeSnapshotAndFlussLogSplit) decoded;
        assertThat(restored.isStreaming()).isTrue();
        assertThat(restored.isLakeSplitFinished()).isFalse();
        assertThat(restored.getLakeSplits()).containsExactly(LAKE_SPLIT);
        assertThat(restored.getRecordsToSkip()).isEqualTo(2L);
    }

    @Test
    void testDeserializeLegacyCompletedStreamingHybridSplit() throws IOException {
        // Reproduce the existing serialized layout containing completed lake metadata.
        byte[] lakeBytes = sourceSplitSerializer.serialize(LAKE_SPLIT);
        DataOutputSerializer legacy = new DataOutputSerializer(64);
        legacy.writeInt(sourceSplitSerializer.getVersion());
        legacy.writeBoolean(true);
        legacy.writeInt(1);
        legacy.writeInt(lakeBytes.length);
        legacy.write(lakeBytes);
        legacy.writeLong(EARLIEST_OFFSET);
        legacy.writeLong(LogSplit.NO_STOPPING_OFFSET);
        legacy.writeLong(2L);
        legacy.writeInt(0);
        legacy.writeBoolean(true);

        SourceSplitBase decoded =
                serializer.deserialize(
                        LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(legacy.getCopyOfBuffer()),
                        true);

        LakeSnapshotAndFlussLogSplit restored = (LakeSnapshotAndFlussLogSplit) decoded;
        assertThat(restored.isStreaming()).isTrue();
        assertThat(restored.isLakeSplitFinished()).isTrue();
        assertThat(restored.getLakeSplits()).containsExactly(LAKE_SPLIT);
        assertThat(restored.getStartingOffset()).isEqualTo(EARLIEST_OFFSET);
        assertThat(restored.getRecordsToSkip()).isEqualTo(2L);
        assertThat(restored.getCurrentLakeSplitIndex()).isZero();

        // The next checkpoint must discard obsolete metadata from the legacy split.
        DataOutputSerializer compact = new DataOutputSerializer(64);
        serializer.serialize(compact, restored);
        assertThat(compact.getCopyOfBuffer().length).isLessThan(legacy.getCopyOfBuffer().length);

        LakeSnapshotAndFlussLogSplit compactRestored =
                (LakeSnapshotAndFlussLogSplit)
                        serializer.deserialize(
                                LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND,
                                tableBucket,
                                "2025-08-18",
                                new DataInputDeserializer(compact.getCopyOfBuffer()),
                                true);

        assertThat(compactRestored.getLakeSplits()).isNull();
        assertThat(compactRestored.isLakeSplitFinished()).isTrue();
        assertThat(compactRestored.getStartingOffset()).isEqualTo(EARLIEST_OFFSET);
    }

    @Test
    void testStreamingHybridWithoutLakeSnapshotRoundTrips() throws IOException {
        LakeSnapshotAndFlussLogSplit original =
                new LakeSnapshotAndFlussLogSplit(
                        tableBucket,
                        "2025-08-18",
                        null,
                        EARLIEST_OFFSET,
                        LogSplit.NO_STOPPING_OFFSET);

        DataOutputSerializer output = new DataOutputSerializer(64);
        serializer.serialize(output, original);

        SourceSplitBase decoded =
                serializer.deserialize(
                        LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND,
                        tableBucket,
                        "2025-08-18",
                        new DataInputDeserializer(output.getCopyOfBuffer()),
                        true);

        LakeSnapshotAndFlussLogSplit restored = (LakeSnapshotAndFlussLogSplit) decoded;
        assertThat(restored.isStreaming()).isTrue();
        assertThat(restored.isLakeSplitFinished()).isTrue();
        assertThat(restored.getLakeSplits()).isNull();
        assertThat(restored.getStartingOffset()).isEqualTo(EARLIEST_OFFSET);
    }

    @Test
    void testCompletedStreamingCheckpointRemainsCompactWithManyLakeSplits() throws IOException {
        LakeSnapshotAndFlussLogSplit active =
                new LakeSnapshotAndFlussLogSplit(
                        tableBucket,
                        "2025-08-18",
                        Collections.nCopies(1_000, LAKE_SPLIT),
                        42L,
                        LogSplit.NO_STOPPING_OFFSET,
                        7L,
                        0,
                        false);

        LakeSnapshotAndFlussLogSplit finished =
                new LakeSnapshotAndFlussLogSplit(
                        tableBucket,
                        "2025-08-18",
                        Collections.nCopies(1_000, LAKE_SPLIT),
                        42L,
                        LogSplit.NO_STOPPING_OFFSET,
                        7L,
                        1_000,
                        true);

        DataOutputSerializer activeOutput = new DataOutputSerializer(64);
        serializer.serialize(activeOutput, active);

        DataOutputSerializer finishedOutput = new DataOutputSerializer(64);
        serializer.serialize(finishedOutput, finished);
        byte[] finishedBytes = finishedOutput.getCopyOfBuffer();

        assertThat(finishedBytes.length).isLessThan(128);
        assertThat(activeOutput.getCopyOfBuffer().length).isGreaterThan(finishedBytes.length * 10);

        LakeSnapshotAndFlussLogSplit restored =
                (LakeSnapshotAndFlussLogSplit)
                        serializer.deserialize(
                                LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND,
                                tableBucket,
                                "2025-08-18",
                                new DataInputDeserializer(finishedBytes),
                                true);

        assertThat(restored.isStreaming()).isTrue();
        assertThat(restored.isLakeSplitFinished()).isTrue();
        assertThat(restored.getLakeSplits()).isNull();
        assertThat(restored.getStartingOffset()).isEqualTo(42L);
        assertThat(restored.getRecordsToSkip()).isEqualTo(7L);
        assertThat(restored.getCurrentLakeSplitIndex()).isEqualTo(1_000);

        DataOutputSerializer repeatedOutput = new DataOutputSerializer(64);
        serializer.serialize(repeatedOutput, restored);
        assertThat(repeatedOutput.getCopyOfBuffer()).containsExactly(finishedBytes);

        restored.updateWithStartingOffset(43L);
        DataOutputSerializer nextOutput = new DataOutputSerializer(64);
        serializer.serialize(nextOutput, restored);
        assertThat(nextOutput.getCopyOfBuffer().length).isEqualTo(finishedBytes.length);

        LakeSnapshotAndFlussLogSplit nextRestored =
                (LakeSnapshotAndFlussLogSplit)
                        serializer.deserialize(
                                LAKE_SNAPSHOT_FLUSS_LOG_SPLIT_KIND,
                                tableBucket,
                                "2025-08-18",
                                new DataInputDeserializer(nextOutput.getCopyOfBuffer()),
                                true);

        assertThat(nextRestored.getStartingOffset()).isEqualTo(43L);
        assertThat(nextRestored.getLakeSplits()).isNull();
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
                                        new DataInputDeserializer(output.getCopyOfBuffer()),
                                        true))
                .withFailMessage(() -> "Unsupported split kind: ")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static class TestSimpleVersionedSerializer
            implements SimpleVersionedSerializer<LakeSplit> {

        private static final int V1 = 1;

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
            return V1;
        }
    }

    private static class TestSimpleVersionedSerializerV2
            implements SimpleVersionedSerializer<LakeSplit> {

        private static final int V2 = 2;

        @Override
        public byte[] serialize(LakeSplit split) throws IOException {
            return TEST_DATA;
        }

        @Override
        public LakeSplit deserialize(int version, byte[] serialized) throws IOException {
            if (version < V2) {
                return LAKE_SPLIT;
            }
            return new TestingLakeSplit(0, Collections.singletonList("2025-08-19"));
        }

        @Override
        public int getVersion() {
            return V2;
        }
    }
}
