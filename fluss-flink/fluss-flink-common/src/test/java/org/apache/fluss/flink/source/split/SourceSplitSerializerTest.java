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

package org.apache.fluss.flink.source.split;

import org.apache.fluss.flink.lake.split.LakeSnapshotSplit;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.lake.source.TestingLakeSplit;
import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link org.apache.fluss.flink.source.split.SourceSplitSerializer} of serializing
 * {@link org.apache.fluss.flink.source.split.SnapshotSplit}, {@link
 * org.apache.fluss.flink.source.split.LogSplit} and {@link
 * org.apache.fluss.flink.source.split.KvBatchSplit}.
 */
class SourceSplitSerializerTest {

    private static final SourceSplitSerializer serializer = new SourceSplitSerializer(null);
    private static final TestingLakeSource lakeSource = new TestingLakeSource();
    private static final SourceSplitSerializer lakeSerializer =
            new SourceSplitSerializer(lakeSource);
    private static final TableBucket tableBucket = new TableBucket(1, 2);
    private static final TableBucket partitionedTableBucket = new TableBucket(1, 100L, 2);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHybridSnapshotLogSplitSerde(boolean isPartitioned) throws Exception {
        int snapshotId = 100;
        long logStartingOffset = 3L;
        long recordsToSkip = 4L;
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;

        HybridSnapshotLogSplit split =
                new HybridSnapshotLogSplit(
                        bucket,
                        partitionName,
                        snapshotId,
                        0,
                        false,
                        logStartingOffset,
                        LogSplit.NO_STOPPING_OFFSET,
                        false);
        byte[] serialized = serializer.serialize(split);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(split);

        split =
                new HybridSnapshotLogSplit(
                        bucket,
                        partitionName,
                        snapshotId,
                        recordsToSkip,
                        true,
                        5,
                        LogSplit.NO_STOPPING_OFFSET,
                        false);
        serialized = serializer.serialize(split);
        deserializedSplit = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(split);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLogSplitSerde(boolean isPartitioned) throws Exception {
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;
        LogSplit logSplit = new LogSplit(bucket, partitionName, 100);

        byte[] serialized = serializer.serialize(logSplit);
        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(logSplit);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHybridSnapshotLogSplitSerdeWithStoppingOffset(boolean isPartitioned) throws Exception {
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;
        HybridSnapshotLogSplit split =
                new HybridSnapshotLogSplit(bucket, partitionName, 100L, 0L, false, 20L, 30L, true);

        byte[] serialized = serializer.serialize(split);
        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserializedSplit).isEqualTo(split);
        assertThat(deserializedSplit.asHybridSnapshotLogSplit().isBatch()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeserializeVersionZeroHybridSnapshotLogSplit(boolean isPartitioned) throws Exception {
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeByte(SourceSplitBase.HYBRID_SNAPSHOT_SPLIT_FLAG);
        out.writeLong(bucket.getTableId());
        out.writeBoolean(bucket.getPartitionId() != null);
        if (bucket.getPartitionId() != null) {
            out.writeLong(bucket.getPartitionId());
            out.writeUTF(partitionName);
        }
        out.writeInt(bucket.getBucket());
        out.writeLong(100L);
        out.writeLong(3L);
        out.writeBoolean(false);
        out.writeLong(20L);

        SourceSplitBase deserializedSplit = serializer.deserialize(0, out.getCopyOfBuffer());

        assertThat(deserializedSplit)
                .isEqualTo(
                        new HybridSnapshotLogSplit(
                                bucket,
                                partitionName,
                                100L,
                                3L,
                                false,
                                20L,
                                LogSplit.NO_STOPPING_OFFSET,
                                false));
        assertThat(deserializedSplit.asHybridSnapshotLogSplit().isBatch()).isFalse();
        assertThat(deserializedSplit.asHybridSnapshotLogSplit().getLogStoppingOffset()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testDeserializeVersionOneHybridSnapshotLogSplit(boolean isPartitioned) throws Exception {
        // the layout of HybridSnapshotLogSplit is unchanged since VERSION_1
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;
        HybridSnapshotLogSplit split =
                new HybridSnapshotLogSplit(bucket, partitionName, 100L, 3L, true, 20L, 30L, true);

        SourceSplitBase deserializedSplit = serializer.deserialize(1, serializer.serialize(split));

        assertThat(deserializedSplit).isEqualTo(split);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testLakeSnapshotSplitSerde(boolean isPartitioned) throws Exception {
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;
        TestingLakeSplit lakeSplit =
                new TestingLakeSplit(
                        bucket.getBucket(),
                        isPartitioned ? Collections.singletonList(partitionName) : null);
        LakeSnapshotSplit split = new LakeSnapshotSplit(bucket, partitionName, lakeSplit, 5, 7L);

        LakeSnapshotSplit deserializedSplit =
                (LakeSnapshotSplit)
                        lakeSerializer.deserialize(
                                lakeSerializer.getVersion(), lakeSerializer.serialize(split));

        assertLakeSnapshotSplit(deserializedSplit, split, 7L);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testDeserializeLakeSnapshotSplitBeforeVersionTwo(int version) throws Exception {
        TestingLakeSplit lakeSplit = new TestingLakeSplit(2, null);
        LakeSnapshotSplit split = new LakeSnapshotSplit(tableBucket, null, lakeSplit, 5, 7L);
        byte[] lakeSplitBytes = lakeSource.getSplitSerializer().serialize(lakeSplit);

        // the layout of LakeSnapshotSplit before VERSION_2, which has no records to skip
        DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeByte(LakeSnapshotSplit.LAKE_SNAPSHOT_SPLIT_KIND);
        out.writeLong(tableBucket.getTableId());
        out.writeBoolean(false);
        out.writeInt(tableBucket.getBucket());
        out.writeInt(lakeSource.getSplitSerializer().getVersion());
        out.writeInt(5);
        out.writeInt(lakeSplitBytes.length);
        out.write(lakeSplitBytes);

        LakeSnapshotSplit deserializedSplit =
                (LakeSnapshotSplit) lakeSerializer.deserialize(version, out.getCopyOfBuffer());

        // restored from the beginning of the lake split
        assertLakeSnapshotSplit(deserializedSplit, split, 0L);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 3})
    void testDeserializeUnknownVersion(int version) {
        assertThatThrownBy(
                        () ->
                                serializer.deserialize(
                                        version,
                                        serializer.serialize(new LogSplit(tableBucket, null, 0))))
                .isInstanceOf(IOException.class)
                .hasMessage("Unknown version " + version);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testKvBatchSplitSerde(boolean isPartitioned) throws Exception {
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;
        KvBatchSplit split = new KvBatchSplit(bucket, partitionName);

        byte[] serialized = serializer.serialize(split);
        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(split);
        assertThat(deserializedSplit.isKvBatchSplit()).isTrue();
        assertThat(deserializedSplit.asKvBatchSplit().splitId()).isEqualTo(split.splitId());
    }

    private static void assertLakeSnapshotSplit(
            LakeSnapshotSplit actual, LakeSnapshotSplit expected, long expectedRecordsToSkip) {
        assertThat(actual.getTableBucket()).isEqualTo(expected.getTableBucket());
        assertThat(actual.getPartitionName()).isEqualTo(expected.getPartitionName());
        assertThat(actual.getSplitIndex()).isEqualTo(expected.getSplitIndex());
        assertThat(actual.getRecordsToSkip()).isEqualTo(expectedRecordsToSkip);
        assertThat(actual.getLakeSplit().bucket()).isEqualTo(expected.getLakeSplit().bucket());
        assertThat(actual.getLakeSplit().partition())
                .isEqualTo(expected.getLakeSplit().partition());
        assertThat(actual.splitId()).isEqualTo(expected.splitId());
    }
}
