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

import org.apache.fluss.metadata.TableBucket;

import org.apache.flink.core.memory.DataOutputSerializer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link org.apache.fluss.flink.source.split.SourceSplitSerializer} of serializing
 * {@link org.apache.fluss.flink.source.split.SnapshotSplit} and {@link
 * org.apache.fluss.flink.source.split.LogSplit}.
 */
class SourceSplitSerializerTest {

    private static final SourceSplitSerializer serializer = new SourceSplitSerializer(null);
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
}
