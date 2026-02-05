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
        int recordsToSkip = 3;
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;

        HybridSnapshotLogSplit split =
                new HybridSnapshotLogSplit(bucket, partitionName, snapshotId, recordsToSkip);
        byte[] serialized = serializer.serialize(split);

        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(split);

        split =
                new HybridSnapshotLogSplit(
                        bucket, partitionName, snapshotId, recordsToSkip, true, 5);
        serialized = serializer.serialize(split);
        deserializedSplit = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(split);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testHybridSnapshotLogSplitSerdeWithBacklogOffset(boolean isPartitioned) throws Exception {
        int snapshotId = 100;
        long logStartingOffset = 50;
        long backlogMarkedStoppingOffset = 200;
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2026" : null;

        HybridSnapshotLogSplit splitWithBacklog =
                new HybridSnapshotLogSplit(
                        bucket,
                        partitionName,
                        snapshotId,
                        logStartingOffset,
                        backlogMarkedStoppingOffset);
        byte[] serialized = serializer.serialize(splitWithBacklog);
        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(splitWithBacklog);
        assertThat(((HybridSnapshotLogSplit) deserializedSplit).getBacklogMarkedStoppingOffset())
                .isPresent()
                .hasValue(backlogMarkedStoppingOffset);

        HybridSnapshotLogSplit splitFull =
                new HybridSnapshotLogSplit(
                        bucket,
                        partitionName,
                        snapshotId,
                        5L,
                        true,
                        logStartingOffset,
                        backlogMarkedStoppingOffset);
        serialized = serializer.serialize(splitFull);
        deserializedSplit = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(splitFull);
        assertThat(((HybridSnapshotLogSplit) deserializedSplit).getBacklogMarkedStoppingOffset())
                .isPresent()
                .hasValue(backlogMarkedStoppingOffset);

        HybridSnapshotLogSplit splitNoBacklog =
                new HybridSnapshotLogSplit(
                        bucket,
                        partitionName,
                        snapshotId,
                        5L,
                        true,
                        logStartingOffset,
                        LogSplit.NO_STOPPING_OFFSET);
        serialized = serializer.serialize(splitNoBacklog);
        deserializedSplit = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(splitNoBacklog);
        assertThat(((HybridSnapshotLogSplit) deserializedSplit).getBacklogMarkedStoppingOffset())
                .isEmpty();
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
    void testLogSplitSerdeWithBacklogOffset(boolean isPartitioned) throws Exception {
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;

        long startingOffset = 100;
        long stoppingOffset = 500;
        long backlogMarkedOffset = 300;
        LogSplit logSplitWithBacklog =
                new LogSplit(
                        bucket, partitionName, startingOffset, stoppingOffset, backlogMarkedOffset);

        byte[] serialized = serializer.serialize(logSplitWithBacklog);
        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(logSplitWithBacklog);
        assertThat(((LogSplit) deserializedSplit).getBacklogMarkedOffset())
                .isPresent()
                .hasValue(backlogMarkedOffset);

        LogSplit logSplitWithStoppingOnly =
                new LogSplit(bucket, partitionName, startingOffset, stoppingOffset);
        serialized = serializer.serialize(logSplitWithStoppingOnly);
        deserializedSplit = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(logSplitWithStoppingOnly);
        assertThat(((LogSplit) deserializedSplit).getBacklogMarkedOffset()).isEmpty();
        assertThat(((LogSplit) deserializedSplit).getStoppingOffset())
                .isPresent()
                .hasValue(stoppingOffset);

        LogSplit logSplitWithBacklogOnly =
                new LogSplit(
                        bucket,
                        partitionName,
                        startingOffset,
                        LogSplit.NO_STOPPING_OFFSET,
                        backlogMarkedOffset);
        serialized = serializer.serialize(logSplitWithBacklogOnly);
        deserializedSplit = serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(logSplitWithBacklogOnly);
        assertThat(((LogSplit) deserializedSplit).getBacklogMarkedOffset())
                .isPresent()
                .hasValue(backlogMarkedOffset);
        assertThat(((LogSplit) deserializedSplit).getStoppingOffset()).isEmpty();
    }
}
