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
    void testLogSplitSerde(boolean isPartitioned) throws Exception {
        TableBucket bucket = isPartitioned ? partitionedTableBucket : tableBucket;
        String partitionName = isPartitioned ? "2024" : null;
        LogSplit logSplit = new LogSplit(bucket, partitionName, 100);

        byte[] serialized = serializer.serialize(logSplit);
        SourceSplitBase deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(logSplit);
    }
}
