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

package org.apache.fluss.flink.tiering.source.split;

import org.apache.fluss.client.tiering.TieringLogSplit;
import org.apache.fluss.client.tiering.TieringSnapshotSplit;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for serialization and deserialization of {@link TieringSnapshotSplit} and {@link
 * TieringLogSplit}.
 */
class TieringSplitSerializerTest {

    private static final TieringSplitSerializer serializer = TieringSplitSerializer.INSTANCE;
    private static final TableBucket tableBucket = new TableBucket(1, 2);
    private static final TablePath tablePath = TablePath.of("test_db", "test_table");
    private static final TableBucket partitionedTableBucket = new TableBucket(1, 100L, 2);
    private static final TablePath partitionedTablePath =
            TablePath.of("test_db", "test_partitioned_table");

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringSnapshotSplitSerde(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        TieringSnapshotSplit tieringSplit =
                new TieringSnapshotSplit(path, bucket, partitionName, 0L, 200L, 10);

        byte[] serialized = serializer.serialize(new FlinkTieringSplit(tieringSplit));
        FlinkTieringSplit deserializedFlinkSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        TieringSnapshotSplit deserializedSplit = deserializedFlinkSplit.asTieringSnapshotSplit();
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringSnapshotSplitStringExpression(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        String expectedSplitId =
                isPartitionedTable
                        ? "tiering-snapshot-split-1-p100-2"
                        : "tiering-snapshot-split-1-2";
        assertThat(new TieringSnapshotSplit(path, bucket, partitionName, 0L, 200L, 20).splitId())
                .isEqualTo(expectedSplitId);

        String expectedSplitString =
                isPartitionedTable
                        ? "TieringSnapshotSplit{tablePath=test_db.test_partitioned_table, tableBucket=TableBucket{tableId=1, partitionId=100, bucket=2}, partitionName='1024', numberOfSplits=30, skipCurrentRound=false, snapshotId=0, logOffsetOfSnapshot=200, splitIndex=-1, tieringRoundTimestamp=-1}"
                        : "TieringSnapshotSplit{tablePath=test_db.test_table, tableBucket=TableBucket{tableId=1, bucket=2}, partitionName='null', numberOfSplits=30, skipCurrentRound=false, snapshotId=0, logOffsetOfSnapshot=200, splitIndex=-1, tieringRoundTimestamp=-1}";
        assertThat(new TieringSnapshotSplit(path, bucket, partitionName, 0L, 200L, 30).toString())
                .isEqualTo(expectedSplitString);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringLogSplitSerde(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        TieringLogSplit tieringSplit =
                new TieringLogSplit(path, bucket, partitionName, 100, 200, 40);

        byte[] serialized = serializer.serialize(new FlinkTieringSplit(tieringSplit));
        FlinkTieringSplit deserializedFlinkSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        TieringLogSplit deserializedSplit = deserializedFlinkSplit.asTieringLogSplit();
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringLogSplitStringExpression(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        String expectedSplitId =
                isPartitionedTable ? "tiering-log-split-1-p100-2" : "tiering-log-split-1-2";
        assertThat(new TieringLogSplit(path, bucket, partitionName, 100, 200, 3).splitId())
                .isEqualTo(expectedSplitId);

        String expectedSplitString =
                isPartitionedTable
                        ? "TieringLogSplit{tablePath=test_db.test_partitioned_table, tableBucket=TableBucket{tableId=1, partitionId=100, bucket=2}, partitionName='1024', numberOfSplits=2, skipCurrentRound=false, startingOffset=100, stoppingOffset=200, splitIndex=-1, tieringRoundTimestamp=-1}"
                        : "TieringLogSplit{tablePath=test_db.test_table, tableBucket=TableBucket{tableId=1, bucket=2}, partitionName='null', numberOfSplits=2, skipCurrentRound=false, startingOffset=100, stoppingOffset=200, splitIndex=-1, tieringRoundTimestamp=-1}";
        assertThat(new TieringLogSplit(path, bucket, partitionName, 100, 200, 2).toString())
                .isEqualTo(expectedSplitString);
    }

    @Test
    void testSkipCurrentRoundSerde() throws Exception {
        // Test TieringSnapshotSplit with skipCurrentRound set at creation
        TieringSnapshotSplit snapshotSplitWithSkipCurrentRound =
                new TieringSnapshotSplit(tablePath, tableBucket, null, 0L, 200L, 10, true);
        byte[] serialized =
                serializer.serialize(new FlinkTieringSplit(snapshotSplitWithSkipCurrentRound));
        TieringSnapshotSplit deserializedSnapshotSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .asTieringSnapshotSplit();
        assertThat(deserializedSnapshotSplit).isEqualTo(snapshotSplitWithSkipCurrentRound);

        // Test TieringLogSplit with skipCurrentRound set at creation
        TieringLogSplit logSplitWithSkipCurrentRound =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40, true);
        serialized = serializer.serialize(new FlinkTieringSplit(logSplitWithSkipCurrentRound));
        TieringLogSplit deserializedLogSplit =
                serializer.deserialize(serializer.getVersion(), serialized).asTieringLogSplit();
        assertThat(deserializedLogSplit).isEqualTo(logSplitWithSkipCurrentRound);

        // Test TieringSnapshotSplit with skipCurrentRound set after creation
        TieringSnapshotSplit snapshotSplit =
                new TieringSnapshotSplit(tablePath, tableBucket, null, 0L, 200L, 10, false);
        assertThat(snapshotSplit.shouldSkipCurrentRound()).isFalse();
        snapshotSplit.skipCurrentRound();
        assertThat(snapshotSplit.shouldSkipCurrentRound()).isTrue();

        serialized = serializer.serialize(new FlinkTieringSplit(snapshotSplit));
        deserializedSnapshotSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .asTieringSnapshotSplit();
        assertThat(deserializedSnapshotSplit).isEqualTo(snapshotSplit);

        // Test TieringLogSplit with skipCurrentRound set after creation
        TieringLogSplit logSplit =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40, false);
        assertThat(logSplit.shouldSkipCurrentRound()).isFalse();
        logSplit.skipCurrentRound();
        assertThat(logSplit.shouldSkipCurrentRound()).isTrue();

        serialized = serializer.serialize(new FlinkTieringSplit(logSplit));
        deserializedLogSplit =
                serializer.deserialize(serializer.getVersion(), serialized).asTieringLogSplit();
        assertThat(deserializedLogSplit).isEqualTo(logSplit);
    }

    @Test
    void testTieringRoundTimestampSerde() throws Exception {
        TieringSnapshotSplit snapshotSplit =
                new TieringSnapshotSplit(tablePath, tableBucket, null, 0L, 200L, 10, 0, 1000L);
        byte[] serialized = serializer.serialize(new FlinkTieringSplit(snapshotSplit));
        TieringSnapshotSplit deserializedSnapshotSplit =
                serializer
                        .deserialize(serializer.getVersion(), serialized)
                        .asTieringSnapshotSplit();
        assertThat(deserializedSnapshotSplit.getSplitIndex()).isZero();
        assertThat(deserializedSnapshotSplit.isFirstSplit()).isTrue();
        assertThat(deserializedSnapshotSplit.getTieringRoundTimestamp()).isEqualTo(1000L);

        TieringLogSplit logSplit =
                new TieringLogSplit(tablePath, tableBucket, null, 100, 200, 40, 2, 2000L);
        serialized = serializer.serialize(new FlinkTieringSplit(logSplit));
        TieringLogSplit deserializedLogSplit =
                serializer.deserialize(serializer.getVersion(), serialized).asTieringLogSplit();
        assertThat(deserializedLogSplit.getSplitIndex()).isEqualTo(2);
        assertThat(deserializedLogSplit.isFirstSplit()).isFalse();
        assertThat(deserializedLogSplit.getTieringRoundTimestamp()).isEqualTo(2000L);
    }
}
