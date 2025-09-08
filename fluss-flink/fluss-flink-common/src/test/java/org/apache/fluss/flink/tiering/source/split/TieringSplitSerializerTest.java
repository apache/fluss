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

import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;

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

        byte[] serialized = serializer.serialize(tieringSplit);
        TieringSnapshotSplit deserializedSplit =
                (TieringSnapshotSplit) serializer.deserialize(serializer.getVersion(), serialized);
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
                        ? "TieringSnapshotSplit{tablePath=test_db.test_partitioned_table, tableBucket=TableBucket{tableId=1, partitionId=100, bucket=2}, partitionName='1024', snapshotId=0, logOffsetOfSnapshot=200, numberOfSplits=30}"
                        : "TieringSnapshotSplit{tablePath=test_db.test_table, tableBucket=TableBucket{tableId=1, bucket=2}, partitionName='null', snapshotId=0, logOffsetOfSnapshot=200, numberOfSplits=30}";
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

        byte[] serialized = serializer.serialize(tieringSplit);
        TieringLogSplit deserializedSplit =
                (TieringLogSplit) serializer.deserialize(serializer.getVersion(), serialized);
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
                        ? "TieringLogSplit{tablePath=test_db.test_partitioned_table, tableBucket=TableBucket{tableId=1, partitionId=100, bucket=2}, partitionName='1024', startingOffset=100, stoppingOffset=200, numberOfSplits=2}"
                        : "TieringLogSplit{tablePath=test_db.test_table, tableBucket=TableBucket{tableId=1, bucket=2}, partitionName='null', startingOffset=100, stoppingOffset=200, numberOfSplits=2}";
        assertThat(new TieringLogSplit(path, bucket, partitionName, 100, 200, 2).toString())
                .isEqualTo(expectedSplitString);
    }
}
