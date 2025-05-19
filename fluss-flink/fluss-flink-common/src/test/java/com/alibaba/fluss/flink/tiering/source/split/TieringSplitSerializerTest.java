/*
 *  Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.alibaba.fluss.flink.tiering.source.split;

import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TablePath;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for serialization and deserialization of {@link TieringSplit}. */
class TieringSplitSerializerTest {

    private static final TieringSplitSerializer serializer = TieringSplitSerializer.INSTANCE;
    private static final TableBucket tableBucket = new TableBucket(1, 2);
    private static final TablePath tablePath = TablePath.of("test_db", "test_table");
    private static final TableBucket partitionedTableBucket = new TableBucket(1, 100L, 2);
    private static final TablePath partitionedTablePath =
            TablePath.of("test_db", "test_partitioned_table");

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTieringSplitSerde(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        TieringSplit tieringSplit = new TieringSplit(path, bucket, partitionName, 100, 200);

        byte[] serialized = serializer.serialize(tieringSplit);
        TieringSplit deserializedSplit =
                serializer.deserialize(serializer.getVersion(), serialized);
        assertThat(deserializedSplit).isEqualTo(tieringSplit);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSplitStringExpression(Boolean isPartitionedTable) throws Exception {
        TableBucket bucket = isPartitionedTable ? partitionedTableBucket : tableBucket;
        TablePath path = isPartitionedTable ? partitionedTablePath : tablePath;
        String partitionName = isPartitionedTable ? "1024" : null;
        String expectedSplitId =
                isPartitionedTable ? "tiering-split-1-p100-2" : "tiering-split-1-2";
        assertThat(new TieringSplit(path, bucket, partitionName, 100, 200).splitId())
                .isEqualTo(expectedSplitId);

        String expectedSplitString =
                isPartitionedTable
                        ? "TieringSplit{tablePath=test_db.test_partitioned_table, tableBucket=TableBucket{tableId=1, partitionId=100, bucket=2}, partitionName='1024', startingOffset=100, stoppingOffset=200}"
                        : "TieringSplit{tablePath=test_db.test_table, tableBucket=TableBucket{tableId=1, bucket=2}, partitionName='null', startingOffset=100, stoppingOffset=200}";
        assertThat(new TieringSplit(path, bucket, partitionName, 100, 200).toString())
                .isEqualTo(expectedSplitString);
    }
}
