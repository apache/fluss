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

package org.apache.fluss.flink.source.lookup;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.flink.row.FlinkAsFlussRow;
import org.apache.fluss.flink.utils.FlinkConversions;
import org.apache.fluss.row.encode.KeyEncoder;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FlussLookupInputPartitioner}, verifying that the lookup-join probe stream
 * is partitioned consistently with the client-side Fluss bucket assignment.
 */
class FlussLookupInputPartitionerTest {

    /** Independently computes the Fluss bucket id for the given key row. */
    private static int flussBucketOf(
            RowType keyRowType, List<String> bucketKeyNames, RowData key, int numBuckets) {
        org.apache.fluss.types.RowType flussKeyType = FlinkConversions.toFlussRowType(keyRowType);
        KeyEncoder encoder = KeyEncoder.ofBucketKeyEncoder(flussKeyType, bucketKeyNames, null);
        byte[] bytes = encoder.encodeKey(new FlinkAsFlussRow().replace(key));
        return BucketingFunction.of(null).bucketing(bytes, numBuckets);
    }

    private static FlussLookupInputPartitioner partitionerFor(
            RowType keyRowType, List<String> bucketKeyNames, int numBuckets) {
        // primary key == bucket key -> identity normalizer (no reordering)
        int[] pkIndexes = new int[bucketKeyNames.size()];
        for (int i = 0; i < pkIndexes.length; i++) {
            pkIndexes[i] = i;
        }
        LookupNormalizer normalizer =
                LookupNormalizer.createPrimaryKeyLookupNormalizer(pkIndexes, keyRowType);
        return new FlussLookupInputPartitioner(
                normalizer, keyRowType, bucketKeyNames, /* lakeFormat */ null, numBuckets);
    }

    @Test
    void testPartitionEqualsFlussBucketModNumPartitions() {
        RowType keyRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        List<String> bucketKeyNames = Collections.singletonList("id");
        int numBuckets = 8;
        FlussLookupInputPartitioner partitioner =
                partitionerFor(keyRowType, bucketKeyNames, numBuckets);

        for (int numPartitions : new int[] {1, 2, 3, 5, 8}) {
            for (int id = 0; id < 100; id++) {
                RowData key = GenericRowData.of(id);
                int actual = partitioner.partition(key, numPartitions);
                int expected =
                        Math.floorMod(
                                flussBucketOf(keyRowType, bucketKeyNames, key, numBuckets),
                                numPartitions);
                assertThat(actual)
                        .as("id=%d, numPartitions=%d", id, numPartitions)
                        .isEqualTo(expected)
                        .isBetween(0, numPartitions - 1);
            }
        }
    }

    @Test
    void testDeterministicAndStable() {
        RowType keyRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        FlussLookupInputPartitioner partitioner =
                partitionerFor(keyRowType, Collections.singletonList("id"), 8);

        assertThat(partitioner.isDeterministic()).isTrue();
        // same key -> same partition across repeated calls
        for (int id = 0; id < 50; id++) {
            int first = partitioner.partition(GenericRowData.of(id), 4);
            for (int i = 0; i < 5; i++) {
                assertThat(partitioner.partition(GenericRowData.of(id), 4)).isEqualTo(first);
            }
        }
    }

    @Test
    void testRowsInSameBucketGoToSamePartition() {
        RowType keyRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        List<String> bucketKeyNames = Collections.singletonList("id");
        int numBuckets = 4;
        int numPartitions = 2;
        FlussLookupInputPartitioner partitioner =
                partitionerFor(keyRowType, bucketKeyNames, numBuckets);

        // group ids by their fluss bucket, then assert every id in the same bucket maps to the
        // same partition (co-partitioning by bucket key).
        Map<Integer, Integer> bucketToPartition = new HashMap<>();
        for (int id = 0; id < 200; id++) {
            RowData key = GenericRowData.of(id);
            int bucket = flussBucketOf(keyRowType, bucketKeyNames, key, numBuckets);
            int partition = partitioner.partition(key, numPartitions);
            Integer previous = bucketToPartition.putIfAbsent(bucket, partition);
            if (previous != null) {
                assertThat(partition)
                        .as("all ids in bucket %d must share a partition", bucket)
                        .isEqualTo(previous);
            }
        }
    }

    @Test
    void testCompositeBucketKey() {
        RowType keyRowType =
                RowType.of(
                        new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)},
                        new String[] {"id", "region"});
        List<String> bucketKeyNames = Arrays.asList("id", "region");
        int numBuckets = 6;
        FlussLookupInputPartitioner partitioner =
                partitionerFor(keyRowType, bucketKeyNames, numBuckets);

        int numPartitions = 3;
        for (int id = 0; id < 50; id++) {
            RowData key = GenericRowData.of(id, StringData.fromString("region-" + (id % 4)));
            int actual = partitioner.partition(key, numPartitions);
            org.apache.fluss.types.RowType flussKeyType =
                    FlinkConversions.toFlussRowType(keyRowType);
            KeyEncoder encoder = KeyEncoder.ofBucketKeyEncoder(flussKeyType, bucketKeyNames, null);
            byte[] bytes = encoder.encodeKey(new FlinkAsFlussRow().replace(key));
            int expected =
                    Math.floorMod(
                            BucketingFunction.of(null).bucketing(bytes, numBuckets), numPartitions);
            assertThat(actual).as("id=%d", id).isEqualTo(expected).isBetween(0, numPartitions - 1);
        }
    }
}
