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
import org.apache.fluss.flink.utils.FlinkUtils;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.row.encode.KeyEncoder;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link FlussLookupInputPartitioner}, verifying that the lookup-join probe stream
 * is partitioned consistently with the client-side Fluss bucket assignment.
 */
class FlussLookupInputPartitionerTest {

    private static final int GOLDEN_NUM_BUCKETS = 16;

    /**
     * Frozen golden vectors: for a single {@code INT} bucket key and {@code numBuckets = 16}, these
     * are the exact bucket ids produced by the client-compatible routing for each lake format that
     * has its own bucketing implementation ({@code null}/default Fluss, {@code PAIMON}, {@code
     * ICEBERG} and {@code HUDI}). {@code LANCE} shares the default Fluss implementation, so it is
     * not covered separately. Baking these values in as literals makes them a regression guard that
     * is independent of re-deriving the value with the same encoder/bucketing code (addressing the
     * "same-implementation oracle" concern).
     *
     * <p>This test pins only the exact bucket id ({@code numPartitions == numBuckets}); the {@code
     * bucket % numPartitions} channel selection is already covered by {@link
     * #testPartitionEqualsFlussBucketModNumPartitions()}.
     */
    @Test
    void testGoldenBucketVectors() {
        RowType keyRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        List<String> bucketKeyNames = Collections.singletonList("id");

        Map<Integer, Integer> flussGolden = new LinkedHashMap<>();
        flussGolden.put(1, 12);
        flussGolden.put(2, 10);
        flussGolden.put(42, 10);
        flussGolden.put(100, 8);
        flussGolden.put(12345, 5);
        assertGoldenRouting(keyRowType, bucketKeyNames, null, flussGolden);

        Map<Integer, Integer> paimonGolden = new LinkedHashMap<>();
        paimonGolden.put(1, 14);
        paimonGolden.put(2, 0);
        paimonGolden.put(42, 5);
        paimonGolden.put(100, 5);
        paimonGolden.put(12345, 14);
        assertGoldenRouting(keyRowType, bucketKeyNames, DataLakeFormat.PAIMON, paimonGolden);

        Map<Integer, Integer> icebergGolden = new LinkedHashMap<>();
        icebergGolden.put(1, 4);
        icebergGolden.put(2, 4);
        icebergGolden.put(42, 14);
        icebergGolden.put(100, 0);
        icebergGolden.put(12345, 1);
        assertGoldenRouting(keyRowType, bucketKeyNames, DataLakeFormat.ICEBERG, icebergGolden);

        Map<Integer, Integer> hudiGolden = new LinkedHashMap<>();
        hudiGolden.put(1, 0);
        hudiGolden.put(2, 1);
        hudiGolden.put(42, 13);
        hudiGolden.put(100, 0);
        hudiGolden.put(12345, 2);
        assertGoldenRouting(keyRowType, bucketKeyNames, DataLakeFormat.HUDI, hudiGolden);
    }

    private static void assertGoldenRouting(
            RowType keyRowType,
            List<String> bucketKeyNames,
            DataLakeFormat lakeFormat,
            Map<Integer, Integer> goldenBucketById) {
        LookupNormalizer normalizer =
                LookupNormalizer.createPrimaryKeyLookupNormalizer(new int[] {0}, keyRowType);
        FlussLookupInputPartitioner partitioner =
                new FlussLookupInputPartitioner(
                        normalizer, keyRowType, bucketKeyNames, lakeFormat, GOLDEN_NUM_BUCKETS);
        for (Map.Entry<Integer, Integer> entry : goldenBucketById.entrySet()) {
            int id = entry.getKey();
            int goldenBucket = entry.getValue();
            // numPartitions == numBuckets pins the exact bucket id.
            assertThat(partitioner.partition(GenericRowData.of(id), GOLDEN_NUM_BUCKETS))
                    .as("id=%d fmt=%s", id, lakeFormat)
                    .isEqualTo(goldenBucket);
        }
    }

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

    /**
     * A prefix lookup (bucket key is a strict prefix of the primary key) must be shuffled too: the
     * normalized key row is the bucket keys (+ partition keys), and routing must match the Fluss
     * bucket assignment for that prefix.
     */
    @Test
    void testPrefixLookupPartitionMatchesBucket() {
        // schema: region (bucket key), id, name; primary key (region, id), bucket key (region)
        RowType tableSchema =
                RowType.of(
                        new LogicalType[] {
                            new VarCharType(VarCharType.MAX_LENGTH),
                            new IntType(),
                            new VarCharType(VarCharType.MAX_LENGTH)
                        },
                        new String[] {"region", "id", "name"});
        int[] pkIndexes = {0, 1};
        int[] bucketKeyIndexes = {0};
        // prefix lookup: only the bucket key (region) is used as the lookup key
        int[][] lookupKeyIndexes = {{0}};
        int numBuckets = 5;

        FlussLookupInputPartitioner partitioner =
                buildPartitioner(
                        tableSchema, lookupKeyIndexes, pkIndexes, bucketKeyIndexes, numBuckets);

        RowType keyRowType =
                RowType.of(
                        new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH)},
                        new String[] {"region"});
        List<String> bucketKeyNames = Collections.singletonList("region");
        int numPartitions = 3;
        for (int i = 0; i < 40; i++) {
            RowData joinKeys = GenericRowData.of(StringData.fromString("region-" + i));
            int actual = partitioner.partition(joinKeys, numPartitions);
            int expected =
                    Math.floorMod(
                            flussBucketOf(keyRowType, bucketKeyNames, joinKeys, numBuckets),
                            numPartitions);
            assertThat(actual)
                    .as("region-%d", i)
                    .isEqualTo(expected)
                    .isBetween(0, numPartitions - 1);
        }
    }

    /**
     * Flink 2.2 delivers lookup keys in ascending field-index order, so a key reorder is driven by
     * the primary key being <em>declared</em> in a different order than the table fields (here PK
     * {@code (region, id)} over fields {@code (id, region)}). The normalizer must reorder the probe
     * key into Fluss primary-key order before bucketing, and routing must be computed on that
     * reordered (normalized) key rather than on the raw ascending join-key order.
     */
    @Test
    void testKeyReorderMatchesNormalizedBucket() {
        // schema field order: id(0), region(1), name(2)
        RowType tableSchema =
                RowType.of(
                        new LogicalType[] {
                            new IntType(),
                            new VarCharType(VarCharType.MAX_LENGTH),
                            new VarCharType(VarCharType.MAX_LENGTH)
                        },
                        new String[] {"id", "region", "name"});
        // primary key & bucket key declared as (region, id) -> differs from schema field order
        int[] pkIndexes = {1, 0};
        int[] bucketKeyIndexes = {1, 0};
        // Flink sorts lookup keys ascending by field index, so the probe side delivers (id, region)
        int[][] lookupKeyIndexes = {{0}, {1}};
        int numBuckets = 7;

        FlussLookupInputPartitioner partitioner =
                buildPartitioner(
                        tableSchema, lookupKeyIndexes, pkIndexes, bucketKeyIndexes, numBuckets);

        // the normalized key row type is (region, id) in Fluss primary-key order
        RowType normalizedKeyRowType =
                RowType.of(
                        new LogicalType[] {new VarCharType(VarCharType.MAX_LENGTH), new IntType()},
                        new String[] {"region", "id"});
        List<String> bucketKeyNames = Arrays.asList("region", "id");
        int numPartitions = 4;
        for (int id = 0; id < 40; id++) {
            StringData region = StringData.fromString("region-" + (id % 5));
            // probe delivers the join key row in ascending field order (id, region)
            RowData joinKeys = GenericRowData.of(id, region);
            int actual = partitioner.partition(joinKeys, numPartitions);
            // expected: bucket computed on the normalized (region, id) row
            RowData normalizedKey = GenericRowData.of(region, id);
            int expected =
                    Math.floorMod(
                            flussBucketOf(
                                    normalizedKeyRowType,
                                    bucketKeyNames,
                                    normalizedKey,
                                    numBuckets),
                            numPartitions);
            assertThat(actual).as("id=%d", id).isEqualTo(expected).isBetween(0, numPartitions - 1);
        }
    }

    /** Builds a partitioner exactly the way {@code FlinkLookupShuffleTableSource} does. */
    private static FlussLookupInputPartitioner buildPartitioner(
            RowType tableSchema,
            int[][] lookupKeyIndexes,
            int[] pkIndexes,
            int[] bucketKeyIndexes,
            int numBuckets) {
        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        pkIndexes,
                        bucketKeyIndexes,
                        new int[0],
                        tableSchema,
                        null);
        RowType keyRowType =
                FlinkUtils.projectRowType(tableSchema, normalizer.getLookupKeyIndexes());
        List<String> allNames = tableSchema.getFieldNames();
        List<String> bucketKeyNames = new ArrayList<>(bucketKeyIndexes.length);
        for (int idx : bucketKeyIndexes) {
            bucketKeyNames.add(allNames.get(idx));
        }
        return new FlussLookupInputPartitioner(
                normalizer, keyRowType, bucketKeyNames, /* lakeFormat */ null, numBuckets);
    }
}
