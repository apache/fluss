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

import org.apache.fluss.client.lookup.LookupType;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link LookupNormalizer}. */
class LookupNormalizerTest {

    // Schema: [uid(int), name(varchar), dt(varchar), address(varchar)]
    // PrimaryKey: [uid, name, dt]
    // BucketKey: [uid]
    // PartitionKey: [dt]
    private static final RowType TEST_SCHEMA =
            RowType.of(
                    new LogicalType[] {
                        new IntType(),
                        new VarCharType(50),
                        new VarCharType(20),
                        new VarCharType(100)
                    },
                    new String[] {"uid", "name", "dt", "address"});

    @Test
    void testPrefixLookupWithBucketKeysOnly() {
        // Test case: lookup keys contain all bucket keys but no partition keys
        // This should work after our modification
        int[][] lookupKeyIndexes = {{0}}; // [uid]
        int[] primaryKeys = {0, 1, 2}; // [uid, name, dt]
        int[] bucketKeys = {0}; // [uid]
        int[] partitionKeys = {2}; // [dt]

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        primaryKeys,
                        bucketKeys,
                        partitionKeys,
                        TEST_SCHEMA,
                        null);

        assertThat(normalizer).isNotNull();
        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.PREFIX_LOOKUP);
        assertThat(normalizer.getLookupKeyIndexes()).containsExactly(0); // uid
    }

    @Test
    void testPrefixLookupWithBucketKeysAndPartialPartitionKeys() {
        // Test case: lookup keys contain bucket keys and some partition keys
        int[][] lookupKeyIndexes = {{0}, {2}}; // [uid, dt]
        int[] primaryKeys = {0, 1, 2}; // [uid, name, dt]
        int[] bucketKeys = {0}; // [uid]
        int[] partitionKeys = {2}; // [dt]

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        primaryKeys,
                        bucketKeys,
                        partitionKeys,
                        TEST_SCHEMA,
                        null);

        assertThat(normalizer).isNotNull();
        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.PREFIX_LOOKUP);
        assertThat(normalizer.getLookupKeyIndexes()).containsExactly(0, 2); // uid, dt
    }

    @Test
    void testPrefixLookupWithAllKeys() {
        // Test case: lookup keys contain all bucket keys and all partition keys
        // This was already supported before our modification
        int[][] lookupKeyIndexes = {{0}, {2}}; // [uid, dt]
        int[] primaryKeys = {0, 1, 2}; // [uid, name, dt]
        int[] bucketKeys = {0}; // [uid]
        int[] partitionKeys = {2}; // [dt]

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        primaryKeys,
                        bucketKeys,
                        partitionKeys,
                        TEST_SCHEMA,
                        null);

        assertThat(normalizer).isNotNull();
        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.PREFIX_LOOKUP);
    }

    @Test
    void testPrimaryKeyLookup() {
        // Test case: lookup keys contain all primary keys
        int[][] lookupKeyIndexes = {{0}, {1}, {2}}; // [uid, name, dt]
        int[] primaryKeys = {0, 1, 2}; // [uid, name, dt]
        int[] bucketKeys = {0}; // [uid]
        int[] partitionKeys = {2}; // [dt]

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        primaryKeys,
                        bucketKeys,
                        partitionKeys,
                        TEST_SCHEMA,
                        null);

        assertThat(normalizer).isNotNull();
        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.LOOKUP);
    }

    @Test
    void testFailureWhenMissingBucketKeys() {
        // Test case: lookup keys don't contain all bucket keys - should fail
        int[][] lookupKeyIndexes = {{1}}; // [name] - missing bucket key uid
        int[] primaryKeys = {0, 1, 2}; // [uid, name, dt]
        int[] bucketKeys = {0}; // [uid]
        int[] partitionKeys = {2}; // [dt]

        assertThatThrownBy(
                        () ->
                                LookupNormalizer.validateAndCreateLookupNormalizer(
                                        lookupKeyIndexes,
                                        primaryKeys,
                                        bucketKeys,
                                        partitionKeys,
                                        TEST_SCHEMA,
                                        null))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("lookup keys include all bucket keys")
                .hasMessageContaining("Required bucket keys are [uid]")
                .hasMessageContaining("but the lookup keys are [name]");
    }

    @Test
    void testFailureWhenMissingAllPrimaryKeysForNonPrefixSupport() {
        // Test case: lookup keys don't support prefix lookup and don't contain all primary keys
        // Schema: [id(int), name(varchar)] - bucket key is not a prefix of encoded primary key
        RowType nonPrefixSchema =
                RowType.of(
                        new LogicalType[] {new IntType(), new VarCharType(50)},
                        new String[] {"id", "name"});

        int[][] lookupKeyIndexes = {{1}}; // [name]
        int[] primaryKeys = {0, 1}; // [id, name]
        int[] bucketKeys = {1}; // [name] - not a prefix of [id, name] (encoded primary key)
        int[] partitionKeys = {}; // no partition keys

        assertThatThrownBy(
                        () ->
                                LookupNormalizer.validateAndCreateLookupNormalizer(
                                        lookupKeyIndexes,
                                        primaryKeys,
                                        bucketKeys,
                                        partitionKeys,
                                        nonPrefixSchema,
                                        null))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("lookup keys include all primary keys");
    }

    @Test
    void testMultipleBucketKeys() {
        // Test case: multiple bucket keys, lookup contains all bucket keys but no partition keys
        // Schema: [uid(int), name(varchar), dt(varchar), address(varchar)]
        // BucketKeys: [uid, name]
        int[][] lookupKeyIndexes = {{0}, {1}}; // [uid, name]
        int[] primaryKeys = {0, 1, 2}; // [uid, name, dt]
        int[] bucketKeys = {0, 1}; // [uid, name]
        int[] partitionKeys = {2}; // [dt]

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        primaryKeys,
                        bucketKeys,
                        partitionKeys,
                        TEST_SCHEMA,
                        null);

        assertThat(normalizer).isNotNull();
        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.PREFIX_LOOKUP);
        assertThat(normalizer.getLookupKeyIndexes()).containsExactly(0, 1); // uid, name
    }

    @Test
    void testMultipleBucketKeysPartialMissing() {
        // Test case: multiple bucket keys, lookup doesn't contain all bucket keys - should fail
        int[][] lookupKeyIndexes = {{0}}; // [uid] - missing name
        int[] primaryKeys = {0, 1, 2}; // [uid, name, dt]
        int[] bucketKeys = {0, 1}; // [uid, name]
        int[] partitionKeys = {2}; // [dt]

        assertThatThrownBy(
                        () ->
                                LookupNormalizer.validateAndCreateLookupNormalizer(
                                        lookupKeyIndexes,
                                        primaryKeys,
                                        bucketKeys,
                                        partitionKeys,
                                        TEST_SCHEMA,
                                        null))
                .isInstanceOf(TableException.class)
                .hasMessageContaining("Required bucket keys are [uid, name]")
                .hasMessageContaining("but the lookup keys are [uid]");
    }

    @Test
    void testNonPartitionedTable() {
        // Test case: non-partitioned table with prefix lookup
        int[][] lookupKeyIndexes = {{0}}; // [uid]
        int[] primaryKeys = {0, 1}; // [uid, name]
        int[] bucketKeys = {0}; // [uid]
        int[] partitionKeys = {}; // no partition keys

        RowType nonPartitionedSchema =
                RowType.of(
                        new LogicalType[] {
                            new IntType(), new VarCharType(50), new VarCharType(100)
                        },
                        new String[] {"uid", "name", "address"});

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        primaryKeys,
                        bucketKeys,
                        partitionKeys,
                        nonPartitionedSchema,
                        null);

        assertThat(normalizer).isNotNull();
        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.PREFIX_LOOKUP);
        assertThat(normalizer.getLookupKeyIndexes()).containsExactly(0); // uid
    }

    @Test
    void testWithProjection() {
        // Test case: lookup with projection
        int[][] lookupKeyIndexes = {{0}}; // [0] after projection
        int[] primaryKeys = {0, 1, 2}; // [uid, name, dt] before projection
        int[] bucketKeys = {0}; // [uid] before projection
        int[] partitionKeys = {2}; // [dt] before projection
        int[] projectedFields = {0, 3}; // project uid, address

        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeyIndexes,
                        primaryKeys,
                        bucketKeys,
                        partitionKeys,
                        TEST_SCHEMA,
                        projectedFields);

        assertThat(normalizer).isNotNull();
        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.PREFIX_LOOKUP);
    }
}
