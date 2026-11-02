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
import org.apache.fluss.flink.source.lookup.LookupNormalizer.RemainingFilter;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.apache.fluss.client.lookup.LookupType.LOOKUP;
import static org.apache.fluss.client.lookup.LookupType.PREFIX_LOOKUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for lookup access-path selection and key normalization. */
class LookupNormalizerTest {

    private static final RowType SCHEMA =
            RowType.of(
                    new LogicalType[] {new IntType(), new IntType(), new IntType(), new IntType()},
                    new String[] {"customer_id", "order_id", "status", "year"});

    @ParameterizedTest
    @MethodSource("lookupPaths")
    void testLookupPathPriority(
            int[] lookupKeys, int[] bucketKeys, LookupType expectedType, int[] expectedKeys) {
        LookupNormalizer normalizer = create(lookupKeys, bucketKeys, null);
        assertThat(normalizer.getLookupType()).isEqualTo(expectedType);
        assertThat(normalizer.getLookupKeyIndexes()).containsExactly(expectedKeys);
    }

    private static Stream<Arguments> lookupPaths() {
        return Stream.of(
                Arguments.of(
                        new int[] {0, 1, 2, 3},
                        new int[] {0},
                        LookupType.LOOKUP,
                        new int[] {0, 1, 3}),
                Arguments.of(
                        new int[] {0, 2, 3},
                        new int[] {0},
                        LookupType.PREFIX_LOOKUP,
                        new int[] {0, 3}),
                Arguments.of(
                        new int[] {2, 3},
                        new int[] {0},
                        LookupType.SECONDARY_INDEX_LOOKUP,
                        new int[] {2}),
                Arguments.of(
                        new int[] {0, 2},
                        new int[] {0},
                        LookupType.SECONDARY_INDEX_LOOKUP,
                        new int[] {2}),
                Arguments.of(
                        new int[] {1, 2, 3},
                        new int[] {1},
                        LookupType.SECONDARY_INDEX_LOOKUP,
                        new int[] {2}));
    }

    @Test
    void testPrefixLookupPreservesProjectedKeysAndRemainingFilter() {
        LookupNormalizer normalizer =
                create(new int[] {0, 1, 2}, new int[] {0}, new int[] {2, 3, 0, 1});
        assertThat(normalizer.getLookupType()).isEqualTo(LookupType.PREFIX_LOOKUP);
        assertThat(normalizer.getLookupKeyIndexes()).containsExactly(0, 3);

        RowData lookup = GenericRowData.of(7, 2026, 42);
        RowData normalized = normalizer.normalizeLookupKey(lookup);
        assertThat(normalized.getArity()).isEqualTo(2);
        assertThat(normalized.getInt(0)).isEqualTo(42);
        assertThat(normalized.getInt(1)).isEqualTo(2026);
        LookupNormalizer.RemainingFilter filter = normalizer.createRemainingFilter(lookup);
        assertThat(filter).isNotNull();
        assertThat(filter.isMatch(GenericRowData.of(7, 2026, 42, 1))).isTrue();
        assertThat(filter.isMatch(GenericRowData.of(8, 2026, 42, 1))).isFalse();
    }

    @Test
    void testUnsupportedLookupIsRejected() {
        assertThatThrownBy(() -> create(new int[] {1, 3}, new int[] {0}, null))
                .isInstanceOf(TableException.class);
    }

    private static LookupNormalizer create(
            int[] lookupKeys, int[] bucketKeys, int[] projectedFields) {
        int[][] keys = new int[lookupKeys.length][];
        for (int i = 0; i < lookupKeys.length; i++) {
            keys[i] = new int[] {lookupKeys[i]};
        }
        return LookupNormalizer.validateAndCreateLookupNormalizer(
                keys,
                new int[] {0, 1, 3},
                bucketKeys,
                new int[] {3},
                SCHEMA,
                projectedFields,
                new int[][] {{2}});
    }

    private static final RowType MULTI_KEY_SCHEMA = multiKeySchema();
    private static final int[] PK = {0, 1, 2, 3};
    private static final int[] NONE = {};

    /**
     * Name, lookup keys, primary key, bucket key, partition keys, projection, then the expected
     * lookup type, lookup keys sent to Fluss, and whether the client filters the result. Unless
     * stated, the primary key is (a, b, c, d) and the bucket key a.
     */
    static Stream<Arguments> cases() {
        return Stream.of(
                prefix("bucket key", keys(0), ints(0), false),
                prefix("next primary key", keys(0, 1), ints(0, 1), false),
                prefix("next primary keys", keys(0, 1, 2), ints(0, 1, 2), false),
                prefix("skipped primary key", keys(0, 2), ints(0), true),
                prefix("gap after the prefix", keys(0, 1, 3), ints(0, 1), true),
                prefix("non-key column", keys(0, 1, 4), ints(0, 1), true),
                Arguments.of(
                        "all primary keys",
                        keys(0, 1, 2, 3),
                        PK,
                        ints(0),
                        NONE,
                        null,
                        LOOKUP,
                        ints(0, 1, 2, 3),
                        false),
                Arguments.of(
                        "bucket key (a, b)",
                        keys(0, 1, 2),
                        PK,
                        ints(0, 1),
                        NONE,
                        null,
                        PREFIX_LOOKUP,
                        ints(0, 1, 2),
                        false),
                Arguments.of(
                        "partition key d",
                        keys(0, 1, 3),
                        PK,
                        ints(0),
                        ints(3),
                        null,
                        PREFIX_LOOKUP,
                        ints(0, 1, 3),
                        false),
                Arguments.of(
                        "primary key (b, a, c, d)",
                        keys(0, 1),
                        ints(1, 0, 2, 3),
                        ints(1),
                        NONE,
                        null,
                        PREFIX_LOOKUP,
                        ints(1, 0),
                        false),
                Arguments.of(
                        "projection (d, b, a)",
                        new int[][] {{2}, {1}},
                        PK,
                        ints(0),
                        NONE,
                        ints(3, 1, 0),
                        PREFIX_LOOKUP,
                        ints(0, 1),
                        false));
    }

    private static Arguments prefix(
            String name, int[][] lookupKeys, int[] expectedLookupKeys, boolean clientFilter) {
        return Arguments.of(
                name,
                lookupKeys,
                PK,
                ints(0),
                NONE,
                null,
                PREFIX_LOOKUP,
                expectedLookupKeys,
                clientFilter);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void testLookupKeys(
            String name,
            int[][] lookupKeys,
            int[] primaryKey,
            int[] bucketKey,
            int[] partitionKeys,
            @Nullable int[] projection,
            LookupType expectedType,
            int[] expectedLookupKeys,
            boolean expectedClientFilter) {
        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        lookupKeys,
                        primaryKey,
                        bucketKey,
                        partitionKeys,
                        MULTI_KEY_SCHEMA,
                        projection);

        assertThat(normalizer.getLookupType()).isEqualTo(expectedType);
        assertThat(normalizer.getLookupKeyIndexes()).containsExactly(expectedLookupKeys);
        // keys and results differ in every field, so any filter condition rejects the result
        RemainingFilter filter = normalizer.createRemainingFilter(row(lookupKeys.length, 1L));
        assertThat(filter != null && !filter.isMatch(row(5, 2L))).isEqualTo(expectedClientFilter);
    }

    @Test
    void testNormalizeLookupKeyAndFilter() {
        // (a, c): Fluss is asked for a, the client filters on c
        LookupNormalizer skipped =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        keys(0, 2), PK, ints(0), NONE, MULTI_KEY_SCHEMA, null);
        assertThat(skipped.normalizeLookupKey(GenericRowData.of(1L, 3L)))
                .isEqualTo(GenericRowData.of(1L));
        RemainingFilter filter = skipped.createRemainingFilter(GenericRowData.of(1L, 3L));
        assertThat(filter.isMatch(GenericRowData.of(1L, 9L, 3L, 9L, 9L))).isTrue();
        assertThat(filter.isMatch(GenericRowData.of(1L, 9L, 4L, 9L, 9L))).isFalse();

        // (a, b) on the primary key (b, a, c, d): the key is reordered to (b, a)
        LookupNormalizer reordered =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        keys(0, 1), ints(1, 0, 2, 3), ints(1), NONE, MULTI_KEY_SCHEMA, null);
        assertThat(reordered.normalizeLookupKey(GenericRowData.of(1L, 2L)))
                .isEqualTo(GenericRowData.of(2L, 1L));
    }

    private static int[][] keys(int... indexes) {
        return Arrays.stream(indexes).mapToObj(i -> new int[] {i}).toArray(int[][]::new);
    }

    private static int[] ints(int... values) {
        return values;
    }

    private static GenericRowData row(int arity, long value) {
        Object[] fields = new Object[arity];
        Arrays.fill(fields, value);
        return GenericRowData.of(fields);
    }

    private static RowType multiKeySchema() {
        LogicalType[] types = new LogicalType[5];
        Arrays.fill(types, new BigIntType());
        return RowType.of(types, new String[] {"a", "b", "c", "d", "e"});
    }
}
