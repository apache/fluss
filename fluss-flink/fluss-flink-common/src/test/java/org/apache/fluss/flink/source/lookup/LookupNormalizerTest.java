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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarBinaryType;
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

/** Tests for {@link LookupNormalizer} on a table with the BIGINT fields a, b, c, d and e. */
class LookupNormalizerTest {

    private static final RowType SCHEMA = schema();
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
                        lookupKeys, primaryKey, bucketKey, partitionKeys, SCHEMA, projection);

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
                        keys(0, 2), PK, ints(0), NONE, SCHEMA, null);
        assertThat(skipped.normalizeLookupKey(GenericRowData.of(1L, 3L)))
                .isEqualTo(GenericRowData.of(1L));
        RemainingFilter filter = skipped.createRemainingFilter(GenericRowData.of(1L, 3L));
        assertThat(filter.isMatch(GenericRowData.of(1L, 9L, 3L, 9L, 9L))).isTrue();
        assertThat(filter.isMatch(GenericRowData.of(1L, 9L, 4L, 9L, 9L))).isFalse();

        // (a, b) on the primary key (b, a, c, d): the key is reordered to (b, a)
        LookupNormalizer reordered =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        keys(0, 1), ints(1, 0, 2, 3), ints(1), NONE, SCHEMA, null);
        assertThat(reordered.normalizeLookupKey(GenericRowData.of(1L, 2L)))
                .isEqualTo(GenericRowData.of(2L, 1L));
    }

    @Test
    void testRemainingFilterComparesBinaryByContent() {
        // Primary key (id); Join also has an equality condition on non-key BYTES column.
        RowType schema =
                RowType.of(
                        new LogicalType[] {
                            new BigIntType(), new VarBinaryType(VarBinaryType.MAX_LENGTH)
                        },
                        new String[] {"id", "payload"});
        LookupNormalizer normalizer =
                LookupNormalizer.validateAndCreateLookupNormalizer(
                        keys(0, 1), ints(0), ints(0), NONE, schema, null);
        assertThat(normalizer.normalizeLookupKey(GenericRowData.of(1L, new byte[] {1, 2})))
                .isEqualTo(GenericRowData.of(1L));

        RemainingFilter filter =
                normalizer.createRemainingFilter(GenericRowData.of(1L, new byte[] {1, 2}));
        assertThat(filter).isNotNull();
        // The looked-up row carries a different byte[] instance with the same content.
        assertThat(filter.isMatch(GenericRowData.of(1L, new byte[] {1, 2}))).isTrue();
        assertThat(filter.isMatch(GenericRowData.of(1L, new byte[] {1, 3}))).isFalse();
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

    private static RowType schema() {
        LogicalType[] types = new LogicalType[5];
        Arrays.fill(types, new BigIntType());
        return RowType.of(types, new String[] {"a", "b", "c", "d", "e"});
    }
}
