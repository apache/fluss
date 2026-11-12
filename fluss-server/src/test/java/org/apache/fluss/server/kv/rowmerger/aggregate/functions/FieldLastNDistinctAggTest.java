/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server.kv.rowmerger.aggregate.functions;

import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.row.BinaryArray;
import org.apache.fluss.row.BinaryRow.BinaryRowFormat;
import org.apache.fluss.row.BinarySegmentUtils;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.serializer.ArraySerializer;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Regression tests for bounded collection by last distinct occurrence. */
class FieldLastNDistinctAggTest {

    @Test
    void testParameters() {
        AggFunctions.LAST_N(3, true).validateParameters();
        AggFunctions.LAST_N(3, false).validateParameters();
        assertThatThrownBy(() -> AggFunctionType.LAST_N.validateParameter("distinct", "yes"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("true")
                .hasMessageContaining("last_n");
        assertThatThrownBy(() -> AggFunctions.LAST_N(0, true).validateParameters())
                .isInstanceOf(IllegalArgumentException.class);
        AggFunctionType.LAST_N.validateParameter("distinct", "TRUE");
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testRandomBatchesAndSingleUpdates(BinaryRowFormat format) {
        Random random = new Random(86231);
        for (int size : new int[] {1, 3, 10, 100, Integer.MAX_VALUE}) {
            FieldLastNAgg agg = new FieldLastNAgg(DataTypes.ARRAY(DataTypes.BIGINT()), size, true);
            InternalArray acc = null;
            LinkedHashSet<Long> expected = new LinkedHashSet<>();
            for (int round = 0; round < 100; round++) {
                Object[] input = new Object[random.nextInt(12)];
                for (int i = 0; i < input.length; i++) {
                    Long value = random.nextInt(7) == 0 ? null : (long) random.nextInt(20);
                    input[i] = value;
                    expected.remove(value);
                    expected.add(value);
                    if (expected.size() > size) {
                        expected.remove(expected.iterator().next());
                    }
                }
                InternalArray array =
                        round % 2 == 0
                                ? binary(DataTypes.BIGINT(), format, input)
                                : new GenericArray(input);
                acc = (InternalArray) agg.agg(acc, array);
                List<Long> actual = new ArrayList<>();
                for (int i = 0; i < acc.size(); i++) {
                    actual.add(acc.isNullAt(i) ? null : acc.getLong(i));
                }
                assertThat(actual).containsExactlyElementsOf(expected);
                assertThat(agg.agg(acc, null)).isSameAs(acc);
            }
        }
    }

    @Test
    void testFailedInputDoesNotAffectNextAggregation() {
        FieldLastNAgg agg = new FieldLastNAgg(DataTypes.ARRAY(DataTypes.BIGINT()), 3, true);
        assertThatThrownBy(
                        () ->
                                agg.agg(
                                        null,
                                        new GenericArray(new Object[] {"invalid long", 2L, 1L})))
                .isInstanceOf(ClassCastException.class);
        InternalArray result =
                (InternalArray) agg.agg(null, new GenericArray(new long[] {1, 2, 3}));
        assertThat(result.toLongArray()).containsExactly(1, 2, 3);
    }

    @Test
    void testHashCollisionsDoNotRemoveDifferentValues() {
        FieldLastNAgg agg = new FieldLastNAgg(DataTypes.ARRAY(DataTypes.BIGINT()), 3, true);
        InternalArray result =
                (InternalArray)
                        agg.agg(null, new GenericArray(new long[] {1L, 1L << 32, 1L, 1L << 32}));
        assertThat(result.toLongArray()).containsExactly(1L, 1L << 32);
    }

    @Test
    void testDeduplicatesAndBoundsRawAccumulatorAfterOverwrite() {
        FieldLastNAgg agg = new FieldLastNAgg(DataTypes.ARRAY(DataTypes.BIGINT()), 3, true);
        InternalArray repeated = new GenericArray(new long[] {1, 1, 2});
        assertThat(
                        ((InternalArray) agg.agg(repeated, new GenericArray(new long[] {2})))
                                .toLongArray())
                .containsExactly(1, 2);
        InternalArray oversized = new GenericArray(new long[] {1, 2, 3, 4});
        assertThat(
                        ((InternalArray) agg.agg(oversized, new GenericArray(new long[] {5})))
                                .toLongArray())
                .containsExactly(3, 4, 5);
        assertThat(((InternalArray) agg.agg(repeated, new GenericArray(new long[0]))).toLongArray())
                .containsExactly(1, 2);
    }

    @Test
    void testRepeatedTailAndEmptyInputs() {
        FieldLastNAgg agg = new FieldLastNAgg(DataTypes.ARRAY(DataTypes.BIGINT()), 2, true);
        InternalArray acc = (InternalArray) agg.agg(null, new GenericArray(new long[] {1, 2}));
        assertThat(agg.agg(acc, new GenericArray(new long[] {2}))).isSameAs(acc);
        assertThat(agg.agg(acc, new GenericArray(new long[0]))).isSameAs(acc);
        assertThat(((InternalArray) agg.agg(null, new GenericArray(new long[0]))).size()).isZero();
    }

    @Test
    void testReversedMergeDeduplicatesRawOlderInput() {
        FieldLastNAgg agg = new FieldLastNAgg(DataTypes.ARRAY(DataTypes.BIGINT()), 3, true);
        InternalArray result =
                (InternalArray)
                        agg.aggReversed(
                                new GenericArray(new long[] {2}),
                                new GenericArray(new long[] {1, 1, 3}));
        assertThat(result.toLongArray()).containsExactly(1, 3, 2);
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testElementTypesAndMixedRepresentations(BinaryRowFormat format) {
        checkType(DataTypes.BOOLEAN(), format, true, false, true);
        checkType(DataTypes.TINYINT(), format, (byte) 1, (byte) 2, (byte) 1);
        checkType(DataTypes.SMALLINT(), format, (short) 1, (short) 2, (short) 1);
        checkType(DataTypes.INT(), format, 1, 2, 1);
        checkType(DataTypes.BIGINT(), format, 1L, 2L, 1L);
        checkType(
                DataTypes.FLOAT(),
                format,
                Float.NaN,
                -0.0F,
                0.0F,
                Float.intBitsToFloat(0x7fc00001));
        checkType(
                DataTypes.DOUBLE(),
                format,
                Double.NaN,
                -0.0D,
                0.0D,
                Double.longBitsToDouble(0x7ff8000000000001L));
        checkType(
                DataTypes.STRING(),
                format,
                BinaryString.fromString(""),
                BinaryString.fromString("short"),
                BinaryString.fromString("a long string outside the slot"),
                BinaryString.fromString("short"));
        checkType(
                DataTypes.CHAR(3),
                format,
                BinaryString.fromString("abc"),
                BinaryString.fromString("xyz"),
                BinaryString.fromString("abc"));
        checkType(
                DataTypes.BYTES(),
                format,
                new byte[0],
                new byte[] {1, 2},
                new byte[31],
                new byte[] {1, 2});
        checkType(
                DataTypes.BINARY(2),
                format,
                new byte[] {1, 2},
                new byte[] {3, 4},
                new byte[] {1, 2});
        checkType(DataTypes.DATE(), format, 1, 2, 1);
        checkType(DataTypes.TIME(), format, 1, 2, 1);
        checkType(
                DataTypes.DECIMAL(10, 2),
                format,
                decimal("1.00", 10),
                decimal("2.00", 10),
                decimal("1.00", 10));
        checkType(
                DataTypes.DECIMAL(30, 2),
                format,
                decimal("1.00", 30),
                decimal("2.00", 30),
                decimal("1.00", 30));
        checkType(
                DataTypes.TIMESTAMP(3),
                format,
                TimestampNtz.fromMillis(1),
                TimestampNtz.fromMillis(2),
                TimestampNtz.fromMillis(1));
        checkType(
                DataTypes.TIMESTAMP(9),
                format,
                TimestampNtz.fromMillis(1, 1),
                TimestampNtz.fromMillis(1, 2),
                TimestampNtz.fromMillis(1, 1));
        checkType(
                DataTypes.TIMESTAMP_LTZ(3),
                format,
                TimestampLtz.fromEpochMillis(1),
                TimestampLtz.fromEpochMillis(2),
                TimestampLtz.fromEpochMillis(1));
        checkType(
                DataTypes.TIMESTAMP_LTZ(9),
                format,
                TimestampLtz.fromEpochMillis(1, 1),
                TimestampLtz.fromEpochMillis(1, 2),
                TimestampLtz.fromEpochMillis(1, 1));
        checkType(
                DataTypes.ARRAY(DataTypes.BIGINT()),
                format,
                new GenericArray(new long[] {1, 2}),
                new GenericArray(new long[] {3}),
                new GenericArray(new long[] {1, 2}));
        checkType(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("bytes", DataTypes.BYTES())),
                format,
                GenericRow.of(1, new byte[] {1}),
                GenericRow.of(2, new byte[] {2}),
                GenericRow.of(1, new byte[] {1}));
        checkType(
                DataTypes.MAP(DataTypes.INT(), DataTypes.BYTES()),
                format,
                GenericMap.of(1, new byte[] {1}, 2, new byte[] {2}),
                GenericMap.of(3, new byte[] {3}),
                GenericMap.of(2, new byte[] {2}, 1, new byte[] {1}));
    }

    private static Decimal decimal(String value, int precision) {
        return Decimal.fromBigDecimal(new BigDecimal(value), precision, 2);
    }

    private static void checkType(DataType type, BinaryRowFormat format, Object... values) {
        Object[] input = new Object[values.length + 2];
        System.arraycopy(values, 0, input, 1, values.length);
        BinaryArray encoded = binary(type, format, input);
        FieldLastNAgg agg = new FieldLastNAgg(DataTypes.ARRAY(type), values.length + 2, true);
        InternalArray acc = (InternalArray) agg.agg(null, encoded);
        // Rewriting the same sequence must not depend on binary vs generic representation.
        InternalArray mixed = (InternalArray) agg.agg(acc, new GenericArray(input));
        ArraySerializer canonical = new ArraySerializer(type, BinaryRowFormat.ALIGNED);
        BinaryArray before = canonical.toBinaryArray(acc);
        byte[] expectedBytes =
                BinarySegmentUtils.copyToBytes(
                        before.getSegments(), before.getOffset(), before.getSizeInBytes());
        BinaryArray after = canonical.toBinaryArray(mixed);
        assertThat(
                        BinarySegmentUtils.copyToBytes(
                                after.getSegments(), after.getOffset(), after.getSizeInBytes()))
                .as("%s / %s", type, format)
                .isEqualTo(expectedBytes);
        assertThat(mixed.size()).isEqualTo(values.length);
        assertThat(mixed.isNullAt(mixed.size() - 1)).isTrue();
    }

    private static BinaryArray binary(DataType type, BinaryRowFormat format, Object[] values) {
        return new ArraySerializer(type, format).toBinaryArray(new GenericArray(values));
    }
}
