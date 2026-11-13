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

import static org.apache.fluss.types.DataTypeRoot.ARRAY;
import static org.apache.fluss.types.DataTypeRoot.MAP;
import static org.apache.fluss.types.DataTypeRoot.ROW;
import static org.assertj.core.api.Assertions.assertThat;

/** Regression tests for bounded collection by last distinct occurrence. */
class FieldLastNDistinctAggTest {

    @Test
    void testParameters() {
        AggFunctions.LAST_N(3, true).validateParameters();
        AggFunctions.LAST_N(3, false).validateParameters();
        AggFunctionType.LAST_N.validateParameter("distinct", "TRUE");
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testRandomBatchesAndSingleUpdates(BinaryRowFormat format) {
        Random random = new Random(86231);
        for (int size : new int[] {1, 3, 10, Integer.MAX_VALUE}) {
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
            }
        }
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
    void testElementEquality(BinaryRowFormat format) {
        checkType(DataTypes.BOOLEAN(), format, new Object[] {false, true, null}, true, false, true);
        checkType(
                DataTypes.TINYINT(),
                format,
                new Object[] {(byte) 2, (byte) 1, null},
                (byte) 1,
                (byte) 2,
                (byte) 1);
        checkType(
                DataTypes.SMALLINT(),
                format,
                new Object[] {(short) 2, (short) 1, null},
                (short) 1,
                (short) 2,
                (short) 1);
        checkType(DataTypes.INT(), format, new Object[] {2, 1, null}, 1, 2, 1);
        checkType(DataTypes.BIGINT(), format, new Object[] {2L, 1L, null}, 1L, 2L, 1L);
        checkType(
                DataTypes.FLOAT(),
                format,
                new Object[] {-0.0F, 0.0F, Float.NaN, null},
                Float.NaN,
                -0.0F,
                0.0F,
                Float.intBitsToFloat(0x7fc00001));
        checkType(
                DataTypes.DOUBLE(),
                format,
                new Object[] {-0.0D, 0.0D, Double.NaN, null},
                Double.NaN,
                -0.0D,
                0.0D,
                Double.longBitsToDouble(0x7ff8000000000001L));
        checkType(
                DataTypes.STRING(),
                format,
                new Object[] {
                    BinaryString.fromString(""),
                    BinaryString.fromString("a long string outside the slot"),
                    BinaryString.fromString("short"),
                    null
                },
                BinaryString.fromString(""),
                BinaryString.fromString("short"),
                BinaryString.fromString("a long string outside the slot"),
                BinaryString.fromString("short"));
        checkType(
                DataTypes.CHAR(3),
                format,
                new Object[] {BinaryString.fromString("xyz"), BinaryString.fromString("abc"), null},
                BinaryString.fromString("abc"),
                BinaryString.fromString("xyz"),
                BinaryString.fromString("abc"));
        checkType(
                DataTypes.BYTES(),
                format,
                new Object[] {new byte[0], new byte[31], new byte[] {1, 2}, null},
                new byte[0],
                new byte[] {1, 2},
                new byte[31],
                new byte[] {1, 2});
        checkType(
                DataTypes.BINARY(2),
                format,
                new Object[] {new byte[] {3, 4}, new byte[] {1, 2}, null},
                new byte[] {1, 2},
                new byte[] {3, 4},
                new byte[] {1, 2});
        checkType(DataTypes.DATE(), format, new Object[] {2, 1, null}, 1, 2, 1);
        checkType(DataTypes.TIME(), format, new Object[] {2, 1, null}, 1, 2, 1);
        checkType(
                DataTypes.DECIMAL(10, 2),
                format,
                new Object[] {decimal("2.00", 10), decimal("1.00", 10), null},
                decimal("1.00", 10),
                decimal("2.00", 10),
                decimal("1.00", 10));
        checkType(
                DataTypes.DECIMAL(30, 2),
                format,
                new Object[] {decimal("2.00", 30), decimal("1.00", 30), null},
                decimal("1.00", 30),
                decimal("2.00", 30),
                decimal("1.00", 30));
        checkType(
                DataTypes.TIMESTAMP(3),
                format,
                new Object[] {TimestampNtz.fromMillis(2), TimestampNtz.fromMillis(1), null},
                TimestampNtz.fromMillis(1),
                TimestampNtz.fromMillis(2),
                TimestampNtz.fromMillis(1));
        checkType(
                DataTypes.TIMESTAMP(9),
                format,
                new Object[] {TimestampNtz.fromMillis(1, 2), TimestampNtz.fromMillis(1, 1), null},
                TimestampNtz.fromMillis(1, 1),
                TimestampNtz.fromMillis(1, 2),
                TimestampNtz.fromMillis(1, 1));
        checkType(
                DataTypes.TIMESTAMP_LTZ(3),
                format,
                new Object[] {
                    TimestampLtz.fromEpochMillis(2), TimestampLtz.fromEpochMillis(1), null
                },
                TimestampLtz.fromEpochMillis(1),
                TimestampLtz.fromEpochMillis(2),
                TimestampLtz.fromEpochMillis(1));
        checkType(
                DataTypes.TIMESTAMP_LTZ(9),
                format,
                new Object[] {
                    TimestampLtz.fromEpochMillis(1, 2), TimestampLtz.fromEpochMillis(1, 1), null
                },
                TimestampLtz.fromEpochMillis(1, 1),
                TimestampLtz.fromEpochMillis(1, 2),
                TimestampLtz.fromEpochMillis(1, 1));
        checkType(
                DataTypes.ARRAY(DataTypes.BIGINT()),
                format,
                new Object[] {
                    new GenericArray(new long[] {3, 4}), new GenericArray(new long[] {1, 2}), null
                },
                new GenericArray(new long[] {1, 2}),
                new GenericArray(new long[] {3, 4}),
                new GenericArray(new long[] {1, 2}));
        checkType(
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("bytes", DataTypes.BYTES())),
                format,
                new Object[] {
                    GenericRow.of(1, new byte[] {2}),
                    GenericRow.of(2, new byte[] {1}),
                    GenericRow.of(1, new byte[] {1}),
                    null
                },
                GenericRow.of(1, new byte[] {1}),
                GenericRow.of(1, new byte[] {2}),
                GenericRow.of(2, new byte[] {1}),
                GenericRow.of(1, new byte[] {1}));
        checkType(
                DataTypes.MAP(DataTypes.INT(), DataTypes.BYTES()),
                format,
                new Object[] {
                    GenericMap.of(1, new byte[] {1}, 2, new byte[] {2}),
                    GenericMap.of(1, new byte[] {3}, 2, new byte[] {2}),
                    GenericMap.of(3, new byte[] {1}, 2, new byte[] {2}),
                    GenericMap.of(2, new byte[] {2}, 1, new byte[] {1}),
                    null
                },
                GenericMap.of(1, new byte[] {1}, 2, new byte[] {2}),
                GenericMap.of(1, new byte[] {3}, 2, new byte[] {2}),
                GenericMap.of(3, new byte[] {1}, 2, new byte[] {2}),
                GenericMap.of(2, new byte[] {2}, 1, new byte[] {1}));
        checkType(
                DataTypes.MAP(DataTypes.BYTES(), DataTypes.INT()),
                format,
                new Object[] {
                    GenericMap.of(new byte[] {2}, 10), GenericMap.of(new byte[] {1}, 10), null
                },
                GenericMap.of(new byte[] {1}, 10),
                GenericMap.of(new byte[] {2}, 10),
                GenericMap.of(new byte[] {1}, 10));
    }

    private static Decimal decimal(String value, int precision) {
        return Decimal.fromBigDecimal(new BigDecimal(value), precision, 2);
    }

    private static void checkType(
            DataType type, BinaryRowFormat format, Object[] expected, Object... values) {
        Object[] input = new Object[values.length + 2];
        System.arraycopy(values, 0, input, 1, values.length);
        BinaryArray encoded = binary(type, format, input);
        FieldLastNAgg agg = new FieldLastNAgg(DataTypes.ARRAY(type), values.length + 2, true);
        InternalArray acc = (InternalArray) agg.agg(null, encoded);
        // Nested values use representation-sensitive element equality, as collect(distinct) does.
        InternalArray replay = type.isAnyOf(ARRAY, MAP, ROW) ? encoded : new GenericArray(input);
        InternalArray repeated = (InternalArray) agg.agg(acc, replay);
        ArraySerializer canonical = new ArraySerializer(type, BinaryRowFormat.ALIGNED);
        BinaryArray expectedArray = binary(type, BinaryRowFormat.ALIGNED, expected);
        for (InternalArray result : new InternalArray[] {acc, repeated}) {
            assertThat(canonical.toBinaryArray(result))
                    .as("%s / %s", type, format)
                    .isEqualTo(expectedArray);
        }
    }

    private static BinaryArray binary(DataType type, BinaryRowFormat format, Object[] values) {
        return new ArraySerializer(type, format).toBinaryArray(new GenericArray(values));
    }
}
