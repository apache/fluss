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

package org.apache.fluss.row.array;

import org.apache.fluss.memory.MemorySegment;
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
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.apache.fluss.types.DataTypeChecks.getLength;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BinaryArraySplicer}. */
class BinaryArraySplicerTest {

    /** Every element type, with compact and non-compact decimals and timestamps. */
    private static final DataType[] ELEMENT_TYPES = {
        DataTypes.BOOLEAN(),
        DataTypes.TINYINT(),
        DataTypes.SMALLINT(),
        DataTypes.INT(),
        DataTypes.BIGINT(),
        DataTypes.FLOAT(),
        DataTypes.DOUBLE(),
        DataTypes.DATE(),
        DataTypes.TIME(),
        DataTypes.CHAR(3),
        DataTypes.STRING(),
        DataTypes.BINARY(3),
        DataTypes.BYTES(),
        DataTypes.DECIMAL(5, 2),
        DataTypes.DECIMAL(20, 2),
        DataTypes.TIMESTAMP(3),
        DataTypes.TIMESTAMP(9),
        DataTypes.TIMESTAMP_LTZ(3),
        DataTypes.TIMESTAMP_LTZ(9),
        DataTypes.ARRAY(DataTypes.STRING()),
        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
        DataTypes.ROW(
                DataTypes.FIELD("id", DataTypes.INT()), DataTypes.FIELD("name", DataTypes.STRING()))
    };

    private static final int ROUNDS = 50;

    @Test
    void testElementTypesCoverEveryTypeRoot() {
        Set<DataTypeRoot> roots = EnumSet.noneOf(DataTypeRoot.class);
        for (DataType type : ELEMENT_TYPES) {
            assertThat(BinaryArraySplicer.supports(type)).as("%s", type).isTrue();
            collectTypeRoots(type, roots);
        }
        // a new type needs a sample here, so that the test below covers it
        assertThat(roots).containsExactlyInAnyOrder(DataTypeRoot.values());
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testSpliceMatchesTheArrayWriter(BinaryRowFormat format) {
        Random random = new Random(42);
        for (DataType type : ELEMENT_TYPES) {
            for (int round = 0; round < ROUNDS; round++) {
                BinaryArray first = write(type, format, randomTopLevelElements(type, random));
                BinaryArray second = write(type, format, randomTopLevelElements(type, random));
                moveBehindRandomBytes(first, random);
                moveBehindRandomBytes(second, random);
                maybeNullOneInPlace(first, random);
                maybeNullOneInPlace(second, random);
                int firstStart = random.nextInt(first.size() + 1);
                int firstCount = random.nextInt(first.size() - firstStart + 1);
                int secondStart = random.nextInt(second.size() + 1);
                int secondCount = random.nextInt(second.size() - secondStart + 1);

                BinaryArray spliced =
                        new BinaryArraySplicer(type)
                                .append(first, firstStart, firstCount)
                                .append(second, secondStart, secondCount)
                                .build();

                List<Object> elements = new ArrayList<>();
                addElements(elements, type, first, firstStart, firstCount);
                addElements(elements, type, second, secondStart, secondCount);
                BinaryArray expected = write(type, format, elements.toArray());
                // BinaryArray#equals compares bytes only, not the class
                assertThat(spliced)
                        .as("%s, %s, round %s", type, format, round)
                        .isEqualTo(expected)
                        .isExactlyInstanceOf(expected.getClass());
            }
        }
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testManyRunsFromOneArray(BinaryRowFormat format) {
        DataType type = DataTypes.STRING();
        Object[] elements = new Object[10];
        for (int i = 0; i < elements.length; i++) {
            elements[i] = i % 4 == 0 ? null : BinaryString.fromString("element-" + i);
        }
        BinaryArray source = write(type, format, elements);

        BinaryArray spliced =
                new BinaryArraySplicer(type)
                        .append(source, 8, 2)
                        .append(source, 0, 3)
                        .append(source, 5, 0)
                        .append(source, 3, 5)
                        .append(source, 9, 1)
                        .build();

        Object[] expected = {
            elements[8],
            elements[9],
            elements[0],
            elements[1],
            elements[2],
            elements[3],
            elements[4],
            elements[5],
            elements[6],
            elements[7],
            elements[9]
        };
        assertThat(spliced).isEqualTo(write(type, format, expected));
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testNullElementGetsAZeroSlot(BinaryRowFormat format) {
        // Nulling an element in place keeps its old slot, while the writer leaves it zero.
        DataType strings = DataTypes.STRING();
        BinaryArray source =
                write(
                        strings,
                        format,
                        new Object[] {
                            BinaryString.fromString("a value longer than a slot"),
                            BinaryString.fromString("b")
                        });
        source.setNullAt(0);
        assertThat(new BinaryArraySplicer(strings).append(source, 0, 2).build())
                .isEqualTo(
                        write(strings, format, new Object[] {null, BinaryString.fromString("b")}));

        DataType longs = DataTypes.BIGINT();
        source = write(longs, format, new Object[] {1L, 2L});
        source.setNullAt(0);
        assertThat(new BinaryArraySplicer(longs).append(source, 0, 2).build())
                .isEqualTo(write(longs, format, new Object[] {null, 2L}));
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testNullsNextToRunsWithoutNulls(BinaryRowFormat format) {
        DataType type = DataTypes.BIGINT();
        Object[] elements = new Object[200];
        for (int i = 0; i < elements.length; i++) {
            elements[i] = (long) i;
        }
        // no null in [0, 64): a run that starts at 3 must still see the null at 65
        elements[65] = null;
        elements[130] = null;
        elements[199] = null;
        BinaryArray source = write(type, format, elements);

        BinaryArray spliced =
                new BinaryArraySplicer(type).append(source, 3, 150).append(source, 64, 136).build();

        List<Object> expected = new ArrayList<>();
        addElements(expected, type, source, 3, 150);
        addElements(expected, type, source, 64, 136);
        assertThat(spliced).isEqualTo(write(type, format, expected.toArray()));
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testDataOutOfElementOrder(BinaryRowFormat format) {
        DataType type = DataTypes.STRING();
        Object[] elements = {
            BinaryString.fromString("the first long value"),
            BinaryString.fromString("short"),
            BinaryString.fromString("the second long value")
        };
        BinaryArray source = write(type, format, elements);
        // swap the slots of the long values, so their data is no longer in element order
        MemorySegment segment = source.getSegments()[0];
        int slots = source.getOffset() + BinaryArray.calculateHeaderInBytes(source.size());
        long first = segment.getLong(slots);
        segment.putLong(slots, segment.getLong(slots + 16));
        segment.putLong(slots + 16, first);

        BinaryArray spliced = new BinaryArraySplicer(type).append(source, 0, 3).build();

        Object[] swapped = {elements[2], elements[1], elements[0]};
        assertThat(spliced).isEqualTo(write(type, format, swapped));
    }

    @ParameterizedTest
    @EnumSource(BinaryRowFormat.class)
    void testEmptyResult(BinaryRowFormat format) {
        DataType type = DataTypes.STRING();
        BinaryArray source = write(type, format, new Object[] {BinaryString.fromString("a")});

        BinaryArray spliced = new BinaryArraySplicer(type).append(source, 1, 0).build();

        assertThat(spliced).isEqualTo(write(type, format, new Object[0]));
    }

    @Test
    void testCanSplice() {
        DataType type = DataTypes.BIGINT();
        BinaryArray compacted = write(type, BinaryRowFormat.COMPACTED, new Object[] {1L});
        BinaryArray indexed = write(type, BinaryRowFormat.INDEXED, new Object[] {1L});
        GenericArray generic = new GenericArray(new Object[] {1L});

        assertThat(BinaryArraySplicer.canSplice(compacted, compacted)).isTrue();
        assertThat(BinaryArraySplicer.canSplice(null, indexed)).isTrue();
        assertThat(BinaryArraySplicer.canSplice(compacted, indexed)).isFalse();
        assertThat(BinaryArraySplicer.canSplice(generic, null)).isFalse();
        assertThat(BinaryArraySplicer.canSplice(compacted, generic)).isFalse();
    }

    @Test
    void testRejectsInvalidRuns() {
        DataType type = DataTypes.BIGINT();
        BinaryArray compacted = write(type, BinaryRowFormat.COMPACTED, new Object[] {1L, 2L});
        BinaryArray indexed = write(type, BinaryRowFormat.INDEXED, new Object[] {1L});

        assertThatThrownBy(() -> new BinaryArraySplicer(type).append(compacted, 1, 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BinaryArraySplicer(type).append(compacted, -1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        // start + count overflows to a negative number
        assertThatThrownBy(
                        () -> new BinaryArraySplicer(type).append(compacted, Integer.MAX_VALUE, 2))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(
                        () ->
                                new BinaryArraySplicer(type)
                                        .append(compacted, 0, 1)
                                        .append(indexed, 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BinaryArraySplicer(type).build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** Writes the elements the way rows write their array fields. */
    private static BinaryArray write(DataType type, BinaryRowFormat format, Object[] elements) {
        return new ArraySerializer(type, format).toBinaryArray(new GenericArray(elements));
    }

    /**
     * Copies the array to a random offset of a new buffer and points it there, with random bytes
     * before and after it, the way an array sits among the other fields of a row.
     */
    private static void moveBehindRandomBytes(BinaryArray array, Random random) {
        int size = array.getSizeInBytes();
        int offset = random.nextInt(16);
        byte[] bytes = new byte[offset + size + random.nextInt(16)];
        random.nextBytes(bytes);
        array.getSegments()[0].get(array.getOffset(), bytes, offset, size);
        array.pointTo(MemorySegment.wrap(bytes), offset, size);
    }

    /** Sometimes nulls an element in place, which leaves a gap in the variable-length data. */
    private static void maybeNullOneInPlace(BinaryArray array, Random random) {
        if (array.size() > 0 && random.nextInt(3) == 0) {
            array.setNullAt(random.nextInt(array.size()));
        }
    }

    private static void addElements(
            List<Object> elements, DataType type, BinaryArray source, int start, int count) {
        InternalArray.ElementGetter getter = InternalArray.createElementGetter(type);
        for (int i = start; i < start + count; i++) {
            elements.add(getter.getElementOrNull(source, i));
        }
    }

    /** Mostly short arrays, sometimes long ones with few nulls, to cover the 64-element skip. */
    private static Object[] randomTopLevelElements(DataType type, Random random) {
        return random.nextInt(4) == 0
                ? randomElements(type, random, 64 + random.nextInt(140), 50)
                : randomElements(type, random, random.nextInt(12), 5);
    }

    private static Object[] randomElements(DataType type, Random random, int size, int oneNullIn) {
        Object[] elements = new Object[size];
        for (int i = 0; i < elements.length; i++) {
            elements[i] = random.nextInt(oneNullIn) == 0 ? null : randomValue(type, random);
        }
        return elements;
    }

    private static Object randomValue(DataType type, Random random) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return random.nextBoolean();
            case TINYINT:
                return (byte) random.nextInt();
            case SMALLINT:
                return (short) random.nextInt();
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                return random.nextInt(1_000_000);
            case BIGINT:
                return random.nextLong();
            case FLOAT:
                return random.nextFloat();
            case DOUBLE:
                return random.nextDouble();
            case CHAR:
                return randomString(random, getLength(type));
            case STRING:
                return randomString(random, random.nextInt(20));
            case BINARY:
                return randomBytes(random, getLength(type));
            case BYTES:
                return randomBytes(random, random.nextInt(20));
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                BigInteger unscaled =
                        BigInteger.valueOf(random.nextLong())
                                .mod(BigInteger.TEN.pow(decimalType.getPrecision()));
                return Decimal.fromBigDecimal(
                        new BigDecimal(unscaled, decimalType.getScale()),
                        decimalType.getPrecision(),
                        decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampNtz.fromMillis(
                        random.nextInt(Integer.MAX_VALUE), randomNanos(type, random));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampLtz.fromEpochMillis(
                        random.nextInt(Integer.MAX_VALUE), randomNanos(type, random));
            case ARRAY:
                DataType elementType = ((ArrayType) type).getElementType();
                return new GenericArray(randomElements(elementType, random, random.nextInt(4), 5));
            case MAP:
                MapType mapType = (MapType) type;
                Map<Object, Object> map = new HashMap<>();
                for (int i = random.nextInt(4); i > 0; i--) {
                    map.put(
                            randomValue(mapType.getKeyType(), random),
                            random.nextBoolean()
                                    ? null
                                    : randomValue(mapType.getValueType(), random));
                }
                return new GenericMap(map);
            case ROW:
                RowType rowType = (RowType) type;
                GenericRow row = new GenericRow(rowType.getFieldCount());
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    row.setField(i, randomValue(rowType.getTypeAt(i), random));
                }
                return row;
            default:
                throw new IllegalArgumentException("No random value for type " + type);
        }
    }

    private static int randomNanos(DataType type, Random random) {
        // compact timestamps only keep milliseconds
        return getPrecision(type) > 3 ? random.nextInt(1_000_000) : 0;
    }

    private static BinaryString randomString(Random random, int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append((char) ('a' + random.nextInt(26)));
        }
        return BinaryString.fromString(builder.toString());
    }

    private static byte[] randomBytes(Random random, int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }

    private static void collectTypeRoots(DataType type, Set<DataTypeRoot> roots) {
        roots.add(type.getTypeRoot());
        for (DataType child : type.getChildren()) {
            collectTypeRoots(child, roots);
        }
    }
}
