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

package org.apache.fluss.server.kv.rowmerger.aggregate;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.AggFunction;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.columnar.ColumnarMap;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.server.kv.rowmerger.AggregateRowMerger;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldCollectAgg;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.MapType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.InternalMapAssert.assertThatMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/** Tests for collection aggregation semantics and encoding in both KV formats. */
class FieldCollectionAggregatorTest {

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDistinctCollectOnFirstWrite(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(true));
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue input = value(schema, format, GenericArray.of(3, 1, 3, null, null));
        BinaryValue result = merger.merge(null, input);
        assertCollectedValues(result, DataTypes.INT(), 3, 1, null);
        assertCollectedValues(
                merger.merge(result, value(schema, format, null)), DataTypes.INT(), 3, 1, null);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDistinctCollectAfterNull(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(true));
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(schema, format, null),
                                value(schema, format, GenericArray.of(3, 1, 3, null, null)));
        assertCollectedValues(result, DataTypes.INT(), 3, 1, null);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDistinctCollectKeepsFirstOccurrenceOrder(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(true));
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(schema, format, GenericArray.of(3, 1)),
                                value(schema, format, GenericArray.of(1, 2, 3, null)));
        assertCollectedValues(result, DataTypes.INT(), 3, 1, 2, null);
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testDistinctCollectWithRepeatedInput(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(true));
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue result =
                merger.merge(
                        value(schema, format, GenericArray.of(3, 1, null)),
                        value(schema, format, GenericArray.of(null, 1, 3, 1)));
        assertCollectedValues(result, DataTypes.INT(), 3, 1, null);
        result =
                merger.merge(
                        value(schema, format, GenericArray.of(3, 3, null, null)),
                        value(schema, format, GenericArray.of(3, null)));
        assertCollectedValues(result, DataTypes.INT(), 3, null);
    }

    @ParameterizedTest
    @MethodSource("collectModes")
    void testCollectWithEmptyArrays(KvFormat format, boolean distinct) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(distinct));
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue empty = value(schema, format, GenericArray.of());
        BinaryValue input = value(schema, format, GenericArray.of(3, 3, null));
        Object[] expected = distinct ? new Object[] {3, null} : new Object[] {3, 3, null};
        assertCollectedValues(merger.merge(null, empty), DataTypes.INT());
        assertCollectedValues(merger.merge(value(schema, format, null), empty), DataTypes.INT());
        assertCollectedValues(merger.merge(empty, empty), DataTypes.INT());
        assertCollectedValues(merger.merge(empty, input), DataTypes.INT(), expected);
        assertCollectedValues(merger.merge(input, empty), DataTypes.INT(), expected);
    }

    static Stream<Arguments> collectModes() {
        return Stream.of(KvFormat.values())
                .flatMap(
                        format ->
                                Stream.of(false, true)
                                        .map(distinct -> Arguments.of(format, distinct)));
    }

    @ParameterizedTest(name = "{0}, distinct={1}, element={2}")
    @MethodSource("collectElements")
    void testCollectElements(
            KvFormat format,
            boolean distinct,
            DataType elementType,
            Object first,
            Object second,
            Object third) {
        Schema schema = schema(DataTypes.ARRAY(elementType), AggFunctions.COLLECT(distinct));
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(schema, format, GenericArray.of(first, second, null)),
                                value(schema, format, GenericArray.of(first, third, null, first)));
        Object[] expected =
                distinct
                        ? new Object[] {first, second, null, third}
                        : new Object[] {first, second, null, first, third, null, first};
        assertCollectedValues(result, elementType, expected);
        if (distinct) {
            BinaryValue repeated =
                    merger(schema, format)
                            .merge(
                                    value(schema, format, new GenericArray(expected)),
                                    value(
                                            schema,
                                            format,
                                            GenericArray.of(third, first, null, second)));
            assertCollectedValues(repeated, elementType, expected);
        }
    }

    static Stream<Arguments> collectElements() {
        Stream.Builder<Arguments> cases = Stream.builder();
        for (KvFormat format : KvFormat.values()) {
            for (boolean distinct : new boolean[] {false, true}) {
                cases.add(Arguments.of(format, distinct, DataTypes.INT(), 1, 2, 3));
                cases.add(
                        Arguments.of(
                                format,
                                distinct,
                                DataTypes.STRING(),
                                BinaryString.fromString("b"),
                                BinaryString.fromString("a"),
                                BinaryString.fromString("c")));
                cases.add(
                        Arguments.of(
                                format,
                                distinct,
                                DataTypes.BYTES(),
                                new byte[] {2, 3},
                                new byte[] {4},
                                new byte[] {1, 4, 5}));
                cases.add(
                        Arguments.of(
                                format,
                                distinct,
                                DataTypes.ARRAY(DataTypes.INT()),
                                GenericArray.of(1, null),
                                GenericArray.of(1, 3),
                                GenericArray.of(1, 4)));
                cases.add(
                        Arguments.of(
                                format,
                                distinct,
                                DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING())),
                                GenericRow.of(1, BinaryString.fromString("a")),
                                GenericRow.of(1, BinaryString.fromString("b")),
                                GenericRow.of(1, BinaryString.fromString("c"))));
            }
        }
        return cases.build();
    }

    @ParameterizedTest
    @MethodSource("collectModes")
    void testReversedCollect(KvFormat format, boolean distinct) {
        FieldCollectAgg aggregator =
                new FieldCollectAgg(DataTypes.ARRAY(DataTypes.INT()), distinct);
        Schema schema = schema(DataTypes.ARRAY(DataTypes.INT()), AggFunctions.COLLECT(distinct));
        InternalArray result =
                (InternalArray)
                        aggregator.aggReversed(
                                value(schema, format, GenericArray.of(3, 4)).row.getArray(1),
                                value(schema, format, GenericArray.of(1, 2)).row.getArray(1));
        assertThat(result.size()).isEqualTo(4);
        for (int i = 0; i < result.size(); i++) {
            assertThat(result.getInt(i)).isEqualTo(i + 1);
        }
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testCollectAcrossUpdates(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.STRING()), AggFunctions.COLLECT(false));
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue result = null;
        List<Object> expected = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Object[] input = {
                BinaryString.fromString("s" + i), null, BinaryString.fromString("long value " + i)
            };
            expected.addAll(Arrays.asList(input));
            result = merger.merge(result, value(schema, format, new GenericArray(input)));
            assertCollectedValues(result, DataTypes.STRING(), expected.toArray());
        }
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testCollectWithMixedArrayRepresentations(KvFormat format) {
        Schema schema = schema(DataTypes.ARRAY(DataTypes.STRING()), AggFunctions.COLLECT(false));
        FieldCollectAgg aggregator =
                new FieldCollectAgg(DataTypes.ARRAY(DataTypes.STRING()), false);
        GenericArray generic = GenericArray.of(BinaryString.fromString("a"), null);
        InternalArray binary =
                value(schema, format, GenericArray.of(BinaryString.fromString("b"), null))
                        .row
                        .getArray(1);
        KvFormat otherFormat = format == KvFormat.COMPACTED ? KvFormat.INDEXED : KvFormat.COMPACTED;
        InternalArray otherBinary = value(schema, otherFormat, generic).row.getArray(1);
        for (InternalArray input : new InternalArray[] {generic, otherBinary}) {
            assertCollectedValues(
                    (InternalArray) aggregator.agg(binary, input),
                    DataTypes.STRING(),
                    BinaryString.fromString("b"),
                    null,
                    BinaryString.fromString("a"),
                    null);
            assertCollectedValues(
                    (InternalArray) aggregator.agg(input, binary),
                    DataTypes.STRING(),
                    BinaryString.fromString("a"),
                    null,
                    BinaryString.fromString("b"),
                    null);
        }
    }

    @ParameterizedTest
    @EnumSource(KvFormat.class)
    void testMergeMap(KvFormat format) {
        MapType type = DataTypes.MAP(DataTypes.INT(), DataTypes.STRING());
        Schema schema = schema(type, AggFunctions.MERGE_MAP());
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(
                                        schema,
                                        format,
                                        GenericMap.of(
                                                1,
                                                BinaryString.fromString("a"),
                                                2,
                                                BinaryString.fromString("b"))),
                                value(
                                        schema,
                                        format,
                                        GenericMap.of(2, BinaryString.fromString("c"), 3, null)));
        InternalMap map = result.row.getMap(1);
        assertThat(map.size()).isEqualTo(3);
        assertThatMap(map)
                .withMapType(type)
                .isEqualTo(
                        GenericMap.of(
                                1,
                                BinaryString.fromString("a"),
                                2,
                                BinaryString.fromString("c"),
                                3,
                                null));
    }

    @ParameterizedTest
    @MethodSource("binaryTypes")
    void testMergeMapBinaryKeys(KvFormat format, DataType keyType) {
        Schema schema =
                schema(DataTypes.MAP(keyType, DataTypes.STRING()), AggFunctions.MERGE_MAP());
        AggregateRowMerger merger = merger(schema, format);
        BinaryValue result =
                merger.merge(
                        value(
                                schema,
                                format,
                                GenericMap.of(
                                        new byte[] {1, 2}, BinaryString.fromString("old"),
                                        new byte[] {3, 4}, BinaryString.fromString("kept"))),
                        value(
                                schema,
                                format,
                                GenericMap.of(
                                        new byte[] {1, 2}, BinaryString.fromString("new"),
                                        new byte[] {5, 6}, BinaryString.fromString("added"))));
        InternalMap map = result.row.getMap(1);
        assertThat(map.size()).isEqualTo(3);
        assertThat(binaryMapEntries(map))
                .containsOnly(
                        entry("[1, 2]", "new"), entry("[3, 4]", "kept"), entry("[5, 6]", "added"));

        result =
                merger.merge(result, value(schema, format, GenericMap.of(new byte[] {1, 2}, null)));
        assertThat(result.row.getMap(1).size()).isEqualTo(3);
        assertThat(binaryMapEntries(result.row.getMap(1)))
                .containsOnly(
                        entry("[1, 2]", null), entry("[3, 4]", "kept"), entry("[5, 6]", "added"));
    }

    @ParameterizedTest
    @MethodSource("binaryTypes")
    void testMergeMapKeepsLastStoredValue(KvFormat format, DataType keyType) {
        Schema schema = schema(DataTypes.MAP(keyType, DataTypes.BYTES()), AggFunctions.MERGE_MAP());
        InternalMap old =
                new ColumnarMap(
                        GenericArray.of(new byte[] {1, 2}, new byte[] {1, 2}),
                        GenericArray.of(new byte[] {1}, new byte[] {2}));
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(schema, format, old),
                                value(
                                        schema,
                                        format,
                                        GenericMap.of(new byte[] {3, 4}, new byte[] {3})));
        InternalMap map = result.row.getMap(1);
        assertThat(map.size()).isEqualTo(2);
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            entries.put(
                    Arrays.toString(map.keyArray().getBytes(i)),
                    Arrays.toString(map.valueArray().getBytes(i)));
        }
        assertThat(entries).containsOnly(entry("[1, 2]", "[2]"), entry("[3, 4]", "[3]"));
    }

    @ParameterizedTest
    @MethodSource("binaryTypes")
    void testMergeMapNullValueOverridesStoredBytes(KvFormat format, DataType valueType) {
        Schema schema = schema(DataTypes.MAP(DataTypes.INT(), valueType), AggFunctions.MERGE_MAP());
        BinaryValue result =
                merger(schema, format)
                        .merge(
                                value(schema, format, GenericMap.of(1, new byte[] {1, 2})),
                                value(schema, format, GenericMap.of(1, null)));
        InternalMap map = result.row.getMap(1);
        assertThat(map.size()).isEqualTo(1);
        assertThat(map.keyArray().getInt(0)).isEqualTo(1);
        assertThat(map.valueArray().isNullAt(0)).isTrue();
    }

    static Stream<Arguments> binaryTypes() {
        return Stream.of(KvFormat.values())
                .flatMap(
                        format ->
                                Stream.of(DataTypes.BYTES(), DataTypes.BINARY(2))
                                        .map(type -> Arguments.of(format, type)));
    }

    private static Map<String, String> binaryMapEntries(InternalMap map) {
        Map<String, String> entries = new HashMap<>();
        for (int i = 0; i < map.size(); i++) {
            entries.put(
                    Arrays.toString(map.keyArray().getBytes(i)),
                    map.valueArray().isNullAt(i) ? null : map.valueArray().getString(i).toString());
        }
        return entries;
    }

    private static void assertCollectedValues(
            BinaryValue value, DataType elementType, Object... expected) {
        assertThat(value.row.isNullAt(1)).isFalse();
        assertCollectedValues(value.row.getArray(1), elementType, expected);
    }

    private static void assertCollectedValues(
            InternalArray array, DataType elementType, Object... expected) {
        InternalArray.ElementGetter getter =
                InternalArray.createDeepElementGetter(elementType, true);
        Object[] actual = new Object[array.size()];
        for (int i = 0; i < array.size(); i++) {
            actual[i] = getter.getElementOrNull(array, i);
        }
        assertThat(actual).containsExactly(expected);
    }

    private static Schema schema(DataType type, AggFunction function) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("value", type, function)
                .primaryKey("id")
                .build();
    }

    private static AggregateRowMerger merger(Schema schema, KvFormat format) {
        AggregateRowMerger merger =
                new AggregateRowMerger(
                        new TableConfig(new Configuration()),
                        format,
                        new TestingSchemaGetter(new SchemaInfo(schema, (short) 1)));
        merger.configureTargetColumns(null, (short) 1, schema);
        return merger;
    }

    private static BinaryValue value(Schema schema, KvFormat format, Object field) {
        RowEncoder encoder = RowEncoder.create(format, schema.getRowType());
        encoder.startNewRow();
        encoder.encodeField(0, 1);
        encoder.encodeField(1, field);
        return new BinaryValue((short) 1, encoder.finishRow());
    }
}
