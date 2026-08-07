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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.record.BinaryValue;
import org.apache.fluss.record.TestingSchemaGetter;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.server.utils.RoaringBitmapUtils;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.testutils.DataTestUtils.compactedRow;
import static org.assertj.core.api.Assertions.assertThat;

/** Contract tests between server aggregation semantics and idempotence classification. */
class AggregateRowMergerIdempotenceTest {

    private static final short SCHEMA_ID = (short) 1;

    private static final SemanticComparator LONG_EQUALITY =
            (left, right) ->
                    haveSameNullState(left, right)
                            && (left.isNullAt(1) || left.getLong(1) == right.getLong(1));

    private static final SemanticComparator INT_EQUALITY =
            (left, right) ->
                    haveSameNullState(left, right)
                            && (left.isNullAt(1) || left.getInt(1) == right.getInt(1));

    private static final SemanticComparator STRING_EQUALITY =
            (left, right) ->
                    haveSameNullState(left, right)
                            && (left.isNullAt(1) || left.getString(1).equals(right.getString(1)));

    private static final SemanticComparator BOOLEAN_EQUALITY =
            (left, right) ->
                    haveSameNullState(left, right)
                            && (left.isNullAt(1) || left.getBoolean(1) == right.getBoolean(1));

    @ParameterizedTest(name = "{0}")
    @MethodSource("aggregationIdempotenceCases")
    void testAggregationSemanticsMatchIdempotenceClassification(
            AggFunctionType functionType,
            Schema schema,
            Configuration configuration,
            Object committedValue,
            List<Object> dirtySequence,
            SemanticComparator semanticComparator)
            throws IOException {
        AggregateRowMerger merger = createMerger(schema, configuration);
        BinaryValue committed = value(schema, committedValue);

        BinaryValue once = apply(merger, schema, committed, dirtySequence);
        BinaryValue twice = apply(merger, schema, once, dirtySequence);

        assertThat(semanticComparator.areEqual(once.row, twice.row))
                .as("semantic idempotence for %s", functionType)
                .isEqualTo(functionType.isIdempotent());
    }

    @Test
    void testContractCasesCoverEveryAggregationFunction() throws IOException {
        Set<AggFunctionType> coveredTypes =
                aggregationIdempotenceCases()
                        .map(arguments -> (AggFunctionType) arguments.get()[0])
                        .collect(
                                Collectors.toCollection(
                                        () -> EnumSet.noneOf(AggFunctionType.class)));

        assertThat(coveredTypes).containsExactlyInAnyOrder(AggFunctionType.values());
    }

    static Stream<Arguments> aggregationIdempotenceCases() throws IOException {
        return Stream.of(
                Arguments.of(
                        AggFunctionType.SUM,
                        schema(AggFunctionType.SUM, DataTypes.BIGINT()),
                        new Configuration(),
                        10L,
                        Arrays.<Object>asList(2L, 3L),
                        LONG_EQUALITY),
                Arguments.of(
                        AggFunctionType.PRODUCT,
                        schema(AggFunctionType.PRODUCT, DataTypes.BIGINT()),
                        new Configuration(),
                        2L,
                        Arrays.<Object>asList(3L, 5L),
                        LONG_EQUALITY),
                Arguments.of(
                        AggFunctionType.MAX,
                        schema(AggFunctionType.MAX, DataTypes.INT()),
                        new Configuration(),
                        10,
                        Arrays.<Object>asList(8, 20, 15),
                        INT_EQUALITY),
                Arguments.of(
                        AggFunctionType.MIN,
                        schema(AggFunctionType.MIN, DataTypes.INT()),
                        new Configuration(),
                        10,
                        Arrays.<Object>asList(12, 3, 7),
                        INT_EQUALITY),
                Arguments.of(
                        AggFunctionType.LAST_VALUE,
                        schema(AggFunctionType.LAST_VALUE, DataTypes.STRING()),
                        new Configuration(),
                        "committed",
                        Arrays.<Object>asList("tail", null),
                        STRING_EQUALITY),
                Arguments.of(
                        AggFunctionType.LAST_VALUE_IGNORE_NULLS,
                        schema(AggFunctionType.LAST_VALUE_IGNORE_NULLS, DataTypes.STRING()),
                        new Configuration(),
                        "committed",
                        Arrays.<Object>asList(null, "tail", null),
                        STRING_EQUALITY),
                Arguments.of(
                        AggFunctionType.FIRST_VALUE,
                        schema(AggFunctionType.FIRST_VALUE, DataTypes.STRING()),
                        new Configuration(),
                        null,
                        Arrays.<Object>asList("later", null),
                        STRING_EQUALITY),
                Arguments.of(
                        AggFunctionType.FIRST_VALUE_IGNORE_NULLS,
                        schema(AggFunctionType.FIRST_VALUE_IGNORE_NULLS, DataTypes.STRING()),
                        new Configuration(),
                        null,
                        Arrays.<Object>asList(null, "first", "later"),
                        STRING_EQUALITY),
                Arguments.of(
                        AggFunctionType.LISTAGG,
                        schema(
                                AggFunctionType.LISTAGG,
                                DataTypes.STRING(),
                                Collections.singletonMap(AggFunctions.PARAM_DELIMITER, "|")),
                        new Configuration(),
                        "committed",
                        Arrays.<Object>asList("dirty-1", "dirty-2"),
                        STRING_EQUALITY),
                Arguments.of(
                        AggFunctionType.STRING_AGG,
                        schema(
                                AggFunctionType.STRING_AGG,
                                DataTypes.STRING(),
                                Collections.singletonMap(AggFunctions.PARAM_DELIMITER, ";")),
                        new Configuration(),
                        "committed",
                        Arrays.<Object>asList("dirty-1", "dirty-2"),
                        STRING_EQUALITY),
                Arguments.of(
                        AggFunctionType.BOOL_AND,
                        schema(AggFunctionType.BOOL_AND, DataTypes.BOOLEAN()),
                        new Configuration(),
                        true,
                        Arrays.<Object>asList(true, false, true),
                        BOOLEAN_EQUALITY),
                Arguments.of(
                        AggFunctionType.BOOL_OR,
                        schema(AggFunctionType.BOOL_OR, DataTypes.BOOLEAN()),
                        new Configuration(),
                        false,
                        Arrays.<Object>asList(false, true, false),
                        BOOLEAN_EQUALITY),
                Arguments.of(
                        AggFunctionType.RBM32,
                        schema(AggFunctionType.RBM32, DataTypes.BYTES()),
                        new Configuration(),
                        bitmap32(1, 2),
                        Arrays.<Object>asList(bitmap32(2, 3), bitmap32(4)),
                        (SemanticComparator) AggregateRowMergerIdempotenceTest::haveEqualRbm32Sets),
                Arguments.of(
                        AggFunctionType.RBM64,
                        schema(AggFunctionType.RBM64, DataTypes.BYTES()),
                        new Configuration(),
                        bitmap64(10L, 20L),
                        Arrays.<Object>asList(bitmap64(20L, 30L), bitmap64(40L)),
                        (SemanticComparator)
                                AggregateRowMergerIdempotenceTest::haveEqualRbm64Sets));
    }

    private static AggregateRowMerger createMerger(Schema schema, Configuration configuration) {
        TableConfig tableConfig = new TableConfig(configuration);
        TestingSchemaGetter schemaGetter =
                new TestingSchemaGetter(new SchemaInfo(schema, SCHEMA_ID));
        AggregateRowMerger merger =
                new AggregateRowMerger(tableConfig, tableConfig.getKvFormat(), schemaGetter);
        merger.configureTargetColumns(null, SCHEMA_ID, schema);
        return merger;
    }

    private static BinaryValue apply(
            AggregateRowMerger merger,
            Schema schema,
            BinaryValue committed,
            List<Object> dirtySequence) {
        BinaryValue result = committed;
        for (Object dirtyValue : dirtySequence) {
            result = merger.merge(result, value(schema, dirtyValue));
        }
        return result;
    }

    private static BinaryValue value(Schema schema, Object aggregateValue) {
        BinaryRow row = compactedRow(schema.getRowType(), new Object[] {1, aggregateValue});
        return new BinaryValue(SCHEMA_ID, row);
    }

    private static Schema schema(AggFunctionType functionType, DataType dataType) {
        return schema(functionType, dataType, Collections.emptyMap());
    }

    private static Schema schema(
            AggFunctionType functionType, DataType dataType, Map<String, String> parameters) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("value", dataType, AggFunctions.of(functionType, parameters))
                .primaryKey("id")
                .build();
    }

    private static boolean haveSameNullState(BinaryRow left, BinaryRow right) {
        return left.isNullAt(1) == right.isNullAt(1);
    }

    private static boolean haveEqualRbm32Sets(BinaryRow left, BinaryRow right) throws IOException {
        RoaringBitmap leftBitmap = new RoaringBitmap();
        RoaringBitmap rightBitmap = new RoaringBitmap();
        RoaringBitmapUtils.deserializeRoaringBitmap32(leftBitmap, left.getBytes(1));
        RoaringBitmapUtils.deserializeRoaringBitmap32(rightBitmap, right.getBytes(1));
        return leftBitmap.equals(rightBitmap);
    }

    private static boolean haveEqualRbm64Sets(BinaryRow left, BinaryRow right) throws IOException {
        Roaring64Bitmap leftBitmap = new Roaring64Bitmap();
        Roaring64Bitmap rightBitmap = new Roaring64Bitmap();
        RoaringBitmapUtils.deserializeRoaringBitmap64(leftBitmap, left.getBytes(1));
        RoaringBitmapUtils.deserializeRoaringBitmap64(rightBitmap, right.getBytes(1));
        return leftBitmap.equals(rightBitmap);
    }

    private static byte[] bitmap32(int... values) throws IOException {
        RoaringBitmap bitmap = new RoaringBitmap();
        bitmap.add(values);
        return RoaringBitmapUtils.serializeRoaringBitmap32(bitmap);
    }

    private static byte[] bitmap64(long... values) throws IOException {
        Roaring64Bitmap bitmap = new Roaring64Bitmap();
        for (long value : values) {
            bitmap.add(value);
        }
        return RoaringBitmapUtils.serializeRoaringBitmap64(bitmap);
    }

    @FunctionalInterface
    private interface SemanticComparator {
        boolean areEqual(BinaryRow left, BinaryRow right) throws IOException;
    }
}
