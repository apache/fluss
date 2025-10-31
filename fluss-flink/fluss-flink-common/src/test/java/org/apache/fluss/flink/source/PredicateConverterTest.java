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

package org.apache.fluss.flink.source;

import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.BinaryString;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.ApiExpressionUtils;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.paimon.format.SimpleColStats;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link PredicateConverter}. */
public class PredicateConverterTest {

    private static final PredicateBuilder BUILDER =
            new PredicateBuilder(
                    new org.apache.fluss.types.RowType(
                            Arrays.asList(
                                    new org.apache.fluss.types.DataField(
                                            "long1", org.apache.fluss.types.DataTypes.BIGINT()),
                                    new org.apache.fluss.types.DataField(
                                            "double1", org.apache.fluss.types.DataTypes.DOUBLE()),
                                    new org.apache.fluss.types.DataField(
                                            "string1",
                                            org.apache.fluss.types.DataTypes.STRING()))));

    private static final PredicateConverter CONVERTER = new PredicateConverter(BUILDER);

    @MethodSource("provideResolvedExpression")
    @ParameterizedTest
    public void testVisitAndAutoTypeInference(ResolvedExpression expression, Predicate expected) {
        if (expression instanceof CallExpression) {
            assertThat(CONVERTER.visit((CallExpression) expression).toString())
                    .isEqualTo(expected.toString());
        } else {
            assertThatThrownBy(() -> CONVERTER.visit(expression))
                    .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
        }
    }

    public static Stream<Arguments> provideResolvedExpression() {
        FieldReferenceExpression longRefExpr =
                new FieldReferenceExpression(
                        "long1", DataTypes.BIGINT(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        ValueLiteralExpression intLitExpr = new ValueLiteralExpression(10);
        ValueLiteralExpression intLitExpr2 = new ValueLiteralExpression(20);
        long longLit = 10L;
        ValueLiteralExpression nullLongLitExpr =
                new ValueLiteralExpression(null, DataTypes.BIGINT());

        FieldReferenceExpression doubleRefExpr =
                new FieldReferenceExpression(
                        "double1", DataTypes.DOUBLE(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        ValueLiteralExpression floatLitExpr = new ValueLiteralExpression(3.14f);
        double doubleLit = 3.14d;

        FieldReferenceExpression stringRefExpr =
                new FieldReferenceExpression(
                        "string1", DataTypes.STRING(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        String stringLit = "haha";
        // same type
        ValueLiteralExpression stringLitExpr1 =
                new ValueLiteralExpression(stringLit, DataTypes.STRING().notNull());
        // different type, char(4)
        ValueLiteralExpression stringLitExpr2 = new ValueLiteralExpression(stringLit);

        return Stream.of(
                Arguments.of(longRefExpr, null),
                Arguments.of(intLitExpr, null),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IS_NULL,
                                Collections.singletonList(longRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.isNull(0)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IS_NOT_NULL,
                                Collections.singletonList(doubleRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.isNotNull(1)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                // test literal on left
                                Arrays.asList(intLitExpr, longRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.equal(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                Arrays.asList(nullLongLitExpr, longRefExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.equal(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.NOT_EQUALS,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.notEqual(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.NOT_EQUALS,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.notEqual(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.greaterThan(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.greaterThan(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.greaterOrEqual(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.greaterOrEqual(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.lessThan(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.lessThan(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, intLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.lessOrEqual(0, longLit)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                Arrays.asList(longRefExpr, nullLongLitExpr),
                                DataTypes.BOOLEAN()),
                        BUILDER.lessOrEqual(0, null)),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.AND,
                                Arrays.asList(
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                                                Arrays.asList(longRefExpr, intLitExpr),
                                                DataTypes.BOOLEAN()),
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.EQUALS,
                                                Arrays.asList(doubleRefExpr, floatLitExpr),
                                                DataTypes.BOOLEAN())),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.and(
                                BUILDER.lessOrEqual(0, longLit), BUILDER.equal(1, doubleLit))),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.OR,
                                Arrays.asList(
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.NOT_EQUALS,
                                                Arrays.asList(longRefExpr, intLitExpr),
                                                DataTypes.BOOLEAN()),
                                        CallExpression.permanent(
                                                BuiltInFunctionDefinitions.EQUALS,
                                                Arrays.asList(doubleRefExpr, floatLitExpr),
                                                DataTypes.BOOLEAN())),
                                DataTypes.BOOLEAN()),
                        PredicateBuilder.or(
                                BUILDER.notEqual(0, longLit), BUILDER.equal(1, doubleLit))),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.IN,
                                Arrays.asList(
                                        longRefExpr, intLitExpr, nullLongLitExpr, intLitExpr2),
                                DataTypes.BOOLEAN()),
                        BUILDER.in(0, Arrays.asList(10L, null, 20L))),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                Arrays.asList(stringLitExpr1, stringRefExpr),
                                DataTypes.STRING()),
                        BUILDER.equal(2, BinaryString.fromString("haha"))),
                Arguments.of(
                        CallExpression.permanent(
                                BuiltInFunctionDefinitions.EQUALS,
                                Arrays.asList(stringLitExpr2, stringRefExpr),
                                DataTypes.STRING()),
                        BUILDER.equal(2, BinaryString.fromString("haha"))));
    }

    public static Stream<Arguments> provideLikeExpressions() {
        CallExpression expr1 =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("abd%", STRING()));
        List<Object[]> valuesList1 =
                Arrays.asList(
                        new Object[] {null},
                        new Object[] {BinaryString.fromString("a")},
                        new Object[] {BinaryString.fromString("ab")},
                        new Object[] {BinaryString.fromString("abd")},
                        new Object[] {BinaryString.fromString("abd%")},
                        new Object[] {BinaryString.fromString("abd1")},
                        new Object[] {BinaryString.fromString("abde@")},
                        new Object[] {BinaryString.fromString("abd_")},
                        new Object[] {BinaryString.fromString("abd_%")});
        List<Boolean> expectedForValues1 =
                Arrays.asList(false, false, false, true, true, true, true, true, true);
        List<Long> rowCountList1 = Arrays.asList(0L, 3L, 3L, 3L);
        List<SimpleColStats[]> statsList1 =
                Arrays.asList(
                        new SimpleColStats[] {new SimpleColStats(null, null, 0L)},
                        new SimpleColStats[] {new SimpleColStats(null, null, 3L)},
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("ab"),
                                    BinaryString.fromString("abc123"),
                                    1L)
                        },
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("abc"),
                                    BinaryString.fromString("abe"),
                                    1L)
                        });
        List<Boolean> expectedForStats1 = Arrays.asList(false, false, false, true);

        CallExpression expr2 =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("test=_%", STRING()),
                        literal("=", STRING()));
        List<Object[]> valuesList2 =
                Arrays.asList(
                        new Object[] {BinaryString.fromString("test%")},
                        new Object[] {BinaryString.fromString("test_123")},
                        new Object[] {BinaryString.fromString("test_%")},
                        new Object[] {BinaryString.fromString("test__")});
        List<Boolean> expectedForValues2 = Arrays.asList(false, true, true, true);
        List<Long> rowCountList2 = Collections.singletonList(3L);
        List<SimpleColStats[]> statsList2 =
                Collections.singletonList(
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("test_123"),
                                    BinaryString.fromString("test_789"),
                                    0L)
                        });
        List<Boolean> expectedForStats2 = Collections.singletonList(true);

        // currently, SQL wildcards '[]' and '[^]' are deemed as normal characters in Flink
        CallExpression expr3 =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("[a-c]xyz%", STRING()));
        List<Object[]> valuesList3 =
                Arrays.asList(
                        new Object[] {BinaryString.fromString("axyz")},
                        new Object[] {BinaryString.fromString("bxyz")},
                        new Object[] {BinaryString.fromString("cxyz")},
                        new Object[] {BinaryString.fromString("[a-c]xyz")});
        List<Boolean> expectedForValues3 = Arrays.asList(false, false, false, true);
        List<Long> rowCountList3 = Collections.singletonList(3L);
        List<SimpleColStats[]> statsList3 =
                Collections.singletonList(
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("[a-c]xyz"),
                                    BinaryString.fromString("[a-c]xyzz"),
                                    0L)
                        });
        List<Boolean> expectedForStats3 = Collections.singletonList(true);

        CallExpression expr4 =
                call(
                        BuiltInFunctionDefinitions.LIKE,
                        field(0, STRING()),
                        literal("[^a-d]xyz%", STRING()));
        List<Object[]> valuesList4 =
                Arrays.asList(
                        new Object[] {BinaryString.fromString("exyz")},
                        new Object[] {BinaryString.fromString("fxyz")},
                        new Object[] {BinaryString.fromString("axyz")},
                        new Object[] {BinaryString.fromString("[^a-d]xyz")});
        List<Boolean> expectedForValues4 = Arrays.asList(false, false, false, true);
        List<Long> rowCountList4 = Collections.singletonList(3L);
        List<SimpleColStats[]> statsList4 =
                Collections.singletonList(
                        new SimpleColStats[] {
                            new SimpleColStats(
                                    BinaryString.fromString("[^a-d]xyz"),
                                    BinaryString.fromString("[^a-d]xyzz"),
                                    1L)
                        });
        List<Boolean> expectedForStats4 = Collections.singletonList(true);

        return Stream.of(
                Arguments.of(
                        expr1,
                        valuesList1,
                        expectedForValues1,
                        rowCountList1,
                        statsList1,
                        expectedForStats1),
                Arguments.of(
                        expr2,
                        valuesList2,
                        expectedForValues2,
                        rowCountList2,
                        statsList2,
                        expectedForStats2),
                Arguments.of(
                        expr3,
                        valuesList3,
                        expectedForValues3,
                        rowCountList3,
                        statsList3,
                        expectedForStats3),
                Arguments.of(
                        expr4,
                        valuesList4,
                        expectedForValues4,
                        rowCountList4,
                        statsList4,
                        expectedForStats4));
    }

    @Test
    public void testUnsupportedExpression() {
        CallExpression expression =
                call(
                        BuiltInFunctionDefinitions.AND,
                        call(
                                BuiltInFunctionDefinitions.EQUALS,
                                field(0, DataTypes.INT()),
                                literal(3)),
                        call(
                                BuiltInFunctionDefinitions.SIMILAR,
                                field(1, DataTypes.INT()),
                                literal(5)));
        assertThatThrownBy(
                        () ->
                                expression.accept(
                                        new PredicateConverter(
                                                RowType.of(new IntType(), new IntType()))))
                .isInstanceOf(PredicateConverter.UnsupportedExpression.class);
    }

    private static FieldReferenceExpression field(int i, DataType type) {
        return new FieldReferenceExpression("f" + i, type, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    private static CallExpression call(FunctionDefinition function, ResolvedExpression... args) {
        return new CallExpression(false, null, function, Arrays.asList(args), DataTypes.BOOLEAN());
    }

    public static ValueLiteralExpression literal(Object value) {
        return ApiExpressionUtils.valueLiteral(value);
    }

    public static ValueLiteralExpression literal(Object value, DataType type) {
        return value != null
                ? ApiExpressionUtils.valueLiteral(value, (DataType) type.notNull())
                : ApiExpressionUtils.valueLiteral((Object) null, (DataType) type.nullable());
    }
}
