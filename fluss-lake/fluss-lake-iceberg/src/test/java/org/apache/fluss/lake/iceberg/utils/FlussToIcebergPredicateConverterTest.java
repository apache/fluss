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

package org.apache.fluss.lake.iceberg.utils;

import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.row.BinaryString.fromString;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link FlussToIcebergPredicateConverter}. */
class FlussToIcebergPredicateConverterTest {

    private static final PredicateBuilder FLUSS_BUILDER =
            new PredicateBuilder(
                    RowType.builder()
                            .field("f1", DataTypes.BIGINT())
                            .field("f2", DataTypes.DOUBLE())
                            .field("f3", DataTypes.STRING())
                            .build());

    private static final Schema ICEBERG_SCHEMA =
            new Schema(
                    required(1, "f1", Types.LongType.get()),
                    optional(2, "f2", Types.DoubleType.get()),
                    required(3, "f3", Types.StringType.get()));

    public static Stream<Arguments> parameters() {
        // A comprehensive set of test cases for different predicate types.
        return Stream.of(
                // Leaf Predicates
                Arguments.of(FLUSS_BUILDER.equal(0, 12L), Expressions.equal("f1", 12L)),
                Arguments.of(
                        FLUSS_BUILDER.notEqual(2, fromString("test")),
                        Expressions.notEqual("f3", "test")),
                Arguments.of(
                        FLUSS_BUILDER.greaterThan(1, 99.9d), Expressions.greaterThan("f2", 99.9d)),
                Arguments.of(
                        FLUSS_BUILDER.greaterOrEqual(0, 100L),
                        Expressions.greaterThanOrEqual("f1", 100L)),
                Arguments.of(FLUSS_BUILDER.lessThan(1, 0.1d), Expressions.lessThan("f2", 0.1d)),
                Arguments.of(
                        FLUSS_BUILDER.lessOrEqual(0, 50L), Expressions.lessThanOrEqual("f1", 50L)),
                Arguments.of(FLUSS_BUILDER.isNull(2), Expressions.isNull("f3")),
                Arguments.of(FLUSS_BUILDER.isNotNull(1), Expressions.notNull("f2")),
                Arguments.of(
                        FLUSS_BUILDER.in(
                                2,
                                Stream.of("a", "b", "c")
                                        .map(BinaryString::fromString)
                                        .collect(Collectors.toList())),
                        Stream.of("a", "b", "c")
                                .map(s -> (Expression) Expressions.equal("f3", s))
                                .reduce(Expressions::or)
                                .get()),
                Arguments.of(
                        FLUSS_BUILDER.in(
                                2,
                                Stream.of(
                                                "a", "b", "c", "a", "b", "c", "a", "b", "c", "a",
                                                "b", "c", "a", "b", "c", "a", "b", "c", "a", "b",
                                                "c", "a", "b", "c")
                                        .map(BinaryString::fromString)
                                        .collect(Collectors.toList())),
                        Expressions.in(
                                "f3",
                                Arrays.asList(
                                        "a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b", "c",
                                        "a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b",
                                        "c"))),
                Arguments.of(
                        FLUSS_BUILDER.notIn(
                                2,
                                Stream.of(
                                                "a", "b", "c", "a", "b", "c", "a", "b", "c", "a",
                                                "b", "c", "a", "b", "c", "a", "b", "c", "a", "b",
                                                "c", "a", "b", "c")
                                        .map(BinaryString::fromString)
                                        .collect(Collectors.toList())),
                        Expressions.notIn(
                                "f3",
                                Arrays.asList(
                                        "a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b", "c",
                                        "a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b",
                                        "c"))),
                Arguments.of(
                        FLUSS_BUILDER.startsWith(2, fromString("start")),
                        Expressions.startsWith("f3", "start")),

                // Compound Predicates
                Arguments.of(
                        PredicateBuilder.and(
                                FLUSS_BUILDER.equal(0, 1L), FLUSS_BUILDER.isNotNull(2)),
                        Expressions.and(Expressions.equal("f1", 1L), Expressions.notNull("f3"))),
                Arguments.of(
                        PredicateBuilder.or(
                                FLUSS_BUILDER.lessThan(1, 10.0),
                                FLUSS_BUILDER.greaterThan(1, 100.0)),
                        Expressions.or(
                                Expressions.lessThan("f2", 10.0),
                                Expressions.greaterThan("f2", 100.0))),

                // Nested Predicate
                Arguments.of(
                        PredicateBuilder.and(
                                FLUSS_BUILDER.equal(2, fromString("test")),
                                PredicateBuilder.or(
                                        FLUSS_BUILDER.equal(0, 1L),
                                        FLUSS_BUILDER.greaterThan(1, 50.0))),
                        Expressions.and(
                                Expressions.equal("f3", "test"),
                                Expressions.or(
                                        Expressions.equal("f1", 1L),
                                        Expressions.greaterThan("f2", 50.0)))));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testPredicateConverter(Predicate flussPredicate, Expression expectedPredicate) {
        Expression convertedIcebergExpression =
                FlussToIcebergPredicateConverter.convert(ICEBERG_SCHEMA, flussPredicate).get();
        assertThat(convertedIcebergExpression.toString()).isEqualTo(expectedPredicate.toString());
    }

    public static Stream<Arguments> parametersNotSupported() {
        return Stream.of(
                Arguments.of(FLUSS_BUILDER.endsWith(2, fromString("end"))),
                Arguments.of(FLUSS_BUILDER.contains(2, fromString("mid"))));
    }

    @ParameterizedTest
    @MethodSource("parametersNotSupported")
    void testNotSupportedPredicateConverter(Predicate flussPredicate) {
        assertThat(FlussToIcebergPredicateConverter.convert(ICEBERG_SCHEMA, flussPredicate))
                .isEqualTo(Optional.empty());
    }
}
