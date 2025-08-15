/*
 * Copyright (c) 2025 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.lake.paimon.utils;

import com.alibaba.fluss.predicate.Predicate;
import com.alibaba.fluss.predicate.PredicateBuilder;
import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.types.RowType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlussToPaimonPredicateConverter}. */
class FlussToPaimonPredicateConverterTest {

    private static final PredicateBuilder FLUSS_BUILDER =
            new PredicateBuilder(
                    RowType.builder()
                            .field("f1", DataTypes.BIGINT())
                            .field("f2", DataTypes.DOUBLE())
                            .field("f3", DataTypes.STRING())
                            .build());

    private static final org.apache.paimon.types.RowType PAIMON_ROW_TYPE =
            org.apache.paimon.types.RowType.builder()
                    .field("f1", org.apache.paimon.types.DataTypes.BIGINT())
                    .field("f2", org.apache.paimon.types.DataTypes.DOUBLE())
                    .field("f3", org.apache.paimon.types.DataTypes.STRING())
                    .build();

    private static final org.apache.paimon.predicate.PredicateBuilder PAIMON_BUILDER =
            new org.apache.paimon.predicate.PredicateBuilder(PAIMON_ROW_TYPE);

    public static Stream<Arguments> parameters() {
        // A comprehensive set of test cases for different predicate types.
        return Stream.of(
                // Leaf Predicates
                Arguments.of(FLUSS_BUILDER.equal(0, 12L), PAIMON_BUILDER.equal(0, 12L)),
                Arguments.of(
                        FLUSS_BUILDER.notEqual(2, fromString("test")),
                        PAIMON_BUILDER.notEqual(2, "test")),
                Arguments.of(
                        FLUSS_BUILDER.greaterThan(1, 99.9d), PAIMON_BUILDER.greaterThan(1, 99.9d)),
                Arguments.of(
                        FLUSS_BUILDER.greaterOrEqual(0, 100L),
                        PAIMON_BUILDER.greaterOrEqual(0, 100L)),
                Arguments.of(FLUSS_BUILDER.lessThan(1, 0.1d), PAIMON_BUILDER.lessThan(1, 0.1d)),
                Arguments.of(FLUSS_BUILDER.lessOrEqual(0, 50L), PAIMON_BUILDER.lessOrEqual(0, 50L)),
                Arguments.of(FLUSS_BUILDER.isNull(2), PAIMON_BUILDER.isNull(2)),
                Arguments.of(FLUSS_BUILDER.isNotNull(1), PAIMON_BUILDER.isNotNull(1)),
                Arguments.of(
                        FLUSS_BUILDER.in(
                                2,
                                Stream.of("a", "b", "c")
                                        .map(BinaryString::fromString)
                                        .collect(Collectors.toList())),
                        PAIMON_BUILDER.in(2, Arrays.asList("a", "b", "c"))),
                Arguments.of(
                        FLUSS_BUILDER.in(
                                2,
                                Stream.of(
                                                "a", "b", "c", "a", "b", "c", "a", "b", "c", "a",
                                                "b", "c", "a", "b", "c", "a", "b", "c", "a", "b",
                                                "c", "a", "b", "c")
                                        .map(BinaryString::fromString)
                                        .collect(Collectors.toList())),
                        PAIMON_BUILDER.in(
                                2,
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
                        PAIMON_BUILDER.notIn(
                                2,
                                Arrays.asList(
                                        "a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b", "c",
                                        "a", "b", "c", "a", "b", "c", "a", "b", "c", "a", "b",
                                        "c"))),
                Arguments.of(
                        FLUSS_BUILDER.startsWith(2, fromString("start")),
                        PAIMON_BUILDER.startsWith(2, "start")),
                Arguments.of(
                        FLUSS_BUILDER.endsWith(2, fromString("end")),
                        PAIMON_BUILDER.endsWith(2, "end")),
                Arguments.of(
                        FLUSS_BUILDER.contains(2, fromString("mid")),
                        PAIMON_BUILDER.contains(2, "mid")),

                // Compound Predicates
                Arguments.of(
                        PredicateBuilder.and(
                                FLUSS_BUILDER.equal(0, 1L), FLUSS_BUILDER.isNotNull(2)),
                        org.apache.paimon.predicate.PredicateBuilder.and(
                                PAIMON_BUILDER.equal(0, 1L), PAIMON_BUILDER.isNotNull(2))),
                Arguments.of(
                        PredicateBuilder.or(
                                FLUSS_BUILDER.lessThan(1, 10.0),
                                FLUSS_BUILDER.greaterThan(1, 100.0)),
                        org.apache.paimon.predicate.PredicateBuilder.or(
                                PAIMON_BUILDER.lessThan(1, 10.0),
                                PAIMON_BUILDER.greaterThan(1, 100.0))),

                // Nested Predicate
                Arguments.of(
                        PredicateBuilder.and(
                                FLUSS_BUILDER.equal(2, fromString("test")),
                                PredicateBuilder.or(
                                        FLUSS_BUILDER.equal(0, 1L),
                                        FLUSS_BUILDER.greaterThan(1, 50.0))),
                        org.apache.paimon.predicate.PredicateBuilder.and(
                                PAIMON_BUILDER.equal(2, "test"),
                                org.apache.paimon.predicate.PredicateBuilder.or(
                                        PAIMON_BUILDER.equal(0, 1L),
                                        PAIMON_BUILDER.greaterThan(1, 50.0)))));
    }

    @ParameterizedTest
    @MethodSource("parameters")
    void testPredicateConverter(
            Predicate flussPredicate, org.apache.paimon.predicate.Predicate expectedPredicate) {
        org.apache.paimon.predicate.Predicate convertedPaimonPredicate =
                FlussToPaimonPredicateConverter.convert(PAIMON_ROW_TYPE, flussPredicate).get();
        assertThat(convertedPaimonPredicate.toString()).isEqualTo(expectedPredicate.toString());
    }
}
