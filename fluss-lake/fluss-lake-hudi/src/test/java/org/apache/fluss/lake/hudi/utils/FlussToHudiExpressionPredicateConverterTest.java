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

package org.apache.fluss.lake.hudi.utils;

import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.predicate.PredicateBuilder;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.apache.hudi.org.apache.avro.Schema;
import org.apache.hudi.source.ExpressionPredicates;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FlussToHudiExpressionPredicateConverter}. */
class FlussToHudiExpressionPredicateConverterTest {

    private static final PredicateBuilder PREDICATE_BUILDER =
            new PredicateBuilder(
                    RowType.builder()
                            .field("id", DataTypes.BIGINT())
                            .field("name", DataTypes.STRING())
                            .field("score", DataTypes.DOUBLE())
                            .field("event_time", DataTypes.TIMESTAMP(3))
                            .field("payload", DataTypes.BYTES())
                            .field("amount", DataTypes.DECIMAL(10, 2))
                            .build());

    public static Stream<Arguments> supportedPredicates() {
        return Stream.of(
                Arguments.of(PREDICATE_BUILDER.equal(0, 12L), "id = 12"),
                Arguments.of(
                        PREDICATE_BUILDER.notEqual(1, BinaryString.fromString("test")),
                        "name != test"),
                Arguments.of(PREDICATE_BUILDER.lessThan(2, 10.5D), "score < 10.5"),
                Arguments.of(PREDICATE_BUILDER.lessOrEqual(0, 50L), "id <= 50"),
                Arguments.of(PREDICATE_BUILDER.greaterThan(2, 100.5D), "score > 100.5"),
                Arguments.of(PREDICATE_BUILDER.greaterOrEqual(0, 100L), "id >= 100"),
                Arguments.of(
                        PREDICATE_BUILDER.lessOrEqual(3, TimestampNtz.fromMillis(1000L)),
                        "event_time <= 1000"),
                Arguments.of(
                        PREDICATE_BUILDER.in(0, longValues(21)),
                        "id IN([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, "
                                + "10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20])"),
                Arguments.of(
                        PREDICATE_BUILDER.notIn(0, longValues(21)),
                        "NOT(id IN([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, "
                                + "10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]))"));
    }

    @ParameterizedTest
    @MethodSource("supportedPredicates")
    void testSupportedPredicates(Predicate flussPredicate, String expectedPredicate) {
        Optional<ExpressionPredicates.Predicate> convertedPredicate =
                FlussToHudiExpressionPredicateConverter.convert(hudiSchema(), flussPredicate);

        assertThat(convertedPredicate).isPresent();
        assertThat(convertedPredicate.get().toString()).isEqualTo(expectedPredicate);
    }

    public static Stream<Arguments> unsupportedPredicates() {
        return Stream.of(
                Arguments.of(PREDICATE_BUILDER.isNull(1)),
                Arguments.of(PREDICATE_BUILDER.isNotNull(1)),
                Arguments.of(PREDICATE_BUILDER.startsWith(1, BinaryString.fromString("prefix"))),
                Arguments.of(PREDICATE_BUILDER.endsWith(1, BinaryString.fromString("suffix"))),
                Arguments.of(PREDICATE_BUILDER.contains(1, BinaryString.fromString("part"))),
                Arguments.of(PREDICATE_BUILDER.equal(0, null)),
                Arguments.of(PREDICATE_BUILDER.equal(5, 12.34D)));
    }

    @ParameterizedTest
    @MethodSource("unsupportedPredicates")
    void testUnsupportedPredicates(Predicate flussPredicate) {
        assertThat(FlussToHudiExpressionPredicateConverter.convert(hudiSchema(), flussPredicate))
                .isEqualTo(Optional.empty());
    }

    @ParameterizedTest
    @MethodSource("compoundPredicates")
    void testCompoundPredicates(Predicate flussPredicate, String[] expectedReferences) {
        Optional<ExpressionPredicates.Predicate> convertedPredicate =
                FlussToHudiExpressionPredicateConverter.convert(hudiSchema(), flussPredicate);

        assertThat(convertedPredicate).isPresent();
        assertThat(convertedPredicate.get().references()).containsExactly(expectedReferences);
    }

    public static Stream<Arguments> compoundPredicates() {
        return Stream.of(
                Arguments.of(
                        PredicateBuilder.and(
                                PREDICATE_BUILDER.equal(0, 1L),
                                PREDICATE_BUILDER.equal(1, BinaryString.fromString("one")),
                                PREDICATE_BUILDER.greaterThan(2, 10.0D)),
                        new String[] {"id", "name", "score"}),
                Arguments.of(
                        PredicateBuilder.or(
                                PREDICATE_BUILDER.equal(0, 1L),
                                PREDICATE_BUILDER.greaterThan(2, 10.0D)),
                        new String[] {"id", "score"}));
    }

    private static Schema hudiSchema() {
        return new Schema.Parser()
                .parse(
                        "{"
                                + "\"type\":\"record\","
                                + "\"name\":\"hudi_record\","
                                + "\"fields\":["
                                + "{\"name\":\"_hoodie_commit_time\",\"type\":\"string\"},"
                                + "{\"name\":\"_hoodie_commit_seqno\",\"type\":\"string\"},"
                                + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\"},"
                                + "{\"name\":\"_hoodie_partition_path\",\"type\":\"string\"},"
                                + "{\"name\":\"_hoodie_file_name\",\"type\":\"string\"},"
                                + "{\"name\":\"id\",\"type\":\"long\"},"
                                + "{\"name\":\"name\",\"type\":\"string\"},"
                                + "{\"name\":\"score\",\"type\":\"double\"},"
                                + "{\"name\":\"event_time\","
                                + "\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
                                + "{\"name\":\"payload\",\"type\":\"bytes\"},"
                                + "{\"name\":\"amount\","
                                + "\"type\":{\"type\":\"bytes\","
                                + "\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}},"
                                + "{\"name\":\"__bucket\",\"type\":\"int\"},"
                                + "{\"name\":\"__offset\",\"type\":\"long\"},"
                                + "{\"name\":\"__timestamp\",\"type\":\"long\"}"
                                + "]"
                                + "}");
    }

    private static java.util.List<Object> longValues(int size) {
        return IntStream.range(0, size)
                .mapToObj(i -> (Object) (long) i)
                .collect(Collectors.toList());
    }
}
