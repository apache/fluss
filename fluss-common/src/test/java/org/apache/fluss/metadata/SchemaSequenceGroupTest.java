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

package org.apache.fluss.metadata;

import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema-level rejection tests for sequence groups. These checks are decided from the schema alone,
 * so they run inside {@link Schema.Builder#build()}. Table-level rejections (merge engine, log
 * table) still live in {@code SequenceGroupValidationTest}.
 */
class SchemaSequenceGroupTest {

    private static Schema.Builder pkSchema() {
        return Schema.newBuilder().column("k", DataTypes.INT()).column("a", DataTypes.STRING());
    }

    private static Stream<DataType> supportedSequenceTypes() {
        return Stream.of(
                DataTypes.INT(),
                DataTypes.BIGINT(),
                DataTypes.TIMESTAMP(),
                DataTypes.TIMESTAMP_LTZ());
    }

    @ParameterizedTest
    @MethodSource("supportedSequenceTypes")
    void testSupportedSequenceColumnTypeIsAccepted(DataType sequenceType) {
        assertThatCode(
                        () ->
                                pkSchema()
                                        .column("g", sequenceType)
                                        .sequenceGroup(singletonList("g"), singletonList("a"))
                                        .primaryKey("k")
                                        .build())
                .doesNotThrowAnyException();
    }

    @Test
    void testEveryColumnOfACompositeSequenceKeyIsChecked() {
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .column("g1", DataTypes.INT())
                                        // only the trailing column has an unsupported type
                                        .column("g2", DataTypes.STRING())
                                        .sequenceGroup(asList("g1", "g2"), singletonList("a"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The sequence column 'g2' must be one type of "
                                + "[INT, BIGINT, TIMESTAMP, TIMESTAMP_LTZ], but got STRING");
    }

    @Test
    void testUnknownSequenceColumnIsRejected() {
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .sequenceGroup(singletonList("missing"), singletonList("a"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The sequence column 'missing' doesn't exist in schema.");
    }

    @Test
    void testUnknownProtectedColumnIsRejected() {
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .column("g", DataTypes.INT())
                                        .sequenceGroup(singletonList("g"), singletonList("missing"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The protected column 'missing' doesn't exist in schema.");
    }

    @Test
    void testSequenceColumnWithAggregateFunctionIsRejected() {
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .column(
                                                "g",
                                                DataTypes.INT(),
                                                AggFunctions.of(AggFunctionType.SUM))
                                        .sequenceGroup(singletonList("g"), singletonList("a"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The sequence column 'g' orders a sequence group, "
                                + "so it must not have an aggregate function.");
    }

    @Test
    void testPrimaryKeyColumnInSequenceGroupIsRejected() {
        // a primary key holds the same value in both rows being merged, so a group can neither
        // arbitrate it nor be ordered by it
        assertThatThrownBy(
                        () ->
                                Schema.newBuilder()
                                        .column("k", DataTypes.INT())
                                        .column("g", DataTypes.INT())
                                        .sequenceGroup(singletonList("g"), singletonList("k"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The primary key column 'k' must not be put in a sequence group.");
    }

    @Test
    void testPrimaryKeyAsSequenceColumnIsRejected() {
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .sequenceGroup(singletonList("k"), singletonList("a"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("The sequence column 'k' must not be a primary key column.");
    }

    @Test
    void testSequenceColumnProtectedByAnotherGroupIsRejected() {
        // a sequence column reports the order of its own group, so following another one would
        // leave it out of step with the columns it orders
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .column("pay_time", DataTypes.TIMESTAMP())
                                        .column("ship_time", DataTypes.TIMESTAMP())
                                        .sequenceGroup(
                                                singletonList("pay_time"), singletonList("a"))
                                        .sequenceGroup(
                                                singletonList("ship_time"),
                                                singletonList("pay_time"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The sequence column 'pay_time' orders a sequence group, "
                                + "so it must not be put into another one.");
    }

    @Test
    void testRepeatedSequenceColumnWithinGroupIsRejected() {
        // naming a column twice as a sequence column degenerates the comparison key, so the intent
        // is always a typo
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .column("g", DataTypes.INT())
                                        .sequenceGroup(asList("g", "g"), singletonList("a"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The sequence column 'g' is declared more than once in the same sequence group.");
    }

    @Test
    void testRepeatedProtectedColumnWithinGroupIsRejected() {
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .column("g", DataTypes.INT())
                                        .sequenceGroup(singletonList("g"), asList("a", "a"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The protected column 'a' is declared more than once in the same sequence group.");
    }

    @Test
    void testSequenceColumnSharedByTwoGroupsIsRejected() {
        // a sequence column shared by two groups would define two different orders; this also
        // rejects two groups with identical sequence columns, which are the same group twice.
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .column("b", DataTypes.STRING())
                                        .column("g", DataTypes.INT())
                                        .sequenceGroup(singletonList("g"), singletonList("a"))
                                        .sequenceGroup(singletonList("g"), singletonList("b"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The sequence column 'g' must not be shared by more than one sequence group.");
    }

    @Test
    void testColumnProtectedByTwoGroupsIsRejected() {
        assertThatThrownBy(
                        () ->
                                pkSchema()
                                        .column("g1", DataTypes.INT())
                                        .column("g2", DataTypes.INT())
                                        .sequenceGroup(singletonList("g1"), singletonList("a"))
                                        .sequenceGroup(singletonList("g2"), singletonList("a"))
                                        .primaryKey("k")
                                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "The column 'a' must not be protected by more than one sequence group.");
    }
}
