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

package org.apache.fluss.server.utils;

import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.exception.InvalidConfigException;
import org.apache.fluss.metadata.AggFunctionType;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the sequence group part of {@link TableDescriptorValidation}, which rejects at table
 * creation what would otherwise be silently ignored or fail while merging.
 */
class SequenceGroupValidationTest {

    private static Schema.Builder pkSchema() {
        return Schema.newBuilder().column("k", DataTypes.INT()).column("a", DataTypes.STRING());
    }

    /** A schema whose {@code a} is ordered by a {@code g} column of the given type. */
    private static Schema orderedByG(DataType sequenceType) {
        return pkSchema()
                .withSequenceColumns("g")
                .column("g", sequenceType)
                .primaryKey("k")
                .build();
    }

    private static void validate(Schema schema) {
        validate(schema, null);
    }

    private static void validate(Schema schema, MergeEngineType mergeEngine) {
        TableDescriptor.Builder builder =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property(ConfigOptions.TABLE_REPLICATION_FACTOR, 1);
        if (mergeEngine != null) {
            builder.property(ConfigOptions.TABLE_MERGE_ENGINE, mergeEngine);
        }
        TableDescriptorValidation.validateTableDescriptor(builder.build(), 1024, null);
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
        assertThatCode(() -> validate(orderedByG(sequenceType))).doesNotThrowAnyException();
    }

    @Test
    void testSchemaWithoutSequenceGroupIsNotAffected() {
        Schema schema = pkSchema().primaryKey("k").build();

        assertThatCode(() -> validate(schema)).doesNotThrowAnyException();
        // a merge engine is only rejected together with a sequence group
        assertThatCode(() -> validate(schema, MergeEngineType.FIRST_ROW))
                .doesNotThrowAnyException();
    }

    @Test
    void testEveryColumnOfACompositeSequenceKeyIsChecked() {
        Schema schema =
                pkSchema()
                        .withSequenceColumns("g1", "g2")
                        .column("g1", DataTypes.INT())
                        // only the trailing column has an unsupported type
                        .column("g2", DataTypes.STRING())
                        .primaryKey("k")
                        .build();

        assertThatThrownBy(() -> validate(schema))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "The sequence column 'g2' must be one type of "
                                + "[INT, BIGINT, TIMESTAMP, TIMESTAMP_LTZ], but got STRING");
    }

    @Test
    void testUnknownSequenceColumnIsRejected() {
        Schema schema = pkSchema().withSequenceColumns("missing").primaryKey("k").build();

        assertThatThrownBy(() -> validate(schema))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("The sequence column 'missing' doesn't exist in schema.");
    }

    @Test
    void testLogTableIsRejected() {
        // nothing consults the sequence groups when merging, as there is no merging at all
        Schema schema = pkSchema().withSequenceColumns("g").column("g", DataTypes.INT()).build();

        assertThatThrownBy(() -> validate(schema))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("Sequence group is only supported in primary key table.");
    }

    @ParameterizedTest
    @EnumSource(
            value = MergeEngineType.class,
            names = {"AGGREGATION"},
            mode = EnumSource.Mode.EXCLUDE)
    void testMergeEngineWithoutSequenceGroupSupportIsRejected(MergeEngineType mergeEngine) {
        assertThatThrownBy(() -> validate(orderedByG(DataTypes.INT()), mergeEngine))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        String.format(
                                "Sequence group is not supported for '%s' merge engine.",
                                mergeEngine));
    }

    @Test
    void testAggregationMergeEngineIsAccepted() {
        // the aggregation engine reads the groups as an ordering key rather than a version filter,
        // so it takes part in the arbitration instead of rejecting it
        assertThatCode(() -> validate(orderedByG(DataTypes.INT()), MergeEngineType.AGGREGATION))
                .doesNotThrowAnyException();
    }

    @Test
    void testSequenceColumnWithAggregateFunctionIsRejected() {
        Schema schema =
                pkSchema()
                        .withSequenceColumns("g")
                        .column("g", DataTypes.INT(), AggFunctions.of(AggFunctionType.SUM))
                        .primaryKey("k")
                        .build();

        assertThatThrownBy(() -> validate(schema, MergeEngineType.AGGREGATION))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "The sequence column 'g' orders a sequence group, "
                                + "so it must not have an aggregate function.");
    }

    @Test
    void testPrimaryKeyColumnInSequenceGroupIsRejected() {
        // a primary key holds the same value in both rows being merged, so a group can neither
        // arbitrate it nor be ordered by it
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .withSequenceColumns("g")
                        .column("g", DataTypes.INT())
                        .primaryKey("k")
                        .build();

        assertThatThrownBy(() -> validate(schema))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "The primary key column 'k' must not be put in a sequence group.");
    }

    @Test
    void testPrimaryKeyAsSequenceColumnIsRejected() {
        Schema schema = pkSchema().withSequenceColumns("k").primaryKey("k").build();

        assertThatThrownBy(() -> validate(schema))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining("The sequence column 'k' must not be a primary key column.");
    }

    @Test
    void testSequenceColumnProtectedByAnotherGroupIsRejected() {
        // a sequence column reports the order of its own group, so following another one would
        // leave it out of step with the columns it orders
        Schema schema =
                pkSchema()
                        .withSequenceColumns("pay_time")
                        .column("pay_time", DataTypes.TIMESTAMP())
                        .withSequenceColumns("ship_time")
                        .column("ship_time", DataTypes.TIMESTAMP())
                        .primaryKey("k")
                        .build();

        assertThatThrownBy(() -> validate(schema))
                .isInstanceOf(InvalidConfigException.class)
                .hasMessageContaining(
                        "The sequence column 'pay_time' orders a sequence group, "
                                + "so it must not be put into another one.");
    }
}
