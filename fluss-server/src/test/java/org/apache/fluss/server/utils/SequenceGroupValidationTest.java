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
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Table-level rejection tests for sequence groups. These rejections depend on the merge engine or
 * the table type (log vs primary key), so they live in {@link TableDescriptorValidation}. The
 * schema-level rejections (existence, types, cross-group relations) are covered by {@code
 * SchemaSequenceGroupTest}.
 */
class SequenceGroupValidationTest {

    /** A primary-key schema whose {@code a} is ordered by {@code g}. */
    private static Schema orderedByG() {
        return Schema.newBuilder()
                .column("k", DataTypes.INT())
                .column("a", DataTypes.STRING())
                .column("g", DataTypes.INT())
                .sequenceGroup(singletonList("g"), singletonList("a"))
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

    @Test
    void testSchemaWithoutSequenceGroupIsNotAffected() {
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("a", DataTypes.STRING())
                        .primaryKey("k")
                        .build();

        assertThatCode(() -> validate(schema)).doesNotThrowAnyException();
        // a merge engine is only rejected together with a sequence group
        assertThatCode(() -> validate(schema, MergeEngineType.FIRST_ROW))
                .doesNotThrowAnyException();
    }

    @Test
    void testLogTableIsRejected() {
        // nothing consults the sequence groups when merging, as there is no merging at all. Since
        // a log table has no primary key, we build the schema without one.
        Schema schema =
                Schema.newBuilder()
                        .column("k", DataTypes.INT())
                        .column("a", DataTypes.STRING())
                        .column("g", DataTypes.INT())
                        .sequenceGroup(singletonList("g"), singletonList("a"))
                        .build();

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
        assertThatThrownBy(() -> validate(orderedByG(), mergeEngine))
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
        assertThatCode(() -> validate(orderedByG(), MergeEngineType.AGGREGATION))
                .doesNotThrowAnyException();
    }
}
