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

package org.apache.fluss.flink.sink;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.flink.sink.shuffle.DistributionMode;
import org.apache.fluss.metadata.DeleteBehavior;
import org.apache.fluss.metadata.MergeEngineType;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkTableSink}. */
class FlinkTableSinkTest {

    @Test
    void testGetChangelogModeIncludesUpdateBeforeForRetractCapableAggregationTable() {
        FlinkTableSink sink =
                createPkTableSink(MergeEngineType.AGGREGATION, /* schemaSupportsRetract= */ true);
        ChangelogMode requestedMode = allKindsChangelogMode();

        ChangelogMode result = sink.getChangelogMode(requestedMode);

        assertThat(result.contains(RowKind.INSERT)).isTrue();
        assertThat(result.contains(RowKind.UPDATE_AFTER)).isTrue();
        assertThat(result.contains(RowKind.DELETE)).isTrue();
        assertThat(result.contains(RowKind.UPDATE_BEFORE)).isTrue();
    }

    @Test
    void testGetChangelogModeExcludesUpdateBeforeForNonRetractAggregationTable() {
        FlinkTableSink sink =
                createPkTableSink(MergeEngineType.AGGREGATION, /* schemaSupportsRetract= */ false);
        ChangelogMode requestedMode = allKindsChangelogMode();

        assertThatThrownBy(() -> sink.getChangelogMode(requestedMode))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
                .hasMessageContaining("cannot consume UPDATE changelog safely");
    }

    @Test
    void testGetChangelogModeExcludesUpdateBeforeForNonAggregationTable() {
        FlinkTableSink sink = createPkTableSink(null, /* schemaSupportsRetract= */ false);
        ChangelogMode requestedMode = allKindsChangelogMode();

        ChangelogMode result = sink.getChangelogMode(requestedMode);

        assertThat(result.contains(RowKind.INSERT)).isTrue();
        assertThat(result.contains(RowKind.UPDATE_AFTER)).isTrue();
        assertThat(result.contains(RowKind.DELETE)).isTrue();
        assertThat(result.contains(RowKind.UPDATE_BEFORE)).isFalse();
    }

    @Test
    void testGetChangelogModeRejectsUpdateBeforeForNonRetractAggregationEvenWithIgnoreDelete() {
        FlinkTableSink sink =
                createPkTableSink(
                        MergeEngineType.AGGREGATION,
                        /* schemaSupportsRetract= */ false,
                        /* sinkIgnoreDelete= */ true);
        ChangelogMode requestedMode = allKindsChangelogMode();

        assertThatThrownBy(() -> sink.getChangelogMode(requestedMode))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
                .hasMessageContaining("cannot consume UPDATE changelog safely");
    }

    /** Creates a PK table FlinkTableSink for changelog mode testing. */
    private static FlinkTableSink createPkTableSink(
            MergeEngineType mergeEngineType, boolean schemaSupportsRetract) {
        return createPkTableSink(mergeEngineType, schemaSupportsRetract, false);
    }

    private static FlinkTableSink createPkTableSink(
            MergeEngineType mergeEngineType,
            boolean schemaSupportsRetract,
            boolean sinkIgnoreDelete) {
        return new FlinkTableSink(
                TablePath.of("test_db", "test_table"),
                new Configuration(),
                org.apache.flink.table.types.logical.RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            new org.apache.flink.table.types.logical.IntType(),
                            new org.apache.flink.table.types.logical.IntType()
                        },
                        new String[] {"id", "value"}),
                new int[] {0}, // primary key on first column
                Collections.emptyList(),
                true, // streaming
                mergeEngineType,
                null, // lakeFormat
                sinkIgnoreDelete,
                DeleteBehavior.IGNORE,
                1, // numBucket
                new ArrayList<>(),
                DistributionMode.BUCKET,
                null, // producerId
                schemaSupportsRetract ? Collections.emptySet() : Collections.singleton("value"));
    }

    /** Builds a ChangelogMode containing all four RowKinds. */
    private static ChangelogMode allKindsChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }
}
