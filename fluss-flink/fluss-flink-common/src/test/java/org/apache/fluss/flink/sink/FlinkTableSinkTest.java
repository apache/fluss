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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkTableSink}. */
class FlinkTableSinkTest {

    @ParameterizedTest
    @CsvSource({"true, false", "false, false", "true, true", "false, true"})
    void testDefaultModePreservesUpsertChangelog(boolean supportsRetract, boolean ignoreDelete) {
        FlinkTableSink sink =
                createPkTableSink(MergeEngineType.AGGREGATION, supportsRetract, ignoreDelete);
        ChangelogMode result = sink.getChangelogMode(ChangelogMode.all());
        assertThat(result.getContainedKinds())
                .containsExactlyInAnyOrder(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE);
    }

    @Test
    void testGetChangelogModeExcludesUpdateBeforeForNonAggregationTable() {
        FlinkTableSink sink = createPkTableSink(null, false);
        assertThat(sink.getChangelogMode(ChangelogMode.all()).getContainedKinds())
                .containsExactlyInAnyOrder(RowKind.INSERT, RowKind.UPDATE_AFTER, RowKind.DELETE);
    }

    @Test
    void testRetractModeValidatesActualTargetFields() {
        FlinkTableSink sink =
                createPkTableSink(MergeEngineType.AGGREGATION, false, false, true, false);
        assertThat(sink.getChangelogMode(ChangelogMode.all())).isEqualTo(ChangelogMode.all());
        assertThatCode(() -> sink.getSinkRuntimeProvider(new TargetContext(new int[][] {{0}, {1}})))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> sink.getSinkRuntimeProvider(new TargetContext(null)))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class)
                .hasMessageContaining("other");
        assertThatThrownBy(() -> sink.copy().getSinkRuntimeProvider(new TargetContext(null)))
                .isInstanceOf(org.apache.flink.table.api.ValidationException.class);
    }

    @Test
    void testRetractModeAllowsAppendInputWithNonRetractableFields() {
        FlinkTableSink sink =
                createPkTableSink(MergeEngineType.AGGREGATION, false, false, true, false);
        sink.getChangelogMode(ChangelogMode.insertOnly());
        assertThatCode(() -> sink.getSinkRuntimeProvider(new TargetContext(null)))
                .doesNotThrowAnyException();
    }

    @Test
    void testIgnoreDeleteRetainsLegacyNegotiationWhenRetractEnabled() {
        FlinkTableSink sink =
                createPkTableSink(MergeEngineType.AGGREGATION, false, true, true, false);
        assertThat(sink.getChangelogMode(ChangelogMode.all()).contains(RowKind.UPDATE_BEFORE))
                .isFalse();
        assertThatCode(() -> sink.getSinkRuntimeProvider(new TargetContext(null)))
                .doesNotThrowAnyException();
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
        return createPkTableSink(
                mergeEngineType, schemaSupportsRetract, sinkIgnoreDelete, false, false);
    }

    private static FlinkTableSink createPkTableSink(
            MergeEngineType mergeEngineType,
            boolean schemaSupportsRetract,
            boolean sinkIgnoreDelete,
            boolean enableRetract,
            boolean removeRecordOnDelete) {
        return new FlinkTableSink(
                TablePath.of("test_db", "test_table"),
                new Configuration(),
                org.apache.flink.table.types.logical.RowType.of(
                        new org.apache.flink.table.types.logical.LogicalType[] {
                            new org.apache.flink.table.types.logical.IntType(),
                            new org.apache.flink.table.types.logical.IntType(),
                            new org.apache.flink.table.types.logical.IntType()
                        },
                        new String[] {"id", "value", "other"}),
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
                schemaSupportsRetract ? Collections.emptySet() : Collections.singleton("other"),
                enableRetract,
                removeRecordOnDelete);
    }

    private static class TargetContext implements DynamicTableSink.Context {
        private final int[][] targets;

        private TargetContext(int[][] targets) {
            this.targets = targets;
        }

        @Override
        public boolean isBounded() {
            return false;
        }

        @Override
        public <T> TypeInformation<T> createTypeInformation(DataType type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> TypeInformation<T> createTypeInformation(LogicalType type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DynamicTableSink.DataStructureConverter createDataStructureConverter(DataType type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<int[][]> getTargetColumns() {
            return Optional.ofNullable(targets);
        }
    }
}
