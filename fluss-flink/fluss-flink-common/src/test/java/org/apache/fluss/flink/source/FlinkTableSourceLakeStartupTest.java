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

package org.apache.fluss.flink.source;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.flink.FlinkConnectorOptions;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.lake.source.LakeSource;
import org.apache.fluss.lake.source.TestingLakeSource;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.predicate.LeafPredicate;
import org.apache.fluss.predicate.Predicate;
import org.apache.fluss.row.TimestampLtz;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.shaded.guava31.com.google.common.collect.Maps;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.ScanTableSource.ScanContext;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/** Tests for lake source startup behavior in {@link FlinkTableSource}. */
class FlinkTableSourceLakeStartupTest {

    @Test
    void testTimestampFilter() throws Exception {
        long startupTimestampMs = 12_345L;
        RecordingLakeSource lakeSource = new RecordingLakeSource(true);
        FlinkTableSource tableSource =
                createTableSource(new int[0], timestampStartup(startupTimestampMs));
        setField(tableSource, "lakeSource", lakeSource);

        Source<RowData, ?, ?> source = createSource(tableSource);

        assertThat(getField(source, "lakeSource")).isSameAs(lakeSource);
        assertThat(lakeSource.withFiltersCalls).isOne();
        assertThat(lakeSource.pushedPredicates).hasSize(1);
        LeafPredicate timestampPredicate = (LeafPredicate) lakeSource.pushedPredicates.get(0);
        assertThat(timestampPredicate.fieldName()).isEqualTo("__timestamp");
        assertThat(timestampPredicate.index()).isEqualTo(4);
        assertThat(timestampPredicate.literals())
                .containsExactly(TimestampLtz.fromEpochMillis(startupTimestampMs));
    }

    @Test
    void testPkTableWithoutTimestampFilter() throws Exception {
        RecordingLakeSource lakeSource = new RecordingLakeSource(true);
        FlinkTableSource tableSource = createTableSource(new int[] {0}, timestampStartup(1_000L));
        setField(tableSource, "lakeSource", lakeSource);

        Source<RowData, ?, ?> source = createSource(tableSource);

        assertThat(getField(source, "lakeSource")).isNull();
        assertThat(lakeSource.withFiltersCalls).isZero();
    }

    private static FlinkTableSource createTableSource(
            int[] primaryKeyIndexes, FlinkConnectorOptionsUtils.StartupOptions startupOptions) {
        RowType tableOutputType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.INT()),
                                        DataTypes.FIELD("name", DataTypes.STRING()))
                                .getLogicalType();
        Configuration flussConfig = new Configuration();
        flussConfig.setString(FlinkConnectorOptions.BOOTSTRAP_SERVERS.key(), "localhost:9092");

        return new FlinkTableSource(
                TablePath.of("test_db", "test_table"),
                flussConfig,
                new TableConfig(new Configuration()),
                tableOutputType,
                primaryKeyIndexes,
                new int[0],
                new int[0],
                true,
                startupOptions,
                false,
                false,
                null,
                1_000L,
                false,
                null,
                Maps.newHashMap(),
                null);
    }

    private static FlinkConnectorOptionsUtils.StartupOptions timestampStartup(
            long startupTimestampMs) {
        FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                new FlinkConnectorOptionsUtils.StartupOptions();
        startupOptions.startupMode = FlinkConnectorOptions.ScanStartupMode.TIMESTAMP;
        startupOptions.startupTimestampMs = startupTimestampMs;
        return startupOptions;
    }

    private static Source<RowData, ?, ?> createSource(FlinkTableSource tableSource) {
        ScanRuntimeProvider provider = tableSource.getScanRuntimeProvider(mock(ScanContext.class));
        return ((SourceProvider) provider).createSource();
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static class RecordingLakeSource extends TestingLakeSource {
        private final boolean acceptFilters;
        private int withFiltersCalls;
        private List<Predicate> pushedPredicates = Collections.emptyList();

        private RecordingLakeSource(boolean acceptFilters) {
            this.acceptFilters = acceptFilters;
        }

        @Override
        public LakeSource.FilterPushDownResult withFilters(List<Predicate> predicates) {
            withFiltersCalls++;
            pushedPredicates = new ArrayList<>(predicates);
            if (acceptFilters) {
                return LakeSource.FilterPushDownResult.of(predicates, Collections.emptyList());
            } else {
                return LakeSource.FilterPushDownResult.of(
                        Collections.emptyList(), new ArrayList<>(predicates));
            }
        }
    }
}
