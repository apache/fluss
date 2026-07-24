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
import org.apache.fluss.flink.utils.ChangelogRowConverter;
import org.apache.fluss.flink.utils.FlinkConnectorOptionsUtils;
import org.apache.fluss.metadata.TablePath;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.fluss.flink.FlinkConnectorOptions.ScanStartupMode;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link ChangelogFlinkTableSource} projection and filter pushdown. */
class ChangelogFlinkTableSourceTest {

    // Data columns: (id INT, name STRING, amount BIGINT)
    private static final RowType DATA_COLUMNS_TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.INT()),
                                    DataTypes.FIELD("name", DataTypes.STRING()),
                                    DataTypes.FIELD("amount", DataTypes.BIGINT()))
                            .getLogicalType();

    private ChangelogFlinkTableSource createSource(int[] partitionKeyIndexes) {
        return createSource(partitionKeyIndexes, Collections.emptyMap());
    }

    private ChangelogFlinkTableSource createSource(
            int[] partitionKeyIndexes, Map<String, String> extraOptions) {
        FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                new FlinkConnectorOptionsUtils.StartupOptions();
        startupOptions.startupMode = ScanStartupMode.EARLIEST;
        Map<String, String> tableOptions = new HashMap<>(extraOptions);
        return new ChangelogFlinkTableSource(
                TablePath.of("db", "t"),
                new Configuration(),
                ChangelogRowConverter.buildChangelogRowType(DATA_COLUMNS_TYPE),
                partitionKeyIndexes,
                true,
                startupOptions,
                1000L,
                tableOptions);
    }

    private DataType projectedType(int... virtualIndexes) {
        RowType full = ChangelogRowConverter.buildChangelogRowType(DATA_COLUMNS_TYPE);
        DataTypes.Field[] fields = new DataTypes.Field[virtualIndexes.length];
        for (int i = 0; i < virtualIndexes.length; i++) {
            RowType.RowField f = full.getFields().get(virtualIndexes[i]);
            fields[i] =
                    DataTypes.FIELD(
                            f.getName(),
                            org.apache.flink.table.types.utils.TypeConversions
                                    .fromLogicalToDataType(f.getType()));
        }
        return DataTypes.ROW(fields);
    }

    private int[][] nested(int... indexes) {
        int[][] result = new int[indexes.length][];
        for (int i = 0; i < indexes.length; i++) {
            result[i] = new int[] {indexes[i]};
        }
        return result;
    }

    private CallExpression equals(String field, DataType fieldType, Object value) {
        return call(BuiltInFunctionDefinitions.EQUALS, field, fieldType, value);
    }

    private CallExpression call(
            org.apache.flink.table.functions.BuiltInFunctionDefinition func,
            String field,
            DataType fieldType,
            Object value) {
        FieldReferenceExpression ref = new FieldReferenceExpression(field, fieldType, 0, 0);
        ValueLiteralExpression lit = new ValueLiteralExpression(value, fieldType.notNull());
        return CallExpression.permanent(func, Arrays.asList(ref, lit), DataTypes.BOOLEAN());
    }

    @ParameterizedTest
    @MethodSource("projectionCases")
    void testApplyProjection(
            String description,
            int[] virtual,
            int[] expectedDataProjection,
            int[] expectedBaseProjection) {
        ChangelogFlinkTableSource source = createSource(new int[0]);
        source.applyProjection(nested(virtual), projectedType(virtual));

        assertThat(source.getProjectedFields()).containsExactly(virtual);

        // dataProjection null for metadata-only projection
        if (expectedDataProjection == null) {
            assertThat(source.getDataProjection()).isNull();
        } else {
            assertThat(source.getDataProjection()).containsExactly(expectedDataProjection);
        }

        assertThat(source.getBaseRowProjection()).containsExactly(expectedBaseProjection);
        assertThat(source.getProducedDataType()).isEqualTo(projectedType(virtual).getLogicalType());
    }

    private static Stream<Arguments> projectionCases() {
        return Stream.of(
            // description, virtual, expectedDataProjection, expectedBaseProjection
            // metadata only
            Arguments.of("metadata-only", new int[] {0}, null, new int[] {0}),
            // data only
            Arguments.of("data-only", new int[] {3, 5}, new int[] {0, 2}, new int[] {3, 4}),
            // reordered mix
            Arguments.of(
                    "reorderedMix", new int[] {0, 5, 3}, new int[] {2, 0}, new int[] {0, 3, 4}));
    }

    @Test
    void testCopyPreservesPushdownState() {
        ChangelogFlinkTableSource source = createSource(new int[0], statsAll());
        source.applyProjection(nested(0, 3), projectedType(0, 3));
        source.applyFilters(Collections.singletonList(equals("amount", DataTypes.BIGINT(), 100L)));

        ChangelogFlinkTableSource copy = (ChangelogFlinkTableSource) source.copy();
        assertThat(copy.getProjectedFields()).containsExactly(0, 3);
        assertThat(copy.getDataProjection()).containsExactly(0);
        assertThat(copy.getBaseRowProjection()).containsExactly(0, 3);
        assertThat(copy.getProducedDataType()).isEqualTo(source.getProducedDataType());
        assertThat(copy.getLogRecordBatchFilter()).isEqualTo(source.getLogRecordBatchFilter());
    }

    @Test
    void testApplyFiltersDataColumnPushed() {
        ChangelogFlinkTableSource source = createSource(new int[0], statsAll());
        ResolvedExpression filter = equals("amount", DataTypes.BIGINT(), 100L);

        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).containsExactly(filter);
        // All filters are returned as remaining (safety net).
        assertThat(result.getRemainingFilters()).containsExactly(filter);
        assertThat(source.getLogRecordBatchFilter()).isNotNull();
    }

    @Test
    void testApplyFiltersDataColumnNotInStatisticsNotPushed() {
        // Statistics are collected only for "amount". A filter on "name" cannot use batch
        // statistics, so it must not be pushed as a record-batch filter even though it is a
        // convertible data-column predicate.
        ChangelogFlinkTableSource source = createSource(new int[0], statsColumns("amount"));
        ResolvedExpression filter = equals("name", DataTypes.STRING(), "p1");

        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(result.getRemainingFilters()).containsExactly(filter);
        assertThat(source.getLogRecordBatchFilter()).isNull();
    }

    @Test
    void testApplyFiltersCompoundDataColumnsPushed() {
        // WHERE amount > 100 AND amount < 500: both leaves reference a statistics-backed data
        // column, so the whole AND predicate is pushed as a single record-batch filter.
        ChangelogFlinkTableSource source = createSource(new int[0], statsAll());
        ResolvedExpression compound =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.AND,
                        Arrays.asList(
                                call(
                                        BuiltInFunctionDefinitions.GREATER_THAN,
                                        "amount",
                                        DataTypes.BIGINT(),
                                        100L),
                                call(
                                        BuiltInFunctionDefinitions.LESS_THAN,
                                        "amount",
                                        DataTypes.BIGINT(),
                                        500L)),
                        DataTypes.BOOLEAN());

        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(compound));

        assertThat(result.getAcceptedFilters()).containsExactly(compound);
        assertThat(result.getRemainingFilters()).containsExactly(compound);
        assertThat(source.getLogRecordBatchFilter()).isNotNull();
    }

    @Test
    void testApplyFiltersMetadataColumnNotPushed() {
        ChangelogFlinkTableSource source = createSource(new int[0], statsAll());
        ResolvedExpression filter = equals("_change_type", DataTypes.STRING(), "insert");

        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(result.getRemainingFilters()).containsExactly(filter);
        assertThat(source.getLogRecordBatchFilter()).isNull();
    }

    @Test
    void testApplyFiltersPartitionKeyPushed() {
        // partition key = data column "name" (index 1)
        ChangelogFlinkTableSource source = createSource(new int[] {1});
        ResolvedExpression filter = equals("name", DataTypes.STRING(), "p1");

        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        assertThat(result.getAcceptedFilters()).containsExactly(filter);
        assertThat(result.getRemainingFilters()).containsExactly(filter);
        assertThat(source.getPartitionFilters()).isNotNull();
    }

    @Test
    void testApplyFiltersNonPartitionKeyNotPushedToPartitionFilters() {
        // partition key = data column "name" (index 1); the filter references the non-partition
        // column "amount", so it must not create a partition filter and must stay a post-filter.
        ChangelogFlinkTableSource source = createSource(new int[] {1});
        ResolvedExpression filter = equals("amount", DataTypes.BIGINT(), 100L);

        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        // Statistics are disabled by default, so the filter is not accepted as a batch filter
        // either.
        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(result.getRemainingFilters()).containsExactly(filter);
        assertThat(source.getPartitionFilters()).isNull();
        assertThat(source.getLogRecordBatchFilter()).isNull();
    }

    @Test
    void testApplyFiltersCompoundPartitionAndNonPartitionNotPushed() {
        // partition key = data column "name" (index 1). A compound filter referencing both a
        // partition column and a non-partition column cannot be used for partition pruning as a
        // whole; it must remain a post-filter so no condition is silently dropped.
        ChangelogFlinkTableSource source = createSource(new int[] {1});
        ResolvedExpression compound =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.AND,
                        Arrays.asList(
                                equals("name", DataTypes.STRING(), "p1"),
                                equals("amount", DataTypes.BIGINT(), 100L)),
                        DataTypes.BOOLEAN());

        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(compound));

        // Statistics are disabled by default, so nothing is accepted at all.
        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(result.getRemainingFilters()).containsExactly(compound);
        assertThat(source.getPartitionFilters()).isNull();
        assertThat(source.getLogRecordBatchFilter()).isNull();
    }

    private Map<String, String> statsAll() {
        return statsColumns("*");
    }

    private Map<String, String> statsColumns(String columns) {
        Map<String, String> options = new HashMap<>();
        options.put("table.statistics.columns", columns);
        return options;
    }
}
