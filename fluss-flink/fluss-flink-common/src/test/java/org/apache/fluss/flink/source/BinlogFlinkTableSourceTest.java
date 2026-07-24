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
import org.apache.fluss.flink.utils.BinlogRowConverter;
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

import java.util.Arrays;
import java.util.Collections;

import static org.apache.fluss.flink.FlinkConnectorOptions.ScanStartupMode;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link BinlogFlinkTableSource} projection and filter pushdown. */
class BinlogFlinkTableSourceTest {

    // Data columns: (id INT, name STRING, amount BIGINT)
    private static final RowType DATA_COLUMNS_TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.INT()),
                                    DataTypes.FIELD("name", DataTypes.STRING()),
                                    DataTypes.FIELD("amount", DataTypes.BIGINT()))
                            .getLogicalType();

    private BinlogFlinkTableSource createSource() {
        FlinkConnectorOptionsUtils.StartupOptions startupOptions =
                new FlinkConnectorOptionsUtils.StartupOptions();
        startupOptions.startupMode = ScanStartupMode.EARLIEST;
        return new BinlogFlinkTableSource(
                TablePath.of("db", "t"),
                new Configuration(),
                BinlogRowConverter.buildBinlogRowType(DATA_COLUMNS_TYPE),
                false,
                true,
                startupOptions,
                1000L,
                Collections.emptyMap());
    }

    private DataType projectedType(int... topLevelIndexes) {
        RowType full = BinlogRowConverter.buildBinlogRowType(DATA_COLUMNS_TYPE);
        DataTypes.Field[] fields = new DataTypes.Field[topLevelIndexes.length];
        for (int i = 0; i < topLevelIndexes.length; i++) {
            RowType.RowField f = full.getFields().get(topLevelIndexes[i]);
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

    @Test
    void testApplyProjectionStoresTopLevel() {
        BinlogFlinkTableSource source = createSource();
        // virtual [_change_type(0), after(4)]
        source.applyProjection(nested(0, 4), projectedType(0, 4));

        assertThat(source.getProjectedTopLevel()).containsExactly(0, 4);
        assertThat(source.getProducedDataType()).isEqualTo(projectedType(0, 4).getLogicalType());
    }

    @Test
    void testCopyPreservesPushdownState() {
        BinlogFlinkTableSource source = createSource();
        source.applyProjection(nested(3, 4), projectedType(3, 4));

        BinlogFlinkTableSource copy = (BinlogFlinkTableSource) source.copy();
        assertThat(copy.getProjectedTopLevel()).containsExactly(3, 4);
        assertThat(copy.getProducedDataType()).isEqualTo(source.getProducedDataType());
    }

    @Test
    void testApplyFiltersReturnsAllRemaining() {
        BinlogFlinkTableSource source = createSource();
        FieldReferenceExpression ref =
                new FieldReferenceExpression("_change_type", DataTypes.STRING(), 0, 0);
        ValueLiteralExpression lit =
                new ValueLiteralExpression("insert", DataTypes.STRING().notNull());
        ResolvedExpression filter =
                CallExpression.permanent(
                        BuiltInFunctionDefinitions.EQUALS,
                        Arrays.asList(ref, lit),
                        DataTypes.BOOLEAN());

        SupportsFilterPushDown.Result result =
                source.applyFilters(Collections.singletonList(filter));

        // Binlog data/partition columns are nested and metadata is non-pushable: nothing accepted.
        assertThat(result.getAcceptedFilters()).isEmpty();
        assertThat(result.getRemainingFilters()).containsExactly(filter);
    }
}
