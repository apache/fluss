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

package org.apache.fluss.server.kv.rowmerger;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldAggregator;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldBoolAndAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldFirstNonNullValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldFirstValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldLastNonNullValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldLastValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldListaggAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldMaxAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldMinAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldPrimaryKeyAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldProductAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.FieldSumAgg;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AggregatorHelper}. */
class AggregatorHelperTest {

    @Test
    void testPrimaryKeyFieldsNotAggregated() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames(), tableConfig);

        // First field is primary key, should use FieldPrimaryKeyAgg
        assertThat(aggregators[0]).isInstanceOf(FieldPrimaryKeyAgg.class);
        // Second field should use default (last_non_null_value)
        assertThat(aggregators[1]).isInstanceOf(FieldLastNonNullValueAgg.class);
    }

    @Test
    void testFieldSpecificAggregateFunction() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("count", DataTypes.BIGINT())
                        .column("total", DataTypes.DOUBLE())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.count", "sum");
        conf.setString("table.merge-engine.aggregate.total", "sum");
        TableConfig tableConfig = new TableConfig(conf);

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames(), tableConfig);

        assertThat(aggregators[0]).isInstanceOf(FieldPrimaryKeyAgg.class);
        assertThat(aggregators[1]).isInstanceOf(FieldSumAgg.class);
        assertThat(aggregators[2]).isInstanceOf(FieldSumAgg.class);
    }

    @Test
    void testDefaultAggregateFunction() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value1", DataTypes.INT())
                        .column("value2", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.default-function", "max");
        TableConfig tableConfig = new TableConfig(conf);

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames(), tableConfig);

        assertThat(aggregators[0]).isInstanceOf(FieldPrimaryKeyAgg.class);
        // Both non-primary-key fields should use the default (max)
        assertThat(aggregators[1]).isInstanceOf(FieldMaxAgg.class);
        assertThat(aggregators[2]).isInstanceOf(FieldMaxAgg.class);
    }

    @Test
    void testFieldSpecificOverridesDefault() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value1", DataTypes.INT())
                        .column("value2", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.default-function", "max");
        conf.setString("table.merge-engine.aggregate.value1", "min");
        TableConfig tableConfig = new TableConfig(conf);

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames(), tableConfig);

        assertThat(aggregators[0]).isInstanceOf(FieldPrimaryKeyAgg.class);
        // value1 has specific config, should use min
        assertThat(aggregators[1]).isInstanceOf(FieldMinAgg.class);
        // value2 uses default, should use max
        assertThat(aggregators[2]).isInstanceOf(FieldMaxAgg.class);
    }

    @Test
    void testMultipleAggregatorTypes() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("sum_val", DataTypes.BIGINT())
                        .column("max_val", DataTypes.INT())
                        .column("min_val", DataTypes.INT())
                        .column("product_val", DataTypes.DOUBLE())
                        .column("last_val", DataTypes.STRING())
                        .column("first_val", DataTypes.STRING())
                        .column("bool_and_val", DataTypes.BOOLEAN())
                        .column("listagg_val", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.sum_val", "sum");
        conf.setString("table.merge-engine.aggregate.max_val", "max");
        conf.setString("table.merge-engine.aggregate.min_val", "min");
        conf.setString("table.merge-engine.aggregate.product_val", "product");
        conf.setString("table.merge-engine.aggregate.last_val", "last_value");
        conf.setString("table.merge-engine.aggregate.first_val", "first_value");
        conf.setString("table.merge-engine.aggregate.bool_and_val", "bool_and");
        conf.setString("table.merge-engine.aggregate.listagg_val", "listagg");
        TableConfig tableConfig = new TableConfig(conf);

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames(), tableConfig);

        assertThat(aggregators[0]).isInstanceOf(FieldPrimaryKeyAgg.class);
        assertThat(aggregators[1]).isInstanceOf(FieldSumAgg.class);
        assertThat(aggregators[2]).isInstanceOf(FieldMaxAgg.class);
        assertThat(aggregators[3]).isInstanceOf(FieldMinAgg.class);
        assertThat(aggregators[4]).isInstanceOf(FieldProductAgg.class);
        assertThat(aggregators[5]).isInstanceOf(FieldLastValueAgg.class);
        assertThat(aggregators[6]).isInstanceOf(FieldFirstValueAgg.class);
        assertThat(aggregators[7]).isInstanceOf(FieldBoolAndAgg.class);
        assertThat(aggregators[8]).isInstanceOf(FieldListaggAgg.class);
    }

    @Test
    void testCompositePrimaryKey() {
        Schema schema =
                Schema.newBuilder()
                        .column("id1", DataTypes.INT())
                        .column("id2", DataTypes.STRING())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id1", "id2")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.value", "sum");
        TableConfig tableConfig = new TableConfig(conf);

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames(), tableConfig);

        // Both primary key fields should not be aggregated
        assertThat(aggregators[0]).isInstanceOf(FieldPrimaryKeyAgg.class);
        assertThat(aggregators[1]).isInstanceOf(FieldPrimaryKeyAgg.class);
        assertThat(aggregators[2]).isInstanceOf(FieldSumAgg.class);
    }

    @Test
    void testFirstNonNullValueAggregator() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.value", "first_non_null_value");
        TableConfig tableConfig = new TableConfig(conf);

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames(), tableConfig);

        assertThat(aggregators[0]).isInstanceOf(FieldPrimaryKeyAgg.class);
        assertThat(aggregators[1]).isInstanceOf(FieldFirstNonNullValueAgg.class);
    }

    @Test
    void testNoConfigurationUsesLastNonNullValue() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.STRING())
                        .primaryKey("id")
                        .build();

        // No configuration at all
        TableConfig tableConfig = new TableConfig(new Configuration());

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames(), tableConfig);

        assertThat(aggregators[0]).isInstanceOf(FieldPrimaryKeyAgg.class);
        // Should default to last_non_null_value
        assertThat(aggregators[1]).isInstanceOf(FieldLastNonNullValueAgg.class);
    }

    @Test
    void testInvalidAggregateFunctionThrowsException() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey("id")
                        .build();

        Configuration conf = new Configuration();
        conf.setString("table.merge-engine.aggregate.value", "invalid_function");
        TableConfig tableConfig = new TableConfig(conf);

        assertThatThrownBy(
                        () ->
                                AggregatorHelper.createAggregators(
                                        schema.getRowType(),
                                        schema.getPrimaryKeyColumnNames(),
                                        tableConfig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported aggregation function");
    }

    @Test
    void testAllFieldsCount() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value1", DataTypes.INT())
                        .column("value2", DataTypes.INT())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(
                        schema.getRowType(), schema.getPrimaryKeyColumnNames(), tableConfig);

        // Should have one aggregator per field
        assertThat(aggregators).hasSize(3);
    }

    @Test
    void testEmptyPrimaryKeys() {
        RowType rowType =
                RowType.of(
                        new DataType[] {DataTypes.INT(), DataTypes.STRING()},
                        new String[] {"value1", "value2"});

        TableConfig tableConfig = new TableConfig(new Configuration());

        FieldAggregator[] aggregators =
                AggregatorHelper.createAggregators(rowType, Arrays.asList(), tableConfig);

        // All fields should use default aggregator (last_non_null_value)
        assertThat(aggregators[0]).isInstanceOf(FieldLastNonNullValueAgg.class);
        assertThat(aggregators[1]).isInstanceOf(FieldLastNonNullValueAgg.class);
    }
}
