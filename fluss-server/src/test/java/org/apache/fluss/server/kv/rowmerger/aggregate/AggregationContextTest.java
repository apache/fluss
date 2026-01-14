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

package org.apache.fluss.server.kv.rowmerger.aggregate;

import org.apache.fluss.config.Configuration;
import org.apache.fluss.config.TableConfig;
import org.apache.fluss.metadata.AggFunctions;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldBoolAndAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldBoolOrAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldFirstNonNullValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldFirstValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldLastNonNullValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldLastValueAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldListaggAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldMaxAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldMinAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldProductAgg;
import org.apache.fluss.server.kv.rowmerger.aggregate.functions.FieldSumAgg;
import org.apache.fluss.types.DataTypes;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for aggregator creation in {@link AggregationContext}. */
class AggregationContextTest {

    @Test
    void testAllAggregatorTypesFromSchema() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("sum_col", DataTypes.BIGINT(), AggFunctions.SUM())
                        .column("product_col", DataTypes.DOUBLE(), AggFunctions.PRODUCT())
                        .column("max_col", DataTypes.INT(), AggFunctions.MAX())
                        .column("min_col", DataTypes.INT(), AggFunctions.MIN())
                        .column("last_val_col", DataTypes.STRING(), AggFunctions.LAST_VALUE())
                        .column(
                                "last_nonnull_col",
                                DataTypes.STRING(),
                                AggFunctions.LAST_VALUE_IGNORE_NULLS())
                        .column("first_val_col", DataTypes.STRING(), AggFunctions.FIRST_VALUE())
                        .column(
                                "first_nonnull_col",
                                DataTypes.STRING(),
                                AggFunctions.FIRST_VALUE_IGNORE_NULLS())
                        .column("bool_and_col", DataTypes.BOOLEAN(), AggFunctions.BOOL_AND())
                        .column("bool_or_col", DataTypes.BOOLEAN(), AggFunctions.BOOL_OR())
                        .column("listagg_col", DataTypes.STRING(), AggFunctions.LISTAGG())
                        .column("string_agg_col", DataTypes.STRING(), AggFunctions.STRING_AGG())
                        .primaryKey("id")
                        .build();

        TableConfig tableConfig = new TableConfig(new Configuration());
        AggregationContext context = AggregationContext.create(schema, KvFormat.COMPACTED);

        // Primary key field should use FieldLastValueAgg (not aggregated)
        assertThat(context.getAggregators()[0]).isInstanceOf(FieldLastValueAgg.class);
        assertThat(context.getAggregators()[1]).isInstanceOf(FieldSumAgg.class);
        assertThat(context.getAggregators()[2]).isInstanceOf(FieldProductAgg.class);
        assertThat(context.getAggregators()[3]).isInstanceOf(FieldMaxAgg.class);
        assertThat(context.getAggregators()[4]).isInstanceOf(FieldMinAgg.class);
        assertThat(context.getAggregators()[5]).isInstanceOf(FieldLastValueAgg.class);
        assertThat(context.getAggregators()[6]).isInstanceOf(FieldLastNonNullValueAgg.class);
        assertThat(context.getAggregators()[7]).isInstanceOf(FieldFirstValueAgg.class);
        assertThat(context.getAggregators()[8]).isInstanceOf(FieldFirstNonNullValueAgg.class);
        assertThat(context.getAggregators()[9]).isInstanceOf(FieldBoolAndAgg.class);
        assertThat(context.getAggregators()[10]).isInstanceOf(FieldBoolOrAgg.class);
        assertThat(context.getAggregators()[11]).isInstanceOf(FieldListaggAgg.class);
        assertThat(context.getAggregators()[12])
                .isInstanceOf(FieldListaggAgg.class); // string_agg is alias
    }
}
