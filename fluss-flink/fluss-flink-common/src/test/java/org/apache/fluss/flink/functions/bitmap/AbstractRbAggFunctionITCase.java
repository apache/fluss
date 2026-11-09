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

package org.apache.fluss.flink.functions.bitmap;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end smoke test that validates the Flink Table planner accepts the {@link
 * AbstractRbAggFunction} contract — namely the {@code @FunctionHint(accumulator
 * = @DataTypeHint(value = "RAW", bridgedTo = RoaringBitmap.class))} hint together with the custom
 * {@link RoaringBitmapTypeInfo} returned from {@link AbstractRbAggFunction#getAccumulatorType()}.
 *
 * <p>Direct method-level invocation of the aggregate function does not exercise the Flink planner's
 * type-extraction path, so this IT test wires a minimal concrete subclass through a batch {@link
 * TableEnvironment} to verify the planner accepts and runs it.
 */
class AbstractRbAggFunctionITCase {

    /** Minimal concrete subclass used to exercise the planner. */
    public static final class TestRbAggFunction extends AbstractRbAggFunction {
        public void accumulate(RoaringBitmap acc, Integer value) {
            if (value != null) {
                acc.add(value);
            }
        }
    }

    @Test
    void testPlannerAcceptsRawAccumulatorHintAndProducesCorrectResult() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("test_rb_agg", new TestRbAggFunction());

        Table source = tEnv.fromValues(1, 2, 3, 1, 2).as("v");
        Table result = source.select(call("test_rb_agg", $("v")));

        List<Row> rows = CollectionUtil.iteratorToList(result.execute().collect());
        assertThat(rows).hasSize(1);

        byte[] bytes = (byte[]) rows.get(0).getField(0);
        assertThat(bytes).isNotNull();

        RoaringBitmap restored = BitmapUtils.fromBytes(bytes);
        assertThat(restored).isNotNull();
        assertThat(restored.getLongCardinality()).isEqualTo(3L);
        assertThat(restored.contains(1)).isTrue();
        assertThat(restored.contains(2)).isTrue();
        assertThat(restored.contains(3)).isTrue();
    }

    @Test
    void testPlannerReturnsNullForEmptyInput() throws Exception {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.createTemporarySystemFunction("test_rb_agg", new TestRbAggFunction());

        // Filter eliminates all rows; non-grouped aggregation over zero rows must emit
        // one row whose value is the result of getValue() on an empty accumulator (null).
        Table source = tEnv.fromValues(1, 2, 3).as("v").filter($("v").isGreater(100));
        Table result = source.select(call("test_rb_agg", $("v")));

        List<Row> rows = CollectionUtil.iteratorToList(result.execute().collect());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isNull();
    }
}
