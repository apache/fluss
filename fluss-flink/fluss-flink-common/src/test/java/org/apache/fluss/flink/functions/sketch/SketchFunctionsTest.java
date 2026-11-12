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

package org.apache.fluss.flink.functions.sketch;

import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Sketch SQL execution, partial aggregation and persisted accumulator coverage. */
class SketchFunctionsTest {
    @Test
    void testSqlBuildAndEstimate() throws Exception {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig()
                .getConfiguration()
                .setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        env.createTemporarySystemFunction("fluss_hll_build_agg", HllBuildAggFunction.class);
        env.createTemporarySystemFunction("fluss_hll_estimate", HllEstimateFunction.class);
        env.createTemporarySystemFunction(
                "fluss_kll_double_build_agg", KllDoubleBuildAggFunction.class);
        env.createTemporarySystemFunction(
                "fluss_kll_double_quantile", KllDoubleQuantileFunction.class);
        try (CloseableIterator<Row> rows =
                env.executeSql(
                                "SELECT fluss_hll_estimate(fluss_hll_build_agg(v)), "
                                        + "fluss_kll_double_quantile(fluss_kll_double_build_agg(CAST(v AS DOUBLE)), 0.5) "
                                        + "FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(2 AS BIGINT))) t(v)")
                        .collect()) {
            Row row = rows.next();
            assertThat((Double) row.getField(0))
                    .isCloseTo(2.0, org.assertj.core.data.Offset.offset(0.01));
            assertThat((Double) row.getField(1)).isBetween(1.0, 2.0);
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void testHllMergeAndSnapshot() throws Exception {
        HllBuildAggFunction build = new HllBuildAggFunction();
        Union first = build.createAccumulator();
        build.accumulate(first, 1L);
        build.accumulate(first, 2L);
        Union second = build.createAccumulator();
        build.accumulate(second, 2L);
        build.accumulate(second, 3L);
        DataOutputSerializer out = new DataOutputSerializer(64);
        HllUnionSerializer.INSTANCE.serialize(first, out);
        Union restored =
                HllUnionSerializer.INSTANCE.deserialize(
                        new DataInputDeserializer(out.getCopyOfBuffer()));
        HllMergeAggFunction merge = new HllMergeAggFunction();
        merge.accumulate(restored, build.getValue(second));
        merge.merge(restored, Collections.singletonList(build.createAccumulator()));
        assertThat(new HllEstimateFunction().eval(merge.getValue(restored)))
                .isCloseTo(3.0, org.assertj.core.data.Offset.offset(0.01));
        assertThat(HllSketch.heapify(merge.getValue(restored)).getLgConfigK()).isEqualTo(12);
        assertThat(build.getValue(build.createAccumulator())).isNull();
        build.resetAccumulator(first);
        assertThat(first.isEmpty()).isTrue();
    }

    @Test
    void testKllMergeAndSnapshot() throws Exception {
        KllDoubleBuildAggFunction build = new KllDoubleBuildAggFunction();
        KllDoublesSketch first = build.createAccumulator();
        build.accumulate(first, 1.0);
        build.accumulate(first, 2.0);
        KllDoublesSketch second = build.createAccumulator();
        build.accumulate(second, 3.0);
        build.accumulate(second, 4.0);
        DataOutputSerializer out = new DataOutputSerializer(64);
        KllDoublesSerializer.INSTANCE.serialize(first, out);
        KllDoublesSketch restored =
                KllDoublesSerializer.INSTANCE.deserialize(
                        new DataInputDeserializer(out.getCopyOfBuffer()));
        KllDoubleMergeAggFunction merge = new KllDoubleMergeAggFunction();
        merge.accumulate(restored, build.getValue(second));
        merge.merge(restored, Arrays.asList(build.createAccumulator()));
        KllDoublesSketch result = KllDoublesSketch.heapify(Memory.wrap(merge.getValue(restored)));
        assertThat(result.getN()).isEqualTo(4);
        assertThat(result.getMinItem()).isEqualTo(1.0);
        assertThat(result.getMaxItem()).isEqualTo(4.0);
        assertThat(new KllDoubleQuantileFunction().eval(merge.getValue(restored), 0.5))
                .isBetween(2.0, 3.0);
        assertThat(build.getValue(build.createAccumulator())).isNull();
    }

    @Test
    void testNullAndInvalidArguments() {
        HllEstimateFunction hll = new HllEstimateFunction();
        KllDoubleQuantileFunction kll = new KllDoubleQuantileFunction();
        assertThat(hll.eval(null)).isNull();
        assertThat(kll.eval(null, 0.5)).isNull();
        assertThat(kll.eval(KllDoublesSketch.newHeapInstance().toByteArray(), 0.5)).isNull();
        assertThatThrownBy(() -> hll.eval(new byte[] {1, 2, 3}))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> kll.eval(new byte[] {1, 2, 3}, 0.5))
                .isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> kll.eval(new byte[0], Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> kll.eval(new byte[0], 1.1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
