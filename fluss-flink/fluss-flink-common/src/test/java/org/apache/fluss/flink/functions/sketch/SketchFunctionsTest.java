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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import javax.management.JMException;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Sketch SQL execution, partial aggregation and persisted accumulator coverage. */
class SketchFunctionsTest {
    /** Opt-in SQL benchmark; iteration 0 and 1 warm up the engine. */
    @Test
    void benchmarkSql() throws Exception {
        assumeTrue(Boolean.parseBoolean(System.getenv("FLUSS_SKETCH_BENCHMARK")));
        int events = 1_000_000;
        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();
        execution.setRuntimeMode(RuntimeExecutionMode.BATCH);
        execution.setParallelism(2);
        StreamTableEnvironment env = StreamTableEnvironment.create(execution);
        env.getConfig()
                .getConfiguration()
                .setString("table.optimizer.agg-phase-strategy", "TWO_PHASE");
        env.createTemporaryView(
                "benchmark_input",
                env.fromDataStream(execution.fromSequence(0, events - 1)).as("id"));
        env.createTemporarySystemFunction("fluss_hll_build_agg", HllBuildAggFunction.class);
        env.createTemporarySystemFunction("fluss_hll_merge_agg", HllMergeAggFunction.class);
        env.createTemporarySystemFunction("fluss_hll_estimate", HllEstimateFunction.class);
        env.createTemporarySystemFunction(
                "fluss_kll_double_build_agg", KllDoubleBuildAggFunction.class);
        env.createTemporarySystemFunction(
                "fluss_kll_double_merge_agg", KllDoubleMergeAggFunction.class);
        env.createTemporarySystemFunction(
                "fluss_kll_double_quantile", KllDoubleQuantileFunction.class);
        for (String workload : Arrays.asList("global", "grouped", "merge")) {
            int groups = workload.equals("global") ? 1 : 10_000;
            int outputGroups = workload.equals("grouped") ? groups : 1;
            String query =
                    "SELECT MOD(id, "
                            + groups
                            + ") AS g, fluss_hll_build_agg(id) AS h, "
                            + "fluss_kll_double_build_agg(CAST(id AS DOUBLE)) AS q FROM benchmark_input GROUP BY MOD(id, "
                            + groups
                            + ")";
            if (workload.equals("merge")) {
                query =
                        "SELECT CAST(0 AS BIGINT) AS g, fluss_hll_merge_agg(h) AS h, fluss_kll_double_merge_agg(q) AS q FROM ("
                                + query
                                + ")";
            }
            query =
                    "SELECT g, h, q, fluss_hll_estimate(h), fluss_kll_double_quantile(q, 0.5) FROM ("
                            + query
                            + ")";
            System.out.println(env.explainSql(query));
            for (int iteration = 0; iteration < 7; iteration++) {
                long allocatedBefore = allocatedBytes();
                long start = System.nanoTime();
                List<Row> result = new ArrayList<>();
                try (CloseableIterator<Row> rows = env.executeSql(query).collect()) {
                    rows.forEachRemaining(result::add);
                }
                long elapsed = System.nanoTime() - start;
                long allocatedAfter = allocatedBytes();
                long allocated =
                        allocatedBefore < 0 || allocatedAfter < allocatedBefore
                                ? -1
                                : allocatedAfter - allocatedBefore;
                assertThat(result).hasSize(outputGroups);
                long bytes = 0;
                double hllError = 0;
                double rankError = 0;
                for (Row row : result) {
                    long key = ((Number) row.getField(0)).longValue();
                    byte[] h = (byte[]) row.getField(1);
                    byte[] q = (byte[]) row.getField(2);
                    bytes += h.length + q.length;
                    KllDoublesSketch kll = KllDoublesSketch.heapify(Memory.wrap(q));
                    long count = events / outputGroups;
                    assertThat(kll.getN()).isEqualTo(count);
                    assertThat(kll.getMinItem()).isEqualTo((double) key);
                    assertThat(kll.getMaxItem()).isEqualTo((double) (events - outputGroups + key));
                    hllError = Math.max(hllError, Math.abs((Double) row.getField(3) / count - 1));
                    rankError =
                            Math.max(
                                    rankError,
                                    Math.abs(
                                            ((Double) row.getField(4) - key) / outputGroups / count
                                                    - 0.5));
                }
                assertThat(hllError).isLessThan(0.08);
                assertThat(rankError).isLessThan(0.04);
                // Timing includes planning, source generation, exchange and result collection, not
                // validation.
                System.out.printf(
                        Locale.ROOT,
                        "SKETCH_SQL_BENCH engine=flink workload=%s iteration=%d events=%d output_groups=%d elapsed_ms=%.3f allocated_bytes=%d sketch_bytes=%d hll_error=%.6f kll_rank_error=%.6f%n",
                        workload,
                        iteration,
                        events,
                        outputGroups,
                        elapsed / 1_000_000.0,
                        allocated,
                        bytes,
                        hllError,
                        rankError);
            }
        }
    }

    private static long allocatedBytes() {
        try {
            return (Long)
                    ManagementFactory.getPlatformMBeanServer()
                            .getAttribute(
                                    new ObjectName("java.lang:type=Threading"),
                                    "TotalThreadAllocatedBytes");
        } catch (JMException e) {
            // Older JVMs do not expose the all-thread allocation counter.
            return -1;
        }
    }

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
