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

package org.apache.fluss.trino.functions;

import org.apache.fluss.trino.FlussPlugin;

import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.GroupedAccumulatorState;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;
import org.apache.datasketches.hll.HllUnion;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.management.JMException;
import javax.management.ObjectName;

import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** SQL registration, grouped/intermediate state and legacy-byte compatibility tests. */
class SketchFunctionsTest {
    /** Opt-in SQL benchmark; iteration 0 and 1 warm up the engine. */
    @Test
    void benchmarkSql() {
        assumeTrue(Boolean.parseBoolean(System.getenv("FLUSS_SKETCH_BENCHMARK")));
        int events = 1_000_000;
        for (String workload : Arrays.asList("global", "grouped", "merge")) {
            int groups = workload.equals("global") ? 1 : 10_000;
            int outputGroups = workload.equals("grouped") ? groups : 1;
            String query =
                    "SELECT mod(id, "
                            + groups
                            + ") AS g, fluss_hll_build_agg(id) AS h, "
                            + "fluss_kll_double_build_agg(CAST(id AS DOUBLE)) AS q FROM "
                            + "(SELECT a * BIGINT '1000' + b AS id FROM UNNEST(sequence(BIGINT '0', BIGINT '999')) t(a) "
                            + "CROSS JOIN UNNEST(sequence(BIGINT '0', BIGINT '999')) u(b)) GROUP BY mod(id, "
                            + groups
                            + ")";
            if (workload.equals("merge")) {
                query =
                        "SELECT BIGINT '0' AS g, fluss_hll_merge_agg(h) AS h, fluss_kll_double_merge_agg(q) AS q FROM ("
                                + query
                                + ")";
            }
            query =
                    "SELECT g, h, q, fluss_hll_estimate(h), fluss_kll_double_quantile(q, 0.5) FROM ("
                            + query
                            + ")";
            runner.execute("EXPLAIN (TYPE DISTRIBUTED) " + query)
                    .getMaterializedRows()
                    .forEach(System.out::println);
            for (int iteration = 0; iteration < 7; iteration++) {
                long allocatedBefore = allocatedBytes();
                long start = System.nanoTime();
                List<MaterializedRow> result = runner.execute(query).getMaterializedRows();
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
                for (MaterializedRow row : result) {
                    long key = ((Number) row.getField(0)).longValue();
                    byte[] h = (byte[]) row.getField(1);
                    byte[] q = (byte[]) row.getField(2);
                    bytes += h.length + q.length;
                    KllDoublesSketch kll = KllDoublesSketch.heapify(MemorySegment.ofArray(q));
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
                // Timing includes planning, source generation, exchange and collection, not
                // validation.
                System.out.println(
                        String.format(
                                Locale.ROOT,
                                "SKETCH_SQL_BENCH engine=trino workload=%s iteration=%d events=%d output_groups=%d elapsed_ms=%.3f allocated_bytes=%d sketch_bytes=%d hll_error=%.6f kll_rank_error=%.6f",
                                workload,
                                iteration,
                                events,
                                outputGroups,
                                elapsed / 1_000_000.0,
                                allocated,
                                bytes,
                                hllError,
                                rankError));
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
    void testLargeLegacySketches() throws Exception {
        byte[] hll =
                java.nio.file.Files.readAllBytes(
                        java.nio.file.Paths.get(
                                getClass()
                                        .getResource("/sketches/datasketches62/hll.bin")
                                        .toURI()));
        byte[] kll =
                java.nio.file.Files.readAllBytes(
                        java.nio.file.Paths.get(
                                getClass()
                                        .getResource("/sketches/datasketches62/kll.bin")
                                        .toURI()));
        assertThat(SketchReaders.hllEstimate(Slices.wrappedBuffer(hll)))
                .isBetween(18000.0, 22000.0);
        assertThat(SketchReaders.kllQuantile(Slices.wrappedBuffer(kll), 0.5))
                .isBetween(9000.0, 11000.0);
    }

    private static DistributedQueryRunner runner;

    @BeforeAll
    static void start() throws Exception {
        runner =
                DistributedQueryRunner.builder(testSessionBuilder().build())
                        .setWorkerCount(1)
                        .build();
        runner.installPlugin(new FlussPlugin());
    }

    @AfterAll
    static void stop() {
        if (runner != null) {
            runner.close();
        }
    }

    @Test
    void testSqlBuildAndRead() {
        MaterializedRow row =
                runner.execute(
                                "SELECT fluss_hll_estimate(fluss_hll_build_agg(v)), "
                                        + "fluss_kll_double_quantile(fluss_kll_double_build_agg(CAST(v AS DOUBLE)), 0.5) "
                                        + "FROM (VALUES BIGINT '1', BIGINT '2', BIGINT '2') t(v)")
                        .getMaterializedRows()
                        .get(0);
        assertThat((Double) row.getField(0)).isBetween(1.99, 2.01);
        assertThat((Double) row.getField(1)).isEqualTo(2.0);
    }

    @Test
    void testSqlMergeAcrossGroups() {
        MaterializedRow row =
                runner.execute(
                                "SELECT fluss_hll_estimate(fluss_hll_merge_agg(h)), "
                                        + "fluss_kll_double_quantile(fluss_kll_double_merge_agg(q), 0.5) "
                                        + "FROM (SELECT v % 2 AS k, fluss_hll_build_agg(v) h, "
                                        + "fluss_kll_double_build_agg(CAST(v AS DOUBLE)) q "
                                        + "FROM UNNEST(sequence(BIGINT '1', BIGINT '100')) t(v) GROUP BY v % 2)")
                        .getMaterializedRows()
                        .get(0);
        assertThat((Double) row.getField(0)).isBetween(95.0, 105.0);
        assertThat((Double) row.getField(1)).isBetween(45.0, 55.0);
    }

    @Test
    void testReadsDataSketches62Fixtures() {
        // Generated with DataSketches Java 6.2.0 on JDK 17 from the values 1, 2, 3.
        MaterializedRow row =
                runner.execute(
                                "SELECT fluss_hll_estimate(from_base64('AgEHDAMIAwAr8vsGhi/5DXWBZgc=')), "
                                        + "fluss_kll_double_quantile(from_base64("
                                        + "'BQEPAMgACAADAAAAAAAAAMgAAQDFAAAAAAAAAAAA8D8AAAAAAAAIQAAAAAAAAAhAAAAAAAAAAEAAAAAAAADwPw=='), 0.5)")
                        .getMaterializedRows()
                        .get(0);
        assertThat((Double) row.getField(0)).isBetween(2.99, 3.01);
        assertThat((Double) row.getField(1)).isEqualTo(2.0);
    }

    @Test
    void testNullAndInvalidArguments() {
        MaterializedRow row =
                runner.execute(
                                "SELECT fluss_hll_estimate(CAST(NULL AS VARBINARY)), "
                                        + "fluss_kll_double_quantile(CAST(NULL AS VARBINARY), 0.5)")
                        .getMaterializedRows()
                        .get(0);
        assertThat(row.getField(0)).isNull();
        assertThat(row.getField(1)).isNull();
        assertThatThrownBy(() -> runner.execute("SELECT fluss_hll_estimate(X'010203')"))
                .hasMessageContaining("Invalid HLL sketch bytes");
        assertThatThrownBy(() -> runner.execute("SELECT fluss_kll_double_quantile(X'00', 1.1)"))
                .hasMessageContaining("Quantile rank must be in [0, 1]");
    }

    @Test
    void testIntermediateStateAndGroupedIsolation() {
        SketchStateFactory factory = new SketchStateFactory();
        SketchState first = factory.createSingleState();
        HllBuildAggregation.input(first, 1L);
        BlockBuilder out = VARBINARY.createBlockBuilder(null, 1);
        SketchStateSerializer serializer = new SketchStateSerializer();
        serializer.serialize(first, out);
        SketchState restored = factory.createSingleState();
        serializer.deserialize(out.build(), 0, restored);
        HllBuildAggregation.input(restored, 2L);
        assertThat(((HllUnion) first.getSketch()).getEstimate()).isBetween(0.99, 1.01);
        assertThat(((HllUnion) restored.getSketch()).getEstimate()).isBetween(1.99, 2.01);

        SketchState grouped = factory.createGroupedState();
        GroupedAccumulatorState groups = (GroupedAccumulatorState) grouped;
        groups.ensureCapacity(2);
        groups.setGroupId(0);
        KllDoubleBuildAggregation.input(grouped, 1.0);
        long before = grouped.getEstimatedSize();
        groups.setGroupId(1);
        assertThat(grouped.getSketch()).isNull();
        KllDoubleBuildAggregation.input(grouped, 2.0);
        assertThat(grouped.getEstimatedSize()).isGreaterThan(before);
        groups.setGroupId(0);
        assertThat(((KllDoublesSketch) grouped.getSketch()).getMaxItem()).isEqualTo(1.0);
    }

    @Test
    void testCombiningDoesNotAliasPartialState() {
        SketchStateFactory factory = new SketchStateFactory();
        SketchState first = factory.createSingleState();
        SketchState second = factory.createSingleState();
        KllDoubleBuildAggregation.input(second, 42.0);
        KllDoubleBuildAggregation.combine(first, second);
        KllDoubleBuildAggregation.input(first, 43.0);
        assertThat(((KllDoublesSketch) second.getSketch()).getN()).isEqualTo(1);
        assertThat(((KllDoublesSketch) first.getSketch()).getN()).isEqualTo(2);
        assertThat(
                        SketchReaders.kllQuantile(
                                Slices.wrappedBuffer(
                                        ((KllDoublesSketch) first.getSketch()).toByteArray()),
                                0.5))
                .isBetween(42.0, 43.0);
    }
}
