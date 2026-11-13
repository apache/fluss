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

import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SQL registration, grouped/intermediate state and legacy-byte compatibility tests. */
class SketchFunctionsTest {
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
