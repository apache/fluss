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

import org.apache.fluss.flink.catalog.FlinkCatalog;
import org.apache.fluss.server.testutils.FlussClusterExtension;

import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.roaringbitmap.RoaringBitmap;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.fluss.config.ConfigOptions.BOOTSTRAP_SERVERS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test that validates bitmap aggregate functions registered via {@link
 * FlinkCatalog} are correctly resolved by the Flink Table planner.
 *
 * <p>Unlike {@link AbstractRbAggFunctionITCase}, which registers functions directly via {@link
 * TableEnvironment#createTemporarySystemFunction}, this test exercises the full catalog
 * registration path: functions are looked up through {@code USE CATALOG fluss_catalog}, validating
 * that {@link FlinkCatalog#getFunction} returns the correct {@code CatalogFunctionImpl} and that
 * the Flink planner correctly instantiates and runs the UDF through real codegen.
 */
class RbFunctionsCatalogITCase {

    @RegisterExtension
    static final FlussClusterExtension FLUSS_CLUSTER =
            FlussClusterExtension.builder().setNumOfTabletServers(1).build();

    private static final String CATALOG_NAME = "fluss_catalog";
    private static final String DATABASE_NAME = "fluss";

    private TableEnvironment tEnv;

    @BeforeEach
    void beforeEach() throws Exception {
        String bootstrapServers =
                String.join(",", FLUSS_CLUSTER.getClientConfig().get(BOOTSTRAP_SERVERS));

        FlinkCatalog catalog =
                new FlinkCatalog(
                        CATALOG_NAME,
                        DATABASE_NAME,
                        bootstrapServers,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyMap(),
                        Collections::emptyMap);

        tEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tEnv.registerCatalog(CATALOG_NAME, catalog);
        tEnv.useCatalog(CATALOG_NAME);
    }

    @Test
    void testRbBuildAggResolvedFromCatalog() throws Exception {
        // Use fromValues so no Fluss table is needed for data.
        // The key assertion: rb_build_agg is resolved from FlinkCatalog, not manually registered.
        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.INT()),
                                DataTypes.FIELD("v", DataTypes.INT())),
                        Row.of(1, 10),
                        Row.of(1, 20),
                        Row.of(1, 10), // duplicate — should be deduplicated
                        Row.of(2, 30));
        tEnv.createTemporaryView("events", source);

        TableResult result = tEnv.executeSql("SELECT k, rb_build_agg(v) FROM events GROUP BY k");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(2);

        // Sort by k for deterministic assertion
        rows.sort((a, b) -> Integer.compare((int) a.getField(0), (int) b.getField(0)));

        // k=1: bitmap of {10, 20}
        byte[] group1Bytes = (byte[]) rows.get(0).getField(1);
        assertThat(group1Bytes).isNotNull();
        RoaringBitmap group1 = BitmapUtils.fromBytes(group1Bytes);
        assertThat(group1.getLongCardinality()).isEqualTo(2L);
        assertThat(group1.contains(10)).isTrue();
        assertThat(group1.contains(20)).isTrue();

        // k=2: bitmap of {30}
        byte[] group2Bytes = (byte[]) rows.get(1).getField(1);
        RoaringBitmap group2 = BitmapUtils.fromBytes(group2Bytes);
        assertThat(group2.getLongCardinality()).isEqualTo(1L);
        assertThat(group2.contains(30)).isTrue();
    }

    @Test
    void testSketchFunctionsResolvedFromCatalog() throws Exception {
        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("v", DataTypes.BIGINT())),
                        Row.of(1L),
                        Row.of(2L),
                        Row.of(2L));
        tEnv.createTemporaryView("sketch_events", source);
        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT fluss_hll_estimate(fluss_hll_build_agg(v)), "
                                                + "fluss_kll_double_quantile(fluss_kll_double_build_agg(CAST(v AS DOUBLE)), 0.5) "
                                                + "FROM sketch_events")
                                .collect());
        assertThat(rows).hasSize(1);
        assertThat((Double) rows.get(0).getField(0)).isBetween(1.99, 2.01);
        assertThat((Double) rows.get(0).getField(1)).isEqualTo(2.0);

        List<Row> merged =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT fluss_hll_estimate(fluss_hll_merge_agg(h)), "
                                                + "fluss_kll_double_quantile(fluss_kll_double_merge_agg(q), 0.5) "
                                                + "FROM (SELECT MOD(v, 2) AS k, fluss_hll_build_agg(v) AS h, "
                                                + "fluss_kll_double_build_agg(CAST(v AS DOUBLE)) AS q "
                                                + "FROM sketch_events GROUP BY MOD(v, 2))")
                                .collect());
        assertThat((Double) merged.get(0).getField(0)).isBetween(1.99, 2.01);
        assertThat((Double) merged.get(0).getField(1)).isEqualTo(2.0);
    }

    @Test
    void testWindowedSketchBatchesWrittenToFluss() throws Exception {
        LocalDateTime start = LocalDateTime.of(2026, 1, 1, 0, 0);
        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();
        execution.setParallelism(1);
        StreamTableEnvironment streaming = StreamTableEnvironment.create(execution);
        streaming.registerCatalog(CATALOG_NAME, tEnv.getCatalog(CATALOG_NAME).get());
        streaming.useCatalog(CATALOG_NAME);
        streaming.createTemporaryView(
                "requests",
                streaming.fromDataStream(
                        execution.fromCollection(
                                Arrays.asList(
                                        Row.of("2026-01-01", 1L, 10.0, start),
                                        Row.of("2026-01-01", 2L, 20.0, start.plusSeconds(1)),
                                        Row.of("2026-01-01", 2L, 30.0, start.plusSeconds(5)),
                                        Row.of("2026-01-01", 3L, 40.0, start.plusSeconds(6))),
                                Types.ROW_NAMED(
                                        new String[] {"metric_day", "user_id", "latency_ms", "ts"},
                                        Types.STRING,
                                        Types.LONG,
                                        Types.DOUBLE,
                                        Types.LOCAL_DATE_TIME)),
                        Schema.newBuilder()
                                .columnByExpression("event_time", "CAST(ts AS TIMESTAMP(3))")
                                .watermark("event_time", "event_time - INTERVAL '1' SECOND")
                                .build()));
        tEnv.executeSql(
                "CREATE TABLE daily_metric_sketches (metric_day STRING, "
                        + "user_hll BYTES, latency_kll BYTES, PRIMARY KEY(metric_day) NOT ENFORCED) WITH ("
                        + "'table.merge-engine' = 'aggregation', 'fields.user_hll.agg' = 'hll_sketch', "
                        + "'fields.latency_kll.agg' = 'kll_double_sketch')");
        streaming
                .executeSql(
                        "INSERT INTO daily_metric_sketches SELECT metric_day, "
                                + "fluss_hll_build_agg(user_id), fluss_kll_double_build_agg(latency_ms) "
                                + "FROM TABLE(TUMBLE(TABLE requests, DESCRIPTOR(event_time), INTERVAL '5' SECOND)) "
                                + "GROUP BY metric_day, window_start, window_end")
                .await();
        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT fluss_hll_estimate(user_hll), latency_kll FROM daily_metric_sketches")
                                .collect());
        assertThat(rows).hasSize(1);
        assertThat((Double) rows.get(0).getField(0)).isBetween(2.99, 3.01);
        KllDoublesSketch kll =
                KllDoublesSketch.heapify(Memory.wrap((byte[]) rows.get(0).getField(1)));
        assertThat(kll.getN()).isEqualTo(4);
        assertThat(kll.getMinItem()).isEqualTo(10.0);
        assertThat(kll.getMaxItem()).isEqualTo(40.0);
    }

    @Test
    void testRbOrAggResolvedFromCatalog() throws Exception {
        byte[] bitmap1 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));
        byte[] bitmap2 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4, 5));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.INT()),
                                DataTypes.FIELD("bmap", DataTypes.BYTES())),
                        Row.of(1, bitmap1),
                        Row.of(1, bitmap2));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result = tEnv.executeSql("SELECT k, rb_or_agg(bmap) FROM bitmaps GROUP BY k");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        byte[] resultBytes = (byte[]) rows.get(0).getField(1);
        RoaringBitmap union = BitmapUtils.fromBytes(resultBytes);
        assertThat(union.getLongCardinality()).isEqualTo(5L);
        assertThat(union.contains(1)).isTrue();
        assertThat(union.contains(5)).isTrue();
    }

    @Test
    void testRbAndAggResolvedFromCatalog() throws Exception {
        byte[] bitmap1 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3, 4));
        byte[] bitmap2 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3, 5));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.INT()),
                                DataTypes.FIELD("bmap", DataTypes.BYTES())),
                        Row.of(1, bitmap1),
                        Row.of(1, bitmap2));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result = tEnv.executeSql("SELECT k, rb_and_agg(bmap) FROM bitmaps GROUP BY k");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        byte[] resultBytes = (byte[]) rows.get(0).getField(1);
        RoaringBitmap intersection = BitmapUtils.fromBytes(resultBytes);
        assertThat(intersection.getLongCardinality()).isEqualTo(2L);
        assertThat(intersection.contains(2)).isTrue();
        assertThat(intersection.contains(3)).isTrue();
    }

    @Test
    void testAllThreeAggFunctionsInSingleQuery() throws Exception {
        // Validates all three aggregate functions are callable after USE CATALOG fluss_catalog
        byte[] bmap1 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));
        byte[] bmap2 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3, 4));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.INT()),
                                DataTypes.FIELD("id", DataTypes.INT()),
                                DataTypes.FIELD("bmap", DataTypes.BYTES())),
                        Row.of(1, 10, bmap1),
                        Row.of(1, 20, bmap2));
        tEnv.createTemporaryView("events", source);

        TableResult result =
                tEnv.executeSql(
                        "SELECT k, rb_build_agg(id), rb_or_agg(bmap), rb_and_agg(bmap) "
                                + "FROM events GROUP BY k");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(1)).isNotNull(); // rb_build_agg
        assertThat(rows.get(0).getField(2)).isNotNull(); // rb_or_agg
        assertThat(rows.get(0).getField(3)).isNotNull(); // rb_and_agg
    }

    @Test
    void testRbCardinalityResolvedFromCatalog() throws Exception {
        byte[] bitmap = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(10, 20, 30, 40, 50));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("bmap", DataTypes.BYTES())), Row.of(bitmap));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result = tEnv.executeSql("SELECT rb_cardinality(bmap) FROM bitmaps");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(5L);
    }

    @Test
    void testRbContainsResolvedFromCatalog() throws Exception {
        byte[] bitmap = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("bmap", DataTypes.BYTES())), Row.of(bitmap));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result =
                tEnv.executeSql("SELECT rb_contains(bmap, 2), rb_contains(bmap, 99) FROM bitmaps");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(true);
        assertThat(rows.get(0).getField(1)).isEqualTo(false);
    }

    @Test
    void testRbToArrayResolvedFromCatalog() throws Exception {
        byte[] bitmap = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 1, 2));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(DataTypes.FIELD("bmap", DataTypes.BYTES())), Row.of(bitmap));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result = tEnv.executeSql("SELECT rb_to_array(bmap) FROM bitmaps");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        Integer[] arr = (Integer[]) rows.get(0).getField(0);
        assertThat(arr).containsExactly(1, 2, 3); // ascending order
    }

    @Test
    void testRbOrScalarResolvedFromCatalog() throws Exception {
        byte[] left = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));
        byte[] right = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4, 5));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("l", DataTypes.BYTES()),
                                DataTypes.FIELD("r", DataTypes.BYTES())),
                        Row.of(left, right));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result = tEnv.executeSql("SELECT rb_cardinality(rb_or(l, r)) FROM bitmaps");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(5L);
    }

    @Test
    void testRbAndScalarResolvedFromCatalog() throws Exception {
        byte[] left = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3, 4));
        byte[] right = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3, 5));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("l", DataTypes.BYTES()),
                                DataTypes.FIELD("r", DataTypes.BYTES())),
                        Row.of(left, right));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result = tEnv.executeSql("SELECT rb_cardinality(rb_and(l, r)) FROM bitmaps");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(2L); // {2, 3}
    }

    @Test
    void testScalarAndAggFunctionsCombined() throws Exception {
        // Validates the full Phase 1 function set works together:
        // build a bitmap via rb_build_agg, then query it with scalar functions
        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.INT()),
                                DataTypes.FIELD("v", DataTypes.INT())),
                        Row.of(1, 10),
                        Row.of(1, 20),
                        Row.of(1, 30));
        tEnv.createTemporaryView("events", source);

        TableResult result =
                tEnv.executeSql(
                        "SELECT rb_cardinality(rb_build_agg(v)), "
                                + "rb_contains(rb_build_agg(v), 20) "
                                + "FROM events GROUP BY k");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(3L);
        assertThat(rows.get(0).getField(1)).isEqualTo(true);
    }

    @Test
    void testRbBuildFromArrayResolvedFromCatalog() throws Exception {
        TableResult result = tEnv.executeSql("SELECT rb_cardinality(rb_build(ARRAY[1, 2, 3, 2]))");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(3L); // duplicate 2 ignored
    }

    @Test
    void testRbXorAggResolvedFromCatalog() throws Exception {
        byte[] bitmap1 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));
        byte[] bitmap2 = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3, 4));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.INT()),
                                DataTypes.FIELD("bmap", DataTypes.BYTES())),
                        Row.of(1, bitmap1),
                        Row.of(1, bitmap2));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result = tEnv.executeSql("SELECT k, rb_xor_agg(bmap) FROM bitmaps GROUP BY k");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());
        assertThat(rows).hasSize(1);

        byte[] resultBytes = (byte[]) rows.get(0).getField(1);
        RoaringBitmap xor = BitmapUtils.fromBytes(resultBytes);
        // XOR of {1,2,3} and {2,3,4} = {1,4}
        assertThat(xor.getLongCardinality()).isEqualTo(2L);
        assertThat(xor.contains(1)).isTrue();
        assertThat(xor.contains(4)).isTrue();
    }

    @Test
    void testRbXorScalarResolvedFromCatalog() throws Exception {
        byte[] left = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));
        byte[] right = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(2, 3, 4));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("l", DataTypes.BYTES()),
                                DataTypes.FIELD("r", DataTypes.BYTES())),
                        Row.of(left, right));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result = tEnv.executeSql("SELECT rb_cardinality(rb_xor(l, r)) FROM bitmaps");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(2L); // {1, 4}
    }

    @Test
    void testRbAndNotResolvedFromCatalog() throws Exception {
        byte[] left = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3, 4));
        byte[] right = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(3, 4, 5));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("l", DataTypes.BYTES()),
                                DataTypes.FIELD("r", DataTypes.BYTES())),
                        Row.of(left, right));
        tEnv.createTemporaryView("bitmaps", source);

        TableResult result = tEnv.executeSql("SELECT rb_cardinality(rb_andnot(l, r)) FROM bitmaps");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(2L); // {1, 2}
    }

    @Test
    void testRbXorAggIdenticalBitmapsReturnsEmptyBitmap() throws Exception {
        byte[] bitmap = BitmapUtils.toBytes(RoaringBitmap.bitmapOf(1, 2, 3));

        Table source =
                tEnv.fromValues(
                        DataTypes.ROW(
                                DataTypes.FIELD("k", DataTypes.INT()),
                                DataTypes.FIELD("bmap", DataTypes.BYTES())),
                        Row.of(1, bitmap),
                        Row.of(1, bitmap));
        tEnv.createTemporaryView("identical_bitmaps", source);

        TableResult result =
                tEnv.executeSql(
                        "SELECT rb_cardinality(rb_xor_agg(bmap)) "
                                + "FROM identical_bitmaps GROUP BY k");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(0L);
    }
}
