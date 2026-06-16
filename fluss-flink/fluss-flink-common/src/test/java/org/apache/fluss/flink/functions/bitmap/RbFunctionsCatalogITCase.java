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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.roaringbitmap.RoaringBitmap;

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
    void testAllThreeFunctionsInSingleQuery() throws Exception {
        // Validates the PR title claim: all three functions callable after USE CATALOG
        // fluss_catalog
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
    void testRbBuildResolvedFromCatalog() throws Exception {
        TableResult result = tEnv.executeSql("SELECT rb_cardinality(rb_build(1, 2, 3, 2))");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).getField(0)).isEqualTo(3L); // duplicate 2 ignored
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
}
