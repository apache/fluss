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

package org.apache.fluss.client.lookup;

import org.apache.fluss.bucketing.BucketingFunction;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.admin.Admin;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.DataLakeFormat;
import org.apache.fluss.metadata.DatabaseDescriptor;
import org.apache.fluss.metadata.PartitionSpec;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.encode.KeyEncoder;
import org.apache.fluss.server.testutils.FlussClusterExtension;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.fluss.config.ConfigOptions.CURRENT_KV_FORMAT_VERSION;
import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** ITCase for prefix lookups with multiple keys. */
class PrefixLookupITCase {

    private static final String DATABASE = "test_prefix_lookup_db";
    private static final int NUM_BUCKETS = 3;

    @RegisterExtension
    public static final FlussClusterExtension FLUSS_CLUSTER_EXTENSION =
            FlussClusterExtension.builder().setNumOfTabletServers(3).build();

    private Connection conn;
    private Admin admin;

    @BeforeEach
    void setup() throws Exception {
        // each lookup is sent alone, so don't wait for the batch to fill up
        Configuration clientConf = FLUSS_CLUSTER_EXTENSION.getClientConfig();
        clientConf.set(ConfigOptions.CLIENT_LOOKUP_BATCH_TIMEOUT, Duration.ofMillis(1));
        conn = ConnectionFactory.createConnection(clientConf);
        admin = conn.getAdmin();
        admin.createDatabase(DATABASE, DatabaseDescriptor.EMPTY, true).get();
    }

    @AfterEach
    void teardown() throws Exception {
        if (admin != null) {
            admin.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    static Stream<Arguments> lakeFormats() {
        return Stream.of(
                Arguments.of("none", null),
                Arguments.of("paimon", DataLakeFormat.PAIMON),
                Arguments.of("iceberg", DataLakeFormat.ICEBERG),
                Arguments.of("lance", DataLakeFormat.LANCE),
                Arguments.of("hudi", DataLakeFormat.HUDI));
    }

    static Stream<Arguments> lakeFormatsAndKvFormatVersions() {
        // legacy tables (kv format version 1) encode the primary key with the lake format's
        // encoder, which for Iceberg supports a single column only
        return lakeFormats()
                .flatMap(
                        args ->
                                Stream.of(1, 2)
                                        .filter(
                                                v ->
                                                        v == 2
                                                                || args.get()[1]
                                                                        != DataLakeFormat.ICEBERG)
                                        .map(v -> Arguments.of(args.get()[0], args.get()[1], v)));
    }

    static Stream<Arguments> compositeBucketKeyLakeFormats() {
        // Iceberg supports a single bucket key column only
        return lakeFormats().filter(args -> args.get()[1] != DataLakeFormat.ICEBERG);
    }

    @ParameterizedTest(name = "lake format = {0}, kv format version = {2}")
    @MethodSource("lakeFormatsAndKvFormatVersions")
    void testPrefixLookup(String name, @Nullable DataLakeFormat lakeFormat, int kvFormatVersion)
            throws Exception {
        try (Table table =
                createTable(
                        "prefix_" + name + "_v" + kvFormatVersion,
                        lakeFormat,
                        kvFormatVersion,
                        false,
                        "a")) {
            List<Object[]> rows = rows(null);
            write(table, rows);
            assertSeveralBuckets(table, rows);

            assertPrefixLookups(table, rows, "a");
            assertPrefixLookups(table, rows, "a", "b");
            assertPrefixLookups(table, rows, "a", "b", "c");
            assertThat(lookup(table, Arrays.asList("a", "b"), 1L, "abc")).isEmpty();
            assertThat(lookup(table, Arrays.asList("a", "b", "c"), 1L, "a", 2L)).isEmpty();
        }
    }

    @ParameterizedTest(name = "lake format = {0}")
    @MethodSource("compositeBucketKeyLakeFormats")
    void testPrefixLookupWithCompositeBucketKey(String name, @Nullable DataLakeFormat lakeFormat)
            throws Exception {
        try (Table table =
                createTable(
                        "composite_" + name,
                        lakeFormat,
                        CURRENT_KV_FORMAT_VERSION,
                        false,
                        "a",
                        "b")) {
            List<Object[]> rows = rows(null);
            write(table, rows);
            assertSeveralBuckets(table, rows);

            assertPrefixLookups(table, rows, "a", "b");
            assertPrefixLookups(table, rows, "a", "b", "c");
            assertThatThrownBy(() -> table.newLookup().lookupBy("a").createLookuper())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageEndingWith(
                            "must contain all bucket keys [a, b] in order, optionally followed by "
                                    + "the next physical primary keys [c] in order.");
        }
    }

    @ParameterizedTest(name = "lake format = {0}, kv format version = {2}")
    @MethodSource("lakeFormatsAndKvFormatVersions")
    void testPrefixLookupOnPartitionedTable(
            String name, @Nullable DataLakeFormat lakeFormat, int kvFormatVersion)
            throws Exception {
        try (Table table =
                createTable(
                        "partitioned_" + name + "_v" + kvFormatVersion,
                        lakeFormat,
                        kvFormatVersion,
                        true,
                        "a")) {
            List<Object[]> rows = rows("p1");
            rows.addAll(rows("p2"));
            write(table, rows);

            assertPrefixLookups(table, rows, "a", "b", "p");
            assertPrefixLookups(table, rows, "p", "a", "b", "c");
            assertThat(lookup(table, Arrays.asList("a", "b", "p"), 1L, "a", "p3")).isEmpty();
        }
    }

    @Test
    void testInvalidPrefixLookupColumns() throws Exception {
        try (Table table = createTable("invalid", null, CURRENT_KV_FORMAT_VERSION, false, "a")) {
            for (List<String> columns :
                    Arrays.asList(
                            Arrays.asList("a", "c"),
                            Arrays.asList("b", "a"),
                            Collections.singletonList("b"),
                            Arrays.asList("a", "b", "v"))) {
                assertThatThrownBy(() -> table.newLookup().lookupBy(columns).createLookuper())
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessage(
                                "Can not perform prefix lookup on table '%s', because the lookup "
                                        + "columns %s must contain all bucket keys [a] in order, "
                                        + "optionally followed by the next physical primary keys "
                                        + "[b, c] in order.",
                                table.getTableInfo().getTablePath(), columns);
            }
            assertThatThrownBy(
                            () -> table.newLookup().lookupBy("a", "b", "c", "d").createLookuper())
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Please use primary key lookup");
        }
    }

    /** Creates a table keyed by a, b, c and d, partitioned by p (p1, p2) if requested. */
    private Table createTable(
            String name,
            @Nullable DataLakeFormat lakeFormat,
            int kvFormatVersion,
            boolean partitioned,
            String... bucketKeys)
            throws Exception {
        Schema.Builder schema =
                Schema.newBuilder()
                        .column("a", DataTypes.BIGINT())
                        .column("b", DataTypes.STRING())
                        .column("c", DataTypes.BIGINT())
                        .column("d", DataTypes.BIGINT())
                        .column("v", DataTypes.STRING());
        if (partitioned) {
            schema.column("p", DataTypes.STRING()).primaryKey("a", "b", "c", "d", "p");
        } else {
            schema.primaryKey("a", "b", "c", "d");
        }
        TableDescriptor.Builder descriptor =
                TableDescriptor.builder()
                        .schema(schema.build())
                        .distributedBy(NUM_BUCKETS, bucketKeys)
                        .property(ConfigOptions.TABLE_KV_FORMAT_VERSION, kvFormatVersion);
        if (partitioned) {
            descriptor.partitionedBy("p");
        }
        if (lakeFormat != null) {
            descriptor.property(ConfigOptions.TABLE_DATALAKE_FORMAT, lakeFormat);
        }
        TablePath path = TablePath.of(DATABASE, name);
        admin.createTable(path, descriptor.build(), false).get();
        if (partitioned) {
            for (String partition : Arrays.asList("p1", "p2")) {
                admin.createPartition(
                                path,
                                new PartitionSpec(Collections.singletonMap("p", partition)),
                                false)
                        .get();
            }
        }
        return conn.getTable(path);
    }

    /**
     * Rows whose keys are prefixes of each other, e.g. b = "a" and "ab", including strings longer
     * than 7 bytes, which legacy Paimon keys don't store inline.
     */
    private static List<Object[]> rows(@Nullable String partition) {
        List<Object[]> rows = new ArrayList<>();
        for (long a = 1; a <= 6; a++) {
            for (String b : Arrays.asList("", "a", "ab", "abcdefghij", "abcdefghijk")) {
                for (long c : new long[] {1, 129}) {
                    for (long d = 1; d <= 2; d++) {
                        String v = a + "-" + b + "-" + c + "-" + d;
                        rows.add(
                                partition == null
                                        ? new Object[] {a, b, c, d, v}
                                        : new Object[] {a, b, c, d, v, partition});
                    }
                }
            }
        }
        return rows;
    }

    private static void write(Table table, List<Object[]> rows) throws Exception {
        UpsertWriter writer = table.newUpsert().createWriter();
        for (Object[] values : rows) {
            writer.upsert(row(values));
        }
        writer.flush();
    }

    /**
     * With a single bucket, routing by the whole prefix instead of the bucket key goes unnoticed.
     */
    private static void assertSeveralBuckets(Table table, List<Object[]> rows) {
        TableInfo info = table.getTableInfo();
        DataLakeFormat lakeFormat = info.getTableConfig().getDataLakeFormat().orElse(null);
        KeyEncoder encoder =
                KeyEncoder.ofBucketKeyEncoder(info.getRowType(), info.getBucketKeys(), lakeFormat);
        BucketingFunction bucketing = BucketingFunction.of(lakeFormat);
        Set<Integer> buckets = new HashSet<>();
        for (Object[] values : rows) {
            buckets.add(bucketing.bucketing(encoder.encodeKey(row(values)), NUM_BUCKETS));
        }
        assertThat(buckets).hasSizeGreaterThan(1);
    }

    /** Looks up every distinct value of the columns and expects exactly the rows having it. */
    private static void assertPrefixLookups(Table table, List<Object[]> rows, String... columns)
            throws Exception {
        List<String> fieldNames = table.getTableInfo().getRowType().getFieldNames();
        Map<List<Object>, List<String>> expected = new LinkedHashMap<>();
        for (Object[] values : rows) {
            List<Object> key = new ArrayList<>();
            for (String column : columns) {
                key.add(values[fieldNames.indexOf(column)]);
            }
            expected.computeIfAbsent(key, k -> new ArrayList<>())
                    .add(
                            Arrays.stream(values)
                                    .map(String::valueOf)
                                    .collect(Collectors.joining(",")));
        }
        for (Map.Entry<List<Object>, List<String>> entry : expected.entrySet()) {
            assertThat(lookup(table, Arrays.asList(columns), entry.getKey().toArray()))
                    .as("lookup by %s = %s", Arrays.toString(columns), entry.getKey())
                    .containsExactlyInAnyOrderElementsOf(entry.getValue());
        }
    }

    private static List<String> lookup(Table table, List<String> columns, Object... key)
            throws Exception {
        RowType rowType = table.getTableInfo().getRowType();
        List<InternalRow.FieldGetter> getters = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            getters.add(InternalRow.createFieldGetter(rowType.getTypeAt(i), i));
        }
        Lookuper lookuper = table.newLookup().lookupBy(columns).createLookuper();
        return lookuper.lookup(row(key)).get().getRowList().stream()
                .map(
                        r ->
                                getters.stream()
                                        .map(getter -> String.valueOf(getter.getFieldOrNull(r)))
                                        .collect(Collectors.joining(",")))
                .collect(Collectors.toList());
    }
}
