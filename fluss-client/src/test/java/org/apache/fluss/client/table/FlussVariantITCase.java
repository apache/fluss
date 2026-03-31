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

package org.apache.fluss.client.table;

import org.apache.fluss.client.admin.ClientToServerITCaseBase;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.scanner.ScanRecord;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.client.table.scanner.log.ScanRecords;
import org.apache.fluss.client.table.writer.AppendWriter;
import org.apache.fluss.client.table.writer.UpsertWriter;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.variant.Variant;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.testutils.DataTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;

/** IT case for Variant type end-to-end: client write to Fluss server and read back. */
class FlussVariantITCase extends ClientToServerITCaseBase {

    // --------------------------------------------------------------------------------------------
    // Append (log table) with Variant
    // --------------------------------------------------------------------------------------------

    @Test
    void testAppendAndScanVariant() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_variant_append");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("data", DataTypes.VARIANT())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1).build();
        createTable(tablePath, tableDescriptor, false);

        Variant v1 = Variant.fromJson("{\"name\":\"Alice\",\"age\":30}");
        Variant v2 = Variant.fromJson("[1,2,3]");
        Variant v3 = Variant.fromJson("\"hello\"");
        Variant v4 = Variant.fromJson("42");
        Variant v5 = Variant.ofNull();

        try (Table table = conn.getTable(tablePath)) {
            AppendWriter appendWriter = table.newAppend().createWriter();
            appendWriter.append(row(1, v1));
            appendWriter.append(row(2, v2));
            appendWriter.append(row(3, v3));
            appendWriter.append(row(4, v4));
            appendWriter.append(row(5, v5));
            appendWriter.flush();

            // Read back via log scanner
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);

            List<InternalRow> results = new ArrayList<>();
            while (results.size() < 5) {
                ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : scanRecords) {
                    assertThat(record.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    results.add(record.getRow());
                }
            }
            logScanner.close();

            // Verify variant data
            assertThat(results).hasSize(5);

            // Row 1: JSON object
            Variant readV1 = results.get(0).getVariant(1);
            assertThat(readV1.isObject()).isTrue();
            assertThat(readV1.getFieldByName("name").getString()).isEqualTo("Alice");
            assertThat(readV1.getFieldByName("age").getByte()).isEqualTo((byte) 30);

            // Row 2: JSON array
            Variant readV2 = results.get(1).getVariant(1);
            assertThat(readV2.isArray()).isTrue();
            assertThat(readV2.arraySize()).isEqualTo(3);

            // Row 3: string
            Variant readV3 = results.get(2).getVariant(1);
            assertThat(readV3.getString()).isEqualTo("hello");

            // Row 4: number
            Variant readV4 = results.get(3).getVariant(1);
            assertThat(readV4.getByte()).isEqualTo((byte) 42);

            // Row 5: variant null
            Variant readV5 = results.get(4).getVariant(1);
            assertThat(readV5.isNull()).isTrue();
        }
    }

    // --------------------------------------------------------------------------------------------
    // Upsert and Lookup (PK table) with Variant
    // --------------------------------------------------------------------------------------------

    @Test
    void testUpsertAndLookupVariant() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_variant_upsert");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("data", DataTypes.VARIANT())
                        .primaryKey("id")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();
        createTable(tablePath, tableDescriptor, false);

        Variant v1 = Variant.fromJson("{\"event\":\"click\",\"count\":10}");
        Variant v2 = Variant.fromJson("[\"tag1\",\"tag2\"]");

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            upsertWriter.upsert(row(1, v1));
            upsertWriter.upsert(row(2, v2));
            upsertWriter.flush();

            // Lookup by primary key
            Lookuper lookuper = table.newLookup().createLookuper();

            InternalRow result1 = lookupRow(lookuper, row(1));
            assertThat(result1).isNotNull();
            Variant readV1 = result1.getVariant(1);
            assertThat(readV1.isObject()).isTrue();
            assertThat(readV1.getFieldByName("event").getString()).isEqualTo("click");
            assertThat(readV1.getFieldByName("count").getByte()).isEqualTo((byte) 10);

            InternalRow result2 = lookupRow(lookuper, row(2));
            assertThat(result2).isNotNull();
            Variant readV2 = result2.getVariant(1);
            assertThat(readV2.isArray()).isTrue();
            assertThat(readV2.arraySize()).isEqualTo(2);
            assertThat(readV2.getElementAt(0).getString()).isEqualTo("tag1");

            // Update existing key with new variant value
            Variant v1Updated = Variant.fromJson("{\"event\":\"click\",\"count\":20}");
            upsertWriter.upsert(row(1, v1Updated));
            upsertWriter.flush();

            InternalRow result1Updated = lookupRow(lookuper, row(1));
            assertThat(result1Updated).isNotNull();
            Variant readV1Updated = result1Updated.getVariant(1);
            assertThat(readV1Updated.getFieldByName("count").getByte()).isEqualTo((byte) 20);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Shredding + projection read
    // --------------------------------------------------------------------------------------------

    @Test
    void testShreddingWithProjectionRead() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_variant_shredding_projection");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("data", DataTypes.VARIANT())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property("table.variant.shredding.enabled", "true")
                        .property("table.variant.shredding.min-sample-size", "5")
                        .build();
        createTable(tablePath, tableDescriptor, false);

        int totalRecords = 15;

        // Write records with consistent STRING fields to trigger local shredding inference.
        // With Writer-independent approach, the Writer infers the shredding schema locally
        // and uses it for subsequent batches. No server-side schema evolution occurs.
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < totalRecords; i++) {
                writer.append(
                        row(
                                i,
                                Variant.fromJson(
                                        String.format(
                                                "{\"name\":\"user_%d\",\"city\":\"city_%d\"}",
                                                i, i))));
            }
            writer.flush();
        }

        // Read back with projection — only the variant column [1]
        try (Table table = conn.getTable(tablePath)) {
            LogScanner logScanner = createLogScanner(table, new int[] {1});
            subscribeFromBeginning(logScanner, table);

            List<InternalRow> results = new ArrayList<>();
            while (results.size() < totalRecords) {
                ScanRecords records = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    assertThat(record.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    results.add(record.getRow());
                }
            }
            logScanner.close();

            // Verify all variant values are correctly reconstructed
            assertThat(results).hasSize(totalRecords);
            for (int i = 0; i < totalRecords; i++) {
                Variant v = results.get(i).getVariant(0); // projected index 0 = "data"
                assertThat(v.isObject()).isTrue();
                assertThat(v.getFieldByName("name").getString()).isEqualTo("user_" + i);
                assertThat(v.getFieldByName("city").getString()).isEqualTo("city_" + i);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Design-doc example: type mismatch + non-shredded fields + projection pushdown
    // --------------------------------------------------------------------------------------------

    /**
     * Shredding schema inferred from warm-up data: name(STRING), age(BIGINT). Two test rows:
     *
     * <pre>
     *   Row 0: {"name":"alice", "age":30,        "email":"a@b.com"}
     *   Row 1: {"name":"bob",   "age":"unknown", "city":"beijing"}
     *                              ↑ type mismatch BIGINT
     * </pre>
     *
     * <p>Verifies 4 read scenarios from the design doc:
     *
     * <ol>
     *   <li>Read "name" — shredded, type match → zero deserialization path
     *   <li>Read "age" — shredded, type mismatch in Row 1 → per-field value fallback
     *   <li>Read "email"/"city" — non-shredded → residual decode path
     *   <li>Full reconstruction — merge shredded + residual
     * </ol>
     *
     * <p>Also validates column-level projection pushdown ({@code [1]} = variant-only).
     */
    @Test
    void testShreddingDesignDocExampleWithProjection() throws Exception {
        TablePath tablePath =
                TablePath.of("test_db_1", "test_variant_shredding_design_doc_example");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("data", DataTypes.VARIANT())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property("table.variant.shredding.enabled", "true")
                        .property("table.variant.shredding.min-sample-size", "5")
                        .build();
        createTable(tablePath, tableDescriptor, false);

        int warmUpCount = 10;
        int totalRecords = warmUpCount + 2;

        // Phase 1: write warm-up rows to trigger shredding inference for name(STRING), age(BIGINT)
        // Phase 2: write the 2 design-doc example rows through the shredded writer path
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < warmUpCount; i++) {
                writer.append(
                        row(
                                i,
                                Variant.fromJson(
                                        String.format(
                                                "{\"name\":\"user_%d\",\"age\":%d}", i, i * 10))));
            }
            writer.flush();

            // These 2 rows exercise shredding with type mismatch and non-shredded fields
            writer.append(
                    row(
                            100,
                            Variant.fromJson(
                                    "{\"name\":\"alice\",\"age\":30,\"email\":\"a@b.com\"}")));
            writer.append(
                    row(
                            101,
                            Variant.fromJson(
                                    "{\"name\":\"bob\",\"age\":\"unknown\",\"city\":\"beijing\"}")));
            writer.flush();
        }

        // ---- Read scenario A: full scan (no column projection) ----
        try (Table table = conn.getTable(tablePath)) {
            LogScanner logScanner = createLogScanner(table);
            subscribeFromBeginning(logScanner, table);

            List<InternalRow> results = new ArrayList<>();
            while (results.size() < totalRecords) {
                ScanRecords records = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    results.add(record.getRow());
                }
            }
            logScanner.close();

            assertThat(results).hasSize(totalRecords);
            // variantIndex = 1 (schema: [id, data])
            verifyDesignDocRow0(results.get(warmUpCount).getVariant(1));
            verifyDesignDocRow1(results.get(warmUpCount + 1).getVariant(1));
        }

        // ---- Read scenario B: projection — only variant column [1] ----
        try (Table table = conn.getTable(tablePath)) {
            LogScanner logScanner = createLogScanner(table, new int[] {1});
            subscribeFromBeginning(logScanner, table);

            List<InternalRow> results = new ArrayList<>();
            while (results.size() < totalRecords) {
                ScanRecords records = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    results.add(record.getRow());
                }
            }
            logScanner.close();

            assertThat(results).hasSize(totalRecords);
            // projected index 0 = "data"
            verifyDesignDocRow0(results.get(warmUpCount).getVariant(0));
            verifyDesignDocRow1(results.get(warmUpCount + 1).getVariant(0));
        }

        // ---- Read scenario C: projection — only id column [0] ----
        try (Table table = conn.getTable(tablePath)) {
            LogScanner logScanner = createLogScanner(table, new int[] {0});
            subscribeFromBeginning(logScanner, table);

            List<InternalRow> results = new ArrayList<>();
            while (results.size() < totalRecords) {
                ScanRecords records = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    results.add(record.getRow());
                }
            }
            logScanner.close();

            assertThat(results).hasSize(totalRecords);
            assertThat(results.get(warmUpCount).getInt(0)).isEqualTo(100);
            assertThat(results.get(warmUpCount + 1).getInt(0)).isEqualTo(101);
        }
    }

    /**
     * Verify design-doc Row 0: {"name":"alice", "age":30, "email":"a@b.com"}.
     *
     * <ul>
     *   <li>"name" — shredded STRING, type match
     *   <li>"age" — shredded BIGINT, type match (encoded as INT8 or INT64 depending on path)
     *   <li>"email" — non-shredded, from residual
     *   <li>"city" — absent
     * </ul>
     */
    private void verifyDesignDocRow0(Variant v) {
        assertThat(v.isObject()).isTrue();
        assertThat(v.getFieldByName("name").getString()).isEqualTo("alice");
        // age=30 may be INT8 (un-shredded) or INT64 (shredded), verify via toJson()
        assertThat(v.getFieldByName("age").toJson()).isEqualTo("30");
        assertThat(v.getFieldByName("email").getString()).isEqualTo("a@b.com");
        assertThat(v.getFieldByName("city")).isNull();
    }

    /**
     * Verify design-doc Row 1: {"name":"bob", "age":"unknown", "city":"beijing"}.
     *
     * <ul>
     *   <li>"name" — shredded STRING, type match
     *   <li>"age" — shredded BIGINT but value is STRING → type mismatch, per-field value fallback
     *   <li>"email" — absent
     *   <li>"city" — non-shredded, from residual
     * </ul>
     */
    private void verifyDesignDocRow1(Variant v) {
        assertThat(v.isObject()).isTrue();
        assertThat(v.getFieldByName("name").getString()).isEqualTo("bob");
        assertThat(v.getFieldByName("age").getString()).isEqualTo("unknown");
        assertThat(v.getFieldByName("email")).isNull();
        assertThat(v.getFieldByName("city").getString()).isEqualTo("beijing");
    }

    // --------------------------------------------------------------------------------------------
    // Variant sub-field projection API
    // --------------------------------------------------------------------------------------------

    /**
     * Verifies that the {@code variantFieldProjection} API works end-to-end.
     *
     * <p>The API passes sub-field hints to the server so that only the relevant {@code typed_value}
     * children are transferred when a Variant column has been shredded. The hint is a transfer
     * optimisation; the full variant value is still returned.
     *
     * <p>Two read scenarios:
     *
     * <ol>
     *   <li>Column projection {@code [1]} + sub-field hint {@code "name"}
     *   <li>Full schema (no column projection) + sub-field hint {@code "name","city"}
     * </ol>
     */
    @Test
    void testVariantSubFieldProjection() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_variant_sub_field_projection");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("data", DataTypes.VARIANT())
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .property("table.variant.shredding.enabled", "true")
                        .property("table.variant.shredding.min-sample-size", "5")
                        .build();
        createTable(tablePath, tableDescriptor, false);

        int warmUpCount = 10;
        int testCount = 5;
        int totalRecords = warmUpCount + testCount;

        // Phase 1: warm-up to trigger shredding inference for name(STRING), city(STRING)
        // Phase 2: test rows through the shredded writer path
        try (Table table = conn.getTable(tablePath)) {
            AppendWriter writer = table.newAppend().createWriter();
            for (int i = 0; i < warmUpCount; i++) {
                writer.append(
                        row(
                                i,
                                Variant.fromJson(
                                        String.format(
                                                "{\"name\":\"user_%d\",\"city\":\"city_%d\",\"score\":%d}",
                                                i, i, i * 10))));
            }
            writer.flush();

            for (int i = 0; i < testCount; i++) {
                int idx = warmUpCount + i;
                writer.append(
                        row(
                                idx,
                                Variant.fromJson(
                                        String.format(
                                                "{\"name\":\"user_%d\",\"city\":\"city_%d\",\"score\":%d}",
                                                idx, idx, idx * 10))));
            }
            writer.flush();
        }

        // ---- Scenario A: column projection [1] + sub-field hint {1 -> ["name"]} ----
        try (Table table = conn.getTable(tablePath)) {
            Map<Integer, List<String>> variantHints = new HashMap<>();
            variantHints.put(1, Arrays.asList("name"));

            LogScanner logScanner =
                    table.newScan()
                            .project(new int[] {1})
                            .variantFieldProjection(variantHints)
                            .createLogScanner();
            subscribeFromBeginning(logScanner, table);

            List<InternalRow> results = new ArrayList<>();
            while (results.size() < totalRecords) {
                ScanRecords records = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    assertThat(record.getChangeType()).isEqualTo(ChangeType.APPEND_ONLY);
                    results.add(record.getRow());
                }
            }
            logScanner.close();

            assertThat(results).hasSize(totalRecords);
            // Sub-field projection is a server-side transfer hint;
            // the full variant value is still returned.
            for (int i = 0; i < totalRecords; i++) {
                Variant v = results.get(i).getVariant(0); // projected index 0 = "data"
                assertThat(v.isObject()).isTrue();
                assertThat(v.getFieldByName("name").getString()).isEqualTo("user_" + i);
            }
        }

        // ---- Scenario B: full schema + sub-field hint {1 -> ["name","city"]} ----
        try (Table table = conn.getTable(tablePath)) {
            Map<Integer, List<String>> variantHints = new HashMap<>();
            variantHints.put(1, Arrays.asList("name", "city"));

            LogScanner logScanner =
                    table.newScan().variantFieldProjection(variantHints).createLogScanner();
            subscribeFromBeginning(logScanner, table);

            List<InternalRow> results = new ArrayList<>();
            while (results.size() < totalRecords) {
                ScanRecords records = logScanner.poll(Duration.ofSeconds(1));
                for (ScanRecord record : records) {
                    results.add(record.getRow());
                }
            }
            logScanner.close();

            assertThat(results).hasSize(totalRecords);
            for (int i = 0; i < totalRecords; i++) {
                Variant v = results.get(i).getVariant(1); // full schema index 1 = "data"
                assertThat(v.isObject()).isTrue();
                assertThat(v.getFieldByName("name").getString()).isEqualTo("user_" + i);
                assertThat(v.getFieldByName("city").getString()).isEqualTo("city_" + i);
            }
        }
    }

    // --------------------------------------------------------------------------------------------
    // Null Variant column value (SQL NULL, not JSON null)
    // --------------------------------------------------------------------------------------------

    @Test
    void testNullVariantColumn() throws Exception {
        TablePath tablePath = TablePath.of("test_db_1", "test_variant_null_column");
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("data", DataTypes.VARIANT())
                        .primaryKey("id")
                        .build();
        TableDescriptor tableDescriptor =
                TableDescriptor.builder().schema(schema).distributedBy(1, "id").build();
        createTable(tablePath, tableDescriptor, false);

        try (Table table = conn.getTable(tablePath)) {
            UpsertWriter upsertWriter = table.newUpsert().createWriter();
            // null column value (SQL NULL, not JSON null)
            upsertWriter.upsert(row(1, null));
            upsertWriter.upsert(row(2, Variant.fromJson("{\"key\":\"value\"}")));
            upsertWriter.flush();

            Lookuper lookuper = table.newLookup().createLookuper();

            InternalRow result1 = lookupRow(lookuper, row(1));
            assertThat(result1).isNotNull();
            assertThat(result1.isNullAt(1)).isTrue();

            InternalRow result2 = lookupRow(lookuper, row(2));
            assertThat(result2).isNotNull();
            assertThat(result2.isNullAt(1)).isFalse();
            assertThat(result2.getVariant(1).isObject()).isTrue();
        }
    }
}
