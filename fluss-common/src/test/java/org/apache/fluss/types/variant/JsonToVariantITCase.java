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

package org.apache.fluss.types.variant;

import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test for JSON to Variant conversion with Arrow read/write roundtrip.
 *
 * <p>This test covers the full pipeline: JSON String → Variant → Arrow write → Arrow read → Variant
 * → JSON String, verifying data integrity through the entire flow.
 */
class JsonToVariantITCase {

    // --------------------------------------------------------------------------------------------
    // Simple primitives through JSON → Variant → Arrow
    // --------------------------------------------------------------------------------------------

    @Test
    void testJsonPrimitivesRoundtrip() throws IOException {
        // Schema: (id INT, data VARIANT)
        RowType rowType = DataTypes.ROW(new DataType[] {DataTypes.INT(), DataTypes.VARIANT()});

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(1, Variant.fromJson("null")),
                        GenericRow.of(2, Variant.fromJson("true")),
                        GenericRow.of(3, Variant.fromJson("false")),
                        GenericRow.of(4, Variant.fromJson("42")),
                        GenericRow.of(5, Variant.fromJson("3.14")),
                        GenericRow.of(6, Variant.fromJson("\"hello world\"")));

        writeReadAndVerify(
                rowType,
                rows,
                results -> {
                    assertThat(results).hasSize(6);

                    // Row 1: null variant
                    assertThat(results.get(0).getInt(0)).isEqualTo(1);
                    assertThat(results.get(0).getVariant(1).isNull()).isTrue();

                    // Row 2: true
                    assertThat(results.get(1).getInt(0)).isEqualTo(2);
                    assertThat(results.get(1).getVariant(1).getBoolean()).isTrue();

                    // Row 3: false
                    assertThat(results.get(2).getVariant(1).getBoolean()).isFalse();

                    // Row 4: integer 42 (encoded as int8 by JSON parser)
                    assertThat(results.get(3).getVariant(1).getByte()).isEqualTo((byte) 42);

                    // Row 5: decimal 3.14 (decimal point without exponent => Decimal)
                    assertThat(results.get(4).getVariant(1).getDecimal())
                            .isEqualByComparingTo(new java.math.BigDecimal("3.14"));

                    // Row 6: string
                    assertThat(results.get(5).getVariant(1).getString()).isEqualTo("hello world");
                });
    }

    // --------------------------------------------------------------------------------------------
    // JSON objects through Arrow
    // --------------------------------------------------------------------------------------------

    @Test
    void testJsonObjectsRoundtrip() throws IOException {
        RowType rowType = DataTypes.ROW(new DataType[] {DataTypes.STRING(), DataTypes.VARIANT()});

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(
                                fromString("simple"),
                                Variant.fromJson("{\"name\":\"Alice\",\"age\":30}")),
                        GenericRow.of(
                                fromString("nested"),
                                Variant.fromJson(
                                        "{\"user\":{\"name\":\"Bob\"},\"scores\":[90,85,95]}")),
                        GenericRow.of(fromString("empty"), Variant.fromJson("{}")));

        writeReadAndVerify(
                rowType,
                rows,
                results -> {
                    assertThat(results).hasSize(3);

                    // Row 1: simple object
                    Variant v1 = results.get(0).getVariant(1);
                    assertThat(v1.isObject()).isTrue();
                    assertThat(v1.getFieldByName("name").getString()).isEqualTo("Alice");
                    assertThat(v1.getFieldByName("age").getByte()).isEqualTo((byte) 30);

                    // Row 2: nested object
                    Variant v2 = results.get(1).getVariant(1);
                    assertThat(v2.isObject()).isTrue();
                    assertThat(v2.getFieldByName("user").isObject()).isTrue();
                    assertThat(v2.getFieldByName("user").getFieldByName("name").getString())
                            .isEqualTo("Bob");
                    assertThat(v2.getFieldByName("scores").isArray()).isTrue();
                    assertThat(v2.getFieldByName("scores").arraySize()).isEqualTo(3);

                    // Row 3: empty object
                    Variant v3 = results.get(2).getVariant(1);
                    assertThat(v3.isObject()).isTrue();
                    assertThat(v3.objectSize()).isZero();
                });
    }

    // --------------------------------------------------------------------------------------------
    // JSON arrays through Arrow
    // --------------------------------------------------------------------------------------------

    @Test
    void testJsonArraysRoundtrip() throws IOException {
        RowType rowType = DataTypes.ROW(new DataType[] {DataTypes.INT(), DataTypes.VARIANT()});

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(1, Variant.fromJson("[1, 2, 3]")),
                        GenericRow.of(2, Variant.fromJson("[\"a\", \"b\", \"c\"]")),
                        GenericRow.of(3, Variant.fromJson("[1, \"mixed\", true, null]")),
                        GenericRow.of(4, Variant.fromJson("[]")),
                        GenericRow.of(5, Variant.fromJson("[[1,2],[3,4]]")));

        writeReadAndVerify(
                rowType,
                rows,
                results -> {
                    assertThat(results).hasSize(5);

                    // Row 1: int array
                    Variant arr1 = results.get(0).getVariant(1);
                    assertThat(arr1.isArray()).isTrue();
                    assertThat(arr1.arraySize()).isEqualTo(3);
                    assertThat(arr1.getElementAt(0).getByte()).isEqualTo((byte) 1);
                    assertThat(arr1.getElementAt(2).getByte()).isEqualTo((byte) 3);

                    // Row 2: string array
                    Variant arr2 = results.get(1).getVariant(1);
                    assertThat(arr2.isArray()).isTrue();
                    assertThat(arr2.getElementAt(0).getString()).isEqualTo("a");

                    // Row 3: mixed array
                    Variant arr3 = results.get(2).getVariant(1);
                    assertThat(arr3.arraySize()).isEqualTo(4);
                    assertThat(arr3.getElementAt(3).isNull()).isTrue();

                    // Row 4: empty array
                    assertThat(results.get(3).getVariant(1).arraySize()).isZero();

                    // Row 5: nested array
                    Variant arr5 = results.get(4).getVariant(1);
                    assertThat(arr5.arraySize()).isEqualTo(2);
                    assertThat(arr5.getElementAt(0).isArray()).isTrue();
                });
    }

    // --------------------------------------------------------------------------------------------
    // JSON string roundtrip (JSON → Variant → Arrow → Variant → JSON)
    // --------------------------------------------------------------------------------------------

    @Test
    void testJsonStringRoundtrip() throws IOException {
        RowType rowType = DataTypes.ROW(new DataType[] {DataTypes.VARIANT()});

        String[] jsonStrings = {
            "null",
            "true",
            "false",
            "42",
            "3.14",
            "\"hello\"",
            "{}",
            "[]",
            "{\"key\":\"value\"}",
            "[1,2,3]",
        };

        List<InternalRow> rows = new ArrayList<>();
        for (String json : jsonStrings) {
            rows.add(GenericRow.of(Variant.fromJson(json)));
        }

        writeReadAndVerify(
                rowType,
                rows,
                results -> {
                    assertThat(results).hasSize(jsonStrings.length);
                    for (int i = 0; i < jsonStrings.length; i++) {
                        Variant readBack = results.get(i).getVariant(0);
                        assertThat(readBack.toJson()).isEqualTo(jsonStrings[i]);
                    }
                });
    }

    // --------------------------------------------------------------------------------------------
    // Complex real-world JSON documents
    // --------------------------------------------------------------------------------------------

    @Test
    void testComplexJsonDocuments() throws IOException {
        RowType rowType = DataTypes.ROW(new DataType[] {DataTypes.INT(), DataTypes.VARIANT()});

        String json1 =
                "{\"event\":\"page_view\","
                        + "\"timestamp\":1234567890,"
                        + "\"user\":{\"id\":1001,\"name\":\"Alice\",\"tags\":[\"premium\",\"active\"]},"
                        + "\"properties\":{\"page\":\"/home\",\"duration\":5.2}}";

        String json2 =
                "{\"event\":\"purchase\","
                        + "\"timestamp\":1234567891,"
                        + "\"user\":{\"id\":1002,\"name\":\"Bob\"},"
                        + "\"properties\":{\"item\":\"widget\",\"price\":29.99,\"quantity\":2}}";

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(1, Variant.fromJson(json1)),
                        GenericRow.of(2, Variant.fromJson(json2)));

        writeReadAndVerify(
                rowType,
                rows,
                results -> {
                    assertThat(results).hasSize(2);

                    // Verify first document
                    Variant v1 = results.get(0).getVariant(1);
                    assertThat(v1.getFieldByName("event").getString()).isEqualTo("page_view");
                    Variant user1 = v1.getFieldByName("user");
                    assertThat(user1.getFieldByName("name").getString()).isEqualTo("Alice");
                    Variant tags = user1.getFieldByName("tags");
                    assertThat(tags.isArray()).isTrue();
                    assertThat(tags.getElementAt(0).getString()).isEqualTo("premium");

                    // Verify second document
                    Variant v2 = results.get(1).getVariant(1);
                    assertThat(v2.getFieldByName("event").getString()).isEqualTo("purchase");
                    Variant props2 = v2.getFieldByName("properties");
                    assertThat(props2.getFieldByName("price").getDecimal())
                            .isEqualByComparingTo(new java.math.BigDecimal("29.99"));
                });
    }

    // --------------------------------------------------------------------------------------------
    // Null variant column values
    // --------------------------------------------------------------------------------------------

    @Test
    void testNullVariantColumn() throws IOException {
        RowType rowType = DataTypes.ROW(new DataType[] {DataTypes.INT(), DataTypes.VARIANT()});

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(1, Variant.fromJson("{\"key\":\"value\"}")),
                        GenericRow.of(2, null), // null column value (not JSON null)
                        GenericRow.of(3, Variant.fromJson("42")));

        writeReadAndVerify(
                rowType,
                rows,
                results -> {
                    assertThat(results).hasSize(3);
                    assertThat(results.get(0).getVariant(1).isObject()).isTrue();
                    assertThat(results.get(1).isNullAt(1)).isTrue();
                    assertThat(results.get(2).getVariant(1).getByte()).isEqualTo((byte) 42);
                });
    }

    // --------------------------------------------------------------------------------------------
    // Multiple variant columns
    // --------------------------------------------------------------------------------------------

    @Test
    void testMultipleVariantColumns() throws IOException {
        RowType rowType =
                DataTypes.ROW(
                        new DataType[] {DataTypes.INT(), DataTypes.VARIANT(), DataTypes.VARIANT()});

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(
                                1,
                                Variant.fromJson("{\"type\":\"a\"}"),
                                Variant.fromJson("[1,2,3]")),
                        GenericRow.of(
                                2,
                                Variant.fromJson("{\"type\":\"b\"}"),
                                Variant.fromJson("\"text\"")));

        writeReadAndVerify(
                rowType,
                rows,
                results -> {
                    assertThat(results).hasSize(2);

                    // Row 1
                    assertThat(results.get(0).getVariant(1).getFieldByName("type").getString())
                            .isEqualTo("a");
                    assertThat(results.get(0).getVariant(2).isArray()).isTrue();

                    // Row 2
                    assertThat(results.get(1).getVariant(1).getFieldByName("type").getString())
                            .isEqualTo("b");
                    assertThat(results.get(1).getVariant(2).getString()).isEqualTo("text");
                });
    }

    // --------------------------------------------------------------------------------------------
    // JSON with special characters
    // --------------------------------------------------------------------------------------------

    @Test
    void testJsonWithSpecialCharacters() throws IOException {
        RowType rowType = DataTypes.ROW(new DataType[] {DataTypes.VARIANT()});

        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(Variant.fromJson("{\"msg\":\"hello\\nworld\"}")),
                        GenericRow.of(Variant.fromJson("{\"path\":\"C:\\\\Users\\\\test\"}")),
                        GenericRow.of(Variant.fromJson("{\"quote\":\"He said \\\"hi\\\"\"}")));

        writeReadAndVerify(
                rowType,
                rows,
                results -> {
                    assertThat(results).hasSize(3);
                    assertThat(results.get(0).getVariant(0).getFieldByName("msg").getString())
                            .isEqualTo("hello\nworld");
                    assertThat(results.get(1).getVariant(0).getFieldByName("path").getString())
                            .isEqualTo("C:\\Users\\test");
                    assertThat(results.get(2).getVariant(0).getFieldByName("quote").getString())
                            .isEqualTo("He said \"hi\"");
                });
    }

    // --------------------------------------------------------------------------------------------
    // Variant binary serialization roundtrip through Arrow
    // --------------------------------------------------------------------------------------------

    @Test
    void testVariantBinarySerializationIntegrity() throws IOException {
        RowType rowType = DataTypes.ROW(new DataType[] {DataTypes.VARIANT()});

        // Create variants with factory methods (not from JSON)
        List<InternalRow> rows =
                Arrays.asList(
                        GenericRow.of(Variant.ofNull()),
                        GenericRow.of(Variant.ofBoolean(true)),
                        GenericRow.of(Variant.ofInt(Integer.MAX_VALUE)),
                        GenericRow.of(Variant.ofLong(Long.MAX_VALUE)),
                        GenericRow.of(Variant.ofFloat(Float.MAX_VALUE)),
                        GenericRow.of(Variant.ofDouble(Double.MAX_VALUE)),
                        GenericRow.of(Variant.ofString("test string")));

        writeReadAndVerify(
                rowType,
                rows,
                results -> {
                    assertThat(results).hasSize(7);
                    assertThat(results.get(0).getVariant(0).isNull()).isTrue();
                    assertThat(results.get(1).getVariant(0).getBoolean()).isTrue();
                    assertThat(results.get(2).getVariant(0).getInt()).isEqualTo(Integer.MAX_VALUE);
                    assertThat(results.get(3).getVariant(0).getLong()).isEqualTo(Long.MAX_VALUE);
                    assertThat(results.get(4).getVariant(0).getFloat()).isEqualTo(Float.MAX_VALUE);
                    assertThat(results.get(5).getVariant(0).getDouble())
                            .isEqualTo(Double.MAX_VALUE);
                    assertThat(results.get(6).getVariant(0).getString()).isEqualTo("test string");
                });
    }

    // --------------------------------------------------------------------------------------------
    // Helper: write rows to Arrow and read them back, then verify within resource scope
    // --------------------------------------------------------------------------------------------

    private void writeReadAndVerify(
            RowType rowType, List<InternalRow> rows, Consumer<List<ColumnarRow>> verifier)
            throws IOException {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
                ArrowWriterPool pool = new ArrowWriterPool(allocator);
                ArrowWriter writer =
                        pool.getOrCreateWriter(1L, 1, Integer.MAX_VALUE, rowType, NO_COMPRESSION)) {

            for (InternalRow row : rows) {
                writer.writeRow(row);
            }

            AbstractPagedOutputView pagedOutputView =
                    new ManagedPagedOutputView(new TestingMemorySegmentPool(64 * 1024));

            int size =
                    writer.serializeToOutputView(
                            pagedOutputView, recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE));
            int heapMemorySize = Math.max(size, writer.estimatedSizeInBytes());
            MemorySegment segment = MemorySegment.allocateHeapMemory(heapMemorySize);

            MemorySegment firstSegment = pagedOutputView.getCurrentSegment();
            firstSegment.copyTo(recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE), segment, 0, size);

            ArrowReader reader =
                    ArrowUtils.createArrowReader(segment, 0, size, root, allocator, rowType);

            List<ColumnarRow> results = new ArrayList<>();
            int rowCount = reader.getRowCount();
            for (int i = 0; i < rowCount; i++) {
                ColumnarRow row = reader.read(i);
                row.setRowId(i);
                results.add(row);
            }
            verifier.accept(results);
        }
    }
}
