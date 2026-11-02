/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fluss.row;

import org.apache.fluss.memory.AbstractPagedOutputView;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.MemorySegment;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.row.arrow.ArrowReader;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.columnar.ColumnarRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.fluss.testutils.DataTestUtils;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.ArrowUtils;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.fluss.compression.ArrowCompressionInfo.NO_COMPRESSION;
import static org.apache.fluss.record.LogRecordBatch.CURRENT_LOG_MAGIC_VALUE;
import static org.apache.fluss.record.LogRecordBatchFormat.recordBatchHeaderSize;
import static org.apache.fluss.row.BinaryString.fromString;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link InternalRow#createFieldToString} and {@link InternalRow#createFieldToStrings}.
 */
public class FieldToStringTest {

    @Test
    void testScalarTypes() {
        DataType[] types = {
            DataTypes.BOOLEAN(),
            DataTypes.TINYINT(),
            DataTypes.SMALLINT(),
            DataTypes.INT(),
            DataTypes.BIGINT(),
            DataTypes.FLOAT(),
            DataTypes.DOUBLE(),
            DataTypes.STRING(),
            DataTypes.CHAR(3),
            DataTypes.DECIMAL(10, 2)
        };
        Object[] values = {
            true,
            (byte) 1,
            (short) 2,
            3,
            4L,
            5.5f,
            6.25,
            fromString("hello"),
            fromString("abc"),
            Decimal.fromUnscaledLong(150, 10, 2)
        };
        String[] expected = {"true", "1", "2", "3", "4", "5.5", "6.25", "hello", "abc", "1.50"};
        assertFieldToStrings(types, values, expected);
    }

    @Test
    void testNegativeAndLargeValues() {
        DataType[] types = {
            DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DECIMAL(38, 18), DataTypes.DOUBLE()
        };
        Object[] values = {
            Integer.MIN_VALUE,
            Long.MIN_VALUE,
            Decimal.fromBigDecimal(new BigDecimal("-0.123456789012345678"), 38, 18),
            -1.5e-10
        };
        String[] expected = {
            String.valueOf(Integer.MIN_VALUE),
            String.valueOf(Long.MIN_VALUE),
            "-0.123456789012345678",
            "-1.5E-10"
        };
        assertFieldToStrings(types, values, expected);
    }

    @Test
    void testTemporalTypes() {
        int days20261030 = (int) LocalDate.of(2026, 10, 30).toEpochDay();
        DataType[] types = {
            DataTypes.DATE(),
            DataTypes.DATE(),
            DataTypes.DATE(),
            DataTypes.TIME(0),
            DataTypes.TIME(3),
            DataTypes.TIME(9),
            DataTypes.TIME(3),
            DataTypes.TIME(3),
            DataTypes.TIMESTAMP(0),
            DataTypes.TIMESTAMP(3),
            DataTypes.TIMESTAMP(6),
            DataTypes.TIMESTAMP(9),
            DataTypes.TIMESTAMP_LTZ(3),
            DataTypes.TIMESTAMP_LTZ(6),
            // values carrying a non-zero fraction render without the fraction when the declared
            // precision is 0
            DataTypes.TIME(0),
            DataTypes.TIMESTAMP(0),
            DataTypes.TIMESTAMP_LTZ(0)
        };
        Object[] values = {
            0,
            -1,
            days20261030,
            0,
            48930123,
            48930123,
            48930000,
            48930120,
            TimestampNtz.fromLocalDateTime(LocalDateTime.of(2026, 10, 30, 13, 35, 30)),
            TimestampNtz.fromLocalDateTime(LocalDateTime.of(2026, 10, 30, 13, 35, 30, 123456000)),
            TimestampNtz.fromLocalDateTime(LocalDateTime.of(2026, 10, 30, 13, 35, 30, 123456000)),
            TimestampNtz.fromLocalDateTime(LocalDateTime.of(2026, 10, 30, 13, 35, 30, 123000000)),
            TimestampLtz.fromInstant(Instant.parse("2026-10-30T05:35:30.123Z")),
            TimestampLtz.fromInstant(Instant.parse("2026-10-30T05:35:30.123456Z")),
            48930123,
            TimestampNtz.fromLocalDateTime(LocalDateTime.of(2026, 10, 30, 13, 35, 30, 123000000)),
            TimestampLtz.fromInstant(Instant.parse("2026-10-30T05:35:30.123Z"))
        };
        String[] expected = {
            "1970-01-01",
            "1969-12-31",
            "2026-10-30",
            "00:00:00",
            "13:35:30.123",
            "13:35:30.123",
            "13:35:30",
            "13:35:30.12",
            "2026-10-30 13:35:30",
            "2026-10-30 13:35:30.123",
            "2026-10-30 13:35:30.123456",
            "2026-10-30 13:35:30.123",
            "2026-10-30 05:35:30.123Z",
            "2026-10-30 05:35:30.123456Z",
            "13:35:30",
            "2026-10-30 13:35:30",
            "2026-10-30 05:35:30Z"
        };
        assertFieldToStrings(types, values, expected);
    }

    @Test
    void testCharShorterThanDeclaredLength() {
        // IndexedRowWriter zero-pads a CHAR(n) value shorter than n to the fixed length and
        // IndexedRow.getChar trims the trailing zero bytes on read, so a short value survives the
        // IndexedRow write/read path and renders the same as in a GenericRow
        DataType[] types = {DataTypes.CHAR(5)};
        Object[] values = {fromString("ab")};
        String[] expected = {"ab"};
        assertFieldToStrings(types, values, expected);
    }

    @Test
    void testBinaryTypes() {
        DataType[] types = {DataTypes.BYTES(), DataTypes.BINARY(4), DataTypes.BYTES()};
        // IndexedRow pads fixed-length BINARY with zero bytes on write but trims trailing zero
        // bytes on read, so a value not ending with zero bytes renders the same as in GenericRow
        Object[] values = {new byte[] {1, 2, 3}, new byte[] {1, 2}, new byte[] {}};
        String[] expected = {"AQID", "AQI=", ""};
        assertFieldToStrings(types, values, expected);
        // nested binary renders as a quoted Base64 string, same encoding as the top-level one
        assertThat(
                        toStringOf(
                                DataTypes.ARRAY(DataTypes.BYTES()),
                                GenericRow.of(GenericArray.of(new byte[] {1, 2, 3}))))
                .isEqualTo("[\"AQID\"]");
    }

    @Test
    void testBinaryTrailingZeroByteIsTrimmedOnlyByIndexedRow() throws Exception {
        // Pins the known behavior documented in InternalRow#createFieldToString: IndexedRow,
        // including rows nested inside an indexed row, strips trailing 0x00 bytes when reading a
        // BINARY(n) field, because its fixed-length encoding does not record the actual length,
        // while GenericRow, CompactedRow, ColumnarRow and elements inside nested arrays return
        // the bytes as stored.
        byte[] value = new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, 0x00};

        // top-level field: GenericRow renders all four bytes, IndexedRow renders three
        assertThat(toStringOf(DataTypes.BINARY(4), GenericRow.of(value))).isEqualTo("yv66AA==");
        // CompactedRow renders the bytes as stored, like GenericRow
        assertThat(
                        toStringOf(
                                DataTypes.BINARY(4),
                                DataTestUtils.compactedRow(
                                        RowType.of(DataTypes.BINARY(4)), new Object[] {value})))
                .isEqualTo("yv66AA==");
        // so does ColumnarRow, the implementation the default arrow log format is read back as
        assertColumnarFieldToStrings(
                DataTypes.ROW(DataTypes.FIELD("b", DataTypes.BINARY(4))),
                GenericRow.of(value),
                new String[] {"yv66AA=="});
        assertThat(
                        toStringOf(
                                DataTypes.BINARY(4),
                                DataTestUtils.indexedRow(
                                        RowType.of(DataTypes.BINARY(4)), new Object[] {value})))
                .isEqualTo("yv66");

        // same value nested in an array inside an indexed row: rendered as stored
        assertThat(
                        toStringOf(
                                DataTypes.ARRAY(DataTypes.BINARY(4)),
                                DataTestUtils.indexedRow(
                                        RowType.of(DataTypes.ARRAY(DataTypes.BINARY(4))),
                                        new Object[] {GenericArray.of(value)})))
                .isEqualTo("[\"yv66AA==\"]");

        // same value nested in a row inside an indexed row: trimmed like a top-level field
        assertThat(
                        toStringOf(
                                DataTypes.ROW(new DataField("f", DataTypes.BINARY(4))),
                                DataTestUtils.indexedRow(
                                        RowType.of(
                                                DataTypes.ROW(
                                                        new DataField("f", DataTypes.BINARY(4)))),
                                        new Object[] {GenericRow.of(value)})))
                .isEqualTo("{\"f\":\"yv66\"}");
    }

    @Test
    void testComplexTypesAcrossRowImplementations() {
        DataType rowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()));
        DataType[] types = {
            DataTypes.ARRAY(DataTypes.STRING()),
            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
            rowType,
            // decimal and float elements keep their own string form through the IndexedRow
            // write/read path as well (no widening, no scientific notation)
            DataTypes.ARRAY(DataTypes.DECIMAL(10, 2)),
            DataTypes.ARRAY(DataTypes.DECIMAL(18, 8)),
            DataTypes.ARRAY(DataTypes.FLOAT()),
            // non-string map keys through the IndexedRow path
            DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()),
            // array of rows
            DataTypes.ARRAY(rowType)
        };
        Object[] values = {
            GenericArray.of(fromString("x"), fromString("y")),
            GenericMap.of(fromString("k"), 1),
            GenericRow.of(1, fromString("a")),
            GenericArray.of(Decimal.fromUnscaledLong(150, 10, 2)),
            GenericArray.of(Decimal.fromBigDecimal(new BigDecimal("0.00000025"), 18, 8)),
            GenericArray.of(0.1f, 0.2f),
            GenericMap.of(1L, fromString("x")),
            GenericArray.of(GenericRow.of(1, fromString("a")))
        };
        String[] expected = {
            "[\"x\",\"y\"]",
            "{\"k\":1}",
            "{\"id\":1,\"name\":\"a\"}",
            "[1.50]",
            "[0.00000025]",
            "[0.1,0.2]",
            "{\"1\":\"x\"}",
            "[{\"id\":1,\"name\":\"a\"}]"
        };
        assertFieldToStrings(types, values, expected);
    }

    @Test
    void testColumnarRowAfterArrowRoundTrip() throws Exception {
        // arrow is the default log format and is read back as a ColumnarRow; the values are
        // chosen from the cases prone to diverging across row implementations
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("s", DataTypes.STRING()),
                        DataTypes.FIELD("c", DataTypes.CHAR(5)),
                        DataTypes.FIELD("b", DataTypes.BINARY(4)),
                        DataTypes.FIELD("t", DataTypes.TIME(3)),
                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)),
                        DataTypes.FIELD("tsz", DataTypes.TIMESTAMP_LTZ(3)),
                        DataTypes.FIELD("arr", DataTypes.ARRAY(DataTypes.STRING())),
                        DataTypes.FIELD("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
                        DataTypes.FIELD("r", DataTypes.ROW(DataTypes.FIELD("f", DataTypes.INT()))));
        InternalRow row =
                GenericRow.of(
                        1,
                        fromString("hello"),
                        fromString("ab"),
                        new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, 0x00},
                        48930123,
                        TimestampNtz.fromLocalDateTime(
                                LocalDateTime.of(2026, 10, 30, 13, 35, 30, 123000000)),
                        TimestampLtz.fromInstant(Instant.parse("2026-10-30T05:35:30.123Z")),
                        GenericArray.of(fromString("x"), fromString("y")),
                        GenericMap.of(fromString("k"), 1),
                        GenericRow.of(7));
        String[] expected = {
            "1",
            "hello",
            "ab",
            "yv66AA==",
            "13:35:30.123",
            "2026-10-30 13:35:30.123",
            "2026-10-30 05:35:30.123Z",
            "[\"x\",\"y\"]",
            "{\"k\":1}",
            "{\"f\":7}"
        };
        assertColumnarFieldToStrings(rowType, row, expected);
    }

    @Test
    void testNullHandling() {
        // nullable field renders as Java null
        assertThat(toStringOf(DataTypes.STRING(), GenericRow.of(new Object[] {null}))).isNull();
        // a null sneaking into a NOT NULL column also renders as null, instead of crashing the
        // raw getter with an NPE
        assertThat(toStringOf(DataTypes.INT().copy(false), GenericRow.of(new Object[] {null})))
                .isNull();

        // nulls inside nested structures render as JSON null
        assertThat(
                        toStringOf(
                                DataTypes.ARRAY(DataTypes.INT()),
                                GenericRow.of(GenericArray.of(1, null, 3))))
                .isEqualTo("[1,null,3]");
        assertThat(
                        toStringOf(
                                DataTypes.ROW(new DataField("a", DataTypes.STRING())),
                                GenericRow.of(GenericRow.of(new Object[] {null}))))
                .isEqualTo("{\"a\":null}");
        Map<Object, Object> javaMap = new LinkedHashMap<>();
        javaMap.put(fromString("k"), null);
        assertThat(
                        toStringOf(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                GenericRow.of(new GenericMap(javaMap))))
                .isEqualTo("{\"k\":null}");
        // a null key (the declared non-nullability is not enforced by writers) is rendered as
        // the string "null" instead of crashing the rendering path
        Map<Object, Object> nullKeyMap = new LinkedHashMap<>();
        nullKeyMap.put(null, 1);
        assertFieldToStrings(
                new DataType[] {DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())},
                new Object[] {new GenericMap(nullKeyMap)},
                new String[] {"{\"null\":1}"});
    }

    @Test
    void testNestedArray() {
        assertThat(
                        toStringOf(
                                DataTypes.ARRAY(DataTypes.INT()),
                                GenericRow.of(GenericArray.of(1, 2, 3))))
                .isEqualTo("[1,2,3]");
        // empty array
        assertThat(toStringOf(DataTypes.ARRAY(DataTypes.INT()), GenericRow.of(GenericArray.of())))
                .isEqualTo("[]");
        // strings inside arrays are JSON-escaped and quoted
        InternalRow row =
                GenericRow.of(
                        GenericArray.of(
                                fromString("a\"b"),
                                fromString("back\\slash"),
                                fromString("中文"),
                                fromString("tab\tb")));
        assertThat(toStringOf(DataTypes.ARRAY(DataTypes.STRING()), row))
                .isEqualTo("[\"a\\\"b\",\"back\\\\slash\",\"中文\",\"tab\\tb\"]");
    }

    @Test
    void testNestedMap() {
        assertThat(
                        toStringOf(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                GenericRow.of(
                                        GenericMap.of(fromString("one"), 1, fromString("two"), 2))))
                .isEqualTo("{\"one\":1,\"two\":2}");
        assertThat(
                        toStringOf(
                                DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()),
                                GenericRow.of(new GenericMap(new LinkedHashMap<>()))))
                .isEqualTo("{}");
        // nested values inside maps
        Map<Object, Object> javaMap = new LinkedHashMap<>();
        javaMap.put(
                fromString("nums"),
                GenericArray.of(
                        Decimal.fromUnscaledLong(150, 10, 2),
                        Decimal.fromUnscaledLong(275, 10, 2)));
        assertThat(
                        toStringOf(
                                DataTypes.MAP(
                                        DataTypes.STRING(),
                                        DataTypes.ARRAY(DataTypes.DECIMAL(10, 2))),
                                GenericRow.of(new GenericMap(javaMap))))
                .isEqualTo("{\"nums\":[1.50,2.75]}");
    }

    @Test
    void testNestedRow() {
        DataType rowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("name", DataTypes.STRING()));
        assertThat(toStringOf(rowType, GenericRow.of(GenericRow.of(1, fromString("a")))))
                .isEqualTo("{\"id\":1,\"name\":\"a\"}");
        // row containing a map
        DataType rowWithMapType =
                DataTypes.ROW(
                        new DataField("m", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())));
        assertThat(
                        toStringOf(
                                rowWithMapType,
                                GenericRow.of(GenericRow.of(GenericMap.of(fromString("k"), 1)))))
                .isEqualTo("{\"m\":{\"k\":1}}");
    }

    @Test
    void testCreateFieldToStrings() {
        RowType rowType =
                DataTypes.ROW(
                        new DataField("id", DataTypes.INT()),
                        new DataField("tags", DataTypes.ARRAY(DataTypes.STRING())),
                        new DataField("d", DataTypes.DATE()));
        // Converters are stateless: rendering a row must not depend on previously rendered rows,
        // e.g. elements of a longer array must not leak into a shorter one.
        InternalRow.FieldToString[] converters = InternalRow.createFieldToStrings(rowType);
        int day = (int) LocalDate.of(2026, 10, 30).toEpochDay();
        InternalRow row1 = GenericRow.of(1, GenericArray.of(fromString("a"), fromString("b")), day);
        InternalRow row2 = GenericRow.of(2, GenericArray.of(fromString("c")), null);
        assertThat(render(converters, row1)).containsExactly("1", "[\"a\",\"b\"]", "2026-10-30");
        assertThat(render(converters, row2)).containsExactly("2", "[\"c\"]", null);
        // rendering after a row with a null field must not be affected by it
        assertThat(render(converters, row1)).containsExactly("1", "[\"a\",\"b\"]", "2026-10-30");
    }

    @Test
    void testConverterSerializationRoundTrip() throws Exception {
        // ARRAY converter
        InternalRow.FieldToString arrayConverter =
                InternalRow.createFieldToString(DataTypes.ARRAY(DataTypes.TIMESTAMP(3)), 0);
        InternalRow arrayRow =
                GenericRow.of(
                        GenericArray.of(
                                TimestampNtz.fromLocalDateTime(
                                        LocalDateTime.of(2026, 10, 30, 13, 35, 30, 123000000))));
        assertThat(roundTrip(arrayConverter).getFieldAsStringOrNull(arrayRow))
                .isEqualTo("[\"2026-10-30 13:35:30.123\"]");

        // MAP converter (captures key/value getters and their converters)
        InternalRow.FieldToString mapConverter =
                InternalRow.createFieldToString(
                        DataTypes.MAP(DataTypes.BIGINT(), DataTypes.STRING()), 0);
        assertThat(
                        roundTrip(mapConverter)
                                .getFieldAsStringOrNull(
                                        GenericRow.of(GenericMap.of(1L, fromString("x")))))
                .isEqualTo("{\"1\":\"x\"}");

        // ROW converter (captures field names and per-field getters)
        InternalRow.FieldToString rowConverter =
                InternalRow.createFieldToString(
                        DataTypes.ROW(
                                new DataField("id", DataTypes.INT()),
                                new DataField("name", DataTypes.STRING())),
                        0);
        assertThat(
                        roundTrip(rowConverter)
                                .getFieldAsStringOrNull(
                                        GenericRow.of(GenericRow.of(1, fromString("a")))))
                .isEqualTo("{\"id\":1,\"name\":\"a\"}");

        // scalar converters: method reference (STRING), temporal lambda capturing the precision
        // (TIMESTAMP), and the Base64 lambda (BYTES) — each branch has its own captured state, so
        // each family must stay serializable (e.g. hoisting a non-serializable Base64.Encoder
        // into a captured variable would only fail here)
        assertThat(
                        roundTrip(InternalRow.createFieldToString(DataTypes.STRING(), 0))
                                .getFieldAsStringOrNull(GenericRow.of(fromString("hello"))))
                .isEqualTo("hello");
        assertThat(
                        roundTrip(InternalRow.createFieldToString(DataTypes.TIMESTAMP(3), 0))
                                .getFieldAsStringOrNull(
                                        GenericRow.of(
                                                TimestampNtz.fromLocalDateTime(
                                                        LocalDateTime.of(
                                                                2026, 10, 30, 13, 35, 30,
                                                                123000000)))))
                .isEqualTo("2026-10-30 13:35:30.123");
        assertThat(
                        roundTrip(InternalRow.createFieldToString(DataTypes.BYTES(), 0))
                                .getFieldAsStringOrNull(GenericRow.of(new byte[] {1, 2, 3})))
                .isEqualTo("AQID");
    }

    /**
     * Asserts the string representation of each field for the same values held in an {@link
     * org.apache.fluss.row.indexed.IndexedRow}, in a {@link GenericRow} and in a {@link
     * org.apache.fluss.row.compacted.CompactedRow}: the results must be identical across row
     * implementations.
     */
    private static void assertFieldToStrings(DataType[] types, Object[] values, String[] expected) {
        RowType rowType = RowType.of(types);
        InternalRow indexedRow = DataTestUtils.indexedRow(rowType, values);
        InternalRow genericRow = GenericRow.of(values);
        InternalRow compactedRow = DataTestUtils.compactedRow(rowType, values);
        for (int i = 0; i < types.length; i++) {
            InternalRow.FieldToString converter = InternalRow.createFieldToString(types[i], i);
            assertThat(converter.getFieldAsStringOrNull(indexedRow))
                    .as("field %d of type %s, IndexedRow", i, types[i])
                    .isEqualTo(expected[i]);
            assertThat(converter.getFieldAsStringOrNull(genericRow))
                    .as("field %d of type %s, GenericRow", i, types[i])
                    .isEqualTo(expected[i]);
            assertThat(converter.getFieldAsStringOrNull(compactedRow))
                    .as("field %d of type %s, CompactedRow", i, types[i])
                    .isEqualTo(expected[i]);
        }
    }

    private static String toStringOf(DataType type, InternalRow row) {
        return InternalRow.createFieldToString(type, 0).getFieldAsStringOrNull(row);
    }

    private static List<String> render(InternalRow.FieldToString[] converters, InternalRow row) {
        List<String> values = new ArrayList<>();
        for (InternalRow.FieldToString converter : converters) {
            values.add(converter.getFieldAsStringOrNull(row));
        }
        return values;
    }

    /**
     * Asserts the string representation of each field after a round trip through arrow, the default
     * log format, read back as a {@link ColumnarRow}; the rendering happens inside this method
     * because the arrow vectors are released when it returns.
     */
    private static void assertColumnarFieldToStrings(
            RowType rowType, InternalRow row, String[] expected) throws Exception {
        try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                VectorSchemaRoot root =
                        VectorSchemaRoot.create(ArrowUtils.toArrowSchema(rowType), allocator);
                ArrowWriterPool provider = new ArrowWriterPool(allocator);
                ArrowWriter writer =
                        provider.getOrCreateWriter(
                                1L, 1, Integer.MAX_VALUE, rowType, NO_COMPRESSION)) {
            writer.writeRow(row);
            AbstractPagedOutputView pagedOutputView =
                    new ManagedPagedOutputView(new TestingMemorySegmentPool(10 * 1024));
            // skip the log record batch header, like the log record batch read path does
            int size =
                    writer.serializeToOutputView(
                            pagedOutputView, recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE));
            MemorySegment segment =
                    MemorySegment.allocateHeapMemory(Math.max(size, writer.estimatedSizeInBytes()));
            pagedOutputView
                    .getCurrentSegment()
                    .copyTo(recordBatchHeaderSize(CURRENT_LOG_MAGIC_VALUE), segment, 0, size);

            ArrowReader reader =
                    ArrowUtils.createArrowReader(segment, 0, size, root, allocator, rowType);
            ColumnarRow columnarRow = reader.read(0);
            columnarRow.setRowId(0);
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                assertThat(
                                InternalRow.createFieldToString(rowType.getTypeAt(i), i)
                                        .getFieldAsStringOrNull(columnarRow))
                        .as("field %d of type %s, ColumnarRow", i, rowType.getTypeAt(i))
                        .isEqualTo(expected[i]);
            }
        }
    }

    private static InternalRow.FieldToString roundTrip(InternalRow.FieldToString converter)
            throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeObject(converter);
        }
        return (InternalRow.FieldToString)
                new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray())).readObject();
    }
}
