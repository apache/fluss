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

package org.apache.fluss.record;

import org.apache.fluss.compression.ArrowCompressionInfo;
import org.apache.fluss.compression.ArrowCompressionType;
import org.apache.fluss.memory.ManagedPagedOutputView;
import org.apache.fluss.memory.TestingMemorySegmentPool;
import org.apache.fluss.memory.UnmanagedPagedOutputView;
import org.apache.fluss.metadata.KvFormat;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.Schema;
import org.apache.fluss.metadata.SchemaInfo;
import org.apache.fluss.metadata.TableDescriptor;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.LogRecordReadContext.SchemaResolution;
import org.apache.fluss.record.bytesview.BytesView;
import org.apache.fluss.row.BinaryRow;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.Decimal;
import org.apache.fluss.row.GenericArray;
import org.apache.fluss.row.GenericMap;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalArray;
import org.apache.fluss.row.InternalMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.InternalRow.FieldGetter;
import org.apache.fluss.row.TimestampLtz;
import org.apache.fluss.row.TimestampNtz;
import org.apache.fluss.row.arrow.ArrowWriter;
import org.apache.fluss.row.arrow.ArrowWriterPool;
import org.apache.fluss.row.compacted.CompactedRow;
import org.apache.fluss.row.encode.RowEncoder;
import org.apache.fluss.row.indexed.IndexedRow;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.BufferAllocator;
import org.apache.fluss.shaded.arrow.org.apache.arrow.memory.RootAllocator;
import org.apache.fluss.types.ArrayType;
import org.apache.fluss.types.DataField;
import org.apache.fluss.types.DataType;
import org.apache.fluss.types.DataTypeRoot;
import org.apache.fluss.types.DataTypes;
import org.apache.fluss.types.DecimalType;
import org.apache.fluss.types.MapType;
import org.apache.fluss.types.RowType;
import org.apache.fluss.utils.CloseableIterator;
import org.apache.fluss.utils.Projection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.fluss.record.ChangeType.APPEND_ONLY;
import static org.apache.fluss.record.TestData.DEFAULT_MAGIC;
import static org.apache.fluss.record.TestData.DEFAULT_REMOTE_DATA_DIR;
import static org.apache.fluss.record.TestData.DEFAULT_SCHEMA_ID;
import static org.apache.fluss.types.DataTypeChecks.getPrecision;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LogRecordReadContext}. */
class LogRecordReadContextTest {

    /** Every leaf type, with both encodings of DECIMAL and the timestamps. */
    private static final DataType[] LEAF_TYPES = {
        DataTypes.BOOLEAN(),
        DataTypes.TINYINT(),
        DataTypes.SMALLINT(),
        DataTypes.INT(),
        DataTypes.BIGINT(),
        DataTypes.FLOAT(),
        DataTypes.DOUBLE(),
        DataTypes.DATE(),
        DataTypes.TIME(),
        DataTypes.CHAR(3),
        DataTypes.STRING(),
        DataTypes.BINARY(3),
        DataTypes.BYTES(),
        DataTypes.DECIMAL(5, 2),
        DataTypes.DECIMAL(20, 2),
        DataTypes.TIMESTAMP(3),
        DataTypes.TIMESTAMP(9),
        DataTypes.TIMESTAMP_LTZ(3),
        DataTypes.TIMESTAMP_LTZ(9)
    };

    private static final RowType ROW_TYPE = allTypesRowType();

    @ParameterizedTest
    @EnumSource(ArrowCompressionType.class)
    void testArrowProjectionLoadsOnlySelectedColumns(ArrowCompressionType compression)
            throws Exception {
        RowType rowType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.INT()),
                        DataTypes.FIELD("unused", DataTypes.STRING()),
                        DataTypes.FIELD("value", DataTypes.STRING()));
        Schema schema = Schema.newBuilder().fromRowType(rowType).build();
        GenericRow row =
                GenericRow.of(
                        42, BinaryString.fromString("unused"), BinaryString.fromString("selected"));
        for (boolean appendOnly : new boolean[] {true, false}) {
            LogRecordBatch batch =
                    buildArrowBatch(
                            rowType,
                            DEFAULT_SCHEMA_ID,
                            compression,
                            appendOnly,
                            row,
                            GenericRow.of(43, null, null));
            try (LogRecordReadContext context =
                    LogRecordReadContext.createReadContext(
                            tableInfo(LogFormat.ARROW, schema, DEFAULT_SCHEMA_ID),
                            true,
                            SchemaResolution.TARGET,
                            Projection.of(new int[] {2, 0}),
                            new TestingSchemaGetter(DEFAULT_SCHEMA_ID, schema),
                            null)) {
                assertThat(context.isProjectionPushDowned()).isFalse();
                batch.ensureValid();
                try (CloseableIterator<LogRecord> records = batch.records(context)) {
                    FieldGetter[] getters = context.getSelectedFieldGetters(DEFAULT_SCHEMA_ID);
                    LogRecord first = records.next();
                    assertThat(getters[0].getFieldOrNull(first.getRow()).toString())
                            .isEqualTo("selected");
                    assertThat(getters[1].getFieldOrNull(first.getRow())).isEqualTo(42);
                    assertThat(first.getChangeType())
                            .isEqualTo(appendOnly ? APPEND_ONLY : ChangeType.UPDATE_AFTER);
                    InternalRow second = records.next().getRow();
                    assertThat(getters[0].getFieldOrNull(second)).isNull();
                    assertThat(getters[1].getFieldOrNull(second)).isEqualTo(43);
                    assertThat(records.hasNext()).isFalse();
                    assertThat(
                                    context.getArrowReadPlan(DEFAULT_SCHEMA_ID)
                                            .getLoadedRowType()
                                            .getFieldNames())
                            .containsExactly("id", "value");
                }
                assertThat(context.getBufferAllocator().getAllocatedMemory()).isZero();
                batch.ensureValid();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(ArrowCompressionType.class)
    void testArrowProjectionWithNestedColumns(ArrowCompressionType compression) throws Exception {
        Schema schema = Schema.newBuilder().fromRowType(ROW_TYPE).build();
        GenericRow expected = (GenericRow) sampleValue(ROW_TYPE);
        int count = ROW_TYPE.getFieldCount();
        int[] selected = {count - 1, count - 2, count - 3, 2, 1, 0};
        LogRecordBatch batch =
                buildArrowBatch(
                        ROW_TYPE,
                        DEFAULT_SCHEMA_ID,
                        compression,
                        true,
                        expected,
                        new GenericRow(count));
        try (LogRecordReadContext context =
                LogRecordReadContext.createReadContext(
                        tableInfo(LogFormat.ARROW, schema, DEFAULT_SCHEMA_ID),
                        true,
                        SchemaResolution.TARGET,
                        Projection.of(selected),
                        new TestingSchemaGetter(DEFAULT_SCHEMA_ID, schema),
                        null)) {
            // Reusing the root must not retain buffers from the preceding batch.
            for (int iteration = 0; iteration < 2; iteration++) {
                try (CloseableIterator<LogRecord> records = batch.records(context)) {
                    FieldGetter[] getters = context.getSelectedFieldGetters(DEFAULT_SCHEMA_ID);
                    InternalRow first = records.next().getRow();
                    for (int i = 0; i < selected.length; i++) {
                        DataType type = ROW_TYPE.getTypeAt(selected[i]);
                        assertThat(toJava(getters[i].getFieldOrNull(first), type))
                                .isEqualTo(toJava(expected.getField(selected[i]), type));
                    }
                    InternalRow second = records.next().getRow();
                    for (FieldGetter getter : getters) {
                        assertThat(getter.getFieldOrNull(second)).isNull();
                    }
                    assertThat(records.hasNext()).isFalse();
                    assertThat(
                                    context.getArrowReadPlan(DEFAULT_SCHEMA_ID)
                                            .getLoadedRowType()
                                            .getFieldCount())
                            .isEqualTo(selected.length);
                }
                assertThat(context.getBufferAllocator().getAllocatedMemory()).isZero();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(SchemaResolution.class)
    void testArrowProjectionWithSchemaEvolution(SchemaResolution resolution) throws Exception {
        Schema source =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("unused", DataTypes.STRING())
                        .column("value", DataTypes.STRING())
                        .build();
        Schema target =
                Schema.newBuilder()
                        .fromColumns(
                                Arrays.asList(
                                        new Schema.Column("added", DataTypes.STRING(), null, 100),
                                        new Schema.Column(
                                                "renamed",
                                                DataTypes.STRING(),
                                                null,
                                                source.getColumn("value").getColumnId()),
                                        source.getColumn("id")))
                        .build();
        TestingSchemaGetter schemas = new TestingSchemaGetter(0, source);
        schemas.updateLatestSchemaInfo(new SchemaInfo(target, 1));
        LogRecordBatch oldBatch =
                buildArrowBatch(
                        source.getRowType(),
                        0,
                        ArrowCompressionType.ZSTD,
                        true,
                        GenericRow.of(
                                42,
                                BinaryString.fromString("skip"),
                                BinaryString.fromString("old")));
        LogRecordBatch newBatch =
                buildArrowBatch(
                        target.getRowType(),
                        1,
                        ArrowCompressionType.LZ4_FRAME,
                        true,
                        GenericRow.of(
                                BinaryString.fromString("new"),
                                BinaryString.fromString("value"),
                                43));
        try (LogRecordReadContext context =
                LogRecordReadContext.createReadContext(
                        tableInfo(LogFormat.ARROW, target, 1),
                        true,
                        resolution,
                        Projection.of(new int[] {1, 0}),
                        schemas,
                        null)) {
            assertThat(context.getArrowReadPlan(0)).isSameAs(context.getArrowReadPlan(0));
            assertThat(context.getArrowReadPlan(0).getLoadedRowType().getFieldNames())
                    .containsExactly("value");
            assertThat(context.getArrowReadPlan(1).getLoadedRowType().getFieldNames())
                    .containsExactly("added", "renamed");
            FieldGetter[] getters = context.getSelectedFieldGetters(0);
            try (CloseableIterator<LogRecord> records = oldBatch.records(context)) {
                InternalRow row = records.next().getRow();
                assertThat(getters[0].getFieldOrNull(row).toString()).isEqualTo("old");
                assertThat(getters[1].getFieldOrNull(row)).isNull();
            }
            try (CloseableIterator<LogRecord> records = newBatch.records(context)) {
                InternalRow row = records.next().getRow();
                assertThat(getters[0].getFieldOrNull(row).toString()).isEqualTo("value");
                assertThat(getters[1].getFieldOrNull(row).toString()).isEqualTo("new");
            }
            assertThat(context.getBufferAllocator().getAllocatedMemory()).isZero();
        }

        // A requested column can be absent from every physical field of a historical batch.
        try (LogRecordReadContext context =
                        LogRecordReadContext.createReadContext(
                                tableInfo(LogFormat.ARROW, target, 1),
                                true,
                                resolution,
                                Projection.of(new int[] {0}),
                                schemas,
                                null);
                CloseableIterator<LogRecord> records = oldBatch.records(context)) {
            assertThat(context.getArrowReadPlan(0).getLoadedRowType().getFieldCount()).isZero();
            assertThat(
                            context.getSelectedFieldGetters(0)[0].getFieldOrNull(
                                    records.next().getRow()))
                    .isNull();
            assertThat(records.hasNext()).isFalse();
        }
    }

    @Test
    void testArrowProjectionFastPaths() {
        Schema schema =
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.STRING())
                        .build();
        TableInfo table = tableInfo(LogFormat.ARROW, schema, DEFAULT_SCHEMA_ID);
        TestingSchemaGetter schemas = new TestingSchemaGetter(DEFAULT_SCHEMA_ID, schema);
        try (LogRecordReadContext allFields =
                        LogRecordReadContext.createReadContext(
                                table,
                                true,
                                SchemaResolution.TARGET,
                                Projection.of(new int[] {1, 0}),
                                schemas,
                                null);
                LogRecordReadContext dynamic =
                        LogRecordReadContext.createReadContext(
                                table, true, SchemaResolution.DYNAMIC, null, schemas, null);
                LogRecordReadContext serverProjected =
                        LogRecordReadContext.createReadContext(
                                table,
                                false,
                                SchemaResolution.TARGET,
                                Projection.of(new int[] {1}),
                                schemas,
                                null)) {
            for (LogRecordReadContext context : Arrays.asList(allFields, dynamic)) {
                ArrowReadPlan plan = context.getArrowReadPlan(DEFAULT_SCHEMA_ID);
                assertThat(plan.getLoadedRowType()).isEqualTo(schema.getRowType());
                assertThat(plan.createOutputRow()).isNull();
            }
            assertThat(serverProjected.isProjectionPushDowned()).isTrue();
            ArrowReadPlan serverPlan = serverProjected.getArrowReadPlan(DEFAULT_SCHEMA_ID);
            assertThat(serverPlan.getLoadedRowType().getFieldNames()).containsExactly("b");
            assertThat(serverPlan.createOutputRow()).isNull();
        }
    }

    @Test
    void testRowTypeCoversEveryTypeRoot() {
        Set<DataTypeRoot> roots = EnumSet.noneOf(DataTypeRoot.class);
        collectTypeRoots(ROW_TYPE, roots);
        // A new type must be added here, so the test below checks that it doesn't borrow the
        // network buffer.
        assertThat(roots).containsExactlyInAnyOrder(DataTypeRoot.values());
    }

    @ParameterizedTest
    @EnumSource(
            value = LogFormat.class,
            names = {"INDEXED", "COMPACTED"})
    void testSelectedFieldGettersCopyOutOfBatch(LogFormat logFormat) throws Exception {
        GenericRow expected = (GenericRow) sampleValue(ROW_TYPE);
        for (SchemaResolution schemaResolution : SchemaResolution.values()) {
            byte[] batchBytes = buildBatch(logFormat, expected);
            List<GenericRow> copies = new ArrayList<>();
            try (LogRecordReadContext readContext =
                    LogRecordReadContext.createReadContext(
                            tableInfo(logFormat),
                            false,
                            schemaResolution,
                            null,
                            new TestingSchemaGetter(DEFAULT_SCHEMA_ID, ROW_TYPE),
                            null)) {
                LogRecordBatch batch =
                        MemoryLogRecords.pointToBytes(batchBytes).batches().iterator().next();
                FieldGetter[] fieldGetters = readContext.getSelectedFieldGetters(batch.schemaId());
                try (CloseableIterator<LogRecord> records = batch.records(readContext)) {
                    while (records.hasNext()) {
                        InternalRow row = records.next().getRow();
                        GenericRow copy = new GenericRow(fieldGetters.length);
                        for (int i = 0; i < fieldGetters.length; i++) {
                            copy.setField(i, fieldGetters[i].getFieldOrNull(row));
                        }
                        copies.add(copy);
                    }
                }
            }

            // The client releases the fetch buffer once the records are converted, and the pool
            // hands the memory to the next response.
            Arrays.fill(batchBytes, (byte) 0);

            assertThat(copies).hasSize(1);
            for (int i = 0; i < ROW_TYPE.getFieldCount(); i++) {
                DataType fieldType = ROW_TYPE.getTypeAt(i);
                assertThat(toJava(copies.get(0).getField(i), fieldType))
                        .as(
                                "%s, %s schema, field %s",
                                logFormat, schemaResolution, ROW_TYPE.getFieldNames().get(i))
                        .isEqualTo(toJava(expected.getField(i), fieldType));
            }
        }
    }

    private static RowType allTypesRowType() {
        List<DataField> leafFields = new ArrayList<>();
        List<DataField> fields = new ArrayList<>();
        for (int i = 0; i < LEAF_TYPES.length; i++) {
            DataType leaf = LEAF_TYPES[i];
            leafFields.add(new DataField("n" + i, leaf));
            fields.add(new DataField("f" + i, leaf));
            fields.add(new DataField("array_f" + i, DataTypes.ARRAY(leaf)));
            fields.add(
                    new DataField(
                            "map_f" + i, DataTypes.MAP(DataTypes.STRING().copy(false), leaf)));
        }
        RowType leafRow = new RowType(leafFields);
        fields.add(new DataField("row", leafRow));
        fields.add(new DataField("array_row", DataTypes.ARRAY(leafRow)));
        fields.add(
                new DataField("array_array", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()))));
        return new RowType(fields);
    }

    private static void collectTypeRoots(DataType type, Set<DataTypeRoot> roots) {
        roots.add(type.getTypeRoot());
        for (DataType child : type.getChildren()) {
            collectTypeRoots(child, roots);
        }
    }

    private static Object sampleValue(DataType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return true;
            case TINYINT:
                return (byte) 7;
            case SMALLINT:
                return (short) 300;
            case INTEGER:
                return 70_000;
            case BIGINT:
                return 5_000_000_000L;
            case FLOAT:
                return 1.5f;
            case DOUBLE:
                return 2.25d;
            case DATE:
                return 19_000;
            case TIME_WITHOUT_TIME_ZONE:
                return 3_600_000;
            case CHAR:
                return BinaryString.fromString("abc");
            case STRING:
                return BinaryString.fromString("hello");
            case BINARY:
                return new byte[] {1, 2, 3};
            case BYTES:
                return new byte[] {4, 5, 6, 7};
            case DECIMAL:
                DecimalType decimalType = (DecimalType) type;
                return Decimal.fromBigDecimal(
                        new BigDecimal("123.45"),
                        decimalType.getPrecision(),
                        decimalType.getScale());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return TimestampNtz.fromMillis(1_700_000_000_123L, nanosOfMillisecond(type));
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return TimestampLtz.fromEpochMillis(1_700_000_000_123L, nanosOfMillisecond(type));
            case ARRAY:
                DataType elementType = ((ArrayType) type).getElementType();
                return new GenericArray(
                        new Object[] {sampleValue(elementType), sampleValue(elementType)});
            case MAP:
                MapType mapType = (MapType) type;
                Map<Object, Object> map = new HashMap<>();
                map.put(sampleValue(mapType.getKeyType()), sampleValue(mapType.getValueType()));
                return new GenericMap(map);
            case ROW:
                RowType rowType = (RowType) type;
                GenericRow row = new GenericRow(rowType.getFieldCount());
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    row.setField(i, sampleValue(rowType.getTypeAt(i)));
                }
                return row;
            default:
                throw new IllegalArgumentException("No sample value for type " + type);
        }
    }

    private static int nanosOfMillisecond(DataType timestampType) {
        // compact timestamps only keep milliseconds
        return getPrecision(timestampType) > 3 ? 456_789 : 0;
    }

    /** Converts an internal value to plain Java values, so that equality ignores its encoding. */
    private static Object toJava(Object value, DataType type) {
        if (value == null) {
            return null;
        }
        switch (type.getTypeRoot()) {
            case CHAR:
            case STRING:
                return value.toString();
            case BINARY:
            case BYTES:
                return Arrays.toString((byte[]) value);
            case ARRAY:
                DataType elementType = ((ArrayType) type).getElementType();
                InternalArray array = (InternalArray) value;
                InternalArray.ElementGetter elementGetter =
                        InternalArray.createElementGetter(elementType);
                List<Object> elements = new ArrayList<>();
                for (int i = 0; i < array.size(); i++) {
                    elements.add(toJava(elementGetter.getElementOrNull(array, i), elementType));
                }
                return elements;
            case MAP:
                MapType mapType = (MapType) type;
                InternalMap map = (InternalMap) value;
                List<?> keys =
                        (List<?>) toJava(map.keyArray(), DataTypes.ARRAY(mapType.getKeyType()));
                List<?> values =
                        (List<?>) toJava(map.valueArray(), DataTypes.ARRAY(mapType.getValueType()));
                Map<Object, Object> entries = new HashMap<>();
                for (int i = 0; i < keys.size(); i++) {
                    entries.put(keys.get(i), values.get(i));
                }
                return entries;
            case ROW:
                RowType rowType = (RowType) type;
                InternalRow row = (InternalRow) value;
                List<Object> fields = new ArrayList<>();
                for (int i = 0; i < rowType.getFieldCount(); i++) {
                    DataType fieldType = rowType.getTypeAt(i);
                    fields.add(
                            toJava(
                                    InternalRow.createFieldGetter(fieldType, i).getFieldOrNull(row),
                                    fieldType));
                }
                return fields;
            default:
                return value;
        }
    }

    private static byte[] buildBatch(LogFormat logFormat, GenericRow row) throws Exception {
        KvFormat rowFormat = logFormat == LogFormat.INDEXED ? KvFormat.INDEXED : KvFormat.COMPACTED;
        RowEncoder rowEncoder = RowEncoder.create(rowFormat, ROW_TYPE);
        rowEncoder.startNewRow();
        for (int i = 0; i < ROW_TYPE.getFieldCount(); i++) {
            rowEncoder.encodeField(i, row.getField(i));
        }
        BinaryRow binaryRow = rowEncoder.finishRow();

        int pageSize = 64 * 1024;
        ManagedPagedOutputView outputView =
                new ManagedPagedOutputView(new TestingMemorySegmentPool(pageSize));
        BytesView bytesView;
        if (logFormat == LogFormat.INDEXED) {
            MemoryLogRecordsIndexedBuilder builder =
                    MemoryLogRecordsIndexedBuilder.builder(
                            0L, DEFAULT_SCHEMA_ID, pageSize, DEFAULT_MAGIC, outputView);
            builder.append(APPEND_ONLY, (IndexedRow) binaryRow);
            builder.close();
            bytesView = builder.build();
        } else {
            MemoryLogRecordsCompactedBuilder builder =
                    MemoryLogRecordsCompactedBuilder.builder(
                            0L, DEFAULT_SCHEMA_ID, pageSize, DEFAULT_MAGIC, outputView);
            builder.append(APPEND_ONLY, (CompactedRow) binaryRow);
            builder.close();
            bytesView = builder.build();
        }

        // copy the batch into memory the test owns, so that it can overwrite it later
        MemoryLogRecords records = MemoryLogRecords.pointToBytesView(bytesView);
        byte[] bytes = new byte[records.sizeInBytes()];
        records.getMemorySegment().get(records.getPosition(), bytes, 0, bytes.length);
        return bytes;
    }

    private static TableInfo tableInfo(LogFormat logFormat) {
        return tableInfo(
                logFormat, Schema.newBuilder().fromRowType(ROW_TYPE).build(), DEFAULT_SCHEMA_ID);
    }

    private static TableInfo tableInfo(LogFormat logFormat, Schema schema, int schemaId) {
        TableDescriptor descriptor =
                TableDescriptor.builder()
                        .schema(schema)
                        .distributedBy(1)
                        .logFormat(logFormat)
                        .build();
        return TableInfo.of(
                TablePath.of("test_db", "test_table"),
                1L,
                schemaId,
                descriptor,
                DEFAULT_REMOTE_DATA_DIR,
                0L,
                0L);
    }

    private static LogRecordBatch buildArrowBatch(
            RowType rowType,
            int schemaId,
            ArrowCompressionType compression,
            boolean appendOnly,
            GenericRow... rows)
            throws Exception {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowWriterPool pool = new ArrowWriterPool(allocator)) {
            ArrowWriter writer =
                    pool.getOrCreateWriter(
                            1L,
                            schemaId,
                            1024 * 1024,
                            rowType,
                            new ArrowCompressionInfo(compression, 3));
            try (MemoryLogRecordsArrowBuilder builder =
                    MemoryLogRecordsArrowBuilder.builder(
                            0L,
                            DEFAULT_MAGIC,
                            schemaId,
                            writer,
                            new UnmanagedPagedOutputView(1024 * 1024),
                            appendOnly)) {
                for (GenericRow row : rows) {
                    builder.append(appendOnly ? APPEND_ONLY : ChangeType.UPDATE_AFTER, row);
                }
                MemoryLogRecords records = MemoryLogRecords.pointToBytesView(builder.build());
                byte[] bytes = new byte[records.sizeInBytes()];
                records.getMemorySegment().get(records.getPosition(), bytes, 0, bytes.length);
                return MemoryLogRecords.pointToBytes(bytes).batches().iterator().next();
            }
        }
    }
}
